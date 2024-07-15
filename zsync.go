// Copyright 2017, The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.md file.

package main

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"os"
	"os/user"
	"slices"
	"strings"
	"time"

	"github.com/dsnet/golib/cron"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
	gomail "gopkg.in/gomail.v2"
	"tailscale.com/syncs"
)

type dataset struct {
	name   string
	target execTarget

	latestSnapshot syncs.AtomicValue[string]
}

// PoolPath returns the pool path (e.g., ""//host/pool").
func (ds dataset) PoolPath() string {
	pool := ds.name
	if i := strings.IndexByte(pool, '/'); i >= 0 {
		pool = pool[:i]
	}
	return fmt.Sprintf("//%s/%s", ds.target.host, pool)
}

// DatasetPath returns the dataset path (e.g., "//host/pool/dataset").
func (ds dataset) DatasetPath() string {
	return fmt.Sprintf("//%s/%s", ds.target.host, ds.name)
}

// SnapshotPath returns the snapshot path (e.g., "//host/pool/dataset@snap").
func (ds dataset) SnapshotPath(s string) string {
	return fmt.Sprintf("%s@%s", ds.DatasetPath(), s)
}

// SnapshotName returns the snapshot name (e.g., "pool/dataset@snap").
func (ds dataset) SnapshotName(s string) string {
	return fmt.Sprintf("%s@%s", ds.name, s)
}

type zsyncer struct {
	log *log.Logger

	replSema chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc

	smtp smtpConfig
	http httpConfig

	poolMonitors     map[string]*poolMonitor
	replicaManagers  map[string]*replicaManager
	snapshotManagers map[string]*snapshotManager
}

func newZSyncer(conf config, logger *log.Logger) *zsyncer {
	ctx, cancel := context.WithCancel(context.Background())
	zs := &zsyncer{
		log: logger,

		replSema: make(chan struct{}, conf.ConcurrentTransfers),
		ctx:      ctx,
		cancel:   cancel,

		smtp: conf.SMTP,
		http: conf.HTTP,

		poolMonitors:     make(map[string]*poolMonitor),
		replicaManagers:  make(map[string]*replicaManager),
		snapshotManagers: make(map[string]*snapshotManager),
	}

	// Process SSH-related configuration options.
	var (
		auth         []ssh.AuthMethod
		hostKeys     ssh.HostKeyCallback
		keepAlive    keepAliveConfig
		localAliases []string
	)

	// Parse all of the private keys.
	var keys []ssh.Signer
	for _, kf := range conf.SSH.KeyFiles {
		b, err := os.ReadFile(kf)
		if err != nil {
			logger.Fatalf("private key error: %v", err)
		}
		k, err := ssh.ParsePrivateKey(b)
		if err != nil {
			logger.Fatalf("private key error: %v", err)
		}
		keys = append(keys, k)
	}
	if len(keys) > 0 {
		auth = append(auth, ssh.PublicKeys(keys...))
	}

	// Parse all of the host public keys.
	if len(conf.SSH.KnownHostFiles) == 0 && conf.SSH.KnownHostFiles != nil {
		hostKeys = ssh.InsecureIgnoreHostKey()
	} else if len(conf.SSH.KnownHostFiles) > 0 {
		var err error
		hostKeys, err = knownhosts.New(conf.SSH.KnownHostFiles...)
		if err != nil {
			logger.Fatalf("public key error: %v", err)
		}
	}

	keepAlive = *conf.SSH.KeepAlive
	localAliases = append([]string{"localhost"}, conf.SSH.LocalhostAliases...)
	if host, _, ok := strings.Cut(conf.HTTP.Address, ":"); ok && host != "" {
		localAliases = append(localAliases, host)
	}

	// Process each of the dataset sources and their mirrors.
	for _, ds := range conf.Datasets {
		makeDataset := func(dp datasetPath) dataset {
			ds := dataset{
				name:   strings.Trim(dp.Path, "/"),
				target: execTarget{host: dp.Hostname()},
			}
			isLocalhost := slices.Contains(localAliases, ds.target.host)
			if isLocalhost && dp.Port() == "" {
				ds.target.isLocalhost = true
				return ds
			}

			// Setup all parameters for SSH.
			ds.target.port = cmp.Or(dp.Port(), "22")
			ds.target.auth = append([]ssh.AuthMethod{}, auth...)
			ds.target.hostKeys = hostKeys
			if dp.User != nil {
				ds.target.user = dp.User.Username()
				if p, ok := dp.User.Password(); ok {
					ds.target.auth = append(ds.target.auth, ssh.Password(p))
				}
			} else {
				u, err := user.Current()
				if err != nil {
					logger.Fatalf("unexpected error: %v", err)
				}
				ds.target.user = u.Username
			}
			ds.target.keepAlive = keepAlive
			if ds.target.hostKeys == nil {
				logger.Fatal("no hostkey callback specified")
			}
			return ds
		}

		src := makeDataset(ds.Source)
		var dsts []dataset
		for _, dp := range ds.Mirrors {
			dsts = append(dsts, makeDataset(dp))
		}

		// Parse replica manager options.
		sendFlags, recvFlags, initRecvFlags := conf.SendFlags, conf.RecvFlags, conf.InitRecvFlags
		if ds.SendFlags != nil {
			sendFlags = ds.SendFlags
		}
		if ds.RecvFlags != nil {
			recvFlags = ds.RecvFlags
		}
		if ds.InitRecvFlags != nil {
			initRecvFlags = ds.InitRecvFlags
		}

		// Parse snapshot manager options.
		var ssOpts snapshotOptions
		if as := ds.AutoSnapshot; as != nil {
			ssOpts = *as
		} else if as := conf.AutoSnapshot; as != nil {
			ssOpts = *as
		}
		sched, err := cron.ParseSchedule(ssOpts.Cron)
		if err != nil {
			logger.Fatalf("could not parse: %v", err)
		}
		tz, err := time.LoadLocation(ssOpts.TimeZone)
		if err != nil {
			logger.Fatalf("invalid timezone: %v", err)
		}

		// Register the pool monitors, dataset replicator, and dataset manager.
		zs.RegisterPoolMonitors(src, dsts)
		zs.RegisterReplicaManager(src, dsts, sendFlags, recvFlags, initRecvFlags)
		zs.RegisterSnapshotManager(src, dsts, sched, tz, ssOpts.Count, ssOpts.SkipEmpty)
	}
	return zs
}

func (zs *zsyncer) Run() {
	if zs.http.Address != "" {
		go zs.ServeHTTP()
	}

	var group syncs.WaitGroup
	defer group.Wait()
	for _, pm := range zs.poolMonitors {
		group.Go(pm.Run)
	}
	for _, rm := range zs.replicaManagers {
		group.Go(rm.Run)
	}
	for _, sm := range zs.snapshotManagers {
		group.Go(sm.Run)
	}
}

func (zs *zsyncer) Close() error {
	zs.cancel()
	return nil
}

// zsyncError is a wrapper for only recovering panics from checkError.
type zsyncError struct{ error }

func recoverError(f func(error)) {
	ex := recover()
	if ze, ok := ex.(zsyncError); ok {
		f(ze.error)
	} else if ex != nil {
		panic(ex)
	}
}
func checkError(err error) {
	if err != nil {
		panic(zsyncError{err})
	}
}

func trySignal(c chan<- struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

func indentLines(s string) string {
	ss := strings.Split(strings.Trim(s, "\n"), "\n")
	return "\t" + strings.Join(ss, "\n\t")
}

// timeoutAfter returns the timeout to wait before retrying an operation.
// It takes the previous timeout (starting at zero) as input.
func timeoutAfter(d time.Duration) time.Duration {
	switch {
	case d < 1*time.Minute:
		return 1 * time.Minute
	case d < 5*time.Minute:
		return 5 * time.Minute
	case d < 10*time.Minute:
		return 10 * time.Minute
	case d < 30*time.Minute:
		return 30 * time.Minute
	default:
		return 60 * time.Minute
	}
}

func sendEmail(smtp smtpConfig, subject, body string) error {
	msg := gomail.NewMessage()
	msg.SetAddressHeader("From", smtp.Username, "ZSync Daemon")
	msg.SetAddressHeader("To", smtp.ToAddress, smtp.ToName)
	msg.SetHeader("Subject", subject)
	msg.SetBody("text/html", body)

	d := gomail.NewDialer(smtp.Host, smtp.Port, smtp.Username, smtp.Password)
	return d.DialAndSend(msg)
}

type (
	funcReader func([]byte) (int, error)
	funcWriter func([]byte) (int, error)
)

func (f funcReader) Read(b []byte) (int, error)  { return f(b) }
func (f funcWriter) Write(b []byte) (int, error) { return f(b) }
