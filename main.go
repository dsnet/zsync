// Copyright 2017, The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.md file.

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path"
	"reflect"
	"strings"
	"syscall"

	"github.com/dsnet/golib/jsonfmt"
)

// Version of the zsync binary. May be set by linker when building.
var version string

const Help = `
The zsync daemon auto-snapshots ZFS datasets, replicates datasets, and
auto-deletes stale snapshots. The replication can be performed across machines
in either a push or pull mechanism using either SSH or local OS subprocesses.

In order for the daemon to properly perform ZFS operations, the "zfs allow"
feature must be used to enable permissions on certain operations.

The following permissions should be granted:
	# On all sources:
	sudo zfs allow $USER send $DATASET

	# On all mirrors:
	sudo zfs allow $USER receive,create,mount,mountpoint,readonly $DATASET

	# On all sources and mirrors:
	sudo zfs allow $USER snapshot,destroy,mount $DATASET


The operation of zsync is configured using a JSON configuration file.
The JSON format used permits the use of comments and takes the following form:
{
	// The default value of each field is shown, unless otherwise specified.

	// LogFile is where the daemon will direct its output log.
	// If the path is empty, then the log outputs to os.Stderr.
	"LogFile": "",

	// SMTP configures a mail client to email about potential issues.
	// Emails are sent when pools switch health states,
	// when snapshots cannot be created or deleted, and
	// when replication fails. Errors due to network failures are ignored.
	"SMTP": {
		"Host": "", // E.g., "mail.name.com"
		"Port": 587,
		"Username": "", // E.g., "zsync-daemon@example.com"
		"Password": "",
		"ToAddress": "", // E.g., "user@example.com"
		"ToName": "",    // E.g., "FirstName LastName"
	},

	// HTTP configures a web server to diagnose zsync progress.
	"HTTP": {
		// Address is the TCP network address for the server to bind to.
		// By default, there is no HTTP server enabled.
		"Address": "",
	},

	// SSH is a map of SSH-related configuration options.
	"SSH": {
		// KeyFiles is a list of SSH private key files.
		"KeyFiles": null, // E.g., ["key.priv"]

		// KnownHostFiles is a list of key database files for host public keys
		// in the OpenSSH known_hosts file format.
		//
		// Host-key checking is disabled if the empty list is specified.
		"KnownHostFiles": null, // E.g., ["known_hosts"]

		// KeepAlive sets the keep alive settings for each SSH connection.
		// It is recommended that these values match the AliveInterval and
		// AliveCountMax parameters on the remote OpenSSH server.
		"KeepAlive": {
			"Interval": 30, "CountMax": 2,
		},

		// LocalhostAlias considers the hostname provided to be equivalent
		// to "localhost". This allows a vanity hostname to be used, yet
		// allowing the use of OS subprocesses instead of SSH subprocesses.
		"LocalhostAlias": "", // E.g., ["myhostname.local"]
	},

	// ConcurrentTransfers specifies the maximum number of concurrent transfers
	// that may occur. Even if a source has multiple mirrors, at most one
	// transfer occurs at a time for each source.
	"ConcurrentTransfers": 1,

	// AutoSnapshot specifies when snapshots are taken and how many to keep.
	"AutoSnapshot": {
		// Cron uses the standard cron syntax to specify when snapshots trigger.
		"Cron": "@daily", // E.g., "0 0 * * * *"

		// TimeZone is the time zone to run the cron schedule in.
		"TimeZone": "Local", // E.g., "UTC" or "America/Los_Angeles"

		// Snapshots are automatically deleted after this many are made.
		// Snapshots are only deleted if there exist at least some
		// common snapshot across the source and all mirrors.
		// A zero value indicates that snapshots are never deleted.
		"Count": 0,

		// SkipEmpty skips taking a snapshot if the data used by the
		// latest snapshot is 0B, indicating that no mutations have occurred
		// to the dataset since the latest snapshot.
		"SkipEmpty": false,
	},

	// SendFlags is a list of flags to pass in when invoking "zfs send".
	// The "-w" flag is useful for transferring data as stored on disk,
	// when trying to send encrypted and/or compressed datasets.
	"SendFlags": [], // E.g., ["-w"]

	// RecvFlags is a list of flags to pass in when invoking "zfs recv".
	// Resumable transfers are not enabled by default to support older versions;
	// pass the "-s" flag to enable use of this ZFS feature.
	"RecvFlags": [], // E.g., ["-s"]

	// Datasets is a list of datasets to replicate with "zfs send" and
	// "zfs recv". By default, there are no datasets specified.
	"Datasets": [{
		// The AutoSnapshot, SendFlags, and RecvFlags parameters
		// may also be specified on a per-dataset basis.

		// The Source represents the ZFS dataset to replicate from.
		// The value is a path URI of the form:
		//	//[userinfo@]host[:port]/pool/dataset
		// where the userinfo and port are optional.
		//
		// If the host is "localhost" or the same value as LocalhostAlias,
		// then this dataset is accessed using an OS subprocess.
		//
		// Otherwise, the userinfo, host, port, and SSH options from above are
		// used to access the dataset using a SSH subprocess.
		"Source": "", // E.g., "//localhost/tank/dataset"

		// Mirrors is a list of remote datasets to replicate the source to.
		// Each path in the mirror follows the same syntax as the source.
		"Mirrors": [], // E.g., ["//user@remotehost.local/tank/dataset-mirror"]
	}],
}`

type config struct {
	LogFile string `json:",omitempty"`

	SMTP smtpConfig
	HTTP httpConfig
	SSH  sshConfig

	// TODO: Add DryRun mode where prints any snapshot|destroy|send|recv
	// commands to be performed and then exists.

	// TODO: Add option to control I/O bandwidth globally and per-dataset.
	// Perhaps even allow controlling the bandwidth based on a cron schedule.

	ConcurrentTransfers int
	datasetOptions
	Datasets []datasetConfig
}

type smtpConfig struct {
	Host      string `json:",omitempty"`
	Port      int    `json:",omitempty"`
	Username  string `json:",omitempty"`
	Password  string `json:",omitempty"`
	ToAddress string `json:",omitempty"`
	ToName    string `json:",omitempty"`
}

type httpConfig struct {
	Address string `json:",omitempty"`
}

type sshConfig struct {
	KeyFiles       []string         `json:",omitempty"`
	KnownHostFiles []string         `json:",omitempty"`
	KeepAlive      *keepAliveConfig `json:",omitempty"`
	LocalhostAlias string           `json:",omitempty"`
}

type keepAliveConfig struct {
	Interval uint
	CountMax uint
}

type datasetConfig struct {
	datasetOptions
	Source  datasetPath
	Mirrors []datasetPath
}

type datasetPath struct{ *url.URL }

func (p datasetPath) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.String())
}
func (p *datasetPath) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	u, err := url.Parse(s)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(u, &url.URL{User: u.User, Host: u.Host, Path: u.Path, RawPath: u.RawPath}) {
		return errors.New("URL may only have user, host, and path components")
	}
	p.URL = u
	return nil
}

type datasetOptions struct {
	AutoSnapshot *snapshotOptions `json:",omitempty"`
	SendFlags    []string         `json:",omitempty"`
	RecvFlags    []string         `json:",omitempty"`
}

type snapshotOptions struct {
	Cron      string `json:",omitempty"`
	TimeZone  string `json:",omitempty"`
	Count     int    `json:",omitempty"`
	SkipEmpty bool   `json:",omitempty"`
}

func loadConfig(path string) (conf config, logger *log.Logger, closer func() error) {
	var logBuf bytes.Buffer
	logger = log.New(io.MultiWriter(os.Stderr, &logBuf), "", log.Ldate|log.Ltime|log.Lshortfile)

	var hash string
	if b, _ := ioutil.ReadFile(os.Args[0]); len(b) > 0 {
		hash = fmt.Sprintf("%x", sha256.Sum256(b))
	}

	// Load configuration file.
	c, err := ioutil.ReadFile(path)
	if err != nil {
		logger.Fatalf("unable to read config: %v", err)
	}
	if c, err = jsonfmt.Format(c, jsonfmt.Standardize()); err != nil {
		logger.Fatalf("unable to parse config: %v", err)
	}
	if err := json.Unmarshal(c, &conf); err != nil {
		logger.Fatalf("unable to decode config: %v", err)
	}

	// Set configuration defaults.
	if conf.SMTP.Port == 0 {
		conf.SMTP.Port = 587
	}
	if conf.SSH.KeepAlive == nil {
		conf.SSH.KeepAlive = &keepAliveConfig{Interval: 30, CountMax: 2}
	}
	if conf.ConcurrentTransfers <= 0 {
		conf.ConcurrentTransfers = 1
	}
	if conf.AutoSnapshot == nil {
		conf.AutoSnapshot = &snapshotOptions{Cron: "@daily", TimeZone: "Local"}
	}

	// Print the configuration.
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "\t")
	enc.Encode(struct {
		config
		BinaryVersion string `json:",omitempty"`
		BinarySHA256  string `json:",omitempty"`
	}{conf, version, hash})
	logger.Printf("loaded config:\n%s", b.String())

	// Setup the log output.
	if conf.LogFile == "" {
		logger.SetOutput(os.Stderr)
		closer = func() error { return nil }
	} else {
		f, err := os.OpenFile(conf.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0664)
		if err != nil {
			logger.Fatalf("error opening log file: %v", err)
		}
		f.Write(logBuf.Bytes()) // Write log output prior to this point
		logger.Printf("suppress stderr logging (redirected to %s)", f.Name())
		logger.SetOutput(f)
		closer = f.Close
	}

	return conf, logger, closer
}

func main() {
	if len(os.Args) != 2 || strings.HasPrefix(os.Args[1], "-") {
		fmt.Fprintf(os.Stderr, "Usage: %s [CONF_FILE]\n%s\n", os.Args[0], Help)
		os.Exit(1)
	}

	// Parse and use the configuration file.
	conf, logger, closer := loadConfig(os.Args[1])
	defer closer()
	zs := newZSyncer(conf, logger)

	// Register shutdown hook.
	go func() {
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
		logger.Printf("received %v - initiating shutdown", <-sigc)
		zs.Close()
	}()

	logger.Printf("%s starting", path.Base(os.Args[0]))
	defer logger.Printf("%s shutdown", path.Base(os.Args[0]))
	zs.Run()
}
