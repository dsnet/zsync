// Copyright 2017, The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.md file.

package main

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"

	"github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
	"github.com/tailscale/hujson"
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
	sudo zfs allow $USER send,snapshot,destroy,mount $DATASET

	# On all mirrors:
	sudo zfs allow $USER receive,create,mount,mountpoint,readonly,snapshot,destroy,mount $DATASET


The operation of zsync is configured using a JSON configuration file.
The JSON format used permits the use of comments and takes the following form:
{
	// The default value of each field is shown, unless otherwise specified.

	// Log configures how log lines are produced by zsync.
	"Log": {
		// File is where the daemon will direct its output log.
		// If the path is empty, then the log outputs to os.Stderr.
		"File": "",

		// ExcludeTimestamp specifies that a timestamp should not be logged.
		// This is useful if another mechanism (e.g., systemd) records timestamps.
		"ExcludeTimestamp": false,
	},

	// SMTP configures a mail client to email about potential issues.
	// Emails are sent when pools switch health states,
	// when snapshots cannot be created or deleted, and
	// when replication fails. Errors due to network failures are ignored.
	// If unspecified, then emails are not sent.
	"SMTP": {
		"Host":      "", // e.g., "mail.name.com"
		"Port":      587,
		"Username":  "", // e.g., "zsync-daemon@example.com"
		"Password":  "",
		"ToAddress": "", // e.g., "user@example.com"
		"ToName":    "", // e.g., "FirstName LastName"
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
		"KeyFiles": null, // e.g., ["key.priv"]

		// KnownHostFiles is a list of key database files for host public keys
		// in the OpenSSH known_hosts file format.
		//
		// Host-key checking is disabled if the empty list is specified.
		"KnownHostFiles": null, // e.g., ["known_hosts"]

		// KeepAlive sets the keep alive settings for each SSH connection.
		// It is recommended that these values match the AliveInterval and
		// AliveCountMax parameters on the remote OpenSSH server.
		"KeepAlive": {
			"Interval": 30, "CountMax": 2,
		},

		// LocalhostAliases considers hostnames provided to be equivalent
		// to "localhost". This allows a vanity hostname to be used, yet
		// allowing the use of OS subprocesses instead of SSH subprocesses.
		// The host portion of HTTP.Address is automatically inserted here.
		"LocalhostAliases": null, // e.g., ["myhostname.local"]
	},

	// ConcurrentTransfers specifies the maximum number of concurrent transfers
	// that may occur. Even if a source has multiple mirrors, at most one
	// transfer occurs at a time for each source.
	"ConcurrentTransfers": 1,

	// AutoSnapshot specifies when snapshots are taken and how many to keep.
	"AutoSnapshot": {
		// Cron uses the standard cron syntax to specify when snapshots trigger.
		"Cron": "@daily", // e.g., "0 0 * * * *"

		// TimeZone is the time zone to run the cron schedule in.
		"TimeZone": "Local", // e.g., "UTC" or "America/Los_Angeles"

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
	"SendFlags": [], // e.g., ["-w"]

	// RecvFlags is a list of flags to pass in when invoking "zfs recv".
	// Resumable transfers are not enabled by default to support older versions;
	// pass the "-s" flag to enable use of this ZFS feature.
	"RecvFlags": [], // e.g., ["-s"]

	// InitRecvFlags is a list of flags to pass in when invoking "zfs recv"
	// the first time if the destination dataset does not already exist.
	"InitRecvFlags": [], // e.g., ["-s", "-o", "mountpoint=none", "-o", "readonly=on"]

	// Datasets is a list of datasets to replicate with "zfs send" and
	// "zfs recv". By default, there are no datasets specified.
	"Datasets": [{
		// The AutoSnapshot, SendFlags, RecvFlags, and InitRecvFlags parameters
		// may also be specified on a per-dataset basis.

		// The Source represents the ZFS dataset to replicate from.
		// The value is a path URI of the form:
		//	//[userinfo@]host[:port]/pool/dataset
		// where the userinfo and port are optional.
		//
		// If the host is "localhost" or the same value as LocalhostAliases
		// and no port is specified, then this dataset is accessed directly
		// using an OS subprocess.
		//
		// Otherwise, the userinfo, host, port, and SSH options from above are
		// used to access the dataset using a SSH subprocess.
		"Source": "", // e.g., "//localhost/tank/dataset"

		// Mirrors is a list of remote datasets to replicate the source to.
		// Each path in the mirror follows the same syntax as the source.
		"Mirrors": [], // e.g., ["//user@remotehost.local/tank/dataset-mirror"]
	}],
}`

type Config struct {
	Log  LogConfig
	SMTP SMTPConfig
	HTTP HTTPConfig
	SSH  SSHConfig

	// TODO: Add DryRun mode where prints any snapshot|destroy|send|recv
	// commands to be performed and then exits.

	// TODO: Add option to control I/O bandwidth globally and per-dataset.
	// Perhaps even allow controlling the bandwidth based on a cron schedule.

	ConcurrentTransfers int
	DatasetOptions
	Datasets []DatasetConfig
}

type LogConfig struct {
	File             string `json:",omitzero"`
	ExcludeTimestamp bool   `json:",omitzero"`
}

type SMTPConfig struct {
	Host      string `json:",omitzero"`
	Port      int    `json:",omitzero"`
	Username  string `json:",omitzero"`
	Password  string `json:",omitzero"`
	ToAddress string `json:",omitzero"`
	ToName    string `json:",omitzero"`
}

type HTTPConfig struct {
	Address string `json:",omitzero"`
}

type SSHConfig struct {
	KeyFiles         []string         `json:",omitzero"`
	KnownHostFiles   []string         `json:",omitzero"`
	KeepAlive        *KeepAliveConfig `json:",omitzero"`
	LocalhostAliases []string         `json:",omitzero"`
}

type KeepAliveConfig struct {
	Interval uint
	CountMax uint
}

type DatasetConfig struct {
	DatasetOptions
	Source  DatasetPath
	Mirrors []DatasetPath
}

type DatasetPath struct{ *url.URL }

func (p DatasetPath) MarshalJSON() ([]byte, error) {
	if p.URL == nil {
		return []byte("null"), nil
	}
	return json.Marshal(p.String())
}
func (p *DatasetPath) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	u, err := url.Parse(s)
	if err != nil {
		return err
	}
	if *u != (url.URL{User: u.User, Host: u.Host, Path: u.Path, RawPath: u.RawPath}) {
		return errors.New("URL may only have user, host, and path components")
	}
	p.URL = u
	return nil
}

type DatasetOptions struct {
	AutoSnapshot  *SnapshotOptions `json:",omitzero"`
	SendFlags     []string         `json:",omitzero"`
	RecvFlags     []string         `json:",omitzero"`
	InitRecvFlags []string         `json:",omitzero"`
}

type SnapshotOptions struct {
	Cron      string `json:",omitzero"`
	TimeZone  string `json:",omitzero"`
	Count     int    `json:",omitzero"`
	SkipEmpty bool   `json:",omitzero"`
}

func loadConfig(path string) (conf Config, logger *log.Logger, closer func() error) {
	var logBuf bytes.Buffer
	logger = log.New(io.MultiWriter(os.Stderr, &logBuf), "", log.Ldate|log.Ltime|log.Lshortfile)

	var hash string
	if b, _ := os.ReadFile(os.Args[0]); len(b) > 0 {
		hash = fmt.Sprintf("%x", sha256.Sum256(b))
	}

	// Load configuration file.
	c, err := os.ReadFile(path)
	if err != nil {
		logger.Fatalf("unable to read config: %v", err)
	}
	if c, err = hujson.Standardize(c); err != nil {
		logger.Fatalf("unable to parse config: %v", err)
	}
	if err := json.Unmarshal(c, &conf, json.RejectUnknownMembers(true)); err != nil {
		logger.Fatalf("unable to decode config: %v", err)
	}
	if conf.Log.ExcludeTimestamp {
		logger.SetFlags(logger.Flags() &^ (log.Ldate | log.Ltime | log.Lmicroseconds))
	}

	// Set configuration defaults.
	conf.SMTP.Port = cmp.Or(conf.SMTP.Port, 587)
	conf.SSH.KeepAlive = cmp.Or(conf.SSH.KeepAlive, &KeepAliveConfig{Interval: 30, CountMax: 2})
	conf.ConcurrentTransfers = cmp.Or(max(0, conf.ConcurrentTransfers), 1)
	conf.AutoSnapshot = cmp.Or(conf.AutoSnapshot, &SnapshotOptions{})
	conf.AutoSnapshot.Cron = cmp.Or(conf.AutoSnapshot.Cron, "@daily")
	conf.AutoSnapshot.TimeZone = cmp.Or(conf.AutoSnapshot.TimeZone, "Local")
	for _, ds := range conf.Datasets {
		if ds.AutoSnapshot != nil {
			ds.AutoSnapshot.Cron = cmp.Or(ds.AutoSnapshot.Cron, "@daily")
			ds.AutoSnapshot.TimeZone = cmp.Or(ds.AutoSnapshot.TimeZone, "Local")
		}
	}

	// Print the configuration.
	b, err := json.Marshal(struct {
		Config
		BinaryVersion string `json:",omitzero"`
		BinarySHA256  string `json:",omitzero"`
	}{conf, version, hash}, jsontext.WithIndent("\t"))
	if err != nil {
		logger.Fatalf("unable to encode config: %v", err)
	}
	logger.Printf("loaded config:\n%s", string(b))

	// Setup the log output.
	if conf.Log.File == "" {
		logger.SetOutput(os.Stderr)
		closer = func() error { return nil }
	} else {
		f, err := os.OpenFile(conf.Log.File, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0664)
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
