// Copyright 2017, The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.md file.

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ssh"
)

type executor struct {
	host string

	// Set only if executing remotely through crypto/ssh.
	client    *ssh.Client
	keepAlive KeepAliveConfig

	ctx    context.Context
	cancel context.CancelFunc
}

// execTarget represents a target for where execution occur.
// If the target host is "localhost" or isLocalhost is true,
// then subprocesses are run locally using the os/exec package.
// Otherwise, subprocesses are run remotely using the crypto/ssh package.
type execTarget struct {
	user string
	host string
	port string

	isLocalhost bool

	// These must be set for SSH authentication and tuning.
	auth      []ssh.AuthMethod
	hostKeys  ssh.HostKeyCallback
	keepAlive KeepAliveConfig
}

// openExecutor starts a new execution processor for the provided target.
// The executor must be closed to cleanup associated resources.
func openExecutor(ctx context.Context, t execTarget) (*executor, error) {
	ctx, cancel := context.WithCancel(ctx)
	x := &executor{ctx: ctx, cancel: cancel}
	if t.host == "localhost" || t.isLocalhost {
		return x, nil
	}

	cl, err := ssh.Dial("tcp", fmt.Sprintf("%s:%s", t.host, t.port), &ssh.ClientConfig{
		User:            t.user,
		Auth:            t.auth,
		HostKeyCallback: t.hostKeys,
		Timeout:         5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	x.host, x.client, x.keepAlive = t.host, cl, t.keepAlive
	go func() {
		<-ctx.Done()
		cl.Close()
	}()
	go x.keepAliveMonitor()
	return x, nil
}

// Exec executes a process and returns the stdout,
// blocking until the process is completed.
func (x *executor) Exec(args ...string) (string, error) {
	out := new(bytes.Buffer)
	err := x.ExecStream(nil, out, args...)
	return out.String(), err
}

// ExecStream executes a process using the provided in and out as the pipes
// for stdin and stdout. This blocks until the process completes.
func (x *executor) ExecStream(in io.Reader, out io.Writer, args ...string) error {
	eout := new(bytes.Buffer)
	if x.client != nil {
		ses, err := x.client.NewSession()
		if err != nil {
			return err
		}
		defer ses.Close()

		ses.Stdin = in
		ses.Stdout = out
		ses.Stderr = eout
		if err := ses.Run(strings.Join(args, " ")); err != nil {
			sshArgs := []string{"ssh", x.host}
			if _, ok := err.(*ssh.ExitError); ok {
				return exitError{err, append(sshArgs, args...), eout.String()}
			}
			return err
		}
	} else {
		cmd := exec.CommandContext(x.ctx, args[0], args[1:]...)
		cmd.Stdin = in
		cmd.Stdout = out
		cmd.Stderr = eout
		if err := cmd.Run(); err != nil {
			if _, ok := err.(*exec.ExitError); ok {
				return exitError{err, args, eout.String()}
			}
			return err
		}
	}
	return nil
}

// Close releases any resources associated with the executor..
// When called concurrently with Exec and ExecStream, it terminates the
// ongoing process.
func (x *executor) Close() error {
	x.cancel()
	return nil
}

// keepAliveMonitor periodically sends messages to invoke a response.
// If the server does not respond after some period of time,
// assume that the underlying net.Conn abruptly died.
func (x *executor) keepAliveMonitor() {
	if x.keepAlive.Interval == 0 || x.keepAlive.CountMax == 0 {
		return
	}

	// Repeatedly check if the remote server is still alive.
	var aliveCount atomic.Int32
	ticker := time.NewTicker(time.Duration(x.keepAlive.Interval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-x.ctx.Done():
			return
		case <-ticker.C:
			if n := aliveCount.Add(1); n > int32(x.keepAlive.CountMax) {
				x.client.Close() // Forcibly close the client
				return
			}
		}

		go func() {
			_, _, err := x.client.SendRequest("keepalive@openssh.com", true, nil)
			if err == nil {
				aliveCount.Store(0)
			}
		}()
	}
}

type exitError struct {
	error
	Args   []string
	Stderr string
}

func (e exitError) Error() string {
	return fmt.Sprintf("%v\n\t$ %v\n\n%s", e.error, strings.Join(e.Args, " "), indentLines(e.Stderr))
}
