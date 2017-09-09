// Copyright 2017, The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.md file.

package main

import (
	"strings"
	"time"
)

// poolMonitor polls the ZFS pools to ensure that they are healthy.
type poolMonitor struct {
	zs *zsyncer

	pool   string
	target execTarget

	signal chan struct{}
	timer  *time.Timer
}

func (zs *zsyncer) RegisterPoolMonitor(pool string, target execTarget) {
	pm := &poolMonitor{
		zs: zs,

		pool:   pool,
		target: target,

		signal: make(chan struct{}, 1),
		timer:  time.NewTimer(0),
	}
	id := dataset{pool, target}.PoolPath()
	if _, ok := zs.poolMonitors[id]; ok {
		zs.log.Fatalf("%s already registered", id)
	}
	zs.poolMonitors[id] = pm
}

func (pm *poolMonitor) Run() {
	var state int // -1: offline, 0: unknown, +1: online

	// Cache the executor for efficiency purposes since the pool monitor
	// checks for the status relatively frequently.
	var exec *executor
	tryCloseExec := func() {
		if exec != nil {
			exec.Close()
			exec = nil
		}
	}
	defer tryCloseExec()

	for {
		select {
		case <-pm.signal:
		case <-pm.timer.C:
		case <-pm.zs.ctx.Done():
			return
		}

		func() {
			defer recoverError(func(err error) {
				pm.zs.log.Printf("unexpected error: %v", err)
				pm.timer.Reset(30 * time.Second)
				tryCloseExec()
			})

			// Query for the pool status.
			if exec == nil {
				var err error
				exec, err = openExecutor(pm.zs.ctx, pm.target)
				checkError(err)
			}
			out, err := exec.Exec("zpool", "status", "-P", "-x", pm.pool)
			checkError(err) // Unhealthy pools are not a exec error

			// Parse the pool status.
			if strings.Contains(strings.Split(out, "\n")[0], "is healthy") {
				if state <= 0 {
					state = +1
					pm.zs.log.Printf("pool %q is healthy", pm.pool)
				}
				pm.timer.Reset(5 * time.Minute)
			} else {
				if state >= 0 {
					state = -1
					pm.zs.log.Printf("pool %q is unhealthy\n%s", pm.pool, indentLines(out))
				}
				pm.timer.Reset(30 * time.Second)
			}
		}()
	}
}
