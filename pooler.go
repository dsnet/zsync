// Copyright 2017, The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.md file.

package main

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// poolMonitor polls the ZFS pools to ensure that they are healthy.
type poolMonitor struct {
	zs *zsyncer

	pool   string
	target execTarget

	signal chan struct{}
	timer  *time.Timer

	statusMu sync.Mutex
	status   poolStatus
}

type poolStatus struct {
	State int // -2: unhealthy, -1: maybe unhealthy, 0: unknown, +1: maybe healthy, +2 healthy
}

func (zs *zsyncer) RegisterPoolMonitors(src dataset, dsts []dataset) {
	if _, ok := zs.poolMonitors[src.PoolPath()]; !ok {
		zs.registerPoolMonitor(src)
	}
	for _, dst := range dsts {
		if _, ok := zs.poolMonitors[dst.PoolPath()]; !ok {
			zs.registerPoolMonitor(dst)
		}
	}
}
func (zs *zsyncer) registerPoolMonitor(ds dataset) {
	pool := ds.name
	if i := strings.IndexByte(pool, '/'); i >= 0 {
		pool = pool[:i]
	}
	pm := &poolMonitor{
		zs: zs,

		pool:   pool,
		target: ds.target,

		signal: make(chan struct{}, 1),
		timer:  time.NewTimer(0),
	}
	id := dataset{name: pool, target: ds.target}.PoolPath()
	if _, ok := zs.poolMonitors[id]; ok {
		zs.log.Fatalf("%s already registered", id)
	}
	zs.poolMonitors[id] = pm
}

func (pm *poolMonitor) Status() poolStatus {
	pm.statusMu.Lock()
	defer pm.statusMu.Unlock()
	return pm.status
}

func (pm *poolMonitor) Run() {
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

	var retryDelay time.Duration
	for {
		select {
		case <-pm.signal:
		case <-pm.timer.C:
		case <-pm.zs.ctx.Done():
			return
		}

		func() {
			defer recoverError(func(err error) {
				pm.statusMu.Lock()
				switch pm.status.State {
				case -2:
					pm.status.State = -1 // maybe unhealthy
				case +2:
					pm.status.State = +1 // maybe healthy
				}
				pm.statusMu.Unlock()

				id := dataset{name: pm.pool, target: pm.target}.PoolPath()
				pm.zs.log.Printf("pool %s: unexpected error: %v", id, err)
				retryDelay = timeoutAfter(retryDelay)
				pm.timer.Reset(retryDelay)
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
			pm.statusMu.Lock()
			if strings.Contains(strings.Split(out, "\n")[0], "is healthy") {
				if pm.status.State <= 0 {
					id := dataset{name: pm.pool, target: pm.target}.PoolPath()
					if pm.status.State < 0 {
						if err := sendEmail(pm.zs.smtp, fmt.Sprintf("Pool %q became healthy", id), ""); err != nil {
							pm.zs.log.Printf("unable to send email: %v", err)
						}
					}
					pm.zs.log.Printf("pool %q is healthy", id)
				}
				pm.status.State = +2
			} else {
				if pm.status.State >= 0 {
					id := dataset{name: pm.pool, target: pm.target}.PoolPath()
					if pm.status.State > 0 {
						if err := sendEmail(pm.zs.smtp, fmt.Sprintf("Pool %q became unhealthy", id), "<pre>"+out+"</pre>"); err != nil {
							pm.zs.log.Printf("unable to send email: %v", err)
						}
					}
					pm.zs.log.Printf("pool %q is unhealthy\n%s", id, indentLines(out))
				}
				pm.status.State = -2
			}
			pm.statusMu.Unlock()
			retryDelay = 0
			pm.timer.Reset(10 * time.Minute)
		}()
	}
}
