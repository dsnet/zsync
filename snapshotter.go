// Copyright 2017, The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.md file.

package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/dsnet/golib/cron"
)

// snapshotManager is responsible for creating and deleting snapshots
// on all sources and their mirrors.
// The manager is careful not to delete snapshots unless a common snapshot can
// be found across the source and its mirrors.
type snapshotManager struct {
	zs *zsyncer

	srcDataset  dataset
	dstDatasets []dataset

	sched    cron.Schedule
	timeZone *time.Location
	count    int

	signal chan struct{}
	timer  *time.Timer
}

func (zs *zsyncer) RegisterSnapshotManager(src dataset, dsts []dataset, sched cron.Schedule, tz *time.Location, count int) {
	sm := &snapshotManager{
		zs: zs,

		srcDataset:  src,
		dstDatasets: dsts,

		sched:    sched,
		timeZone: tz,
		count:    count,

		signal: make(chan struct{}, 1),
		timer:  time.NewTimer(0),
	}
	id := src.DatasetPath()
	if _, ok := zs.snapshotManagers[id]; ok {
		zs.log.Fatalf("%s already registered", id)
	}
	zs.snapshotManagers[id] = sm
}

func (sm *snapshotManager) Run() {
	cronTimer := cron.NewCron(sm.sched, sm.timeZone)
	defer cronTimer.Stop()

	var makeSnapshot bool
	var retryDelay time.Duration
	for {
		select {
		case <-sm.signal:
		case <-sm.timer.C:
		case <-cronTimer.C:
			makeSnapshot = true
		case <-sm.zs.ctx.Done():
			return
		}

		func() {
			defer recoverError(func(err error) {
				sm.zs.log.Printf("unexpected error: %v", err)
				retryDelay = timeoutAfter(retryDelay)
				sm.timer.Reset(retryDelay)
			})

			// Open an executor for the source dataset.
			srcExec, err := openExecutor(sm.zs.ctx, sm.srcDataset.target)
			checkError(err)
			defer srcExec.Close()

			// Determine if we need to make a dataset.
			if !makeSnapshot {
				ss, err := listSnapshots(srcExec, sm.srcDataset.name)
				checkError(err)
				makeSnapshot = len(ss) == 0 // No snapshots, so make first one
			}
			if makeSnapshot {
				snapshot := time.Now().UTC().Format(time.RFC3339)
				sm.zs.log.Printf("creating snapshot: %s", sm.srcDataset.SnapshotName(snapshot))
				checkError(createSnapshot(srcExec, sm.srcDataset.name, snapshot))
				makeSnapshot = false

				// Signal the replica manager to mirror the snapshot.
				trySignal(sm.zs.replicaManagers[sm.srcDataset.DatasetPath()].signal)
			}

			// Delete old snapshots.
			if sm.count > 0 {
				// Open executors for all of the destinations.
				var dstExecs []*executor
				for _, dstDataset := range sm.dstDatasets {
					dstExec, err := openExecutor(sm.zs.ctx, dstDataset.target)
					checkError(err)
					defer dstExec.Close()
					dstExecs = append(dstExecs, dstExec)
				}

				// Retrieve all snapshots on the source and all destinations.
				srcSnapshots, err := listSnapshots(srcExec, sm.srcDataset.name)
				checkError(err)
				var dstSnapshots2D []snapshots
				for i := range sm.dstDatasets {
					ss, err := listSnapshots(dstExecs[i], sm.dstDatasets[i].name)
					checkError(err)
					dstSnapshots2D = append(dstSnapshots2D, ss)
				}

				// Destroy old snapshots, ensuring at least one common snapshot
				// exists between the source and all destination datasets.
				destroy2D, _ := filterSnapshots(append([]snapshots{srcSnapshots}, dstSnapshots2D...), sm.count)
				if ss := destroy2D[0]; len(ss) > 0 {
					sm.zs.log.Printf("destroying snapshots: %s", sm.srcDataset.SnapshotName(strings.Join(ss, ",")))
					checkError(destroySnapshots(srcExec, sm.srcDataset.name, ss))
				}
				for i := range sm.dstDatasets {
					if ss := destroy2D[i+1]; len(ss) > 0 {
						sm.zs.log.Printf("destroying snapshots: %s", sm.dstDatasets[i].SnapshotName(strings.Join(ss, ",")))
						checkError(destroySnapshots(dstExecs[i], sm.dstDatasets[i].name, ss))
					}
				}
			}

			retryDelay = 0
			sm.timer.Stop()
		}()
	}
}

// snapshots is a list of snapshots where the older snapshots come first.
// This does not contain the dataset name.
type snapshots []string

var dateRegex = regexp.MustCompile("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")

func listSnapshots(exec *executor, dataset string) (snapshots, error) {
	list, err := exec.Exec("zfs", "list", "-H", "-r", "-t", "snapshot", "-o", "name", dataset)
	if err != nil {
		return nil, err
	}

	var ss snapshots
	for _, s := range strings.Split(list, "\n") {
		if strings.HasPrefix(s, dataset+"@") {
			if s := s[len(dataset)+1:]; dateRegex.MatchString(s) {
				ss = append(ss, s)
			}
		}
	}
	return ss, nil
}

func createSnapshot(exec *executor, dataset, snapshot string) error {
	name := fmt.Sprintf("%s@%s", dataset, snapshot)
	_, err := exec.Exec("zfs", "snapshot", name)
	return err
}

func destroySnapshots(exec *executor, dataset string, ss snapshots) error {
	names := fmt.Sprintf("%s@%s", dataset, strings.Join(ss, ","))
	_, err := exec.Exec("zfs", "destroy", names)
	return err
}

func filterSnapshots(ss []snapshots, n int) (destroy, preserve []snapshots) {
	destroy = make([]snapshots, len(ss))
	preserve = append([]snapshots{}, ss...) // Preserve by default
	if len(ss) == 0 || n <= 0 {
		return destroy, preserve
	}

	// Find the latest snapshot common to all datasets.
	var common string
	srcSnapshots, dstSnapshots2D := ss[0], ss[1:]
	for i := len(srcSnapshots) - 1; i >= 0; i-- {
		var numDatasets int
		for _, dstSnapshots := range dstSnapshots2D {
			if findString(dstSnapshots, srcSnapshots[i]) >= 0 {
				numDatasets++
			}
		}
		if numDatasets == len(dstSnapshots2D) {
			common = srcSnapshots[i]
			break
		}
	}

	// Destroy everything before the common snapshot,
	// but ensure there is at least n snapshots preserved.
	for i := range ss {
		j := findString(ss[i], common)
		if len(ss[i])-j < n {
			j = len(ss[i]) - n
		}
		if j < 0 {
			j = 0 // Preserve everything
		}
		destroy[i], preserve[i] = ss[i][:j], ss[i][j:]
	}
	return destroy, preserve
}

func findString(ss []string, s string) int {
	for i, x := range ss {
		if x == s {
			return i
		}
	}
	return -1
}
