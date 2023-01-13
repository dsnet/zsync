// Copyright 2017, The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.md file.

package main

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/dsnet/golib/unitconv"
)

// replicaManager is responsible for sending snapshots from one target
// and receiving them at another target.
type replicaManager struct {
	zs *zsyncer

	srcDataset  dataset
	dstDatasets []dataset

	sendFlags []string
	recvFlags []string

	signal chan struct{}
	timer  *time.Timer

	statusMu sync.Mutex
	statuses []replicationStatus // len(statuses) == len(dst)
}

type replicationStatus struct {
	Started     time.Time
	Finished    time.Time
	Transferred int64
	FaultReason string
}

func (zs *zsyncer) RegisterReplicaManager(src dataset, dsts []dataset, sendFlags, recvFlags []string) {
	rm := &replicaManager{
		zs: zs,

		srcDataset:  src,
		dstDatasets: dsts,

		sendFlags: sendFlags,
		recvFlags: recvFlags,

		signal: make(chan struct{}, 1),
		timer:  time.NewTimer(0),

		statuses: make([]replicationStatus, len(dsts)),
	}
	id := src.DatasetPath()
	if _, ok := zs.replicaManagers[id]; ok {
		zs.log.Fatalf("%s already registered", id)
	}
	zs.replicaManagers[id] = rm
}

func (rm *replicaManager) Status() []replicationStatus {
	rm.statusMu.Lock()
	defer rm.statusMu.Unlock()
	return append([]replicationStatus(nil), rm.statuses...)
}

func (rm *replicaManager) Run() {
	var retryDelay time.Duration
	for {
		select {
		case <-rm.signal:
		case <-rm.timer.C:
		case <-rm.zs.ctx.Done():
			return
		}

		var replicated, failed bool
		for i := range rm.dstDatasets {
			rm.replicate(i, &replicated, &failed)
		}
		if failed {
			retryDelay = timeoutAfter(retryDelay)
			rm.timer.Reset(retryDelay)
		} else {
			retryDelay = 0
			rm.timer.Stop()
		}

		// Signal the snapshot manager to delete synced snapshots.
		if replicated {
			trySignal(rm.zs.snapshotManagers[rm.srcDataset.DatasetPath()].signal)
		}
	}
}

func (rm *replicaManager) replicate(idx int, replicated, failed *bool) {
	// Acquire the semaphore to limit number of concurrent transfers.
	select {
	case rm.zs.replSema <- struct{}{}:
		defer func() { <-rm.zs.replSema }()
	case <-rm.zs.ctx.Done():
		return
	}

	src, dst := rm.srcDataset, rm.dstDatasets[idx]

	defer recoverError(func(err error) {
		if xerr, ok := err.(exitError); ok {
			subject := fmt.Sprintf("Replication failure from %q to %q", src.DatasetPath(), src.DatasetPath())
			if merr := sendEmail(rm.zs.smtp, subject, "<pre>"+xerr.Error()+"</pre>"); merr != nil {
				rm.zs.log.Printf("unable to send email: %v", merr)
			}
		}
		rm.zs.log.Printf("unexpected error: %v", err)
		*failed = true
	})

	// Open an executor for the source and destination dataset.
	srcExec, err := openExecutor(rm.zs.ctx, src.target)
	checkError(err)
	defer srcExec.Close()
	dstExec, err := openExecutor(rm.zs.ctx, dst.target)
	checkError(err)
	defer dstExec.Close()

	// Resume a partial receive if there is a token.
	s, err := dstExec.Exec("zfs", "get", "-H", "-o", "value", "receive_resume_token", dst.name)
	if tok := strings.TrimSpace(s); err == nil && len(tok) > 1 {
		rm.transfer(idx, transferArgs{
			Mode:     "partial",
			SrcLabel: src.DatasetPath(),
			DstLabel: dst.DatasetPath(),
			SendArgs: zsendArgs("-t", tok),
			RecvArgs: zrecvArgs(rm.recvFlags, dst.name),
			SrcExec:  srcExec, DstExec: dstExec,
		})
		*replicated = true
	}

	for {
		// Obtain a list of source and destination snapshots.
		srcSnapshots, err := listSnapshots(srcExec, src.name)
		checkError(err)
		if len(srcSnapshots) == 0 {
			return
		} else {
			src.latestSnapshot.Store(srcSnapshots[len(srcSnapshots)-1])
		}
		dstSnapshots, err := listSnapshots(dstExec, dst.name)
		if xerr, ok := err.(exitError); ok && strings.Contains(xerr.Stderr, "does not exist") {
			err = nil
		}
		checkError(err)

		// Clone first snapshot if destination has no snapshots.
		if len(dstSnapshots) == 0 {
			rm.transfer(idx, transferArgs{
				Mode:     "initial",
				SrcLabel: src.SnapshotPath(srcSnapshots[0]),
				DstLabel: dst.DatasetPath(),
				SendArgs: zsendArgs(rm.sendFlags, src.SnapshotName(srcSnapshots[0])),
				RecvArgs: zrecvArgs(rm.recvFlags, "-o", "mountpoint=none", "-o", "readonly=on", dst.name),
				SrcExec:  srcExec, DstExec: dstExec,
			})
			*replicated = true
			continue
		} else {
			dst.latestSnapshot.Store(dstSnapshots[len(dstSnapshots)-1])
		}

		// Use last destination snapshot for incremental send.
		ss := dstSnapshots[len(dstSnapshots)-1]
		i := findString(srcSnapshots, ss)
		if i < 0 {
			checkError(fmt.Errorf("snapshot %s does not exist", src.SnapshotPath(ss)))
		}
		if i+1 == len(srcSnapshots) {
			return
		}

		// TODO: If one of destination datasets is on localhost and
		// it is already up-to-date, then use that as the source rather than
		// the real source to avoid double bandwidth usage.

		// Perform incremental transfer for all snapshots.
		rm.transfer(idx, transferArgs{
			Mode:     "incremental",
			SrcLabel: src.SnapshotPath(srcSnapshots[i+1]),
			DstLabel: dst.SnapshotPath(srcSnapshots[i]),
			SendArgs: zsendArgs(rm.sendFlags, "-i",
				src.SnapshotName(srcSnapshots[i]), src.SnapshotName(srcSnapshots[i+1])),
			RecvArgs: zrecvArgs(rm.recvFlags, dst.name),
			SrcExec:  srcExec, DstExec: dstExec,
		})
		*replicated = true
	}
}

type transferArgs struct {
	Mode, SrcLabel, DstLabel string
	SendArgs, RecvArgs       []string
	SrcExec, DstExec         *executor
}

func (rm *replicaManager) transfer(idx int, args transferArgs) {
	// Update status for the latest transfer operation.
	rm.statusMu.Lock()
	rm.statuses[idx] = replicationStatus{Started: time.Now()}
	rm.statusMu.Unlock()
	defer func() {
		rm.statusMu.Lock()
		rm.statuses[idx].Finished = time.Now()
		rm.statusMu.Unlock()
	}()

	// Perform actual transfer.
	now := time.Now()
	rm.zs.log.Printf("transferring %s: %s -> %s", args.Mode, args.SrcLabel, args.DstLabel)
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()
	w2 := funcWriter(func(b []byte) (int, error) {
		n, err := w.Write(b)
		rm.statusMu.Lock()
		rm.statuses[idx].Transferred += int64(n) // TODO: use atomic updates
		rm.statusMu.Unlock()
		return n, err
	})
	errc := make(chan error, 2)
	go func() {
		err := args.SrcExec.ExecStream(nil, w2, args.SendArgs...)
		w.CloseWithError(err)
		errc <- err
	}()
	go func() {
		err := args.DstExec.ExecStream(r, nil, args.RecvArgs...)
		r.CloseWithError(err)
		errc <- err
	}()
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			rm.statusMu.Lock()
			rm.statuses[idx].FaultReason = err.Error()
			rm.statusMu.Unlock()
			checkError(err)
		}
	}

	rm.statusMu.Lock()
	n := rm.statuses[idx].Transferred
	rm.statusMu.Unlock()
	d := time.Now().Sub(now).Truncate(time.Second)
	rm.zs.log.Printf("transfer complete (copied %vB in %v) to destination: %s",
		unitconv.FormatPrefix(float64(n), unitconv.IEC, 1), d, args.DstLabel)
}

func zsendArgs(xs ...interface{}) []string {
	return flattenArgs(append([]interface{}{"zfs", "send"}, xs...)...)
}

func zrecvArgs(xs ...interface{}) []string {
	return flattenArgs(append([]interface{}{"zfs", "recv"}, xs...)...)
}

func flattenArgs(xs ...interface{}) (out []string) {
	for _, x := range xs {
		switch x := x.(type) {
		case string:
			out = append(out, x)
		case []string:
			out = append(out, x...)
		default:
			panic(fmt.Sprintf("unknown value: %#v", x))
		}
	}
	return out
}
