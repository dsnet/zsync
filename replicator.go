// Copyright 2017, The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.md file.

package main

import (
	"bytes"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dsnet/golib/unitconv"
	"tailscale.com/syncs"
	"tailscale.com/tstime/rate"
)

// replicaManager is responsible for sending snapshots from one target
// and receiving them at another target.
type replicaManager struct {
	zs *zsyncer

	srcDataset  dataset
	dstDatasets []dataset

	sendFlags     []string
	recvFlags     []string
	initRecvFlags []string

	signal chan struct{}
	timer  *time.Timer

	statuses []replicationStatus // len(statuses) == len(dst)
}

type replicationStatus struct {
	atomicMu         sync.Mutex // held while mutating multiple fields together
	started          syncs.AtomicValue[time.Time]
	finished         syncs.AtomicValue[time.Time]
	transferByteRate rate.Value
	transferredBytes atomic.Int64
	estimatedBytes   atomic.Int64
	faultReason      syncs.AtomicValue[string]
}

func (zs *zsyncer) RegisterReplicaManager(src dataset, dsts []dataset, sendFlags, recvFlags, initRecvFlags []string) {
	rm := &replicaManager{
		zs: zs,

		srcDataset:  src,
		dstDatasets: dsts,

		sendFlags:     sendFlags,
		recvFlags:     recvFlags,
		initRecvFlags: initRecvFlags,

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

func (rm *replicaManager) Run() {
	var retryDelay time.Duration
	var attempts int
	for {
		select {
		case <-rm.signal:
		case <-rm.timer.C:
		case <-rm.zs.ctx.Done():
			return
		}

		attempts++
		var replicated, failed bool
		for i := range rm.dstDatasets {
			rm.replicate(i, attempts, &replicated, &failed)
		}
		if failed {
			retryDelay = timeoutAfter(retryDelay)
			rm.timer.Reset(retryDelay)
		} else {
			retryDelay = 0
			attempts = 0
			rm.timer.Stop()
		}

		// Signal the snapshot manager to delete synced snapshots.
		if replicated {
			trySignal(rm.zs.snapshotManagers[rm.srcDataset.DatasetPath()].signal)
		}
	}
}

func (rm *replicaManager) replicate(idx, attempts int, replicated, failed *bool) {
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
		rm.zs.log.Printf("dataset %s: replication error (attempt %d): %v", dst.DatasetPath(), attempts, err)
		*failed = true
	})

	// Open an executor for the source and destination dataset.
	srcExec := mustGet(openExecutor(rm.zs.ctx, src.target))
	defer srcExec.Close()
	dstExec := mustGet(openExecutor(rm.zs.ctx, dst.target))
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
		srcSnapshots := mustGet(listSnapshots(srcExec, src.name))
		if len(srcSnapshots) == 0 {
			return
		} else {
			src.latestSnapshot.Store(srcSnapshots[len(srcSnapshots)-1])
		}
		dstSnapshots, err := listSnapshots(dstExec, dst.name)
		if xerr, ok := err.(exitError); ok && strings.Contains(xerr.Stderr, "does not exist") {
			err = nil
		}
		mustDo(err)

		// Clone first snapshot if destination has no snapshots.
		if len(dstSnapshots) == 0 {
			rm.transfer(idx, transferArgs{
				Mode:     "initial",
				SrcLabel: src.SnapshotPath(srcSnapshots[0]),
				DstLabel: dst.DatasetPath(),
				SendArgs: zsendArgs(rm.sendFlags, src.SnapshotName(srcSnapshots[0])),
				RecvArgs: zrecvArgs(rm.initRecvFlags, dst.name),
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
			mustDo(fmt.Errorf("snapshot %s does not exist", src.SnapshotPath(ss)))
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
	status := &rm.statuses[idx]
	status.atomicMu.Lock()
	status.started.Store(time.Now())
	status.finished.Store(time.Time{})
	status.transferByteRate.UnmarshalJSON([]byte("{}"))
	status.transferredBytes.Store(0)
	status.estimatedBytes.Store(0)
	status.faultReason.Store("")
	status.atomicMu.Unlock()
	defer func() { status.finished.Store(time.Now()) }()

	// Best-effort estimate at total size.
	var sizeBuf bytes.Buffer
	drySendArgs := slices.Insert(slices.Clone(args.SendArgs), 2, "-nP")
	args.SrcExec.ExecStream(nil, &sizeBuf, drySendArgs...)
	for _, line := range strings.Split(sizeBuf.String(), "\n") {
		name, value, ok := strings.Cut(strings.TrimSpace(line), "\t")
		if ok && name == "size" {
			if size, err := strconv.ParseUint(value, 10, 64); err == nil {
				status.estimatedBytes.Store(int64(size))
			}
		}
	}

	// Perform actual transfer.
	now := time.Now()
	rm.zs.log.Printf("transferring %s: %s -> %s", args.Mode, args.SrcLabel, args.DstLabel)
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()
	w2 := funcWriter(func(b []byte) (int, error) {
		n, err := w.Write(b)
		status.transferredBytes.Add(int64(n))
		status.transferByteRate.Add(float64(n))
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
	for range 2 {
		if err := <-errc; err != nil {
			status.faultReason.Store(err.Error())
			mustDo(err)
		}
	}

	n := status.transferredBytes.Load()
	d := time.Now().Sub(now).Truncate(time.Second)
	rm.zs.log.Printf("transfer complete (copied %vB in %v) to destination: %s",
		unitconv.FormatPrefix(float64(n), unitconv.IEC, 1), d, args.DstLabel)
}

func zsendArgs(xs ...any) []string {
	return flattenArgs(append([]any{"zfs", "send"}, xs...)...)
}

func zrecvArgs(xs ...any) []string {
	return flattenArgs(append([]any{"zfs", "recv"}, xs...)...)
}

func flattenArgs(xs ...any) (out []string) {
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
