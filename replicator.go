// Copyright 2017, The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.md file.

package main

import (
	"fmt"
	"io"
	"strings"
	"time"
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
	}
	id := src.DatasetPath()
	if _, ok := zs.replicaManagers[id]; ok {
		zs.log.Fatalf("%s already registered", id)
	}
	zs.replicaManagers[id] = rm
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

func (rm *replicaManager) replicate(i int, replicated, failed *bool) {
	// Acquire the semaphore to limit number of concurrent transfers.
	select {
	case rm.zs.replSema <- struct{}{}:
		defer func() { <-rm.zs.replSema }()
	case <-rm.zs.ctx.Done():
		return
	}

	defer recoverError(func(err error) {
		rm.zs.log.Printf("unexpected error: %v", err)
		*failed = true
	})

	src, dst := rm.srcDataset, rm.dstDatasets[i]

	// Open an executor for the source and destination dataset.
	srcExec, err := openExecutor(rm.zs.ctx, src.target)
	checkError(err)
	defer srcExec.Close()
	dstExec, err := openExecutor(rm.zs.ctx, dst.target)
	checkError(err)
	defer dstExec.Close()

	// Resume a partial receive if there is a token.
	s, err := dstExec.Exec("zfs", "get", "-H", "receive_resume_token", dst.name)
	if toks := strings.Split(s, "\t"); err == nil && len(toks) > 2 && len(toks[2]) > 1 {
		rm.transfer(transferArgs{
			Mode:     "partial",
			SrcLabel: src.DatasetPath(),
			DstLabel: dst.DatasetPath(),
			SendArgs: zsendArgs("-t", toks[2]),
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
		}
		dstSnapshots, err := listSnapshots(dstExec, dst.name)
		if xerr, ok := err.(exitError); ok && strings.Contains(xerr.Stderr, "does not exist") {
			err = nil
		}
		checkError(err)

		// Clone first snapshot if destination has no snapshots.
		if len(dstSnapshots) == 0 {
			rm.transfer(transferArgs{
				Mode:     "initial",
				SrcLabel: src.SnapshotPath(srcSnapshots[0]),
				DstLabel: dst.DatasetPath(),
				SendArgs: zsendArgs(rm.sendFlags, src.SnapshotName(srcSnapshots[0])),
				RecvArgs: zrecvArgs(rm.recvFlags, "-o", "mountpoint=none", "-o", "readonly=on", dst.name),
				SrcExec:  srcExec, DstExec: dstExec,
			})
			*replicated = true
			continue
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
		rm.transfer(transferArgs{
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

func (rm *replicaManager) transfer(args transferArgs) {
	// Perform actual transfer.
	now := time.Now()
	rm.zs.log.Printf("transferring %s: %s -> %s", args.Mode, args.SrcLabel, args.DstLabel)
	r, w := io.Pipe()
	errc := make(chan error, 2)
	go func() {
		err := args.SrcExec.ExecStream(nil, w, args.SendArgs...)
		w.CloseWithError(err)
		errc <- err
	}()
	go func() {
		err := args.DstExec.ExecStream(r, nil, args.RecvArgs...)
		r.CloseWithError(err)
		errc <- err
	}()
	err1, err2 := <-errc, <-errc
	checkError(err1)
	checkError(err2)
	d := time.Now().Sub(now).Truncate(time.Second)
	rm.zs.log.Printf("transfer complete after %v to destination: %s", d, args.DstLabel)
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
