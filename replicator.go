// Copyright 2017, The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.md file.

package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dsnet/golib/iolimit"
	"github.com/dsnet/golib/unitconv"
)

// replicaManager is responsible for sending snapshots from one target
// and receiving them at another target.
type replicaManager struct {
	zs *zsyncer

	srcDataset  dataset
	dstDatasets []dataset

	lim       *iolimit.Limiter
	sendFlags []string
	recvFlags []string

	signal chan struct{}
	timer  *time.Timer
}

func (zs *zsyncer) RegisterReplicaManager(src dataset, dsts []dataset, rate float64, sendFlags, recvFlags []string) {
	rm := &replicaManager{
		zs: zs,

		srcDataset:  src,
		dstDatasets: dsts,

		lim:       zs.lim.SubLimiter(rate, 256<<10),
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
	for {
		select {
		case <-rm.signal:
		case <-rm.timer.C:
		case <-rm.zs.ctx.Done():
			return
		}

		var hadFailure bool
		for i := range rm.dstDatasets {
			rm.replicate(i, &hadFailure)
		}
		if hadFailure {
			rm.timer.Reset(30 * time.Second)
		} else {
			rm.timer.Stop()
		}

		// Signal the snapshot manager to delete synced snapshots.
		trySignal(rm.zs.snapshotManagers[rm.srcDataset.DatasetPath()].signal)
	}
}

func (rm *replicaManager) replicate(i int, hadFailure *bool) {
	// Acquire the semaphore to limit number of concurrent transfers.
	select {
	case rm.zs.replSema <- struct{}{}:
		defer func() { <-rm.zs.replSema }()
	case <-rm.zs.ctx.Done():
		return
	}

	defer recoverError(func(err error) {
		rm.zs.log.Printf("unexpected error: %v", err)
		*hadFailure = true
	})

	src, dst := rm.srcDataset, rm.dstDatasets[i]

	// Open an executor for the source and destination dataset.
	srcExec, err := openExecutor(rm.zs.ctx, src.target)
	checkError(err)
	defer srcExec.Close()
	dstExec, err := openExecutor(rm.zs.ctx, dst.target)
	checkError(err)
	defer dstExec.Close()

	// Retrieve list of source and destination snapshots.
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

	s, err := dstExec.Exec("zfs", "get", "-H", "receive_resume_token", dst.name)
	if toks := strings.Split(s, "\t"); err == nil && len(toks) > 2 && len(toks[2]) > 1 {
		// Resume a partial receive if there is a token.
		rm.transfer(transferArgs{
			Mode:     "partial",
			SrcLabel: src.DatasetPath(),
			DstLabel: dst.DatasetPath(),
			SendArgs: zsendArgs("-t", toks[2]),
			RecvArgs: zrecvArgs(rm.recvFlags, dst.name),
			SrcExec:  srcExec, DstExec: dstExec,
		})
	} else if len(dstSnapshots) == 0 {
		// Clone first snapshot if destination has no snapshots.
		rm.transfer(transferArgs{
			Mode:     "initial",
			SrcLabel: src.SnapshotPath(srcSnapshots[0]),
			DstLabel: dst.DatasetPath(),
			SendArgs: zsendArgs(rm.sendFlags, src.SnapshotName(srcSnapshots[0])),
			RecvArgs: zrecvArgs(rm.recvFlags, "-o", "mountpoint=none", "-o", "readonly=on", dst.name),
			SrcExec:  srcExec, DstExec: dstExec,
		})
	}

	for {
		// Use last destination snapshot for incremental send.
		srcSnapshots, err := listSnapshots(srcExec, src.name)
		checkError(err)
		dstSnapshots, err := listSnapshots(dstExec, dst.name)
		checkError(err)
		if len(dstSnapshots) == 0 {
			checkError(fmt.Errorf("destination has no snapshots: %s", src.DatasetPath()))
		}
		ss := dstSnapshots[len(dstSnapshots)-1]
		i := findString(srcSnapshots, ss)
		if i < 0 {
			checkError(fmt.Errorf("snapshot %s does not exist", src.SnapshotPath(ss)))
		}
		if i+1 == len(srcSnapshots) {
			return
		}

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
	}
}

type transferArgs struct {
	Mode, SrcLabel, DstLabel string
	SendArgs, RecvArgs       []string
	SrcExec, DstExec         *executor
}

func (rm *replicaManager) transfer(args transferArgs) {
	// Best-effort at send size estimation.
	s, _ := args.SrcExec.Exec(flattenArgs(args.SendArgs[:2], "-nP", args.SendArgs[2:])...)
	currSize, totalSize := parseDryOutput(s)
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		last := time.Now()
		lastCnt := rm.lim.Transferred()
		for run := true; run; {
			var now time.Time
			select {
			case now = <-t.C:
			case <-done:
				now = time.Now()
				run = false
			}

			currCnt := rm.lim.Transferred()
			currSize += currCnt - lastCnt
			rate := float64(currCnt-lastCnt) / now.Sub(last).Seconds()
			ratio := float64(currSize) / float64(totalSize)
			last = now
			lastCnt = currCnt

			if totalSize == 0 {
				fmt.Printf("% 7sB/s\n", unitconv.FormatPrefix(rate, unitconv.IEC, 1))
			} else {
				if ratio > 1 {
					ratio = 1
				}
				fmt.Printf("% 7sB/s %5.1f%%\n", unitconv.FormatPrefix(rate, unitconv.IEC, 1), 100*ratio)
			}
		}
	}()

	// Perform actual transfer.
	rm.zs.log.Printf("transferring %s: %s -> %s", args.Mode, args.SrcLabel, args.DstLabel)
	r, w := rm.lim.Pipe()
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
	rm.zs.log.Printf("transfer complete to destination: %s", args.DstLabel)
}

func parseDryOutput(s string) (currSize, totalSize int64) {
	ss := strings.Split(s, "\n")
	for _, s := range ss {
		if regexp.MustCompile(`^(full|incremental)\t[^\t]+\t([^\t]+\t)?[0-9]+$`).MatchString(s) {
			// This appears in all sends as either the first or last line:
			//	"full	tank/mirror2@2017-09-08T09:58:22Z	25185392"
			totalSize, _ = strconv.ParseInt(s[strings.LastIndexByte(s, '\t')+1:], 10, 64)
		}
		if regexp.MustCompile(`^\s+bytes = 0x[0-9a-f]+$`).MatchString(s) {
			// This appears in resumed sends only:
			//	"	bytes = 0xd9099c8"
			currSize, _ = strconv.ParseInt(s[strings.LastIndexByte(s, 'x')+1:], 16, 64)
		}
	}
	fmt.Println(s, currSize, totalSize)
	totalSize += currSize
	return currSize, totalSize
}

func zsendArgs(xs ...interface{}) []string {
	return flattenArgs(append([]interface{}{"zfs", "send"}, xs)...)
}

func zrecvArgs(xs ...interface{}) []string {
	return flattenArgs(append([]interface{}{"zfs", "recv"}, xs)...)
}

func flattenArgs(xs ...interface{}) (out []string) {
	for _, x := range xs {
		switch x := x.(type) {
		case string:
			out = append(out, x)
		case []string:
			out = append(out, x...)
		default:
			panic(fmt.Sprintf("unknown type: %T", x))
		}
	}
	return out
}
