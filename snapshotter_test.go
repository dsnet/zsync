// Copyright 2019, The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.md file.

package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestFilterSnapshots(t *testing.T) {
	tests := []struct {
		label        string
		inSnapshots  []snapshots
		inCount      int
		wantDestroy  []snapshots
		wantPreserve []snapshots
	}{{
		label: "EmptyCountPreservesAll",
		inSnapshots: []snapshots{
			{"00", "01", "02"},
			{"00", "01", "02"},
		},
		wantDestroy: []snapshots{{}, {}},
		wantPreserve: []snapshots{
			{"00", "01", "02"},
			{"00", "01", "02"},
		},
	}, {
		label: "NoCommonPreservesAll",
		inSnapshots: []snapshots{
			{"00", "03"},
			{"01", "04"},
			{"02", "05"},
		},
		inCount:     1,
		wantDestroy: []snapshots{{}, {}, {}},
		wantPreserve: []snapshots{
			{"00", "03"},
			{"01", "04"},
			{"02", "05"},
		},
	}, {
		label: "DeleteOlder",
		inSnapshots: []snapshots{
			{"00", "01", "02", "03", "04"},
			{"00", "01", "02", "03", "04"},
			{"00", "01", "02", "03", "04"},
		},
		inCount: 2,
		wantDestroy: []snapshots{
			{"00", "01", "02"},
			{"00", "01", "02"},
			{"00", "01", "02"},
		},
		wantPreserve: []snapshots{
			{"03", "04"},
			{"03", "04"},
			{"03", "04"},
		},
	}, {
		label: "DeleteOlderPreserveCommon",
		inSnapshots: []snapshots{
			{"00", "01", "02", "03", "04"},
			{"00", "02", "03", "04"},
			{"00", "01", "02", "04"},
		},
		inCount: 2,
		wantDestroy: []snapshots{
			{"00", "01", "02"},
			{"00", "02"},
			{"00", "01"},
		},
		wantPreserve: []snapshots{
			{"03", "04"},
			{"03", "04"},
			{"02", "04"},
		},
	}, {
		label: "DeleteOlderPreserveCommon",
		inSnapshots: []snapshots{
			{"00", "01", "02", "03"},
			{"00", "01", "02", "03", "04"},
			{"00", "01", "02", "04"},
		},
		inCount: 2,
		wantDestroy: []snapshots{
			{"00", "01"},
			{"00", "01"},
			{"00", "01"},
		},
		wantPreserve: []snapshots{
			{"02", "03"},
			{"02", "03", "04"},
			{"02", "04"},
		},
	}}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			type result struct{ Destroy, Preserve []snapshots }
			gotDestroy, gotPreserve := filterSnapshots(tt.inSnapshots, tt.inCount)
			got := result{gotDestroy, gotPreserve}
			want := result{tt.wantDestroy, tt.wantPreserve}
			if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("filterSnapshots() mismatch (-want +got):\n%v", diff)
			}
		})
	}
}
