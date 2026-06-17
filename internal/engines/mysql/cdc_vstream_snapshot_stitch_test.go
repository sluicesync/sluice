// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"
)

// uuidA / uuidB (valid 32-hex MySQL GTID UUIDs) are declared in
// position_orderer_test.go and reused here.

// TestStitchSnapshotMin_SetMinPerShard is the ADR-0095 correctness core:
// the CDC-resume position stitched from N per-table snapshots is the
// per-shard GTID-set MINIMUM (intersection), never the maximum. Pinned
// across the GTID-set families — single-uuid unsharded, multi-uuid,
// multi-shard — and across shape variants (in-order, reverse-order,
// from-beginning sentinel present, the disjoint loud-refuse edge).
//
// Why "the class, not the representative" matters here (the Bug-74
// lesson applied to GTID-set selection): the wrong direction (max/union)
// is silent loss, and a representative single-table or single-shard case
// would not exercise the multi-shard per-shard selection where a
// mis-stitch is most likely. Each family is pinned with src-ground-truth
// GTID-set containment.
func TestStitchSnapshotMin_SetMinPerShard(t *testing.T) {
	tests := []struct {
		name    string
		perTbl  [][]shardGtid
		wantMin map[string]string // shardKey "ks/shard" -> expected gtid
		wantErr string            // substring; "" means no error
	}{
		{
			name: "single-uuid unsharded: min is the smallest range end",
			perTbl: [][]shardGtid{
				{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidA + ":1-100"}},
				{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidA + ":1-250"}},
				{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidA + ":1-180"}},
			},
			wantMin: map[string]string{"main/-": "MySQL56/" + uuidA + ":1-100"},
		},
		{
			name: "reverse arrival order still selects the subset",
			perTbl: [][]shardGtid{
				{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidA + ":1-900"}},
				{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidA + ":1-50"}},
			},
			wantMin: map[string]string{"main/-": "MySQL56/" + uuidA + ":1-50"},
		},
		{
			name: "multi-uuid: min is the set contained by all the others",
			perTbl: [][]shardGtid{
				// later table observed more of uuid B
				{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidA + ":1-10," + uuidB + ":1-5"}},
				{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidA + ":1-10," + uuidB + ":1-40"}},
			},
			wantMin: map[string]string{"main/-": "MySQL56/" + uuidA + ":1-10," + uuidB + ":1-5"},
		},
		{
			name: "multi-shard: each shard's min selected independently",
			perTbl: [][]shardGtid{
				{
					{Keyspace: "main", Shard: "-80", Gtid: "MySQL56/" + uuidA + ":1-100"},
					{Keyspace: "main", Shard: "80-", Gtid: "MySQL56/" + uuidB + ":1-300"},
				},
				{
					{Keyspace: "main", Shard: "-80", Gtid: "MySQL56/" + uuidA + ":1-200"},
					{Keyspace: "main", Shard: "80-", Gtid: "MySQL56/" + uuidB + ":1-150"},
				},
			},
			wantMin: map[string]string{
				"main/-80": "MySQL56/" + uuidA + ":1-100",
				"main/80-": "MySQL56/" + uuidB + ":1-150",
			},
		},
		{
			name: "from-beginning sentinel dominates as the minimum",
			perTbl: [][]shardGtid{
				{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidA + ":1-100"}},
				{{Keyspace: "main", Shard: "-", Gtid: ""}}, // a table with no observed VGTID
			},
			wantMin: map[string]string{"main/-": ""},
		},
		{
			name: "single per-table snapshot returns unchanged",
			perTbl: [][]shardGtid{
				{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidA + ":1-7"}},
			},
			wantMin: map[string]string{"main/-": "MySQL56/" + uuidA + ":1-7"},
		},
		{
			name: "disjoint sets refuse loudly (the impossible-on-one-session edge)",
			perTbl: [][]shardGtid{
				{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidA + ":1-10"}},
				{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidB + ":1-10"}},
			},
			wantErr: "disjoint",
		},
		{
			name:    "empty input refuses",
			perTbl:  nil,
			wantErr: "no per-table snapshot positions",
		},
		{
			name: "shard missing from one table's snapshot refuses",
			perTbl: [][]shardGtid{
				{
					{Keyspace: "main", Shard: "-80", Gtid: "MySQL56/" + uuidA + ":1-10"},
					{Keyspace: "main", Shard: "80-", Gtid: "MySQL56/" + uuidB + ":1-10"},
				},
				{
					{Keyspace: "main", Shard: "-80", Gtid: "MySQL56/" + uuidA + ":1-20"},
				},
			},
			wantErr: "same shard layout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stitchSnapshotMin(tt.perTbl)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("stitchSnapshotMin() = %+v, nil error; want error containing %q", got, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("stitchSnapshotMin() error = %q; want substring %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("stitchSnapshotMin() unexpected error: %v", err)
			}
			if len(got) != len(tt.wantMin) {
				t.Fatalf("stitchSnapshotMin() returned %d shards; want %d (%+v)", len(got), len(tt.wantMin), got)
			}
			for _, sg := range got {
				key := sg.Keyspace + "/" + sg.Shard
				want, ok := tt.wantMin[key]
				if !ok {
					t.Fatalf("stitchSnapshotMin() returned unexpected shard %q", key)
				}
				if sg.Gtid != want {
					t.Errorf("shard %q: stitched gtid = %q; want %q (the set-MIN, not max)", key, sg.Gtid, want)
				}
			}
		})
	}
}

// TestStitchSnapshotMin_NeverSelectsMax is the explicit silent-loss
// guard: the stitched position must be a SUBSET of every per-table
// snapshot (so CDC re-delivers (P_start, P_i] for each table and skips
// nothing), and must NOT equal the union/maximum (which would gap the
// lagging tables). Verified via the engine's own containment primitive.
func TestStitchSnapshotMin_NeverSelectsMax(t *testing.T) {
	perTbl := [][]shardGtid{
		{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidA + ":1-100"}},
		{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidA + ":1-500"}},
		{{Keyspace: "main", Shard: "-", Gtid: "MySQL56/" + uuidA + ":1-300"}},
	}
	got, err := stitchSnapshotMin(perTbl)
	if err != nil {
		t.Fatalf("stitchSnapshotMin: %v", err)
	}
	gotMin := got[0].Gtid

	// The stitched gotMin must be at-or-before EVERY per-table snapshot:
	// every P_i ⊇ P_start (gtidAtOrAfter(P_i, P_start) == true).
	for _, snap := range perTbl {
		pi := snap[0].Gtid
		after, err := gtidAtOrAfter(stripGTIDFlavor(pi), stripGTIDFlavor(gotMin))
		if err != nil {
			t.Fatalf("gtidAtOrAfter(%q,%q): %v", pi, gotMin, err)
		}
		if !after {
			t.Errorf("stitched gotMin %q is NOT a subset of per-table snapshot %q — CDC would gap that table (silent loss)", gotMin, pi)
		}
	}

	// And it must be the smallest (1-100), not the max (1-500).
	if gotMin != "MySQL56/"+uuidA+":1-100" {
		t.Errorf("stitched gotMin = %q; want MySQL56/"+uuidA+":1-100 (the minimum). Selecting the max is the silent-loss bug.", gotMin)
	}
}
