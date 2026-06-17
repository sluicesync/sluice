// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"reflect"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
)

// TestVStreamCopyTableParallelismFromDSN pins the DSN-knob parse (ADR-0099):
// absent ⇒ default 1 (sequential), a valid integer passes through as the raw
// intent, and a malformed value is a LOUD error (not a silent fallback).
func TestVStreamCopyTableParallelismFromDSN(t *testing.T) {
	cases := []struct {
		name    string
		param   string
		want    int
		wantErr bool
	}{
		{name: "absent", param: "", want: defaultCopyTableParallelism},
		{name: "explicit one", param: "1", want: 1},
		{name: "four", param: "4", want: 4},
		{name: "zero", param: "0", want: 0},
		{name: "malformed loud", param: "lots", wantErr: true},
		{name: "malformed float loud", param: "2.5", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &gomysql.Config{Params: map[string]string{}}
			if tc.param != "" {
				cfg.Params["vstream_copy_table_parallelism"] = tc.param
			}
			got, err := vstreamCopyTableParallelismFromDSN(cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want a loud error for %q, got nil (val=%d)", tc.param, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// TestResolveCopyTableParallelism pins the zero-value-safe resolver
// (ADR-0099, the v0.99.51 trap): the Go zero value and every n <= 1 resolve
// to 1 (sequential — never "zero streams = copies nothing"); n > 1 clamps to
// min(n, nTables, ceiling).
func TestResolveCopyTableParallelism(t *testing.T) {
	cases := []struct {
		name    string
		n       int
		nTables int
		want    int
	}{
		{name: "zero value is sequential", n: 0, nTables: 10, want: 1},
		{name: "negative is sequential", n: -5, nTables: 10, want: 1},
		{name: "one is sequential", n: 1, nTables: 10, want: 1},
		{name: "four under table count", n: 4, nTables: 10, want: 4},
		{name: "clamped to table count", n: 8, nTables: 3, want: 3},
		{name: "clamped to ceiling", n: 1000, nTables: 1000, want: maxCopyTableParallelism},
		{name: "one table never concurrent", n: 8, nTables: 1, want: 1},
		{name: "zero tables guarded", n: 4, nTables: 0, want: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveCopyTableParallelism(tc.n, tc.nTables); got != tc.want {
				t.Fatalf("resolveCopyTableParallelism(%d,%d) = %d; want %d", tc.n, tc.nTables, got, tc.want)
			}
		})
	}
}

// TestPartitionTablesForStreams_Coverage is the load-bearing silent-loss pin
// (ADR-0099 §1): every in-scope table lands in EXACTLY ONE group — none
// dropped (a silently un-copied table), none duplicated (a table
// double-produced into one shared rowBuffer queue from two pumps). Checked
// across K ∈ {1, 2, 3, len, len+1}.
func TestPartitionTablesForStreams_Coverage(t *testing.T) {
	tables := []string{"users", "orders", "items", "audit", "blobs"}
	for _, k := range []int{1, 2, 3, len(tables), len(tables) + 1} {
		t.Run("k="+itoa(k), func(t *testing.T) {
			groups := partitionTablesForStreams(tables, k, nil)

			// K clamps to len(tables): never more groups than tables (no empty
			// group), never zero.
			wantGroups := k
			if wantGroups > len(tables) {
				wantGroups = len(tables)
			}
			if len(groups) != wantGroups {
				t.Fatalf("group count = %d; want %d (clamped to table count)", len(groups), wantGroups)
			}

			seen := make(map[string]int)
			for _, g := range groups {
				if len(g) == 0 {
					t.Errorf("empty group present (wasteful, K should clamp to table count)")
				}
				for _, tbl := range g {
					seen[tbl]++
				}
			}
			// Coverage: every input table seen exactly once.
			if len(seen) != len(tables) {
				t.Fatalf("distinct tables seen = %d; want %d (a table was dropped or duplicated)", len(seen), len(tables))
			}
			for _, tbl := range tables {
				if seen[tbl] != 1 {
					t.Errorf("table %q assigned to %d groups; want exactly 1", tbl, seen[tbl])
				}
			}
		})
	}
}

// TestPartitionTablesForStreams_Deterministic is the second load-bearing
// silent-loss pin (ADR-0099 §5): the same (tables, K) produces an IDENTICAL
// partition across calls, and a SHUFFLED input produces the SAME partition
// (the sort-before-assign invariant). This is the cold-start-vs-resume
// stability guarantee — on resume each stream must re-derive the same
// table→stream assignment it had on cold-start, or a table could land in a
// different stream than the one whose cursor names it.
func TestPartitionTablesForStreams_Deterministic(t *testing.T) {
	base := []string{"users", "orders", "items", "audit", "blobs", "events", "logs"}
	const k = 3

	want := partitionTablesForStreams(base, k, nil)

	// Identical across repeated calls.
	for i := 0; i < 5; i++ {
		got := partitionTablesForStreams(base, k, nil)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("call %d differs: got %v want %v", i, got, want)
		}
	}

	// Shuffle-invariant: any permutation of the input yields the same partition.
	shuffles := [][]string{
		{"blobs", "users", "logs", "items", "audit", "events", "orders"},
		{"logs", "events", "blobs", "audit", "items", "orders", "users"},
		{"orders", "items", "users", "blobs", "logs", "events", "audit"},
	}
	for i, sh := range shuffles {
		got := partitionTablesForStreams(sh, k, nil)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("shuffle %d differs from canonical: got %v want %v", i, got, want)
		}
	}
}

// TestPartitionTablesForStreams_SizeBalanced pins that a size estimator
// steers large tables apart (ADR-0099 §1): with one huge table and several
// small ones, the huge table is not piled into a group with all the others —
// the per-group estimated load is balanced. Still deterministic + covering.
func TestPartitionTablesForStreams_SizeBalanced(t *testing.T) {
	tables := []string{"big", "s1", "s2", "s3", "s4", "s5"}
	sizes := func(tbl string) (int64, bool) {
		if tbl == "big" {
			return 1_000_000, true
		}
		return 100, true
	}
	const k = 2
	groups := partitionTablesForStreams(tables, k, sizes)

	if len(groups) != k {
		t.Fatalf("group count = %d; want %d", len(groups), k)
	}
	// Coverage holds.
	seen := map[string]int{}
	for _, g := range groups {
		for _, tbl := range g {
			seen[tbl]++
		}
	}
	for _, tbl := range tables {
		if seen[tbl] != 1 {
			t.Errorf("table %q in %d groups; want 1", tbl, seen[tbl])
		}
	}
	// The "big" table's group should NOT also hold the majority of the small
	// tables — the greedy fills the other group with smalls to balance load.
	var bigGroup []string
	for _, g := range groups {
		for _, tbl := range g {
			if tbl == "big" {
				bigGroup = g
			}
		}
	}
	if len(bigGroup) > 2 {
		t.Errorf("big table's group has %d tables (%v); size-balancing should keep it lean", len(bigGroup), bigGroup)
	}

	// Deterministic with sizes too.
	again := partitionTablesForStreams(tables, k, sizes)
	if !reflect.DeepEqual(again, groups) {
		t.Fatalf("size-balanced partition not deterministic: %v vs %v", again, groups)
	}
}

// TestPartitionTablesForStreams_PartialEstimatesFallBack pins that a partial
// size estimator (ok=false for some table) falls back to deterministic
// round-robin rather than a non-deterministic mixed greedy.
func TestPartitionTablesForStreams_PartialEstimatesFallBack(t *testing.T) {
	tables := []string{"a", "b", "c", "d"}
	partial := func(tbl string) (int64, bool) {
		if tbl == "c" {
			return 0, false // no estimate for c → fall back
		}
		return 100, true
	}
	got := partitionTablesForStreams(tables, 2, partial)
	wantRoundRobin := partitionTablesForStreams(tables, 2, nil)
	if !reflect.DeepEqual(got, wantRoundRobin) {
		t.Fatalf("partial estimates did not fall back to round-robin: got %v want %v", got, wantRoundRobin)
	}
}

// TestPartitionTablesForStreams_Empty pins the empty-input edge.
func TestPartitionTablesForStreams_Empty(t *testing.T) {
	if g := partitionTablesForStreams(nil, 4, nil); g != nil {
		t.Fatalf("empty tables = %v; want nil", g)
	}
	if g := partitionTablesForStreams([]string{}, 4, nil); g != nil {
		t.Fatalf("empty tables = %v; want nil", g)
	}
}
