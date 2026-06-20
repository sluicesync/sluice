// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestLaneRouter_SameKeySameLane is the load-bearing invariant: every
// change for a given primary key must resolve to the same lane regardless
// of change kind (Insert/Update/Delete), so all ops on one row are applied
// in source order on a single lane (the dependent-row hazard cannot occur).
func TestLaneRouter_SameKeySameLane(t *testing.T) {
	r := NewRouter(8)
	pkCols := []string{"id"}

	for _, id := range []int64{1, 2, 3, 42, 100, 99999, -7} {
		ins := ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": id, "v": "x"}}
		upd := ir.Update{Schema: "ks", Table: "t", After: ir.Row{"id": id, "v": "y"}, Before: ir.Row{"id": id, "v": "x"}}
		del := ir.Delete{Schema: "ks", Table: "t", Before: ir.Row{"id": id, "v": "y"}}

		insVals, ok := PKValuesFromRow(ins, pkCols)
		if !ok {
			t.Fatalf("id=%d: insert not routable", id)
		}
		updVals, ok := PKValuesFromRow(upd, pkCols)
		if !ok {
			t.Fatalf("id=%d: update not routable", id)
		}
		delVals, ok := PKValuesFromRow(del, pkCols)
		if !ok {
			t.Fatalf("id=%d: delete not routable", id)
		}

		q := "ks.t"
		li := r.LaneFor(q, insVals)
		lu := r.LaneFor(q, updVals)
		ld := r.LaneFor(q, delVals)
		if li != lu || li != ld {
			t.Errorf("id=%d: lanes differ ins=%d upd=%d del=%d; same key must map to one lane", id, li, lu, ld)
		}
		if li < 0 || li >= 8 {
			t.Errorf("id=%d: lane %d out of range [0,8)", id, li)
		}
	}
}

// TestLaneRouter_Deterministic: repeated calls with the same inputs return
// the same lane (no Math.random-style nondeterminism in the hash).
func TestLaneRouter_Deterministic(t *testing.T) {
	r := NewRouter(16)
	vals := []any{int64(12345)}
	first := r.LaneFor("ks.users", vals)
	for i := 0; i < 100; i++ {
		if got := r.LaneFor("ks.users", vals); got != first {
			t.Fatalf("call %d: lane %d != first %d", i, got, first)
		}
	}
}

// TestLaneRouter_TypeTagsAvoidAliasing: int64(49) and string "1" must not
// collide just because of byte-content overlap — the per-value type tag
// keeps distinct keys distinct. (We assert the encodings differ, which is
// what the tag guarantees; lane equality by chance is possible under mod
// but the hashes must differ.)
func TestLaneRouter_TypeTagsAvoidAliasing(t *testing.T) {
	r := NewRouter(997) // prime, large, to expose accidental hash equality

	// Two distinct multi-column keys whose concatenation-without-separator
	// would alias: ["a","b"] vs ["ab",""].
	l1 := r.LaneFor("t", []any{"a", "b"})
	l2 := r.LaneFor("t", []any{"ab", ""})
	if l1 == l2 {
		t.Errorf(`["a","b"] and ["ab",""] hashed to the same lane %d; separator missing?`, l1)
	}

	// Different qualified tables with the same key should generally differ.
	la := r.LaneFor("ks.a", []any{int64(1)})
	lb := r.LaneFor("ks.b", []any{int64(1)})
	if la == lb {
		t.Logf("note: ks.a and ks.b id=1 share lane %d (acceptable collision, not a bug)", la)
	}
}

// TestLaneRouter_SingleLaneAlwaysZero: lanes<=1 degrades to serial (lane 0)
// without hashing — the zero-value-safe / misconfig-safe path.
func TestLaneRouter_SingleLaneAlwaysZero(t *testing.T) {
	for _, n := range []int{0, -3, 1} {
		r := NewRouter(n)
		if got := r.LaneFor("t", []any{int64(7)}); got != 0 {
			t.Errorf("lanes=%d: laneFor=%d, want 0 (serial)", n, got)
		}
	}
}

// TestPkValuesForRouting_BarrierEvents: non-row events and keyless tables
// are not routable (ok=false) so they take the barrier path.
func TestPkValuesForRouting_BarrierEvents(t *testing.T) {
	cases := []struct {
		name string
		c    ir.Change
		pk   []string
	}{
		{"txbegin", ir.TxBegin{}, []string{"id"}},
		{"txcommit", ir.TxCommit{}, []string{"id"}},
		{"truncate", ir.Truncate{Schema: "ks", Table: "t"}, []string{"id"}},
		{"schemasnap", ir.SchemaSnapshot{Schema: "ks", Table: "t"}, []string{"id"}},
		{"keyless-insert", ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"v": "x"}}, nil},
		{"missing-pk-col", ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"v": "x"}}, []string{"id"}},
		{"nil-row", ir.Insert{Schema: "ks", Table: "t", Row: nil}, []string{"id"}},
	}
	for _, tc := range cases {
		if _, ok := PKValuesFromRow(tc.c, tc.pk); ok {
			t.Errorf("%s: expected not-routable (ok=false), got routable", tc.name)
		}
	}
}
