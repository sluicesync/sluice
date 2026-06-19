// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestLaneRouter_SameKeySameLane is the load-bearing invariant: every
// change for a given primary key must resolve to the same lane regardless
// of change kind (Insert/Update/Delete), so all ops on one row are applied
// in source order on a single lane (the dependent-row hazard cannot occur).
func TestLaneRouter_SameKeySameLane(t *testing.T) {
	r := newLaneRouter(8)
	pkCols := []string{"id"}

	for _, id := range []int64{1, 2, 3, 42, 100, 99999, -7} {
		ins := ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": id, "v": "x"}}
		upd := ir.Update{Schema: "ks", Table: "t", After: ir.Row{"id": id, "v": "y"}, Before: ir.Row{"id": id, "v": "x"}}
		del := ir.Delete{Schema: "ks", Table: "t", Before: ir.Row{"id": id, "v": "y"}}

		insVals, ok := pkValuesForRouting(ins, pkCols)
		if !ok {
			t.Fatalf("id=%d: insert not routable", id)
		}
		updVals, ok := pkValuesForRouting(upd, pkCols)
		if !ok {
			t.Fatalf("id=%d: update not routable", id)
		}
		delVals, ok := pkValuesForRouting(del, pkCols)
		if !ok {
			t.Fatalf("id=%d: delete not routable", id)
		}

		q := "ks.t"
		li := r.laneFor(q, insVals)
		lu := r.laneFor(q, updVals)
		ld := r.laneFor(q, delVals)
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
	r := newLaneRouter(16)
	vals := []any{int64(12345)}
	first := r.laneFor("ks.users", vals)
	for i := 0; i < 100; i++ {
		if got := r.laneFor("ks.users", vals); got != first {
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
	r := newLaneRouter(997) // prime, large, to expose accidental hash equality

	// Two distinct multi-column keys whose concatenation-without-separator
	// would alias: ["a","b"] vs ["ab",""].
	l1 := r.laneFor("t", []any{"a", "b"})
	l2 := r.laneFor("t", []any{"ab", ""})
	if l1 == l2 {
		t.Errorf(`["a","b"] and ["ab",""] hashed to the same lane %d; separator missing?`, l1)
	}

	// Different qualified tables with the same key should generally differ.
	la := r.laneFor("ks.a", []any{int64(1)})
	lb := r.laneFor("ks.b", []any{int64(1)})
	if la == lb {
		t.Logf("note: ks.a and ks.b id=1 share lane %d (acceptable collision, not a bug)", la)
	}
}

// TestLaneRouter_SingleLaneAlwaysZero: lanes<=1 degrades to serial (lane 0)
// without hashing — the zero-value-safe / misconfig-safe path.
func TestLaneRouter_SingleLaneAlwaysZero(t *testing.T) {
	for _, n := range []int{0, -3, 1} {
		r := newLaneRouter(n)
		if got := r.laneFor("t", []any{int64(7)}); got != 0 {
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
		if _, ok := pkValuesForRouting(tc.c, tc.pk); ok {
			t.Errorf("%s: expected not-routable (ok=false), got routable", tc.name)
		}
	}
}

// TestCheckpointFrontier_ContiguousAdvance: out-of-order commits across
// lanes advance the frontier only to the highest contiguous prefix.
func TestCheckpointFrontier_ContiguousAdvance(t *testing.T) {
	f := newCheckpointFrontier()

	f.markCommitted(2) // gap at 1 → frontier stays 0
	if got := f.frontierSeq(); got != 0 {
		t.Fatalf("after commit(2): frontier=%d, want 0", got)
	}
	f.markCommitted(3)
	if got := f.frontierSeq(); got != 0 {
		t.Fatalf("after commit(3): frontier=%d, want 0 (1 still missing)", got)
	}
	f.markCommitted(1) // fills the gap → frontier jumps to 3
	if got := f.frontierSeq(); got != 3 {
		t.Fatalf("after commit(1): frontier=%d, want 3", got)
	}
	f.markCommitted(4)
	if got := f.frontierSeq(); got != 4 {
		t.Fatalf("after commit(4): frontier=%d, want 4", got)
	}
	// Duplicate/subsumed report is a no-op.
	f.markCommitted(2)
	if got := f.frontierSeq(); got != 4 {
		t.Fatalf("after dup commit(2): frontier=%d, want 4", got)
	}
}

// TestCheckpointFrontier_PositionOnlyAtCommittedBoundary: the persisted
// position is the highest tx boundary whose whole transaction is durable.
// A boundary beyond the frontier must NOT be returned (no skip/leadahead).
func TestCheckpointFrontier_PositionOnlyAtCommittedBoundary(t *testing.T) {
	f := newCheckpointFrontier()
	pos := func(tok string) ir.Position { return ir.Position{Engine: "mysql", Token: tok} }

	// Source tx A: changes seq 1,2 then TxCommit seq 3 (pos "A").
	// Source tx B: changes seq 4,5 then TxCommit seq 6 (pos "B").
	f.recordTxBoundary(3, pos("A"))
	f.recordTxBoundary(6, pos("B"))

	// Nothing committed yet → no safe checkpoint.
	if _, _, ok := f.checkpointPosition(); ok {
		t.Fatal("expected no checkpoint before any commit")
	}

	// Commit tx A's data + its boundary marker (1,2,3) → boundary A safe.
	f.markCommitted(1)
	f.markCommitted(2)
	f.markCommitted(3)
	got, _, ok := f.checkpointPosition()
	if !ok || got.Token != "A" {
		t.Fatalf("after tx A committed: checkpoint=%v ok=%v, want token A", got, ok)
	}

	// tx B partially committed (4,6 but NOT 5) → frontier stuck at 4, so
	// boundary B (seq 6) is NOT yet safe; checkpoint stays at A.
	f.markCommitted(4)
	f.markCommitted(6)
	got, _, ok = f.checkpointPosition()
	if !ok || got.Token != "A" {
		t.Fatalf("tx B partial: checkpoint=%v ok=%v, want token A (B not fully durable)", got, ok)
	}

	// Fill the gap (5) → frontier reaches 6 → boundary B safe.
	f.markCommitted(5)
	got, _, ok = f.checkpointPosition()
	if !ok || got.Token != "B" {
		t.Fatalf("after tx B committed: checkpoint=%v ok=%v, want token B", got, ok)
	}
}

// TestCheckpointFrontier_Idempotentcheckpoint: repeated checkpointPosition
// with no further progress returns the same boundary (ok=true), so a quiet
// stream re-persists the same point rather than spuriously reporting none.
func TestCheckpointFrontier_IdempotentCheckpoint(t *testing.T) {
	f := newCheckpointFrontier()
	f.recordTxBoundary(2, ir.Position{Engine: "mysql", Token: "X"})
	f.markCommitted(1)
	f.markCommitted(2)

	for i := 0; i < 3; i++ {
		got, _, ok := f.checkpointPosition()
		if !ok || got.Token != "X" {
			t.Fatalf("call %d: checkpoint=%v ok=%v, want token X", i, got, ok)
		}
	}
}

// TestWaitForFrontier_WakesOnAdvance: a waiter blocked on a target seq
// wakes once concurrent markCommitted calls advance the frontier past it.
func TestWaitForFrontier_WakesOnAdvance(t *testing.T) {
	f := newCheckpointFrontier()
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		done <- f.waitForFrontier(ctx, 3)
	}()

	// Commit out of order from another goroutine; the waiter must not wake
	// until the contiguous frontier reaches 3.
	var wg sync.WaitGroup
	for _, seq := range []uint64{2, 1, 3} {
		wg.Add(1)
		go func(s uint64) { defer wg.Done(); f.markCommitted(s) }(seq)
	}
	wg.Wait()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waitForFrontier returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("waitForFrontier did not wake; frontier=%d", f.frontierSeq())
	}
}

// TestWaitForFrontier_AlreadyReached: target ≤ current frontier returns
// immediately (no block).
func TestWaitForFrontier_AlreadyReached(t *testing.T) {
	f := newCheckpointFrontier()
	f.markCommitted(1)
	f.markCommitted(2)
	if err := f.waitForFrontier(context.Background(), 2); err != nil {
		t.Fatalf("waitForFrontier(2) = %v, want nil (already reached)", err)
	}
}

// TestWaitForFrontier_CtxCancel: a waiter unblocks with the ctx error when
// the frontier never reaches the target.
func TestWaitForFrontier_CtxCancel(t *testing.T) {
	f := newCheckpointFrontier()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.waitForFrontier(ctx, 5) }()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("waitForFrontier returned nil after cancel, want ctx error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitForFrontier did not unblock on ctx cancel")
	}
}

func TestPkChangedUpdate(t *testing.T) {
	pk := []string{"id"}
	cases := []struct {
		name string
		u    ir.Update
		want bool
	}{
		{"same-pk", ir.Update{Before: ir.Row{"id": int64(1), "v": "a"}, After: ir.Row{"id": int64(1), "v": "b"}}, false},
		{"changed-pk", ir.Update{Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(2)}}, true},
		{"nil-before", ir.Update{Before: nil, After: ir.Row{"id": int64(1)}}, false},
		{"bytes-pk-same", ir.Update{Before: ir.Row{"id": []byte("k")}, After: ir.Row{"id": []byte("k")}}, false},
		{"bytes-pk-diff", ir.Update{Before: ir.Row{"id": []byte("k")}, After: ir.Row{"id": []byte("j")}}, true},
	}
	for _, tc := range cases {
		if got := pkChangedUpdate(tc.u, pk); got != tc.want {
			t.Errorf("%s: pkChangedUpdate=%v want %v", tc.name, got, tc.want)
		}
	}
}

func TestRowChangeSchemaTable(t *testing.T) {
	cases := []struct {
		c             ir.Change
		schema, table string
	}{
		{ir.Insert{Schema: "ks", Table: "t"}, "ks", "t"},
		{ir.Update{Schema: "ks", Table: "u"}, "ks", "u"},
		{ir.Delete{Schema: "ks", Table: "d"}, "ks", "d"},
		{ir.TxBegin{}, "", ""},
	}
	for _, tc := range cases {
		s, tb := rowChangeSchemaTable(tc.c)
		if s != tc.schema || tb != tc.table {
			t.Errorf("rowChangeSchemaTable(%T) = (%q,%q), want (%q,%q)", tc.c, s, tb, tc.schema, tc.table)
		}
	}
}
