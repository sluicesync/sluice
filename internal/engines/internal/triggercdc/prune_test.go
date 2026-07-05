// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package triggercdc

import (
	"context"
	"testing"
	"time"
)

// These pins moved here verbatim from the pgtrigger + sqlite-trigger packages,
// where they were maintained statement-for-statement in parallel until this
// shared core was extracted. The engines keep only their transport-specific
// prune tests (the real DELETE SQL against a live/temp DB); the pure batching +
// bookkeeping logic is pinned once, here.

// recordedBatch is one (floor, upper] keyset step a fake BatchFunc saw.
type recordedBatch struct{ floor, upper int64 }

// TestInBatches_StepsBoundedKeyset pins the batching shape: a backlog is reaped
// as multiple bounded `floor < id <= upper` steps — never one monolithic
// statement — with every upper clamped at the cut (the invariant carrier) and
// the floor starting at MIN(id)-1 (not 0, so a high-id log doesn't step through
// millions of empty ranges).
func TestInBatches_StepsBoundedKeyset(t *testing.T) {
	cases := []struct {
		name        string
		minID, cut  int64
		step        int64
		wantBatches []recordedBatch
	}{
		{
			name: "low ids", minID: 1, cut: 25, step: 10,
			wantBatches: []recordedBatch{{0, 10}, {10, 20}, {20, 25}},
		},
		{
			name: "floor starts at MIN(id)-1", minID: 101, cut: 125, step: 10,
			wantBatches: []recordedBatch{{100, 110}, {110, 120}, {120, 125}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var batches []recordedBatch
			del := func(_ context.Context, floor, upper int64) (int64, error) {
				batches = append(batches, recordedBatch{floor, upper})
				return upper - floor, nil
			}
			deleted, done, err := InBatches(context.Background(), tc.minID, tc.cut, tc.step, 0, del)
			if err != nil {
				t.Fatalf("InBatches: %v", err)
			}
			if !done {
				t.Error("done = false; want true (no budget)")
			}
			if want := tc.cut - (tc.minID - 1); deleted != want {
				t.Errorf("deleted = %d; want %d", deleted, want)
			}
			if len(batches) != len(tc.wantBatches) {
				t.Fatalf("batches = %v; want %v", batches, tc.wantBatches)
			}
			for i, b := range batches {
				if b != tc.wantBatches[i] {
					t.Errorf("batch[%d] = %v; want %v", i, b, tc.wantBatches[i])
				}
				if b.upper > tc.cut {
					t.Errorf("batch[%d] upper %d exceeds cut %d — rows above the cut must NEVER be touched", i, b.upper, tc.cut)
				}
			}
		})
	}
}

// TestInBatches_NothingBelowCut pins the two no-work shapes: an empty change-log
// (minID=0) and a log whose lowest row is already above the cut.
func TestInBatches_NothingBelowCut(t *testing.T) {
	del := func(context.Context, int64, int64) (int64, error) {
		t.Fatal("del must not be called when there is nothing below the cut")
		return 0, nil
	}
	for _, minID := range []int64{0, 6} {
		deleted, done, err := InBatches(context.Background(), minID, 5, 10, 0, del)
		if err != nil || !done || deleted != 0 {
			t.Errorf("minID=%d: got (deleted=%d, done=%v, err=%v); want (0, true, nil)", minID, deleted, done, err)
		}
	}
}

// TestInBatches_BudgetStopsEarly pins the time-budget early exit: when the
// budget is exhausted mid-backlog the loop stops after the current batch with
// done=false (no error — the next tick resumes). The fake del sleeps so the
// clock provably advances past the 1ns deadline after the first batch.
func TestInBatches_BudgetStopsEarly(t *testing.T) {
	var batches int
	del := func(context.Context, int64, int64) (int64, error) {
		batches++
		time.Sleep(2 * time.Millisecond)
		return 10, nil
	}
	deleted, done, err := InBatches(context.Background(), 1, 100, 10, time.Nanosecond, del)
	if err != nil {
		t.Fatalf("InBatches: %v", err)
	}
	if done {
		t.Error("done = true; want false (budget exhausted with rows still below cut)")
	}
	if batches != 1 {
		t.Errorf("batches = %d; want 1 (stop after the batch that exhausted the budget)", batches)
	}
	if deleted != 10 {
		t.Errorf("deleted = %d; want 10", deleted)
	}
}

// TestInBatches_CtxCanceled pins the between-batch ctx check: a shutdown never
// waits behind a multi-batch backlog.
func TestInBatches_CtxCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	del := func(context.Context, int64, int64) (int64, error) {
		t.Fatal("del must not run under a canceled ctx")
		return 0, nil
	}
	if _, _, err := InBatches(ctx, 1, 100, 10, 0, del); err == nil {
		t.Error("InBatches under canceled ctx returned nil; want ctx error")
	}
}

// TestBookkeeper_RecountCadenceAndArithmetic pins the P-1 estimate bookkeeping:
// recount on the first tick and every RecountEvery-th after, rows-affected
// arithmetic (floored at 0) between, no effect until anchored.
func TestBookkeeper_RecountCadenceAndArithmetic(t *testing.T) {
	var b Bookkeeper
	b.NoteDeleted(99) // unanchored: must be a no-op, not a bogus negative
	if b.Anchored() || b.Remaining() != 0 {
		t.Fatalf("unanchored NoteDeleted mutated the bookkeeper: %+v", b)
	}
	if !b.Tick() {
		t.Error("tick 1 must recount (nothing anchored yet)")
	}
	b.Anchor(100)
	for i := int64(2); i < RecountEvery; i++ {
		if b.Tick() {
			t.Errorf("tick %d recounted; want arithmetic-only between recounts", i)
		}
		b.NoteDeleted(5)
	}
	if b.Remaining() != 100-5*(RecountEvery-2) {
		t.Errorf("remaining = %d; want %d", b.Remaining(), 100-5*(RecountEvery-2))
	}
	if !b.Tick() {
		t.Errorf("tick %d must recount", RecountEvery)
	}
	b.Anchor(40)
	b.NoteDeleted(1000)
	if b.Remaining() != 0 {
		t.Errorf("remaining = %d; want 0 (floored, never negative)", b.Remaining())
	}
}

// TestBookkeeper_PrimeRecount pins the test seam: PrimeRecount positions the
// counter so the very next Tick recounts (what the engine auto-prune tests use
// to force a recount tick deterministically).
func TestBookkeeper_PrimeRecount(t *testing.T) {
	var b Bookkeeper
	b.PrimeRecount()
	if !b.Tick() {
		t.Error("Tick after PrimeRecount did not recount; want a recount on the next tick")
	}
}
