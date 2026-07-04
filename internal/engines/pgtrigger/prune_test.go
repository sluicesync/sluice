// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"testing"
	"time"
)

// TestAppliedLastID covers the token decode `sluice trigger prune` uses to derive
// the prune bound from the target's durable frontier. The Prune DELETE itself
// needs a real PG and lives in the pipeline integration suite.
func TestAppliedLastID(t *testing.T) {
	got, err := AppliedLastID(`{"last_id":99}`)
	if err != nil {
		t.Fatalf("AppliedLastID valid token: %v", err)
	}
	if got != 99 {
		t.Errorf("AppliedLastID = %d; want 99", got)
	}

	if _, err := AppliedLastID(""); err == nil {
		t.Error("AppliedLastID(empty) returned nil; want a loud error")
	}
	if _, err := AppliedLastID("{bad"); err == nil {
		t.Error("AppliedLastID(malformed) returned nil; want a loud error")
	}
	if _, err := AppliedLastID(`{"last_id":-5}`); err == nil {
		t.Error("AppliedLastID(negative) returned nil; want a loud error")
	}
	// A FOREIGN token that unmarshals cleanly (a vanilla-PG pgoutput {slot,lsn},
	// a broker envelope) must REFUSE — not silently decode to last_id=0.
	for _, foreign := range []string{
		`{"slot":"sluice_slot","lsn":"0/16B3748"}`,
		`{"chain_id":"c1","segment":3}`,
	} {
		if _, err := AppliedLastID(foreign); err == nil {
			t.Errorf("AppliedLastID(%q) returned nil; want a loud refuse (no last_id key)", foreign)
		}
	}
}

// --- P-1 batched-prune pins --------------------------------------------------
// These mirror the sqlite-trigger package's pins statement-for-statement (the
// two trigger engines deliberately keep the prune helpers structurally
// parallel until a shared core package is extracted). The real bounded-DELETE
// SQL needs a live PG and is pinned in prune_integration_test.go.

// recordedBatch is one (floor, upper] keyset step a fake pruneBatchFunc saw.
type recordedBatch struct{ floor, upper int64 }

// TestPruneInBatches_StepsBoundedKeyset pins the batching shape: a backlog is
// reaped as multiple bounded `floor < id <= upper` steps — never one monolithic
// statement — with every upper clamped at the cut (the invariant carrier) and
// the floor starting at MIN(id)-1 (not 0, so a high-id log doesn't step through
// millions of empty ranges).
func TestPruneInBatches_StepsBoundedKeyset(t *testing.T) {
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
			deleted, done, err := pruneInBatches(context.Background(), tc.minID, tc.cut, tc.step, 0, del)
			if err != nil {
				t.Fatalf("pruneInBatches: %v", err)
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

// TestPruneInBatches_NothingBelowCut pins the two no-work shapes: an empty
// change-log (minID=0) and a log whose lowest row is already above the cut.
func TestPruneInBatches_NothingBelowCut(t *testing.T) {
	del := func(context.Context, int64, int64) (int64, error) {
		t.Fatal("del must not be called when there is nothing below the cut")
		return 0, nil
	}
	for _, minID := range []int64{0, 6} {
		deleted, done, err := pruneInBatches(context.Background(), minID, 5, 10, 0, del)
		if err != nil || !done || deleted != 0 {
			t.Errorf("minID=%d: got (deleted=%d, done=%v, err=%v); want (0, true, nil)", minID, deleted, done, err)
		}
	}
}

// TestPruneInBatches_BudgetStopsEarly pins the time-budget early exit: when the
// budget is exhausted mid-backlog the loop stops after the current batch with
// done=false (no error — the next tick resumes). The fake del sleeps so the
// clock provably advances past the 1ns deadline after the first batch.
func TestPruneInBatches_BudgetStopsEarly(t *testing.T) {
	var batches int
	del := func(context.Context, int64, int64) (int64, error) {
		batches++
		time.Sleep(2 * time.Millisecond)
		return 10, nil
	}
	deleted, done, err := pruneInBatches(context.Background(), 1, 100, 10, time.Nanosecond, del)
	if err != nil {
		t.Fatalf("pruneInBatches: %v", err)
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

// TestPruneInBatches_CtxCanceled pins the between-batch ctx check: a shutdown
// never waits behind a multi-batch backlog.
func TestPruneInBatches_CtxCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	del := func(context.Context, int64, int64) (int64, error) {
		t.Fatal("del must not run under a canceled ctx")
		return 0, nil
	}
	if _, _, err := pruneInBatches(ctx, 1, 100, 10, 0, del); err == nil {
		t.Error("pruneInBatches under canceled ctx returned nil; want ctx error")
	}
}

// TestPruneBookkeeper_RecountCadenceAndArithmetic pins the P-1 estimate
// bookkeeping: recount on the first tick and every pruneRecountEvery-th after,
// rows-affected arithmetic (floored at 0) between, no effect until anchored.
func TestPruneBookkeeper_RecountCadenceAndArithmetic(t *testing.T) {
	var b pruneBookkeeper
	b.noteDeleted(99) // unanchored: must be a no-op, not a bogus negative
	if b.anchored || b.remaining != 0 {
		t.Fatalf("unanchored noteDeleted mutated the bookkeeper: %+v", b)
	}
	if !b.tick() {
		t.Error("tick 1 must recount (nothing anchored yet)")
	}
	b.anchor(100)
	for i := int64(2); i < pruneRecountEvery; i++ {
		if b.tick() {
			t.Errorf("tick %d recounted; want arithmetic-only between recounts", i)
		}
		b.noteDeleted(5)
	}
	if b.remaining != 100-5*(pruneRecountEvery-2) {
		t.Errorf("remaining = %d; want %d", b.remaining, 100-5*(pruneRecountEvery-2))
	}
	if !b.tick() {
		t.Errorf("tick %d must recount", pruneRecountEvery)
	}
	b.anchor(40)
	b.noteDeleted(1000)
	if b.remaining != 0 {
		t.Errorf("remaining = %d; want 0 (floored, never negative)", b.remaining)
	}
}
