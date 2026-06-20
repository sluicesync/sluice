// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"context"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCheckpointFrontier_ContiguousAdvance: out-of-order commits across
// lanes advance the frontier only to the highest contiguous prefix.
func TestCheckpointFrontier_ContiguousAdvance(t *testing.T) {
	f := NewFrontier()

	f.MarkCommitted(2) // gap at 1 → frontier stays 0
	if got := f.FrontierSeq(); got != 0 {
		t.Fatalf("after commit(2): frontier=%d, want 0", got)
	}
	f.MarkCommitted(3)
	if got := f.FrontierSeq(); got != 0 {
		t.Fatalf("after commit(3): frontier=%d, want 0 (1 still missing)", got)
	}
	f.MarkCommitted(1) // fills the gap → frontier jumps to 3
	if got := f.FrontierSeq(); got != 3 {
		t.Fatalf("after commit(1): frontier=%d, want 3", got)
	}
	f.MarkCommitted(4)
	if got := f.FrontierSeq(); got != 4 {
		t.Fatalf("after commit(4): frontier=%d, want 4", got)
	}
	// Duplicate/subsumed report is a no-op.
	f.MarkCommitted(2)
	if got := f.FrontierSeq(); got != 4 {
		t.Fatalf("after dup commit(2): frontier=%d, want 4", got)
	}
}

// TestCheckpointFrontier_PositionOnlyAtCommittedBoundary: the persisted
// position is the highest tx boundary whose whole transaction is durable.
// A boundary beyond the frontier must NOT be returned (no skip/leadahead).
func TestCheckpointFrontier_PositionOnlyAtCommittedBoundary(t *testing.T) {
	f := NewFrontier()
	pos := func(tok string) ir.Position { return ir.Position{Engine: "mysql", Token: tok} }

	// Source tx A: changes seq 1,2 then TxCommit seq 3 (pos "A").
	// Source tx B: changes seq 4,5 then TxCommit seq 6 (pos "B").
	f.RecordTxBoundary(3, pos("A"))
	f.RecordTxBoundary(6, pos("B"))

	// Nothing committed yet → no safe checkpoint.
	if _, _, ok := f.CheckpointPosition(); ok {
		t.Fatal("expected no checkpoint before any commit")
	}

	// Commit tx A's data + its boundary marker (1,2,3) → boundary A safe.
	f.MarkCommitted(1)
	f.MarkCommitted(2)
	f.MarkCommitted(3)
	got, _, ok := f.CheckpointPosition()
	if !ok || got.Token != "A" {
		t.Fatalf("after tx A committed: checkpoint=%v ok=%v, want token A", got, ok)
	}

	// tx B partially committed (4,6 but NOT 5) → frontier stuck at 4, so
	// boundary B (seq 6) is NOT yet safe; checkpoint stays at A.
	f.MarkCommitted(4)
	f.MarkCommitted(6)
	got, _, ok = f.CheckpointPosition()
	if !ok || got.Token != "A" {
		t.Fatalf("tx B partial: checkpoint=%v ok=%v, want token A (B not fully durable)", got, ok)
	}

	// Fill the gap (5) → frontier reaches 6 → boundary B safe.
	f.MarkCommitted(5)
	got, _, ok = f.CheckpointPosition()
	if !ok || got.Token != "B" {
		t.Fatalf("after tx B committed: checkpoint=%v ok=%v, want token B", got, ok)
	}
}

// TestCheckpointFrontier_IdempotentCheckpoint: repeated CheckpointPosition
// with no further progress returns the same boundary (ok=true), so a quiet
// stream re-persists the same point rather than spuriously reporting none.
func TestCheckpointFrontier_IdempotentCheckpoint(t *testing.T) {
	f := NewFrontier()
	f.RecordTxBoundary(2, ir.Position{Engine: "mysql", Token: "X"})
	f.MarkCommitted(1)
	f.MarkCommitted(2)

	for i := 0; i < 3; i++ {
		got, _, ok := f.CheckpointPosition()
		if !ok || got.Token != "X" {
			t.Fatalf("call %d: checkpoint=%v ok=%v, want token X", i, got, ok)
		}
	}
}

// TestWaitForFrontier_WakesOnAdvance: a waiter blocked on a target seq
// wakes once concurrent MarkCommitted calls advance the frontier past it.
func TestWaitForFrontier_WakesOnAdvance(t *testing.T) {
	f := NewFrontier()
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		done <- f.WaitForFrontier(ctx, 3)
	}()

	// Commit out of order from another goroutine; the waiter must not wake
	// until the contiguous frontier reaches 3.
	var wg sync.WaitGroup
	for _, seq := range []uint64{2, 1, 3} {
		wg.Add(1)
		go func(s uint64) { defer wg.Done(); f.MarkCommitted(s) }(seq)
	}
	wg.Wait()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitForFrontier returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("WaitForFrontier did not wake; frontier=%d", f.FrontierSeq())
	}
}

// TestWaitForFrontier_AlreadyReached: target ≤ current frontier returns
// immediately (no block).
func TestWaitForFrontier_AlreadyReached(t *testing.T) {
	f := NewFrontier()
	f.MarkCommitted(1)
	f.MarkCommitted(2)
	if err := f.WaitForFrontier(context.Background(), 2); err != nil {
		t.Fatalf("WaitForFrontier(2) = %v, want nil (already reached)", err)
	}
}

// TestWaitForFrontier_CtxCancel: a waiter unblocks with the ctx error when
// the frontier never reaches the target.
func TestWaitForFrontier_CtxCancel(t *testing.T) {
	f := NewFrontier()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.WaitForFrontier(ctx, 5) }()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("WaitForFrontier returned nil after cancel, want ctx error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitForFrontier did not unblock on ctx cancel")
	}
}
