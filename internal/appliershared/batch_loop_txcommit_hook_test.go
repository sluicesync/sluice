// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"context"
	"testing"
)

// TestRunBatchLoop_OnSourceTxCommitPrecedesTheBoundaryWrite pins the
// ADR-0190 notification at BOTH places the loop consumes a TxCommit — the
// in-batch boundary flush and the position-only write for a commit that lands
// on an empty batch — and that it fires BEFORE the position write that
// persists the commit, which is what lets that write delete the committed
// transaction's apply marks in the same target transaction.
func TestRunBatchLoop_OnSourceTxCommitPrecedesTheBoundaryWrite(t *testing.T) {
	rec := &recorder{}
	cfg := testConfig(t, rec, false)
	cfg.CheckpointOnlyAtTxBoundary = true
	cfg.OnSourceTxCommit = func() { rec.add("txCommitNoted") }

	// tx 1 fits its batch (in-batch boundary); tx 2 is split by the cap of 1
	// so its commit arrives on an empty batch (writeBoundaryOnly).
	ch := feed(
		true,
		txBegin("b1"), insertAt("p1"), txCommit("c1"),
	)
	if err := RunBatchLoop(context.Background(), cfg, "stream", ch, 10); err != nil {
		t.Fatalf("RunBatchLoop (in-batch boundary): %v", err)
	}
	assertEvents(t, rec, []string{"begin", "dispatch:p1", "txCommitNoted", "writePosition:c1", "commit"})

	rec2 := &recorder{}
	cfg2 := testConfig(t, rec2, false)
	cfg2.CheckpointOnlyAtTxBoundary = true
	cfg2.OnSourceTxCommit = func() { rec2.add("txCommitNoted") }
	ch2 := feed(true, txBegin("b2"), insertAt("p2"), txCommit("c2"))
	if err := RunBatchLoop(context.Background(), cfg2, "stream", ch2, 1); err != nil {
		t.Fatalf("RunBatchLoop (empty-batch boundary): %v", err)
	}
	assertEvents(t, rec2, []string{"begin", "dispatch:p2", "commit", "txCommitNoted", "begin", "writePosition:c2", "commit"})
}
