// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// --- In-lane AIMD + tx-killer shrink-and-retry pins (ADR-0104 graduation) ---
//
// These pins were extracted verbatim-in-assertion from the MySQL package's
// change_applier_concurrent_test.go (ADR-0105 STEP 1). The only PLUMBING
// difference from the MySQL originals: laneapply must not import the mysql
// classifier (that would be an engine→shared cycle), so the retriable
// tx-killer is modelled by a SENTINEL error (errTxKiller) that the test's
// LaneApplier seam (testSeam.ClassifyError) maps to an [ir.RetriableError].
// Every ASSERTED value/timing is unchanged — attempts, shrinks, controller
// size, frontier seq, maxInLaneRetries+1 — because those exercise the
// orchestrator's retry/split logic, not the classifier.

// errTxKiller is the laneapply-side analogue of the MySQL pins' real Vitess
// Error 1105 tx-killer payload: a sentinel the test seam classifies as
// retriable, so the in-lane shrink/split path engages exactly as it does for
// a real tx-killer abort. (The MySQL integration pins still drive the REAL
// classifier end-to-end; this unit pin only needs a retriable signal.)
var errTxKiller = errors.New("test: transaction rolled back for tx killer rollback")

// testRetriableError implements [ir.RetriableError] for the test classifier.
type testRetriableError struct{ error }

func (testRetriableError) Retriable() bool          { return true }
func (testRetriableError) RetryHint() time.Duration { return 0 }
func (e testRetriableError) Unwrap() error          { return e.error }

// classifyTest is the test seam's ClassifyError: it wraps errTxKiller in a
// retriable error and returns every other error verbatim (non-retriable),
// mirroring an engine classifier's transient-set decision. nil → nil.
func classifyTest(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, errTxKiller) {
		return testRetriableError{err}
	}
	return err
}

// testSeam is a no-DB [LaneApplier] for the unit pins: ApplyLaneBatch runs
// the test's commit closure (returning the raw error), ClassifyError uses
// classifyTest, and the rest is unused on this path (applyLaneBatch /
// frontier only). Only ApplyLaneBatch + ClassifyError are exercised.
type testSeam struct {
	commit func(ctx context.Context, batch []ir.Change) error
}

func (s *testSeam) PKValuesForRouting(context.Context, ir.Change) (qualified string, pkVals []any, ok bool, err error) {
	return "", nil, false, nil
}

func (s *testSeam) ApplyLaneBatch(ctx context.Context, _ int, batch []ir.Change) (int, error) {
	if err := s.commit(ctx, batch); err != nil {
		return 0, err
	}
	return len(batch), nil
}

func (s *testSeam) ClassifyError(err error) error                       { return classifyTest(err) }
func (s *testSeam) WriteCheckpoint(context.Context, ir.Position) error  { return nil }
func (s *testSeam) ApplyBarrierChange(context.Context, ir.Change) error { return nil }
func (s *testSeam) InvalidateMetadataCaches(string, string)             {}

// fakeLaneController is a deterministic [ir.BatchSizeController] stand-in for
// the per-lane AIMD pins: NextBatchSize returns the current size; ObserveBatch
// HALVES it (floor 1) on a retriable error (mirroring the real controller's MD
// on [ir.TransactionKilledError]) and records the observed outcomes. It does
// no latency/windowing — the unit pins only care that a tx-killer drives a
// shrink and that observations land on the right lane's controller.
type fakeLaneController struct {
	mu       sync.Mutex
	size     int
	observed int
	shrinks  int
}

func newFakeLaneController(initial int) *fakeLaneController {
	return &fakeLaneController{size: initial}
}

func (c *fakeLaneController) NextBatchSize() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.size
}

func (c *fakeLaneController) ObserveBatch(_ context.Context, _ time.Duration, _ int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observed++
	// ObserveBatch receives the already-CLASSIFIED error (the orchestrator
	// classifies before observing), so retriability is read directly off the
	// classified value's [ir.RetriableError] surface — the same shape the real
	// controller's MD-on-tx-killer inspects.
	var re ir.RetriableError
	if errors.As(err, &re) && re.Retriable() {
		c.shrinks++
		c.size /= 2
		if c.size < 1 {
			c.size = 1
		}
	}
}

func (c *fakeLaneController) snapshot() (size, observed, shrinks int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.size, c.observed, c.shrinks
}

// newTestOrchestrator builds an orchestrator wired for the no-DB unit pins: a
// fake commit closure (via testSeam) and a frontier. It does NOT open a lane
// pool — only applyLaneBatch / the frontier are exercised. Mirrors the MySQL
// newTestLaneManager helper.
func newTestOrchestrator(lanes int, commit func(ctx context.Context, batch []ir.Change) error) *Orchestrator {
	return &Orchestrator{
		la:           &testSeam{commit: commit},
		lanes:        lanes,
		maxBatchSize: 8,
		frontier:     NewFrontier(),
		cancel:       func() {},
	}
}

// mkLaneChanges builds n LaneChange envelopes with seqs 1..n.
func mkLaneChanges(n int) []LaneChange {
	buf := make([]LaneChange, n)
	for i := range buf {
		buf[i] = LaneChange{Seq: uint64(i + 1), Change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(i + 1)}}}
	}
	return buf
}

// TestLaneApply_PersistentTxKillerSplitsToConverge pins the re-chunk
// convergence the live Track-B validation exposed: a batch too large to ever
// commit under the tx-killer timeout must be SPLIT (not re-applied whole) to
// make progress. The fake commit tx-kills ANY batch larger than a threshold
// and succeeds only at/below it, so re-applying the same oversized batch can
// never converge — only halving does. Asserts the whole batch lands
// exactly-once (frontier reaches the last seq), every COMMITTED sub-batch is
// at/below the threshold (proof it split down), and the committed rows sum to
// the input with no gap/dup. (The transient-only TxKillerShrinkAndRetry pin
// below did NOT cover this — its injected tx-killer succeeds on retry-same.)
func TestLaneApply_PersistentTxKillerSplitsToConverge(t *testing.T) {
	const commitThreshold = 3 // any batch larger than this persistently tx-kills
	ctrl := newFakeLaneController(8)

	var committedSizes []int
	commit := func(_ context.Context, batch []ir.Change) error {
		if len(batch) > commitThreshold {
			return errTxKiller // persistent — re-applying whole can't converge
		}
		committedSizes = append(committedSizes, len(batch))
		return nil
	}
	m := newTestOrchestrator(1, commit)

	const n = 8
	buf := mkLaneChanges(n)
	committed, err := m.applyLaneBatch(context.Background(), 0, ctrl, buf)
	if err != nil {
		t.Fatalf("applyLaneBatch = %v; want nil (re-chunk must converge a persistent tx-killer)", err)
	}
	if got := m.frontier.FrontierSeq(); got != n {
		t.Errorf("frontier = %d; want %d (every change committed exactly once via splitting)", got, n)
	}
	// The returned committable size (the lane-read-cap input) must be ≤ the
	// threshold — proof applyLaneBatch reports the size its splits proved
	// committable, so laneApplyLoop caps the next read at that band instead of
	// the over-large ceiling (v0.99.81 churn fix).
	if committed <= 0 || committed > commitThreshold {
		t.Errorf("returned committable size = %d; want in (0, %d] (the learned read-cap input)", committed, commitThreshold)
	}
	total := 0
	for _, s := range committedSizes {
		if s > commitThreshold {
			t.Errorf("committed a sub-batch of %d > threshold %d (did not split down far enough)", s, commitThreshold)
		}
		total += s
	}
	if total != n {
		t.Errorf("committed rows = %d; want %d exactly (no gap, no dup)", total, n)
	}
}

// TestLaneApply_TxKillerShrinkAndRetry pins the core graduation claim: a
// tx-killer on a lane's FIRST commit attempt causes the SAME batch to be
// re-applied and succeed, the frontier advances exactly-once (every seq, no
// dup/gap), the controller shrank, and applyLaneBatch returns nil (no run
// cancel — in-lane recovery).
func TestLaneApply_TxKillerShrinkAndRetry(t *testing.T) {
	attempts := 0
	commit := func(_ context.Context, _ []ir.Change) error {
		attempts++
		if attempts == 1 {
			return errTxKiller // first attempt: tx-killer
		}
		return nil // retry succeeds
	}
	m := newTestOrchestrator(1, commit)
	ctrl := newFakeLaneController(8)

	buf := []LaneChange{
		{Seq: 1, Change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(1)}}},
		{Seq: 2, Change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(2)}}},
	}
	if _, err := m.applyLaneBatch(context.Background(), 0, ctrl, buf); err != nil {
		t.Fatalf("applyLaneBatch returned %v; want nil (in-lane recovery)", err)
	}
	if attempts != 2 {
		t.Errorf("commit attempts = %d; want 2 (one tx-killer + one success)", attempts)
	}
	// Frontier advanced to seq 2, exactly-once (no gap, no dup).
	if got := m.frontier.FrontierSeq(); got != 2 {
		t.Errorf("frontier = %d; want 2 (both seqs committed exactly once)", got)
	}
	size, observed, shrinks := ctrl.snapshot()
	if shrinks != 1 {
		t.Errorf("controller shrinks = %d; want 1 (tx-killer must shrink)", shrinks)
	}
	if size != 4 {
		t.Errorf("controller size = %d; want 4 (8 halved once)", size)
	}
	if observed != 2 {
		t.Errorf("controller observed = %d; want 2 (one per attempt)", observed)
	}
}

// TestLaneApply_MarkCommittedOnlyOnDurableCommit pins the load-bearing
// exactly-once invariant: while a batch keeps failing retriably, NO seq
// advances; the frontier moves only after the commit finally succeeds.
func TestLaneApply_MarkCommittedOnlyOnDurableCommit(t *testing.T) {
	ctrl := newFakeLaneController(8)
	buf := []LaneChange{{Seq: 1, Change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(1)}}}}

	attempts := 0
	var m *Orchestrator
	// The commit checks the frontier is still 0 on every failing attempt —
	// the frontier advance must NOT fire before a durable commit.
	commit := func(_ context.Context, _ []ir.Change) error {
		if got := m.frontier.FrontierSeq(); got != 0 {
			t.Fatalf("frontier advanced to %d before a durable commit", got)
		}
		attempts++
		if attempts < 3 {
			return errTxKiller
		}
		return nil
	}
	m = newTestOrchestrator(1, commit)

	if _, err := m.applyLaneBatch(context.Background(), 0, ctrl, buf); err != nil {
		t.Fatalf("applyLaneBatch = %v; want nil", err)
	}
	if got := m.frontier.FrontierSeq(); got != 1 {
		t.Errorf("frontier = %d after success; want 1", got)
	}
}

// TestLaneApply_RetryExhaustionIsFatal pins the loud-failure bound: a target
// that tx-kills on EVERY attempt fails the batch after maxInLaneRetries+1
// attempts (no infinite loop), surfaces a classified error, and never
// advances the frontier.
func TestLaneApply_RetryExhaustionIsFatal(t *testing.T) {
	attempts := 0
	commit := func(_ context.Context, _ []ir.Change) error {
		attempts++
		return errTxKiller
	}
	m := newTestOrchestrator(1, commit)
	ctrl := newFakeLaneController(8)
	buf := []LaneChange{{Seq: 1, Change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(1)}}}}

	_, err := m.applyLaneBatch(context.Background(), 0, ctrl, buf)
	if err == nil {
		t.Fatal("applyLaneBatch = nil; want a fatal error after retry exhaustion")
	}
	// One initial attempt + maxInLaneRetries retries.
	if want := maxInLaneRetries + 1; attempts != want {
		t.Errorf("commit attempts = %d; want %d (initial + maxInLaneRetries)", attempts, want)
	}
	if got := m.frontier.FrontierSeq(); got != 0 {
		t.Errorf("frontier = %d; want 0 (nothing durable)", got)
	}
	// The surfaced error stays classified-retriable so the streamer's
	// ADR-0038 warm-resume loop activates rather than exiting the stream.
	var re ir.RetriableError
	if !errors.As(err, &re) || !re.Retriable() {
		t.Errorf("exhaustion error = %v; want a classified RetriableError (warm-resume)", err)
	}
}

// TestLaneApply_NonRetriableIsImmediatelyFatal pins that a NON-retriable
// failure (e.g. a duplicate-key data bug) fails on the FIRST attempt without
// burning the retry budget — retry is for transients only.
func TestLaneApply_NonRetriableIsImmediatelyFatal(t *testing.T) {
	attempts := 0
	fatal := errors.New("Error 1062: Duplicate entry") // not classified retriable
	commit := func(_ context.Context, _ []ir.Change) error {
		attempts++
		return fatal
	}
	m := newTestOrchestrator(1, commit)
	ctrl := newFakeLaneController(8)
	buf := []LaneChange{{Seq: 1, Change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(1)}}}}

	if _, err := m.applyLaneBatch(context.Background(), 0, ctrl, buf); err == nil {
		t.Fatal("applyLaneBatch = nil; want a fatal error for a non-retriable failure")
	}
	if attempts != 1 {
		t.Errorf("commit attempts = %d; want 1 (non-retriable must not retry)", attempts)
	}
	if got := m.frontier.FrontierSeq(); got != 0 {
		t.Errorf("frontier = %d; want 0", got)
	}
}

// TestLaneApply_PerLaneIndependence pins that a tx-killer on lane i shrinks
// only lane i's controller — lane j's controller is untouched, so a slow
// lane doesn't drag the others down.
func TestLaneApply_PerLaneIndependence(t *testing.T) {
	// Lane 0 hits one tx-killer then succeeds; lane 1 succeeds immediately.
	lane0Attempts := 0
	commitLane0 := func(_ context.Context, _ []ir.Change) error {
		lane0Attempts++
		if lane0Attempts == 1 {
			return errTxKiller
		}
		return nil
	}
	commitLane1 := func(_ context.Context, _ []ir.Change) error { return nil }

	m0 := newTestOrchestrator(2, commitLane0)
	m1 := newTestOrchestrator(2, commitLane1)
	ctrl0 := newFakeLaneController(8)
	ctrl1 := newFakeLaneController(8)

	buf0 := []LaneChange{{Seq: 1, Change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(10)}}}}
	buf1 := []LaneChange{{Seq: 2, Change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(11)}}}}

	if _, err := m0.applyLaneBatch(context.Background(), 0, ctrl0, buf0); err != nil {
		t.Fatalf("lane 0 applyLaneBatch = %v; want nil", err)
	}
	if _, err := m1.applyLaneBatch(context.Background(), 1, ctrl1, buf1); err != nil {
		t.Fatalf("lane 1 applyLaneBatch = %v; want nil", err)
	}
	if _, _, s0 := ctrl0.snapshot(); s0 != 1 {
		t.Errorf("lane 0 controller shrinks = %d; want 1", s0)
	}
	if size1, _, s1 := ctrl1.snapshot(); s1 != 0 || size1 != 8 {
		t.Errorf("lane 1 controller shrinks=%d size=%d; want 0 shrinks, size 8 (independent of lane 0)", s1, size1)
	}
}

// TestLaneApply_NilControllerStaticSizeStillRetries pins the nil-controller
// path: with no AIMD controller the lane still does bounded in-lane retry on
// a retriable error (just no adaptive shrink), recovers, and advances the
// frontier — so --apply-concurrency without auto-tune is still resilient.
func TestLaneApply_NilControllerStaticSizeStillRetries(t *testing.T) {
	attempts := 0
	commit := func(_ context.Context, _ []ir.Change) error {
		attempts++
		if attempts == 1 {
			return errTxKiller
		}
		return nil
	}
	m := newTestOrchestrator(1, commit)
	buf := []LaneChange{{Seq: 1, Change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(1)}}}}

	if _, err := m.applyLaneBatch(context.Background(), 0, nil, buf); err != nil {
		t.Fatalf("applyLaneBatch(nil ctrl) = %v; want nil (bounded retry without AIMD)", err)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d; want 2", attempts)
	}
	if got := m.frontier.FrontierSeq(); got != 1 {
		t.Errorf("frontier = %d; want 1", got)
	}
}

// TestLaneApply_CtxCancelAbortsRetry pins that a cancelled ctx stops the
// in-lane retry promptly even while the commit keeps returning a retriable
// error — ctx cancellation must win over the retry budget.
func TestLaneApply_CtxCancelAbortsRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	commit := func(_ context.Context, _ []ir.Change) error {
		attempts++
		cancel() // cancel after the first failing attempt
		return errTxKiller
	}
	m := newTestOrchestrator(1, commit)
	ctrl := newFakeLaneController(8)
	buf := []LaneChange{{Seq: 1, Change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(1)}}}}

	if _, err := m.applyLaneBatch(ctx, 0, ctrl, buf); err == nil {
		t.Fatal("applyLaneBatch = nil; want an error after ctx cancel")
	}
	if attempts != 1 {
		t.Errorf("attempts = %d; want 1 (ctx cancel must stop the retry loop)", attempts)
	}
}
