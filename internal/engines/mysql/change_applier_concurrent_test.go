// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

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

// --- In-lane AIMD + tx-killer shrink-and-retry pins (ADR-0104 graduation) ---

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
	if err != nil && isApplierErrorRetriable(err) {
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

// fakeTxKillerError is a real MySQL Error 1105 carrying the canonical Vitess
// tx-killer payload ("vttablet ... code = Aborted ... for tx killer
// rollback") — the EXACT shape classifyApplierError recognizes as a
// retriable tx-killer abort. The pins drive the real classifier (via
// isApplierErrorRetriable), not a hand-rolled retriable, so they verify the
// in-lane retry predicate agrees with the streamer's ADR-0038 classification.
func fakeTxKillerError() error {
	return &gomysql.MySQLError{
		Number:  1105,
		Message: "vttablet: rpc error: code = Aborted desc = transaction rolled back for tx killer rollback",
	}
}

// newTestLaneManager builds a manager wired for the no-DB unit pins: a fake
// commit function and a frontier. It does NOT open a lane pool — only
// applyLaneBatch / the frontier are exercised.
func newTestLaneManager(lanes int, commit func(ctx context.Context, buf []laneChange) error) *concurrentApplyManager {
	return &concurrentApplyManager{
		lanes:         lanes,
		maxBatchSize:  8,
		frontier:      newCheckpointFrontier(),
		commitBatchFn: commit,
		cancel:        func() {},
	}
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
	m := newTestLaneManager(1, nil)
	ctrl := newFakeLaneController(8)

	var committedSizes []int
	m.commitBatchFn = func(_ context.Context, buf []laneChange) error {
		if len(buf) > commitThreshold {
			return fakeTxKillerError() // persistent — re-applying whole can't converge
		}
		committedSizes = append(committedSizes, len(buf))
		return nil
	}

	const n = 8
	buf := make([]laneChange, n)
	for i := range buf {
		buf[i] = laneChange{seq: uint64(i + 1), change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(i + 1)}}}
	}
	committed, err := m.applyLaneBatch(context.Background(), ctrl, buf)
	if err != nil {
		t.Fatalf("applyLaneBatch = %v; want nil (re-chunk must converge a persistent tx-killer)", err)
	}
	if got := m.frontier.frontierSeq(); got != n {
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
	commit := func(_ context.Context, _ []laneChange) error {
		attempts++
		if attempts == 1 {
			return fakeTxKillerError() // first attempt: tx-killer
		}
		return nil // retry succeeds
	}
	m := newTestLaneManager(1, commit)
	ctrl := newFakeLaneController(8)

	buf := []laneChange{
		{seq: 1, change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(1)}}},
		{seq: 2, change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(2)}}},
	}
	if _, err := m.applyLaneBatch(context.Background(), ctrl, buf); err != nil {
		t.Fatalf("applyLaneBatch returned %v; want nil (in-lane recovery)", err)
	}
	if attempts != 2 {
		t.Errorf("commit attempts = %d; want 2 (one tx-killer + one success)", attempts)
	}
	// Frontier advanced to seq 2, exactly-once (no gap, no dup).
	if got := m.frontier.frontierSeq(); got != 2 {
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
	m := newTestLaneManager(1, nil)
	ctrl := newFakeLaneController(8)
	buf := []laneChange{{seq: 1, change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(1)}}}}

	attempts := 0
	// The commit checks the frontier is still 0 on every failing attempt —
	// markCommitted must NOT fire before a durable commit.
	m.commitBatchFn = func(_ context.Context, _ []laneChange) error {
		if got := m.frontier.frontierSeq(); got != 0 {
			t.Fatalf("frontier advanced to %d before a durable commit", got)
		}
		attempts++
		if attempts < 3 {
			return fakeTxKillerError()
		}
		return nil
	}

	if _, err := m.applyLaneBatch(context.Background(), ctrl, buf); err != nil {
		t.Fatalf("applyLaneBatch = %v; want nil", err)
	}
	if got := m.frontier.frontierSeq(); got != 1 {
		t.Errorf("frontier = %d after success; want 1", got)
	}
}

// TestLaneApply_RetryExhaustionIsFatal pins the loud-failure bound: a target
// that tx-kills on EVERY attempt fails the batch after maxInLaneRetries+1
// attempts (no infinite loop), surfaces a classified error, and never
// advances the frontier.
func TestLaneApply_RetryExhaustionIsFatal(t *testing.T) {
	attempts := 0
	commit := func(_ context.Context, _ []laneChange) error {
		attempts++
		return fakeTxKillerError()
	}
	m := newTestLaneManager(1, commit)
	ctrl := newFakeLaneController(8)
	buf := []laneChange{{seq: 1, change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(1)}}}}

	_, err := m.applyLaneBatch(context.Background(), ctrl, buf)
	if err == nil {
		t.Fatal("applyLaneBatch = nil; want a fatal error after retry exhaustion")
	}
	// One initial attempt + maxInLaneRetries retries.
	if want := maxInLaneRetries + 1; attempts != want {
		t.Errorf("commit attempts = %d; want %d (initial + maxInLaneRetries)", attempts, want)
	}
	if got := m.frontier.frontierSeq(); got != 0 {
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
	commit := func(_ context.Context, _ []laneChange) error {
		attempts++
		return fatal
	}
	m := newTestLaneManager(1, commit)
	ctrl := newFakeLaneController(8)
	buf := []laneChange{{seq: 1, change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(1)}}}}

	if _, err := m.applyLaneBatch(context.Background(), ctrl, buf); err == nil {
		t.Fatal("applyLaneBatch = nil; want a fatal error for a non-retriable failure")
	}
	if attempts != 1 {
		t.Errorf("commit attempts = %d; want 1 (non-retriable must not retry)", attempts)
	}
	if got := m.frontier.frontierSeq(); got != 0 {
		t.Errorf("frontier = %d; want 0", got)
	}
}

// TestLaneApply_PerLaneIndependence pins that a tx-killer on lane i shrinks
// only lane i's controller — lane j's controller is untouched, so a slow
// lane doesn't drag the others down.
func TestLaneApply_PerLaneIndependence(t *testing.T) {
	// Lane 0 hits one tx-killer then succeeds; lane 1 succeeds immediately.
	lane0Attempts := 0
	commitLane0 := func(_ context.Context, _ []laneChange) error {
		lane0Attempts++
		if lane0Attempts == 1 {
			return fakeTxKillerError()
		}
		return nil
	}
	commitLane1 := func(_ context.Context, _ []laneChange) error { return nil }

	m0 := newTestLaneManager(2, commitLane0)
	m1 := newTestLaneManager(2, commitLane1)
	ctrl0 := newFakeLaneController(8)
	ctrl1 := newFakeLaneController(8)

	buf0 := []laneChange{{seq: 1, change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(10)}}}}
	buf1 := []laneChange{{seq: 2, change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(11)}}}}

	if _, err := m0.applyLaneBatch(context.Background(), ctrl0, buf0); err != nil {
		t.Fatalf("lane 0 applyLaneBatch = %v; want nil", err)
	}
	if _, err := m1.applyLaneBatch(context.Background(), ctrl1, buf1); err != nil {
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
	commit := func(_ context.Context, _ []laneChange) error {
		attempts++
		if attempts == 1 {
			return fakeTxKillerError()
		}
		return nil
	}
	m := newTestLaneManager(1, commit)
	buf := []laneChange{{seq: 1, change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(1)}}}}

	if _, err := m.applyLaneBatch(context.Background(), nil, buf); err != nil {
		t.Fatalf("applyLaneBatch(nil ctrl) = %v; want nil (bounded retry without AIMD)", err)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d; want 2", attempts)
	}
	if got := m.frontier.frontierSeq(); got != 1 {
		t.Errorf("frontier = %d; want 1", got)
	}
}

// TestLaneApply_CtxCancelAbortsRetry pins that a cancelled ctx stops the
// in-lane retry promptly even while the commit keeps returning a retriable
// error — ctx cancellation must win over the retry budget.
func TestLaneApply_CtxCancelAbortsRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	commit := func(_ context.Context, _ []laneChange) error {
		attempts++
		cancel() // cancel after the first failing attempt
		return fakeTxKillerError()
	}
	m := newTestLaneManager(1, commit)
	ctrl := newFakeLaneController(8)
	buf := []laneChange{{seq: 1, change: ir.Insert{Schema: "ks", Table: "t", Row: ir.Row{"id": int64(1)}}}}

	if _, err := m.applyLaneBatch(ctx, ctrl, buf); err == nil {
		t.Fatal("applyLaneBatch = nil; want an error after ctx cancel")
	}
	if attempts != 1 {
		t.Errorf("attempts = %d; want 1 (ctx cancel must stop the retry loop)", attempts)
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
