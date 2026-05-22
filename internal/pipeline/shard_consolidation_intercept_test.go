// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// drainChanges drains the channel into a slice until close or
// timeout. Test helper.
func drainChanges(t *testing.T, ch <-chan ir.Change, timeout time.Duration) []ir.Change {
	t.Helper()
	var out []ir.Change
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, c)
		case <-deadline.C:
			t.Fatalf("drainChanges timed out after %s; got %d changes", timeout, len(out))
		}
	}
}

func TestIntercept_NilRouter_PassThrough(t *testing.T) {
	t.Parallel()
	in := make(chan ir.Change, 4)
	in <- ir.Insert{Schema: "public", Table: "users", Row: ir.Row{"id": 1}}
	in <- ir.TxCommit{}
	close(in)
	var errStore atomic.Pointer[error]
	var inRecv <-chan ir.Change = in
	out := interceptSchemaSnapshotsForCoordination(context.Background(), inRecv, nil, nil, &errStore)
	if out != inRecv {
		t.Errorf("nil router should pass-through verbatim; got a wrapped channel")
	}
}

func TestIntercept_FirstSnapshotSeedsCache_NoRoute(t *testing.T) {
	t.Parallel()
	clock := newMockClock(testClockNow())
	store := newFakeLeaseStore(clock.Now)
	prober := &fakeProber{}
	applier := &fakeShapeApplier{}
	mgr := newTestLeaseManager(t, store, "stream-a", LeaseConfig{}, clock)
	router, err := NewBoundaryRouter(mgr, applier, prober)
	if err != nil {
		t.Fatalf("NewBoundaryRouter: %v", err)
	}

	in := make(chan ir.Change, 2)
	tbl := fixtureTable("users", "id", "email")
	in <- ir.SchemaSnapshot{Schema: "public", Table: "users", Position: ir.Position{Token: "p1"}, IR: tbl}
	close(in)

	var errStore atomic.Pointer[error]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := interceptSchemaSnapshotsForCoordination(ctx, in, nil, router, &errStore)
	got := drainChanges(t, out, 2*time.Second)
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 change forwarded; got %d", len(got))
	}
	if _, ok := got[0].(ir.SchemaSnapshot); !ok {
		t.Errorf("expected SchemaSnapshot forwarded; got %T", got[0])
	}
	if applier.addColCalls != 0 {
		t.Errorf("first snapshot should not invoke applier; got %d AlterAddColumn calls", applier.addColCalls)
	}
	if errStore.Load() != nil {
		t.Errorf("errStore should be empty on first snapshot")
	}
}

func TestIntercept_SecondSnapshotRoutesAddColumn(t *testing.T) {
	t.Parallel()
	clock := newMockClock(testClockNow())
	store := newFakeLeaseStore(clock.Now)
	prober := &fakeProber{}
	applier := &fakeShapeApplier{}
	mgr := newTestLeaseManager(t, store, "stream-a", LeaseConfig{LeaseDuration: time.Hour, RenewDeadline: 30 * time.Minute, RetryPeriod: 5 * time.Minute}, clock)
	router, err := NewBoundaryRouter(mgr, applier, prober)
	if err != nil {
		t.Fatalf("NewBoundaryRouter: %v", err)
	}

	in := make(chan ir.Change, 4)
	pre := fixtureTable("users", "id")
	post := fixtureTable("users", "id", "added_at")
	in <- ir.SchemaSnapshot{Schema: "public", Table: "users", Position: ir.Position{Token: "p1"}, IR: pre}
	in <- ir.SchemaSnapshot{Schema: "public", Table: "users", Position: ir.Position{Token: "p2"}, IR: post}
	close(in)

	var errStore atomic.Pointer[error]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := interceptSchemaSnapshotsForCoordination(ctx, in, nil, router, &errStore)
	got := drainChanges(t, out, 2*time.Second)
	if len(got) != 2 {
		t.Fatalf("expected 2 snapshots forwarded; got %d", len(got))
	}
	if applier.addColCalls != 1 {
		t.Errorf("expected exactly 1 AlterAddColumn call on second snapshot; got %d", applier.addColCalls)
	}
}

func TestIntercept_UnrecognizedShape_ShortCircuitsAndStoresError(t *testing.T) {
	t.Parallel()
	clock := newMockClock(testClockNow())
	store := newFakeLeaseStore(clock.Now)
	prober := &fakeProber{}
	applier := &fakeShapeApplier{}
	mgr := newTestLeaseManager(t, store, "stream-a", LeaseConfig{}, clock)
	router, err := NewBoundaryRouter(mgr, applier, prober)
	if err != nil {
		t.Fatalf("NewBoundaryRouter: %v", err)
	}

	in := make(chan ir.Change, 4)
	pre := fixtureTable("users", "id", "deprecated")
	post := fixtureTable("users", "id", "added_at") // combo: drop + add
	in <- ir.SchemaSnapshot{Schema: "public", Table: "users", Position: ir.Position{Token: "p1"}, IR: pre}
	in <- ir.SchemaSnapshot{Schema: "public", Table: "users", Position: ir.Position{Token: "p2"}, IR: post}
	close(in)

	var errStore atomic.Pointer[error]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := interceptSchemaSnapshotsForCoordination(ctx, in, nil, router, &errStore)
	got := drainChanges(t, out, 2*time.Second)
	// First snapshot forwarded (cache seed); second NOT forwarded
	// (refusal short-circuits).
	if len(got) != 1 {
		t.Errorf("expected 1 snapshot forwarded (cache seed only); got %d", len(got))
	}
	storedErr := errStore.Load()
	if storedErr == nil || *storedErr == nil {
		t.Fatal("expected errStore to carry the refusal")
	}
}

// TestIntercept_SeededFromColdStart_FirstCDCSnapshotRoutes pins the
// ADR-0054 Bug 83 fix: a non-empty cold-start seed pre-populates the
// intercept's cache so the first CDC SchemaSnapshot per seeded table
// is treated as a real boundary (pre = seed, post = CDC snapshot) and
// fires RouteBoundary. Pre-fix the first CDC snapshot was treated as
// the cold-start anchor and the boundary was never routed.
func TestIntercept_SeededFromColdStart_FirstCDCSnapshotRoutes(t *testing.T) {
	t.Parallel()
	clock := newMockClock(testClockNow())
	store := newFakeLeaseStore(clock.Now)
	prober := &fakeProber{}
	applier := &fakeShapeApplier{}
	mgr := newTestLeaseManager(t, store, "stream-a", LeaseConfig{LeaseDuration: time.Hour, RenewDeadline: 30 * time.Minute, RetryPeriod: 5 * time.Minute}, clock)
	router, err := NewBoundaryRouter(mgr, applier, prober)
	if err != nil {
		t.Fatalf("NewBoundaryRouter: %v", err)
	}

	pre := fixtureTable("users", "id")
	pre.Schema = "public"
	post := fixtureTable("users", "id", "added_at")

	seed := []ir.SchemaSnapshot{
		{Schema: "public", Table: "users", IR: pre},
	}
	in := make(chan ir.Change, 2)
	in <- ir.SchemaSnapshot{Schema: "public", Table: "users", Position: ir.Position{Token: "p1"}, IR: post}
	close(in)

	var errStore atomic.Pointer[error]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := interceptSchemaSnapshotsForCoordination(ctx, in, seed, router, &errStore)
	got := drainChanges(t, out, 2*time.Second)
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 change forwarded (the CDC SchemaSnapshot; seed is NOT forwarded); got %d", len(got))
	}
	if _, ok := got[0].(ir.SchemaSnapshot); !ok {
		t.Errorf("expected SchemaSnapshot forwarded; got %T", got[0])
	}
	if applier.addColCalls != 1 {
		t.Errorf("expected exactly 1 AlterAddColumn call (seed + 1 CDC snapshot = 1 boundary); got %d", applier.addColCalls)
	}
	if errStore.Load() != nil {
		t.Errorf("errStore should be empty after successful route")
	}
}

// TestIntercept_SeededFromColdStart_NoCDCSnapshot_NoRoute confirms
// the seed alone does not drive RouteBoundary — seeds are pre-cache
// entries, not synthetic boundaries.
func TestIntercept_SeededFromColdStart_NoCDCSnapshot_NoRoute(t *testing.T) {
	t.Parallel()
	clock := newMockClock(testClockNow())
	store := newFakeLeaseStore(clock.Now)
	prober := &fakeProber{}
	applier := &fakeShapeApplier{}
	mgr := newTestLeaseManager(t, store, "stream-a", LeaseConfig{}, clock)
	router, err := NewBoundaryRouter(mgr, applier, prober)
	if err != nil {
		t.Fatalf("NewBoundaryRouter: %v", err)
	}

	pre := fixtureTable("users", "id", "email")
	seed := []ir.SchemaSnapshot{
		{Schema: "public", Table: "users", IR: pre},
	}
	in := make(chan ir.Change)
	close(in)

	var errStore atomic.Pointer[error]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := interceptSchemaSnapshotsForCoordination(ctx, in, seed, router, &errStore)
	got := drainChanges(t, out, 2*time.Second)
	if len(got) != 0 {
		t.Errorf("expected 0 forwarded changes; got %d (seeds are NOT forwarded)", len(got))
	}
	if applier.addColCalls != 0 {
		t.Errorf("seed alone should not invoke applier; got %d AlterAddColumn calls", applier.addColCalls)
	}
	if errStore.Load() != nil {
		t.Errorf("errStore should be empty when no CDC snapshot arrives")
	}
}

// TestIntercept_SeededFromColdStart_MultiTable confirms per-table
// seed dispatch: a multi-table seed pre-populates the cache for each
// table; a CDC snapshot for one of them routes the right boundary
// without disturbing the other.
func TestIntercept_SeededFromColdStart_MultiTable(t *testing.T) {
	t.Parallel()
	clock := newMockClock(testClockNow())
	store := newFakeLeaseStore(clock.Now)
	prober := &fakeProber{}
	applier := &fakeShapeApplier{}
	mgr := newTestLeaseManager(t, store, "stream-a", LeaseConfig{LeaseDuration: time.Hour, RenewDeadline: 30 * time.Minute, RetryPeriod: 5 * time.Minute}, clock)
	router, err := NewBoundaryRouter(mgr, applier, prober)
	if err != nil {
		t.Fatalf("NewBoundaryRouter: %v", err)
	}

	usersPre := fixtureTable("users", "id")
	ordersPre := fixtureTable("orders", "id", "total")
	seed := []ir.SchemaSnapshot{
		{Schema: "public", Table: "users", IR: usersPre},
		{Schema: "public", Table: "orders", IR: ordersPre},
	}

	usersPost := fixtureTable("users", "id", "added_at")
	in := make(chan ir.Change, 2)
	in <- ir.SchemaSnapshot{Schema: "public", Table: "users", Position: ir.Position{Token: "p1"}, IR: usersPost}
	close(in)

	var errStore atomic.Pointer[error]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := interceptSchemaSnapshotsForCoordination(ctx, in, seed, router, &errStore)
	got := drainChanges(t, out, 2*time.Second)
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 forwarded change (users CDC snapshot); got %d", len(got))
	}
	if applier.addColCalls != 1 {
		t.Errorf("expected exactly 1 AlterAddColumn call (users add_col); got %d", applier.addColCalls)
	}
	if errStore.Load() != nil {
		t.Errorf("errStore should be empty after successful route")
	}
}

func TestSynthesizeColdStartSeedSnapshots(t *testing.T) {
	t.Parallel()
	// nil schema → empty seed (defensive).
	if got := synthesizeColdStartSeedSnapshots(nil); got != nil {
		t.Errorf("nil schema → got %d snapshots; want nil", len(got))
	}
	// Empty schema → empty seed.
	emptyOut := synthesizeColdStartSeedSnapshots(&ir.Schema{})
	if len(emptyOut) != 0 {
		t.Errorf("empty schema → got %d snapshots; want 0", len(emptyOut))
	}
	// Multi-table schema → one snapshot per non-nil table; nil entries
	// skipped defensively.
	tblA := fixtureTable("users", "id")
	tblA.Schema = "public"
	tblB := fixtureTable("orders", "id")
	tblB.Schema = "public"
	schema := &ir.Schema{Tables: []*ir.Table{tblA, nil, tblB}}
	out := synthesizeColdStartSeedSnapshots(schema)
	if len(out) != 2 {
		t.Fatalf("expected 2 snapshots; got %d", len(out))
	}
	if out[0].Table != "users" || out[0].IR != tblA {
		t.Errorf("snapshot[0] = %+v; want Table=users, IR=tblA", out[0])
	}
	if out[1].Table != "orders" || out[1].IR != tblB {
		t.Errorf("snapshot[1] = %+v; want Table=orders, IR=tblB", out[1])
	}
}

func TestDeriveDDLText_Deterministic(t *testing.T) {
	t.Parallel()
	t1 := fixtureTable("users", "id", "email")
	t2 := fixtureTable("users", "id", "email")
	if deriveDDLText(t1) != deriveDDLText(t2) {
		t.Error("deriveDDLText should be deterministic for structurally identical tables")
	}
	t3 := fixtureTable("users", "id", "email", "added_at")
	if deriveDDLText(t1) == deriveDDLText(t3) {
		t.Error("deriveDDLText should differ for different schemas")
	}
	if deriveDDLText(nil) != "" {
		t.Errorf("deriveDDLText(nil) = %q, want empty", deriveDDLText(nil))
	}
}
