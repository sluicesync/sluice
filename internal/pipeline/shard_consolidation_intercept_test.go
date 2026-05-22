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
	out := interceptSchemaSnapshotsForCoordination(context.Background(), inRecv, nil, &errStore)
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
	out := interceptSchemaSnapshotsForCoordination(ctx, in, router, &errStore)
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
	out := interceptSchemaSnapshotsForCoordination(ctx, in, router, &errStore)
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
	out := interceptSchemaSnapshotsForCoordination(ctx, in, router, &errStore)
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
