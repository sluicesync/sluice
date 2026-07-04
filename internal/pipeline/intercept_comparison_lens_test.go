// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Pins for the two-sided application of the Bug 84/86 comparison lens
// ([ir.CDCSchemaSnapshotNormalizer]) in both live-DDL intercepts.
//
// The TRIAGE-#3 regression shape: the lens was applied ONLY to the
// cold-start seed, so any representation change in the CDC projection
// (there: the temporal PrecisionUnspecified state) made the normalized
// seed compare unequal against the raw post — a phantom altered-column
// on every affected column at the first boundary, which combined with
// a genuine RENAME into the multi-shape combo refusal that stalled the
// stream. These tests pin, engine-independently via a stub lens, that
// each intercept normalizes the POST side before classification while
// still forwarding the ORIGINAL snapshot downstream.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// nullableStrippingLens is a stub [ir.CDCSchemaSnapshotNormalizer]
// whose lens zeroes Column.Nullable — the smallest field
// [diffAlteredColumn] inspects, mimicking the Bug-86 pgoutput
// nullability asymmetry. Deep-enough copy, idempotent (the interface
// contract).
type nullableStrippingLens struct{}

func (nullableStrippingLens) NormalizeForCDCComparison(t *ir.Table) *ir.Table {
	if t == nil {
		return nil
	}
	out := *t
	out.Columns = make([]*ir.Column, 0, len(t.Columns))
	for _, c := range t.Columns {
		nc := *c
		nc.Nullable = false
		out.Columns = append(out.Columns, &nc)
	}
	return &out
}

// lensFixtureTable is fixtureTable plus a nullable marker column whose
// Nullable flag only the lens can reconcile between the two sides.
func lensFixtureTable(nullable bool, colNames ...string) *ir.Table {
	tbl := &ir.Table{Name: "users", Schema: "public"}
	for _, c := range colNames {
		tbl.Columns = append(tbl.Columns, &ir.Column{Name: c, Type: ir.Integer{Width: 32}})
	}
	tbl.Columns = append(tbl.Columns, &ir.Column{Name: "marker", Type: ir.Integer{Width: 32}, Nullable: nullable})
	return tbl
}

// TestIntercept_ShapeA_LensAppliedToPostSide pins that the Shape-A
// intercept normalizes each incoming CDC snapshot with the source
// engine's lens before classifying it against the (already-normalized)
// seed: a post whose only delta is a lens-stripped field is a
// ShapeKindNone boundary — no applier call, no refusal — and the
// snapshot forwarded downstream is the ORIGINAL, un-normalized one
// (schema-history must record the faithful projection).
func TestIntercept_ShapeA_LensAppliedToPostSide(t *testing.T) {
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

	// Seed in comparison form (Nullable already stripped, as
	// synthesizeColdStartSeedSnapshots produces); CDC post carries the
	// raw Nullable=true the wire-side projection would.
	seedTbl := lensFixtureTable(false, "id", "email")
	rawPost := lensFixtureTable(true, "id", "email")
	seed := []ir.SchemaSnapshot{{Schema: "public", Table: "users", IR: seedTbl}}

	in := make(chan ir.Change, 2)
	in <- ir.SchemaSnapshot{Schema: "public", Table: "users", Position: ir.Position{Token: "p1"}, IR: rawPost}
	close(in)

	var errStore atomic.Pointer[error]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := interceptSchemaSnapshotsForCoordination(ctx, in, seed, router, nullableStrippingLens{}, &errStore)
	got := drainChanges(t, out, 2*time.Second)
	if e := errStore.Load(); e != nil {
		t.Fatalf("lens-covered delta must classify as no-op, not refuse: %v", *e)
	}
	if calls := applier.callNames(); len(calls) != 0 {
		t.Errorf("applier called %v on a lens-covered delta; want none (phantom alter — the TRIAGE-#3 regression shape)", calls)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 snapshot forwarded; got %d", len(got))
	}
	fwd, ok := got[0].(ir.SchemaSnapshot)
	if !ok {
		t.Fatalf("expected SchemaSnapshot forwarded; got %T", got[0])
	}
	if fwd.IR != rawPost {
		t.Errorf("forwarded snapshot IR is not the original projection; downstream schema-history must record the raw IR, not the comparison form")
	}
}

// TestIntercept_ShapeA_NoLens_PhantomAlterStillFires is the
// discrimination half of the pin above: with NO lens threaded (an
// engine without the normalizer surface), the same seed-vs-raw-post
// nullability delta classifies as a real AlterColumnNullability and
// reaches the applier. If this ever starts passing with a nil lens,
// the intercept has grown its own field-stripping and the lens pin
// above stops being load-bearing — revisit both.
func TestIntercept_ShapeA_NoLens_PhantomAlterStillFires(t *testing.T) {
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

	seedTbl := lensFixtureTable(false, "id", "email")
	rawPost := lensFixtureTable(true, "id", "email")
	seed := []ir.SchemaSnapshot{{Schema: "public", Table: "users", IR: seedTbl}}

	in := make(chan ir.Change, 2)
	in <- ir.SchemaSnapshot{Schema: "public", Table: "users", Position: ir.Position{Token: "p1"}, IR: rawPost}
	close(in)

	var errStore atomic.Pointer[error]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := interceptSchemaSnapshotsForCoordination(ctx, in, seed, router, nil, &errStore)
	drainChanges(t, out, 2*time.Second)
	calls := applier.callNames()
	if len(calls) != 1 || calls[0] != "AlterColumnNullability" {
		t.Errorf("applier calls = %v; want exactly [AlterColumnNullability] (nil lens must not silently strip fields)", calls)
	}
}

// TestForwardIntercept_LensAppliedToPostSide pins the same two-sided
// lens contract on the ADR-0058/0091 single-stream forward intercept,
// in the exact Bug-86 harm shape: a genuine ADD COLUMN arriving on the
// first post-cold-start boundary alongside a lens-covered nullability
// asymmetry on an EXISTING column. With the lens threaded
// (deps.normalizer) the boundary classifies as a clean AddColumn and
// forwards; without it, the phantom alter combines into the
// multi-shape combo refusal and the stream dies.
func TestForwardIntercept_LensAppliedToPostSide(t *testing.T) {
	run := func(t *testing.T, lens ir.CDCSchemaSnapshotNormalizer) (calls []string, forwarded int, refusal error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		applier := &fakeShapeApplier{}
		seedTbl := lensFixtureTable(false, "id")
		rawPost := lensFixtureTable(true, "id")
		rawPost.Columns = append(rawPost.Columns, &ir.Column{Name: "nickname", Type: ir.Varchar{Length: 100}})

		in := make(chan ir.Change, 1)
		in <- ir.SchemaSnapshot{Schema: "public", Table: "users", IR: rawPost}
		close(in)
		errStore := &atomic.Pointer[error]{}
		seed := []ir.SchemaSnapshot{{Schema: "public", Table: "users", IR: seedTbl}}
		out := interceptAddColumnForward(ctx, in, seed, schemaForwardDeps{
			applier:          applier,
			sourceEngineName: "postgres",
			targetEngineName: "postgres",
			normalizer:       lens,
		}, errStore)
		got := drainChannel(t, out, time.Second)
		var refErr error
		if e := errStore.Load(); e != nil {
			refErr = *e
		}
		return applier.callNames(), len(got), refErr
	}

	t.Run("with_lens_add_column_forwards", func(t *testing.T) {
		calls, forwarded, refusal := run(t, nullableStrippingLens{})
		if refusal != nil {
			t.Fatalf("lens-covered boundary must not refuse: %v", refusal)
		}
		if len(calls) != 1 || calls[0] != "AlterAddColumn" {
			t.Errorf("applier calls = %v; want exactly [AlterAddColumn]", calls)
		}
		if forwarded != 1 {
			t.Errorf("forwarded %d changes; want 1 (snapshot continues to schema-history)", forwarded)
		}
	})

	t.Run("without_lens_combo_refusal", func(t *testing.T) {
		// Discrimination half: nil lens leaves the phantom alter in
		// place and the boundary degrades into the combo refusal — the
		// pre-lens Bug 86 behaviour. Documents that the lens is
		// load-bearing for this shape.
		calls, _, refusal := run(t, nil)
		if refusal == nil {
			t.Fatalf("expected multi-shape combo refusal without the lens; got nil (did the intercept grow its own stripping?)")
		}
		if len(calls) != 0 {
			t.Errorf("applier calls = %v; want none (combo refusal precedes any apply)", calls)
		}
	})
}
