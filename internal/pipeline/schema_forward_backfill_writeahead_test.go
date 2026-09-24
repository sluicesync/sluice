// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// The added-column backfill's durable write-ahead record
// (schema_forward_backfill_ledger.go, "The write-ahead record"): what a
// process that never reaches its exit path leaves on the target is the
// ONLY evidence its restart has, so these pin that it is written before the
// ALTER, refuses when it cannot be, and is cleared only when it is still
// exactly the text this ledger wrote and nothing it covers is owed.

// writeAheadTestApplier is a target applier with the refusal store and a
// persisted position, the two things the write-ahead and the settle read.
type writeAheadTestApplier struct {
	*fakeRefusalStore
	pos ir.Position
}

func (a writeAheadTestApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return a.pos, a.pos.Token != "", nil
}

// alterObservingApplier records whether the write-ahead record was already
// on the target when AlterAddColumn ran.
type alterObservingApplier struct {
	*fakeShapeApplier
	store         *fakeRefusalStore
	recordAtAlter string
}

func (a *alterObservingApplier) AlterAddColumn(ctx context.Context, t *ir.Table, cols []*ir.Column) error {
	a.recordAtAlter = a.store.msg
	return a.fakeShapeApplier.AlterAddColumn(ctx, t, cols)
}

func writeAheadStreamer(store *fakeRefusalStore) (*Streamer, writeAheadTestApplier) {
	s := &Streamer{Source: orderedTestEngine{}}
	applier := writeAheadTestApplier{fakeRefusalStore: store}
	s.addedColumnBackfills.bind(applier, "s")
	return s, applier
}

// TestBackfillWriteAhead_RecordedBeforeTheAlter: the single-stream forward
// records the owed backfill on the target BEFORE AlterAddColumn, and a
// record that cannot be written refuses the boundary with the ALTER never
// issued.
func TestBackfillWriteAhead_RecordedBeforeTheAlter(t *testing.T) {
	pre := addColForwardTable("dj")
	post := addColForwardTable("dj", &ir.Column{Name: "flag", Type: ir.Text{}, Nullable: true})
	snap := addColForwardSnap(post)
	shape, err := ClassifyShape(pre, post)
	if err != nil {
		t.Fatal(err)
	}

	store := &fakeRefusalStore{}
	s, _ := writeAheadStreamer(store)
	applier := &alterObservingApplier{fakeShapeApplier: &fakeShapeApplier{}, store: store}
	deps := schemaForwardDeps{
		applier: applier, sourceEngineName: "postgres", targetEngineName: "postgres",
		backfill: &schemaForwardBackfill{ledger: &s.addedColumnBackfills, streamID: "s"},
	}
	if err := applyAddColumnForward(context.Background(), deps, "public.dj", snap, shape); err != nil {
		t.Fatalf("forward: %v", err)
	}
	if applier.addColCalls != 1 {
		t.Fatalf("AlterAddColumn calls = %d; want 1", applier.addColCalls)
	}
	for _, want := range []string{addColumnBackfillIncompleteMarker, "public.dj", "flag", "without settling it", unforwardedRefusalAckFlag} {
		if !strings.Contains(applier.recordAtAlter, want) {
			t.Errorf("the record on the target when the ALTER ran does not name %q: %q", want, applier.recordAtAlter)
		}
	}

	failing := &fakeRefusalStore{recordErr: errors.New("permission denied for table sluice_cdc_state")}
	s2, _ := writeAheadStreamer(failing)
	applier2 := &alterObservingApplier{fakeShapeApplier: &fakeShapeApplier{}, store: failing}
	deps.applier = applier2
	deps.backfill = &schemaForwardBackfill{ledger: &s2.addedColumnBackfills, streamID: "s"}
	err = applyAddColumnForward(context.Background(), deps, "public.dj", snap, shape)
	if err == nil || !strings.Contains(err.Error(), addColumnBackfillIncompleteMarker) || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("a write-ahead that could not be recorded returned %v; want a refusal naming the marker and the cause", err)
	}
	if applier2.addColCalls != 0 {
		t.Fatalf("the ALTER ran (%d calls) although its write-ahead record was never written", applier2.addColCalls)
	}

	// Opted out: no backfill is owed, so nothing is recorded.
	optOut := &fakeRefusalStore{}
	deps.applier = &alterObservingApplier{fakeShapeApplier: &fakeShapeApplier{}, store: optOut}
	deps.backfill = nil
	if err := applyAddColumnForward(context.Background(), deps, "public.dj", snap, shape); err != nil || len(optOut.recorded) != 0 {
		t.Fatalf("opted out: err %v, %d records; want the ALTER alone", err, len(optOut.recorded))
	}
}

// TestBackfillWriteAhead_ShapeARouterRecordsBeforeTheHoldersAlter: the
// Shape A lease holder's apply goes through the same write-ahead first, and
// a failed record refuses before the ALTER.
func TestBackfillWriteAhead_ShapeARouterRecordsBeforeTheHoldersAlter(t *testing.T) {
	post := addColForwardTable("dj", &ir.Column{Name: "flag", Type: ir.Text{}, Nullable: true})
	shape := Shape{Kind: ShapeKindAddColumn, AddedColumns: []*ir.Column{post.Columns[len(post.Columns)-1]}}
	var calledWith string
	applier := &fakeShapeApplier{}
	r := &BoundaryRouter{applier: applier, beforeAddColumn: func(_ context.Context, table string, cols []string) error {
		calledWith = table + ":" + strings.Join(cols, ",")
		return errors.New("record failed")
	}}
	if err := r.applyShape(context.Background(), "public.dj", post, shape); err == nil || !strings.Contains(err.Error(), "record failed") {
		t.Fatalf("applyShape returned %v; want the write-ahead failure", err)
	}
	if calledWith != "public.dj:flag" || applier.addColCalls != 0 {
		t.Fatalf("hook called with %q, %d ALTERs; want public.dj:flag and no ALTER", calledWith, applier.addColCalls)
	}
	// Any other shape does not owe a backfill.
	calledWith = ""
	_ = r.applyShape(context.Background(), "public.dj", post, Shape{Kind: ShapeKindDropColumn, DroppedColumns: shape.AddedColumns})
	if calledWith != "" {
		t.Fatalf("a DROP COLUMN ran the add-column write-ahead (%q)", calledWith)
	}
}

// TestBackfillWriteAhead_OncePerBoundaryAndCoversEverythingOwed: the
// pre-ALTER write and the plan-time write of one boundary record once; a
// second boundary's record names both, since it replaces the first.
func TestBackfillWriteAhead_OncePerBoundaryAndCoversEverythingOwed(t *testing.T) {
	store := &fakeRefusalStore{}
	s, _ := writeAheadStreamer(store)
	l := &s.addedColumnBackfills
	ctx := context.Background()
	for range 2 {
		if err := l.writeAhead(ctx, "public.a", []string{"x"}); err != nil {
			t.Fatal(err)
		}
	}
	if len(store.recorded) != 1 {
		t.Fatalf("one boundary recorded %d times; want once", len(store.recorded))
	}
	l.open("public.a", []string{"x"})
	if err := l.writeAhead(ctx, "public.b", []string{"y"}); err != nil {
		t.Fatal(err)
	}
	last := store.recorded[len(store.recorded)-1]
	if !strings.Contains(last, "public.a (x)") || !strings.Contains(last, "public.b (y)") {
		t.Fatalf("the replacing record does not cover both owed backfills: %q", last)
	}
}

// TestBackfillWriteAhead_CompareAndClear pins when the attempt-end settle
// clears the record: only once nothing it covers is owed, and only while
// the target still holds exactly the text this ledger wrote.
func TestBackfillWriteAhead_CompareAndClear(t *testing.T) {
	ctx := context.Background()
	runErr := errors.New("attempt ended")
	durable := func(s *Streamer, table string) {
		e := s.addedColumnBackfills.open(table, []string{"flag"})
		s.addedColumnBackfills.finished(e, nil)
		s.addedColumnBackfills.markWatermark(e, testPos(5))
	}

	t.Run("durable: cleared", func(t *testing.T) {
		store := &fakeRefusalStore{}
		s, applier := writeAheadStreamer(store)
		applier.pos = testPos(7)
		_ = s.addedColumnBackfills.writeAhead(ctx, "public.dj", []string{"flag"})
		durable(s, "public.dj")
		if got := s.settleAddedColumnBackfills(ctx, applier, "s", runErr); !errors.Is(got, runErr) {
			t.Fatalf("settle returned %v; want the attempt's own error", got)
		}
		if store.has || store.cleared != 1 {
			t.Fatalf("a durable backfill's write-ahead record was not cleared (has %v, cleared %d)", store.has, store.cleared)
		}
	})

	t.Run("replaced by another refusal: left alone", func(t *testing.T) {
		store := &fakeRefusalStore{}
		s, applier := writeAheadStreamer(store)
		applier.pos = testPos(7)
		_ = s.addedColumnBackfills.writeAhead(ctx, "public.dj", []string{"flag"})
		durable(s, "public.dj")
		store.msg = "[recorded …] UNFORWARDED-SCHEMA-CHANGE on public.dj: a CHECK was added"
		_ = s.settleAddedColumnBackfills(ctx, applier, "s", runErr)
		if !store.has || store.cleared != 0 || !strings.Contains(store.msg, "CHECK was added") {
			t.Fatalf("a record this ledger did not write was cleared (has %v, cleared %d, msg %q)", store.has, store.cleared, store.msg)
		}
	})

	t.Run("not durable: replaced by the attempt-end refusal, not cleared", func(t *testing.T) {
		store := &fakeRefusalStore{}
		s, applier := writeAheadStreamer(store)
		applier.pos = testPos(3) // short of the watermark
		_ = s.addedColumnBackfills.writeAhead(ctx, "public.dj", []string{"flag"})
		durable(s, "public.dj")
		var gap *addedColumnBackfillIncompleteError
		if got := s.settleAddedColumnBackfills(ctx, applier, "s", runErr); !errors.As(got, &gap) {
			t.Fatalf("settle returned %v; want ADD-COLUMN-BACKFILL-INCOMPLETE", got)
		}
		if !store.has || store.cleared != 0 {
			t.Fatal("the write-ahead record of a backfill that is still owed was cleared")
		}
	})

	t.Run("ALTER never confirmed: record kept", func(t *testing.T) {
		store := &fakeRefusalStore{}
		s, applier := writeAheadStreamer(store)
		applier.pos = testPos(7)
		// Written before the ALTER; the ALTER failed, so the boundary was
		// never entered on the ledger.
		_ = s.addedColumnBackfills.writeAhead(ctx, "public.dj", []string{"flag"})
		if got := s.settleAddedColumnBackfills(ctx, applier, "s", runErr); !errors.Is(got, runErr) {
			t.Fatalf("settle returned %v", got)
		}
		if !store.has || store.cleared != 0 {
			t.Fatal("the record of an ALTER that may have landed was cleared although its backfill never ran")
		}
	})

	t.Run("hard kill: nothing settled", func(t *testing.T) {
		store := &fakeRefusalStore{}
		s, applier := writeAheadStreamer(store)
		applier.pos = testPos(7)
		s.simulateHardKillForTest = true
		_ = s.addedColumnBackfills.writeAhead(ctx, "public.dj", []string{"flag"})
		s.addedColumnBackfills.open("public.dj", []string{"flag"})
		if got := s.settleAddedColumnBackfills(ctx, applier, "s", runErr); !errors.Is(got, runErr) || !store.has {
			t.Fatalf("hard-kill seam: got %v, record present %v; want the attempt error with the record untouched", got, store.has)
		}
	})
}

// TestBackfillWriteAhead_ClearedMidRun: a long-lived stream does not hold
// the record until it stops — the mid-run watch clears it once the backfill
// is durable, so a hard kill hours later does not come back refusing.
func TestBackfillWriteAhead_ClearedMidRun(t *testing.T) {
	store := &fakeRefusalStore{}
	s, applier := writeAheadStreamer(store)
	applier.pos = testPos(7)
	s.backfillDurabilityIntervalForTest = 5 * time.Millisecond
	ctx := context.Background()
	_ = s.addedColumnBackfills.writeAhead(ctx, "public.dj", []string{"flag"})
	e := s.addedColumnBackfills.open("public.dj", []string{"flag"})
	s.addedColumnBackfills.finished(e, nil)
	s.addedColumnBackfills.markWatermark(e, testPos(5))

	stop := s.watchAddedColumnBackfillDurability(ctx, applier, "s")
	defer stop()
	deadline := time.Now().Add(5 * time.Second)
	for s.addedColumnBackfills.outstanding() {
		if time.Now().After(deadline) {
			t.Fatal("the mid-run watch never settled the durable backfill")
		}
		time.Sleep(5 * time.Millisecond)
	}
	stop()
	if store.has || store.cleared != 1 {
		t.Fatalf("mid-run settle left the record (has %v, cleared %d)", store.has, store.cleared)
	}
}

// TestBackfillWriteAhead_NoStoreNoRecord: an applier without the refusal
// store (a unit-test stub; every real applier implements it) gets no
// write-ahead, as the startup door reads nothing from it — the forward is
// not refused.
func TestBackfillWriteAhead_NoStoreNoRecord(t *testing.T) {
	s := &Streamer{}
	s.addedColumnBackfills.bind(ledgerTestApplier{}, "s")
	if err := s.addedColumnBackfills.writeAhead(context.Background(), "public.dj", []string{"flag"}); err != nil {
		t.Fatalf("writeAhead without a store returned %v; want nil", err)
	}
	if s.addedColumnBackfills.waStored != "" {
		t.Fatal("a record was noted as written with no store to write it to")
	}
}
