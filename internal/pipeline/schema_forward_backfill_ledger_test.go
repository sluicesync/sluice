// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestBoundaryBackfill_OnlyOnAddColumn: a routed ADD COLUMN boundary owes a
// backfill — entered on the ledger when PLANNED, before the snapshot is
// forwarded — and every other boundary owes nothing.
func TestBoundaryBackfill_OnlyOnAddColumn(t *testing.T) {
	pre := addColForwardTable("dj")
	post := addColForwardTable("dj", &ir.Column{Name: "flag", Type: ir.Text{}, Nullable: true})
	snap := addColForwardSnap(post)
	reader := &pagedBackfillReader{rows: backfillRows(2)}
	var ledger addedColumnBackfillLedger
	bf := &schemaForwardBackfill{reader: staticBackfillReader(reader), streamID: "s", batchSize: 10, ledger: &ledger}

	owed, err := planBoundaryBackfill(bf, "public.dj", pre, post, snap, forwardRecoveryHint)
	if err != nil || owed == nil {
		t.Fatalf("ADD COLUMN boundary planned %v, %v; want a backfill", owed, err)
	}
	if len(ledger.entries) != 1 || ledger.entries[0].emitted {
		t.Fatalf("a planned backfill must be owed (not emitted) on the ledger before it runs; ledger = %+v", ledger.entries)
	}
	out := make(chan ir.Change, 8)
	if err := owed.run(context.Background(), out); err != nil {
		t.Fatalf("run: %v", err)
	}
	close(out)
	if got := drainUpdates(t, out); len(got) != 2 {
		t.Fatalf("ADD COLUMN boundary backfilled %d rows; want 2", len(got))
	}
	if !ledger.entries[0].emitted {
		t.Fatal("a completed backfill is not marked emitted on the ledger")
	}

	// A no-op boundary (same table both sides) owes nothing and reads nothing.
	reader.page = 0
	none, err := planBoundaryBackfill(bf, "public.dj", post, post, snap, forwardRecoveryHint)
	if err != nil || none != nil {
		t.Fatalf("no-op boundary planned %v, %v; want nothing", none, err)
	}
	if err := none.run(context.Background(), make(chan ir.Change, 1)); err != nil || reader.page != 0 {
		t.Fatalf("no-op boundary ran (err %v, %d pages); want nothing", err, reader.page)
	}
	if len(ledger.entries) != 1 {
		t.Fatalf("a no-op boundary entered the ledger; entries = %d", len(ledger.entries))
	}
}

// TestBackfillWatermark: the watermark is the first POSITIONED, non-schema
// change forwarded after the backfill.
func TestBackfillWatermark(t *testing.T) {
	var ledger addedColumnBackfillLedger
	e := ledger.open("public.dj", []string{"flag"})
	ledger.finished(e, nil)
	var w backfillWatermark
	w.await(&boundaryBackfill{bf: &schemaForwardBackfill{ledger: &ledger}, owed: e})

	w.observe(ir.SchemaSnapshot{Position: testPos(3)}) // a schema anchor is not a checkpoint
	w.observe(ir.TxBegin{})                            // no position token
	w.observe(ir.Insert{Position: testPos(8)})
	w.observe(ir.Insert{Position: testPos(9)})
	if e.watermark != testPos(8) {
		t.Fatalf("watermark = %+v; want the first positioned change after the backfill (8)", e.watermark)
	}
}

// ledgerTestApplier answers ReadPosition only.
type ledgerTestApplier struct {
	ir.ChangeApplier
	pos ir.Position
	ok  bool
	err error
}

func (a ledgerTestApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return a.pos, a.ok, a.err
}

// orderedTestEngine is a source engine whose only surface is the numeric
// PositionOrderer.
type orderedTestEngine struct {
	ir.Engine
	numericOrderer
}

// TestSettleAddedColumnBackfills pins the attempt-exit settle: an owed
// backfill leaves the ledger only when it finished AND the applier's
// persisted position — read back from the target, the independent evidence
// — has reached the first change after it. Everything else ends the attempt
// with ADD-COLUMN-BACKFILL-INCOMPLETE, terminal and wrapping
// ErrUnforwardedSchemaChange so the run's recorder persists it.
func TestSettleAddedColumnBackfills(t *testing.T) {
	runErr := errors.New("some other attempt error")
	cases := []struct {
		name      string
		emitted   bool
		cause     error
		watermark ir.Position
		applier   ledgerTestApplier
		source    ir.Engine
		wantGap   bool
	}{
		{name: "finished, persisted past the watermark", emitted: true, watermark: testPos(5), applier: ledgerTestApplier{pos: testPos(7), ok: true}, source: orderedTestEngine{}},
		{name: "finished, persisted exactly at the watermark", emitted: true, watermark: testPos(7), applier: ledgerTestApplier{pos: testPos(7), ok: true}, source: orderedTestEngine{}},
		// The applier returns its persisted position tagged with the TARGET
		// engine (a PG source's token under Engine "mysql"); it must be
		// re-tagged, or every cross-engine settle refuses.
		{name: "persisted position tagged with the target engine", emitted: true, watermark: testPos(5), applier: ledgerTestApplier{pos: ir.Position{Engine: "mysql", Token: "7"}, ok: true}, source: orderedTestEngine{}},
		{name: "stopped early", cause: errors.New("connection reset"), applier: ledgerTestApplier{pos: testPos(7), ok: true}, source: orderedTestEngine{}, wantGap: true},
		{name: "finished, no change after it yet", emitted: true, applier: ledgerTestApplier{pos: testPos(7), ok: true}, source: orderedTestEngine{}, wantGap: true},
		{name: "finished, persisted short of the watermark", emitted: true, watermark: testPos(9), applier: ledgerTestApplier{pos: testPos(7), ok: true}, source: orderedTestEngine{}, wantGap: true},
		{name: "position unreadable", emitted: true, watermark: testPos(5), applier: ledgerTestApplier{err: errors.New("boom")}, source: orderedTestEngine{}, wantGap: true},
		{name: "no persisted position", emitted: true, watermark: testPos(5), applier: ledgerTestApplier{}, source: orderedTestEngine{}, wantGap: true},
		{name: "source cannot order positions", emitted: true, watermark: testPos(5), applier: ledgerTestApplier{pos: testPos(7), ok: true}, wantGap: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := &Streamer{Source: c.source}
			e := s.addedColumnBackfills.open("public.dj", []string{"flag"})
			s.addedColumnBackfills.finished(e, c.cause)
			if c.emitted && c.cause == nil {
				s.addedColumnBackfills.markWatermark(e, c.watermark)
			}
			got := s.settleAddedColumnBackfills(context.Background(), c.applier, "s", runErr)
			if !c.wantGap {
				if !errors.Is(got, runErr) || len(s.addedColumnBackfills.entries) != 0 {
					t.Fatalf("got %v with %d owed; want the attempt's own error and an empty ledger", got, len(s.addedColumnBackfills.entries))
				}
				return
			}
			var gap *addedColumnBackfillIncompleteError
			if !errors.As(got, &gap) {
				t.Fatalf("got %v; want ADD-COLUMN-BACKFILL-INCOMPLETE", got)
			}
			if !errors.Is(got, ir.ErrUnforwardedSchemaChange) || !gap.Terminal() {
				t.Fatal("the gap must wrap ErrUnforwardedSchemaChange (so it is recorded) and be terminal (so no retry skips it)")
			}
			for _, want := range []string{addColumnBackfillIncompleteMarker, "public.dj", "flag", runErr.Error(), unforwardedRefusalAckFlag} {
				if !strings.Contains(got.Error(), want) {
					t.Errorf("the refusal does not name %q: %v", want, got)
				}
			}
		})
	}
}

// TestSettleAddedColumnBackfills_DefersToAnotherRefusal: the control-table
// row holds ONE refusal; when the attempt is already ending on a fresh
// unforwarded-schema-change refusal, that one is kept (and the owed backfill
// is logged as not recorded). An empty ledger passes every error through.
func TestSettleAddedColumnBackfills_DefersToAnotherRefusal(t *testing.T) {
	refusal := ir.WithMarker(errors.New("reader refusal"), ir.ErrUnforwardedSchemaChange)
	s := &Streamer{Source: orderedTestEngine{}}
	s.addedColumnBackfills.open("public.dj", []string{"flag"})
	if got := s.settleAddedColumnBackfills(context.Background(), ledgerTestApplier{}, "s", refusal); !errors.Is(got, refusal) {
		t.Fatalf("got %v; want the reader's own refusal kept", got)
	}
	empty := &Streamer{}
	if got := empty.settleAddedColumnBackfills(context.Background(), nil, "s", nil); got != nil {
		t.Fatalf("empty ledger returned %v; want nil", got)
	}
}
