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

	owed, err := planBoundaryBackfill(context.Background(), bf, "public.dj", pre, post, snap, forwardRecoveryHint)
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
	none, err := planBoundaryBackfill(context.Background(), bf, "public.dj", post, post, snap, forwardRecoveryHint)
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

// TestBackfillWatermark: a backfill's floors are the last POSITIONED,
// non-snapshot change forwarded BEFORE it and the boundary snapshot's own
// position — never a change after it.
func TestBackfillWatermark(t *testing.T) {
	var ledger addedColumnBackfillLedger
	e := ledger.open("public.dj", []string{"flag"})
	ledger.finished(e, nil)
	var w backfillWatermark
	w.observe(ir.TxBegin{Position: testPos(4)})
	w.observe(ir.SchemaSnapshot{Position: testPos(9)}) // a schema anchor is not a stream position
	w.observe(ir.Insert{})                             // no position token
	w.await(&boundaryBackfill{bf: &schemaForwardBackfill{ledger: &ledger}, snap: ir.SchemaSnapshot{Position: testPos(2)}, owed: e})
	w.observe(ir.Insert{Position: testPos(8)}) // after the backfill: not a floor
	if len(e.floors) != 2 || e.floors[0] != testPos(4) || e.floors[1] != testPos(2) {
		t.Fatalf("floors = %+v; want [4 (the last change before the backfill), 2 (the snapshot)]", e.floors)
	}
}

// gtidSetOrderer orders test positions the way a MySQL GTID set (or a
// VStream VGTID) orders: a token is a comma-separated set of transaction
// ids, and p is at or after anchor when p ⊇ anchor — a partial order.
type gtidSetOrderer struct{}

func (gtidSetOrderer) PositionAtOrAfter(p, anchor ir.Position) (bool, error) {
	have := map[string]bool{}
	for _, id := range strings.Split(p.Token, ",") {
		have[id] = true
	}
	for _, id := range strings.Split(anchor.Token, ",") {
		if !have[id] {
			return false, nil
		}
	}
	return true, nil
}

type gtidTestEngine struct {
	ir.Engine
	gtidSetOrderer
}

func gtidPos(set string) ir.Position { return ir.Position{Engine: "test", Token: set} }

// TestSettleAddedColumnBackfills_RowPositionsExcludeTheirOwnTransaction is
// the 2026-09-24 value-fidelity finding 1, per source shape. On a MySQL
// GTID or VStream source a row's position is the executed set WITHOUT its
// own transaction, so the boundary snapshot (S+ddl) and the first row after
// the backfill (still S+ddl) carry the same position — which the serial
// applier persists the moment it applies the snapshot, before any
// backfilled row. That persisted position must NOT prove the backfill
// durable; only the TxCommit that folds in the next transaction (S+ddl+g)
// may. On Postgres the snapshot and the transaction's rows carry its
// CommitLSN and its TxCommit the strictly later end LSN; file/pos is a
// total order.
func TestSettleAddedColumnBackfills_RowPositionsExcludeTheirOwnTransaction(t *testing.T) {
	cases := []struct {
		name      string
		source    ir.Engine
		last      ir.Position // the last change forwarded before the backfill (its TxBegin)
		snap      ir.Position
		persisted ir.Position
		durable   bool
	}{
		{name: "gtid: persisted = the snapshot's set (applied before any backfilled row)", source: gtidTestEngine{}, last: gtidPos("s,ddl"), snap: gtidPos("s,ddl"), persisted: gtidPos("s,ddl")},
		{name: "gtid: an earlier transaction's commit (s+ddl+x) landed before the backfill", source: gtidTestEngine{}, last: gtidPos("s,ddl,x"), snap: gtidPos("s,ddl"), persisted: gtidPos("s,ddl,x")},
		{name: "gtid: the next transaction's commit folds its own gtid", source: gtidTestEngine{}, last: gtidPos("s,ddl"), snap: gtidPos("s,ddl"), persisted: gtidPos("s,ddl,g"), durable: true},
		{name: "gtid: an unrelated set (another lineage) proves nothing", source: gtidTestEngine{}, last: gtidPos("s,ddl"), snap: gtidPos("s,ddl"), persisted: gtidPos("other")},
		{name: "lsn: persisted = the transaction's CommitLSN its rows carry", source: orderedTestEngine{}, last: testPos(100), snap: testPos(90), persisted: testPos(100)},
		{name: "lsn: persisted = its TxCommit end LSN", source: orderedTestEngine{}, last: testPos(100), snap: testPos(90), persisted: testPos(101), durable: true},
		{name: "lsn: a first-touch snapshot at 0/0 is not the only floor", source: orderedTestEngine{}, last: testPos(100), snap: testPos(0), persisted: testPos(50)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := &Streamer{Source: c.source}
			e := s.addedColumnBackfills.open("public.dj", []string{"flag"})
			s.addedColumnBackfills.finished(e, nil)
			var w backfillWatermark
			w.observe(ir.TxBegin{Position: c.last})
			w.await(&boundaryBackfill{bf: &schemaForwardBackfill{ledger: &s.addedColumnBackfills}, snap: ir.SchemaSnapshot{Position: c.snap}, owed: e})
			// The first row after the backfill carries the pre-transaction
			// position — the old watermark, which proved nothing.
			w.observe(ir.Insert{Position: c.last})
			owed := s.settleDurableAddedColumnBackfills(context.Background(), ledgerTestApplier{pos: c.persisted, ok: true}, "s")
			if got := len(owed) == 0; got != c.durable {
				t.Fatalf("backfill proven durable = %v with persisted %q, floors %+v; want %v", got, c.persisted.Token, e.floors, c.durable)
			}
		})
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
// — is strictly past its floors. Everything else ends the attempt
// with ADD-COLUMN-BACKFILL-INCOMPLETE, terminal and wrapping
// ErrUnforwardedSchemaChange so the run's recorder persists it.
func TestSettleAddedColumnBackfills(t *testing.T) {
	runErr := errors.New("some other attempt error")
	cases := []struct {
		name    string
		emitted bool
		cause   error
		floor   ir.Position
		applier ledgerTestApplier
		source  ir.Engine
		wantGap bool
	}{
		{name: "finished, persisted past the floor", emitted: true, floor: testPos(5), applier: ledgerTestApplier{pos: testPos(7), ok: true}, source: orderedTestEngine{}},
		// Equal is not past: a position the applier could have persisted
		// before the backfill's rows committed proves nothing about them.
		{name: "finished, persisted exactly at the floor", emitted: true, floor: testPos(7), applier: ledgerTestApplier{pos: testPos(7), ok: true}, source: orderedTestEngine{}, wantGap: true},
		// The applier returns its persisted position tagged with the TARGET
		// engine (a PG source's token under Engine "mysql"); it must be
		// re-tagged, or every cross-engine settle refuses.
		{name: "persisted position tagged with the target engine", emitted: true, floor: testPos(5), applier: ledgerTestApplier{pos: ir.Position{Engine: "mysql", Token: "7"}, ok: true}, source: orderedTestEngine{}},
		{name: "stopped early", cause: errors.New("connection reset"), applier: ledgerTestApplier{pos: testPos(7), ok: true}, source: orderedTestEngine{}, wantGap: true},
		{name: "finished, floors not yet recorded", emitted: true, applier: ledgerTestApplier{pos: testPos(7), ok: true}, source: orderedTestEngine{}, wantGap: true},
		{name: "finished, persisted short of the floor", emitted: true, floor: testPos(9), applier: ledgerTestApplier{pos: testPos(7), ok: true}, source: orderedTestEngine{}, wantGap: true},
		{name: "position unreadable", emitted: true, floor: testPos(5), applier: ledgerTestApplier{err: errors.New("boom")}, source: orderedTestEngine{}, wantGap: true},
		{name: "no persisted position", emitted: true, floor: testPos(5), applier: ledgerTestApplier{}, source: orderedTestEngine{}, wantGap: true},
		{name: "source cannot order positions", emitted: true, floor: testPos(5), applier: ledgerTestApplier{pos: testPos(7), ok: true}, wantGap: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := &Streamer{Source: c.source}
			e := s.addedColumnBackfills.open("public.dj", []string{"flag"})
			s.addedColumnBackfills.finished(e, c.cause)
			if c.emitted && c.cause == nil && c.floor.Token != "" {
				s.addedColumnBackfills.markFloors(e, c.floor)
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
