//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeApplier_SkipFlushBeforePositionDurable is the audit-PG-4 pin: the
// H-4 at-least-once skip-count contract rests on flushSkippedTables running
// BEFORE the covering position becomes durable, so a failed flush rolls the
// boundary back and the skip re-delivers on resume. That ordering was held
// only by comments at five call sites — moving the flush AFTER the position
// write passed every existing test. This binds it: with the skip ledger made
// un-writable, a skipped event must FAIL the apply AND leave the position where
// it was, never advanced past the skip. Under the mutation (flush after the
// position write) the position advances to the skipped event's token before the
// flush fails — which this test catches.
func TestChangeApplier_SkipFlushBeforePositionDurable(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	applyPGApplier(t, dsn, `CREATE TABLE known_flushord (id BIGINT PRIMARY KEY)`)
	defer applyPGApplier(t, dsn, `DROP TABLE IF EXISTS known_flushord`)

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	pos := func(tok string) ir.Position { return ir.Position{Engine: "postgres", Token: tok} }
	const p0 = `{"lsn":"0/16B2C00"}`
	const skipTok = `{"lsn":"0/16B2FFF"}`

	// Establish a baseline position P0 with a real applied row.
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Insert{Schema: "public", Table: "known_flushord", Row: ir.Row{"id": int64(1)}, Position: pos(p0)},
	})
	got, ok, err := applier.ReadPosition(ctx, testStreamID)
	if err != nil || !ok || got.Token != p0 {
		t.Fatalf("baseline position = (%q, ok=%v, err=%v); want %q", got.Token, ok, err, p0)
	}

	// Make the skip-ledger UPSERT fail: drop the ledger table out from under
	// the applier. The next skip's flush then errors at the boundary.
	applyPGApplier(t, dsn, `DROP TABLE IF EXISTS `+appliershared.SkippedTablesTableName)

	// A standalone skipped event (Insert into a genuinely-missing table). Its
	// per-change position-write boundary flushes the skip ledger FIRST; the
	// flush fails (table gone), so the position write must never happen.
	ch := make(chan ir.Change, 1)
	ch <- ir.Insert{Schema: "public", Table: "ghost_flushord", Row: ir.Row{"id": int64(9)}, Position: pos(skipTok)}
	close(ch)
	if applyErr := applier.Apply(ctx, testStreamID, ch); applyErr == nil {
		t.Fatal("PG-4: Apply with an un-writable skip ledger returned nil — the flush failure was swallowed")
	}

	// The load-bearing assertion: the position did NOT advance to the skipped
	// event's token. Flush-before-durable holds -> still P0. If the flush ran
	// AFTER the position write, this would be skipTok.
	after, ok, err := applier.ReadPosition(ctx, testStreamID)
	if err != nil || !ok {
		t.Fatalf("ReadPosition after failed flush: ok=%v err=%v", ok, err)
	}
	if after.Token == skipTok {
		t.Fatalf("PG-4: position ADVANCED to the skipped event's token %q despite the flush failing — "+
			"flushSkippedTables ran AFTER the position write, breaking the at-least-once skip contract", skipTok)
	}
	if after.Token != p0 {
		t.Fatalf("PG-4: position = %q; want the pre-skip baseline %q (unchanged by a failed-flush boundary)", after.Token, p0)
	}
}
