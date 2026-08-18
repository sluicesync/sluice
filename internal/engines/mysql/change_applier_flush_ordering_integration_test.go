//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeApplier_SkipFlushBeforePositionDurable is the MySQL sibling of the
// audit-PG-4 pin (the audit noted MySQL's flush call sites are "identically
// comment-held"): the H-4 at-least-once skip-count contract rests on
// flushSkippedTables running BEFORE the covering position becomes durable. With
// the skip ledger made un-writable, a skipped event must FAIL the apply AND
// leave the position where it was — never advanced past the skip.
func TestChangeApplier_SkipFlushBeforePositionDurable(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	applyMySQLApplier(t, dsn, `CREATE TABLE known_flushord (id BIGINT PRIMARY KEY)`)
	defer applyMySQLApplier(t, dsn, `DROP TABLE IF EXISTS known_flushord`)

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

	pos := func(tok string) ir.Position { return ir.Position{Engine: "mysql", Token: tok} }
	const p0 = `{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"}`
	const skipTok = `{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-9"}`

	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Insert{Schema: "target_db", Table: "known_flushord", Row: ir.Row{"id": int64(1)}, Position: pos(p0)},
	})
	got, ok, err := applier.ReadPosition(ctx, testStreamID)
	if err != nil || !ok || got.Token != p0 {
		t.Fatalf("baseline position = (%q, ok=%v, err=%v); want %q", got.Token, ok, err, p0)
	}

	// Make the skip-ledger UPSERT fail by dropping the ledger table.
	applyMySQLApplier(t, dsn, `DROP TABLE IF EXISTS `+appliershared.SkippedTablesTableName)

	ch := make(chan ir.Change, 1)
	ch <- ir.Insert{Schema: "target_db", Table: "ghost_flushord", Row: ir.Row{"id": int64(9)}, Position: pos(skipTok)}
	close(ch)
	if applyErr := applier.Apply(ctx, testStreamID, ch); applyErr == nil {
		t.Fatal("PG-4 sibling: Apply with an un-writable skip ledger returned nil — the flush failure was swallowed")
	}

	after, ok, err := applier.ReadPosition(ctx, testStreamID)
	if err != nil || !ok {
		t.Fatalf("ReadPosition after failed flush: ok=%v err=%v", ok, err)
	}
	if after.Token == skipTok {
		t.Fatalf("PG-4 sibling: position ADVANCED to the skipped event's token %q despite the flush failing — "+
			"flushSkippedTables ran AFTER the position write, breaking the at-least-once skip contract", skipTok)
	}
	if after.Token != p0 {
		t.Fatalf("PG-4 sibling: position = %q; want the pre-skip baseline %q", after.Token, p0)
	}
}
