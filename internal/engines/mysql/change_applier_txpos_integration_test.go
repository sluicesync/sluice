//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestApply_PerChangePositionDeferredToTxCommit is the CDCPOS-2 pin (audit
// 2026-08-11): the per-change Apply path must never persist a position
// mid-source-transaction. In file/pos mode every row of one RowsEvent
// carries the event's END LogPos, so the pre-fix per-row persist meant a
// crash between row K and K+1 resumed PAST the un-applied remainder — a
// silent skip on the documented-conservative --apply-batch-size=1 +
// gtid_mode=OFF (MySQL's default) combination.
//
// Three arms, one applier, one real server:
//   - interrupted mid-transaction: rows applied, NOTHING persisted — the
//     resume point stays at the last commit, so the whole transaction
//     re-delivers (data-ahead-of-position, the direction ADR-0010 absorbs).
//     RED pre-fix: the mid-tx row position was persisted and a resume
//     silently skipped the tail.
//   - complete transaction: exactly the TxCommit's own (post-commit)
//     position persists.
//   - a change OUTSIDE any transaction: the legacy position-with-data write,
//     byte-identical to before.
func TestApply_PerChangePositionDeferredToTxCommit(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	applyMySQLApplier(t, dsn, `
		CREATE TABLE txpos (
			id BIGINT NOT NULL,
			v  VARCHAR(32),
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	pos := func(tok string) ir.Position { return ir.Position{Engine: "mysql", Token: tok} }
	readPos := func() (string, bool) {
		t.Helper()
		p, found, err := applier.ReadPosition(ctx, testStreamID)
		if err != nil {
			t.Fatalf("ReadPosition: %v", err)
		}
		return p.Token, found
	}
	rowCount := func() int {
		t.Helper()
		db, err := openDB(ctx, mustParseDSN(t, dsn), nil)
		if err != nil {
			t.Fatalf("openDB: %v", err)
		}
		defer db.Close()
		var n int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM txpos").Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	// Arm 1 — interrupted mid-transaction: TxBegin + two rows, channel closes
	// with NO TxCommit (the crash shape; every row carries the same mid-tx
	// position a RowsEvent would stamp).
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.TxBegin{Position: pos("pre-tx")},
		ir.Insert{Position: pos("mid-tx"), Schema: "target_db", Table: "txpos", Row: ir.Row{"id": int64(1), "v": "a"}},
		ir.Insert{Position: pos("mid-tx"), Schema: "target_db", Table: "txpos", Row: ir.Row{"id": int64(2), "v": "b"}},
	})
	if got := rowCount(); got != 2 {
		t.Fatalf("interrupted tx applied %d rows; want 2 (data ahead of position is the SAFE direction)", got)
	}
	if tok, found := readPos(); found {
		t.Fatalf("interrupted mid-transaction PERSISTED position %q; want nothing persisted — pre-fix this held "+
			"the mid-tx row position and a resume silently skipped the transaction's tail (CDCPOS-2)", tok)
	}

	// Arm 2 — complete transaction: only the TxCommit's own position lands.
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.TxBegin{Position: pos("pre-tx-2")},
		ir.Insert{Position: pos("mid-tx-2"), Schema: "target_db", Table: "txpos", Row: ir.Row{"id": int64(3), "v": "c"}},
		ir.TxCommit{Position: pos("post-commit-2")},
	})
	tok, found := readPos()
	if !found || tok != "post-commit-2" {
		t.Fatalf("after a complete transaction position = (%q, found=%v); want the TxCommit's own post-commit-2 "+
			"and never a mid-tx token", tok, found)
	}

	// Arm 3 — outside any transaction: the legacy position-with-data write.
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Insert{Position: pos("bare-4"), Schema: "target_db", Table: "txpos", Row: ir.Row{"id": int64(4), "v": "d"}},
	})
	tok, found = readPos()
	if !found || tok != "bare-4" {
		t.Fatalf("outside-tx change position = (%q, found=%v); want bare-4 (the unchanged legacy path)", tok, found)
	}
	if got := rowCount(); got != 4 {
		t.Fatalf("final row count %d; want 4", got)
	}
}
