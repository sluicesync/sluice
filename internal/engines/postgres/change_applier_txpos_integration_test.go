//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestApply_PerChangePositionDeferredToTxCommit is the PG half of the
// CDCPOS-2 pin (audit 2026-08-11) — the MySQL applier's mirror, because the
// defect is a property of the SOURCE and this target is fed by every source:
// "a PG source's slot re-delivers whole transactions" is true and irrelevant
// for a MySQL file/pos source feeding this applier, where a persisted mid-
// transaction row position silently skips the transaction's tail on resume.
// The exact one-source-justification trap item 132 recorded.
//
// Same three arms as the MySQL pin: interrupted mid-transaction persists
// NOTHING (data ahead of position, ADR-0010 absorbs the replay); a complete
// transaction persists exactly the TxCommit's own position; an outside-tx
// change keeps the legacy position-with-data write.
func TestApply_PerChangePositionDeferredToTxCommit(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	applyPGApplier(t, dsn, `
		CREATE TABLE txpos (
			id BIGINT PRIMARY KEY,
			v  VARCHAR(32)
		);
	`)

	eng := Engine{}
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

	pos := func(tok string) ir.Position { return ir.Position{Engine: "postgres", Token: tok} }
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
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer func() { _ = db.Close() }()
		var n int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM txpos").Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	// Arm 1 — interrupted mid-transaction (the crash shape).
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.TxBegin{Position: pos("pre-tx")},
		ir.Insert{Position: pos("mid-tx"), Schema: "public", Table: "txpos", Row: ir.Row{"id": int64(1), "v": "a"}},
		ir.Insert{Position: pos("mid-tx"), Schema: "public", Table: "txpos", Row: ir.Row{"id": int64(2), "v": "b"}},
	})
	if got := rowCount(); got != 2 {
		t.Fatalf("interrupted tx applied %d rows; want 2 (data ahead of position is the SAFE direction)", got)
	}
	if tok, found := readPos(); found {
		t.Fatalf("interrupted mid-transaction PERSISTED position %q; want nothing persisted — pre-fix this held "+
			"the mid-tx row position and a MySQL file/pos source's resume silently skipped the transaction's "+
			"tail (CDCPOS-2)", tok)
	}

	// Arm 2 — complete transaction: only the TxCommit's own position lands.
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.TxBegin{Position: pos("pre-tx-2")},
		ir.Insert{Position: pos("mid-tx-2"), Schema: "public", Table: "txpos", Row: ir.Row{"id": int64(3), "v": "c"}},
		ir.TxCommit{Position: pos("post-commit-2")},
	})
	tok, found := readPos()
	if !found || tok != "post-commit-2" {
		t.Fatalf("after a complete transaction position = (%q, found=%v); want the TxCommit's own post-commit-2 "+
			"and never a mid-tx token", tok, found)
	}

	// Arm 3 — outside any transaction: the legacy position-with-data write.
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Insert{Position: pos("bare-4"), Schema: "public", Table: "txpos", Row: ir.Row{"id": int64(4), "v": "d"}},
	})
	tok, found = readPos()
	if !found || tok != "bare-4" {
		t.Fatalf("outside-tx change position = (%q, found=%v); want bare-4 (the unchanged legacy path)", tok, found)
	}
	if got := rowCount(); got != 4 {
		t.Fatalf("final row count %d; want 4", got)
	}
}
