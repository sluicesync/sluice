//go:build d1verify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestD1Verify_RowidShadowedColumn replays audit 2026-09-01 LA-1's exact
// table on REAL Cloudflare D1: a PK-less `shadow(rowid TEXT, v INTEGER)` with
// 1,500 rows at rowid='a' and 1,000 at rowid='b', read at the production page
// size (1,000 → three pages). The HEAD binary the audit ran delivered 2,000 of
// the 2,500 rows at exit 0 on both the direct reader and `--stage-local`; the
// fix keys the pages on `_rowid_` (the name the table does not shadow) and the
// LA-3 COUNT(*) bracket refuses any short read. Both shapes must deliver every
// row exactly once.
//
// Ground truth is independent of the reader under test: the seed is two
// server-side recursive-CTE INSERTs, and the expected total is re-probed with a
// raw COUNT(*) through queryRows before the reader runs.
//
// Same lifecycle as the other d1verify tests — a throwaway database (named
// `la1-<nanos>` so a leaked one is recognisable) created and deleted via the
// REST API; skip-clean without credentials.
func TestD1Verify_RowidShadowedColumn(t *testing.T) {
	account, token := d1AdvCreds(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	dbID := createThrowawayD1DatabaseNamed(ctx, t, account, token, fmt.Sprintf("la1-%d", time.Now().UnixNano()))
	client, err := openD1Client("d1://" + account + "/" + dbID)
	if err != nil {
		t.Fatalf("openD1Client: %v", err)
	}

	d1AdvExec(ctx, t, client, `CREATE TABLE shadow (rowid TEXT, v INTEGER)`)
	d1AdvExec(ctx, t, client, `WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 1500)
		INSERT INTO shadow (rowid, v) SELECT 'a', x FROM n`)
	d1AdvExec(ctx, t, client, `WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 1000)
		INSERT INTO shadow (rowid, v) SELECT 'b', 1500 + x FROM n`)

	// Anti-vacuity: the server holds 2,500 rows and the user column really is
	// the one `rowid` resolves to (two distinct values, not 2,500 rowids).
	probe, err := client.queryRows(ctx, `SELECT CAST(COUNT(*) AS TEXT) AS n, CAST(COUNT(DISTINCT rowid) AS TEXT) AS d FROM shadow`)
	if err != nil || len(probe) != 1 {
		t.Fatalf("probe: %v (%d rows)", err, len(probe))
	}
	if n, _, _ := jsonString(probe[0]["n"]); n != "2500" {
		t.Fatalf("seed: server COUNT(*) = %s; want 2500", n)
	}
	if d, _, _ := jsonString(probe[0]["d"]); d != "2" {
		t.Fatalf("seed: COUNT(DISTINCT rowid) = %s; want 2 — the user column is not shadowing the rowid, so this test is not exercising LA-1", d)
	}

	table := d1AdvFindTable(ctx, t, client, "shadow")

	t.Run("direct", func(t *testing.T) {
		reader := &D1RowReader{client: client} // production page size: 1,000 → the audit's three pages
		rows := d1AdvReadAll(ctx, t, reader, table)
		if len(rows) != 2500 {
			t.Fatalf("direct reader delivered %d rows; want 2500 (LA-1 delivered 2000 at exit 0)", len(rows))
		}
		seen := make(map[int64]bool, len(rows))
		for i, row := range rows {
			v, ok := row["v"].(int64)
			if !ok || seen[v] {
				t.Fatalf("row %d: v = %#v (duplicate or non-integer)", i, row["v"])
			}
			seen[v] = true
		}
	})

	t.Run("stage-local", func(t *testing.T) {
		dest := filepath.Join(t.TempDir(), "stage.db")
		if err := stageD1ClientToLocalFile(ctx, client, dest, nil); err != nil {
			t.Fatalf("stage: %v", err)
		}
		db, err := sql.Open("sqlite", dest)
		if err != nil {
			t.Fatalf("open staged: %v", err)
		}
		defer func() { _ = db.Close() }()
		var n, d int64
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COUNT(DISTINCT v) FROM shadow`).Scan(&n, &d); err != nil {
			t.Fatalf("count staged: %v", err)
		}
		if n != 2500 || d != 2500 {
			t.Fatalf("staged file holds %d rows (%d distinct); want 2500/2500", n, d)
		}
	})
}
