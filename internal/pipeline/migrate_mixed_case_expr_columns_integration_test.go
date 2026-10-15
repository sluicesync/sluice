//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestMigrate_MySQLToPostgres_MixedCaseColumnExpressions is GitHub #356 on
// real servers: MySQL resolves a column reference inside a stored
// expression case-insensitively and keeps the expression text as typed,
// so a functional index can say `lower(status)` over a column named
// `STATUS`. The reader strips the backticks, and Postgres folds a bare
// identifier to lower case, so before the fix every expression position
// naming a non-lower-case column failed on Postgres with SQLSTATE 42703:
//
//   - the functional index, AFTER the full copy (the report: the target
//     held every row and the run never reached CDC);
//   - a functional index spelling the column exactly as declared
//     (`lower(STATUS)`) — the fold does not care that the case matched;
//   - the generated column and the CHECK, at CREATE TABLE.
//
// The control column `plain_col` is lower case and must stay bare.
func TestMigrate_MySQLToPostgres_MixedCaseColumnExpressions(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, mysqlSource, `
		CREATE TABLE DCS_DIS_LOGIC_SETTING (
			id        BIGINT NOT NULL PRIMARY KEY,
			PLANT     VARCHAR(100),
			STATUS    VARCHAR(20),
			Price     INT NOT NULL,
			Qty       INT NOT NULL,
			plain_col VARCHAR(20),
			Total     INT GENERATED ALWAYS AS (Price * qty) STORED,
			CONSTRAINT qty_nonneg CHECK (QTY >= 0)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		CREATE INDEX PLANT_IDX     ON DCS_DIS_LOGIC_SETTING (plant, (lower(status)));
		CREATE INDEX STATUS_EXACT  ON DCS_DIS_LOGIC_SETTING ((lower(STATUS)));
		CREATE INDEX PRICE_PLUS    ON DCS_DIS_LOGIC_SETTING ((Price + 1));
		CREATE INDEX PLAIN_LOWER   ON DCS_DIS_LOGIC_SETTING ((lower(plain_col)));

		INSERT INTO DCS_DIS_LOGIC_SETTING (id, PLANT, STATUS, Price, Qty, plain_col)
		VALUES (1, 'p1', 'ON', 2, 3, 'x'), (2, 'p2', 'OFF', 5, 7, 'y');
	`)

	runMigrate(t, "mysql", "postgres", mysqlSource, pgTarget)

	tgt, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = tgt.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var n int
	if err := tgt.QueryRowContext(ctx, `SELECT COUNT(*) FROM "DCS_DIS_LOGIC_SETTING"`).Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if n != 2 {
		t.Fatalf("row count = %d; want 2", n)
	}

	// The generated column computes over the right columns, for copied
	// rows and for a fresh insert.
	var total int
	if err := tgt.QueryRowContext(ctx, `SELECT "Total" FROM "DCS_DIS_LOGIC_SETTING" WHERE id = 2`).Scan(&total); err != nil {
		t.Fatalf("read generated column: %v", err)
	}
	if total != 35 {
		t.Errorf("Total for row 2 = %d; want 35 (Price 5 * Qty 7)", total)
	}
	if _, err := tgt.ExecContext(ctx,
		`INSERT INTO "DCS_DIS_LOGIC_SETTING" (id, "Price", "Qty") VALUES (3, 4, 6)`); err != nil {
		t.Fatalf("valid insert: %v", err)
	}
	if err := tgt.QueryRowContext(ctx, `SELECT "Total" FROM "DCS_DIS_LOGIC_SETTING" WHERE id = 3`).Scan(&total); err != nil {
		t.Fatalf("read generated column for row 3: %v", err)
	}
	if total != 24 {
		t.Errorf("Total for row 3 = %d; want 24", total)
	}

	// The CHECK enforces over "Qty".
	if _, err := tgt.ExecContext(ctx,
		`INSERT INTO "DCS_DIS_LOGIC_SETTING" (id, "Price", "Qty") VALUES (4, 1, -1)`); err == nil {
		t.Errorf("an insert with Qty = -1 was accepted; the CHECK did not land over \"Qty\"")
	}

	// Every functional index exists and indexes the intended column.
	for _, c := range []struct{ index, want string }{
		{"PLANT_IDX", `"STATUS"`},
		{"STATUS_EXACT", `"STATUS"`},
		{"PRICE_PLUS", `"Price"`},
		{"PLAIN_LOWER", `plain_col`},
	} {
		var def string
		err := tgt.QueryRowContext(ctx,
			`SELECT indexdef FROM pg_indexes WHERE tablename = 'DCS_DIS_LOGIC_SETTING' AND indexname LIKE '%' || $1`,
			c.index).Scan(&def)
		if err != nil {
			t.Errorf("index %s missing on the target: %v", c.index, err)
			continue
		}
		if !strings.Contains(def, c.want) {
			t.Errorf("index %s = %q; want it over %s", c.index, def, c.want)
		}
	}
}

// TestMigrate_MySQLSource_TrimKeywordColumns is Bug 311 on real servers:
// a column literally named `both`, `leading` or `trailing` used inside
// TRIM(...). MySQL's stored text tells the column (backticked) from the
// keyword (bare), but the read boundary strips the backticks, so the
// Postgres writer must tell them apart by position. Each wrong answer is
// valid Postgres that trims different characters, so the generated values
// are compared with the values the SOURCE server computed — the independent
// expected value — rather than with a spelling. v0.160.1 left the column
// bare (`TRIM(BOTH FROM code)`), so g_col trimmed spaces instead of 'b' at
// exit 0, and g_one failed with 42601.
//
// The mysql leg is the sibling on the MySQL writer, which quoted the KEYWORD
// (`trim(`both` 'x' from code)`, a syntax error) before the same rule.
func TestMigrate_MySQLSource_TrimKeywordColumns(t *testing.T) {
	for _, target := range []string{"postgres", "mysql"} {
		t.Run(target, func(t *testing.T) { testTrimKeywordColumns(t, target) })
	}
}

func testTrimKeywordColumns(t *testing.T, target string) {
	mysqlSource, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	tgtDriver, tgtDSN := "mysql", mysqlTarget
	if target == "postgres" {
		_, pgTarget, pgCleanup := startPostgres(t)
		defer pgCleanup()
		tgtDriver, tgtDSN = "pgx", pgTarget
	}

	// MySQL identifier quotes cannot sit in a Go raw string; ~ stands for one.
	applyMySQLDDL(t, mysqlSource, strings.ReplaceAll(`
		CREATE TABLE trim_words (
			id         INT NOT NULL PRIMARY KEY,
			code       VARCHAR(20),
			~both~     VARCHAR(5),
			~Leading~  VARCHAR(5),
			~trailing~ VARCHAR(10),
			g_kw   VARCHAR(20) AS (trim(both 'x' from code)) STORED,
			g_col  VARCHAR(20) AS (trim(~both~ from code)) STORED,
			g_mix  VARCHAR(20) AS (trim(both ~both~ from code)) STORED,
			g_lead VARCHAR(20) AS (trim(leading ~Leading~ from code)) STORED,
			g_one  VARCHAR(20) AS (concat(trim(~trailing~), '!')) STORED
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO trim_words (id, code, ~both~, ~Leading~, ~trailing~)
		VALUES (1, 'xbabx', 'b', 'x', '  t  '), (2, 'bbabb', 'b', 'b', 't'), (3, '  bab  ', 'b', ' ', ' tt');
	`, "~", "`"))

	runMigrate(t, "mysql", target, mysqlSource, tgtDSN)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src, err := sql.Open("mysql", mysqlSource)
	if err != nil {
		t.Fatalf("open mysql source: %v", err)
	}
	defer func() { _ = src.Close() }()
	tgt, err := sql.Open(tgtDriver, tgtDSN)
	if err != nil {
		t.Fatalf("open %s target: %v", target, err)
	}
	defer func() { _ = tgt.Close() }()

	read := func(db *sql.DB, q string) map[int][5]string {
		t.Helper()
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			t.Fatalf("read generated columns: %v", err)
		}
		defer func() { _ = rows.Close() }()
		out := map[int][5]string{}
		for rows.Next() {
			var id int
			var v [5]string
			if err := rows.Scan(&id, &v[0], &v[1], &v[2], &v[3], &v[4]); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out[id] = v
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows: %v", err)
		}
		return out
	}
	want := read(src, "SELECT id, g_kw, g_col, g_mix, g_lead, g_one FROM trim_words ORDER BY id")
	got := read(tgt, `SELECT id, g_kw, g_col, g_mix, g_lead, g_one FROM trim_words ORDER BY id`)
	if len(want) != 3 {
		t.Fatalf("source rows = %d; want 3", len(want))
	}
	names := [5]string{"g_kw", "g_col", "g_mix", "g_lead", "g_one"}
	for id, w := range want {
		g, ok := got[id]
		if !ok {
			t.Errorf("row %d missing on the target", id)
			continue
		}
		for i := range w {
			if g[i] != w[i] {
				t.Errorf("row %d %s = %q on the target; the source computed %q", id, names[i], g[i], w[i])
			}
		}
	}
}
