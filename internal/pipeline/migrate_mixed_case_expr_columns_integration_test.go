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
	if target == "mysql" {
		// The keyword before a unary-minus remove-string, which MySQL stores
		// as trim(both -(1) from code), beside an INT column named both: read
		// as the column it becomes a subtraction, silently (review finding F2).
		// MySQL target only: Postgres has no numeric remove-string, so there
		// the correct reading is a loud 42883 either way.
		applyMySQLDDL(t, mysqlSource, strings.ReplaceAll(`
		CREATE TABLE trim_neg (
			id     INT NOT NULL PRIMARY KEY,
			code   VARCHAR(20),
			~both~ INT,
			g_neg  VARCHAR(20) AS (trim(both -1 from code)) STORED
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO trim_neg (id, code, ~both~) VALUES (1, '-1a-1', 5), (2, '-1-1x', 2);
	`, "~", "`"))
	}

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

	compareGeneratedWithSource(ctx, t, src, tgt, "trim_words", 3, "g_kw", "g_col", "g_mix", "g_lead", "g_one")
	if target == "mysql" {
		compareGeneratedWithSource(ctx, t, src, tgt, "trim_neg", 2, "g_neg")
	}
}

// TestMigrate_PostgresToMySQL_TrimKeywordForms is the Bug 311 review's
// finding F1 on real servers. Postgres prints trim(x) as
// `TRIM(BOTH FROM x)` (and ltrim-by-keyword as `TRIM(LEADING FROM x)`),
// a form MySQL and MariaDB never store, so the positional rule a MySQL
// source's de-quoted text needs would read the keyword as a column on a
// MySQL target: a refusal (`Unknown column 'BOTH'`) for a table without
// such a column, and a silent wrong trim for one with it. Postgres quotes
// every column of these names, so its text takes the keyword reading.
// The table carries a `both` column to make the wrong reading silent, and
// the values are compared with what Postgres computed.
func TestMigrate_PostgresToMySQL_TrimKeywordForms(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, `
		CREATE TABLE trim_pg (
			id     INT PRIMARY KEY,
			code   VARCHAR(20),
			"both" VARCHAR(5),
			g_trim VARCHAR(20) GENERATED ALWAYS AS (trim(code)) STORED,
			g_lead VARCHAR(20) GENERATED ALWAYS AS (trim(leading from code)) STORED,
			g_col  VARCHAR(20) GENERATED ALWAYS AS (trim(both "both" from code)) STORED,
			CONSTRAINT code_not_blank CHECK (trim(leading from code) <> '')
		);
		CREATE INDEX trim_pg_code ON trim_pg ((trim(both from code)));
		INSERT INTO trim_pg (id, code, "both") VALUES (1, '  aba  ', 'a'), (2, 'aba', 'a');
	`)

	runMigrate(t, "postgres", "mysql", pgSource, mysqlTarget)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src, err := sql.Open("pgx", pgSource)
	if err != nil {
		t.Fatalf("open pg source: %v", err)
	}
	defer func() { _ = src.Close() }()
	tgt, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = tgt.Close() }()

	compareGeneratedWithSource(ctx, t, src, tgt, "trim_pg", 2, "g_trim", "g_lead", "g_col")
	// The CHECK landed and still means trim-then-compare: an all-blank code
	// is refused, and a padded one is accepted.
	if _, err := tgt.ExecContext(ctx, "INSERT INTO trim_pg (id, code) VALUES (3, '   ')"); err == nil {
		t.Errorf("an all-blank code was accepted; the CHECK did not land as trim(leading from code) <> ''")
	}
	if _, err := tgt.ExecContext(ctx, "INSERT INTO trim_pg (id, code) VALUES (4, ' x ')"); err != nil {
		t.Errorf("a padded code was refused: %v", err)
	}
}

// compareGeneratedWithSource reads id plus cols from table on both sides and
// requires every target value to equal the value the source server
// computed — the independent expected value for a translated expression.
func compareGeneratedWithSource(ctx context.Context, t *testing.T, src, tgt *sql.DB, table string, wantRows int, cols ...string) {
	t.Helper()
	q := "SELECT id, " + strings.Join(cols, ", ") + " FROM " + table + " ORDER BY id"
	read := func(db *sql.DB, side string) map[int][]sql.NullString {
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			t.Fatalf("read %s on the %s: %v", table, side, err)
		}
		defer func() { _ = rows.Close() }()
		out := map[int][]sql.NullString{}
		for rows.Next() {
			var id int
			v := make([]sql.NullString, len(cols))
			dest := []any{&id}
			for i := range v {
				dest = append(dest, &v[i])
			}
			if err := rows.Scan(dest...); err != nil {
				t.Fatalf("scan %s on the %s: %v", table, side, err)
			}
			out[id] = v
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows %s on the %s: %v", table, side, err)
		}
		return out
	}
	want, got := read(src, "source"), read(tgt, "target")
	if len(want) != wantRows {
		t.Fatalf("%s: source rows = %d; want %d", table, len(want), wantRows)
	}
	for id, w := range want {
		g, ok := got[id]
		if !ok {
			t.Errorf("%s row %d missing on the target", table, id)
			continue
		}
		for i := range w {
			if g[i] != w[i] {
				t.Errorf("%s row %d %s = %q on the target; the source computed %q", table, id, cols[i], g[i].String, w[i].String)
			}
		}
	}
}
