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
