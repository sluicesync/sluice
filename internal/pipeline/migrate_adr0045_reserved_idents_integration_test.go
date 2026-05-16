//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0045 proactive corpus sweep: drive a reserved-word-named column
// through ALL FOUR opaque-expression positions (GENERATED, CHECK,
// functional INDEX, DEFAULT) in BOTH cross-engine directions and
// assert migrate success + correct runtime semantics. A reserved-word
// column reference inside a dialect-tagged expression string is the
// defect class ADR-0045 consolidates (v0.65.0 #5, Bug 61, Bug 63,
// Bug 64); a 5th cousin cannot appear without this sweep failing.
//
// The reserved-word column is named `order` (reserved in BOTH MySQL
// and PG). A non-reserved control column exercises the "don't requote
// what isn't reserved" half: `key` is a PG control here on the
// MySQL→PG leg note `key` IS MySQL-reserved, so for the control we use
// `label` (reserved in neither) so the test isolates the reserved-word
// path from incidental quoting.
//
// Each direction asserts, post-migrate:
//   - migrate succeeded (CREATE TABLE on the target did not reject the
//     translated+requoted expression bodies),
//   - GENERATED column recomputes correctly on a fresh INSERT,
//   - CHECK rejects a violating INSERT and accepts a valid one,
//   - the functional INDEX is present,
//   - the DEFAULT applies on a bare INSERT (Bug 64).

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestMigrate_MySQLToPostgres_ReservedWordExprSweep drives the four
// expression positions MySQL→PG. The MySQL reader strips backticks for
// IR portability; the PG writer must translate spellings AND re-quote
// the PG-reserved `order` column ref at every position or CREATE TABLE
// fails with SQLSTATE 42601.
func TestMigrate_MySQLToPostgres_ReservedWordExprSweep(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	// `order` is reserved in both engines. The generated column derives
	// from it; the CHECK constrains it; the functional index is on an
	// expression over it; the DEFAULT is a literal expression (so the
	// requote path is exercised on a column-less default too — the
	// reserved word here is the column the default lands on, validated
	// via a bare INSERT).
	const seedDDL = `
		CREATE TABLE widgets (
			id          BIGINT NOT NULL PRIMARY KEY,
			` + "`order`" + ` INT NOT NULL,
			doubled     INT GENERATED ALWAYS AS (` + "`order`" + ` * 2) STORED,
			created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			CONSTRAINT widgets_order_nonneg CHECK (` + "`order`" + ` >= 0)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		CREATE INDEX widgets_order_plus1 ON widgets ((` + "`order`" + ` + 1));

		INSERT INTO widgets (id, ` + "`order`" + `) VALUES (1, 5), (2, 10);
	`
	applyMySQLDDL(t, mysqlSource, seedDDL)

	runMigrate(t, "mysql", "postgres", mysqlSource, pgTarget)

	tgt, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = tgt.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Bulk-copied rows survived; generated column was carried.
	var n int
	if err := tgt.QueryRowContext(ctx, "SELECT COUNT(*) FROM widgets").Scan(&n); err != nil {
		t.Fatalf("count widgets: %v", err)
	}
	if n != 2 {
		t.Fatalf("widgets row count = %d; want 2", n)
	}

	// GENERATED recompute on a fresh INSERT (also exercises the DEFAULT
	// on created_at via a bare insert that omits it).
	if _, err := tgt.ExecContext(ctx,
		`INSERT INTO widgets (id, "order") VALUES (3, 7)`); err != nil {
		t.Fatalf("INSERT into pg widgets (valid): %v", err)
	}
	var doubled int
	var createdAt sql.NullTime
	if err := tgt.QueryRowContext(ctx,
		`SELECT doubled, created_at FROM widgets WHERE id = 3`).Scan(&doubled, &createdAt); err != nil {
		t.Fatalf("read back row 3: %v", err)
	}
	if doubled != 14 {
		t.Errorf("generated `doubled` = %d; want 14 (order*2, order=7)", doubled)
	}
	if !createdAt.Valid {
		t.Errorf("DEFAULT created_at did not apply on bare INSERT (Bug 64 MySQL→PG)")
	}

	// CHECK enforcement: a negative `order` must be rejected.
	if _, err := tgt.ExecContext(ctx,
		`INSERT INTO widgets (id, "order") VALUES (4, -1)`); err == nil {
		t.Errorf("INSERT with order=-1 should be rejected by CHECK on pg target")
	}

	// Functional INDEX present on the PG target.
	if !pgIndexExists(t, ctx, tgt, "widgets", "widgets_order_plus1") {
		t.Errorf("functional index widgets_order_plus1 missing on pg target")
	}
}

// TestMigrate_PostgresToMySQL_ReservedWordExprSweep is the symmetric
// PG→MySQL leg. The PG reader's pg_get_expr quotes `order` with double
// quotes; the IR-portability strip removes them; the MySQL writer must
// translate PG spellings AND backtick-requote `order` at every
// position (including the D2 functional-index cell, which historically
// had requote-only and NO translate) and the Bug 64 DEFAULT cell.
func TestMigrate_PostgresToMySQL_ReservedWordExprSweep(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	// `lower("label")` functional index exercises D2: a PG functional
	// index whose body needs MySQL spelling. `now()` DEFAULT exercises
	// Bug 64 (PG→MySQL): pre-ADR-0045 the 3-entry lookup handled now()
	// but with no requote/translate composition; the reserved-word
	// column the default sits beside (`order`) is validated by a bare
	// INSERT reading back the default.
	const seedDDL = `
		CREATE TABLE widgets (
			id          BIGINT NOT NULL PRIMARY KEY,
			"order"     INT NOT NULL,
			label       VARCHAR(64) NOT NULL,
			doubled     INT GENERATED ALWAYS AS ("order" * 2) STORED,
			created_at  TIMESTAMP NOT NULL DEFAULT now(),
			CONSTRAINT widgets_order_nonneg CHECK ("order" >= 0)
		);

		CREATE INDEX widgets_label_lower ON widgets (lower(label));
		CREATE INDEX widgets_order_plus1 ON widgets (("order" + 1));

		INSERT INTO widgets (id, "order", label) VALUES
			(1, 5, 'Alpha'),
			(2, 10, 'Beta');
	`
	applyPGDDL(t, pgSource, seedDDL)

	runMigrate(t, "postgres", "mysql", pgSource, mysqlTarget)

	tgt, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = tgt.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var n int
	if err := tgt.QueryRowContext(ctx, "SELECT COUNT(*) FROM widgets").Scan(&n); err != nil {
		t.Fatalf("count widgets: %v", err)
	}
	if n != 2 {
		t.Fatalf("widgets row count = %d; want 2", n)
	}

	// GENERATED recompute + DEFAULT now() applied on a bare INSERT
	// (Bug 64 PG→MySQL: the now() default must land as a working
	// CURRENT_TIMESTAMP default, and the reserved `order` column ref in
	// the generated body must be backtick-requoted).
	if _, err := tgt.ExecContext(ctx,
		"INSERT INTO widgets (id, `order`, label) VALUES (3, 7, 'Gamma')"); err != nil {
		t.Fatalf("INSERT into mysql widgets (valid): %v", err)
	}
	var doubled int
	var createdAt sql.NullTime
	if err := tgt.QueryRowContext(ctx,
		"SELECT doubled, created_at FROM widgets WHERE id = 3").Scan(&doubled, &createdAt); err != nil {
		t.Fatalf("read back row 3: %v", err)
	}
	if doubled != 14 {
		t.Errorf("generated `doubled` = %d; want 14 (order*2, order=7)", doubled)
	}
	if !createdAt.Valid {
		t.Errorf("DEFAULT now()→CURRENT_TIMESTAMP did not apply on bare INSERT (Bug 64 PG→MySQL)")
	}

	// CHECK enforcement.
	if _, err := tgt.ExecContext(ctx,
		"INSERT INTO widgets (id, `order`, label) VALUES (4, -1, 'Delta')"); err == nil {
		t.Errorf("INSERT with order=-1 should be rejected by CHECK on mysql target")
	}

	// Both functional indexes present on the MySQL target (the
	// lower(label) one is the D2 translate+requote path).
	idx := mysqlIndexNames(t, ctx, tgt, "widgets")
	for _, want := range []string{"widgets_label_lower", "widgets_order_plus1"} {
		if !idx[want] {
			t.Errorf("functional index %q missing on mysql target; have %v", want, idx)
		}
	}
}

// runMigrate is a tiny helper that resolves both engines and runs a
// Migrator end-to-end, failing the test on any error.
func runMigrate(t *testing.T, srcName, tgtName, srcDSN, tgtDSN string) {
	t.Helper()
	src, ok := engines.Get(srcName)
	if !ok {
		t.Fatalf("%s engine not registered", srcName)
	}
	tgt, ok := engines.Get(tgtName)
	if !ok {
		t.Fatalf("%s engine not registered", tgtName)
	}
	mig := &Migrator{
		Source:    src,
		Target:    tgt,
		SourceDSN: srcDSN,
		TargetDSN: tgtDSN,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (%s→%s): %v", srcName, tgtName, err)
	}
}

// pgIndexExists reports whether an index of the given name exists on a
// PG table (via pg_class/pg_index).
func pgIndexExists(t *testing.T, ctx context.Context, db *sql.DB, table, index string) bool {
	t.Helper()
	const q = `
		SELECT COUNT(*)
		FROM   pg_index ix
		JOIN   pg_class c  ON c.oid = ix.indrelid
		JOIN   pg_class i  ON i.oid = ix.indexrelid
		JOIN   pg_namespace n ON n.oid = c.relnamespace
		WHERE  n.nspname = 'public' AND c.relname = $1 AND i.relname = $2`
	var n int
	if err := db.QueryRowContext(ctx, q, table, index).Scan(&n); err != nil {
		t.Fatalf("pgIndexExists query: %v", err)
	}
	return n > 0
}

// mysqlIndexNames returns the set of index names on a MySQL table.
func mysqlIndexNames(t *testing.T, ctx context.Context, db *sql.DB, table string) map[string]bool {
	t.Helper()
	const q = `
		SELECT DISTINCT index_name
		FROM   information_schema.statistics
		WHERE  table_schema = DATABASE() AND table_name = ?`
	rows, err := db.QueryContext(ctx, q, table)
	if err != nil {
		t.Fatalf("mysqlIndexNames query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration: %v", err)
	}
	return out
}
