//go:build integration

// Integration tests for the CHECK-constraint path: the schema reader
// captures the CHECK clause, the DDL writer recreates it on the
// target, and the constraint actually rejects invalid values.
//
// Translation policy is verbatim passthrough — the expression text
// is preserved as-is. These tests use portable expressions
// (`qty >= 0`, `status IN ('open','closed','cancelled')`) that are
// valid in both MySQL and Postgres after identifier-quote
// normalization at the read boundary; non-portable expressions
// (e.g. MySQL's IF(...) versus PG's CASE) would fail loudly on the
// target by design, not silently.
//
// Each test verifies four things post-migrate:
//  1. The target's information_schema reports the CHECK is present.
//  2. Bulk-copied rows that satisfied the source's CHECK are present
//     on the target (CHECK didn't reject valid pre-existing data).
//  3. An INSERT that violates the CHECK is rejected by the target —
//     the constraint is enforced, not just declared.
//  4. An INSERT that satisfies the CHECK is accepted.

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

// TestMigrate_MySQLToMySQL_CheckConstraints verifies that both column-
// scoped and table-scoped CHECKs survive a MySQL→MySQL migrate, that
// the target enforces them, and that pre-existing valid rows pass
// through.
func TestMigrate_MySQLToMySQL_CheckConstraints(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	// Mix column-scoped (`CHECK (qty >= 0)`) and table-scoped
	// (`CONSTRAINT ... CHECK (...)`) declarations — MySQL stores
	// both as table-level entries, so both should land identically.
	const seedDDL = `
		CREATE TABLE orders (
			id     BIGINT      NOT NULL PRIMARY KEY,
			status VARCHAR(20) NOT NULL CHECK (status IN ('open', 'closed', 'cancelled')),
			qty    INT         NOT NULL,
			CONSTRAINT orders_qty_nonneg CHECK (qty >= 0)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO orders (id, status, qty) VALUES
			(1, 'open',   5),
			(2, 'closed', 0);
	`
	applyMySQLDDL(t, sourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	mig := &Migrator{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgt, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgt.Close() }()

	// 1. Both CHECKs are present on the target. MySQL exposes table-
	// scoped CHECKs via information_schema.check_constraints joined
	// against table_constraints (the same shape the schema reader
	// uses). We expect at least two rows for the `orders` table.
	checkNames := readMySQLCheckNames(t, ctx, tgt, "orders")
	if len(checkNames) < 2 {
		t.Fatalf("target check_constraints for orders: got %d (%v); want >= 2",
			len(checkNames), checkNames)
	}

	// 2. Pre-existing valid rows are present.
	assertOrdersRowCount(t, ctx, tgt, 2)

	// 3. Constraint enforcement: invalid status is rejected.
	if _, err := tgt.ExecContext(ctx,
		`INSERT INTO orders (id, status, qty) VALUES (3, 'bogus', 1)`); err == nil {
		t.Errorf("INSERT with invalid status should have been rejected by target CHECK")
	}
	// 3b. Invalid qty is rejected.
	if _, err := tgt.ExecContext(ctx,
		`INSERT INTO orders (id, status, qty) VALUES (4, 'open', -1)`); err == nil {
		t.Errorf("INSERT with negative qty should have been rejected by target CHECK")
	}

	// 4. A valid row is accepted.
	if _, err := tgt.ExecContext(ctx,
		`INSERT INTO orders (id, status, qty) VALUES (5, 'open', 3)`); err != nil {
		t.Errorf("INSERT with valid values should have been accepted; got: %v", err)
	}
}

// TestMigrate_PostgresToPostgres_CheckConstraints mirrors the MySQL
// same-engine test. The same source DDL (modulo identifier quoting)
// is portable to PG, so the assertions are identical in shape.
func TestMigrate_PostgresToPostgres_CheckConstraints(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE orders (
			id     BIGINT      NOT NULL PRIMARY KEY,
			status VARCHAR(20) NOT NULL CHECK (status IN ('open', 'closed', 'cancelled')),
			qty    INTEGER     NOT NULL,
			CONSTRAINT orders_qty_nonneg CHECK (qty >= 0)
		);

		INSERT INTO orders (id, status, qty) VALUES
			(1, 'open',   5),
			(2, 'closed', 0);
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgt, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgt.Close() }()

	// 1. Both CHECKs are present. We query pg_constraint directly
	// rather than information_schema.check_constraints so the
	// implicit `<col> IS NOT NULL` synthetic entries that
	// information_schema includes don't pollute the count — same
	// reasoning the schema reader uses.
	checkNames := readPGCheckNames(t, ctx, tgt, "orders")
	if len(checkNames) < 2 {
		t.Fatalf("target pg_constraint contype='c' for orders: got %d (%v); want >= 2",
			len(checkNames), checkNames)
	}

	// 2. Pre-existing valid rows are present.
	assertOrdersRowCount(t, ctx, tgt, 2)

	// 3. Constraint enforcement: invalid status is rejected.
	if _, err := tgt.ExecContext(ctx,
		`INSERT INTO orders (id, status, qty) VALUES (3, 'bogus', 1)`); err == nil {
		t.Errorf("INSERT with invalid status should have been rejected by target CHECK")
	}
	// 3b. Invalid qty is rejected.
	if _, err := tgt.ExecContext(ctx,
		`INSERT INTO orders (id, status, qty) VALUES (4, 'open', -1)`); err == nil {
		t.Errorf("INSERT with negative qty should have been rejected by target CHECK")
	}

	// 4. A valid row is accepted.
	if _, err := tgt.ExecContext(ctx,
		`INSERT INTO orders (id, status, qty) VALUES (5, 'open', 3)`); err != nil {
		t.Errorf("INSERT with valid values should have been accepted; got: %v", err)
	}
}

// TestMigrate_MySQLToPostgres_CheckConstraints exercises the cross-
// engine path. Both `qty >= 0` and `status IN ('open','closed','cancelled')`
// are portable across dialects after identifier-quote normalization,
// so verbatim passthrough lands working CHECKs on the PG target.
//
// MySQL stores the parsed-and-reformatted expression with backtick-
// quoted identifiers; the schema reader strips them at the read
// boundary so the PG parser doesn't choke. Without that
// normalization this test would fail at CREATE TABLE on the target.
func TestMigrate_MySQLToPostgres_CheckConstraints(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE orders (
			id     BIGINT      NOT NULL PRIMARY KEY,
			status VARCHAR(20) NOT NULL CHECK (status IN ('open', 'closed', 'cancelled')),
			qty    INT         NOT NULL,
			CONSTRAINT orders_qty_nonneg CHECK (qty >= 0)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO orders (id, status, qty) VALUES
			(1, 'open',   5),
			(2, 'closed', 0);
	`
	applyMySQLDDL(t, mysqlSource, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: mysqlSource,
		TargetDSN: pgTarget,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgt, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = tgt.Close() }()

	// 1. CHECKs landed on the PG target.
	checkNames := readPGCheckNames(t, ctx, tgt, "orders")
	if len(checkNames) < 2 {
		t.Fatalf("pg target pg_constraint contype='c' for orders: got %d (%v); want >= 2",
			len(checkNames), checkNames)
	}

	// 2. Pre-existing valid rows are present.
	assertOrdersRowCount(t, ctx, tgt, 2)

	// 3. Constraint enforcement on PG.
	if _, err := tgt.ExecContext(ctx,
		`INSERT INTO orders (id, status, qty) VALUES (3, 'bogus', 1)`); err == nil {
		t.Errorf("INSERT with invalid status should have been rejected on pg target")
	}
	if _, err := tgt.ExecContext(ctx,
		`INSERT INTO orders (id, status, qty) VALUES (4, 'open', -1)`); err == nil {
		t.Errorf("INSERT with negative qty should have been rejected on pg target")
	}
	if _, err := tgt.ExecContext(ctx,
		`INSERT INTO orders (id, status, qty) VALUES (5, 'open', 3)`); err != nil {
		t.Errorf("INSERT with valid values should have been accepted on pg target; got: %v", err)
	}
}

// readMySQLCheckNames returns the names of CHECK constraints on a
// table by joining check_constraints + table_constraints — the same
// shape the schema reader uses, so this stays in sync with the read
// path.
func readMySQLCheckNames(t *testing.T, ctx context.Context, db *sql.DB, table string) []string {
	t.Helper()
	const q = `
		SELECT cc.constraint_name
		FROM   information_schema.check_constraints cc
		JOIN   information_schema.table_constraints  tc
		  ON   tc.constraint_schema = cc.constraint_schema
		 AND   tc.constraint_name   = cc.constraint_name
		WHERE  tc.table_schema    = DATABASE()
		  AND  tc.table_name      = ?
		  AND  tc.constraint_type = 'CHECK'
		ORDER  BY cc.constraint_name`
	rows, err := db.QueryContext(ctx, q, table)
	if err != nil {
		t.Fatalf("read mysql check names: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration: %v", err)
	}
	return out
}

// readPGCheckNames returns the names of CHECK constraints on a table
// using pg_constraint directly. We avoid information_schema.
// check_constraints because PG synthesizes implicit `<col> IS NOT
// NULL` rows there; pg_constraint with contype='c' surfaces only
// user-declared CHECKs.
func readPGCheckNames(t *testing.T, ctx context.Context, db *sql.DB, table string) []string {
	t.Helper()
	const q = `
		SELECT con.conname
		FROM   pg_constraint con
		JOIN   pg_class      cl ON cl.oid = con.conrelid
		JOIN   pg_namespace  n  ON n.oid  = cl.relnamespace
		WHERE  n.nspname    = 'public'
		  AND  cl.relname   = $1
		  AND  con.contype  = 'c'
		ORDER  BY con.conname`
	rows, err := db.QueryContext(ctx, q, table)
	if err != nil {
		t.Fatalf("read pg check names: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration: %v", err)
	}
	return out
}

// assertOrdersRowCount checks the number of rows in the orders
// table. Used to verify bulk-copied rows survived the migrate.
func assertOrdersRowCount(t *testing.T, ctx context.Context, db *sql.DB, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM orders").Scan(&got); err != nil {
		t.Fatalf("count orders: %v", err)
	}
	if got != want {
		t.Errorf("orders row count = %d; want %d", got, want)
	}
}
