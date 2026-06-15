//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0091 (F7a) — shared schema-introspection poll helpers for the
// per-shape × per-direction schema-forward integration matrix. These
// poll the target catalog (column presence, column data-type,
// nullability) so a forwarded DDL shape's convergence can be asserted
// without racing the apply loop. They mirror the tolerant polling style
// of waitForPGRowCount / waitForRowCountMySQL: any introspection error
// is treated as "not yet converged", so a poll during the startup or
// boundary-apply window doesn't spam fatals.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// waitForPGColumn polls the PG target's information_schema for the
// presence (want==true) or absence (want==false) of column on table in
// the public schema. Returns true once the observed presence matches
// want within timeout.
func waitForPGColumn(t *testing.T, db *sql.DB, table, column string, want bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sfPGColumnExists(db, table, column) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func sfPGColumnExists(db *sql.DB, table, column string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema='public' AND table_name=$1 AND column_name=$2
	`, table, column).Scan(&n); err != nil {
		return false
	}
	return n == 1
}

// waitForPGColumnType polls until the PG target column's data_type
// contains wantType (case-insensitive substring, e.g. "bigint"). Used
// to pin a forwarded ALTER COLUMN TYPE converged.
func waitForPGColumnType(t *testing.T, db *sql.DB, table, column, wantType string, timeout time.Duration) bool {
	t.Helper()
	want := strings.ToLower(wantType)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(strings.ToLower(pgColumnType(db, table, column)), want) {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func pgColumnType(db *sql.DB, table, column string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var dt string
	if err := db.QueryRowContext(ctx, `
		SELECT data_type FROM information_schema.columns
		WHERE table_schema='public' AND table_name=$1 AND column_name=$2
	`, table, column).Scan(&dt); err != nil {
		return ""
	}
	return dt
}

// waitForPGColumnNullable polls until the PG target column's
// is_nullable matches want ("YES"==nullable). Used to pin a forwarded
// ALTER COLUMN DROP/SET NOT NULL converged.
func waitForPGColumnNullable(t *testing.T, db *sql.DB, table, column string, wantNullable bool, timeout time.Duration) bool {
	t.Helper()
	want := "NO"
	if wantNullable {
		want = "YES"
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pgColumnNullable(db, table, column) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func pgColumnNullable(db *sql.DB, table, column string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n string
	if err := db.QueryRowContext(ctx, `
		SELECT is_nullable FROM information_schema.columns
		WHERE table_schema='public' AND table_name=$1 AND column_name=$2
	`, table, column).Scan(&n); err != nil {
		return ""
	}
	return n
}

// waitForMySQLColumn polls the MySQL target's information_schema for the
// presence (want==true) or absence (want==false) of column on table in
// the connection's current DATABASE().
func waitForMySQLColumn(t *testing.T, db *sql.DB, table, column string, want bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if mysqlColumnExists(db, table, column) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func mysqlColumnExists(db *sql.DB, table, column string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema=DATABASE() AND table_name=? AND column_name=?
	`, table, column).Scan(&n); err != nil {
		return false
	}
	return n == 1
}

// waitForMySQLColumnType polls until the MySQL target column's
// DATA_TYPE contains wantType (case-insensitive substring, e.g.
// "bigint"). Used to pin a forwarded ALTER COLUMN TYPE / MODIFY COLUMN.
func waitForMySQLColumnType(t *testing.T, db *sql.DB, table, column, wantType string, timeout time.Duration) bool {
	t.Helper()
	want := strings.ToLower(wantType)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(strings.ToLower(mysqlColumnType(db, table, column)), want) {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func mysqlColumnType(db *sql.DB, table, column string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var dt string
	if err := db.QueryRowContext(ctx, `
		SELECT DATA_TYPE FROM information_schema.columns
		WHERE table_schema=DATABASE() AND table_name=? AND column_name=?
	`, table, column).Scan(&dt); err != nil {
		return ""
	}
	return dt
}

// waitForMySQLColumnNullable polls until the MySQL target column's
// IS_NULLABLE matches want ("YES"==nullable).
func waitForMySQLColumnNullable(t *testing.T, db *sql.DB, table, column string, wantNullable bool, timeout time.Duration) bool {
	t.Helper()
	want := "NO"
	if wantNullable {
		want = "YES"
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if mysqlColumnNullable(db, table, column) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func mysqlColumnNullable(db *sql.DB, table, column string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n string
	if err := db.QueryRowContext(ctx, `
		SELECT IS_NULLABLE FROM information_schema.columns
		WHERE table_schema=DATABASE() AND table_name=? AND column_name=?
	`, table, column).Scan(&n); err != nil {
		return ""
	}
	return n
}

// waitForPGRowID polls until a widgets row with the given id exists on
// the PG target. Used instead of a total row-count so the prime-row
// INSERT (needed to push a PG DDL boundary through logical replication)
// doesn't perturb the assertion.
func waitForPGRowID(t *testing.T, db *sql.DB, table string, id int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sfRowIDExists(db, "SELECT COUNT(*) FROM "+table+" WHERE id=$1", id) {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// waitForMySQLRowID is the MySQL counterpart of waitForPGRowID.
func waitForMySQLRowID(t *testing.T, db *sql.DB, table string, id int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sfRowIDExists(db, "SELECT COUNT(*) FROM "+table+" WHERE id=?", id) {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func sfRowIDExists(db *sql.DB, query string, id int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, query, id).Scan(&n); err != nil {
		return false
	}
	return n >= 1
}

// waitForMySQLIndex polls until index idx exists (want==true) or is
// absent (want==false) on table in the connection's DATABASE(). Used to
// pin forwarded CREATE INDEX / DROP INDEX on a MySQL source (the only
// engine whose CDC projection carries index metadata — ADR-0091 §5b).
func waitForMySQLIndex(t *testing.T, db *sql.DB, table, idx string, want bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if mysqlIndexExists(db, table, idx) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func mysqlIndexExists(db *sql.DB, table, idx string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.statistics
		WHERE table_schema=DATABASE() AND table_name=? AND index_name=?
	`, table, idx).Scan(&n); err != nil {
		return false
	}
	return n >= 1
}

// waitForMySQLCheck polls until CHECK constraint chk exists (want==true)
// or is absent (want==false) on table. Used to pin forwarded ADD/DROP
// CHECK on a MySQL source.
func waitForMySQLCheck(t *testing.T, db *sql.DB, table, chk string, want bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if mysqlCheckExists(db, table, chk) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func mysqlCheckExists(db *sql.DB, table, chk string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.table_constraints
		WHERE table_schema=DATABASE() AND table_name=? AND constraint_name=?
		  AND constraint_type='CHECK'
	`, table, chk).Scan(&n); err != nil {
		return false
	}
	return n >= 1
}
