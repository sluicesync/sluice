//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-22 end to end: a SQLite source table carrying an inline `UNIQUE` (and a
// table-level `UNIQUE (a, b)`) migrates to a SQLite, a Postgres and a MySQL
// target, and every target ENFORCES the uniqueness — a duplicate insert is
// refused. Before the fix the reader carried SQLite's `sqlite_autoindex_<t>_N`
// auto-index by its reserved name, and the SQLite target refused to create it
// ("object name reserved for internal use") in the index phase, after the
// whole copy. PG and MySQL accepted it (PG under a table-prefixed rename), so
// those two legs are the "still lands a UNIQUE" regression guard, and the PG
// leg additionally asserts it landed as a CONSTRAINT (pg_constraint contype
// 'u'), the shape the source declared.
//
// The independent expected value on every leg is the target refusing a row
// the source would refuse — not the presence of an index sluice says it made.
//
// The MySQL leg keys its UNIQUEs on INTEGER columns. That is not a
// convenience: the SQLite reader carries every text column as an unbounded
// ir.Text (SQLite enforces no declared length, so none is carried), MySQL
// emits LONGTEXT for it, and MySQL cannot index a LONGTEXT without a key
// length (Error 1170) — a separate, pre-existing, LOUD-after-the-copy gap on
// any SQLite text column under a UNIQUE bound for MySQL, unrelated to the
// reserved name this file pins. It is reported, not fixed, here.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// inlineUniqueShape is one seed shape: the declared type of the three
// constrained columns and the literal SQL values for the two seeded rows,
// the control row (must be accepted) and the two duplicates (one per
// constraint; must be refused).
type inlineUniqueShape struct {
	colType   string
	seedRows  [2][3]string
	control   [3]string
	dupEmail  [3]string // repeats row 0's email
	dupTenHan [3]string // repeats row 0's (tenant, handle)
}

var (
	inlineUniqueText = inlineUniqueShape{
		colType:   "TEXT",
		seedRows:  [2][3]string{{"'a@example.com'", "'t1'", "'alice'"}, {"'b@example.com'", "'t1'", "'bob'"}},
		control:   [3]string{"'c@example.com'", "'t2'", "'carol'"},
		dupEmail:  [3]string{"'a@example.com'", "'t9'", "'zed'"},
		dupTenHan: [3]string{"'z@example.com'", "'t1'", "'alice'"},
	}
	inlineUniqueInteger = inlineUniqueShape{
		colType:   "INTEGER",
		seedRows:  [2][3]string{{"100", "1", "10"}, {"200", "1", "20"}},
		control:   [3]string{"300", "2", "30"},
		dupEmail:  [3]string{"100", "9", "90"},
		dupTenHan: [3]string{"900", "1", "10"},
	}
)

// seedSQLiteInlineUniqueSource writes the source: `accounts` with an inline
// UNIQUE on email and a composite UNIQUE (tenant, handle), two rows.
func seedSQLiteInlineUniqueSource(t *testing.T, shape inlineUniqueShape) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "inline-unique.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite seed: %v", err)
	}
	defer func() { _ = db.Close() }()
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE accounts (
			id     INTEGER PRIMARY KEY,
			email  %[1]s NOT NULL UNIQUE,
			tenant %[1]s NOT NULL,
			handle %[1]s NOT NULL,
			UNIQUE (tenant, handle)
		)`, shape.colType),
	}
	for i, r := range shape.seedRows {
		stmts = append(stmts, fmt.Sprintf(
			`INSERT INTO accounts (id, email, tenant, handle) VALUES (%d, %s, %s, %s)`, i+1, r[0], r[1], r[2],
		))
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed exec: %v", err)
		}
	}
	return path
}

// assertInlineUniqueEnforced inserts the control and both duplicates through
// the target's own driver (the values are SQL literals, so no bind-marker
// dialect is needed; qualified names the target's table).
func assertInlineUniqueEnforced(t *testing.T, db *sql.DB, qualified string, shape inlineUniqueShape) {
	t.Helper()
	ctx := ctx2min(t)
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+qualified).Scan(&n); err != nil {
		t.Fatalf("count accounts: %v", err)
	}
	if n != 2 {
		t.Fatalf("accounts on target = %d rows; want 2", n)
	}
	insert := func(id int, r [3]string) error {
		_, err := db.ExecContext(ctx, fmt.Sprintf(
			`INSERT INTO %s (id, email, tenant, handle) VALUES (%d, %s, %s, %s)`, qualified, id, r[0], r[1], r[2],
		))
		return err
	}
	if err := insert(10, shape.control); err != nil {
		t.Fatalf("control insert refused: %v (the table is not usable, so a refusal below would prove nothing)", err)
	}
	for _, d := range []struct {
		label string
		row   [3]string
	}{
		{"duplicate email", shape.dupEmail},
		{"duplicate (tenant, handle)", shape.dupTenHan},
	} {
		err := insert(20, d.row)
		if err == nil {
			t.Errorf("%s: target ACCEPTED a row the source rejects — the UNIQUE constraint was relaxed", d.label)
			continue
		}
		msg := strings.ToLower(err.Error())
		if !strings.Contains(msg, "unique") && !strings.Contains(msg, "duplicate") {
			t.Errorf("%s: refused for an unrelated reason: %v", d.label, err)
		}
	}
}

// TestMigrate_SQLiteInlineUnique_ToSQLite is the failing shape itself: the
// migrate must complete (it used to die in the index phase) and the target
// must enforce both constraints.
func TestMigrate_SQLiteInlineUnique_ToSQLite(t *testing.T) {
	src := seedSQLiteInlineUniqueSource(t, inlineUniqueText)
	dst := filepath.Join(t.TempDir(), "inline-unique-target.db")
	sqliteEng, _ := engines.Get("sqlite")
	mig := &Migrator{Source: sqliteEng, Target: sqliteEng, SourceDSN: src, TargetDSN: dst}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite→SQLite): %v", err)
	}
	db, err := sql.Open("sqlite", dst)
	if err != nil {
		t.Fatalf("open sqlite target: %v", err)
	}
	defer func() { _ = db.Close() }()
	assertInlineUniqueEnforced(t, db, "accounts", inlineUniqueText)

	// No reserved name reached the target catalog, and the generated names did.
	rows, err := db.QueryContext(ctx2min(t), `SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'accounts'`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(names, ",")
	if !strings.Contains(joined, "accounts_email_key") || !strings.Contains(joined, "accounts_tenant_handle_key") {
		t.Errorf("target indexes = %v; want accounts_email_key and accounts_tenant_handle_key", names)
	}
}

// TestMigrate_SQLiteInlineUnique_ToPostgres: lands as pg_constraint contype
// 'u' (a CONSTRAINT, the shape the source declared) and enforces.
func TestMigrate_SQLiteInlineUnique_ToPostgres(t *testing.T) {
	src := seedSQLiteInlineUniqueSource(t, inlineUniqueText)
	_, pgTarget, cleanup := startPostgres(t)
	defer cleanup()
	sqliteEng, _ := engines.Get("sqlite")
	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{Source: sqliteEng, Target: pgEng, SourceDSN: src, TargetDSN: pgTarget}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite→PG): %v", err)
	}
	db, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = db.Close() }()
	assertInlineUniqueEnforced(t, db, "public.accounts", inlineUniqueText)

	var constraints int
	if err := db.QueryRowContext(
		ctx2min(t),
		`SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'public.accounts'::regclass AND contype = 'u'`,
	).Scan(&constraints); err != nil {
		t.Fatalf("count unique constraints: %v", err)
	}
	if constraints != 2 {
		t.Errorf("pg_constraint contype='u' on accounts = %d; want 2 (email, and (tenant, handle)) — carried as constraints, not demoted to bare indexes", constraints)
	}
}

// TestMigrate_SQLiteInlineUnique_ToMySQL: lands as unique keys and enforces
// (integer-keyed; see the file comment for why).
func TestMigrate_SQLiteInlineUnique_ToMySQL(t *testing.T) {
	src := seedSQLiteInlineUniqueSource(t, inlineUniqueInteger)
	_, myTarget, cleanup := startMySQL(t)
	defer cleanup()
	sqliteEng, _ := engines.Get("sqlite")
	myEng, _ := engines.Get("mysql")
	mig := &Migrator{Source: sqliteEng, Target: myEng, SourceDSN: src, TargetDSN: myTarget}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite→MySQL): %v", err)
	}
	db, err := sql.Open("mysql", myTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = db.Close() }()
	assertInlineUniqueEnforced(t, db, "accounts", inlineUniqueInteger)

	var uniqueKeys int
	if err := db.QueryRowContext(
		ctx2min(t),
		`SELECT COUNT(DISTINCT INDEX_NAME) FROM information_schema.STATISTICS
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND NON_UNIQUE = 0 AND INDEX_NAME <> 'PRIMARY'`,
	).Scan(&uniqueKeys); err != nil {
		t.Fatalf("count unique keys: %v", err)
	}
	if uniqueKeys != 2 {
		t.Errorf("unique keys on accounts = %d; want 2", uniqueKeys)
	}
}
