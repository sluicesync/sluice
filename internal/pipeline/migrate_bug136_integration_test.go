//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for Bug 136 (a PG `text` column carrying a UNIQUE
// or secondary index translates to MySQL index DDL on a TEXT column
// without a key length — MySQL Error 1170 — loudly but LATE, at the
// create-indexes step AFTER all rows have copied; `schema preview`
// emitted the invalid DDL with no advisory):
//
//   - Pre-fix: `migrate` PG → MySQL created every table, copied every
//     row, then died at create-indexes with a raw Error 1170 ("BLOB/
//     TEXT column 'email' used in key specification without a key
//     length") — far past the point of clean recovery.
//
//   - Post-fix: the refusal fires at the cross-engine pre-flight,
//     BEFORE any DDL or data moves (no tables on the target), naming
//     the table.column, the index, and the workaround. sluice
//     deliberately does NOT auto-emit a prefix key length — a prefix
//     UNIQUE index silently changes uniqueness semantics. The verified
//     workaround `--type-override COL=varchar(N)` completes end-to-end
//     with the indexes built; MySQL → MySQL TEXT-prefix-indexed
//     sources are untouched.
//
// This is the verbatim BUG-CATALOG section 136 repro shape.

package pipeline

import (
	"database/sql"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// bug136SeedDDL carries the Bug 74 index-shape matrix on a PG source:
// a UNIQUE index, a plain secondary index, and a composite index each
// covering a column that lands on MySQL as a TEXT/BLOB type.
const bug136SeedDDL = `
	CREATE TABLE users (
	  id     bigint PRIMARY KEY,
	  email  text NOT NULL,
	  note   text,
	  status varchar(50),
	  body   text,
	  raw    bytea,
	  CONSTRAINT users_email_key UNIQUE (email)
	);
	CREATE INDEX users_note_idx ON users (note);
	CREATE INDEX users_status_body_idx ON users (status, body);
	CREATE INDEX users_raw_idx ON users (raw);

	INSERT INTO users (id, email, note, status, body, raw) VALUES
	  (1, 'alice@example.com', 'note a', 'active', 'body a', '\xdeadbeef'),
	  (2, 'bob@example.com',   'note b', 'idle',   'body b', '\xcafe');
`

// TestMigrate_PostgresToMySQL_Bug136TextIndexRefusesEarly pins the
// early refusal: PG → MySQL with text-indexed columns refuses BEFORE
// any DDL/data moves — the target stays completely empty — and the
// error names every offending key part plus the workaround.
func TestMigrate_PostgresToMySQL_Bug136TextIndexRefusesEarly(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, bug136SeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	mig := &Migrator{
		Source:    pgEng,
		Target:    mysqlEng,
		SourceDSN: pgSource,
		TargetDSN: mysqlTarget,
	}
	err := mig.Run(ctx2min(t))
	if err == nil {
		t.Fatal("Migrator.Run = nil; want the Bug 136 early refusal (pre-fix this died with a raw Error 1170 after bulk copy)")
	}
	for _, want := range []string{
		"users.email",     // the UNIQUE-indexed text column
		"users_email_key", // ... and its index
		"users.note",      // the plain-secondary-indexed text column
		"users.body",      // the composite-index text member
		"users.raw",       // the indexed bytea column (BLOB family)
		"Error 1170",      // the late failure it pre-empts
		"--type-override", // the escape hatch
		"varchar(N)",      // ... with the concrete shape
		"before any data", // the early-refusal promise
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal missing %q\n--- got ---\n%v", want, err)
		}
	}
	// The composite index's varchar(50) member is indexable and must
	// NOT be named.
	if strings.Contains(err.Error(), "users.status") {
		t.Errorf("refusal names the indexable varchar member users.status:\n%v", err)
	}

	// EARLY means early: the refusal fires at the cross-engine
	// pre-flight, before CREATE TABLE — the target must hold zero
	// tables and therefore zero rows.
	mysqlDB, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = mysqlDB.Close() }()
	var tables int
	if err := mysqlDB.QueryRow(`
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'`).Scan(&tables); err != nil {
		t.Fatalf("count target tables: %v", err)
	}
	if tables != 0 {
		t.Errorf("target has %d table(s); want 0 (refusal must fire before any DDL)", tables)
	}
}

// TestMigrate_PostgresToMySQL_Bug136TypeOverrideEscapeHatch pins the
// verified workaround end-to-end: with every offending column bounded
// via --type-override varchar(N), migrate completes, the indexes
// (UNIQUE included) exist on the target, and the data round-trips.
func TestMigrate_PostgresToMySQL_Bug136TypeOverrideEscapeHatch(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, bug136SeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	varchar := func(col string, length int) config.Mapping {
		return config.Mapping{
			Table:             "users",
			Column:            col,
			TargetType:        "varchar",
			TargetTypeOptions: map[string]any{"length": length},
		}
	}
	mig := &Migrator{
		Source:    pgEng,
		Target:    mysqlEng,
		SourceDSN: pgSource,
		TargetDSN: mysqlTarget,
		Mappings: []config.Mapping{
			varchar("email", 255),
			varchar("note", 255),
			varchar("body", 255),
			// bytea → bounded binary; varbinary is index-able with no
			// prefix length, closing the BLOB-family member too.
			{Table: "users", Column: "raw", TargetType: "binary_uuid"},
		},
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run with --type-override escape hatch: %v", err)
	}

	mysqlDB, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = mysqlDB.Close() }()

	// Every index must exist on the target — and users_email_key must
	// still be UNIQUE (non_unique = 0).
	indexUnique := map[string]int{}
	rows, err := mysqlDB.Query(`
		SELECT DISTINCT index_name, non_unique FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = 'users'`)
	if err != nil {
		t.Fatalf("read target indexes: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name string
		var nonUnique int
		if err := rows.Scan(&name, &nonUnique); err != nil {
			t.Fatalf("scan index row: %v", err)
		}
		indexUnique[name] = nonUnique
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if nu, ok := indexUnique["users_email_key"]; !ok || nu != 0 {
		t.Errorf("users_email_key: present=%v non_unique=%d; want a UNIQUE index (uniqueness semantics preserved by the full-value varchar override)", ok, nu)
	}
	for _, idx := range []string{"users_note_idx", "users_status_body_idx", "users_raw_idx"} {
		if _, ok := indexUnique[idx]; !ok {
			t.Errorf("index %s missing on target (have: %v)", idx, indexUnique)
		}
	}

	// Data round-trips.
	var n int
	if err := mysqlDB.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&n); err != nil {
		t.Fatalf("count target rows: %v", err)
	}
	if n != 2 {
		t.Errorf("target rows = %d; want 2", n)
	}
	var email string
	if err := mysqlDB.QueryRow(`SELECT email FROM users WHERE id = 1`).Scan(&email); err != nil {
		t.Fatalf("read row 1: %v", err)
	}
	if email != "alice@example.com" {
		t.Errorf("users.email(id=1) = %q; want alice@example.com", email)
	}
}

// TestMigrate_MySQLToMySQL_Bug136TextPrefixIndexUnchanged guards the
// same-engine contract: a MySQL source's TEXT column indexed WITH a
// prefix length (the only valid MySQL shape) must keep migrating —
// the Bug 136 refusal is cross-engine only — and the prefix length
// must round-trip onto the target.
func TestMigrate_MySQLToMySQL_Bug136TextPrefixIndexUnchanged(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	applyMySQLDDL(t, sourceDSN, `
		CREATE TABLE docs (
		  id    BIGINT NOT NULL,
		  body  TEXT,
		  draft TEXT,
		  PRIMARY KEY (id),
		  UNIQUE KEY body_uniq (body(100)),
		  KEY draft_idx (draft(64))
		);
		INSERT INTO docs (id, body, draft) VALUES
		  (1, 'body one', 'draft one'),
		  (2, 'body two', 'draft two');
	`)

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
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (MySQL→MySQL TEXT prefix indexes must stay working): %v", err)
	}

	mysqlDB, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = mysqlDB.Close() }()

	subPart := func(index string) int64 {
		var sp sql.NullInt64
		if err := mysqlDB.QueryRow(`
			SELECT sub_part FROM information_schema.statistics
			WHERE table_schema = DATABASE() AND table_name = 'docs' AND index_name = ?`,
			index).Scan(&sp); err != nil {
			t.Fatalf("read %s sub_part: %v", index, err)
		}
		if !sp.Valid {
			t.Fatalf("%s sub_part is NULL; want the source's prefix length", index)
		}
		return sp.Int64
	}
	if got := subPart("body_uniq"); got != 100 {
		t.Errorf("body_uniq prefix = %d; want 100", got)
	}
	if got := subPart("draft_idx"); got != 64 {
		t.Errorf("draft_idx prefix = %d; want 64", got)
	}

	var n int
	if err := mysqlDB.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&n); err != nil {
		t.Fatalf("count target rows: %v", err)
	}
	if n != 2 {
		t.Errorf("target rows = %d; want 2", n)
	}
}
