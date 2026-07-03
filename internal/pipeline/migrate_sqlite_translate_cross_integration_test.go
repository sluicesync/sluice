//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine end-to-end proof for the SQLite→canonical EXPRESSION translator
// (ADR-0133 follow-up, roadmap item 49). Where migrate_sqlite_cross_integration_
// test.go's feature test uses arithmetic bodies that happen to be valid verbatim
// on every engine, THIS test deliberately uses constructs whose SQLite spelling
// would FAIL verbatim on the target and only work once translated:
//
//   - the generated-column body uses `||` (MySQL has no `||` concat operator by
//     default — it must become CONCAT) AND `ifnull` (Postgres has no IFNULL — it
//     must become COALESCE), so BOTH targets exercise the translator; and
//   - the CHECK uses `length(...)` (→ CHAR_LENGTH on MySQL).
//
// The migrate is asserted to SUCCEED, the generated column to RE-DERIVE the
// exact value on the target, the CHECK to be ENFORCED, the partial index to
// carry its predicate (PG), and the row data to land byte-exact.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// seedSQLiteTranslatedFeatures writes a temp SQLite file whose generated
// column / CHECK / partial index use constructs that REQUIRE translation to
// work on the target (|| and ifnull and length). Rows carry non-NULL a/b so
// the derived label is deterministic ("foo-bar", "baz-qux").
func seedSQLiteTranslatedFeatures(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "translated.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite seed: %v", err)
	}
	defer func() { _ = db.Close() }()
	stmts := []string{
		`CREATE TABLE widgets (
			id    INTEGER PRIMARY KEY,
			a     TEXT NOT NULL,
			b     TEXT NOT NULL,
			n     INTEGER NOT NULL,
			label TEXT GENERATED ALWAYS AS (ifnull(a, 'x') || '-' || ifnull(b, 'y')) STORED,
			CONSTRAINT a_len CHECK (length(a) > 0)
		)`,
		`INSERT INTO widgets (id, a, b, n) VALUES (1, 'foo', 'bar', 5), (2, 'baz', 'qux', 0)`,
		`CREATE INDEX widgets_pos_idx ON widgets(n) WHERE n > 0`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed exec: %v", err)
		}
	}
	return path
}

// TestMigrate_SQLiteTranslatedExprToPostgres pins the translated carry into PG:
// the || / ifnull generated column re-derives, the length CHECK is enforced,
// and the partial index keeps its (translated) predicate.
func TestMigrate_SQLiteTranslatedExprToPostgres(t *testing.T) {
	src := seedSQLiteTranslatedFeatures(t)
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	sqliteEng, _ := engines.Get("sqlite")
	pgEng, _ := engines.Get("postgres")
	mig := &Migrator{Source: sqliteEng, Target: pgEng, SourceDSN: src, TargetDSN: pgTarget}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite translated-expr→PG): %v", err)
	}

	db, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := ctx2min(t)

	// 1. Generated column re-derived the EXACT label from the translated body.
	assertLabel(t, db, ctx, 1, "foo-bar")
	assertLabel(t, db, ctx, 2, "baz-qux")

	// 2. It is a GENERATED column (translated ifnull→COALESCE, ||-chain), not a
	//    plain copied column.
	var isGen string
	if err := db.QueryRowContext(ctx,
		`SELECT is_generated FROM information_schema.columns
		 WHERE table_name = 'widgets' AND column_name = 'label'`).Scan(&isGen); err != nil {
		t.Fatalf("query is_generated: %v", err)
	}
	if isGen != "ALWAYS" {
		t.Errorf("widgets.label is_generated = %q; want ALWAYS", isGen)
	}

	// 3. The length CHECK is enforced on the target (empty a → length 0 → reject).
	if _, err := db.ExecContext(ctx,
		`INSERT INTO widgets (id, a, b, n) VALUES (99, '', 'z', 1)`); err == nil {
		t.Error("INSERT violating CHECK (length(a) > 0) succeeded; want a rejection")
	}

	// 4. Partial index carried its translated predicate.
	var indexdef string
	if err := db.QueryRowContext(ctx,
		`SELECT indexdef FROM pg_indexes WHERE tablename = 'widgets' AND indexdef ILIKE '%WHERE%'`).Scan(&indexdef); err != nil {
		t.Fatalf("query partial index def (none found?): %v", err)
	}
	if !strings.Contains(strings.ToUpper(indexdef), "WHERE") {
		t.Errorf("partial index def = %q; want a WHERE predicate", indexdef)
	}

	// 5. Source data byte-exact.
	assertWidgetData(t, db, ctx)
}

// TestMigrate_SQLiteTranslatedExprToMySQL pins the translated carry into MySQL:
// the || generated column (which would FAIL verbatim — MySQL parses || as
// logical OR) re-derives correctly because it was translated to CONCAT, and the
// length CHECK is enforced.
func TestMigrate_SQLiteTranslatedExprToMySQL(t *testing.T) {
	src := seedSQLiteTranslatedFeatures(t)
	_, myTarget, myCleanup := startMySQL(t)
	defer myCleanup()

	sqliteEng, _ := engines.Get("sqlite")
	myEng, _ := engines.Get("mysql")
	mig := &Migrator{Source: sqliteEng, Target: myEng, SourceDSN: src, TargetDSN: myTarget}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite translated-expr→MySQL): %v", err)
	}

	db, err := sql.Open("mysql", myTarget)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := ctx2min(t)

	// Generated column re-derived via the translated CONCAT(COALESCE(...),...).
	assertLabel(t, db, ctx, 1, "foo-bar")
	assertLabel(t, db, ctx, 2, "baz-qux")

	var extra string
	if err := db.QueryRowContext(ctx,
		`SELECT extra FROM information_schema.columns
		 WHERE table_name = 'widgets' AND column_name = 'label'`).Scan(&extra); err != nil {
		t.Fatalf("query extra: %v", err)
	}
	if !strings.Contains(strings.ToUpper(extra), "GENERATED") {
		t.Errorf("widgets.label extra = %q; want a GENERATED column", extra)
	}

	// length CHECK enforced (→ CHAR_LENGTH on MySQL 8.0.16+).
	if _, err := db.ExecContext(ctx,
		`INSERT INTO widgets (id, a, b, n) VALUES (99, '', 'z', 1)`); err == nil {
		t.Error("INSERT violating CHECK (length(a) > 0) succeeded on MySQL; want a rejection")
	}

	assertWidgetData(t, db, ctx)
}

// assertLabel checks the target-derived generated-column value for one row.
// The id is inlined (a test-controlled integer) so the query is
// placeholder-dialect-neutral across the PG and MySQL drivers.
func assertLabel(t *testing.T, db *sql.DB, ctx context.Context, id int, want string) {
	t.Helper()
	var got string
	q := fmt.Sprintf(`SELECT label FROM widgets WHERE id = %d`, id)
	if err := db.QueryRowContext(ctx, q).Scan(&got); err != nil {
		t.Fatalf("select label id=%d: %v", id, err)
	}
	if got != want {
		t.Errorf("widgets.label id=%d = %q; want %q (re-derived from translated body)", id, got, want)
	}
}

// assertWidgetData checks the two source rows landed byte-exact.
func assertWidgetData(t *testing.T, db *sql.DB, ctx context.Context) {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM widgets`).Scan(&n); err != nil {
		t.Fatalf("count widgets: %v", err)
	}
	if n != 2 {
		t.Errorf("widgets row count = %d; want 2", n)
	}
	rows := map[int][2]string{1: {"foo", "bar"}, 2: {"baz", "qux"}}
	for id, ab := range rows {
		var a, b string
		q := fmt.Sprintf(`SELECT a, b FROM widgets WHERE id = %d`, id)
		if err := db.QueryRowContext(ctx, q).Scan(&a, &b); err != nil {
			t.Fatalf("select a,b id=%d: %v", id, err)
		}
		if a != ab[0] || b != ab[1] {
			t.Errorf("widgets id=%d = (%q,%q); want (%q,%q)", id, a, b, ab[0], ab[1])
		}
	}
}
