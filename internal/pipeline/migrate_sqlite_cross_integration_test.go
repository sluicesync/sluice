//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine end-to-end integration test for the SQLite / Cloudflare D1
// migrate SOURCE (ADR-0128): a temp SQLite file migrates faithfully into
// BOTH a Postgres target and a MySQL target via pipeline.Migrator. This is
// the headline prototype proof — a SQLite/D1 export round-trips into either
// target with matching row counts, an ordered value compare, and the
// affinity-resolved types landing correctly.
//
// A Cloudflare D1 export (`wrangler d1 export`) is just a SQLite file, so
// the same path serves D1 with zero D1-specific code.

package pipeline

import (
	"context"
	"database/sql"
	"path/filepath"
	"strconv"
	"testing"

	_ "modernc.org/sqlite" // pure-Go driver for seeding the temp source file

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	// Register the engines engines.Get needs.
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// seedSQLiteSource writes a temp SQLite file exercising every affinity
// (INTEGER / TEXT / NUMERIC / REAL / BLOB) and a representative value mix
// (int, real, text, blob, NULL), plus a foreign key with ON DELETE
// CASCADE. Returns the file path.
func seedSQLiteSource(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "d1export.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite seed: %v", err)
	}
	defer func() { _ = db.Close() }()

	stmts := []string{
		`CREATE TABLE users (
			id    INTEGER PRIMARY KEY,
			name  TEXT NOT NULL,
			score NUMERIC,
			rate  REAL,
			photo BLOB
		)`,
		`INSERT INTO users (id, name, score, rate, photo) VALUES
			(1, 'alice', 100, 3.5, x'cafe'),
			(2, 'bob',   NULL, NULL, NULL)`,
		`CREATE TABLE posts (
			id      INTEGER PRIMARY KEY,
			user_id INTEGER NOT NULL,
			body    TEXT,
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
		)`,
		`INSERT INTO posts (id, user_id, body) VALUES
			(1, 1, 'hello'),
			(2, 1, 'world'),
			(3, 2, 'hi')`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed exec: %v", err)
		}
	}
	return path
}

// TestMigrate_SQLiteToPostgres is the SQLite→Postgres half of the proof.
func TestMigrate_SQLiteToPostgres(t *testing.T) {
	src := seedSQLiteSource(t)
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    sqliteEng,
		Target:    pgEng,
		SourceDSN: src,
		TargetDSN: pgTarget,
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite→PG): %v", err)
	}

	assertSQLiteRoundTrip(t, pgEng, pgTarget)
}

// TestMigrate_SQLiteToMySQL is the SQLite→MySQL half of the proof.
func TestMigrate_SQLiteToMySQL(t *testing.T) {
	src := seedSQLiteSource(t)
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	mig := &Migrator{
		Source:    sqliteEng,
		Target:    mysqlEng,
		SourceDSN: src,
		TargetDSN: mysqlTarget,
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite→MySQL): %v", err)
	}

	assertSQLiteRoundTrip(t, mysqlEng, mysqlTarget)
}

// assertSQLiteRoundTrip reads the migrated schema + rows back through the
// TARGET engine and asserts the affinity-resolved types landed and every
// value round-tripped exactly-once. Engine-neutral: it reads via the
// target's own RowReader, whose canonical Go value shapes are identical
// across PG and MySQL for these types (decimal→string, float→float64,
// blob→[]byte, text→string, integer→int64).
func assertSQLiteRoundTrip(t *testing.T, eng ir.Engine, dsn string) {
	t.Helper()
	ctx := ctx2min(t)

	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	got, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	if len(got.Tables) != 2 {
		t.Fatalf("target tables = %d; want 2 (%v)", len(got.Tables), targetTableNames(got))
	}
	users := findTable(got, "users")
	posts := findTable(got, "posts")
	if users == nil || posts == nil {
		t.Fatalf("missing target tables; have %v", targetTableNames(got))
	}

	// Type landing: INTEGER affinity → integer; TEXT → text; NUMERIC →
	// decimal; REAL → float; BLOB → bytes. We assert the IR kind on the
	// target's read-back (each engine reports its own canonical IR type).
	assertColKind(t, users, "id", "integer")
	assertColKind(t, users, "name", "text")
	assertColKind(t, users, "score", "decimal")
	assertColKind(t, users, "rate", "float")
	assertColKind(t, users, "photo", "bytes")

	// Row-value compare (ordered by PK). usersRows[0]=alice, [1]=bob.
	rr, err := eng.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(rr)

	usersRows := readAll(t, ctx, rr, users)
	if len(usersRows) != 2 {
		t.Fatalf("users rows = %d; want 2", len(usersRows))
	}
	if name, _ := usersRows[0]["name"].(string); name != "alice" {
		t.Errorf("users[0].name = %#v; want alice", usersRows[0]["name"])
	}
	if got := asFloat(t, usersRows[0]["score"]); got != 100 {
		t.Errorf("users[0].score = %#v; want 100", usersRows[0]["score"])
	}
	if got := asFloat(t, usersRows[0]["rate"]); got != 3.5 {
		t.Errorf("users[0].rate = %#v; want 3.5", usersRows[0]["rate"])
	}
	if photo, ok := usersRows[0]["photo"].([]byte); !ok || len(photo) != 2 || photo[0] != 0xca || photo[1] != 0xfe {
		t.Errorf("users[0].photo = %#v; want bytes {0xca,0xfe}", usersRows[0]["photo"])
	}
	// bob's NULLs must survive as NULL, not a coerced zero value.
	if usersRows[1]["score"] != nil {
		t.Errorf("users[1].score = %#v; want nil", usersRows[1]["score"])
	}
	if usersRows[1]["rate"] != nil {
		t.Errorf("users[1].rate = %#v; want nil", usersRows[1]["rate"])
	}
	if usersRows[1]["photo"] != nil {
		t.Errorf("users[1].photo = %#v; want nil", usersRows[1]["photo"])
	}

	postsRows := readAll(t, ctx, rr, posts)
	if len(postsRows) != 3 {
		t.Errorf("posts rows = %d; want 3", len(postsRows))
	}

	// FK landed: posts.user_id → users.id ON DELETE CASCADE.
	if len(posts.ForeignKeys) != 1 {
		t.Fatalf("posts FKs = %d; want 1", len(posts.ForeignKeys))
	}
	if fk := posts.ForeignKeys[0]; fk.ReferencedTable != "users" || fk.OnDelete != ir.FKActionCascade {
		t.Errorf("posts FK = %+v; want users ON DELETE CASCADE", fk)
	}
}

// assertColKind checks a target column's IR type family by name.
func assertColKind(t *testing.T, tbl *ir.Table, col, kind string) {
	t.Helper()
	c := findColumn(tbl, col)
	if c == nil {
		t.Fatalf("%s.%s missing", tbl.Name, col)
	}
	var ok bool
	switch kind {
	case "integer":
		_, ok = c.Type.(ir.Integer)
	case "text":
		_, ok = c.Type.(ir.Text)
	case "decimal":
		_, ok = c.Type.(ir.Decimal)
	case "float":
		_, ok = c.Type.(ir.Float)
	case "bytes":
		switch c.Type.(type) {
		case ir.Blob, ir.Varbinary, ir.Binary:
			ok = true
		}
	}
	if !ok {
		t.Errorf("%s.%s type = %#v; want IR %s", tbl.Name, col, c.Type, kind)
	}
}

// asFloat coerces a decimal-as-string or float64 row value to float64 for
// numeric comparison, so the assertion is robust to engine-specific
// decimal formatting (PG "100", MySQL "100" / "100.00000", etc.).
func asFloat(t *testing.T, v any) float64 {
	t.Helper()
	switch x := v.(type) {
	case float64:
		return x
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			t.Fatalf("parse %q as float: %v", x, err)
		}
		return f
	case int64:
		return float64(x)
	default:
		t.Fatalf("value %#v (%T) not numeric", v, v)
		return 0
	}
}
