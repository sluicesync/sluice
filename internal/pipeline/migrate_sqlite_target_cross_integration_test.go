//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine end-to-end integration test for the SQLite migrate TARGET
// (ADR-0134): the headline proof that the SQLite write side is the faithful
// INVERSE of the read side.
//
//   - SQLite→Postgres→SQLite and SQLite→MySQL→SQLite round-trips: a source
//     .db with every IR value family migrates out and back, and the final
//     SQLite content is value-identical to the seeded original.
//   - Postgres→SQLite with PG-native types (numeric, timestamptz, bytea,
//     boolean, bigint > 2^53): proves the cross-engine INTO-SQLite path and
//     the tz-aware-timestamp instant-fidelity wart.
//   - FK integrity: a 2-table FK source loads in any order (FK-off bulk
//     load) and PRAGMA foreign_key_check is clean on the produced file.

package pipeline

import (
	"context"
	"database/sql"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// seedSQLiteRich writes a temp SQLite source covering every IR value family
// the writer round-trips: integers (incl. > 2^53), float, unicode text,
// blob, boolean, decimal, date/datetime/time. Row 2 carries NULLs in every
// nullable column. Returns the file path.
func seedSQLiteRich(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rich.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite seed: %v", err)
	}
	defer func() { _ = db.Close() }()
	stmts := []string{
		`CREATE TABLE widgets (
			id        INTEGER PRIMARY KEY,
			big       INTEGER NOT NULL,
			rate      REAL,
			name      TEXT NOT NULL,
			data      BLOB,
			active    BOOLEAN NOT NULL,
			price     NUMERIC,
			made_on   DATE,
			made_at   DATETIME,
			made_time TIME
		)`,
		`INSERT INTO widgets (id, big, rate, name, data, active, price, made_on, made_at, made_time) VALUES
			(1, 9007199254740993, 2.5, 'wîdget ☃', x'cafebabe', 1, 123.45, '2024-01-02', '2024-01-02 03:04:05', '03:04:05'),
			(2, -8, NULL, 'two', NULL, 0, NULL, NULL, NULL, NULL)`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed exec: %v", err)
		}
	}
	return path
}

// TestMigrate_SQLiteToPostgresToSQLite is the SQLite→PG→SQLite round-trip.
func TestMigrate_SQLiteToPostgresToSQLite(t *testing.T) {
	runSQLiteTargetRoundTrip(t, "postgres", startPostgres)
}

// TestMigrate_SQLiteToMySQLToSQLite is the SQLite→MySQL→SQLite round-trip.
func TestMigrate_SQLiteToMySQLToSQLite(t *testing.T) {
	runSQLiteTargetRoundTrip(t, "mysql", startMySQL)
}

// runSQLiteTargetRoundTrip migrates the rich SQLite source OUT to the named
// intermediate engine and BACK into a fresh SQLite file, then asserts the
// final SQLite content matches the seeded values exactly-once (the writer is
// the faithful inverse of the reader).
func runSQLiteTargetRoundTrip(t *testing.T, midName string, start func(*testing.T) (string, string, func())) {
	ctx := ctx2min(t)
	src := seedSQLiteRich(t)
	_, mid, cleanup := start(t)
	defer cleanup()

	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	midEng, ok := engines.Get(midName)
	if !ok {
		t.Fatalf("%s engine not registered", midName)
	}

	// Leg 1: SQLite → intermediate engine.
	out := &Migrator{Source: sqliteEng, Target: midEng, SourceDSN: src, TargetDSN: mid}
	if err := out.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (SQLite→%s): %v", midName, err)
	}

	// Leg 2: intermediate engine → fresh SQLite file (the TARGET writer).
	dst := filepath.Join(t.TempDir(), "back.db")
	back := &Migrator{Source: midEng, Target: sqliteEng, SourceDSN: mid, TargetDSN: dst}
	if err := back.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (%s→SQLite): %v", midName, err)
	}

	// Read the final SQLite file back through the SQLite reader.
	sr, err := sqliteEng.OpenSchemaReader(ctx, dst)
	if err != nil {
		t.Fatalf("OpenSchemaReader(dst): %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema(dst): %v", err)
	}
	widgets := findTable(schema, "widgets")
	if widgets == nil {
		t.Fatalf("final SQLite missing 'widgets'; have %v", targetTableNames(schema))
	}

	// Types: the round-trip must land the families back (NOT numeric for
	// declared temporal/bool).
	assertSQLiteColType(t, widgets, "id", func(x ir.Type) bool { _, ok := x.(ir.Integer); return ok })
	assertSQLiteColType(t, widgets, "rate", func(x ir.Type) bool { _, ok := x.(ir.Float); return ok })
	assertSQLiteColType(t, widgets, "name", func(x ir.Type) bool { _, ok := x.(ir.Text); return ok })
	assertSQLiteColType(t, widgets, "data", func(x ir.Type) bool { _, ok := x.(ir.Blob); return ok })
	assertSQLiteColType(t, widgets, "active", func(x ir.Type) bool { _, ok := x.(ir.Boolean); return ok })
	assertSQLiteColType(t, widgets, "price", func(x ir.Type) bool { _, ok := x.(ir.Decimal); return ok })
	assertSQLiteColType(t, widgets, "made_on", func(x ir.Type) bool { _, ok := x.(ir.Date); return ok })
	assertSQLiteColType(t, widgets, "made_at", func(x ir.Type) bool { _, ok := x.(ir.Timestamp); return ok })
	assertSQLiteColType(t, widgets, "made_time", func(x ir.Type) bool { _, ok := x.(ir.Time); return ok })

	rr, err := sqliteEng.OpenRowReader(ctx, dst)
	if err != nil {
		t.Fatalf("OpenRowReader(dst): %v", err)
	}
	defer closeIf(rr)
	rows := readAll(t, ctx, rr, widgets)
	if len(rows) != 2 {
		t.Fatalf("final rows = %d; want 2", len(rows))
	}
	r0, r1 := rows[0], rows[1]
	if r0["id"].(int64) != 1 {
		t.Fatalf("row order unexpected: id=%v", r0["id"])
	}

	// Row 1: every value family identical to the seed.
	if r0["big"].(int64) != 9007199254740993 {
		t.Errorf("big = %#v; want 9007199254740993 (exact > 2^53)", r0["big"])
	}
	if asNum(t, r0["rate"]) != 2.5 {
		t.Errorf("rate = %#v; want 2.5", r0["rate"])
	}
	if r0["name"].(string) != "wîdget ☃" {
		t.Errorf("name = %#v; want unicode 'wîdget ☃'", r0["name"])
	}
	if b, ok := r0["data"].([]byte); !ok || string(b) != "\xca\xfe\xba\xbe" {
		t.Errorf("data = %#v; want bytes cafebabe", r0["data"])
	}
	if r0["active"].(bool) != true {
		t.Errorf("active = %#v; want true", r0["active"])
	}
	if asNum(t, r0["price"]) != 123.45 {
		t.Errorf("price = %#v; want 123.45", r0["price"])
	}
	if asUTC(t, r0["made_on"]).Format("2006-01-02") != "2024-01-02" {
		t.Errorf("made_on = %#v; want 2024-01-02", r0["made_on"])
	}
	if asUTC(t, r0["made_at"]).Format("2006-01-02 15:04:05") != "2024-01-02 03:04:05" {
		t.Errorf("made_at = %#v; want 2024-01-02 03:04:05", r0["made_at"])
	}
	if s := strings.TrimSpace(r0["made_time"].(string)); !strings.Contains(s, "03:04:05") {
		t.Errorf("made_time = %#v; want it to contain 03:04:05", r0["made_time"])
	}

	// Row 2: NULLs survived as NULL (not coerced zero values).
	if r1["active"].(bool) != false {
		t.Errorf("row2 active = %#v; want false", r1["active"])
	}
	for _, c := range []string{"rate", "data", "price", "made_on", "made_at", "made_time"} {
		if r1[c] != nil {
			t.Errorf("row2[%s] = %#v; want nil", c, r1[c])
		}
	}

	// The produced file is a valid SQLite database (open + count directly).
	assertValidSQLiteFile(t, dst, "widgets", 2)
}

// TestMigrate_PostgresToSQLite_NativeTypes proves the cross-engine
// INTO-SQLite path on PG-native types, including the tz-aware-timestamp
// instant-fidelity wart (timestamptz → SQLite UTC ISO, ADR-0134).
func TestMigrate_PostgresToSQLite_NativeTypes(t *testing.T) {
	ctx := ctx2min(t)
	_, pgSource, cleanup := startPostgres(t)
	defer cleanup()

	pdb, err := sql.Open("pgx", pgSource)
	if err != nil {
		t.Fatalf("open pg source: %v", err)
	}
	seed := []string{
		`CREATE TABLE n (
			id   bigint PRIMARY KEY,
			amt  numeric(12,4),
			ts   timestamptz,
			flag boolean,
			data bytea,
			big  bigint
		)`,
		`INSERT INTO n (id, amt, ts, flag, data, big) VALUES
			(1, 123.4567, '2024-01-02 03:04:05+05', true, '\xcafe', 9007199254740993)`,
	}
	for _, s := range seed {
		if _, err := pdb.ExecContext(ctx, s); err != nil {
			_ = pdb.Close()
			t.Fatalf("seed pg: %v", err)
		}
	}
	_ = pdb.Close()

	sqliteEng, _ := engines.Get("sqlite")
	pgEng, _ := engines.Get("postgres")
	dst := filepath.Join(t.TempDir(), "frompg.db")
	mig := &Migrator{Source: pgEng, Target: sqliteEng, SourceDSN: pgSource, TargetDSN: dst}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (PG→SQLite): %v", err)
	}

	sr, _ := sqliteEng.OpenSchemaReader(ctx, dst)
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tbl := findTable(schema, "n")
	if tbl == nil {
		t.Fatalf("missing table n; have %v", targetTableNames(schema))
	}
	rr, _ := sqliteEng.OpenRowReader(ctx, dst)
	defer closeIf(rr)
	rows := readAll(t, ctx, rr, tbl)
	if len(rows) != 1 {
		t.Fatalf("rows = %d; want 1", len(rows))
	}
	r := rows[0]
	if asNum(t, r["amt"]) != 123.4567 {
		t.Errorf("amt = %#v; want 123.4567 (PG numeric preserved)", r["amt"])
	}
	if r["flag"].(bool) != true {
		t.Errorf("flag = %#v; want true", r["flag"])
	}
	if b, ok := r["data"].([]byte); !ok || string(b) != "\xca\xfe" {
		t.Errorf("data = %#v; want bytes cafe (bytea)", r["data"])
	}
	if r["big"].(int64) != 9007199254740993 {
		t.Errorf("big = %#v; want 9007199254740993", r["big"])
	}
	// timestamptz instant fidelity: '03:04:05+05' is 22:04:05 UTC the day
	// before. SQLite stored the UTC instant (zone dropped, ADR-0134).
	wantTS := time.Date(2024, 1, 1, 22, 4, 5, 0, time.UTC)
	if got := asUTC(t, r["ts"]); !got.Equal(wantTS) {
		t.Errorf("ts = %v; want %v (UTC instant of timestamptz)", got, wantTS)
	}
}

// TestMigrate_SQLiteToSQLiteFKIntegrity proves the FK-off bulk load + the
// post-copy foreign_key_check: a 2-table FK source migrates SQLite→PG→SQLite
// and the produced file has clean FK integrity.
func TestMigrate_SQLiteToSQLiteFKIntegrity(t *testing.T) {
	ctx := ctx2min(t)
	src := seedSQLiteSource(t) // users + posts(user_id) FK ON DELETE CASCADE
	_, pg, cleanup := startPostgres(t)
	defer cleanup()

	sqliteEng, _ := engines.Get("sqlite")
	pgEng, _ := engines.Get("postgres")

	out := &Migrator{Source: sqliteEng, Target: pgEng, SourceDSN: src, TargetDSN: pg}
	if err := out.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run (SQLite→PG): %v", err)
	}
	dst := filepath.Join(t.TempDir(), "fk.db")
	back := &Migrator{Source: pgEng, Target: sqliteEng, SourceDSN: pg, TargetDSN: dst}
	if err := back.Run(ctx); err != nil {
		// A successful run already implies CreateConstraints' foreign_key_check
		// passed — a dangling FK would have failed the migration loudly.
		t.Fatalf("Migrator.Run (PG→SQLite): %v", err)
	}

	// Belt-and-suspenders: open the produced file and assert foreign_key_check
	// is empty AND the inline FK is present on the target catalog.
	db, err := sql.Open("sqlite", dst)
	if err != nil {
		t.Fatalf("open produced db: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		t.Fatalf("foreign_key_check: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		t.Error("foreign_key_check returned a violation on the produced file; want none")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("foreign_key_check iterate: %v", err)
	}

	sr, _ := sqliteEng.OpenSchemaReader(ctx, dst)
	defer closeIf(sr)
	schema, _ := sr.ReadSchema(ctx)
	posts := findTable(schema, "posts")
	if posts == nil || len(posts.ForeignKeys) != 1 {
		t.Fatalf("posts FK missing on produced file: %+v", posts)
	}
	if fk := posts.ForeignKeys[0]; fk.ReferencedTable != "users" {
		t.Errorf("posts FK references %q; want users", fk.ReferencedTable)
	}
}

// assertSQLiteColType checks a column's IR type family on a SQLite read-back.
func assertSQLiteColType(t *testing.T, tbl *ir.Table, col string, ok func(ir.Type) bool) {
	t.Helper()
	c := findColumn(tbl, col)
	if c == nil {
		t.Fatalf("%s.%s missing", tbl.Name, col)
	}
	if !ok(c.Type) {
		t.Errorf("%s.%s type = %#v; wrong family on round-trip", tbl.Name, col, c.Type)
	}
}

// asNum coerces a decimal-as-string or float64 to float64 for numeric compare.
func asNum(t *testing.T, v any) float64 {
	t.Helper()
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			t.Fatalf("parse %q as float: %v", x, err)
		}
		return f
	default:
		t.Fatalf("value %#v (%T) not numeric", v, v)
		return 0
	}
}

// asUTC coerces a temporal value to a UTC time.Time.
func asUTC(t *testing.T, v any) time.Time {
	t.Helper()
	tm, ok := v.(time.Time)
	if !ok {
		t.Fatalf("value %#v (%T) is not time.Time", v, v)
	}
	return tm.UTC()
}

// assertValidSQLiteFile opens the produced file with the driver directly and
// confirms it is a real SQLite database with the expected row count.
func assertValidSQLiteFile(t *testing.T, path, table string, wantRows int) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open produced db: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRowContext(context.Background(), "SELECT count(*) FROM \""+table+"\"").Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if n != wantRows {
		t.Errorf("produced db row count = %d; want %d", n, wantRows)
	}
}
