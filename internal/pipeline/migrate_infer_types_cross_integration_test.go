//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine end-to-end integration test for ADR-0144 opt-in validated
// rich-type inference (`--infer-types`) on a SQLite source migrating into
// Postgres. This is the load-bearing value pin: per type FAMILY (boolean,
// timestamptz, timestamp(naive), jsonb, uuid) it seeds a CONFORMING column
// (promoted + the data round-trips into the rich PG type) AND a NON-CONFORMING
// column with the SAME name-hint (kept `text`/`bigint`, data intact) — crucially
// the `cus_abc123` non-UUID `*_id` case stays text. Every assertion is on the
// REAL PG target (column type via information_schema, plus value equality).
//
// The Bug-74 discipline: each family is exercised with both shapes, and the
// temporal family additionally distinguishes timestamptz (all values carry an
// offset) from timestamp (a naive value must NOT silently become tz).

package pipeline

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite" // pure-Go driver for seeding the temp source file

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// seedInferSource writes a temp SQLite file with, per family, a CONFORMING
// column (correct name-hint + clean data) and a NON-CONFORMING column (same
// hint, one bad value). The declared types resolve to the conservative source
// family the inference requires (INTEGER for boolean, TEXT for the rest).
func seedInferSource(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "infer.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite seed: %v", err)
	}
	defer func() { _ = db.Close() }()

	stmts := []string{
		`CREATE TABLE records (
			id          INTEGER PRIMARY KEY,
			name        TEXT NOT NULL,        -- no hint → stays text
			is_enabled  INTEGER,              -- boolean candidate (conforming)
			is_broken   INTEGER,              -- boolean candidate (non-conforming: a 2)
			created_at  TEXT,                 -- temporal candidate (all offset → timestamptz)
			synced_time TEXT,                 -- temporal candidate (naive → timestamp)
			touched_at  TEXT,                 -- temporal candidate (non-conforming)
			config_json TEXT,                 -- jsonb candidate (object/array)
			notes_json  TEXT,                 -- jsonb candidate (non-conforming)
			user_id     TEXT,                 -- uuid candidate (conforming)
			customer_id TEXT                  -- uuid candidate (non-conforming: cus_abc123)
		)`,
		`INSERT INTO records VALUES (
			1, 'alice', 1, 2,
			'2024-01-15T10:30:00+05:00', '2024-01-15 10:30:00', 'whenever',
			'{"theme":"dark","n":1}', 'not json',
			'550e8400-e29b-41d4-a716-446655440000', 'cus_abc123')`,
		`INSERT INTO records VALUES (
			2, 'bob', 0, 0,
			'2024-02-20T08:00:00-08:00', '2024-02-20 08:00:00', '2024-02-20',
			'[1,2,3]', '{"ok":true}',
			'6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'cust-002')`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed exec: %v", err)
		}
	}
	return path
}

// TestMigrate_InferTypes_SQLiteToPostgres is the per-family value proof.
func TestMigrate_InferTypes_SQLiteToPostgres(t *testing.T) {
	src := seedInferSource(t)
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
		Source:     sqliteEng,
		Target:     pgEng,
		SourceDSN:  src,
		TargetDSN:  pgTarget,
		InferTypes: true,
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (--infer-types SQLite→PG): %v", err)
	}

	pg, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = pg.Close() }()
	ctx := ctx2min(t)

	// ---- Column types on the REAL target (information_schema) ----
	wantType := map[string]string{
		"name":        "text",
		"is_enabled":  "boolean",                     // promoted
		"is_broken":   "bigint",                      // kept (non-conforming)
		"created_at":  "timestamp with time zone",    // promoted: all values carry an offset
		"synced_time": "timestamp without time zone", // promoted: naive → NOT tz
		"touched_at":  "text",                        // kept (non-conforming)
		"config_json": "jsonb",                       // promoted
		"notes_json":  "text",                        // kept (non-conforming)
		"user_id":     "uuid",                        // promoted
		"customer_id": "text",                        // kept: cus_abc123 is not a UUID
	}
	for col, want := range wantType {
		var got string
		if err := pg.QueryRowContext(ctx,
			`SELECT data_type FROM information_schema.columns
			 WHERE table_name = 'records' AND column_name = $1`, col).Scan(&got); err != nil {
			t.Fatalf("query data_type for %s: %v", col, err)
		}
		if got != want {
			t.Errorf("records.%s data_type = %q; want %q", col, got, want)
		}
	}

	// ---- Values round-trip into the rich types ----
	// boolean: 1→true, 0→false.
	var enabled1, enabled2 bool
	if err := pg.QueryRowContext(ctx, `SELECT is_enabled FROM records WHERE id = 1`).Scan(&enabled1); err != nil {
		t.Fatalf("select is_enabled id=1: %v", err)
	}
	if err := pg.QueryRowContext(ctx, `SELECT is_enabled FROM records WHERE id = 2`).Scan(&enabled2); err != nil {
		t.Fatalf("select is_enabled id=2: %v", err)
	}
	if !enabled1 || enabled2 {
		t.Errorf("is_enabled = (%v, %v); want (true, false)", enabled1, enabled2)
	}

	// timestamptz: the stored instant equals the offset-applied UTC instant.
	assertText(t, pg, ctx, `SELECT (created_at AT TIME ZONE 'UTC')::text FROM records WHERE id = 1`,
		"2024-01-15 05:30:00") // 10:30 +05:00 → 05:30 UTC
	assertText(t, pg, ctx, `SELECT (created_at AT TIME ZONE 'UTC')::text FROM records WHERE id = 2`,
		"2024-02-20 16:00:00") // 08:00 -08:00 → 16:00 UTC

	// timestamp (naive): the wall-clock value, no zone conversion.
	assertText(t, pg, ctx, `SELECT synced_time::text FROM records WHERE id = 1`, "2024-01-15 10:30:00")

	// jsonb: value equal (assert a key), and the kept text column intact.
	assertText(t, pg, ctx, `SELECT config_json->>'theme' FROM records WHERE id = 1`, "dark")
	assertText(t, pg, ctx, `SELECT notes_json FROM records WHERE id = 1`, "not json")

	// uuid: promoted value exact; the cus_abc123 customer_id kept as text intact.
	assertText(t, pg, ctx, `SELECT user_id::text FROM records WHERE id = 1`,
		"550e8400-e29b-41d4-a716-446655440000")
	assertText(t, pg, ctx, `SELECT customer_id FROM records WHERE id = 1`, "cus_abc123")

	// non-conforming integer kept as bigint, value intact.
	var broken int64
	if err := pg.QueryRowContext(ctx, `SELECT is_broken FROM records WHERE id = 1`).Scan(&broken); err != nil {
		t.Fatalf("select is_broken: %v", err)
	}
	if broken != 2 {
		t.Errorf("is_broken id=1 = %d; want 2 (kept, value intact)", broken)
	}
	// non-conforming temporal kept as text, value intact.
	assertText(t, pg, ctx, `SELECT touched_at FROM records WHERE id = 1`, "whenever")
}

// TestMigrate_InferTypes_ExplicitOverrideWins pins that an explicit
// --type-override on a candidate column beats inference: user_id would validate
// as a uuid, but the operator forced it to text — it lands text.
func TestMigrate_InferTypes_ExplicitOverrideWins(t *testing.T) {
	src := seedInferSource(t)
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	sqliteEng, _ := engines.Get("sqlite")
	pgEng, _ := engines.Get("postgres")

	mig := &Migrator{
		Source:     sqliteEng,
		Target:     pgEng,
		SourceDSN:  src,
		TargetDSN:  pgTarget,
		InferTypes: true,
		Mappings:   []config.Mapping{{Table: "records", Column: "user_id", TargetType: "text"}},
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	pg, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = pg.Close() }()
	ctx := ctx2min(t)

	var got string
	if err := pg.QueryRowContext(ctx,
		`SELECT data_type FROM information_schema.columns
		 WHERE table_name = 'records' AND column_name = 'user_id'`).Scan(&got); err != nil {
		t.Fatalf("query user_id data_type: %v", err)
	}
	if got != "text" {
		t.Errorf("user_id data_type = %q; want text (explicit --type-override must win over inference)", got)
	}
}

// assertText scans a single text result and compares it.
func assertText(t *testing.T, db *sql.DB, ctx context.Context, query, want string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(ctx, query).Scan(&got); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	if got != want {
		t.Errorf("%s = %q; want %q", query, got, want)
	}
}
