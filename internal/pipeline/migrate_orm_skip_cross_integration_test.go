//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine end-to-end proof for ADR-0143 ORM-table loud-skip-by-default:
// a SQLite source carrying ORM migration-bookkeeping tables (a distinctive
// flyway/prisma name, a generic schema_migrations matching the Rails shape)
// alongside ordinary application tables — and a generic-NAME collision table
// (`migrations` whose columns are NOT Laravel's) — migrates into Postgres.
//
//   - With SkipORMTables on (the CLI default), the recognized bookkeeping
//     tables do NOT land on the target, while the application tables AND the
//     name-collision table (kept as application data) do.
//   - With SkipORMTables off (the --include-orm-tables / programmatic
//     zero-value default), every table lands — byte-identical to before the
//     feature.

package pipeline

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite" // pure-Go driver for seeding the temp source file

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// seedSQLiteORMSource writes a temp SQLite file with two ORM bookkeeping
// tables (flyway distinctive, schema_migrations generic-Rails-shape), one
// application table (users), and a generic-NAME collision table (migrations
// shaped like a real app table, NOT Laravel's id/migration/batch).
func seedSQLiteORMSource(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "orm.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite seed: %v", err)
	}
	defer func() { _ = db.Close() }()

	stmts := []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`,
		`INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')`,
		// Distinctive ORM name → recognized on name alone.
		`CREATE TABLE flyway_schema_history (installed_rank INTEGER PRIMARY KEY, description TEXT)`,
		`INSERT INTO flyway_schema_history (installed_rank, description) VALUES (1, 'baseline')`,
		// Generic ORM name with the matching Rails shape (one text version col).
		`CREATE TABLE schema_migrations (version TEXT PRIMARY KEY)`,
		`INSERT INTO schema_migrations (version) VALUES ('20240101000000')`,
		// Generic NAME collision: a real application table that happens to be
		// called `migrations` but is NOT Laravel's {id,migration,batch} shape.
		// Must be KEPT (loud-failure / no-silent-loss), never skipped.
		`CREATE TABLE migrations (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, body TEXT)`,
		`INSERT INTO migrations (id, user_id, body) VALUES (1, 1, 'note')`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed exec: %v", err)
		}
	}
	return path
}

// migrateORMSkipToPG migrates the ORM seed into a fresh Postgres target with
// the given SkipORMTables setting and returns the lowercased target table-name
// set.
func migrateORMSkipToPG(t *testing.T, skip bool) map[string]bool {
	t.Helper()
	src := seedSQLiteORMSource(t)
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
		Source:        sqliteEng,
		Target:        pgEng,
		SourceDSN:     src,
		TargetDSN:     pgTarget,
		SkipORMTables: skip,
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (SQLite→PG, skip=%v): %v", skip, err)
	}

	ctx := ctx2min(t)
	sr, err := pgEng.OpenSchemaReader(ctx, pgTarget)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer migcore.CloseIf(sr)
	got, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	names := make(map[string]bool, len(got.Tables))
	for _, tb := range got.Tables {
		names[tb.Name] = true
	}
	return names
}

// TestMigrate_ORMSkipDefault pins loud-skip-by-default (SkipORMTables=true):
// the recognized bookkeeping tables are absent; the application table and the
// generic-name collision table are present.
func TestMigrate_ORMSkipDefault(t *testing.T) {
	names := migrateORMSkipToPG(t, true)

	for _, present := range []string{"users", "migrations"} {
		if !names[present] {
			t.Errorf("table %q missing from target; want present (have %v)", present, names)
		}
	}
	for _, absent := range []string{"flyway_schema_history", "schema_migrations"} {
		if names[absent] {
			t.Errorf("ORM table %q landed on target; want skipped (have %v)", absent, names)
		}
	}
}

// TestMigrate_ORMKeepWithFlag pins the opt-out (SkipORMTables=false, the
// --include-orm-tables / programmatic zero-value default): every table lands.
func TestMigrate_ORMKeepWithFlag(t *testing.T) {
	names := migrateORMSkipToPG(t, false)

	for _, present := range []string{"users", "migrations", "flyway_schema_history", "schema_migrations"} {
		if !names[present] {
			t.Errorf("table %q missing from target with skip off; want present (have %v)", present, names)
		}
	}
}
