//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration coverage for `--analyze-after` (perf research delta 4):
// one cheap case per TARGET engine family — the phase dispatches
// through the family-specific [ir.TableAnalyzer] statement (PG ANALYZE
// / MySQL ANALYZE TABLE / SQLite ANALYZE), so a green run on one family
// does not cover the others (pin the class, not the representative).

package pipeline

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	// Register the engines this file drives.
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// TestMigrate_PG_AnalyzeAfter asserts the target-side ANALYZE actually
// ran: pg_stat_user_tables.last_analyze flips non-NULL for the migrated
// table. The stats view is collector-fed and slightly async, so the
// assertion polls briefly rather than reading once.
func TestMigrate_PG_AnalyzeAfter(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	seedLargeIntPK(t, sourceDSN, "events", 1_000)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mig := &Migrator{
		Source:       pgEng,
		Target:       pgEng,
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		AnalyzeAfter: true,
		MigrationID:  "test-analyze-after-pg",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	deadline := time.Now().Add(30 * time.Second)
	for {
		var lastAnalyze sql.NullTime
		err := tgtDB.QueryRowContext(ctx,
			"SELECT last_analyze FROM pg_stat_user_tables WHERE relname = 'events'").Scan(&lastAnalyze)
		if err == nil && lastAnalyze.Valid {
			return // ANALYZE observed — done.
		}
		if time.Now().After(deadline) {
			t.Fatalf("pg_stat_user_tables.last_analyze never flipped non-NULL (err=%v) — --analyze-after did not reach the target", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// TestMigrate_MySQL_AnalyzeAfter asserts the MySQL-family dispatch:
// the run exits 0 with the analyze phase-complete log accounting for
// the migrated table, and no failure counted (ANALYZE TABLE reports
// errors in its result set — the writer surfaces those, so
// tables_failed=0 is a real signal, not decoration).
func TestMigrate_MySQL_AnalyzeAfter(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQL(t)
	defer cleanup()

	srcDB, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := srcDB.ExecContext(ctx, "CREATE TABLE events (id BIGINT PRIMARY KEY, label VARCHAR(64) NOT NULL)"); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := srcDB.ExecContext(ctx, "INSERT INTO events (id, label) VALUES (1, 'a'), (2, 'b'), (3, 'c')"); err != nil {
		t.Fatalf("seed source rows: %v", err)
	}

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	logs := captureSlog(t)
	mig := &Migrator{
		Source:       myEng,
		Target:       myEng,
		SourceDSN:    sourceDSN,
		TargetDSN:    targetDSN,
		AnalyzeAfter: true,
		MigrationID:  "test-analyze-after-mysql",
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	out := logs.String()
	if !strings.Contains(out, "phase=analyze") || !strings.Contains(out, "tables_analyzed=1") {
		t.Errorf("expected the analyze phase-complete log with tables_analyzed=1; got:\n%s", out)
	}
	if strings.Contains(out, "analyze-after failed for table") {
		t.Errorf("analyze reported a per-table failure; got:\n%s", out)
	}
}

// TestMigrate_SQLite_AnalyzeAfter asserts the SQLite-family dispatch:
// ANALYZE materialises sqlite_stat1 on the target file with a row for
// the migrated table's index. The seed carries a secondary index so the
// stat table has a deterministic row to look for (an index-less rowid
// table may legitimately produce none).
func TestMigrate_SQLite_AnalyzeAfter(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src.db")
	dst := filepath.Join(t.TempDir(), "dst.db")

	srcDB, err := sql.Open("sqlite", src)
	if err != nil {
		t.Fatalf("open sqlite seed: %v", err)
	}
	for _, stmt := range []string{
		"CREATE TABLE events (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
		"CREATE INDEX idx_events_label ON events(label)",
		"INSERT INTO events (id, label) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
	} {
		if _, err := srcDB.Exec(stmt); err != nil {
			_ = srcDB.Close()
			t.Fatalf("seed %q: %v", stmt, err)
		}
	}
	_ = srcDB.Close()

	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	mig := &Migrator{
		Source:       sqliteEng,
		Target:       sqliteEng,
		SourceDSN:    src,
		TargetDSN:    dst,
		AnalyzeAfter: true,
		MigrationID:  "test-analyze-after-sqlite",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	dstDB, err := sql.Open("sqlite", dst)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = dstDB.Close() }()

	// sqlite_stat1 exists ONLY after an ANALYZE has run on the file.
	var statTables int
	if err := dstDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE name = 'sqlite_stat1'").Scan(&statTables); err != nil {
		t.Fatalf("probe sqlite_master: %v", err)
	}
	if statTables != 1 {
		t.Fatal("sqlite_stat1 missing on the target — ANALYZE never ran")
	}
	var statRows int
	if err := dstDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl = 'events'").Scan(&statRows); err != nil {
		t.Fatalf("read sqlite_stat1: %v", err)
	}
	if statRows == 0 {
		t.Error("sqlite_stat1 has no row for the migrated table — per-table ANALYZE missed it")
	}
}
