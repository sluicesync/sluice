//go:build psverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pipeline-level verification tests for PlanetScale Postgres
// (PS-PG). Companion to internal/engines/postgres/planetscale_verify_test.go;
// the engine-package tests cover Phase 1 (connectivity), Phase 2
// (schema reader), and Phase 4 (CDC reader). The orchestrator-level
// phases live here because they exercise pipeline.Migrator and
// pipeline.Streamer, which the engine package can't import.
//
// Usage (from repo root):
//
//	go test -tags=psverify -v -count=1 -timeout=15m \
//	  -run 'TestPSPipeline' ./internal/pipeline/...
//
// Each phase that creates objects on PS-PG drops them at the end so
// the same database can host repeated runs.

package pipeline

import (
	"bufio"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// dsnsAllPS returns the four PS DSNs in the credentials file:
// PS-MySQL source/destination and PS-PG source/destination. Skips
// cleanly when any of the four is missing.
func dsnsAllPS(t *testing.T) (mysqlSrc, mysqlDest, pgSrc, pgDest string) {
	t.Helper()
	mysqlSrc = lookupPSCred(t, "SLUICE_MYSQL_SOURCE")
	mysqlDest = lookupPSCred(t, "SLUICE_MYSQL_DESTINATION")
	pgSrc = lookupPSCred(t, "SLUICE_POSTGRES_SOURCE")
	pgDest = lookupPSCred(t, "SLUICE_POSTGRES_DESTINATION")
	missing := []string{}
	if mysqlSrc == "" {
		missing = append(missing, "SLUICE_MYSQL_SOURCE")
	}
	if mysqlDest == "" {
		missing = append(missing, "SLUICE_MYSQL_DESTINATION")
	}
	if pgSrc == "" {
		missing = append(missing, "SLUICE_POSTGRES_SOURCE")
	}
	if pgDest == "" {
		missing = append(missing, "SLUICE_POSTGRES_DESTINATION")
	}
	if len(missing) > 0 {
		t.Skipf("missing PS credentials: %s", strings.Join(missing, ","))
	}
	return mysqlSrc, mysqlDest, pgSrc, pgDest
}

// dsnsPGOnly returns just the PS-PG source/destination — saves a
// skip when only the PG-side phases are running.
func dsnsPGOnly(t *testing.T) (pgSrc, pgDest string) {
	t.Helper()
	pgSrc = lookupPSCred(t, "SLUICE_POSTGRES_SOURCE")
	pgDest = lookupPSCred(t, "SLUICE_POSTGRES_DESTINATION")
	if pgSrc == "" || pgDest == "" {
		t.Skip("PS-PG source/destination DSNs not found")
	}
	return pgSrc, pgDest
}

// lookupPSCred mirrors the postgres-package helper but lives here so
// the file is self-contained. Reads env first, then walks up to the
// repo-root credentials file.
func lookupPSCred(t *testing.T, key string) string {
	t.Helper()
	if v := os.Getenv(key); v != "" {
		return v
	}
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for i := 0; i < 8; i++ {
		candidate := filepath.Join(dir, "PLANETSCALE_CREDENTIALS.env")
		if _, err := os.Stat(candidate); err == nil {
			return parsePSEnv(t, candidate, key)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

func parsePSEnv(t *testing.T, path, key string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()
	prefix := key + "="
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || !strings.HasPrefix(line, prefix) {
			continue
		}
		val := strings.TrimSpace(strings.TrimPrefix(line, prefix))
		if (strings.HasPrefix(val, `"`) && strings.HasSuffix(val, `"`)) ||
			(strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'")) {
			val = val[1 : len(val)-1]
		}
		return val
	}
	return ""
}

// TestPSPipeline_MigrateMigratePGToPG is Phase 3 of the verification
// plan: simple-mode migration with PS-PG on both sides. Seeds a
// small fixture on the source, runs pipeline.Migrator, asserts the
// rows arrive on the destination.
func TestPSPipeline_MigratePGToPG(t *testing.T) {
	pgSrc, pgDest := dsnsPGOnly(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const schemaName = "sluice_psverify_mig"

	// Reset the schema on both sides — repeatable test runs need a
	// clean slate.
	for _, dsn := range []string{pgSrc, pgDest} {
		dropPSSchema(t, ctx, dsn, schemaName)
		if err := execPS(t, ctx, dsn, "CREATE SCHEMA "+schemaName); err != nil {
			t.Fatalf("create schema: %v", err)
		}
	}
	defer func() {
		for _, dsn := range []string{pgSrc, pgDest} {
			dropPSSchema(t, context.Background(), dsn, schemaName)
		}
	}()

	const seedDDL = `
		CREATE TABLE sluice_psverify_mig.users (
			id    BIGINT       PRIMARY KEY,
			email VARCHAR(255) NOT NULL UNIQUE,
			active BOOLEAN     NOT NULL DEFAULT TRUE
		);
		INSERT INTO sluice_psverify_mig.users (id, email, active) VALUES
			(1, 'alice@example.com', TRUE),
			(2, 'bob@example.com',   FALSE);
	`
	if err := execPS(t, ctx, pgSrc, seedDDL); err != nil {
		t.Fatalf("seed: %v", err)
	}

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: withPSSchema(pgSrc, schemaName),
		TargetDSN: withPSSchema(pgDest, schemaName),
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	got := psSelectAllEmails(t, ctx, pgDest, schemaName, "users")
	want := []string{"alice@example.com", "bob@example.com"}
	if !equalPSStringSlices(got, want) {
		t.Errorf("destination emails = %v; want %v", got, want)
	}
}

// TestPSPipeline_StreamerPGToPG is Phase 5: snapshot+CDC handoff
// with PS-PG on both sides. Skipped when the source's wal_level is
// not 'logical'.
func TestPSPipeline_StreamerPGToPG(t *testing.T) {
	pgSrc, pgDest := dsnsPGOnly(t)

	preCtx, preCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer preCancel()
	walLevel := psQueryString(t, preCtx, pgSrc,
		"SELECT setting FROM pg_settings WHERE name = 'wal_level'")
	if walLevel != "logical" {
		t.Skipf("source wal_level = %q; CDC needs 'logical'", walLevel)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// A previous failed run may have left a `sluice_slot` replication
	// slot behind on the source. Drop it BEFORE the schema cleanup
	// because PG can refuse to drop a schema whose tables are
	// referenced by an active publication tied to the slot, and the
	// slot itself can hold the publication's catalog snapshot. Slot
	// teardown is deferred too so reruns are reliable even if this
	// test fails mid-flight.
	step(t, "pre-clean: drop leftover slot", func() {
		dropPSSlotIfExists(t, pgSrc, "sluice_slot")
	})
	defer dropPSSlotIfExists(t, pgSrc, "sluice_slot")

	const schemaName = "sluice_psverify_stream"
	step(t, "pre-clean: drop and recreate test schemas", func() {
		for _, dsn := range []string{pgSrc, pgDest} {
			dropPSSchema(t, ctx, dsn, schemaName)
			if err := execPS(t, ctx, dsn, "CREATE SCHEMA "+schemaName); err != nil {
				t.Fatalf("create schema: %v", err)
			}
		}
	})
	defer func() {
		for _, dsn := range []string{pgSrc, pgDest} {
			dropPSSchema(t, context.Background(), dsn, schemaName)
		}
	}()

	const seedDDL = `
		CREATE TABLE sluice_psverify_stream.users (
			id    BIGINT       PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE sluice_psverify_stream.users REPLICA IDENTITY FULL;
		INSERT INTO sluice_psverify_stream.users (id, email) VALUES (1, 'r1@example.com');
	`
	step(t, "seed source with R1", func() {
		if err := execPS(t, ctx, pgSrc, seedDDL); err != nil {
			t.Fatalf("seed: %v", err)
		}
	})

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: withPSSchema(pgSrc, schemaName),
		TargetDSN: withPSSchema(pgDest, schemaName),
		StreamID:  "psverify-pg-stream",
	}

	t.Logf("phase B: starting Streamer.Run")
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// waitOrSurfaceErr races the row-count poll against the
	// streamer's run-error channel. Without this, an early failure
	// in Streamer.Run (slot creation, snapshot import) surfaces only
	// as a 60s "never delivered" timeout — useless for diagnosis.
	waitOrSurfaceErr := func(want int, label string) {
		t.Helper()
		deadline := time.NewTimer(60 * time.Second)
		defer deadline.Stop()
		for {
			select {
			case err := <-runErr:
				if err != nil {
					t.Fatalf("%s: Streamer.Run returned early with error: %v", label, err)
				}
				t.Fatalf("%s: Streamer.Run returned nil before delivering rows", label)
			case <-deadline.C:
				t.Fatalf("%s: timeout waiting for %d rows on destination", label, want)
			default:
			}
			db, err := sql.Open("pgx", pgDest)
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			var n int
			qErr := db.QueryRowContext(
				queryCtx,
				"SELECT COUNT(*) FROM "+schemaName+".users",
			).Scan(&n)
			cancel()
			_ = db.Close()
			if qErr == nil && n >= want {
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	waitOrSurfaceErr(1, "phase D (bulk-copy R1)")

	if err := execPS(
		t, ctx, pgSrc,
		"INSERT INTO sluice_psverify_stream.users (id, email) VALUES (2, 'r2@example.com');",
	); err != nil {
		t.Fatalf("R2 insert: %v", err)
	}
	waitOrSurfaceErr(2, "phase E (CDC R2)")

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestPSPipeline_MigrateMySQLToPG is Phase 6: cross-engine
// PS-MySQL → PS-PG, simple-mode. The PlanetScale-MySQL engine
// declares CDC=None, so only the snapshot path is exercisable.
func TestPSPipeline_MigrateMySQLToPG(t *testing.T) {
	mysqlSrc, _, _, pgDest := dsnsAllPS(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// MySQL side uses the database-name from the DSN as its schema.
	// We seed into a sluice-scoped TABLE within whatever DB the DSN
	// targets — PS-MySQL doesn't permit ad-hoc CREATE DATABASE on
	// most plans, so we don't try.
	mysqlEng, ok := engines.Get("planetscale")
	if !ok {
		t.Fatal("planetscale (mysql) engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const tableName = "sluice_psverify_xeng_users"
	const targetSchema = "sluice_psverify_xeng"

	// MySQL cleanup: drop just the test table.
	if err := execPSMySQL(
		t, ctx, mysqlSrc,
		"DROP TABLE IF EXISTS "+tableName,
	); err != nil {
		t.Fatalf("pre-clean mysql: %v", err)
	}
	defer func() {
		_ = execPSMySQL(t, context.Background(), mysqlSrc, "DROP TABLE IF EXISTS "+tableName)
	}()

	// PG cleanup: drop and recreate the schema.
	dropPSSchema(t, ctx, pgDest, targetSchema)
	if err := execPS(t, ctx, pgDest, "CREATE SCHEMA "+targetSchema); err != nil {
		t.Fatalf("create pg schema: %v", err)
	}
	defer dropPSSchema(t, context.Background(), pgDest, targetSchema)

	const seedDDL = `
		CREATE TABLE ` + tableName + ` (
			id     BIGINT       NOT NULL AUTO_INCREMENT,
			email  VARCHAR(255) NOT NULL,
			active TINYINT(1)   NOT NULL DEFAULT 1,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	if err := execPSMySQL(t, ctx, mysqlSrc, seedDDL); err != nil {
		t.Fatalf("mysql seed DDL: %v", err)
	}
	if err := execPSMySQL(
		t, ctx, mysqlSrc,
		"INSERT INTO "+tableName+" (email, active) VALUES "+
			"('alice@example.com', 1), ('bob@example.com', 0);",
	); err != nil {
		t.Fatalf("mysql seed insert: %v", err)
	}

	mig := &Migrator{
		Source:    mysqlEng,
		Target:    pgEng,
		SourceDSN: mysqlSrc,
		TargetDSN: withPSSchema(pgDest, targetSchema),
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	got := psSelectAllEmails(t, ctx, pgDest, targetSchema, tableName)
	want := []string{"alice@example.com", "bob@example.com"}
	if !equalPSStringSlices(got, want) {
		t.Errorf("PG target emails = %v; want %v", got, want)
	}
}

// ---- Helpers — small wrappers over database/sql so the phases
//      above stay readable. The PG helpers default to pgx; the MySQL
//      helper opens the standard go-sql-driver/mysql DSN. ----

func execPS(t *testing.T, ctx context.Context, dsn, sqlText string) error {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(ctx, sqlText)
	return err
}

func execPSMySQL(t *testing.T, ctx context.Context, dsn, sqlText string) error {
	t.Helper()
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(ctx, sqlText)
	return err
}

// dropPSSchema is best-effort cleanup. Hard-capped at 30s in case a
// stale lock (from a streamer goroutine that hasn't fully unwound
// yet) blocks the DROP SCHEMA CASCADE — cleanup must never hang the
// test, even when something upstream is holding resources.
func dropPSSchema(t *testing.T, _ context.Context, dsn, schema string) {
	t.Helper()
	const cap = 30 * time.Second
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), cap)
		defer cancel()
		if err := execPS(t, ctx, dsn, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Logf("drop schema %s: %v", schema, err)
		}
	}()
	select {
	case <-done:
	case <-time.After(cap + 2*time.Second):
		t.Logf("drop schema %s: timed out after %v; proceeding", schema, cap)
	}
}

// dropPSSlotIfExists is a best-effort cleanup of a leftover
// replication slot. pg_drop_replication_slot blocks if the slot is
// still marked "active" (a previous failed run can leave it that
// way for tens of seconds), and on managed services the Postgres-
// level cancel packet doesn't always reach the backend in time.
// We hard-cap the whole operation in a goroutine so cleanup never
// hangs the test, even when context cancellation isn't honoured.
//
// Failure to drop the slot here isn't fatal — the next
// CREATE_REPLICATION_SLOT will surface "slot already exists" with
// a clear message that operators recognise.
func dropPSSlotIfExists(t *testing.T, dsn, slotName string) {
	t.Helper()
	const cap = 15 * time.Second
	done := make(chan struct{})
	go func() {
		defer close(done)
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Logf("slot pre-clean open: %v", err)
			return
		}
		defer func() { _ = db.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), cap)
		defer cancel()

		var exists bool
		err = db.QueryRowContext(
			ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
			slotName,
		).Scan(&exists)
		if err != nil {
			t.Logf("slot pre-clean lookup: %v", err)
			return
		}
		if !exists {
			return
		}
		if _, err := db.ExecContext(
			ctx,
			"SELECT pg_drop_replication_slot($1)", slotName,
		); err != nil {
			t.Logf("drop slot %s: %v", slotName, err)
		}
	}()
	select {
	case <-done:
	case <-time.After(cap + 2*time.Second):
		t.Logf("drop slot %s: timed out after %v (PS-PG may not honour cancellation for replication-related calls); proceeding", slotName, cap)
	}
}

func psQueryString(t *testing.T, ctx context.Context, dsn, query string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var s string
	if err := db.QueryRowContext(ctx, query).Scan(&s); err != nil {
		t.Fatalf("query: %v", err)
	}
	return s
}

func waitForPSRowCount(
	t *testing.T,
	ctx context.Context,
	dsn, schema, table string,
	want int, timeout time.Duration,
) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		var n int
		err = db.QueryRowContext(
			queryCtx,
			"SELECT COUNT(*) FROM "+schema+"."+table,
		).Scan(&n)
		cancel()
		_ = db.Close()
		if err == nil && n >= want {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func psSelectAllEmails(t *testing.T, ctx context.Context, dsn, schema, table string) []string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(ctx,
		"SELECT email FROM "+schema+"."+table+" ORDER BY email")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, s)
	}
	return out
}

func withPSSchema(dsn, schema string) string {
	if strings.Contains(dsn, "schema=") {
		panic("dsn already has schema= param; refusing to mutate")
	}
	if strings.Contains(dsn, "?") {
		return dsn + "&schema=" + schema
	}
	return dsn + "?schema=" + schema
}

// step wraps a setup-phase block with start/finish logging so the
// test output shows exactly which step is running at any moment.
// Useful when an external service is mid-failure: a test that hangs
// at "phase A" tells the operator something different than one that
// hangs at "phase D".
func step(t *testing.T, name string, fn func()) {
	t.Helper()
	t.Logf("→ %s", name)
	start := time.Now()
	fn()
	t.Logf("✓ %s (%v)", name, time.Since(start).Round(time.Millisecond))
}

func equalPSStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
