//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 164 regression: the CDC applier must bypass target FOREIGN KEY (and
// user-trigger) enforcement during replay, because a CDC change stream is NOT
// FK-dependency-ordered. A SQLite source with the default PRAGMA
// foreign_keys=OFF happily emits a parent-DELETE that orphans a child, and a
// child-INSERT that references a not-yet/never-existing parent. Before the fix
// the PG applier (no session_replication_role=replica) and the MySQL applier
// (no foreign_key_checks=0) rejected those replayed changes against the target
// FK constraint (PG 23503 / MySQL 1452), failed the apply tx, and HALTED the
// sync into a warm-resume poison-pill loop. With the fix the orphaning change
// replicates faithfully (the target mirrors the source, including the source's
// own FK-inconsistencies) and the sync continues.
//
// The source is a temp SQLite file driven through the sqlite-trigger CDC engine
// (the easiest way to produce a non-dependency-ordered stream); only the TARGET
// is a container.

package pipeline

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver for seeding + mutating the temp source file

	"sluicesync.dev/sluice/internal/engines"
	// sqlitetrigger is imported by name (for Setup/EngineName) — that import
	// also runs its init() registration, so no separate blank import is needed.
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// seedSQLiteTriggerFKSource writes a temp WAL-mode SQLite file with a parent
// table (wide) and a child table (fk_child) whose parent_id REFERENCES wide(id)
// with the default NO ACTION on delete, seeds an FK-consistent set, installs the
// trigger-CDC artifacts on both tables, and returns the path. SQLite's default
// PRAGMA foreign_keys=OFF means subsequent orphaning DML on the source is
// allowed — exactly the non-dependency-ordered stream Bug 164 is about.
func seedSQLiteTriggerFKSource(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fk.db")
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open sqlite seed: %v", err)
	}
	defer func() { _ = db.Close() }()
	stmts := []string{
		`PRAGMA journal_mode=WAL`,
		`CREATE TABLE wide (id INTEGER PRIMARY KEY, rowsum INTEGER NOT NULL)`,
		`CREATE TABLE fk_child (
			id        INTEGER PRIMARY KEY,
			parent_id INTEGER NOT NULL,
			FOREIGN KEY (parent_id) REFERENCES wide(id)
		)`,
		`INSERT INTO wide (id, rowsum) VALUES (1, 10), (2, 20), (3, 30)`,
		`INSERT INTO fk_child (id, parent_id) VALUES (100, 1), (200, 2)`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed exec %q: %v", s, err)
		}
	}
	if _, err := sqlitetrigger.Setup(context.Background(), path, sqlitetrigger.SetupOptions{
		Tables: []string{"wide", "fk_child"},
	}); err != nil {
		t.Fatalf("sqlitetrigger.Setup: %v", err)
	}
	return path
}

// TestStreamer_FKBypass_SQLiteTriggerToPostgres is the Bug 164 PG-target proof.
func TestStreamer_FKBypass_SQLiteTriggerToPostgres(t *testing.T) {
	// applyConcurrency 0 = the serial/pipelined apply path (the streamer's
	// default fast path on a PG target).
	runFKBypassPG(t, "fk-bypass-pg", 0)
}

// TestStreamer_FKBypass_SQLiteTriggerToPostgres_ConcurrentLanes pins the bypass
// on the ADR-0105 CONCURRENT key-hash lane path (ApplyLaneBatch). The default
// test above does not engage lanes, and the other concurrent-apply integration
// tests run FK-consistent streams — so without this variant the lane path's FK
// bypass (a separate tx-open site from serial/pipelined) is unproven for an
// orphaning change. --apply-concurrency=2 forces W>1 → the lane path.
func TestStreamer_FKBypass_SQLiteTriggerToPostgres_ConcurrentLanes(t *testing.T) {
	runFKBypassPG(t, "fk-bypass-pg-w2", 2)
}

// runFKBypassPG drives the Bug-164 PG-target proof at the given apply
// concurrency (0 = serial/pipelined default, >1 = concurrent lanes).
func runFKBypassPG(t *testing.T, streamID string, applyConcurrency int) {
	t.Helper()
	src := seedSQLiteTriggerFKSource(t)
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	srcEng, ok := engines.Get(sqlitetrigger.EngineName)
	if !ok {
		t.Fatal("sqlite-trigger engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:           srcEng,
		Target:           pgEng,
		SourceDSN:        src,
		TargetDSN:        pgTarget,
		StreamID:         streamID,
		ApplyConcurrency: applyConcurrency,
	}
	// The concurrent key-hash lane path (ApplyLaneBatch) lives inside the
	// batched apply; the streamer only routes through ApplyBatch when
	// ApplyBatchSize > 1 (else it uses per-change Apply, which never engages
	// lanes). So a lane-path variant must set both.
	if applyConcurrency > 1 {
		streamer.ApplyBatchSize = 100
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()

	// Cold-start lands the FK-consistent seed: 3 parents + 2 children.
	if !waitForRowCount(t, pgTarget, "wide", 3, 90*time.Second) {
		cancel()
		t.Fatal("cold-start never delivered the 3 wide rows to PG")
	}
	if !waitForRowCount(t, pgTarget, "fk_child", 2, 30*time.Second) {
		cancel()
		t.Fatal("cold-start never delivered the 2 fk_child rows to PG")
	}
	// The test is only meaningful if the FK constraint actually landed on the
	// target — otherwise an orphaning change wouldn't be rejected and a pass
	// would be vacuous. The constraints phase runs after bulk_copy, so poll.
	if !waitForForeignKeyPG(t, pgTarget, "fk_child", 30*time.Second) {
		cancel()
		t.Fatal("target fk_child never got its FK constraint (test would be vacuous without it)")
	}

	// Non-dependency-ordered CDC on the source (FK off, so SQLite allows it):
	//   (a) DELETE a parent that still has a child  -> orphans fk_child id=100
	//   (b) INSERT a child referencing a non-existent parent (999)
	// Before the fix each would be rejected by the PG FK (23503) and halt sync.
	sqliteExec(t, src, `DELETE FROM wide WHERE id = 1`)
	sqliteExec(t, src, `INSERT INTO fk_child (id, parent_id) VALUES (300, 999)`)

	// The parent delete replicated (orphaning its child), and the orphan child
	// insert replicated — without crashing the applier.
	if !waitForRowPresent(t, pgTarget, "wide", 1, false, 60*time.Second) {
		cancel()
		t.Fatal("CDC parent DELETE (wide id=1) never propagated to PG (applier halted on FK?)")
	}
	if !waitForRowPresent(t, pgTarget, "fk_child", 300, true, 30*time.Second) {
		cancel()
		t.Fatal("CDC orphan child INSERT (fk_child id=300) never propagated to PG (applier halted on FK?)")
	}
	// The orphaned child (id=100, parent_id=1) is still there — the target
	// faithfully mirrors the source's FK-inconsistency, not silently dropped.
	if !pgRowExists(t, pgTarget, "fk_child", 100) {
		t.Error("orphaned fk_child id=100 missing on PG; the parent delete must NOT cascade/drop the child")
	}

	// Sync is still alive: a subsequent ordinary change converges.
	sqliteExec(t, src, `INSERT INTO wide (id, rowsum) VALUES (4, 40)`)
	if !waitForRowPresent(t, pgTarget, "wide", 4, true, 30*time.Second) {
		cancel()
		t.Fatal("post-orphan ordinary INSERT (wide id=4) never landed — sync did not continue")
	}

	// The streamer must not have exited with an error.
	select {
	case err := <-runErr:
		t.Fatalf("Streamer.Run exited early: %v", err)
	default:
	}

	cancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_FKBypass_SQLiteTriggerToMySQL is the Bug 164 MySQL-target proof.
func TestStreamer_FKBypass_SQLiteTriggerToMySQL(t *testing.T) {
	src := seedSQLiteTriggerFKSource(t)
	_, myTarget, myCleanup := startMySQL(t)
	defer myCleanup()

	srcEng, ok := engines.Get(sqlitetrigger.EngineName)
	if !ok {
		t.Fatal("sqlite-trigger engine not registered")
	}
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	streamer := &Streamer{
		Source:    srcEng,
		Target:    myEng,
		SourceDSN: src,
		TargetDSN: myTarget,
		StreamID:  "fk-bypass-mysql",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()

	if !waitForRowCountMySQL(t, myTarget, "wide", 3, 90*time.Second) {
		cancel()
		t.Fatal("cold-start never delivered the 3 wide rows to MySQL")
	}
	if !waitForRowCountMySQL(t, myTarget, "fk_child", 2, 30*time.Second) {
		cancel()
		t.Fatal("cold-start never delivered the 2 fk_child rows to MySQL")
	}
	if !waitForForeignKeyMySQL(t, myTarget, "fk_child", 30*time.Second) {
		cancel()
		t.Fatal("target fk_child never got its FK constraint (test would be vacuous without it)")
	}

	sqliteExec(t, src, `DELETE FROM wide WHERE id = 1`)
	sqliteExec(t, src, `INSERT INTO fk_child (id, parent_id) VALUES (300, 999)`)

	if !waitForRowPresentMySQL(t, myTarget, "wide", 1, false, 60*time.Second) {
		cancel()
		t.Fatal("CDC parent DELETE (wide id=1) never propagated to MySQL (applier halted on FK?)")
	}
	if !waitForRowPresentMySQL(t, myTarget, "fk_child", 300, true, 30*time.Second) {
		cancel()
		t.Fatal("CDC orphan child INSERT (fk_child id=300) never propagated to MySQL (applier halted on FK?)")
	}
	if !mysqlRowExists(t, myTarget, "fk_child", 100) {
		t.Error("orphaned fk_child id=100 missing on MySQL; the parent delete must NOT cascade/drop the child")
	}

	sqliteExec(t, src, `INSERT INTO wide (id, rowsum) VALUES (4, 40)`)
	if !waitForRowPresentMySQL(t, myTarget, "wide", 4, true, 30*time.Second) {
		cancel()
		t.Fatal("post-orphan ordinary INSERT (wide id=4) never landed — sync did not continue")
	}

	select {
	case err := <-runErr:
		t.Fatalf("Streamer.Run exited early: %v", err)
	default:
	}

	cancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// --- PG helpers (generic table/id) ---

// pgRowExists reports whether table has a row with id on the PG target.
func pgRowExists(t *testing.T, dsn, table string, id int64) bool {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return false
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var exists bool
	// table is a fixed test literal, not user input.
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM `+table+` WHERE id = $1)`, id).Scan(&exists); err != nil {
		return false
	}
	return exists
}

// waitForRowPresent polls until table/id presence on PG equals want.
func waitForRowPresent(t *testing.T, dsn, table string, id int64, want bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pgRowExists(t, dsn, table, id) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// pgForeignKeyCount returns the number of FOREIGN KEY constraints on table.
func pgForeignKeyCount(t *testing.T, dsn, table string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return -1
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM information_schema.table_constraints
		 WHERE table_name = $1 AND constraint_type = 'FOREIGN KEY'`, table).Scan(&n); err != nil {
		return -1
	}
	return n
}

// waitForForeignKeyPG polls until table has at least one FK constraint on PG.
func waitForForeignKeyPG(t *testing.T, dsn, table string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pgForeignKeyCount(t, dsn, table) >= 1 {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// --- MySQL helpers (generic table/id) ---

// (mysqlRowExists — generic table/id COUNT(*)>0 — is defined in
// backup_parallel_mysql_integration_test.go and reused here.)

func waitForRowPresentMySQL(t *testing.T, dsn, table string, id int64, want bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if mysqlRowExists(t, dsn, table, id) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// mysqlForeignKeyCount counts FK column references on table via the canonical
// key_column_usage detector (referenced_table_name IS NOT NULL) — more robust
// than table_constraints across MySQL versions.
func mysqlForeignKeyCount(t *testing.T, dsn, table string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return -1
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM information_schema.key_column_usage
		 WHERE table_schema = DATABASE() AND table_name = ?
		   AND referenced_table_name IS NOT NULL`, table).Scan(&n); err != nil {
		return -1
	}
	return n
}

// waitForForeignKeyMySQL polls until table has at least one FK on MySQL.
func waitForForeignKeyMySQL(t *testing.T, dsn, table string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if mysqlForeignKeyCount(t, dsn, table) >= 1 {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}
