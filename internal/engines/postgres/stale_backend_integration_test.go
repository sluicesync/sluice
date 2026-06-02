//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for stale-backend detection + opt-in reaping
// (connection-resilience Phase 2, item 2). Boots a real PG container,
// opens a SECOND connection labelled like a sluice backend that holds a
// lock inside an open transaction (the orphan shape a SIGKILL'd COPY
// leaves behind), and ground-truths the detector against the live
// pg_stat_activity / pg_locks catalogs:
//
//   - detection finds the labelled, lock-holding backend;
//   - with reap=true it is terminated;
//   - a NON-sluice backend (no `sluice/` application_name) holding the
//     same kind of lock is left untouched by both detection and reaping
//     (the safety bound).
//
// The pure predicate / scope / formatting layer is unit-tested in
// stale_backend_test.go; this file rides the real catalog so a SQL
// fat-finger or a PG-version drift surfaces here.

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// openLabelledIdleInTx opens a connection with the given application_name,
// creates the table (if needed), then BEGINs and LOCKs it in
// AccessExclusive mode — leaving the backend "idle in transaction" while
// holding a relation lock. Returns a pinned *sql.Conn (kept open by the
// caller so the backend persists) and its server-side pid.
//
// The lock-holding transaction MUST live on a single pinned backend.
// Running BEGIN / LOCK TABLE as separate db.ExecContext calls on a
// *sql.DB does not achieve this even with SetMaxOpenConns(1): the pgx
// stdlib driver implements driver.SessionResetter, so when the BEGIN
// connection is returned to the pool it reports a non-idle TxStatus,
// database/sql discards it as a bad conn, and the next statement opens a
// fresh backend with no open transaction (LOCK TABLE then fails with
// SQLSTATE 25P01, "can only be used in transaction blocks"). Checking out
// one *sql.Conn and holding it keeps every statement on the same backend
// and defers the session reset until Close.
func openLabelledIdleInTx(t *testing.T, dsn, appName, table string) (conn *sql.Conn, pid int) {
	t.Helper()
	labelled := dsn + "&application_name=" + appName

	db, err := sql.Open("pgx", labelled)
	if err != nil {
		t.Fatalf("open labelled connection: %v", err)
	}
	// The parent pool outlives this function via the pinned conn; close it
	// when the test ends (after the caller's defer conn.Close() has run).
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Pin one backend for the whole lock-holding transaction.
	conn, err = db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin connection: %v", err)
	}

	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+table+` (id INT PRIMARY KEY)`); err != nil {
		_ = conn.Close()
		t.Fatalf("create %s: %v", table, err)
	}

	// Begin a transaction and acquire an AccessExclusive lock, then leave
	// it open. Both statements run on the pinned conn so the lock and the
	// pid query observe the SAME backend.
	if _, err := conn.ExecContext(ctx, `BEGIN`); err != nil {
		_ = conn.Close()
		t.Fatalf("begin: %v", err)
	}
	if _, err := conn.ExecContext(ctx, `LOCK TABLE `+table+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		_ = conn.Close()
		t.Fatalf("lock %s: %v", table, err)
	}

	if err := conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		_ = conn.Close()
		t.Fatalf("backend pid: %v", err)
	}
	return conn, pid
}

// backendAlive reports whether a backend with the given pid still exists.
func backendAlive(t *testing.T, dsn string, pid int) bool {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var alive bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)`, pid).Scan(&alive); err != nil {
		t.Fatalf("liveness probe: %v", err)
	}
	return alive
}

// TestDetectStaleBackends_FindsAndReaps opens a sluice-labelled,
// lock-holding orphan and a non-sluice lock-holder, then asserts:
// detection finds only the sluice one, and reap=true terminates it while
// leaving the non-sluice backend alive.
func TestDetectStaleBackends_FindsAndReaps(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	// The orphan: labelled like a hard-killed snapshot COPY, holding a
	// lock on a target table, idle in its transaction.
	orphan, orphanPID := openLabelledIdleInTx(t, dsn, "sluice/snapshot/itest", "stale_target")
	defer func() { _ = orphan.Close() }()

	// A NON-sluice backend holding the same kind of lock on another
	// table. It must be invisible to both detection and reaping — the
	// safety bound (application_name LIKE 'sluice/%').
	bystander, bystanderPID := openLabelledIdleInTx(t, dsn, "psql_operator", "bystander_target")
	defer func() { _ = bystander.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Detection only (reap=false) ---
	report, err := Engine{}.DetectStaleBackends(ctx, dsn, []string{"public"}, false)
	if err != nil {
		t.Fatalf("DetectStaleBackends (report-only): %v", err)
	}
	if report.ProbeFailed {
		t.Fatalf("probe should not have failed: %s", report.Warning)
	}
	if !containsBackend(report.Backends, orphanPID) {
		t.Errorf("detection did not find the sluice orphan pid=%d; found=%v", orphanPID, pidsOf(report.Backends))
	}
	if containsBackend(report.Backends, bystanderPID) {
		t.Errorf("detection wrongly included the NON-sluice bystander pid=%d (safety-bound breach)", bystanderPID)
	}
	if len(report.Reaped) != 0 {
		t.Errorf("report-only path must not reap anything; got %v", report.Reaped)
	}
	// The orphan should carry its held-lock detail.
	for _, b := range report.Backends {
		if b.PID == orphanPID {
			if b.ApplicationName != "sluice/snapshot/itest" {
				t.Errorf("application_name = %q, want sluice/snapshot/itest", b.ApplicationName)
			}
			if b.LockRelation != "public.stale_target" {
				t.Errorf("lock relation = %q, want public.stale_target", b.LockRelation)
			}
		}
	}

	// --- Reap (reap=true) ---
	reapReport, err := Engine{}.DetectStaleBackends(ctx, dsn, []string{"public"}, true)
	if err != nil {
		t.Fatalf("DetectStaleBackends (reap): %v", err)
	}
	if !containsInt(reapReport.Reaped, orphanPID) {
		t.Errorf("reap did not terminate the orphan pid=%d; reaped=%v", orphanPID, reapReport.Reaped)
	}
	if containsInt(reapReport.Reaped, bystanderPID) {
		t.Fatalf("reap terminated the NON-sluice bystander pid=%d — SAFETY-BOUND BREACH", bystanderPID)
	}

	// Give the server a moment to tear the terminated backend down, then
	// assert the orphan is gone and the bystander survives.
	deadline := time.Now().Add(10 * time.Second)
	for backendAlive(t, dsn, orphanPID) && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if backendAlive(t, dsn, orphanPID) {
		t.Errorf("orphan pid=%d still alive after reap", orphanPID)
	}
	if !backendAlive(t, dsn, bystanderPID) {
		t.Errorf("bystander pid=%d was terminated — it must survive", bystanderPID)
	}
}

// TestDetectStaleBackends_NoneWhenClean asserts an empty report against a
// container with no sluice-labelled orphans.
func TestDetectStaleBackends_NoneWhenClean(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	report, err := Engine{}.DetectStaleBackends(ctx, dsn, []string{"public"}, true)
	if err != nil {
		t.Fatalf("DetectStaleBackends: %v", err)
	}
	if report.ProbeFailed {
		t.Fatalf("probe should not fail on a clean container: %s", report.Warning)
	}
	if len(report.Backends) != 0 {
		t.Errorf("expected no orphans on a clean container; got %v", pidsOf(report.Backends))
	}
}

func containsBackend(bs []ir.StaleBackend, pid int) bool {
	for _, b := range bs {
		if b.PID == pid {
			return true
		}
	}
	return false
}

func pidsOf(bs []ir.StaleBackend) []int {
	out := make([]int, len(bs))
	for i, b := range bs {
		out[i] = b.PID
	}
	return out
}

// containsInt mirrors the pipeline helper for the integration assertions
// (kept local so the engine test doesn't import internal/pipeline).
func containsInt(s []int, v int) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
