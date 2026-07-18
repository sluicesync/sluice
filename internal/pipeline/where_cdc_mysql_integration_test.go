//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0173 Phase 2 — continuous *filtered* sync, MySQL (binlog) source axis.
//
// Same row-move convergence pin as the Postgres test, over a real MySQL
// binlog source (binlog_row_image=FULL, which the client-side row-move
// evaluation requires). Uses a NUMERIC predicate (`region = 1`) so the pin
// is collation-independent — the string-collation fidelity gate is pinned
// exhaustively at the unit level (TestColumnInfosFromIR / TestCompileRefusals).

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_WhereFilter_MySQLRowMove pins the filtered cold-start +
// row-move convergence for a MySQL binlog source, including the move-IN →
// INSERT and move-OUT → DELETE cells (non-vacuous).
func TestStreamer_WhereFilter_MySQLRowMove(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyDDLMySQL(t, sourceDSN, `
		CREATE TABLE users (
			id     BIGINT       NOT NULL PRIMARY KEY,
			region INT          NOT NULL,
			name   VARCHAR(64)  NOT NULL
		);
	`)
	applyDDLMySQL(t, sourceDSN, `
		INSERT INTO users (id, region, name) VALUES
			(1, 1, 'one'), (2, 2, 'two'), (3, 1, 'three'), (4, 3, 'four');
	`)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	streamer := &Streamer{
		Source:     mysqlEng,
		Target:     mysqlEng,
		SourceDSN:  sourceDSN,
		TargetDSN:  targetDSN,
		StreamID:   "where-mysql-rowmove",
		RowFilters: map[string]string{"users": "region = 1"},
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Filtered cold-start: only the two region=1 rows (1, 3).
	if !waitForExactRowCountMySQL(targetDSN, "users", 2, 90*time.Second) {
		t.Fatalf("filtered cold-start delivered %d rows; want 2 (only region=1)", pollRowCountMySQL(targetDSN, "users"))
	}
	if mysqlRowExistsByID(t, streamCtx, targetDSN, "users", 2) || mysqlRowExistsByID(t, streamCtx, targetDSN, "users", 4) {
		t.Fatal("filtered cold-start leaked an out-of-scope row (2 or 4)")
	}

	applyDDLMySQL(t, sourceDSN, "INSERT INTO users (id, region, name) VALUES (5, 1, 'five');") // INSERT in
	applyDDLMySQL(t, sourceDSN, "INSERT INTO users (id, region, name) VALUES (6, 2, 'six');")  // INSERT out (drop)
	applyDDLMySQL(t, sourceDSN, "DELETE FROM users WHERE id = 1;")                             // DELETE in
	applyDDLMySQL(t, sourceDSN, "UPDATE users SET name = 'FIVE' WHERE id = 5;")                // UPDATE (yes,yes)
	applyDDLMySQL(t, sourceDSN, "UPDATE users SET region = 2 WHERE id = 3;")                   // move-OUT → DELETE
	applyDDLMySQL(t, sourceDSN, "UPDATE users SET region = 1 WHERE id = 4;")                   // move-IN  → INSERT

	// Converged in-scope set = source region=1 rows = {4, 5}. The count
	// stays 2 throughout, so wait on ROW IDENTITY: the move-IN (id=4, the
	// last op) landing means CDC caught up through every prior cell.
	if !waitForMySQLRow(t, targetDSN, streamCtx, 4, true, 45*time.Second) {
		t.Fatalf("move-IN (id=4) never landed; CDC did not converge (rows=%d)", pollRowCountMySQL(targetDSN, "users"))
	}
	if !waitForMySQLRow(t, targetDSN, streamCtx, 1, false, 15*time.Second) {
		t.Fatalf("DELETE in-scope (id=1) never propagated")
	}
	if !waitForExactRowCountMySQL(targetDSN, "users", 2, 15*time.Second) {
		t.Fatalf("target did not converge to the 2 in-scope rows; rows = %d", pollRowCountMySQL(targetDSN, "users"))
	}
	if !mysqlRowExistsByID(t, streamCtx, targetDSN, "users", 5) {
		t.Error("INSERT in-scope (id=5) did not appear on target")
	}
	if !mysqlRowExistsByID(t, streamCtx, targetDSN, "users", 4) {
		t.Error("move-IN (id=4 region 3→1) did not INSERT the after-image on target")
	}
	if mysqlRowExistsByID(t, streamCtx, targetDSN, "users", 3) {
		t.Error("move-OUT (id=3 region 1→2) did not DELETE the now-out-of-scope row on target")
	}
	if mysqlRowExistsByID(t, streamCtx, targetDSN, "users", 1) {
		t.Error("DELETE in-scope (id=1) did not propagate")
	}
	if mysqlRowExistsByID(t, streamCtx, targetDSN, "users", 6) {
		t.Error("INSERT out-of-scope (id=6) leaked onto target")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// waitForMySQLRow polls until the users row with id has the wanted presence
// (true = exists, false = gone) or the timeout elapses.
func waitForMySQLRow(t *testing.T, dsn string, ctx context.Context, id int, wantExists bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if mysqlRowExistsByID(t, ctx, dsn, "users", id) == wantExists {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}
