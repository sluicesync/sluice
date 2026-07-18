//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0173 Phase 2 — continuous *filtered* sync, Postgres source axis.
//
// Pins the row-move table end to end against a real PG logical-replication
// source: a filtered cold-start seeds only the in-scope subset, and live
// CDC keeps the target holding EXACTLY the in-scope set through every
// row-move cell — including the load-bearing move-IN → INSERT and
// move-OUT → DELETE translations (a naive per-event filter leaks/drops
// these). Also pins the two sync-start refusals (missing REPLICA IDENTITY
// FULL, and an unsupported predicate).

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_WhereFilter_PostgresRowMove drives a filtered sync
// (`--where users=country = 'US'`) and asserts the target converges to
// EXACTLY the in-scope set through: a filtered cold-start, INSERT in/out,
// DELETE in, UPDATE (yes,yes), UPDATE move-IN, UPDATE move-OUT.
func TestStreamer_WhereFilter_PostgresRowMove(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id      BIGINT       NOT NULL PRIMARY KEY,
			country VARCHAR(8)   NOT NULL,
			name    VARCHAR(64)  NOT NULL
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
		INSERT INTO users (id, country, name) VALUES
			(1, 'US', 'one'),
			(2, 'CA', 'two'),
			(3, 'US', 'three'),
			(4, 'MX', 'four');
	`
	applyDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:     pgEng,
		Target:     pgEng,
		SourceDSN:  sourceDSN,
		TargetDSN:  targetDSN,
		StreamID:   "where-pg-rowmove",
		RowFilters: map[string]string{"users": "country = 'US'"},
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Snapshot↔CDC consistency: the filtered cold-start seeds ONLY the two
	// in-scope rows (1, 3) — never 2 (CA) or 4 (MX).
	if !waitForExactRowCount(targetDSN, "users", 2, 90*time.Second) {
		t.Fatalf("filtered cold-start delivered %d rows; want 2 (only US)", pollRowCount(targetDSN, "users"))
	}
	if pgRowExistsByID(t, streamCtx, targetDSN, "users", 2) || pgRowExistsByID(t, streamCtx, targetDSN, "users", 4) {
		t.Fatal("filtered cold-start leaked an out-of-scope row (2=CA or 4=MX)")
	}

	// Drive the row-move cells.
	applyDDL(t, sourceDSN, "INSERT INTO users (id, country, name) VALUES (5, 'US', 'five');") // INSERT in-scope
	applyDDL(t, sourceDSN, "INSERT INTO users (id, country, name) VALUES (6, 'CA', 'six');")  // INSERT out-of-scope (drop)
	applyDDL(t, sourceDSN, "DELETE FROM users WHERE id = 1;")                                 // DELETE in-scope
	applyDDL(t, sourceDSN, "UPDATE users SET name = 'FIVE' WHERE id = 5;")                    // UPDATE (yes,yes)
	applyDDL(t, sourceDSN, "UPDATE users SET country = 'MX' WHERE id = 3;")                   // move-OUT → DELETE
	applyDDL(t, sourceDSN, "UPDATE users SET country = 'US' WHERE id = 4;")                   // move-IN  → INSERT

	// Converged in-scope set = source US rows = {4, 5}. The count stays 2
	// throughout, so wait on ROW IDENTITY: the move-IN (id=4, the last op)
	// landing means CDC caught up through every prior cell.
	if !waitForPGRow(t, targetDSN, 4, true, 45*time.Second) {
		t.Fatalf("move-IN (id=4) never landed; CDC did not converge (rows=%d)", pollRowCount(targetDSN, "users"))
	}
	if !waitForPGRow(t, targetDSN, 1, false, 15*time.Second) {
		t.Fatalf("DELETE in-scope (id=1) never propagated")
	}
	if !waitForExactRowCount(targetDSN, "users", 2, 15*time.Second) {
		t.Fatalf("target did not converge to the 2 in-scope rows; rows = %d", pollRowCount(targetDSN, "users"))
	}
	// Non-vacuous cell assertions.
	if !pgRowExistsByID(t, streamCtx, targetDSN, "users", 5) {
		t.Error("INSERT in-scope (id=5) did not appear on target")
	}
	if !pgRowExistsByID(t, streamCtx, targetDSN, "users", 4) {
		t.Error("move-IN (id=4 MX→US) did not INSERT the after-image on target")
	}
	if pgRowExistsByID(t, streamCtx, targetDSN, "users", 3) {
		t.Error("move-OUT (id=3 US→MX) did not DELETE the now-out-of-scope row on target")
	}
	if pgRowExistsByID(t, streamCtx, targetDSN, "users", 1) {
		t.Error("DELETE in-scope (id=1) did not propagate")
	}
	if pgRowExistsByID(t, streamCtx, targetDSN, "users", 6) {
		t.Error("INSERT out-of-scope (id=6 CA) leaked onto target")
	}
	// UPDATE (yes,yes): the name update landed.
	if got := pgScalarName(t, streamCtx, targetDSN, 5); got != "FIVE" {
		t.Errorf("UPDATE (yes,yes) on id=5: name = %q; want FIVE", got)
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_WhereFilter_PostgresBeforeImagePreflight pins the coded
// refusal when a filtered table lacks REPLICA IDENTITY FULL: the sync
// refuses at start (before any data moves) naming the table + remedy.
func TestStreamer_WhereFilter_PostgresBeforeImagePreflight(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	// REPLICA IDENTITY left at the server DEFAULT (PK-only before-image).
	applyDDL(t, sourceDSN, `
		CREATE TABLE users (
			id      BIGINT      NOT NULL PRIMARY KEY,
			country VARCHAR(8)  NOT NULL
		);
		INSERT INTO users (id, country) VALUES (1, 'US');
	`)

	pgEng, _ := engines.Get("postgres")
	streamer := &Streamer{
		Source:     pgEng,
		Target:     pgEng,
		SourceDSN:  sourceDSN,
		TargetDSN:  targetDSN,
		StreamID:   "where-pg-preflight",
		RowFilters: map[string]string{"users": "country = 'US'"},
	}
	err := streamer.Run(context.Background())
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeWhereCDCBeforeImage {
		t.Fatalf("Run() = %v; want coded %s", err, sluicecode.CodeWhereCDCBeforeImage)
	}
	// No rows should have been copied — the refusal is up-front.
	if n := pollRowCount(targetDSN, "users"); n != 0 {
		t.Errorf("refused sync still copied %d rows; want 0 (refusal is up-front)", n)
	}
}

// TestStreamer_WhereFilter_PostgresUnsupportedPredicate pins the coded
// refusal for a predicate the client-side CDC evaluator can't faithfully
// evaluate — refused at sync-start, not silently mis-evaluated.
func TestStreamer_WhereFilter_PostgresUnsupportedPredicate(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, sourceDSN, `
		CREATE TABLE users (
			id      BIGINT      NOT NULL PRIMARY KEY,
			country VARCHAR(8)  NOT NULL
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
	`)

	pgEng, _ := engines.Get("postgres")
	streamer := &Streamer{
		Source:     pgEng,
		Target:     pgEng,
		SourceDSN:  sourceDSN,
		TargetDSN:  targetDSN,
		StreamID:   "where-pg-unsupported",
		RowFilters: map[string]string{"users": "lower(country) = 'us'"}, // function call
	}
	err := streamer.Run(context.Background())
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeWhereCDCUnsupportedPredicate {
		t.Fatalf("Run() = %v; want coded %s", err, sluicecode.CodeWhereCDCUnsupportedPredicate)
	}
	if n := pollRowCount(targetDSN, "users"); n != 0 {
		t.Errorf("refused sync still copied %d rows; want 0", n)
	}
}

// waitForPGRow polls until the users row with id has the wanted presence
// (true = exists, false = gone) or the timeout elapses.
func waitForPGRow(t *testing.T, dsn string, id int, wantExists bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	ctx := context.Background()
	for time.Now().Before(deadline) {
		if pgRowExistsByID(t, ctx, dsn, "users", id) == wantExists {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// pgScalarName reads the name column of the users row with the given id.
func pgScalarName(t *testing.T, ctx context.Context, dsn string, id int) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var name string
	if err := db.QueryRowContext(c, "SELECT name FROM users WHERE id = $1", id).Scan(&name); err != nil {
		return ""
	}
	return name
}
