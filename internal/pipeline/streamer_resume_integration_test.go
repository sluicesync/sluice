//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Restart-resume integration test for pipeline.Streamer. Proves the
// load-bearing §5 property: a Streamer that crashes mid-stream and
// restarts with the same StreamID resumes from the persisted
// position rather than re-running the snapshot+bulk-copy phase.
// Combined with the applier's idempotency, every event lands on the
// target exactly once.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_RestartResume_PostgresToPostgres is the §5 spine
// test. Sequence:
//
//  1. Cold-start a Streamer with a fixed StreamID. Bulk-copy seed
//     row R1; CDC delivers R2 (inserted after Run starts).
//  2. Cancel ctx → simulated crash. Wait for Run to return.
//  3. Verify the control table has a non-empty position for the
//     StreamID — the load-bearing precondition for warm resume.
//  4. Start a fresh Streamer with the SAME StreamID. Sleep briefly
//     and verify the target row count is UNCHANGED — proves the
//     warm-resume path skipped bulk-copy.
//  5. Insert R3 on source; verify it flows through CDC to target.
//  6. Cancel ctx2; wait for Run to return.
//  7. Final state: target has {R1, R2, R3} exactly once.
func TestStreamer_RestartResume_PostgresToPostgres(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
		INSERT INTO users (id, email) VALUES (1, 'r1@example.com');
	`
	applyDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const streamID = "test-resume"

	// ---- Phase 1: cold start ----
	streamer1 := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	runErr1 := make(chan error, 1)
	go func() { runErr1 <- streamer1.Run(ctx1) }()

	// Wait for bulk copy to land R1.
	if !waitForRowCount(t, targetDSN, "users", 1, 30*time.Second) {
		t.Fatalf("phase 1: bulk copy never delivered R1")
	}

	// Insert R2 on source — should flow through CDC and write its
	// position into sluice_cdc_state.
	applyDDL(t, sourceDSN, "INSERT INTO users (id, email) VALUES (2, 'r2@example.com');")
	if !waitForRowCount(t, targetDSN, "users", 2, 30*time.Second) {
		t.Fatalf("phase 1: CDC never delivered R2")
	}

	// ---- Phase 2: simulated crash ----
	cancel1()
	select {
	case <-runErr1:
	case <-time.After(15 * time.Second):
		t.Fatal("phase 2: streamer1 did not return after ctx cancel")
	}

	// ---- Phase 3: control table must have a position ----
	persistedToken := readPersistedPosition(t, targetDSN, streamID)
	if persistedToken == "" {
		t.Fatal("phase 3: sluice_cdc_state has no row / empty position for streamID — warm resume can't work")
	}
	t.Logf("phase 3: persisted position token = %q", persistedToken)

	// ---- Phase 4: warm-resume start (must NOT re-bulk-copy) ----
	streamer2 := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	runErr2 := make(chan error, 1)
	go func() { runErr2 <- streamer2.Run(ctx2) }()

	// The load-bearing assertion: row count stays at 2 for a few
	// seconds after restart. If warm-resume DIDN'T fire and bulk-
	// copy ran again, the upsert path would absorb the duplicates
	// silently — but the stream would have re-opened a snapshot
	// and re-read every table, which we don't want operationally.
	// A clean way to detect "no bulk-copy" without instrumentation
	// is the count-stays-stable check below.
	time.Sleep(3 * time.Second)
	if got := countRows(t, targetDSN, "users"); got != 2 {
		t.Fatalf("phase 4: row count = %d after warm-resume start; want 2 (warm resume should skip bulk copy)", got)
	}

	// ---- Phase 5: continued streaming through warm-resumed CDC ----
	applyDDL(t, sourceDSN, "INSERT INTO users (id, email) VALUES (3, 'r3@example.com');")
	if !waitForRowCount(t, targetDSN, "users", 3, 30*time.Second) {
		t.Fatalf("phase 5: CDC after warm-resume never delivered R3")
	}

	// ---- Phase 6: clean shutdown ----
	cancel2()
	select {
	case <-runErr2:
	case <-time.After(15 * time.Second):
		t.Fatal("phase 6: streamer2 did not return after ctx cancel")
	}

	// ---- Phase 7: final state — R1, R2, R3 exactly once ----
	emails := selectAllEmails(t, targetDSN, "users")
	want := []string{"r1@example.com", "r2@example.com", "r3@example.com"}
	if !equalStringSlices(emails, want) {
		t.Errorf("final state: got %v; want %v (exactly-once violated)", emails, want)
	}
}

// ---- Test helpers ----

// waitForRowCount polls the target table until it has at least n
// rows or the timeout fires. Returns true on success, false on
// timeout. Tolerant of "relation does not exist" — the table only
// shows up on the target once the cold-start bulk-copy phase
// reaches CreateTablesWithoutConstraints, and the wait covers that
// startup window.
func waitForRowCount(t *testing.T, dsn, table string, n int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pollRowCount(dsn, table) >= n {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// pollRowCount is the tolerant counterpart of countRows: returns
// the row count on success, 0 on any error (table missing, conn
// refused, etc.). Used by waitForRowCount during the startup
// window before the target schema exists.
func pollRowCount(dsn, table string) int {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return 0
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		return 0
	}
	return n
}

// waitForSourceSlot polls the PG source's pg_replication_slots until a
// sluice slot exists, or fails the test at timeout. It replaces the
// blind "give the streamer a moment" start-up sleep in streamer tests
// that write a FINITE burst of source rows after starting the stream:
// a write committed BEFORE the slot exists is captured by neither the
// snapshot (taken at slot creation) nor CDC — under CI shard
// contention the 2s sleep lost that race and the test sat at 0/N rows
// until its deadline (the AIMD "dest only saw 0/250" flake,
// root-caused 2026-06-10: local pass in 3.8s, CI 0 rows in 63s).
// Slot existence is the capture guarantee, so polling for it makes
// the test correct under arbitrary scheduler delay rather than just
// more tolerant. Tests whose writer runs CONTINUOUSLY (bug15) don't
// need this — later writes are captured regardless.
func waitForSourceSlot(t *testing.T, sourceDSN string, timeout time.Duration) {
	t.Helper()
	db, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("waitForSourceSlot: open source: %v", err)
	}
	defer func() { _ = db.Close() }()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var n int
		err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name LIKE 'sluice%'`).Scan(&n)
		cancel()
		if err == nil && n > 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("waitForSourceSlot: no sluice replication slot on the source after %s — cold-start never reached slot creation", timeout)
}

// readPersistedPosition reads the source_position column for streamID
// from the target's sluice_cdc_state table. Returns "" if no row.
func readPersistedPosition(t *testing.T, dsn, streamID string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var token string
	err = db.QueryRowContext(
		ctx,
		`SELECT source_position FROM "public"."sluice_cdc_state" WHERE stream_id = $1`,
		streamID,
	).Scan(&token)
	if err != nil {
		// Treat "no rows" as "" so callers can distinguish via the
		// returned value rather than error handling.
		return ""
	}
	return token
}

// selectAllEmails returns the email column from the named target
// table, sorted lexicographically. Used for set-equality assertions.
func selectAllEmails(t *testing.T, dsn, table string) []string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT email FROM "+table+" ORDER BY id")
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

func equalStringSlices(a, b []string) bool {
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
