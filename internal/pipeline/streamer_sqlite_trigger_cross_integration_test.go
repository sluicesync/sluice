//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine continuous-sync integration tests for the sqlite-trigger CDC
// SOURCE engine (ADR-0135): a LOCAL SQLite file streams its logical row changes
// into a Postgres target (with a hard-stop + warm-resume exactly-once leg) and a
// MySQL target. These prove the faithful-capture crux end-to-end: a row with an
// integer > 2^53 AND a BLOB column round-trip EXACT through capture → poll →
// reconstruct → apply — NOT silently rounded/dropped the way a json_object
// capture would.
//
// The SQLite source needs no container — it is a temp file the test writes (with
// WAL enabled, the ADR-0135 §5 recommendation so the poller and the app's writes
// don't block each other). Only the TARGET is a container.

package pipeline

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver for seeding + mutating the temp source file

	"sluicesync.dev/sluice/internal/engines"
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
)

// sqliteTrigBigInt is 2^53 + 1 — the value a JSON-number capture silently rounds
// to …992. The faithful (typeof, text) capture must carry it EXACT.
const sqliteTrigBigInt = int64(9007199254740993)

// seedSQLiteTriggerSource writes a temp WAL-mode SQLite file with an events
// table (a big-int column + a BLOB column), seeds two rows, installs the
// trigger-CDC artifacts, and returns the file path.
func seedSQLiteTriggerSource(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "app.db")
	// busy_timeout so a transient lock (the CDC poller's read / a WAL auto-
	// checkpoint) makes this connection WAIT rather than fail with SQLITE_BUSY
	// "database is locked" — without it the test flakes under -race (a real app
	// configures the same; sluice's own source connections set it via connect.go).
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open sqlite seed: %v", err)
	}
	defer func() { _ = db.Close() }()
	stmts := []string{
		`PRAGMA journal_mode=WAL`, // ADR-0135 §5 — poller doesn't block writers
		`CREATE TABLE events (
			id   INTEGER PRIMARY KEY,
			big  INTEGER NOT NULL,
			blb  BLOB,
			note TEXT
		)`,
		`INSERT INTO events (id, big, blb, note) VALUES (1, 100, x'cafe', 'seed-1')`,
		`INSERT INTO events (id, big, blb, note) VALUES (2, 200, NULL, 'seed-2')`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("seed exec %q: %v", s, err)
		}
	}
	if _, err := sqlitetrigger.Setup(context.Background(), path, sqlitetrigger.SetupOptions{
		Tables: []string{"events"},
	}); err != nil {
		t.Fatalf("sqlitetrigger.Setup: %v", err)
	}
	return path
}

// sqliteExec opens a short-lived writable connection to the source file and runs
// one statement (its own committed transaction → fires the triggers, gets its
// own change-log id).
func sqliteExec(t *testing.T, path, stmt string, args ...any) {
	t.Helper()
	// busy_timeout so a transient lock (the CDC poller's read / a WAL auto-
	// checkpoint) makes this connection WAIT rather than fail with SQLITE_BUSY
	// "database is locked" — without it the test flakes under -race (a real app
	// configures the same; sluice's own source connections set it via connect.go).
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open source for exec: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(context.Background(), stmt, args...); err != nil {
		t.Fatalf("source exec %q: %v", stmt, err)
	}
}

// TestStreamer_SQLiteTriggerToPostgres is the headline proof: cold-start +
// CDC + big-int/BLOB fidelity + a hard-stop/warm-resume exactly-once leg.
func TestStreamer_SQLiteTriggerToPostgres(t *testing.T) {
	src := seedSQLiteTriggerSource(t)
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

	const streamID = "sqlite-trigger-pg"
	newStreamer := func() *Streamer {
		return &Streamer{
			Source:    srcEng,
			Target:    pgEng,
			SourceDSN: src,
			TargetDSN: pgTarget,
			StreamID:  streamID,
		}
	}

	// ---- Run 1: cold-start + a batch of CDC ----
	ctx1, cancel1 := context.WithCancel(context.Background())
	run1 := make(chan error, 1)
	go func() { run1 <- newStreamer().Run(ctx1) }()

	// Cold-start lands the 2 seed rows.
	if !waitForRowCount(t, pgTarget, "events", 2, 90*time.Second) {
		cancel1()
		t.Fatal("cold-start never delivered the 2 seed rows to PG")
	}
	// Seed-1's blob landed exactly via the cold-start reader.
	if big, blb := pgReadEvent(t, pgTarget, 1); big != 100 || string(blb) != "\xca\xfe" {
		t.Errorf("cold-start row 1: big=%d blb=%x; want 100 / cafe", big, blb)
	}

	// CDC: INSERT a big-int + blob row, UPDATE seed-1, DELETE seed-2.
	sqliteExec(t, src, `INSERT INTO events (id, big, blb, note) VALUES (3, ?, x'deadbeef', 'cdc-3')`, sqliteTrigBigInt)
	sqliteExec(t, src, `UPDATE events SET big = 999, note = 'cdc-upd' WHERE id = 1`)
	sqliteExec(t, src, `DELETE FROM events WHERE id = 2`)

	// Converge: id 3 present, id 2 gone, id 1 updated.
	if !waitForRowCount(t, pgTarget, "events", 2, 60*time.Second) {
		cancel1()
		t.Fatalf("CDC batch never converged (count): %v", pgEventIDs(t, pgTarget))
	}
	if !waitForEventGone(t, pgTarget, 2, 30*time.Second) {
		cancel1()
		t.Fatal("CDC DELETE of id=2 never propagated")
	}
	// The big-int row arrived EXACT through the capture+CDC path (the crux).
	if big, blb := pgReadEvent(t, pgTarget, 3); big != sqliteTrigBigInt || string(blb) != "\xde\xad\xbe\xef" {
		t.Errorf("CDC row 3: big=%d blb=%x; want %d / deadbeef (faithful capture)", big, blb, sqliteTrigBigInt)
	}
	if !waitForEventBig(t, pgTarget, 1, 999, 30*time.Second) {
		t.Errorf("CDC UPDATE of id=1 never propagated (big still != 999)")
	}

	// ---- Hard-stop mid-stream ----
	cancel1()
	select {
	case <-run1:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run (1) did not return after ctx cancel")
	}

	// ---- Run 2: warm-resume from the durable watermark ----
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	run2 := make(chan error, 1)
	go func() { run2 <- newStreamer().Run(ctx2) }()

	// Apply more CDC after the resume; assert it lands AND the pre-stop state is
	// intact (exactly-once: no resurrection of deleted id=2, no duplicate id=3).
	sqliteExec(t, src, `INSERT INTO events (id, big, blb, note) VALUES (4, ?, NULL, 'cdc-4')`, sqliteTrigBigInt)
	sqliteExec(t, src, `UPDATE events SET big = 1001 WHERE id = 3`)

	if !waitForEventBig(t, pgTarget, 4, sqliteTrigBigInt, 60*time.Second) {
		cancel2()
		t.Fatalf("warm-resume: post-resume INSERT id=4 never landed: %v", pgEventIDs(t, pgTarget))
	}
	if !waitForEventBig(t, pgTarget, 3, 1001, 30*time.Second) {
		t.Error("warm-resume: post-resume UPDATE of id=3 never propagated")
	}
	// Final exactly-once state: exactly {1,3,4}, id=2 still gone.
	if got := pgEventIDs(t, pgTarget); len(got) != 3 || got[0] != 1 || got[1] != 3 || got[2] != 4 {
		t.Errorf("warm-resume final id set = %v; want [1 3 4] (exactly-once)", got)
	}

	cancel2()
	select {
	case <-run2:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run (2) did not return after ctx cancel")
	}
}

// TestStreamer_SQLiteTriggerToMySQL is the MySQL half: cold-start + CDC with the
// same big-int/BLOB fidelity assertions.
func TestStreamer_SQLiteTriggerToMySQL(t *testing.T) {
	src := seedSQLiteTriggerSource(t)
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
		StreamID:  "sqlite-trigger-mysql",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()

	if !waitForRowCountMySQL(t, myTarget, "events", 2, 90*time.Second) {
		cancel()
		t.Fatal("cold-start never delivered the 2 seed rows to MySQL")
	}

	sqliteExec(t, src, `INSERT INTO events (id, big, blb, note) VALUES (3, ?, x'deadbeef', 'cdc-3')`, sqliteTrigBigInt)
	sqliteExec(t, src, `DELETE FROM events WHERE id = 2`)

	if !waitForEventBigMySQL(t, myTarget, 3, sqliteTrigBigInt, 60*time.Second) {
		cancel()
		t.Fatalf("CDC big-int row 3 never landed exactly on MySQL")
	}
	big, blb := mysqlReadEvent(t, myTarget, 3)
	if big != sqliteTrigBigInt || string(blb) != "\xde\xad\xbe\xef" {
		t.Errorf("MySQL CDC row 3: big=%d blb=%x; want %d / deadbeef (faithful capture)", big, blb, sqliteTrigBigInt)
	}
	if !waitForRowCountMySQL(t, myTarget, "events", 2, 30*time.Second) {
		t.Errorf("MySQL final count != 2 after delete+insert: %v", countRowsMySQL(t, myTarget, "events"))
	}

	cancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// --- PG target query helpers ---

// pgReadEvent returns (big, blb) for events.id on the PG target; (-1, nil) when
// absent.
func pgReadEvent(t *testing.T, dsn string, id int64) (int64, []byte) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return -1, nil
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var (
		big int64
		blb []byte
	)
	if err := db.QueryRowContext(ctx, `SELECT big, blb FROM events WHERE id = $1`, id).Scan(&big, &blb); err != nil {
		return -1, nil
	}
	return big, blb
}

// pgEventIDs returns the sorted id set on the PG target.
func pgEventIDs(t *testing.T, dsn string) []int64 {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT id FROM events ORDER BY id`)
	if err != nil {
		return nil
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return ids
		}
		ids = append(ids, id)
	}
	return ids
}

// waitForEventBig polls until events.id on PG has big == want.
func waitForEventBig(t *testing.T, dsn string, id, want int64, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if big, _ := pgReadEvent(t, dsn, id); big == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// waitForEventGone polls until events.id is absent on PG.
func waitForEventGone(t *testing.T, dsn string, id int64, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if big, _ := pgReadEvent(t, dsn, id); big == -1 {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// --- MySQL target query helpers ---

func mysqlReadEvent(t *testing.T, dsn string, id int64) (int64, []byte) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return -1, nil
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var (
		big int64
		blb []byte
	)
	if err := db.QueryRowContext(ctx, "SELECT big, blb FROM events WHERE id = ?", id).Scan(&big, &blb); err != nil {
		return -1, nil
	}
	return big, blb
}

func waitForEventBigMySQL(t *testing.T, dsn string, id, want int64, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if big, _ := mysqlReadEvent(t, dsn, id); big == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}
