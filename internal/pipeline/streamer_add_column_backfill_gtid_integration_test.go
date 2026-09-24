//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
)

// TestStreamer_AddColumnBackfill_GTIDRecordHeldUntilTheBackfillCommits is
// the real-server pin for the 2026-09-24 value-fidelity finding 1: the
// backfill ledger's durability evidence was self-referential on a MySQL
// GTID source.
//
// # The defect
//
// A binlog row event's position is the executed GTID set WITHOUT its own
// transaction (item 132), so the first change after a backfill carries the
// set every commit before it already made durable. When a commit on another
// table lands between the ALTER and the boundary row, the applier persists
// exactly that set before the backfill's rows commit, and the old rule,
// "persisted at or after the first change after the backfill", was true
// while the backfill was still in flight: the write-ahead record was
// cleared, and a hard kill then restarted past the boundary with the
// uncommitted rows unfilled, at exit 0. MEASURED on both appliers (the
// *_interleaved cells, red under an at-or-after mutant). The review that
// found this read it as reachable without the interleaved commit too, via
// the serial applier persisting the boundary snapshot's own set; measured
// here it does not (the persisted set lagged the snapshot's by one GTID
// while the lock was held), so the plain serial cell is a regression cell,
// not a discriminating one.
//
// # How the window is held open
//
// The backfill writes rows in primary-key order, so the LAST backfilled
// Update is the one for the highest pre-existing id. The test locks that
// row on the target (SELECT … FOR UPDATE in an open transaction), so the
// applier receives the last Update — the backfill has handed every row
// over, and the ledger marks it emitted — and then blocks on the lock. The
// boundary row is inserted with id 0, so its own backfilled Update (it is
// on the source when the backfill reads) comes first, not last. While the
// lock is held nothing after the backfill can commit, so a durability proof
// that arrives in that window is false by construction.
//
// # The independent expected value
//
// The target's own row state: the locked row is NULL (the backfilled
// Update for it never committed), so the write-ahead record MUST still be
// on the target while the lock is held, and a hard kill in that window
// MUST restart refusing.
func TestStreamer_AddColumnBackfill_GTIDRecordHeldUntilTheBackfillCommits(t *testing.T) {
	for _, mode := range []struct {
		name        string
		concurrency int
		// interleave commits a change to another table between the ALTER
		// and the boundary row — a busy source's shape, and the one both
		// appliers need for the old rule to fire (see the doc above).
		interleave bool
	}{
		{name: "serial", concurrency: 1},
		{name: "serial_interleaved", concurrency: 1, interleave: true},
		{name: "lanes_interleaved", concurrency: 4, interleave: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			abGTIDHeldRecord(t, mode.concurrency, mode.interleave)
		})
	}
}

func abGTIDHeldRecord(t *testing.T, concurrency int, interleave bool) {
	setPollIntervalForTest(t, 200*time.Millisecond)
	src, tgt, cleanup := startMySQLGTID(t)
	defer cleanup()
	if got := globalVarMySQL(t, src, "gtid_mode"); got != "ON" {
		t.Fatalf("premise: gtid_mode = %q, want ON", got)
	}
	const (
		rows     = 300
		streamID = "test-ac-backfill-gtid"
	)
	lastPre := rows + 1 // the first CDC row is the highest pre-existing id
	fdExec(t, fdMySQL, src, `CREATE TABLE w (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(80) NOT NULL) ENGINE=InnoDB`)
	fdExec(t, fdMySQL, src, `CREATE TABLE o (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB`)
	values := make([]string, rows)
	for i := range values {
		values[i] = fmt.Sprintf("(%d, 'r')", i+1)
	}
	fdExec(t, fdMySQL, src, `INSERT INTO w (id, name) VALUES `+strings.Join(values, ", "))
	eng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	newStreamer := func(hardKill bool, ack string) *Streamer {
		return &Streamer{
			Source: eng, Target: eng, SourceDSN: src, TargetDSN: tgt, StreamID: streamID,
			ApplyConcurrency:                  concurrency,
			AcceptUnforwardedSchemaChange:     ack,
			simulateHardKillForTest:           hardKill,
			backfillDurabilityIntervalForTest: 5 * time.Millisecond,
		}
	}
	start := func(s *Streamer) (context.CancelFunc, chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- s.Run(ctx) }()
		return cancel, done
	}
	s := newStreamer(true, "")
	cancel, done := start(s)
	defer cancel()
	abWaitMySQLRows(t, tgt, "w", rows, 3*time.Minute, done)
	fdExec(t, fdMySQL, src, fmt.Sprintf(`INSERT INTO w VALUES (%d, 'warm')`, lastPre))
	abWaitMySQLRows(t, tgt, "w", rows+1, time.Minute, done)

	// Django's AddField: the source fills 'v', then drops the DEFAULT, so
	// the target's own fill is NULL and only the backfill can land 'v'.
	fdExec(t, fdMySQL, src, `ALTER TABLE w ADD COLUMN dj VARCHAR(8) DEFAULT 'v'`)
	fdExec(t, fdMySQL, src, `ALTER TABLE w ALTER COLUMN dj DROP DEFAULT`)
	if interleave {
		fdExec(t, fdMySQL, src, `INSERT INTO o VALUES (1)`)
		abWaitMySQLRows(t, tgt, "o", 1, time.Minute, done)
		time.Sleep(3 * time.Second) // the frontier persists o's commit
	}
	fdExec(t, fdMySQL, src, `INSERT INTO w VALUES (0, 'boundary', 'x')`)

	db, err := sql.Open("mysql", tgt)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	// Lock the last pre-existing row as soon as the target has the column
	// (holding a row lock earlier would block the ALTER itself on the
	// table's metadata lock).
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'w' AND column_name = 'dj'`).Scan(&n); err == nil && n == 1 {
			break
		}
		abFailIfExited(t, done)
		if time.Now().After(deadline) {
			t.Fatal("the forwarded ADD COLUMN never landed on the target")
		}
		time.Sleep(10 * time.Millisecond)
	}
	lock, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	var locked int
	if err := lock.QueryRow(`SELECT id FROM w WHERE id = ? FOR UPDATE`, lastPre).Scan(&locked); err != nil {
		t.Fatalf("lock row %d: %v", lastPre, err)
	}
	var lockReleased bool
	release := func() {
		if !lockReleased {
			lockReleased = true
			_ = lock.Rollback()
		}
	}
	defer release()

	// Wait for the backfill to hand its last row to the applier, then let
	// the 5 ms durability watch run for a second: nothing after the
	// backfill can commit while the lock is held.
	diag := func(tag string) {
		var filled, lo, hi int
		_ = db.QueryRow(`SELECT COUNT(*) FROM w WHERE dj IS NOT NULL`).Scan(&filled)
		_ = db.QueryRow(`SELECT COALESCE(MIN(id),-1), COALESCE(MAX(id),-1) FROM w WHERE dj IS NULL`).Scan(&lo, &hi)
		t.Logf("%s: %d target rows filled; NULL id range [%d, %d]", tag, filled, lo, hi)
		pl, err := db.Query(`SELECT COALESCE(state,''), COALESCE(LEFT(info, 200),'') FROM information_schema.processlist WHERE info IS NOT NULL AND info NOT LIKE '%processlist%'`)
		if err != nil {
			return
		}
		defer func() { _ = pl.Close() }()
		for pl.Next() {
			var st, info string
			_ = pl.Scan(&st, &info)
			t.Logf("%s: processlist [%s] %s", tag, st, info)
		}
	}
	deadline = time.Now().Add(time.Minute)
	var seen bool
	for !abBackfillHandedOver(s, &seen) {
		abFailIfExited(t, done)
		if time.Now().After(deadline) {
			diag("timeout")
			t.Fatal("the backfill never handed its last row to the applier")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(time.Second)
	msg, has := abRecordedRefusal(t, fdMySQL, tgt, streamID)
	if !has || !strings.Contains(msg, addColumnBackfillIncompleteMarker) {
		t.Fatalf("the write-ahead record was cleared (target holds %q, present %v) while the backfill's last row "+
			"(id %d) was still uncommitted behind a lock — the durability proof fired before the backfill committed", msg, has, lastPre)
	}

	cancel() // the "kill" lands while the last backfilled row is uncommitted
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatal("Run did not return after the stop")
	}
	release()
	var dj sql.NullString
	if err := db.QueryRow(`SELECT dj FROM w WHERE id = ?`, lastPre).Scan(&dj); err != nil {
		t.Fatalf("read the locked row: %v", err)
	}
	if dj.Valid {
		t.Fatalf("the locked row holds %q after the kill; the pin did not hold its backfilled row uncommitted", dj.String)
	}

	// The restart refuses on the record instead of resuming past the row.
	_, done2 := start(newStreamer(false, ""))
	var runErr error
	select {
	case runErr = <-done2:
	case <-time.After(2 * time.Minute):
		t.Fatal("the restart did not refuse; it is running past an unfinished backfill")
	}
	var replay *recordedUnforwardedRefusalError
	if !errors.As(runErr, &replay) || !strings.Contains(runErr.Error(), addColumnBackfillIncompleteMarker) {
		t.Fatalf("restart returned %v; want the recorded %s refusal", runErr, addColumnBackfillIncompleteMarker)
	}
}

// abBackfillHandedOver reports whether the backfill s's ledger owed has
// handed every row to the applier: its entry is marked emitted, or — once
// *seen has recorded that it was entered — it has already left the ledger
// as settled (which, with the last row held behind a lock, is exactly the
// premature proof this pin exists to catch; the record assertion then
// reports it).
func abBackfillHandedOver(s *Streamer, seen *bool) bool {
	l := &s.addedColumnBackfills
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 {
		return *seen
	}
	*seen = true
	for _, e := range l.entries {
		if !e.emitted {
			return false
		}
	}
	return true
}

// abWaitMySQLRows waits for table to hold n rows on a MySQL dsn, failing
// early if the stream ended.
func abWaitMySQLRows(t *testing.T, dsn, table string, n int, within time.Duration, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(within)
	for pollRowCountMySQL(dsn, table) < n {
		abFailIfExited(t, done)
		if time.Now().After(deadline) {
			t.Fatalf("%s never reached %d rows (has %d)", table, n, pollRowCountMySQL(dsn, table))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// abFailIfExited fails the test when the stream under test has ended.
func abFailIfExited(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("the stream ended early: %v", err)
	default:
	}
}
