// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// GC-41 end-to-end pins: a Postgres source's slot is never acked past the
// position the TARGET has durably persisted, for every target engine and
// apply path.

// TestStreamer_PostgresToMySQL_SlotAckNeverPassesTheDurablePosition is the
// DEFECT 1 repro, graded on the real target. Before GC-41 only the Postgres
// applier fed the reader applied-LSN feedback; for a MySQL target the reader
// acked the STREAMED LSN. With the target blocked (LOCK TABLES) the 200 rows
// behind the lock were parsed, acked to the slot, and never applied; the
// ungraceful stop (context cancel — the Ctrl-C shape) dropped them, and the
// warm resume started past them. Measured pre-fix: 21 target rows against 221
// source rows, at exit 0.
//
// Two independent checks, neither derived from sluice's own bookkeeping: the
// slot's confirmed_flush_lsn (the server's record of what was acked) against
// the target's persisted position (sampled while the stream runs), and the
// target row count after resume against the SOURCE row count.
//
// The CLI defaults are what matter here: ApplyBatchSize 1000 and
// ApplyConcurrency 0 (auto — concurrent lanes on MySQL). PG → SQLite / D1 is
// not a sync path at all (neither engine has a ChangeApplier;
// TestSlotAckCeilingRoster_EveryRegisteredEngine checks that refusal), so
// MySQL is the non-Postgres target class.
func TestStreamer_PostgresToMySQL_SlotAckNeverPassesTheDurablePosition(t *testing.T) {
	pgSrc, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	root, _, myCleanup := startMySQL(t)
	defer myCleanup()
	dsn := dsNewDatabase(t, root, "gc41")

	const (
		table    = "gc41_rows"
		streamID = "gc41-mysql"
		slot     = "sluice_gc41_mysql"
		seedRows = 20
		blocked  = 200
	)
	applyPGDDL(t, pgSrc, fmt.Sprintf(`
		CREATE TABLE %s (id INT PRIMARY KEY, v TEXT NOT NULL);
		INSERT INTO %s SELECT g, 'seed' FROM generate_series(1, %d) g;`, table, table, seedRows))

	pgEng, _ := engines.Get("postgres")
	myEng, _ := engines.Get("mysql")
	newStreamer := func() *Streamer {
		return &Streamer{
			Source: pgEng, Target: myEng, SourceDSN: pgSrc, TargetDSN: dsn,
			StreamID: streamID, SlotName: slot, PublicationName: "pub_gc41_mysql",
			Filter:           migcore.TableFilter{Include: []string{table}},
			ApplyBatchSize:   1000,
			ApplyConcurrency: 0,
		}
	}

	// ---- Run 1: cold start, one CDC row so a CDC position is persisted. ----
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	run1 := make(chan error, 1)
	go func() { run1 <- newStreamer().Run(ctx1) }()
	if !waitForRowCountMySQLQuoted(t, dsn, table, seedRows, 2*time.Minute) {
		t.Fatalf("cold start never copied the %d seed rows", seedRows)
	}
	applyPGDDL(t, pgSrc, fmt.Sprintf("INSERT INTO %s VALUES (%d, 'cdc')", table, seedRows+1))
	if !waitForRowCountMySQLQuoted(t, dsn, table, seedRows+1, time.Minute) {
		t.Fatal("the first CDC row never landed")
	}

	// ---- Block the target, then write behind the block. ----
	lockDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockDB.Close() }()
	lockConn, err := lockDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockConn.ExecContext(context.Background(), "LOCK TABLES `"+table+"` WRITE"); err != nil {
		t.Fatalf("LOCK TABLES: %v", err)
	}
	// One transaction per row, as an application writes. It matters: the
	// reader acks a transaction's COMMIT LSN, and the walsender re-sends a
	// transaction whose commit record starts exactly at the resume point, so
	// a single 200-row transaction acked early is re-delivered whole and the
	// loss stays invisible to the row count. With 200 transactions, every one
	// before the acked position is gone.
	insertRowsOneTxEach(t, pgSrc, table, seedRows+2, seedRows+1+blocked)

	// ---- Watch ~2.5 keepalives: the slot must never pass the target. ----
	ackViolations := watchSlotAckAgainstPersisted(t, pgSrc, slot, 25*time.Second, func() (pglogrepl.LSN, bool) {
		return readPersistedLSNMySQL(dsn, streamID)
	})

	// ---- Ungraceful stop while the target is still blocked. ----
	cancel1()
	select {
	case <-run1:
	case <-time.After(2 * time.Minute):
		t.Fatal("run 1 did not return after its context was cancelled")
	}
	if _, err := lockConn.ExecContext(context.Background(), "UNLOCK TABLES"); err != nil {
		t.Fatalf("UNLOCK TABLES: %v", err)
	}
	_ = lockConn.Close()

	// ---- Run 2: warm resume must deliver every source row. ----
	ctx2, cancel2 := context.WithCancel(context.Background())
	run2 := make(chan error, 1)
	go func() { run2 <- newStreamer().Run(ctx2) }()
	want := sourceRowCountPG(t, pgSrc, table)
	waitForRowCountMySQLQuoted(t, dsn, table, want, 2*time.Minute)
	time.Sleep(3 * time.Second) // let a straggler (a duplicate would be a PK error) surface
	got := pollRowCountMySQLQuoted(dsn, table)
	cancel2()
	<-run2

	for _, v := range ackViolations {
		t.Errorf("confirmed_flush_lsn %s passed the target's persisted position %s at %s — the slot was "+
			"acked for changes the target did not hold (GC-41)", v.confirmed, v.persisted, v.at.Format(time.RFC3339))
	}
	if got != want {
		t.Errorf("after the stop and warm resume the MySQL target holds %d rows; the source holds %d — %d "+
			"change(s) were acked on the slot and never applied (GC-41 silent loss)", got, want, want-got)
	}
}

// ackViolation is one sample where the slot's acked position was past the
// target's durable one.
type ackViolation struct {
	at        time.Time
	confirmed pglogrepl.LSN
	persisted pglogrepl.LSN
}

// watchSlotAckAgainstPersisted samples the slot's confirmed_flush_lsn and the
// target's persisted position for d, returning every sample where the first
// is past the second. A sample with no persisted row is skipped: nothing is
// durable yet, and the reader's floor is the slot's own creation LSN. Fails
// the test if too few samples could compare both sides — a watcher that
// never saw a persisted row would pass on nothing.
func watchSlotAckAgainstPersisted(t *testing.T, pgDSN, slot string, d time.Duration, persisted func() (pglogrepl.LSN, bool)) []ackViolation {
	t.Helper()
	var out []ackViolation
	compared := 0
	for deadline := time.Now().Add(d); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		// Persisted first, then confirmed: persisted only grows, so reading it
		// first can only make a sample look WORSE, never hide a violation.
		p, ok := persisted()
		if !ok {
			continue
		}
		c, ok := readConfirmedFlushLSN(t, pgDSN, slot)
		if !ok {
			continue
		}
		compared++
		if c > p {
			out = append(out, ackViolation{at: time.Now(), confirmed: c, persisted: p})
		}
	}
	if minCompared := int(d/time.Second) / 2; compared < minCompared {
		t.Fatalf("anti-vacuity: only %d of the watch's samples read both the slot and the persisted position "+
			"(want >= %d); the ack invariant was not actually checked", compared, minCompared)
	}
	return out
}

// readConfirmedFlushLSN reads the slot's confirmed_flush_lsn.
func readConfirmedFlushLSN(t *testing.T, pgDSN, slot string) (pglogrepl.LSN, bool) {
	t.Helper()
	db, err := sql.Open("pgx", pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var s sql.NullString
	err = db.QueryRow(`SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&s)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !s.Valid) {
		return 0, false
	}
	if err != nil {
		t.Fatalf("read confirmed_flush_lsn: %v", err)
	}
	lsn, err := pglogrepl.ParseLSN(s.String)
	if err != nil {
		t.Fatalf("parse confirmed_flush_lsn %q: %v", s.String, err)
	}
	return lsn, true
}

// lsnOfPositionToken parses the LSN out of a Postgres position token as the
// target persisted it.
func lsnOfPositionToken(token string) (pglogrepl.LSN, bool) {
	var p struct {
		LSN string `json:"lsn"`
	}
	if json.Unmarshal([]byte(token), &p) != nil || p.LSN == "" {
		return 0, false
	}
	lsn, err := pglogrepl.ParseLSN(p.LSN)
	return lsn, err == nil
}

// readPersistedLSNMySQL reads the stream's persisted source LSN from a MySQL
// target's control row.
func readPersistedLSNMySQL(dsn, streamID string) (pglogrepl.LSN, bool) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return 0, false
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var token string
	if db.QueryRowContext(ctx, "SELECT source_position FROM sluice_cdc_state WHERE stream_id = ?", streamID).Scan(&token) != nil {
		return 0, false
	}
	return lsnOfPositionToken(token)
}

// insertRowsOneTxEach inserts ids [from, to] into table, each in its own
// autocommit transaction.
func insertRowsOneTxEach(t *testing.T, dsn, table string, from, to int) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for id := from; id <= to; id++ {
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1, 'tx')", table), id); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
	}
}

// sourceRowCountPG is the independent expected value: the source's own count.
func sourceRowCountPG(t *testing.T, dsn, table string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("source count: %v", err)
	}
	return n
}
