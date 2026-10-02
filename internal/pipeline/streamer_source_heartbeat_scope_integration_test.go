//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-43 (e) — the source heartbeat's own table must never reach the
// applier.
//
// `--source-heartbeat-interval` writes rows into `sluice_heartbeat` on the
// source so an idle stream's position keeps moving. On a MySQL source those
// rows are in the binlog, and through v0.156.8 they reached the applier,
// which found no such table on the target, logged "target lacks this table —
// skipping" and counted every one in sluice_cdc_skipped_tables. `sync health`
// then reported SKIPPING and exited 1, telling the operator to `schema
// add-table` sluice's own bookkeeping. sluice_heartbeat was on the
// control-table roster, but the roster reached only the schema readers.
//
// The engine-level TestHeartbeat_AdvancesBinlogPosition proves only that a
// heartbeat write moves the SERVER's binlog tip (SHOW BINARY LOG STATUS)
// — the write it just made, read back from the server it made it on. This
// is the end-to-end half: the stream's PERSISTED position must advance on
// heartbeats alone, and the skip ledger `sync health` reads must stay empty.

package pipeline

import (
	"context"
	"database/sql"
	"io"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

func TestStreamer_MySQLSourceHeartbeat_AdvancesPositionWithoutSkips(t *testing.T) {
	gc43HeartbeatScopeCase(t, "gc43-heartbeat", "")
}

// TestStreamer_MySQLCustomHeartbeatTable_StaysOutOfScope is the same case
// under --source-heartbeat-table-name. The roster lists only the default
// name, and the writer creates its table BEFORE the cold-start schema read,
// so through v0.156.8 a renamed heartbeat table was enumerated as user
// data: created on the target, copied, and kept in sync from then on. The
// streamer now marks the configured name sluice-owned in its filter.
func TestStreamer_MySQLCustomHeartbeatTable_StaysOutOfScope(t *testing.T) {
	gc43HeartbeatScopeCase(t, "gc43-custom-heartbeat", "ops_heartbeat")
}

// gc43HeartbeatScopeCase runs a MySQL→MySQL stream with the source
// heartbeat on — under hbTable, or the default name when it is empty — and
// asserts the heartbeats move the persisted position, never reach the skip
// ledger, and never put their table on the target.
func gc43HeartbeatScopeCase(t *testing.T, streamID, hbTable string) {
	t.Helper()
	table := hbTable
	if table == "" {
		table = DefaultSourceHeartbeatTableName
	}
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyDDLMySQL(t, sourceDSN, `
		CREATE TABLE hb_users (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(32) NOT NULL) ENGINE=InnoDB;
		INSERT INTO hb_users VALUES (1, 'seed');
	`)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	streamer := &Streamer{
		Source:                   mysqlEng,
		Target:                   mysqlEng,
		SourceDSN:                sourceDSN,
		TargetDSN:                targetDSN,
		StreamID:                 streamID,
		SourceHeartbeatInterval:  500 * time.Millisecond,
		SourceHeartbeatTableName: hbTable,
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-runErr:
		case <-time.After(30 * time.Second):
			t.Error("Streamer.Run did not return after ctx cancel")
		}
	}
	defer stop()

	if !waitForRowCountMySQL(t, targetDSN, "hb_users", 1, 90*time.Second) {
		t.Fatal("cold start never delivered the seed row")
	}

	actx, acancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer acancel()
	applier, err := mysqlEng.OpenChangeApplier(actx, targetDSN)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	if c, ok := applier.(io.Closer); ok {
		defer func() { _ = c.Close() }()
	}

	// No user writes from here on: only the heartbeat writes to the source.
	//
	// Order matters, and each step polls rather than checking once. The
	// heartbeat writer attaches asynchronously and its first INSERT lands
	// one interval after that; under CI's -race build that took longer
	// than the stream's position took to move for other reasons, so the
	// old "position moved, now count the rows" order tripped the
	// anti-vacuity floor with no heartbeat yet written. So: wait for
	// heartbeat rows on the source (the floor), THEN take the position,
	// THEN wait for it to move — a move that, with no user writes, only
	// the heartbeats can have caused.
	if !gc43AwaitCountMySQL(t, sourceDSN, "SELECT COUNT(*) FROM `"+table+"`", 1, 120*time.Second) {
		t.Fatal("no heartbeat rows on the source; the test proved nothing about them")
	}
	first := gc43AwaitPosition(t, applier, streamID, "")
	gc43AwaitPosition(t, applier, streamID, first.Token)

	// A user change still arrives, so the stream is live, not merely stamping.
	applyDDLMySQL(t, sourceDSN, "INSERT INTO hb_users VALUES (2, 'after-heartbeats');")
	if !waitForRowCountMySQL(t, targetDSN, "hb_users", 2, 60*time.Second) {
		t.Fatal("a user insert after the heartbeats never arrived")
	}
	stop()

	// The ledger `sync health` trips on, read after the stream stopped so
	// nothing it buffered is still in flight.
	lister, ok := applier.(ir.SkippedTableLister)
	if !ok {
		t.Fatal("mysql applier no longer implements ir.SkippedTableLister")
	}
	records, err := lister.ListSkippedTables(actx)
	if err != nil {
		t.Fatalf("ListSkippedTables: %v", err)
	}
	for _, rec := range records {
		if rec.StreamID == streamID && rec.SkipCount > 0 {
			t.Errorf("stream skipped %d CDC event(s) for %q — `sync health` would report SKIPPING and exit 1 for sluice's own heartbeat table",
				rec.SkipCount, rec.Table)
		}
	}
	if n := gc43CountMySQL(t, targetDSN, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '"+table+"'"); n != 0 {
		t.Errorf("heartbeat table %q appeared on the target", table)
	}
}

// gc43AwaitPosition waits until the stream has a persisted position whose
// token differs from not, and returns it.
func gc43AwaitPosition(t *testing.T, applier ir.ChangeApplier, streamID, not string) ir.Position {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	var last ir.Position
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pos, found, err := applier.ReadPosition(ctx, streamID)
		cancel()
		if err == nil && found && pos.Token != "" && pos.Token != not {
			return pos
		}
		last = pos
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("persisted position for %q never moved off %q (last read %q)", streamID, not, last.Token)
	return ir.Position{}
}

// gc43AwaitCountMySQL polls q (a single-COUNT query) until it returns at
// least want or the timeout passes. The table may not exist yet (the
// heartbeat writer creates it on attach), so a query error means "not
// yet" rather than failure.
func gc43AwaitCountMySQL(t *testing.T, dsn, q string, want int, timeout time.Duration) bool {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var n int
		err := db.QueryRowContext(ctx, q).Scan(&n)
		cancel()
		if err == nil && n >= want {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

func gc43CountMySQL(t *testing.T, dsn, q string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}
