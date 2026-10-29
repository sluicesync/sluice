//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared/streamlivenesstest"
	"sluicesync.dev/sluice/internal/ir"
)

// TestStreamLivenessStore_RoundTrip pins the MySQL liveness column
// (DRAINED-ADD-TABLE-RUNNING-STREAM-UNCHECKED) against a real server, from a
// control table a pre-column binary created (see
// [streamlivenesstest.RoundTrip]): a fresh stamp reads running, a stale one
// and a recorded stop read stopped, a later stamp cannot revive a stop, and a
// missing row or table is nothing found.
func TestStreamLivenessStore_RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, applier := tableSetsStoreFixture(ctx, t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	streamlivenesstest.RoundTrip(ctx, t, applier.(ir.StreamLivenessStore), streamlivenesstest.Raw{
		Set: func(ms *int64) {
			if _, err := db.ExecContext(ctx, "UPDATE sluice_cdc_state SET stream_alive_ms = ? WHERE stream_id = 's1'", ms); err != nil {
				t.Fatal(err)
			}
		},
		Drop: func() {
			if _, err := db.ExecContext(ctx, "DROP TABLE sluice_cdc_state"); err != nil {
				t.Fatal(err)
			}
		},
	})
}

// TestStreamLivenessStore_LeavesUpdatedAtAlone: on the control table sluice
// creates, updated_at is ON UPDATE CURRENT_TIMESTAMP, so an UPDATE that did
// not name it would move it on every stamp. Every stamp assigns it its own
// value, which suppresses the bump: the column keeps saying when something
// other than the liveness stamp last wrote the row.
func TestStreamLivenessStore_LeavesUpdatedAtAlone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, cleanup := startMySQLForApplier(t)
	t.Cleanup(cleanup)
	applyMySQLApplier(t, dsn, controlTableDDL(""))
	applyMySQLApplier(t, dsn, "INSERT INTO sluice_cdc_state (stream_id, source_position, updated_at) VALUES ('s1', 'tok', '2001-02-03 04:05:06')")
	applier, err := Engine{Flavor: FlavorVanilla}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = applier.(interface{ Close() error }).Close() }()
	store := applier.(ir.StreamLivenessStore)
	for _, step := range []func() error{
		func() error { _, err := store.StampStreamAlive(ctx, "s1"); return err },
		func() error { _, err := store.StampStreamAlive(ctx, "s1"); return err },
		func() error { return store.RecordStreamStopped(ctx, "s1") },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var updated string
	if err := db.QueryRowContext(ctx, "SELECT CAST(updated_at AS CHAR) FROM sluice_cdc_state WHERE stream_id = 's1'").Scan(&updated); err != nil {
		t.Fatal(err)
	}
	if updated != "2001-02-03 04:05:06" {
		t.Fatalf("updated_at = %s after three liveness stamps; want it untouched (2001-02-03 04:05:06)", updated)
	}
	if l, err := store.ReadStreamLiveness(ctx, "s1"); err != nil || !l.Stopped {
		t.Fatalf("liveness %+v, %v; want Stopped", l, err)
	}
}

// TestPositionWriteStampsLiveness (F1): a position write stamps the liveness
// column in its own statement, overwriting a stopped marker, once the applier
// knows the column is there; on a control table without the column the same
// write works and names no such column (a role that could not add it keeps
// streaming).
func TestPositionWriteStampsLiveness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, applier := tableSetsStoreFixture(ctx, t)
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() "+
		"AND TABLE_NAME = 'sluice_cdc_state' AND COLUMN_NAME = 'stream_alive_ms'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("RIG: the legacy fixture already has the liveness column (%d, %v)", n, err)
	}
	if err := applier.(ir.PositionWriter).WritePosition(ctx, "s1", ir.Position{Engine: "mysql", Token: "tok-2"}); err != nil {
		t.Fatalf("a position write on a control table without the liveness column: %v", err)
	}
	store := applier.(ir.StreamLivenessStore)
	if _, err := store.StampStreamAlive(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordStreamStopped(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if err := applier.(ir.PositionWriter).WritePosition(ctx, "s1", ir.Position{Engine: "mysql", Token: "tok-3"}); err != nil {
		t.Fatal(err)
	}
	l, err := store.ReadStreamLiveness(ctx, "s1")
	if err != nil || l.Stopped || !l.Recorded || l.Age() > time.Minute || l.Age() < -time.Minute {
		t.Fatalf("after a position write over a stopped marker: %+v, %v; want a fresh running stamp", l, err)
	}
	// Monotonic (the second fix-aimed review of 6b5898dc, F5): a position
	// write over a stamp from the future (a skewed clock, a stamp raised
	// past its predecessor) writes previous+1, never the value it replaces
	// and never an older one, so a start's compare-and-set restore cannot
	// mistake it for that start's own stamp.
	future := time.Now().Add(time.Hour).UnixMilli()
	if _, err := db.ExecContext(ctx, "UPDATE sluice_cdc_state SET stream_alive_ms = ? WHERE stream_id = 's1'", future); err != nil {
		t.Fatal(err)
	}
	if err := applier.(ir.PositionWriter).WritePosition(ctx, "s1", ir.Position{Engine: "mysql", Token: "tok-4"}); err != nil {
		t.Fatal(err)
	}
	var got int64
	if err := db.QueryRowContext(ctx, "SELECT stream_alive_ms FROM sluice_cdc_state WHERE stream_id = 's1'").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != future+1 {
		t.Fatalf("a position write over a future stamp %d wrote %d; want %d (previous + 1)", future, got, future+1)
	}
}
