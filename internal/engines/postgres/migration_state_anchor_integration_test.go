//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The A0909-STOP-1 snapshot-anchor column on real PostgreSQL: the
// cross-version read, byte-exact round-tripping, and the disjointness
// the two header writers depend on.
//
// This is a persisted-state codec, so it gets the new-surface
// treatment: the value that comes back out is compared byte-for-byte
// against what went in, across the shapes a position token can carry
// — not one representative — because a store that normalised it would
// either cost a re-copy (compares unequal) or authorise one it should
// not (compares equal to something else). Its MySQL twin is
// internal/engines/mysql/migration_state_anchor_integration_test.go.

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// anchorTokenMatrix is the shape matrix, not one representative.
var anchorTokenMatrix = []struct {
	name  string
	token string
}{
	{"the real postgres token", `{"slot":"sluice_slot","lsn":"0/1946620"}`},
	{"with the ADR-0051 identity pin", `{"slot":"sluice_slot","lsn":"1A/FFFFFFFF","systemid":"7412345678901234567","timeline":3}`},
	{"quotes and backslashes", `{"slot":"sluice_a\"b\\c","lsn":"0/1"}`},
	{"multi-byte", `{"slot":"sluice_données_🚰","lsn":"0/2"}`},
	{"trailing whitespace", "{\"slot\":\"sluice_slot\",\"lsn\":\"0/3\"}  \t"},
	{"long", `{"slot":"sluice_` + strings.Repeat("s", 400) + `","lsn":"0/4"}`},
}

// TestMigrationStateStore_SnapshotAnchorRoundTripsOnRealPostgres pins
// the column's verbatim contract against a real server — the driver,
// the column type and the SQL are all in the loop here, which the
// scripted unit test cannot cover.
func TestMigrationStateStore_SnapshotAnchorRoundTripsOnRealPostgres(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	store, err := Engine{}.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenMigrationStateStore: %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	recorder, ok := store.(ir.SnapshotAnchorRecorder)
	if !ok {
		t.Fatal("the postgres migrate-state store does not implement ir.SnapshotAnchorRecorder, so a cold " +
			"start against a postgres target records no anchor and can never be resumed")
	}

	for i, tc := range anchorTokenMatrix {
		t.Run(tc.name, func(t *testing.T) {
			id := "sync-anchor-" + string(rune('a'+i))
			if err := recorder.WriteSnapshotAnchor(ctx, id, tc.token); err != nil {
				t.Fatalf("WriteSnapshotAnchor: %v", err)
			}
			got, found, err := store.Read(ctx, id)
			if err != nil || !found {
				t.Fatalf("Read = found=%v err=%v", found, err)
			}
			if got.SnapshotAnchor != tc.token {
				t.Fatalf("anchor round-tripped as %q; want %q byte-for-byte", got.SnapshotAnchor, tc.token)
			}
			// The anchor write creates the header at `pending` when there
			// is none, so the row is readable before any phase mark.
			if got.Phase != ir.MigrationPhasePending {
				t.Errorf("phase on an anchor-created header = %q; want pending", got.Phase)
			}
		})
	}
}

// TestMigrationStateStore_AnchorAndPhaseWritersAreDisjoint pins the
// property the whole design rests on: the phase writer must not erase
// the anchor, and the anchor writer must not move the phase.
//
// If either clobbered the other, the failure would be silent — a
// stopped cold start that simply is not resumable, or a resume that
// runs the wrong post-copy phases.
func TestMigrationStateStore_AnchorAndPhaseWritersAreDisjoint(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	store, err := Engine{}.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenMigrationStateStore: %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	recorder := store.(ir.SnapshotAnchorRecorder)

	const id = "sync-disjoint"
	const anchor = `{"slot":"sluice_slot","lsn":"0/ABCDEF0"}`
	if err := recorder.WriteSnapshotAnchor(ctx, id, anchor); err != nil {
		t.Fatalf("WriteSnapshotAnchor: %v", err)
	}
	// Every phase a cold start walks, in order. The anchor must survive
	// all of them — one clobbering write anywhere in the ladder is
	// enough to make the resume impossible.
	for _, phase := range []ir.MigrationPhase{
		ir.MigrationPhaseTables, ir.MigrationPhaseBulkCopy, ir.MigrationPhaseIdentitySync,
		ir.MigrationPhaseIndexes, ir.MigrationPhaseConstraints, ir.MigrationPhaseViews,
		ir.MigrationPhaseComplete,
	} {
		if err := store.Write(ctx, ir.MigrationState{MigrationID: id, Phase: phase}); err != nil {
			t.Fatalf("Write(%s): %v", phase, err)
		}
		got, found, err := store.Read(ctx, id)
		if err != nil || !found {
			t.Fatalf("Read after %s = found=%v err=%v", phase, found, err)
		}
		if got.SnapshotAnchor != anchor {
			t.Fatalf("the %s phase write ERASED the snapshot anchor (%q); the stopped run silently stops "+
				"being resumable", phase, got.SnapshotAnchor)
		}
		if got.Phase != phase {
			t.Fatalf("phase = %q after writing %q", got.Phase, phase)
		}
	}

	// And the reverse: re-recording an anchor must not move the phase.
	const anchor2 = `{"slot":"sluice_slot","lsn":"0/BBBBBBB"}`
	if err := recorder.WriteSnapshotAnchor(ctx, id, anchor2); err != nil {
		t.Fatalf("WriteSnapshotAnchor (second): %v", err)
	}
	got, _, err := store.Read(ctx, id)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.Phase != ir.MigrationPhaseComplete {
		t.Errorf("the anchor write MOVED the phase to %q; it must touch the anchor column only", got.Phase)
	}
	if got.SnapshotAnchor != anchor2 {
		t.Errorf("anchor = %q; want the re-recorded %q (a fresh cold start must replace the old run's)",
			got.SnapshotAnchor, anchor2)
	}
}

// TestMigrationStateStore_PreAnchorRowReadsCleanly is the CROSS-VERSION
// pin: a header table and row in the shape a binary older than the
// column left behind must read cleanly once EnsureControlTable adds
// the column, with no anchor and everything else intact.
//
// The fixture is the OLD DDL, not this binary's — a fixture built from
// post-change values would only prove the binary agrees with itself.
func TestMigrationStateStore_PreAnchorRowReadsCleanly(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg, err := Engine{}.parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	// The header shape sluice shipped BEFORE snapshot_anchor: with
	// state_format (so this is not merely the ADR-0082 legacy pin
	// again), without the anchor column.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE sluice_migrate_state (
			migration_id    VARCHAR(255) NOT NULL,
			phase           VARCHAR(32)  NOT NULL,
			table_progress  TEXT         NULL,
			state_format    INT          NOT NULL DEFAULT 1,
			started_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			last_error      TEXT         NULL,
			PRIMARY KEY (migration_id)
		)`); err != nil {
		t.Fatalf("create pre-anchor table: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"INSERT INTO sluice_migrate_state (migration_id, phase, table_progress, state_format, last_error) "+
			"VALUES ($1, $2, $3, 2, $4)",
		"sync-old", "indexes", "upgraded", "boom"); err != nil {
		t.Fatalf("insert pre-anchor row: %v", err)
	}

	store, err := Engine{}.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenMigrationStateStore: %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable on a pre-anchor table: %v", err)
	}
	got, found, err := store.Read(ctx, "sync-old")
	if err != nil {
		t.Fatalf("Read of a pre-anchor row: %v — an older binary's row must still read", err)
	}
	if !found {
		t.Fatal("Read found=false for a row written by an older binary")
	}
	if got.SnapshotAnchor != "" {
		t.Errorf("SnapshotAnchor = %q; want empty (no evidence) for a row that predates the column", got.SnapshotAnchor)
	}
	if got.Phase != ir.MigrationPhaseIndexes || got.LastError != "boom" {
		t.Errorf("the pre-anchor row's other fields did not survive: %+v", got)
	}
	// And the added column is writable straight away.
	if err := store.(ir.SnapshotAnchorRecorder).WriteSnapshotAnchor(ctx, "sync-old", `{"slot":"s","lsn":"0/1"}`); err != nil {
		t.Fatalf("WriteSnapshotAnchor after the column migration: %v", err)
	}
}
