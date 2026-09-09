//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The MySQL twin of the postgres snapshot-anchor pins (see
// internal/engines/postgres/migration_state_anchor_integration_test.go
// for the rationale; the matrix and assertions are kept the same shape
// so both engines are pinned against the same values).
//
// The engine sibling is NOT decorative here. The column arrives by a
// different mechanism on each engine — ADD COLUMN IF NOT EXISTS there,
// detect-then-ALTER here — the upsert uses a different spelling, and
// this store is a live TARGET for a PostgreSQL source's cold start, so
// the anchor that a MySQL-target resume would need is written through
// exactly this code. An engine-name-shaped assumption that "only
// postgres needs the column" would leave a PG→MySQL cold start
// unresumable.

package mysql

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

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

func TestMigrationStateStore_SnapshotAnchorRoundTripsOnRealMySQL(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
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
		t.Fatal("the mysql migrate-state store does not implement ir.SnapshotAnchorRecorder, so a PostgreSQL " +
			"source's cold start into a MySQL target records no anchor and can never be resumed")
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
			if got.Phase != ir.MigrationPhasePending {
				t.Errorf("phase on an anchor-created header = %q; want pending", got.Phase)
			}
		})
	}
}

func TestMigrationStateStore_AnchorAndPhaseWritersAreDisjointMySQL(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
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
			t.Fatalf("the %s phase write ERASED the snapshot anchor (%q)", phase, got.SnapshotAnchor)
		}
		if got.Phase != phase {
			t.Fatalf("phase = %q after writing %q", got.Phase, phase)
		}
	}

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
		t.Errorf("anchor = %q; want the re-recorded %q", got.SnapshotAnchor, anchor2)
	}
}

// TestMigrationStateStore_PreAnchorRowReadsCleanlyMySQL is the
// cross-version pin against the header shape sluice shipped before the
// column, exercising the detect-then-ALTER migration this engine uses.
func TestMigrationStateStore_PreAnchorRowReadsCleanlyMySQL(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg, err := parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	db, err := openDB(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	const preAnchorDDL = "CREATE TABLE `sluice_migrate_state` (" +
		"migration_id    VARCHAR(255) NOT NULL," +
		"phase           VARCHAR(32)  NOT NULL," +
		"table_progress  TEXT         NULL," +
		"state_format    INT          NOT NULL DEFAULT 1," +
		"started_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
		"last_error      TEXT         NULL," +
		"ps_query_timeout_raise TEXT  NULL," +
		"PRIMARY KEY (migration_id)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
	if _, err := db.ExecContext(ctx, preAnchorDDL); err != nil {
		t.Fatalf("create pre-anchor table: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"INSERT INTO `sluice_migrate_state` (migration_id, phase, table_progress, state_format, last_error) "+
			"VALUES (?, ?, ?, 2, ?)",
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
		t.Errorf("SnapshotAnchor = %q; want empty (no evidence)", got.SnapshotAnchor)
	}
	if got.Phase != ir.MigrationPhaseIndexes || got.LastError != "boom" {
		t.Errorf("the pre-anchor row's other fields did not survive: %+v", got)
	}
	if err := store.(ir.SnapshotAnchorRecorder).WriteSnapshotAnchor(ctx, "sync-old", `{"slot":"s","lsn":"0/1"}`); err != nil {
		t.Fatalf("WriteSnapshotAnchor after the column migration: %v", err)
	}
}
