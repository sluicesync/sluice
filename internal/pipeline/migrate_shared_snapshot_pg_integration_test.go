//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the migrate shared exported snapshot (perf
// research delta 1, migrate_snapshot.go): on a PG source being mutated
// mid-copy, EVERY parallel reader — the primary, the cross-table pool
// pairs, the within-table chunks — must observe the ONE exported
// snapshot's view, and the snapshot must be released the moment the
// copy phase drains, not at run teardown.
//
// The mutation is deterministic, not timing-based: the test-only
// migrateSharedSnapshotExportedObserver seam fires right after the
// snapshot is exported and strictly before any copy reader reads, and
// the test inserts its marker rows there. The marker PKs extend past
// the seeded range, so they fall inside the LAST chunk's unbounded
// upper range — exactly the rows an UNPINNED late-opening chunk reader
// (its own fresh MVCC view) would have copied. Zero markers on the
// target is therefore the pinned-view proof, not a coincidence of
// scheduling.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	// Register the postgres engine so engines.Get("postgres") works.
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

func TestMigrate_PG_SharedSnapshot_ConsistencyAndCopyEndRelease(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const preRows = 50_000
	const markerRows = 1_000
	seedLargeIntPK(t, sourceDSN, "events", preRows)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	srcDB, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()

	// Marker insert at the deterministic post-export point: these rows
	// commit AFTER the snapshot exists and BEFORE any reader reads.
	var markerInsertErr atomic.Value
	var exportedFired atomic.Int32
	migrateSharedSnapshotExportedObserver = func() {
		exportedFired.Add(1)
		if _, err := srcDB.Exec(
			"INSERT INTO events (id, label) SELECT g, 'marker-' || g FROM generate_series($1::bigint, $2::bigint) AS g",
			preRows+1, preRows+markerRows,
		); err != nil {
			markerInsertErr.Store(err)
		}
	}
	defer func() { migrateSharedSnapshotExportedObserver = nil }()

	// Release probe: at release time (copy phase drained, exporting tx
	// committed, importer closed) NO snapshot session may linger idle-in-
	// transaction on the source — the pg_stat_activity assertion the spec
	// calls for, taken at the release point rather than after teardown
	// where it would be vacuously true. May fire on the copy producer
	// goroutine (the PG target takes the ADR-0077 overlapped branch), so
	// everything is atomics; asserts run after Run returns.
	var releasedFired atomic.Int32
	idleInTxAtRelease := atomic.Int64{}
	idleInTxAtRelease.Store(-1)
	var releaseProbeErr atomic.Value
	migrateSharedSnapshotReleasedObserver = func() {
		releasedFired.Add(1)
		var n int64
		if err := srcDB.QueryRow(
			"SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction'",
		).Scan(&n); err != nil {
			releaseProbeErr.Store(err)
			return
		}
		idleInTxAtRelease.Store(n)
	}
	defer func() { migrateSharedSnapshotReleasedObserver = nil }()

	logs := captureSlog(t)
	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		BulkParallelism:     4,
		BulkParallelMinRows: 10_000, // below the seed so within-table chunking engages
		MigrationID:         "test-shared-snapshot",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	if got := exportedFired.Load(); got != 1 {
		t.Fatalf("exported observer fired %d times; want 1 (shared snapshot not engaged?)", got)
	}
	if err, _ := markerInsertErr.Load().(error); err != nil {
		t.Fatalf("marker insert: %v", err)
	}
	if !strings.Contains(logs.String(), "parallel readers pinned to one shared exported snapshot") {
		t.Errorf("expected the shared-snapshot engage log; got:\n%s", logs.String())
	}

	// Consistency: every pre-export row landed, no post-export marker did.
	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	var gotRows, gotMarkers int
	if err := tgtDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&gotRows); err != nil {
		t.Fatalf("count target rows: %v", err)
	}
	if err := tgtDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM events WHERE label LIKE 'marker-%'").Scan(&gotMarkers); err != nil {
		t.Fatalf("count target markers: %v", err)
	}
	if gotMarkers != 0 {
		t.Errorf("target holds %d post-snapshot marker rows; want 0 — a copy reader observed a fresher view than the exported snapshot (unpinned reader)", gotMarkers)
	}
	if gotRows != preRows {
		t.Errorf("target rows = %d; want %d (every pre-snapshot row, nothing else)", gotRows, preRows)
	}

	// The source really has both generations (the markers committed).
	var srcRows int
	if err := srcDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&srcRows); err != nil {
		t.Fatalf("count source rows: %v", err)
	}
	if srcRows != preRows+markerRows {
		t.Fatalf("source rows = %d; want %d (markers must have committed for the test to prove anything)", srcRows, preRows+markerRows)
	}

	// Release discipline: fired exactly once, at copy end, with no
	// idle-in-transaction snapshot session left on the source.
	if got := releasedFired.Load(); got != 1 {
		t.Fatalf("release observer fired %d times; want exactly 1", got)
	}
	if err, _ := releaseProbeErr.Load().(error); err != nil {
		t.Fatalf("release-time pg_stat_activity probe: %v", err)
	}
	if got := idleInTxAtRelease.Load(); got != 0 {
		t.Errorf("idle-in-transaction sessions on the source at release time = %d; want 0 (the exporting/importing snapshot txs must be gone before the index/constraint phases)", got)
	}
}
