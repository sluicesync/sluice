//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pipeline-level end-to-end pin for the VStream-flavor index-overlap wiring
// (audit T′-1, the adjudicated flavor-routing gap): the orchestrator's
// runOverlappedCopyAndIndexPhase had -race unit pins and VANILLA e2e pins,
// but nothing drove it end-to-end against a VStream-flavor TARGET — the
// engine-level vstream pin (mysql/schema_writer_index_vstream_integration_
// test.go) pre-fills the completed-tables channel by hand, so the pipeline's
// flavor routing (Migrator → IncrementalIndexBuilder → the usesVStream()
// serial build-as-copied branch) ran e2e in no test. That routing is exactly
// where the v0.99.199 silent-index-loss bug lived (the orchestrator's
// UNREACHABLE non-IIB else branch masked a no-op gate).
//
// This boots a real vttestserver as the migrate TARGET (engines.Get("vitess")
// → usesVStream() → the serial branch) plus a vanilla MySQL source, runs a
// real multi-table migrate through Migrator.Run, and asserts:
//
//   (a) every expected secondary index exists on the Vitess target post-
//       migrate (the v0.99.199 class, ground-truthed via
//       information_schema.statistics through vtgate — independent of the
//       in-pipeline SLUICE-E-INDEX-MISSING net, which also runs);
//   (b) the overlap genuinely ENGAGED on the VStream branch:
//       min(indexBuildStart) < max(copyComplete) via the test seams (the
//       mysql engine's index-build-start seam — which the serial branch now
//       fires — and the pipeline's copy-complete seam);
//   (c) the run COMPLETES with a deliberately SLOW builder (the index
//       observer sleeps per table): the completed-tables channel is buffered
//       to #tables, so a slow serial build must never back-pressure or
//       deadlock the copy pool.
//
// Index families here are plain BTREE / UNIQUE / composite — cheap and
// vtgate-safe; the per-family matrix on a real Vitess target (incl. the
// FK-backing key) is pinned at the engine level by
// TestVStream_VTTestServer_SecondaryIndexesBuildAndVerify, and the
// FULLTEXT/SPATIAL ALTER shapes by the vanilla pipeline pin. This test's
// load-bearing axis is the FLAVOR ROUTING, not the ALTER dispatch.
//
// NAME CONTRACT: the test name must match the extended-suites.yml
// vstream-pipeline leg's -run filter `^(TestMigrate_VStream|...)` — enforced
// by scripts/check-run-filter-coverage.sh; a mismatch means it compiles but
// runs in NO workflow (the exact class this pin exists to close).
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
//	$env:TESTCONTAINERS_RYUK_DISABLED = "true"
//	go test -tags='integration vstream' -v -count=1 -timeout=20m \
//	  -run 'TestMigrate_VStreamTarget_IndexOverlap' ./internal/pipeline/...

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/mysql"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestMigrate_VStreamTarget_IndexOverlap_EngagesAndBuildsAll drives a real
// migrate (vanilla MySQL source → vttestserver Vitess target) through the
// live overlap wiring and pins (a) all indexes present, (b) overlap engaged
// on the serial VStream branch, (c) completion under a slow builder.
func TestMigrate_VStreamTarget_IndexOverlap_EngagesAndBuildsAll(t *testing.T) {
	const (
		keyspace   = "idxtgt"
		tableCount = 8
		rowsEach   = 4000
		// slowBuild is the per-table builder delay (installed via the
		// index-build-start seam, which fires on the builder goroutine):
		// 8 tables × 300ms ≈ 2.4s of serial build tail, far above the copy
		// pool's per-table cadence — the (c) no-back-pressure axis.
		slowBuild = 300 * time.Millisecond
	)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	vitessEng, ok := engines.Get("vitess")
	if !ok {
		t.Fatal("vitess engine not registered")
	}

	// Target: single-shard vttestserver behind vtgate (the cheap real-Vitess
	// stand-in the vstream suite uses). restartSource/grpc unused — the
	// target side is plain vtgate MySQL protocol; no VStream params needed.
	vtDSN, _, _, vtCleanup := startShardedVTTestServer(t, keyspace, 1)
	defer vtCleanup()

	// Source: a vanilla MySQL container (source_db; the sibling target_db is
	// unused here — the migrate lands on the Vitess side).
	srcDSN, _, srcCleanup := startMySQL(t)
	defer srcCleanup()
	seedIndexedTablesForVStreamTarget(t, srcDSN, tableCount, rowsEach)

	// Observability seams: the pipeline copy-complete seam records
	// max(copyComplete); the mysql engine index-build-start seam — fired by
	// the VStream serial build-as-copied branch — records
	// min(indexBuildStart) AND sleeps, making the serial builder slow.
	var obsMu sync.Mutex
	var firstIndexStart, lastCopyComplete time.Time
	restoreCopy := setOnTableCopiedObserverForTest(func(_ string) {
		obsMu.Lock()
		lastCopyComplete = time.Now()
		obsMu.Unlock()
	})
	defer restoreCopy()
	restoreIdx := mysql.SetIndexBuildStartObserverForTest(func(_ string) {
		obsMu.Lock()
		if firstIndexStart.IsZero() {
			firstIndexStart = time.Now()
		}
		obsMu.Unlock()
		time.Sleep(slowBuild) // the deliberately slow builder (axis c)
	})
	defer restoreIdx()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	mig := &Migrator{
		Source:           mysqlEng,
		Target:           vitessEng,
		SourceDSN:        srcDSN,
		TargetDSN:        vtDSN,
		TableParallelism: 2,
		// Keep tables single-streamed so the cross-table + index axes are
		// what is exercised, not within-table chunking.
		BulkParallelMinRows: int64(rowsEach * 10),
		MigrationID:         "vstream-idx-overlap",
	}
	// Axis (c): with the slow serial builder this must still complete — a
	// back-pressured copy pool or a builder deadlock times the ctx out.
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Run (vanilla MySQL → Vitess target): %v", err)
	}

	// Axis (a): every expected secondary index exists on the Vitess target
	// (ground truth through vtgate, independent of the in-run
	// SLUICE-E-INDEX-MISSING verification that already gated Run).
	assertNamedMySQLIndexesPresent(ctx, t, vtDSN, tableCount, func(name string) []string {
		return []string{name + "_u_uidx", name + "_v_idx", name + "_uv_idx"}
	})

	// Zero-loss: counts + checksums source vs Vitess target.
	assertManyIndexedTablesZeroLossMySQL(ctx, t, srcDSN, vtDSN, tableCount)

	// Axis (b): the overlap genuinely engaged on the VStream serial branch —
	// an index build STARTED before the last copy completed. With 8 tables
	// on a 2-wide copy pool the first table's build begins while later
	// copies are still running; if the wiring regressed to a post-copy
	// (or no-op) index phase, firstIndexStart would be zero or after
	// lastCopyComplete.
	obsMu.Lock()
	fis, lcc := firstIndexStart, lastCopyComplete
	obsMu.Unlock()
	if fis.IsZero() {
		t.Fatal("no index build ever started on the VStream branch (engine seam never fired) — the flavor routing regressed toward the v0.99.199 no-op class")
	}
	if lcc.IsZero() {
		t.Fatal("no copy ever completed (pipeline seam never fired)")
	}
	t.Logf("overlap: firstIndexStart=%s lastCopyComplete=%s (delta=%s)",
		fis.Format(time.StampMilli), lcc.Format(time.StampMilli), lcc.Sub(fis))
	if !fis.Before(lcc) {
		t.Errorf("index builds did NOT overlap the copy on the VStream branch: firstIndexStart=%s >= lastCopyComplete=%s "+
			"(the serial build-as-copied overlap regressed to a post-copy phase)", fis, lcc)
	}
}

// seedIndexedTablesForVStreamTarget creates tableCount tables on the vanilla
// MySQL source, each with rowsEach rows and three vtgate-safe secondary
// index shapes — UNIQUE single-column, plain BTREE, composite multi-column
// (FULLTEXT/SPATIAL are pinned elsewhere; see the file header). Numeric
// columns are PK-derived so a checksum catches loss/dup.
func seedIndexedTablesForVStreamTarget(t *testing.T, dsn string, tableCount, rowsEach int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for i := 0; i < tableCount; i++ {
		name := fmt.Sprintf("tbl_%02d", i)
		ddl := fmt.Sprintf(`
			CREATE TABLE %s (
				id BIGINT PRIMARY KEY,
				u  BIGINT NOT NULL,
				v  BIGINT NOT NULL,
				UNIQUE INDEX %s_u_uidx (u),
				INDEX %s_v_idx (v),
				INDEX %s_uv_idx (u, v)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, name, name, name, name)
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		const batch = 500
		for off := 0; off < rowsEach; off += batch {
			vals := ""
			for j := off; j < off+batch && j < rowsEach; j++ {
				if vals != "" {
					vals += ","
				}
				vals += fmt.Sprintf("(%d,%d,%d)", j+1, j+1, (j+1)%97)
			}
			if _, err := db.ExecContext(ctx,
				fmt.Sprintf("INSERT INTO %s (id,u,v) VALUES %s", name, vals)); err != nil {
				t.Fatalf("seed %s: %v", name, err)
			}
		}
	}
}
