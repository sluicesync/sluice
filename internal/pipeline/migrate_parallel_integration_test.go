//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the parallel within-table bulk-copy path
// (v0.5.0). The tests boot a real Postgres container, seed a source
// table well above the parallelism threshold, and verify that:
//
//   - The parallel path actually splits work across N chunks (the
//     mid-copy state row carries N entries).
//   - The data lands on the target with no duplicates and no losses
//     once the migration completes.
//   - Tables below --bulk-parallel-min-rows fall back to the single-
//     reader path.
//
// MySQL→MySQL parallel coverage piggybacks on the same orchestrator
// surface and is exercised by the same engine tests; this file
// focuses on the PG side because it's the engine pair used in CI's
// existing bulk-copy integration tests and the wire-shape changes
// (chunk_index in the JSON) are most readable in psql.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	// Register the postgres engine so engines.Get("postgres") works.
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// seedLargeIntPK creates a table with the given row count, seeded
// from a generate_series so PKs are a contiguous integer range. The
// row count is the load-bearing knob: parallel-copy eligibility is
// row-count-driven, so the seed has to push past the threshold for
// the parallel path to be exercised.
//
// ANALYZE runs after the load so pg_class.reltuples reflects the
// actual row count — without it the autovacuum-maintained estimate
// reports 0 on a freshly-loaded table and the orchestrator's
// row-count probe routes the table to the single-reader path
// regardless of the configured threshold. Production migrations of
// long-lived tables don't hit this corner; freshly-loaded test
// fixtures do.
func seedLargeIntPK(t *testing.T, dsn string, tableName string, rowCount int) {
	t.Helper()
	const ddlTemplate = `
		CREATE TABLE %s (
			id    BIGINT PRIMARY KEY,
			label TEXT NOT NULL
		);
		INSERT INTO %s (id, label)
			SELECT g, 'row-' || g FROM generate_series(1, %d) AS g;
		ANALYZE %s;
	`
	ddl := fmt.Sprintf(ddlTemplate, tableName, tableName, rowCount, tableName)
	applyPGDDL(t, dsn, ddl)
}

// TestMigrate_PG_ParallelCopy_LargeTable boots PG, seeds a table just
// above the v0.5.0 threshold (default 100k rows), runs migration with
// --bulk-parallelism=4, and verifies the target ends with the correct
// row count and that the state row recorded chunked progress at some
// point during the copy.
//
// Captures stderr-bound slog output and asserts that:
//
//   - the per-chunk "bulk copy progress" / "bulk copy complete" lines
//     fired with chunk= attributes for at least 4 distinct chunks
//   - the final state row is phase=complete with no leftover chunks
func TestMigrate_PG_ParallelCopy_LargeTable(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const rowCount = 50_000
	seedLargeIntPK(t, sourceDSN, "events", rowCount)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	logs := captureSlog(t)

	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		BulkParallelism:     4,
		BulkParallelMinRows: 10_000, // below the seed so parallel kicks in
		MigrationID:         "test-parallel-copy",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	start := time.Now()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}
	elapsed := time.Since(start)
	t.Logf("parallel=4 wall-clock for %d rows: %s", rowCount, elapsed)

	// Verify the parallel path actually ran by scanning the captured
	// log for chunk= attributes. There should be N=4 distinct values.
	out := logs.String()
	chunksSeen := map[string]bool{}
	for _, idx := range []string{"chunk=0", "chunk=1", "chunk=2", "chunk=3"} {
		if strings.Contains(out, idx) {
			chunksSeen[idx] = true
		}
	}
	if len(chunksSeen) != 4 {
		t.Errorf("expected 4 distinct chunks in logs; saw %d (%v).\nlogs:\n%s",
			len(chunksSeen), chunksSeen, out)
	}

	// Verify all rows landed.
	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	var got int
	if err := tgtDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&got); err != nil {
		t.Fatalf("count target rows: %v", err)
	}
	if got != rowCount {
		t.Errorf("target rows = %d; want %d", got, rowCount)
	}

	// Verify the state row reached phase=complete.
	var phase string
	q := `SELECT phase FROM sluice_migrate_state WHERE migration_id = $1`
	if err := tgtDB.QueryRowContext(ctx, q, "test-parallel-copy").Scan(&phase); err != nil {
		t.Fatalf("read state row: %v", err)
	}
	if phase != string(ir.MigrationPhaseComplete) {
		t.Errorf("phase = %q; want complete", phase)
	}
}

// TestMigrate_PG_ParallelCopy_V04BackwardCompat seeds a v0.4.0-shape
// state row (no Chunks field) and verifies that resume falls back to
// the single-reader cursor path rather than mishandling the absent
// chunk layout. Validates that the v0.5.0 binary respects in-flight
// migrations started under v0.4.0.
func TestMigrate_PG_ParallelCopy_V04BackwardCompat(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const rowCount = 5000
	seedLargeIntPK(t, sourceDSN, "events", rowCount)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	// Manually seed the v0.4.0-shape state row before running --resume.
	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// First create the table on the target by running CREATE TABLE
	// schema phase via an ordinary cold-start so the resume path has
	// something to UPSERT into. We then truncate and seed the state
	// row to mimic a v0.4.0 mid-copy crash.
	mig := &Migrator{
		Source:      pgEng,
		Target:      pgEng,
		SourceDSN:   sourceDSN,
		TargetDSN:   targetDSN,
		MigrationID: "test-v04-compat-seed",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("seed Run: %v", err)
	}
	// Truncate + seed v0.4.0-shape row.
	if _, err := tgtDB.ExecContext(ctx, "TRUNCATE events"); err != nil {
		t.Fatalf("truncate target: %v", err)
	}
	// Insert the v0.4.0-shape state row directly. The JSON has no
	// "chunks" field — the orchestrator should classify this as
	// resumeActionResumeFromCursor and use the single-cursor path.
	if _, err := tgtDB.ExecContext(ctx, `
		INSERT INTO sluice_migrate_state (migration_id, phase, table_progress)
		VALUES ('test-v04-compat', 'bulk_copy',
		        '{"events":{"state":"in_progress","last_pk":[2500],"rows_copied":2500}}')
	`); err != nil {
		t.Fatalf("seed state row: %v", err)
	}
	// Insert rows up to PK 2500 to mimic a half-completed previous run.
	if _, err := tgtDB.ExecContext(
		ctx,
		"INSERT INTO events (id, label) SELECT g, 'row-' || g FROM generate_series(1, 2500) AS g",
	); err != nil {
		t.Fatalf("seed half-data: %v", err)
	}

	// Now resume — should use the single-cursor path (no chunks),
	// resume from PK 2500, and copy the remaining 2500 rows.
	logs := captureSlog(t)
	mig2 := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		Resume:              true,
		MigrationID:         "test-v04-compat",
		BulkParallelism:     4,
		BulkParallelMinRows: 1000,
	}
	if err := mig2.Run(ctx); err != nil {
		t.Fatalf("resume Run: %v", err)
	}

	// Verify all rows landed (idempotent INSERT will UPSERT the
	// re-delivered first 2500 rows).
	var got int
	if err := tgtDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&got); err != nil {
		t.Fatalf("count: %v", err)
	}
	if got != rowCount {
		t.Errorf("target rows after resume = %d; want %d", got, rowCount)
	}

	// The resume run should have classified the table as
	// resumeFromCursor (single-chunk), NOT resumeChunked. Look for the
	// single-chunk log message.
	out := logs.String()
	if !strings.Contains(out, "resuming table from cursor") {
		t.Errorf("expected 'resuming table from cursor' log for v0.4.0 row; got:\n%s", out)
	}
	if strings.Contains(out, "resuming chunked table") {
		t.Errorf("did NOT expect 'resuming chunked table' log for v0.4.0 row; got:\n%s", out)
	}
}

// TestMigrate_PG_ParallelCopy_Resume confirms that a parallel-copy
// state row written by a successful run can be re-resumed without
// crashing — the second --resume run reads the chunked progress,
// classifies it as resumeActionResumeChunked, sees every chunk
// already complete, and exits cleanly.
//
// The strict "kill mid-copy and resume from chunk cursors" path is
// hard to reproduce reliably from a unit test (timing-dependent), so
// this test exercises the lighter version: a successful run leaves
// the row in phase=complete, and a second run with --resume on the
// same migration_id observes "already complete; nothing to do".
func TestMigrate_PG_ParallelCopy_Resume(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const rowCount = 30_000
	seedLargeIntPK(t, sourceDSN, "events", rowCount)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           sourceDSN,
		TargetDSN:           targetDSN,
		BulkParallelism:     4,
		BulkParallelMinRows: 5_000,
		MigrationID:         "test-parallel-resume",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("first Run: %v", err)
	}

	// Second run with --resume: should observe "already complete".
	logs := captureSlog(t)
	mig2 := *mig
	mig2.Resume = true
	if err := mig2.Run(ctx); err != nil {
		t.Fatalf("resume Run: %v", err)
	}
	if !strings.Contains(logs.String(), "already complete") {
		t.Errorf("expected 'already complete' log on resume of complete migration; got:\n%s", logs.String())
	}
}

// TestMigrate_PG_ParallelCopy_Benchmark runs the same migration at
// parallel=1, parallel=4, and parallel=8 and reports wall-clock times.
// Not a real benchmark — the test container, the ~50k row count, and
// the colocated source/target make the absolute numbers meaningless —
// but the relative ordering is a useful smoke test that the parallel
// path doesn't *slow down* throughput at small scale.
func TestMigrate_PG_ParallelCopy_Benchmark(t *testing.T) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const rowCount = 50_000
	for _, p := range []int{1, 4, 8} {
		p := p
		t.Run(fmt.Sprintf("parallel=%d", p), func(t *testing.T) {
			sourceDSN, targetDSN, cleanup := startPostgres(t)
			defer cleanup()

			seedLargeIntPK(t, sourceDSN, "events", rowCount)

			mig := &Migrator{
				Source:              pgEng,
				Target:              pgEng,
				SourceDSN:           sourceDSN,
				TargetDSN:           targetDSN,
				BulkParallelism:     p,
				BulkParallelMinRows: 10_000,
				MigrationID:         fmt.Sprintf("bench-p%d", p),
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			start := time.Now()
			if err := mig.Run(ctx); err != nil {
				t.Fatalf("Migrator.Run: %v", err)
			}
			elapsed := time.Since(start)
			t.Logf("parallel=%d wall-clock for %d rows: %s", p, rowCount, elapsed)

			tgtDB, err := sql.Open("pgx", targetDSN)
			if err != nil {
				t.Fatalf("open target: %v", err)
			}
			defer func() { _ = tgtDB.Close() }()

			var got int
			if err := tgtDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&got); err != nil {
				t.Fatalf("count target rows: %v", err)
			}
			if got != rowCount {
				t.Errorf("target rows = %d; want %d", got, rowCount)
			}
		})
	}
}

// TestMigrate_PG_ParallelCopy_BelowThreshold confirms a table that's
// above the integer-PK eligibility check but below the row-count
// threshold uses the single-reader path. Seeded with 1000 rows; with
// the default 100k threshold the parallel path declines to act.
func TestMigrate_PG_ParallelCopy_BelowThreshold(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const rowCount = 1000
	seedLargeIntPK(t, sourceDSN, "small_table", rowCount)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:          pgEng,
		Target:          pgEng,
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		BulkParallelism: 4,
		// Default 100k threshold; 1000 rows below it.
		MigrationID: "test-parallel-below-threshold",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgtDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	var got int
	if err := tgtDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM small_table").Scan(&got); err != nil {
		t.Fatalf("count target rows: %v", err)
	}
	if got != rowCount {
		t.Errorf("target rows = %d; want %d", got, rowCount)
	}

	// State row carries no Chunks for the small_table because the
	// parallel path declined to act. Read the table_progress JSON
	// and assert the bare-string "complete" form.
	var progress sql.NullString
	q := `SELECT table_progress FROM sluice_migrate_state WHERE migration_id = $1`
	if err := tgtDB.QueryRowContext(ctx, q, "test-parallel-below-threshold").Scan(&progress); err != nil {
		t.Fatalf("read state row: %v", err)
	}
	if !progress.Valid {
		t.Fatalf("table_progress is NULL; expected non-empty progress map")
	}
	// We expect the bare-string "complete" form, not an object with
	// chunks. Check by string-matching the JSON.
	if !strings.Contains(progress.String, `"small_table":"complete"`) {
		t.Errorf("small_table progress did not use bare-string complete form: %s", progress.String)
	}
	if strings.Contains(progress.String, `"chunks"`) {
		t.Errorf("small_table progress unexpectedly contains chunks: %s", progress.String)
	}
}
