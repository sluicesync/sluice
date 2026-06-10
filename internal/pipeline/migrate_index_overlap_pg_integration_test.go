//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for overlapped index builds (ADR-0077, roadmap item
// 3b(a)). Before this chunk, migrate ran a full cross-table copy phase and
// THEN a separate whole-schema index phase; now PG builds each table's
// secondary indexes as its copy lands, concurrently with the still-copying
// tables. These boot a real Postgres container and assert:
//
//   - overlap-actually-happens: min(indexBuildStart) < max(copyComplete),
//     measured via the test-only observability seams — without this the
//     chunk could silently regress to sequential and still pass zero-loss;
//   - zero-loss aggregate-checksum + every secondary index present on the
//     target (pg_indexes);
//   - resume-under-overlap: kill mid-run, resume, no duplicate-index error,
//     all indexes present, zero loss, and IndexesBuilt short-circuits the
//     already-indexed tables (the per-table index-build counter proves the
//     short-circuit);
//   - combined budget on a small max_connections target completes without
//     "remaining connection slots" exhaustion.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// seedManyIndexedTables creates tableCount tables, each with rowsEach rows
// (contiguous PK) and TWO secondary indexes (so the index phase has real
// work to overlap). The v column is PK-derived so a SUM checksum catches
// loss/dup; the b column gives a second indexable column.
func seedManyIndexedTables(t *testing.T, dsn string, tableCount, rowsEach int) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
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
				v  BIGINT NOT NULL,
				b  BIGINT NOT NULL
			);
			INSERT INTO %s (id, v, b)
				SELECT g, g * 7, g %% 100 FROM generate_series(1, %d) AS g;
			CREATE INDEX %s_v_idx ON %s (v);
			CREATE INDEX %s_b_idx ON %s (b);
			ANALYZE %s;
		`, name, name, rowsEach, name, name, name, name, name)
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
}

// assertAllSecondaryIndexesPresent confirms every seeded table's two
// secondary indexes exist on the target (pg_indexes).
func assertAllSecondaryIndexesPresent(ctx context.Context, t *testing.T, dsn string, tableCount int) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	for i := 0; i < tableCount; i++ {
		name := fmt.Sprintf("tbl_%02d", i)
		for _, idx := range []string{name + "_v_idx", name + "_b_idx"} {
			var got int
			if err := db.QueryRowContext(
				ctx,
				`SELECT COUNT(*) FROM pg_indexes WHERE tablename=$1 AND indexname=$2`,
				name, idx,
			).Scan(&got); err != nil {
				t.Fatalf("query pg_indexes %s: %v", idx, err)
			}
			if got != 1 {
				t.Errorf("index %s missing on target (got %d rows in pg_indexes)", idx, got)
			}
		}
	}
}

// TestMigrate_PG_IndexOverlap_ActuallyHappens is the load-bearing pin: it
// records when each table's index build STARTS and when each copy
// COMPLETES via the test-only seams, then asserts min(indexBuildStart) <
// max(copyComplete) — proving the index phase genuinely overlapped the
// copy rather than running after it. Also asserts zero-loss + all indexes.
func TestMigrate_PG_IndexOverlap_ActuallyHappens(t *testing.T) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const (
		tableCount = 16
		rowsEach   = 60_000
	)
	src, tgt, cleanup := startPostgres(t)
	defer cleanup()
	seedManyIndexedTables(t, src, tableCount, rowsEach)

	// Observability: record the FIRST index-build start and the LAST copy
	// completion. If the index phase overlapped the copy, the first index
	// build started before the last copy finished.
	var obsMu sync.Mutex
	var firstIndexStart, lastCopyComplete time.Time

	restoreCopy := setOnTableCopiedObserverForTest(func(_ string) {
		obsMu.Lock()
		lastCopyComplete = time.Now()
		obsMu.Unlock()
	})
	defer restoreCopy()
	restoreIdx := postgres.SetIndexBuildStartObserverForTest(func(_ string) {
		obsMu.Lock()
		if firstIndexStart.IsZero() {
			firstIndexStart = time.Now()
		}
		obsMu.Unlock()
	})
	defer restoreIdx()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	mig := &Migrator{
		Source:           pgEng,
		Target:           pgEng,
		SourceDSN:        src,
		TargetDSN:        tgt,
		TableParallelism: 4,
		// Keep tables single-streamed so the cross-table + index axes are
		// what is exercised, not within-table chunking.
		BulkParallelMinRows: int64(rowsEach * 10),
		MigrationID:         "idx-overlap",
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertManyIndexedTablesZeroLoss(ctx, t, src, tgt, tableCount)
	assertAllSecondaryIndexesPresent(ctx, t, tgt, tableCount)

	obsMu.Lock()
	fis, lcc := firstIndexStart, lastCopyComplete
	obsMu.Unlock()
	if fis.IsZero() {
		t.Fatal("no index build ever started (observability seam never fired)")
	}
	if lcc.IsZero() {
		t.Fatal("no copy ever completed (observability seam never fired)")
	}
	t.Logf("overlap: firstIndexStart=%s lastCopyComplete=%s (delta=%s)",
		fis.Format(time.StampMilli), lcc.Format(time.StampMilli), lcc.Sub(fis))
	if !fis.Before(lcc) {
		t.Errorf("index builds did NOT overlap the copy: firstIndexStart=%s >= lastCopyComplete=%s "+
			"(the chunk regressed to a sequential post-copy index phase)", fis, lcc)
	}
}

// assertManyIndexedTablesZeroLoss compares source/target checksums
// (count + sum(id) + sum(v) + sum(b)) for every seeded table.
func assertManyIndexedTablesZeroLoss(ctx context.Context, t *testing.T, srcDSN, tgtDSN string, tableCount int) {
	t.Helper()
	srcDB, err := sql.Open("pgx", srcDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	tgtDB, err := sql.Open("pgx", tgtDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	checksum := func(db *sql.DB, name string) (c, sid, sv, sb int64) {
		q := fmt.Sprintf("SELECT COUNT(*), COALESCE(SUM(id),0), COALESCE(SUM(v),0), COALESCE(SUM(b),0) FROM %s", name)
		if err := db.QueryRowContext(ctx, q).Scan(&c, &sid, &sv, &sb); err != nil {
			t.Fatalf("checksum %s: %v", name, err)
		}
		return
	}
	for i := 0; i < tableCount; i++ {
		name := fmt.Sprintf("tbl_%02d", i)
		sc, si, sv, sb := checksum(srcDB, name)
		tc, ti, tv, tb := checksum(tgtDB, name)
		if sc != tc || si != ti || sv != tv || sb != tb {
			t.Errorf("table %s checksum mismatch: src(%d,%d,%d,%d) tgt(%d,%d,%d,%d)",
				name, sc, si, sv, sb, tc, ti, tv, tb)
		}
	}
}

// setOnTableCopiedObserverForTest installs the pipeline-side copy-complete
// seam (same package) and returns a restore func.
func setOnTableCopiedObserverForTest(fn func(tableName string)) func() {
	prev := onTableCopiedObserver
	onTableCopiedObserver = fn
	return func() { onTableCopiedObserver = prev }
}

// TestMigrate_PG_IndexOverlap_ResumeShortCircuitsIndexed kills a many-table
// overlapped migrate mid-flight (some tables copied+indexed, some
// copied-not-indexed, some untouched), resumes, and asserts: no
// duplicate-index error, every index present, zero loss, AND the resume
// did NOT re-build indexes for tables already marked IndexesBuilt (proven
// by the per-table index-build-start counter — it must be strictly less
// than the full tableCount×2 on resume, since fully-indexed tables are
// short-circuited).
func TestMigrate_PG_IndexOverlap_ResumeShortCircuitsIndexed(t *testing.T) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const (
		tableCount = 18
		rowsEach   = 40_000
	)
	src, tgt, cleanup := startPostgres(t)
	defer cleanup()
	seedManyIndexedTables(t, src, tableCount, rowsEach)

	mig := &Migrator{
		Source:              pgEng,
		Target:              pgEng,
		SourceDSN:           src,
		TargetDSN:           tgt,
		TableParallelism:    5,
		BulkParallelMinRows: int64(rowsEach * 10),
		BulkBatchSize:       2000,
		MigrationID:         "idx-overlap-resume",
	}

	// First attempt: cancel partway so some tables are fully indexed, some
	// copied-not-indexed, some untouched. The window must be long enough
	// that at least one table reaches fully-indexed (so its IndexesBuilt is
	// persisted) yet short enough that the run doesn't finish — sized
	// against the ~5 s full-run time for this shape. We probe progress and
	// cancel once at least one table is marked IndexesBuilt.
	progressCtx, progressCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer progressCancel()
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = mig.Run(progressCtx) // expected cancellation; ignore.
	}()
	waitForSomeIndexedThenCancel(t, tgt, "idx-overlap-resume", progressCancel)
	<-runDone

	// Resume to completion, counting how many index builds START.
	var startCount int
	var startMu sync.Mutex
	restore := postgres.SetIndexBuildStartObserverForTest(func(_ string) {
		startMu.Lock()
		startCount++
		startMu.Unlock()
	})
	defer restore()

	resumeCtx, resumeCancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer resumeCancel()
	resumeMig := *mig
	resumeMig.Resume = true
	if err := resumeMig.Run(resumeCtx); err != nil {
		t.Fatalf("resume Run: %v", err)
	}

	assertManyIndexedTablesZeroLoss(resumeCtx, t, src, tgt, tableCount)
	assertAllSecondaryIndexesPresent(resumeCtx, t, tgt, tableCount)

	startMu.Lock()
	got := startCount
	startMu.Unlock()
	// The full build set is tableCount*2 index starts. The first attempt
	// got at least one table fully indexed (its IndexesBuilt was persisted),
	// so the resume must START strictly fewer than the full set — proving
	// the IndexesBuilt short-circuit skipped at least one already-indexed
	// table. (CREATE INDEX IF NOT EXISTS would make a re-build a no-op
	// anyway, but the point is it isn't even attempted.)
	full := tableCount * 2
	t.Logf("resume index-build starts: %d (full set would be %d)", got, full)
	if got >= full {
		t.Errorf("resume re-started all %d index builds; IndexesBuilt short-circuit did not skip any already-indexed table (got %d starts)", full, got)
	}
}

// waitForSomeIndexedThenCancel polls the target's migrate-state row until
// at least one table has its IndexesBuilt flag PERSISTED (the authoritative
// signal that the resume will short-circuit it — polling pg_indexes alone
// would race the callback's state write), then cancels. Times out the test
// if no table ever reaches that state (a regression where indexes never
// build during the copy, or the flag is never persisted).
func waitForSomeIndexedThenCancel(t *testing.T, dsn, migrationID string, cancel context.CancelFunc) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open target for progress poll: %v", err)
	}
	defer func() { _ = db.Close() }()

	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		// A fully indexed table's progress JSON carries
		// "indexes_built":true; count its per-table rows (ADR-0082:
		// progress lives in sluice_migrate_table_progress, one row per
		// table). The control table may not exist yet on the very first
		// polls — ignore the error and retry.
		err := db.QueryRow(
			`SELECT COUNT(*) FROM sluice_migrate_table_progress
			   WHERE migration_id = $1 AND progress LIKE '%"indexes_built":true%'`,
			migrationID,
		).Scan(&n)
		if err == nil && n >= 1 {
			cancel()
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	cancel()
	t.Fatal("no table reached persisted IndexesBuilt within the poll window; cannot exercise the resume short-circuit")
}

// TestMigrate_PG_IndexOverlap_CombinedBudget runs an overlapped many-table
// migrate against a target booted with a small max_connections, both axes
// requested high, and asserts it completes zero-loss with all indexes —
// i.e. the copy + index connection budgets, held SIMULTANEOUSLY, were
// split at the single chokepoint rather than exhausting the slots.
func TestMigrate_PG_IndexOverlap_CombinedBudget(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	const maxConns = 40
	container, err := pgtc.Run(
		ctx,
		pgPrebakedImage,
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		pgPrebakedWaitStrategy(),
		testcontainers.WithCmd("postgres", "-c", fmt.Sprintf("max_connections=%d", maxConns)),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}
	defer func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}()

	srcConn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	srcDB, err := sql.Open("pgx", srcConn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := srcDB.ExecContext(ctx, "CREATE DATABASE target_db"); err != nil {
		_ = srcDB.Close()
		t.Fatalf("create target_db: %v", err)
	}
	_ = srcDB.Close()
	tgtConn, err := buildPGDSN(srcConn, "target_db")
	if err != nil {
		t.Fatalf("build target DSN: %v", err)
	}

	const (
		tableCount = 12
		rowsEach   = 20_000
	)
	seedManyIndexedTables(t, srcConn, tableCount, rowsEach)

	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: srcConn,
		TargetDSN: tgtConn,
		// Request both axes high; cap the PRODUCT + index reserve via
		// --max-target-connections so copy AND index connections held at
		// once stay within the budget (source shares this instance).
		TableParallelism:     8,
		BulkParallelism:      2,
		MaxTargetConnections: 10,
		BulkParallelMinRows:  10_000,
		MigrationID:          "idx-overlap-budget",
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Run against small max_connections target: %v", err)
	}

	assertManyIndexedTablesZeroLoss(ctx, t, srcConn, tgtConn, tableCount)
	assertAllSecondaryIndexesPresent(ctx, t, tgtConn, tableCount)

	tgtDB, err := sql.Open("pgx", tgtConn)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()
	var phase string
	if err := tgtDB.QueryRowContext(
		ctx,
		`SELECT phase FROM sluice_migrate_state WHERE migration_id = $1`, "idx-overlap-budget",
	).Scan(&phase); err != nil {
		t.Fatalf("read state row: %v", err)
	}
	if phase != string(ir.MigrationPhaseComplete) {
		t.Errorf("phase = %q; want complete", phase)
	}
}
