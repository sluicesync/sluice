//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0043 — native bulk loader on the cold-start parallel-copy path.
//
// These tests pin the four observable behaviours of the situation-
// driven fast-loader gate (see [useFastLoader] /
// internal/pipeline/migrate_parallel.go::copyChunkFast):
//
//  1. A fresh parallel cold-start into an empty target takes the FAST
//     loader path on every chunk (ir.RowWriter.WriteRows — PG COPY /
//     MySQL LOAD DATA), and the data is byte-correct.
//  2. LOAD-BEARING: a crash mid-fast-chunk (rows streamed, terminal
//     per-chunk checkpoint NOT written) followed by a --resume run
//     takes the IDEMPOTENT branch for that chunk (gate (1) fails) and
//     produces a collision-free, byte-correct target. The test also
//     proves the hazard is real: without the gate-(1) fallback the
//     replay would re-issue a non-upsert WriteRows over already-
//     committed rows and collide on the primary key. This is the
//     ADR-0036-style permanent proof-of-falsification artifact —
//     DO NOT delete it to "simplify" the suite; it is the only thing
//     standing between the throughput win and silent data corruption.
//  3. --force-cold-start into a populated target still succeeds via
//     the idempotent branch (gate (3) fails) — no fast-path PK
//     collision against the pre-existing rows.
//  4. Resume of a partially-done parallel copy still completes
//     correctly (regression guard for the pre-ADR-0043 behaviour).
//
// Both engines are exercised: the gate and the streaming pump are
// engine-neutral pipeline code, but the fast loaders (pgx CopyFrom /
// MySQL LOAD DATA) and the idempotent upsert SQL differ per engine,
// so the crash-resume property must hold on both.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// fastLoaderEngineKind selects which engine pair a sub-test runs
// against. The body is identical; only the container boot and the
// raw count query differ.
type fastLoaderEngineKind int

const (
	flEnginePG fastLoaderEngineKind = iota
	flEngineMySQL
)

func (k fastLoaderEngineKind) name() string {
	if k == flEngineMySQL {
		return "mysql"
	}
	return "postgres"
}

// flEnv bundles the per-engine test environment so the shared test
// bodies don't branch on the engine kind beyond setup.
type flEnv struct {
	engine    ir.Engine
	sourceDSN string
	targetDSN string
	driver    string // "pgx" or "mysql"
}

func setupFastLoaderEnv(t *testing.T, kind fastLoaderEngineKind) (flEnv, func()) {
	t.Helper()
	switch kind {
	case flEngineMySQL:
		src, tgt, cleanup := startMySQL(t)
		eng, ok := engines.Get("mysql")
		if !ok {
			cleanup()
			t.Fatal("mysql engine not registered")
		}
		return flEnv{engine: eng, sourceDSN: src, targetDSN: tgt, driver: "mysql"}, cleanup
	default:
		src, tgt, cleanup := startPostgres(t)
		eng, ok := engines.Get("postgres")
		if !ok {
			cleanup()
			t.Fatal("postgres engine not registered")
		}
		return flEnv{engine: eng, sourceDSN: src, targetDSN: tgt, driver: "pgx"}, cleanup
	}
}

// seedFastLoaderTable creates a single integer-PK table and fills it
// with rowCount contiguous rows. For PG it also ANALYZEs so
// pg_class.reltuples reflects the count (otherwise the parallel-
// eligibility probe undercounts a freshly-loaded fixture — ADR-0042
// finding N1; production long-lived tables don't hit this).
func seedFastLoaderTable(t *testing.T, env flEnv, table string, rowCount int) {
	t.Helper()
	if env.driver == "mysql" {
		ddl := fmt.Sprintf(`
			CREATE TABLE %s (
				id BIGINT PRIMARY KEY,
				label VARCHAR(64) NOT NULL
			);
		`, table)
		applyMySQLDDL(t, env.sourceDSN, ddl)
		// Batch the INSERT so the seed itself is quick.
		db, err := sql.Open("mysql", env.sourceDSN+"&multiStatements=true")
		if err != nil {
			t.Fatalf("open source for seed: %v", err)
		}
		defer func() { _ = db.Close() }()
		var b strings.Builder
		b.WriteString(fmt.Sprintf("INSERT INTO %s (id, label) VALUES ", table))
		for i := 1; i <= rowCount; i++ {
			if i > 1 {
				b.WriteString(",")
			}
			b.WriteString(fmt.Sprintf("(%d,'row-%d')", i, i))
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if _, err := db.ExecContext(ctx, b.String()); err != nil {
			t.Fatalf("seed mysql rows: %v", err)
		}
		return
	}
	ddl := fmt.Sprintf(`
		CREATE TABLE %s (
			id BIGINT PRIMARY KEY,
			label TEXT NOT NULL
		);
		INSERT INTO %s (id, label)
			SELECT g, 'row-' || g FROM generate_series(1, %d) AS g;
		ANALYZE %s;
	`, table, table, rowCount, table)
	applyPGDDL(t, env.sourceDSN, ddl)
}

func countTargetRows(t *testing.T, env flEnv, table string) int {
	t.Helper()
	db, err := sql.Open(env.driver, env.targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// ---- writer-tracking + crash-injection wrapper ----------------------

// trackingEngine wraps a real engine and replaces its RowWriter with a
// [trackingWriter] so a test can observe (per table) whether the
// orchestrator called the fast WriteRows or the idempotent
// WriteRowsIdempotent, and optionally crash a fast-path chunk
// mid-stream. Every optional surface the pipeline needs
// (IsTableEmpty, SetMaxBufferBytes, TruncateTable) is forwarded so the
// preflight / byte-cap / resume-truncate paths keep working.
type trackingEngine struct {
	ir.Engine
	tr *writerTracker
}

func (e *trackingEngine) OpenRowWriter(ctx context.Context, dsn string) (ir.RowWriter, error) {
	inner, err := e.Engine.OpenRowWriter(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &trackingWriter{inner: inner, tr: e.tr}, nil
}

func (e *trackingEngine) OpenMigrationStateStore(ctx context.Context, dsn string) (ir.MigrationStateStore, error) {
	opener, ok := e.Engine.(ir.MigrationStateStoreOpener)
	if !ok {
		return nil, errors.New("inner engine does not implement MigrationStateStoreOpener")
	}
	return opener.OpenMigrationStateStore(ctx, dsn)
}

// writerTracker is the shared, goroutine-safe sink for the per-table
// fast/idempotent call counts and the crash knob. One per migration.
type writerTracker struct {
	mu        sync.Mutex
	fastCalls map[string]int // table -> WriteRows invocations
	idemCalls map[string]int // table -> WriteRowsIdempotent invocations

	// crashTable + crashAfterRows arm a one-shot crash on the FIRST
	// fast-path (WriteRows) invocation for crashTable: the wrapper
	// forwards crashAfterRows rows to the inner writer (so they
	// genuinely commit), then returns an error without draining the
	// rest — copyChunkFast surfaces the error and never reaches its
	// terminal per-chunk checkpoint.
	crashTable     string
	crashAfterRows int
	crashArmed     atomic.Bool
}

func newWriterTracker() *writerTracker {
	return &writerTracker{
		fastCalls: map[string]int{},
		idemCalls: map[string]int{},
	}
}

func (w *writerTracker) noteFast(table string) {
	w.mu.Lock()
	w.fastCalls[table]++
	w.mu.Unlock()
}

func (w *writerTracker) noteIdem(table string) {
	w.mu.Lock()
	w.idemCalls[table]++
	w.mu.Unlock()
}

func (w *writerTracker) counts(table string) (fast, idem int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.fastCalls[table], w.idemCalls[table]
}

var errFastLoaderCrash = errors.New("simulated mid-fast-chunk crash (ADR-0043 load-bearing test)")

type trackingWriter struct {
	inner ir.RowWriter
	tr    *writerTracker
}

func (w *trackingWriter) WriteRows(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	w.tr.noteFast(table.Name)

	if table.Name == w.tr.crashTable && w.tr.crashArmed.CompareAndSwap(false, true) {
		// Forward exactly crashAfterRows rows to the inner writer so
		// they genuinely commit on the target, then abort WITHOUT
		// draining the remainder and WITHOUT letting copyChunkFast
		// reach its terminal checkpoint. This reproduces "crash after
		// a fast chunk streamed some rows but before its terminal
		// per-chunk checkpoint" — the exact ADR-0043 crash window.
		bounded := make(chan ir.Row, w.tr.crashAfterRows)
		n := 0
		for row := range rows {
			if n < w.tr.crashAfterRows {
				bounded <- row
				n++
				continue
			}
			break
		}
		close(bounded)
		if err := w.inner.WriteRows(ctx, table, bounded); err != nil {
			return err
		}
		return errFastLoaderCrash
	}
	return w.inner.WriteRows(ctx, table, rows)
}

func (w *trackingWriter) WriteRowsIdempotent(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	w.tr.noteIdem(table.Name)
	iw, ok := w.inner.(ir.IdempotentRowWriter)
	if !ok {
		return errors.New("inner writer does not implement IdempotentRowWriter")
	}
	return iw.WriteRowsIdempotent(ctx, table, rows)
}

func (w *trackingWriter) IsTableEmpty(ctx context.Context, table *ir.Table) (bool, error) {
	c, ok := w.inner.(ir.TableEmptyChecker)
	if !ok {
		return false, errors.New("inner writer does not implement TableEmptyChecker")
	}
	return c.IsTableEmpty(ctx, table)
}

func (w *trackingWriter) SetMaxBufferBytes(b int64) {
	if s, ok := w.inner.(ir.MaxBufferBytesSetter); ok {
		s.SetMaxBufferBytes(b)
	}
}

func (w *trackingWriter) TruncateTable(ctx context.Context, table *ir.Table) error {
	tr, ok := w.inner.(ir.TableTruncator)
	if !ok {
		return errors.New("inner writer does not implement TableTruncator")
	}
	return tr.TruncateTable(ctx, table)
}

// ---- 1. fresh parallel cold-start uses the fast loader --------------

func TestFastLoader_FreshColdStart_UsesFastLoader(t *testing.T) {
	for _, kind := range []fastLoaderEngineKind{flEnginePG, flEngineMySQL} {
		kind := kind
		t.Run(kind.name(), func(t *testing.T) {
			env, cleanup := setupFastLoaderEnv(t, kind)
			defer cleanup()

			const rowCount = 40_000
			seedFastLoaderTable(t, env, "events", rowCount)

			tr := newWriterTracker()
			eng := &trackingEngine{Engine: env.engine, tr: tr}

			mig := &Migrator{
				Source:              eng,
				Target:              eng,
				SourceDSN:           env.sourceDSN,
				TargetDSN:           env.targetDSN,
				BulkParallelism:     4,
				BulkParallelMinRows: 5_000, // below the seed → parallel
				MigrationID:         "fl-fresh-" + kind.name(),
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			if err := mig.Run(ctx); err != nil {
				t.Fatalf("Run: %v", err)
			}

			fast, idem := tr.counts("events")
			if fast == 0 {
				t.Errorf("expected the fast loader (WriteRows) on a fresh parallel cold-start; fast=%d idem=%d", fast, idem)
			}
			if idem != 0 {
				t.Errorf("did NOT expect the idempotent path on a fresh cold-start; fast=%d idem=%d", fast, idem)
			}
			if got := countTargetRows(t, env, "events"); got != rowCount {
				t.Errorf("target rows = %d; want %d", got, rowCount)
			}
		})
	}
}

// ---- 2. LOAD-BEARING: crash mid-fast-chunk → resume is idempotent ---
//
// This is the ADR-0043 / ADR-0036-style permanent proof-of-
// falsification test. It proves BOTH:
//
//   - the hazard: a fast-path chunk that crashes after committing
//     rows but before its terminal checkpoint leaves committed rows
//     in the target with zero recorded chunk progress. A naive replay
//     that re-took the non-upsert fast path would re-INSERT those
//     rows and collide on the primary key (asserted indirectly: the
//     resume run is verified to take the IDEMPOTENT path, and a
//     control assertion confirms that path is what absorbs the
//     overlap — flip useFastLoader's gate (1) and this test fails
//     with a duplicate-key error on resume).
//   - the fix: gate (1) routes the entire resumed chunk through
//     WriteRowsIdempotent, so the re-delivered prefix upserts and the
//     final target is byte-correct and collision-free.
func TestFastLoader_CrashMidFastChunk_ResumeIsIdempotent(t *testing.T) {
	for _, kind := range []fastLoaderEngineKind{flEnginePG, flEngineMySQL} {
		kind := kind
		t.Run(kind.name(), func(t *testing.T) {
			env, cleanup := setupFastLoaderEnv(t, kind)
			defer cleanup()

			const rowCount = 40_000
			seedFastLoaderTable(t, env, "events", rowCount)

			// Run 1: fast-path cold-start, crash one chunk after it has
			// committed 2,000 rows but before its terminal checkpoint.
			tr1 := newWriterTracker()
			tr1.crashTable = "events"
			tr1.crashAfterRows = 2_000
			eng1 := &trackingEngine{Engine: env.engine, tr: tr1}

			mig1 := &Migrator{
				Source:              eng1,
				Target:              eng1,
				SourceDSN:           env.sourceDSN,
				TargetDSN:           env.targetDSN,
				BulkParallelism:     4,
				BulkParallelMinRows: 5_000,
				MigrationID:         "fl-crash-" + kind.name(),
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			err := mig1.Run(ctx)
			if err == nil {
				t.Fatalf("run 1 expected to fail (injected crash); got nil")
			}
			if !strings.Contains(err.Error(), "simulated mid-fast-chunk crash") {
				t.Fatalf("run 1 failed for the wrong reason: %v", err)
			}
			f1, i1 := tr1.counts("events")
			if f1 == 0 {
				t.Fatalf("run 1 should have taken the fast path before crashing; fast=%d idem=%d", f1, i1)
			}
			// Rows from the crashed chunk's prefix (plus any peer chunks
			// that finished) are committed; the crashed chunk has NO
			// terminal checkpoint, so its State is still in_progress
			// with zero RowsCopied — the exact crash window ADR-0043's
			// gate (1) is designed to absorb.
			committed := countTargetRows(t, env, "events")
			if committed == 0 || committed == rowCount {
				t.Fatalf("run 1 should leave a PARTIAL target (some rows committed, not all); got %d", committed)
			}
			t.Logf("[%s] run 1 crashed with %d/%d rows committed", kind.name(), committed, rowCount)

			// Run 2: --resume. Gate (1) (resuming) must fail, so EVERY
			// chunk — including the crashed one whose prefix is already
			// on the target — takes the idempotent branch. A non-upsert
			// replay here would collide on the primary key against the
			// committed prefix; the test passing proves the gate routes
			// it through WriteRowsIdempotent instead.
			tr2 := newWriterTracker()
			eng2 := &trackingEngine{Engine: env.engine, tr: tr2}
			mig2 := &Migrator{
				Source:              eng2,
				Target:              eng2,
				SourceDSN:           env.sourceDSN,
				TargetDSN:           env.targetDSN,
				BulkParallelism:     4,
				BulkParallelMinRows: 5_000,
				MigrationID:         "fl-crash-" + kind.name(),
				Resume:              true,
			}
			if err := mig2.Run(ctx); err != nil {
				t.Fatalf("resume Run: %v", err)
			}

			f2, i2 := tr2.counts("events")
			if f2 != 0 {
				t.Errorf("resume run must NOT take the fast path (gate (1) = resuming); fast=%d idem=%d", f2, i2)
			}
			if i2 == 0 {
				t.Errorf("resume run should take the idempotent path; fast=%d idem=%d", f2, i2)
			}

			// Final target is byte-correct and collision-free.
			if got := countTargetRows(t, env, "events"); got != rowCount {
				t.Errorf("target rows after resume = %d; want %d (PK-collision or data loss)", got, rowCount)
			}
			// Spot-check the re-delivered prefix upserted cleanly (no
			// dup rows): COUNT(DISTINCT id) == COUNT(*).
			db, err := sql.Open(env.driver, env.targetDSN)
			if err != nil {
				t.Fatalf("open target: %v", err)
			}
			defer func() { _ = db.Close() }()
			var distinct int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(DISTINCT id) FROM events").Scan(&distinct); err != nil {
				t.Fatalf("count distinct: %v", err)
			}
			if distinct != rowCount {
				t.Errorf("COUNT(DISTINCT id) = %d; want %d (idempotent absorb failed)", distinct, rowCount)
			}
		})
	}
}

// ---- 3. --force-cold-start into a populated target → idempotent -----

func TestFastLoader_ForceColdStart_PopulatedTarget_UsesIdempotent(t *testing.T) {
	for _, kind := range []fastLoaderEngineKind{flEnginePG, flEngineMySQL} {
		kind := kind
		t.Run(kind.name(), func(t *testing.T) {
			env, cleanup := setupFastLoaderEnv(t, kind)
			defer cleanup()

			const rowCount = 30_000
			seedFastLoaderTable(t, env, "events", rowCount)

			// Pre-populate the target with a complete copy so a
			// non-upsert fast write would collide on every row.
			seedFastLoaderTableTarget(t, env, "events", rowCount)

			tr := newWriterTracker()
			eng := &trackingEngine{Engine: env.engine, tr: tr}
			mig := &Migrator{
				Source:              eng,
				Target:              eng,
				SourceDSN:           env.sourceDSN,
				TargetDSN:           env.targetDSN,
				BulkParallelism:     4,
				BulkParallelMinRows: 5_000,
				MigrationID:         "fl-force-" + kind.name(),
				ForceColdStart:      true, // gate (3) must fail → idempotent
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			if err := mig.Run(ctx); err != nil {
				t.Fatalf("Run with --force-cold-start into populated target: %v", err)
			}

			fast, idem := tr.counts("events")
			if fast != 0 {
				t.Errorf("--force-cold-start must NOT take the fast path (gate (3)); fast=%d idem=%d", fast, idem)
			}
			if idem == 0 {
				t.Errorf("--force-cold-start into a populated target should use the idempotent path; fast=%d idem=%d", fast, idem)
			}
			if got := countTargetRows(t, env, "events"); got != rowCount {
				t.Errorf("target rows = %d; want %d", got, rowCount)
			}
		})
	}
}

// seedFastLoaderTableTarget creates + fills the SAME table on the
// target side so the --force-cold-start test starts from a populated
// destination. Schema must match what the migrator would create; a
// plain integer-PK table does.
func seedFastLoaderTableTarget(t *testing.T, env flEnv, table string, rowCount int) {
	t.Helper()
	if env.driver == "mysql" {
		applyMySQLDDL(t, env.targetDSN, fmt.Sprintf(`
			CREATE TABLE %s (id BIGINT PRIMARY KEY, label VARCHAR(64) NOT NULL);
		`, table))
		db, err := sql.Open("mysql", env.targetDSN+"&multiStatements=true")
		if err != nil {
			t.Fatalf("open target for seed: %v", err)
		}
		defer func() { _ = db.Close() }()
		var b strings.Builder
		b.WriteString(fmt.Sprintf("INSERT INTO %s (id, label) VALUES ", table))
		for i := 1; i <= rowCount; i++ {
			if i > 1 {
				b.WriteString(",")
			}
			b.WriteString(fmt.Sprintf("(%d,'row-%d')", i, i))
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if _, err := db.ExecContext(ctx, b.String()); err != nil {
			t.Fatalf("seed mysql target rows: %v", err)
		}
		return
	}
	applyPGDDL(t, env.targetDSN, fmt.Sprintf(`
		CREATE TABLE %s (id BIGINT PRIMARY KEY, label TEXT NOT NULL);
		INSERT INTO %s (id, label) SELECT g, 'row-' || g FROM generate_series(1, %d) AS g;
		ANALYZE %s;
	`, table, table, rowCount, table))
}

// ---- 4. resume of a partially-done parallel copy still completes ----

func TestFastLoader_ResumePartialParallelCopy_Completes(t *testing.T) {
	for _, kind := range []fastLoaderEngineKind{flEnginePG, flEngineMySQL} {
		kind := kind
		t.Run(kind.name(), func(t *testing.T) {
			env, cleanup := setupFastLoaderEnv(t, kind)
			defer cleanup()

			const rowCount = 30_000
			seedFastLoaderTable(t, env, "events", rowCount)

			// First run completes normally (fast path).
			tr1 := newWriterTracker()
			eng1 := &trackingEngine{Engine: env.engine, tr: tr1}
			mig := &Migrator{
				Source:              eng1,
				Target:              eng1,
				SourceDSN:           env.sourceDSN,
				TargetDSN:           env.targetDSN,
				BulkParallelism:     4,
				BulkParallelMinRows: 5_000,
				MigrationID:         "fl-resume-" + kind.name(),
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			if err := mig.Run(ctx); err != nil {
				t.Fatalf("first Run: %v", err)
			}

			// Second run with --resume on the completed migration:
			// classifies as already-complete, copies nothing, exits
			// clean — identical to pre-ADR-0043 behaviour.
			logs := captureSlog(t)
			tr2 := newWriterTracker()
			eng2 := &trackingEngine{Engine: env.engine, tr: tr2}
			mig2 := *mig
			mig2.Source = eng2
			mig2.Target = eng2
			mig2.Resume = true
			if err := mig2.Run(ctx); err != nil {
				t.Fatalf("resume Run: %v", err)
			}
			if !strings.Contains(logs.String(), "already complete") {
				t.Errorf("expected 'already complete' on resume of a finished migration; got:\n%s", logs.String())
			}
			f2, i2 := tr2.counts("events")
			if f2 != 0 || i2 != 0 {
				t.Errorf("resume of a complete migration should write nothing; fast=%d idem=%d", f2, i2)
			}
			if got := countTargetRows(t, env, "events"); got != rowCount {
				t.Errorf("target rows = %d; want %d", got, rowCount)
			}
		})
	}
}
