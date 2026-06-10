// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Overlapped index builds for MySQL targets (ADR-0080, roadmap item 3c).
//
// ADR-0077 gave the Postgres target index-build overlap: each table's
// secondary indexes build as that table's copy lands, concurrently with the
// still-copying tables, collapsing the separate post-copy whole-schema index
// phase. The orchestrator engages it purely on the presence of
// [ir.IncrementalIndexBuilder] (internal/pipeline/migrate.go) — but only the
// PG writer implemented the surface, so every MySQL-target migrate
// (MySQL→MySQL and PG→MySQL) still ran the pre-ADR-0077 order: full copy
// phase, then a serial whole-schema CreateIndexes.
//
// This file implements [ir.IncrementalIndexBuilder] +
// [ir.TableIndexedNotifier] for the MySQL [SchemaWriter], mirroring the PG
// implementation (postgres/schema_writer_index_overlap.go): the same
// engine-neutral [tableIndexTracker] (register-before-queue, complete
// decrements under the mutex, onDone fires OUTSIDE the lock) drains the
// orchestrator's completed-tables channel into a bounded worker pool, each
// worker building one table's secondary indexes on its OWN connection via
// the shared [SchemaWriter.buildTableIndexes] (combined `ALTER TABLE … ADD
// INDEX …, ADD INDEX …` for the table's BTREE/UNIQUE indexes in one InnoDB
// scan, FULLTEXT/SPATIAL each separate, detect-then-skip for idempotent
// resume).
//
// Two MySQL-specific deviations from the PG path (ADR-0080):
//
//   - Worker sizing. MySQL has no connection-slot prober, so the
//     orchestrator always hands SetIndexBuildBudget a 0 — sizing from it
//     would floor the pool to a serial single worker, defeating the
//     feature. The pool sizes itself from a fixed-N policy
//     ([resolveIndexBuildWorkers]) instead.
//   - Flavor gate. PlanetScale/Vitess targets (flavor.usesVStream())
//     DECLINE the overlap: BuildTableIndexesFromChannel drains the channel
//     into a no-op that STILL fires the per-table callback (so resume
//     IndexesBuilt accounting stays correct) and returns nil, letting the
//     post-copy whole-schema CreateIndexes run as today. Concurrent
//     `ALTER … ADD INDEX` against vtgate fights their online-DDL /
//     Safe-Migrations queue. The gate is an internal early-return (the type
//     still satisfies the surface) so the code path stays uniform.

package mysql

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	"sluicesync.dev/sluice/internal/ir"
)

// indexBuildWorkerFloor / indexBuildWorkerCeil / indexBuildWorkerDefault
// bound the overlapped index pool's worker count (ADR-0080). MySQL has no
// measured connection budget, so the fixed default N=4 (pgcopydb's
// --index-jobs default, matching ADR-0077's ~0.25-fraction intent) is the
// only sizing input besides the job count and the operator's
// --max-target-connections cap when it's set.
const (
	indexBuildWorkerFloor   = 1
	indexBuildWorkerCeil    = 8
	indexBuildWorkerDefault = 4
)

// onIndexBuildStartObserver is a TEST-ONLY observability seam (ADR-0080):
// when non-nil it fires with a table's name the moment an overlap-path
// worker begins building one of that table's indexes. The overlap
// integration test records min(indexBuildStart) and asserts it precedes
// max(copyComplete) — proving an index build genuinely STARTED before the
// last copy finished (the overlap actually happened, not a silent
// regression to sequential). nil in production. Mirrors the PG seam of the
// same name.
var onIndexBuildStartObserver func(tableName string)

// SetIndexBuildBudget implements [ir.IndexBuildBudgetSetter] (ADR-0080).
// Stored for surface symmetry with the PG writer, but NOT used to size the
// MySQL index pool: MySQL implements no connection-slot prober, so the
// orchestrator always hands this 0 (and sizing from 0 would floor the pool
// to serial, defeating the overlap). [resolveIndexBuildWorkers] uses the
// fixed-N policy instead. Negative is clamped to 0.
func (w *SchemaWriter) SetIndexBuildBudget(connBudget int) {
	if connBudget < 0 {
		connBudget = 0
	}
	w.indexBuildBudget = connBudget
}

// SetTableIndexedCallback implements [ir.TableIndexedNotifier] (ADR-0080).
// Registers a callback the overlapped builder invokes ONCE per table, after
// that table's LAST secondary index has finished building (or immediately,
// for a table with no secondary indexes). The pipeline uses it to flip
// [ir.TableProgress.IndexesBuilt] so a resume skips an already-indexed
// table. Must be set BEFORE BuildTableIndexesFromChannel runs; nil (the
// default) is a no-op.
//
// The callback may run from any build worker goroutine, so it must be safe
// to call concurrently — the pipeline's setTableProgressAndWrite already
// serialises on stateMu, satisfying that.
func (w *SchemaWriter) SetTableIndexedCallback(fn func(table *ir.Table)) {
	w.tableIndexedCallback = fn
}

// BuildTableIndexesFromChannel implements [ir.IncrementalIndexBuilder]
// (ADR-0080). It drains completedTables, building each table's secondary
// indexes through a bounded worker pool as the table arrives, returning nil
// once the channel is closed AND every queued build has finished — or the
// first build error (which cancels its peers via the shared errgroup ctx).
//
// PlanetScale/Vitess targets DECLINE the overlap (the flavor gate): the
// channel is drained into a no-op that still fires the per-table callback
// (so resume IndexesBuilt accounting stays correct) and returns nil, leaving
// the post-copy whole-schema CreateIndexes to build the indexes as today.
func (w *SchemaWriter) BuildTableIndexesFromChannel(ctx context.Context, s *ir.Schema, completedTables <-chan *ir.Table) error {
	// Flavor gate (ADR-0080): VStream flavors route DDL through their own
	// online-DDL / Safe-Migrations queue; concurrent ALTER … ADD INDEX
	// against vtgate fights that machinery. Drain into a no-op (firing the
	// per-table callback so resume accounting stays correct) and defer to
	// the post-copy CreateIndexes.
	if w.flavor.usesVStream() {
		return w.drainTablesFiringCallback(ctx, completedTables)
	}

	if s == nil {
		// No schema to size the pool from; drain so the producer doesn't
		// block, firing the per-table callback for each (resume marks them
		// indexed). Nothing to build.
		return w.drainTablesFiringCallback(ctx, completedTables)
	}

	// Size the worker pool once up front from the count of tables WITH
	// secondary indexes (one job per table now — ADR-0080 follow-up's
	// combined-ALTER model) — the upper bound on useful workers, since each
	// table's indexes build together in one worker — even though tables
	// arrive incrementally.
	totalJobs := len(w.indexBuildJobsForTables(orderedTables(s)))
	if totalJobs == 0 {
		// No secondary indexes anywhere. Still drain (firing the per-table
		// callback so resume marks them indexed); no workers needed.
		return w.drainTablesFiringCallback(ctx, completedTables)
	}
	workers := w.resolveIndexBuildWorkers(totalJobs)

	// outstanding tracks how many of each table's indexes are still
	// building; when a table's count reaches zero the per-table callback
	// fires. Guarded by mu because the N workers decrement concurrently.
	tracker := &tableIndexTracker{
		outstanding: make(map[string]int),
		tableByName: make(map[string]*ir.Table),
		onDone:      w.fireTableIndexed,
	}

	jobCh := make(chan indexBuildJob)
	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < workers; i++ {
		g.Go(func() error {
			return w.indexBuildWorkerTracked(gctx, jobCh, tracker)
		})
	}

	// Feeder: read each completed table, expand it to its index jobs, and
	// push them onto the shared queue. A table with no secondary indexes
	// fires its callback immediately (nothing to queue). Stop on ctx
	// cancellation (a failing worker) so we don't block forever.
	g.Go(func() error {
		defer close(jobCh)
		for {
			select {
			case <-gctx.Done():
				return nil
			case table, ok := <-completedTables:
				if !ok {
					return nil
				}
				jobs := w.indexBuildJobsForTables([]*ir.Table{table})
				if len(jobs) == 0 {
					// No secondary indexes for this table — it is "indexed"
					// the moment its copy lands.
					w.fireTableIndexed(table)
					continue
				}
				// One job per table now (combined-ALTER model): the table's
				// whole index set builds in a single worker, so register a
				// count of 1 (== len(jobs)).
				tracker.register(table, len(jobs))
				for _, job := range jobs {
					select {
					case jobCh <- job:
					case <-gctx.Done():
						return nil
					}
				}
			}
		}
	})
	return g.Wait()
}

// resolveIndexBuildWorkers picks the overlapped index pool's worker count
// (ADR-0080). MySQL has no connection-slot prober, so the size is a fixed
// policy rather than a budget split: min(default-N, jobCount), clamped to
// [floor, ceil]. jobCount is the number of tables WITH secondary indexes
// (the combined-ALTER model builds each table's whole index set in one
// worker), the upper bound on useful concurrency. The --max-target-connections cap the ADR mentions is NOT
// applied here — that value is not threaded to the SchemaWriter today (it
// lives in the pipeline MigratorConfig and is consumed by the copy-axis
// budget resolver, which is a no-op on MySQL). Wiring it would require an
// orchestrator change; the ADR explicitly permits defaulting to the fixed N
// when it is not readily available. See the task report.
func (w *SchemaWriter) resolveIndexBuildWorkers(jobCount int) int {
	n := indexBuildWorkerDefault
	if jobCount < n {
		n = jobCount
	}
	if n < indexBuildWorkerFloor {
		n = indexBuildWorkerFloor
	}
	if n > indexBuildWorkerCeil {
		n = indexBuildWorkerCeil
	}
	return n
}

// indexBuildWorkerTracked is the overlap-path worker: it grabs its OWN
// dedicated connection (so the concurrent ALTERs don't serialise on one
// pooled connection), drains the shared job channel, and reports each
// completed job to the tracker so the per-table callback fires when a
// table's last index lands. The deferred Close releases the connection even
// on the error/cancel path.
func (w *SchemaWriter) indexBuildWorkerTracked(ctx context.Context, jobCh <-chan indexBuildJob, tracker *tableIndexTracker) error {
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("mysql: BuildTableIndexesFromChannel: acquire worker connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case job, ok := <-jobCh:
			if !ok {
				return nil
			}
			if hook := onIndexBuildStartObserver; hook != nil {
				hook(job.tableName)
			}
			if err := w.buildTableIndexes(ctx, conn, job); err != nil {
				return err
			}
			tracker.complete(job.tableName)
		}
	}
}

// drainTablesFiringCallback consumes completedTables until it closes (or ctx
// cancels), building NOTHING but firing the per-table callback for each so
// resume IndexesBuilt accounting stays correct. Used by the VStream flavor
// gate (the overlap is declined; the post-copy CreateIndexes builds the
// indexes) and the no-schema / no-index degenerate cases. Keeps the producer
// from blocking on an unread channel.
func (w *SchemaWriter) drainTablesFiringCallback(ctx context.Context, completedTables <-chan *ir.Table) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case table, ok := <-completedTables:
			if !ok {
				return nil
			}
			w.fireTableIndexed(table)
		}
	}
}

// fireTableIndexed invokes the registered per-table callback if set.
func (w *SchemaWriter) fireTableIndexed(table *ir.Table) {
	if w.tableIndexedCallback != nil {
		w.tableIndexedCallback(table)
	}
}

// tableIndexTracker counts each table's outstanding (still-building)
// secondary indexes and fires onDone once a table's count reaches zero. The
// N build workers call complete concurrently, so every field access goes
// through mu. Replicated from the PG engine (ADR-0080); the type is
// unexported there so it can't be shared via ir.
type tableIndexTracker struct {
	mu          sync.Mutex
	outstanding map[string]int
	tableByName map[string]*ir.Table
	onDone      func(table *ir.Table)
}

// register records that table has n indexes about to build. Called by the
// single feeder goroutine before it queues the table's jobs, so a worker can
// never complete a job before its table is registered.
func (t *tableIndexTracker) register(table *ir.Table, n int) {
	t.mu.Lock()
	t.outstanding[table.Name] = n
	t.tableByName[table.Name] = table
	t.mu.Unlock()
}

// complete decrements tableName's outstanding count; when it hits zero the
// table's onDone callback fires (outside the lock so a slow callback — e.g.
// a state-row write — doesn't block peer workers).
func (t *tableIndexTracker) complete(tableName string) {
	t.mu.Lock()
	t.outstanding[tableName]--
	done := t.outstanding[tableName] <= 0
	table := t.tableByName[tableName]
	if done {
		delete(t.outstanding, tableName)
		delete(t.tableByName, tableName)
	}
	t.mu.Unlock()
	if done && table != nil && t.onDone != nil {
		t.onDone(table)
	}
}
