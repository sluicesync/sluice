// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Overlapped index builds (ADR-0077, roadmap item 3b(a)).
//
// The whole-schema [SchemaWriter.CreateIndexes] builds every secondary
// index in ONE sweep AFTER the bulk copy of every table finishes — a
// sequential tail that, on the 110 GB / 43-table at-scale comparison, was
// ~457 s (29% of sluice's total wall) while pgcopydb hid the same work by
// building each table's indexes as its data landed.
//
// BuildTableIndexesFromChannel closes that gap: the migrate orchestrator
// runs the cross-table copy pool and this builder under ONE errgroup; the
// copy pool hands each table to a completed-tables channel as its copy
// returns nil, and this builder drains that channel with the SAME bounded,
// budget-aware worker pool CreateIndexes uses — building a table's indexes
// concurrently with the still-copying tables. A copy error cancels this
// builder (shared ctx) and a build error cancels the copy pool.
//
// The worker body ([indexBuildWorker]), the per-job CREATE INDEX
// IF NOT EXISTS ([buildOneIndex]), and the concurrency/mem resolution
// ([resolveIndexBuildConcurrency], now budget-aware via indexBuildBudget)
// are all shared verbatim with CreateIndexes — only the feeder differs:
// instead of flattening the whole schema up front, it expands each *Table
// as it arrives and tracks per-table completion so the orchestrator can
// flip IndexesBuilt once a table's last index lands.

package postgres

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	"sluicesync.dev/sluice/internal/ir"
)

// onIndexBuildStartObserver is a TEST-ONLY observability seam (ADR-0077):
// when non-nil it fires with a table's name the moment an overlap-path
// worker begins building one of that table's indexes. The overlap
// integration test records min(indexBuildStart) and asserts it precedes
// max(copyComplete) — proving an index build genuinely STARTED before the
// last copy finished (i.e. the overlap actually happened, not a silent
// regression to sequential). nil in production.
var onIndexBuildStartObserver func(tableName string)

// SetTableIndexedCallback registers a callback the overlapped index
// builder ([BuildTableIndexesFromChannel]) invokes ONCE per table, after
// that table's LAST secondary index has finished building (or immediately,
// for a table with no secondary indexes). The pipeline orchestrator uses
// it to flip [ir.TableProgress.IndexesBuilt] so a resume skips an
// already-indexed table. Must be set BEFORE BuildTableIndexesFromChannel
// runs; nil (the default) is a no-op.
//
// The callback may run from any of the build worker goroutines, so it must
// be safe to call concurrently — the pipeline's setTableProgressAndWrite
// already serialises on stateMu, satisfying that.
func (w *SchemaWriter) SetTableIndexedCallback(fn func(table *ir.Table)) {
	w.tableIndexedCallback = fn
}

// BuildTableIndexesFromChannel implements [ir.IncrementalIndexBuilder]
// (ADR-0077). It drains completedTables, building each table's secondary
// indexes through the shared bounded worker pool as the table arrives,
// returning nil once the channel is closed AND every queued build has
// finished — or the first build error (which cancels its peers).
//
// Concurrency is resolved ONCE up front from the schema's total index
// count (the upper bound on useful workers) and the RESERVED connection
// budget (SetIndexBuildBudget — never a self-probe here; copy connections
// are open simultaneously, see resolveIndexBuildConcurrency). The workers
// are CreateIndexes's workers verbatim; only the feeder differs.
func (w *SchemaWriter) BuildTableIndexesFromChannel(ctx context.Context, s *ir.Schema, completedTables <-chan *ir.Table) error {
	if s == nil {
		// No schema to size the pool from; just drain so the producer
		// doesn't block. Nothing to build.
		return drainTablesNoop(ctx, completedTables)
	}

	// Size the worker pool from the WHOLE schema's index count — the
	// upper bound on workers — even though tables arrive incrementally.
	// resolveIndexBuildConcurrency consults w.indexBuildBudget (the
	// reserved overlap slice) for the connection bound, NOT a self-probe.
	totalJobs := len(w.indexBuildJobs(s))
	conc := w.resolveIndexBuildConcurrency(ctx, totalJobs)

	if totalJobs == 0 {
		// No secondary indexes anywhere. Still drain the channel (firing
		// the per-table callback for each, so resume marks them indexed)
		// and return — no workers needed.
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
	for i := 0; i < conc.workers; i++ {
		g.Go(func() error {
			return w.indexBuildWorkerTracked(gctx, jobCh, conc, tracker)
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

// fireTableIndexed invokes the registered per-table callback if set.
func (w *SchemaWriter) fireTableIndexed(table *ir.Table) {
	if w.tableIndexedCallback != nil {
		w.tableIndexedCallback(table)
	}
}

// indexBuildWorkerTracked is the overlap-path worker: identical to
// [indexBuildWorker] (own dedicated connection, best-effort tuning,
// drains the shared job channel) but reports each completed job to the
// tracker so the per-table callback fires when a table's last index lands.
func (w *SchemaWriter) indexBuildWorkerTracked(ctx context.Context, jobCh <-chan indexBuildJob, plan indexBuildPlan, tracker *tableIndexTracker) error {
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("postgres: BuildTableIndexesFromChannel: acquire worker connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	w.tuneIndexBuildConn(ctx, conn, plan)

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
			if err := w.buildOneIndex(ctx, conn, job); err != nil {
				return err
			}
			tracker.complete(job.tableName)
		}
	}
}

// tableIndexTracker counts each table's outstanding (still-building)
// secondary indexes and fires onDone once a table's count reaches zero.
// The N build workers call complete concurrently, so every field access
// goes through mu.
type tableIndexTracker struct {
	mu          sync.Mutex
	outstanding map[string]int
	tableByName map[string]*ir.Table
	onDone      func(table *ir.Table)
}

// register records that table has n indexes about to build. Called by the
// single feeder goroutine before it queues the table's jobs, so a worker
// can never complete a job before its table is registered.
func (t *tableIndexTracker) register(table *ir.Table, n int) {
	t.mu.Lock()
	t.outstanding[table.Name] = n
	t.tableByName[table.Name] = table
	t.mu.Unlock()
}

// complete decrements tableName's outstanding count; when it hits zero the
// table's onDone callback fires (outside the lock so a slow callback —
// e.g. a state-row write — doesn't block peer workers).
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

// drainTablesNoop consumes completedTables until it closes (or ctx
// cancels), building nothing. Used when there is no schema to size a pool
// from; keeps the producer from blocking on a full/unread channel.
func drainTablesNoop(ctx context.Context, completedTables <-chan *ir.Table) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-completedTables:
			if !ok {
				return nil
			}
		}
	}
}
