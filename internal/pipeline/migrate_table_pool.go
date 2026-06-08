// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-table bulk-copy worker pool (roadmap item 3(a), ADR-0076).
//
// Before ADR-0076 the bulk-copy phase copied tables strictly serially —
// `for _, table := range schema.Tables { bulkCopyOneTable(...) }` — and
// --bulk-parallelism only split work WITHIN a table (ADR-0019 PK-range
// chunking). On a many-medium-table schema (the pgcopydb --table-jobs
// gap: 30 tables each below the within-table-split threshold) every
// table was both single-streamed AND scheduled serially, leaving cores
// idle between tables. This file adds the second axis: a bounded pool
// that copies up to tableParallelism tables CONCURRENTLY, composed with
// the within-table axis.
//
// The two axes multiply. The combined connection budget is enforced at
// the SINGLE budget chokepoint (resolveCopyParallelismBudget): the table
// pool is capped at tableParallelism and each table's own
// copyParallelismGate is seeded with <= withinParallelism tokens, so the
// product tableParallelism × withinParallelism is the construction-time
// ceiling on concurrently-open target connections — no global shared
// runtime semaphore (ADR-0076 rejected option (ii)).
//
// Per-table connections: chunk 0 of each table reuses the table's
// primary reader/writer pair. The table that wins the orchestrator's
// already-open primaries (the "free pair") reuses them, mirroring the
// within-table chunk-0 optimisation; every other concurrent table opens
// its OWN primary pair via openTablePair and closes it deterministically
// when the table finishes.
//
// Resume under concurrency: every table-level state write now races peer
// tables writing distinct keys of the same Go map. That map is not safe
// for concurrent writes, so the discipline the chunk axis already proves
// (mutate under stateMu, cloneStateForWrite under the lock, writeState
// outside it) is extended to every table-level write in bulkCopyOneTable
// and copyTableWithCursor. Resume stays order-independent: each table's
// progress entry is self-contained.
//
// The sync cold-start path (runBulkCopyWithOpts) stays SERIAL by design;
// see ADR-0076 and the comment there.

package pipeline

import (
	"context"
	"log/slog"
	"sync"

	"golang.org/x/sync/errgroup"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
)

// warnStateWriteFailed logs the best-effort per-table state-write failure
// in the shape operators already grep for ("per-table state write
// failed; continuing").
func warnStateWriteFailed(ctx context.Context, tableName string, err error) {
	slog.WarnContext(ctx, "migration: per-table state write failed; continuing",
		slog.String("table", tableName),
		slog.String("err", err.Error()))
}

// runBulkCopyTablePool copies schema.Tables through a bounded cross-table
// worker pool (ADR-0076). tableParallelism caps how many tables copy
// concurrently; 1 collapses to the pre-ADR-0076 serial behaviour (one
// goroutine, reusing the orchestrator primaries for every table in turn).
//
// primaryRows / primaryRW are the orchestrator's already-open reader /
// writer pair — the "free pair". Exactly one running table reuses them at
// a time (claimed via a 1-slot channel); peers open their own pair via
// openTablePair and close it when done. The free pair is NOT closed here
// (the caller owns its lifecycle through its deferred closeIf).
//
// The errgroup's derived ctx cancels on the first table's error so peers
// unwind promptly; g.Wait returns the first error.
func runBulkCopyTablePool(
	ctx context.Context,
	rc resumeContext,
	state *ir.MigrationState,
	stateMu *sync.Mutex,
	schema *ir.Schema,
	primaryRows ir.RowReader,
	primaryRW ir.RowWriter,
	resuming bool,
	bulkBatchSize int,
	parallel *parallelBulkCopyDeps,
	tableParallelism int,
	redactor *redact.Registry,
	shard ShardColumnSpec,
) error {
	limit := tableParallelism
	if limit < 1 {
		limit = 1
	}

	// freePair is a 1-slot pool holding the orchestrator's primaries. A
	// table goroutine tries a non-blocking receive; the winner reuses the
	// free pair (and returns it on completion so a later table can claim
	// it), every other concurrent table opens its own pair. This mirrors
	// the within-table chunk-0 optimisation at the table granularity.
	freePair := make(chan tablePair, 1)
	freePair <- tablePair{rows: primaryRows, rw: primaryRW}

	tg, tctx := errgroup.WithContext(ctx)
	tg.SetLimit(limit)
	for _, table := range schema.Tables {
		table := table
		tg.Go(func() error {
			pair, release, err := acquireTablePair(tctx, freePair, parallel)
			if err != nil {
				return err
			}
			defer release()
			return bulkCopyOneTable(
				tctx, rc, state, stateMu, pair.rows, pair.rw, table,
				resuming, bulkBatchSize, parallel, redactor, shard,
			)
		})
	}
	return tg.Wait()
}

// tablePair is one table's reader/writer pair. Whether it's the
// orchestrator's reusable free pair (returned to the pool on release) or
// a dedicated per-table pair (closed on release) is captured by the
// release closure acquireTablePair returns, not a field here.
type tablePair struct {
	rows ir.RowReader
	rw   ir.RowWriter
}

// acquireTablePair returns the pair a table goroutine should copy
// through, plus a release function the caller defers. It first tries to
// claim the orchestrator's free pair (non-blocking); if another table
// already holds it, it opens a dedicated pair via openTablePair.
//
// The release function returns the free pair to the pool (so a later
// table can reuse it) or closes a dedicated pair. It never closes the
// free pair — the orchestrator owns that lifecycle.
func acquireTablePair(
	ctx context.Context,
	freePair chan tablePair,
	deps *parallelBulkCopyDeps,
) (tablePair, func(), error) {
	select {
	case p := <-freePair:
		// Won the free pair; return it to the pool on release.
		return p, func() { freePair <- p }, nil
	default:
		// Free pair is in use by a peer table; open a dedicated pair.
		rows, rw, err := openTablePair(ctx, deps)
		if err != nil {
			return tablePair{}, func() {}, err
		}
		p := tablePair{rows: rows, rw: rw}
		release := func() {
			closeIf(p.rw)
			closeIf(p.rows)
		}
		return p, release, nil
	}
}

// openTablePair opens a dedicated source reader + target writer pair for
// one table in the cross-table pool. It is the table-granularity twin of
// the per-chunk open path: it delegates to openOneChunkConn so a dedicated
// table pair is opened (and max-buffer-bytes applied) exactly the way a
// within-table chunk pair is, keeping the two axes' connection setup
// identical. On a writer-open failure the just-opened reader is closed by
// openOneChunkConn so no connection leaks.
func openTablePair(ctx context.Context, deps *parallelBulkCopyDeps) (ir.RowReader, ir.RowWriter, error) {
	return openOneChunkConn(ctx, deps)
}

// setTableProgressAndWrite sets one table's progress entry and persists
// the state, taking stateMu and deep-cloning the state UNDER the lock
// before the JSON-encoding writeState call (ADR-0076 resume-under-
// concurrency discipline). Peer tables in the cross-table pool write
// distinct keys of the same Go map concurrently; without the lock the
// encoder would race their writes. A write error is logged at WARN and
// swallowed — the data work is load-bearing, the breadcrumb is best-
// effort — mirroring the pre-pool per-table behaviour.
func setTableProgressAndWrite(
	ctx context.Context,
	rc resumeContext,
	state *ir.MigrationState,
	stateMu *sync.Mutex,
	tableName string,
	entry ir.TableProgress,
) {
	stateMu.Lock()
	state.TableProgress[tableName] = entry
	stateCopy := cloneStateForWrite(state)
	stateMu.Unlock()
	if err := writeState(ctx, rc, stateCopy); err != nil {
		// Logged via the shared warn shape so the message matches the
		// pre-ADR-0076 per-table state-write warnings operators grep for.
		warnStateWriteFailed(ctx, tableName, err)
	}
}

// markFailedLocked is the cross-table-safe wrapper around markFailed. It
// deep-clones the state UNDER stateMu so the failure write's JSON encoding
// doesn't race a peer table mutating the shared TableProgress map. The
// returned error is markFailed's (the original err, possibly joined with a
// state-write error), so callers wrap it exactly as before.
func markFailedLocked(
	ctx context.Context,
	rc resumeContext,
	state *ir.MigrationState,
	stateMu *sync.Mutex,
	phase ir.MigrationPhase,
	err error,
) error {
	stateMu.Lock()
	stateCopy := cloneStateForWrite(state)
	stateMu.Unlock()
	return markFailed(ctx, rc, stateCopy, phase, err)
}
