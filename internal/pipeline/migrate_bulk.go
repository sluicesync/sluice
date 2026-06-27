// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Per-batch checkpointed bulk-copy for the resume-mid-table path.
//
// v0.3.0's resume truncates and re-copies any in-progress table on
// retry. For multi-hour copies of a single huge table that's a full
// workday lost on a single transient failure. v0.4.0 adds per-batch
// cursor checkpointing for tables with a primary key: the orchestrator
// reads up to BulkBatchSize rows ordered by PK > cursor, applies them
// via the engine's idempotent INSERT path (ON CONFLICT / ON DUPLICATE
// KEY UPDATE), then writes the new cursor + row count to
// sluice_migrate_state.table_progress. A crash mid-table re-fetches
// the un-checkpointed batch on the next attempt; the upsert tolerates
// the small overlap window between batch commit and cursor write.
//
// Tables without a primary key fall back to v0.3.0 behaviour
// (truncate-and-redo on resume entry, plain INSERT). The classification
// is sticky in the state row: once a table is marked
// no_pk_truncate_and_redo, every subsequent failure resumes via
// truncate-and-redo regardless of how many rows the previous attempt
// landed.
//
// PG cold-start uses the COPY-protocol writer (faster); the resume
// path here uses batched INSERTs because COPY streams in a single
// logical operation that can't be checkpointed mid-batch. The
// throughput trade-off is documented in ADR-0018.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
)

// defaultBulkBatchSize is the per-batch row count when [Migrator]'s
// BulkBatchSize is left at zero. 5000 rows is a middle ground:
//
//   - Small enough to keep the replay window short on crash. With
//     5000-row batches a worst-case crash redrives ~1MB of data on a
//     typical OLTP row.
//   - Large enough to amortise per-batch tx commit overhead. At
//     ~100 rows/batch the per-tx fsync becomes a noticeable fraction
//     of throughput.
//
// Operators can tune via --bulk-batch-size; the help text on the CLI
// flag covers the trade-off.
const defaultBulkBatchSize = 5000

// bulkCopyOneTable does the per-table bulk-copy work for both the
// cold-start and resume paths. It reads the persisted progress entry
// (if any), classifies the table, and dispatches to either the
// per-batch cursor loop (resume + has-PK + engine supports the
// optional surfaces) or the whole-table copy via [copyTable] (v0.3.0
// path: cold start, no-PK fallback, or engine without batched
// surface).
//
// All state-row updates and error wrapping happen inside this
// function so [runBulkCopyPhases]'s per-table loop stays a single
// dispatch + error check.
func bulkCopyOneTable(
	ctx context.Context,
	rc resumeContext,
	state *ir.MigrationState,
	stateMu *sync.Mutex,
	rows ir.RowReader,
	rw ir.RowWriter,
	table *ir.Table,
	resuming bool,
	bulkBatchSize int,
	parallel *parallelBulkCopyDeps,
	redactor *redact.Registry,
	shard ShardColumnSpec,
) error {
	// Read the resume classification under the lock: peer tables in the
	// cross-table pool (ADR-0076) write distinct keys of state.TableProgress
	// concurrently, and the Go map is not safe for a concurrent read +
	// write. classifyTableForResume only reads this table's own key, but it
	// indexes the shared map, so it takes the same mutex every writer does.
	stateMu.Lock()
	action := classifyTableForResume(*state, table.Name, resuming)
	stateMu.Unlock()
	switch action {
	case resumeActionSkip:
		slog.InfoContext(ctx, "migration: skipping completed table",
			slog.String("table", table.Name))
		return nil
	case resumeActionTruncate:
		slog.InfoContext(ctx, "migration: truncating in-progress table for resume",
			slog.String("table", table.Name))
		if err := truncateForResume(ctx, rw, table); err != nil {
			wrapped := fmt.Errorf("pipeline: truncate before resume: %w", err)
			return wrapWithHint(PhaseBulkCopy, markFailedLocked(ctx, rc, state, stateMu, ir.MigrationPhaseBulkCopy, wrapped))
		}
	case resumeActionResumeFromCursor:
		// Cursor-bearing resume: the per-batch path picks up the
		// previous attempt's progress entry below. No truncate. Read the
		// entry under the lock (shared map; peer tables write concurrently
		// under the cross-table pool — ADR-0076).
		stateMu.Lock()
		entry := state.TableProgress[table.Name]
		stateMu.Unlock()
		slog.InfoContext(ctx, "migration: resuming table from cursor",
			slog.String("table", table.Name),
			slog.Int64("rows_already_copied", entry.RowsCopied),
			slog.Int("pk_columns", len(entry.LastPK)))
	case resumeActionResumeChunked:
		// Parallel-copy resume: per-chunk cursors live in entry.Chunks
		// and the parallel path below picks them up. No truncate.
		stateMu.Lock()
		entry := state.TableProgress[table.Name]
		stateMu.Unlock()
		slog.InfoContext(ctx, "migration: resuming chunked table from per-chunk cursors",
			slog.String("table", table.Name),
			slog.Int("chunks", len(entry.Chunks)))
	case resumeActionFresh:
		// Nothing to do up front; the per-batch path starts at PK > nil.
	}

	// ADR-0109: wrap the per-table data copy in the bounded source-read
	// reconnect-and-resume retry. A transient mid-table SOURCE-read drop
	// (the backpressure-EOF a stalled target induces) re-opens a FRESH
	// source reader and resumes the table copy instead of aborting the
	// whole run. The retry is local to THIS table, so a transient never
	// propagates to the cross-table pool's errgroup and never aborts a
	// sibling table's copy — only a terminal (non-retriable / exhausted)
	// error does. The strategy is per-table:
	//   - keyset/integer-PK CHUNKED tables resume each chunk from its
	//     persisted chunk.LastPK (re-run in RESUME mode) — dup/loss-safe;
	//   - everything else truncates the target + re-copies (always
	//     correct, the named efficiency wart).
	// When the parallel deps (the fresh-reader factory) are absent, the
	// retry is skipped and the data copy runs exactly as before — the
	// retry is purely additive resilience for the migrate path.
	attempt := func(ctx context.Context, rows ir.RowReader, resuming bool) error {
		return copyOneTableData(ctx, rc, state, stateMu, rows, rw, table, resuming, bulkBatchSize, parallel, redactor, shard)
	}
	if parallel == nil {
		return attempt(ctx, rows, resuming)
	}
	strategy := resumeTruncateRestart
	if willKeysetChunk(ctx, parallel, rows, table, resuming, state, stateMu) {
		strategy = resumeFromChunkCursor
	}
	freshReader := func(ctx context.Context) (ir.RowReader, func(), error) {
		rdr, err := openChunkReader(ctx, parallel)
		if err != nil {
			return nil, nil, err
		}
		return rdr, func() { closeIf(rdr) }, nil
	}
	truncate := func(ctx context.Context) error { return truncateForResume(ctx, rw, table) }
	// ADR-0110: thread the run's shared coordinated-pause gate so a
	// classified source-read drop on this table trips it (sibling lanes
	// quiesce) and each (re)attempt Awaits an in-effect pause. nil-safe.
	return copyTableWithSourceReadRetry(ctx, table.Name, strategy, rows, resuming, attempt, freshReader, truncate, parallel.growGate)
}

// willKeysetChunk reports whether table will take the keyset/integer-PK
// within-table CHUNKED copy path on this run — i.e. the path whose per-chunk
// LastPK crash-resume machinery (ADR-0096/0019) makes a source-read drop
// dup/loss-safely resumable from the chunk cursor (ADR-0109 §2 case 1). It
// is true when the table either already has recorded chunks (a resume) OR
// is freshly chunk-eligible-and-large-enough ([shouldParallelChunk]). Every
// other table — plain whole-table, raw-copy passthrough, no-PK, or a
// chunk-eligible table below the size threshold — has no safe mid-table
// cursor on a cold-start and uses the truncate-restart strategy instead.
//
// Pure aside from the row-count probe shouldParallelChunk already runs; it
// runs once per table BEFORE the copy, and tryParallelCopyTable re-derives
// the same decision when it actually dispatches, so the two never disagree.
func willKeysetChunk(
	ctx context.Context,
	parallel *parallelBulkCopyDeps,
	rows ir.RowReader,
	table *ir.Table,
	resuming bool,
	state *ir.MigrationState,
	stateMu *sync.Mutex,
) bool {
	if parallel == nil || parallel.parallelism <= 1 {
		return false
	}
	// Recorded chunks (a resume mid-chunked-copy) always take the chunk path.
	stateMu.Lock()
	entry := state.TableProgress[table.Name]
	stateMu.Unlock()
	if resuming && len(entry.Chunks) > 1 {
		return true
	}
	chunk, _ := shouldParallelChunk(ctx, parallel, rows, table)
	return chunk
}

// copyOneTableData runs the actual per-table data copy (the parallel/chunk
// dispatch, the ADR-0078 raw-copy passthrough, and the single-reader
// whole-table / per-batch-cursor fallbacks). It is the closure the ADR-0109
// source-read retry re-invokes on a fresh reader; the resume-classification
// + up-front truncate (the parts that must run exactly once per table) stay
// in [bulkCopyOneTable] above. Behaviour is byte-identical to the pre-ADR-0109
// inline body — only the reader + resuming arrive as parameters so a retry
// can swap a fresh reader in.
func copyOneTableData(
	ctx context.Context,
	rc resumeContext,
	state *ir.MigrationState,
	stateMu *sync.Mutex,
	rows ir.RowReader,
	rw ir.RowWriter,
	table *ir.Table,
	resuming bool,
	bulkBatchSize int,
	parallel *parallelBulkCopyDeps,
	redactor *redact.Registry,
	shard ShardColumnSpec,
) error {
	// Parallel-copy dispatch: applicable when the deps are configured,
	// the table is large enough, and the eligibility checks pass. The
	// parallel path is independent of resume — it can be used on a
	// fresh cold-start migration as well as on a resume that recorded
	// per-chunk cursors. When [tryParallelCopyTable] returns ran=true,
	// the parallel path handled the table end-to-end; ran=false means
	// "fall through to the single-reader path".
	if ran, err := tryParallelCopyTable(ctx, rc, state, stateMu, rows, rw, table, parallel, resuming, bulkBatchSize, redactor, shard); err != nil {
		wrapped := fmt.Errorf("pipeline: copy table %q (parallel): %w", table.Name, err)
		return wrapWithHint(PhaseBulkCopy, markFailedLocked(ctx, rc, state, stateMu, ir.MigrationPhaseBulkCopy, wrapped))
	} else if ran {
		return nil
	}

	// ADR-0078 raw-copy passthrough — whole-table single-stream path.
	// Mirrors the chunked dispatch in copyChunk but for tables that
	// didn't take the parallel path (below the split threshold). Engaged
	// only on a cold-start (non-resume, non-force-cold-start) run where
	// the target is proven empty (Bug 9), so the non-upsert byte-pipe
	// COPY FROM can't collide; a crash leaves the in-progress breadcrumb
	// below absent, so resume replays through the IR path. Requires the
	// run-level gate, this table's identity projection, and both
	// endpoints implementing the raw surfaces. No PK needed — the
	// whole-table export has no WHERE bound.
	if parallel != nil && parallel.rawCopyOK && !resuming && !parallel.forceColdStart && identityProjection(table) {
		if exp, imp, ok := asRawCopyEndpoints(rows, rw); ok {
			// Breadcrumb so a mid-pipe crash leaves a clean truncate-and-redo
			// entry for the next attempt (same disposition as copyTable's).
			setTableProgressAndWrite(ctx, rc, state, stateMu, table.Name, ir.TableProgress{State: ir.TableProgressInProgress})
			rowsN, rawErr := runRawCopyChunk(ctx, exp, imp, table, nil, parallel.rawCopyFormat)
			if rawErr != nil {
				wrapped := fmt.Errorf("pipeline: copy table %q (raw copy): %w", table.Name, rawErr)
				return wrapWithHint(PhaseBulkCopy, markFailedLocked(ctx, rc, state, stateMu, ir.MigrationPhaseBulkCopy, wrapped))
			}
			slog.InfoContext(ctx, "migration: table copied via raw-copy passthrough",
				slog.String("table", table.Name),
				slog.Int64("rows", rowsN))
			setTableProgressAndWrite(ctx, rc, state, stateMu, table.Name, ir.TableProgress{State: ir.TableProgressComplete})
			return nil
		}
	}

	// Decide whether per-batch checkpointing is available for this
	// table and classify accordingly. The decision is sticky: once
	// classified as no-PK truncate-and-redo, the entry stays that way
	// across attempts.
	canCursor, why := canResumePerBatch(rw, rows, table, resuming)
	if !canCursor {
		// Either no-PK, no engine-side support, or non-resume mode.
		// Fall back to the v0.3.0 whole-table copy. We still record an
		// in-progress breadcrumb so a mid-copy crash leaves a clean
		// "truncate and redo" entry on the next attempt.
		entry := ir.TableProgress{State: ir.TableProgressInProgress}
		if resuming && why == cursorBlockedNoPK {
			// Make the no-PK fallback explicit on disk so a future
			// resume reads it as truncate-and-redo immediately rather
			// than re-discovering the missing PK.
			entry = ir.TableProgress{State: ir.TableProgressNoPKTruncateAndRedo}
			slog.InfoContext(ctx, "migration: table has no primary key; falling back to truncate-and-redo on resume",
				slog.String("table", table.Name))
		}
		// In-progress breadcrumb + terminal complete both go through the
		// locked clone-and-write helper (ADR-0076): peer tables in the
		// cross-table pool write distinct keys of this map concurrently.
		setTableProgressAndWrite(ctx, rc, state, stateMu, table.Name, entry)
		if err := copyTable(ctx, rows, rw, table, redactor, shard); err != nil {
			wrapped := fmt.Errorf("pipeline: copy table %q: %w", table.Name, err)
			return wrapWithHint(PhaseBulkCopy, markFailedLocked(ctx, rc, state, stateMu, ir.MigrationPhaseBulkCopy, wrapped))
		}
		setTableProgressAndWrite(ctx, rc, state, stateMu, table.Name, ir.TableProgress{State: ir.TableProgressComplete})
		return nil
	}

	// Per-batch checkpointed path.
	limit := bulkBatchSize
	if limit <= 0 {
		limit = defaultBulkBatchSize
	}
	if err := copyTableWithCursor(ctx, rc, state, stateMu, rw, rows, table, limit, redactor, shard); err != nil {
		wrapped := fmt.Errorf("pipeline: copy table %q: %w", table.Name, err)
		return wrapWithHint(PhaseBulkCopy, markFailedLocked(ctx, rc, state, stateMu, ir.MigrationPhaseBulkCopy, wrapped))
	}
	setTableProgressAndWrite(ctx, rc, state, stateMu, table.Name, ir.TableProgress{State: ir.TableProgressComplete})
	return nil
}

// cursorBlockReason explains why per-batch checkpointing isn't usable
// for a given table. Used by callers that want to log the specific
// reason rather than a binary "can / can't" signal.
type cursorBlockReason int

const (
	cursorBlockedNotResuming        cursorBlockReason = iota // not in resume mode
	cursorBlockedNoPK                                        // table has no primary key
	cursorBlockedReaderNotImpl                               // reader doesn't implement BatchedRowReader
	cursorBlockedWriterNotImpl                               // writer doesn't implement IdempotentRowWriter
	cursorBlockedReaderDisqualified                          // reader vetoes cursor pagination for this table's PK shape
	cursorBlockedAvailable                                   // sentinel: per-batch is available
)

// canResumePerBatch reports whether the per-batch cursor path can be
// used for this table. The flag is true only when:
//
//   - The Migrator is in resume mode (cold-start uses the faster
//     plain-INSERT / COPY path).
//   - The table has a primary key (the cursor is a PK-ordered tuple).
//   - The reader implements [ir.BatchedRowReader].
//   - The writer implements [ir.IdempotentRowWriter].
//   - The reader does NOT veto cursor pagination for this table via the
//     optional [ir.BatchedReadDisqualifier] surface (a PK whose decoded
//     value can't round-trip a `>` cursor — e.g. a SQLite temporal/decimal
//     PK, whose decoded time.Time/decimal-string re-binds in the wrong
//     class against the column's stored INTEGER/REAL/TEXT and silently
//     truncates or dups the page). A vetoed table falls back to the
//     whole-table single-reader [copyTable] on resume, exactly as it did
//     before the reader implemented BatchedRowReader.
//
// All must be true. The second return value identifies the blocking
// reason for callers that want to log it.
func canResumePerBatch(rw ir.RowWriter, rr ir.RowReader, table *ir.Table, resuming bool) (bool, cursorBlockReason) {
	if !resuming {
		return false, cursorBlockedNotResuming
	}
	if table.PrimaryKey == nil || len(table.PrimaryKey.Columns) == 0 {
		return false, cursorBlockedNoPK
	}
	if _, ok := rr.(ir.BatchedRowReader); !ok {
		return false, cursorBlockedReaderNotImpl
	}
	if _, ok := rw.(ir.IdempotentRowWriter); !ok {
		return false, cursorBlockedWriterNotImpl
	}
	if d, ok := rr.(ir.BatchedReadDisqualifier); ok {
		if disq, _ := d.DisqualifiesBatchedRead(table); disq {
			return false, cursorBlockedReaderDisqualified
		}
	}
	return true, cursorBlockedAvailable
}

// copyTableWithCursor is the per-batch loop. Reads up to limit rows
// at a time via [ir.BatchedRowReader.ReadRowsBatch], applies them
// via [ir.IdempotentRowWriter.WriteRowsIdempotent], then commits a
// cursor update to sluice_migrate_state.
//
// The brief replay window between batch commit and checkpoint write
// is the load-bearing trade-off: a crash there re-applies the most
// recent batch on the next attempt, which the upsert tolerates as
// no-op UPDATEs. ADR-0018 documents this.
//
// PK extraction uses a teeing tracker that snapshots the last row's
// PK column values as they pass from reader to writer. Channel close
// signals batch end; on a clean batch the tracker holds the cursor
// to write next.
func copyTableWithCursor(
	ctx context.Context,
	rc resumeContext,
	state *ir.MigrationState,
	stateMu *sync.Mutex,
	rw ir.RowWriter,
	rr ir.RowReader,
	table *ir.Table,
	limit int,
	redactor *redact.Registry,
	shard ShardColumnSpec,
) (retErr error) {
	br, ok := rr.(ir.BatchedRowReader)
	if !ok {
		return errors.New("pipeline: row reader does not implement BatchedRowReader (caller should have classified this case earlier)")
	}
	iw, ok := rw.(ir.IdempotentRowWriter)
	if !ok {
		return errors.New("pipeline: row writer does not implement IdempotentRowWriter (caller should have classified this case earlier)")
	}

	pkCols := primaryKeyColumnNames(table)

	// Read the entry under the lock: peer tables in the cross-table pool
	// (ADR-0076) write distinct keys of this shared map concurrently.
	stateMu.Lock()
	entry := state.TableProgress[table.Name]
	stateMu.Unlock()
	if entry.State != ir.TableProgressInProgress {
		// Either fresh start (entry is the zero value) or a sticky
		// state we shouldn't be on (caller routes complete/no-PK
		// elsewhere). Reset to in-progress with the existing cursor.
		entry.State = ir.TableProgressInProgress
	}

	cursor := entry.LastPK
	rowsCopied := entry.RowsCopied

	// Persist the in-progress breadcrumb up front (mirrors the v0.3.0
	// behaviour) so a crash before the first batch lands still leaves
	// a meaningful state row for the next attempt.
	setTableProgressAndWrite(ctx, rc, state, stateMu, table.Name, entry)

	// Progress ticker for the long-running per-batch loop. The same
	// shape as copyTable; one ticker per table.
	pt := newProgressTicker(ctx, progressInterval, table.Name)
	// Pre-load the ticker with rows already copied on a previous
	// attempt so the operator sees an accurate running total. The
	// ticker treats it as the starting count.
	pt.rows.Store(rowsCopied)
	// Async row-count probe for ETA reporting; same shape as copyTable.
	kickOffRowCount(ctx, rr, table, pt)
	defer func() { pt.Stop(ctx, retErr) }()

	for {
		// Each batch runs in its own context-derived scope so the
		// reader/tee goroutines unwind cleanly when the writer returns.
		// Same Bug-9 motivation as copyTable.
		batchCtx, cancel := context.WithCancel(ctx)

		rowsCh, err := br.ReadRowsBatch(batchCtx, table, cursor, limit)
		if err != nil {
			cancel()
			return fmt.Errorf("read batch: %w", err)
		}

		var batchCount int64
		tracker := newPKTracker(pkCols)
		teed := teePKAndCount(batchCtx, rowsCh, tracker, &batchCount, pt.observeRow)
		// PII Phase 1: same redact-wrap as [copyTable]. nil/empty
		// Registry is the no-op fast path.
		redacted, redactErrFn := redactRows(batchCtx, teed, redactor, table.Schema, table.Name, table.Columns, pkCols, "")
		// ADR-0048 Shape A: stamp the operator-supplied discriminator
		// onto every batch row before idempotent write. The teePKAndCount
		// tracker above already snapshot the source-side PK BEFORE the
		// stamp — the cursor we persist is the source-side cursor, which
		// is what subsequent batched-reader calls need. Zero-cost
		// passthrough when shard.Name is empty.
		stamped, _ := shardStampRows(batchCtx, redacted, shard.Name, shard.Value)
		if err := iw.WriteRowsIdempotent(batchCtx, table, stamped); err != nil {
			cancel()
			return fmt.Errorf("write batch: %w", err)
		}
		if err := redactErrFn(); err != nil {
			cancel()
			return fmt.Errorf("redact batch: %w", err)
		}
		cancel() // batch goroutines unwind cleanly

		// Loud-failure gate (Bug 68): the batched reader scans/decodes
		// on a background goroutine and aborts the batch by closing the
		// channel on a per-row failure — indistinguishable from a clean
		// short/empty batch below. Check the reader's sticky error
		// before interpreting batchCount, so a decode failure on the
		// first row of a batch fails the migrate instead of looking
		// like "end of table".
		if err := readerStreamErr(rr, table); err != nil {
			return err
		}

		// CRITICAL silent-loss guard (same class as the ADR-0109 chunk-path
		// sibling-cancel fix; a SEPARATE pre-existing latent bug in the
		// single-reader --resume cursor path). The cross-table copy pool
		// (ADR-0076) cancels its errgroup ctx on the FIRST table's terminal
		// error so peers unwind; a peer table cancelled here closes its batch
		// channel early (batchCount==0 or short), and readerStreamErr filters
		// ctx.Canceled to nil — so without this check the cancelled table
		// returns nil, the caller marks it State=Complete, and a later --resume
		// SKIPS it with only a partial copy on disk → silent loss of its unread
		// tail. Returning the cancellation keeps the table NOT-complete so the
		// resume re-runs it from its durable LastPK. Mirrors the copyChunk /
		// copyChunkFast guards.
		if err := ctx.Err(); err != nil {
			return err
		}

		if batchCount == 0 {
			// Empty batch from the reader → end of table.
			return nil
		}

		newCursor, ok := tracker.lastPK()
		if !ok {
			// Should be impossible — the writer accepted batchCount > 0
			// rows but the tracker never saw a row's PK. Surface it
			// rather than silently looping.
			return errors.New("batch produced rows but PK tracker captured none; check primary key resolution")
		}
		cursor = newCursor
		rowsCopied += batchCount

		entry.LastPK = cursor
		entry.RowsCopied = rowsCopied
		// Set under the lock (ADR-0076): peer tables in the cross-table
		// pool write distinct keys of this map concurrently. The
		// persisted write is this table's progress row only (ADR-0082).
		stateMu.Lock()
		state.TableProgress[table.Name] = entry
		entryCopy := cloneTableProgressForWrite(entry)
		stateMu.Unlock()
		if err := writeTableProgress(ctx, rc, table.Name, entryCopy); err != nil {
			// Best-effort; log and continue. The replay window
			// tolerated by the idempotent INSERT will catch any rows
			// that re-deliver on the next attempt.
			slog.WarnContext(ctx, "migration: cursor checkpoint write failed; continuing",
				slog.String("table", table.Name),
				slog.Int64("rows_copied", rowsCopied),
				slog.String("err", err.Error()))
		}

		// If the batch was short of limit, we hit the end of the table.
		// Save one extra round-trip vs. asking for an empty batch.
		if batchCount < int64(limit) {
			return nil
		}
	}
}

// primaryKeyColumnNames returns the PK column names in declaration
// order, or nil when the table has no PK. The orchestrator routes
// no-PK tables away from the cursor path before this gets called;
// the helper is defensive about a nil PK so future callers can use
// it safely.
func primaryKeyColumnNames(table *ir.Table) []string {
	if table == nil || table.PrimaryKey == nil {
		return nil
	}
	out := make([]string, len(table.PrimaryKey.Columns))
	for i, c := range table.PrimaryKey.Columns {
		out[i] = c.Column
	}
	return out
}

// pkTracker captures the PK column values of the last row passing
// through a batch. Used by [teePKAndCount] to extract the cursor for
// the next iteration without changing the writer interface.
//
// The tracker is intentionally simple: it overwrites lastValues on
// every row, and lastValues is a fresh slice per call so the writer's
// downstream consumer can't mutate the captured values out from under
// the orchestrator. Concurrent reads of [pkTracker.lastPK] are not
// supported — the only caller is the orchestrator, which reads after
// the writer has returned.
type pkTracker struct {
	pkCols []string
	last   atomic.Pointer[[]any]
}

// newPKTracker returns a tracker for the given PK column names.
func newPKTracker(pkCols []string) *pkTracker {
	return &pkTracker{pkCols: pkCols}
}

// observe records the PK column values of row. nil row is a no-op
// (defensive — should not happen in practice). Missing PK columns
// produce a slice with nil entries; the next batch's WHERE predicate
// would be incorrect, but classifyTableForResume rejects no-PK tables
// upstream so the situation shouldn't arise.
func (t *pkTracker) observe(row ir.Row) {
	if row == nil || len(t.pkCols) == 0 {
		return
	}
	pk := make([]any, len(t.pkCols))
	for i, c := range t.pkCols {
		pk[i] = row[c]
	}
	t.last.Store(&pk)
}

// lastPK returns the PK values of the most recently observed row,
// plus a flag indicating whether any rows were seen. Returns (nil,
// false) when no rows passed through.
func (t *pkTracker) lastPK() ([]any, bool) {
	p := t.last.Load()
	if p == nil {
		return nil, false
	}
	return *p, true
}

// teePKAndCount wraps the row channel with a tee that observes each
// row's PK columns into the tracker, increments count, and invokes
// onRow for the [progressTicker]. The downstream channel carries the
// standard bounded buffer ([rowChanBuffer]) — back-pressure still
// flows through the writer once the buffer fills; closing happens on
// src close or ctx cancellation. The tracker may run ahead of the
// writer by up to the buffered rows, which is safe because the resume
// cursor is only persisted after the whole batch's WriteRows returns
// (see the checkpoint note on [rowChanBuffer]).
//
// Mirrors [teeRows]'s shape but pulls the per-row hooks together since
// the per-batch loop wants all three. onRow gets the row itself so
// observers like [progressTicker.observeRow] can sum its byte cost
// alongside the count.
func teePKAndCount(ctx context.Context, src <-chan ir.Row, tracker *pkTracker, count *int64, onRow func(ir.Row)) <-chan ir.Row {
	out := make(chan ir.Row, rowChanBuffer)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case row, ok := <-src:
				if !ok {
					return
				}
				tracker.observe(row)
				atomic.AddInt64(count, 1)
				if onRow != nil {
					onRow(row)
				}
				select {
				case out <- row:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
