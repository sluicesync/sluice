// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Parallel within-table bulk-copy orchestrator (v0.5.0).
//
// pgcopydb's signature performance comes from splitting each large
// table into N PK ranges and copying them in parallel into separate
// target connections. v0.5.0 layers that shape on top of the existing
// bulk-copy machinery: the dispatcher in [bulkCopyOneTable] hands off
// to [copyTableParallel] when:
//
//   - the table has at least --bulk-parallel-min-rows (default 100k);
//   - the table has a single integer-typed primary-key column;
//   - --bulk-parallelism is > 1;
//   - the resume state for the table either has no Chunks (fresh) or
//     has a Chunks slice consistent with the requested parallelism.
//
// The orchestrator opens N additional reader and writer connections,
// computes chunk boundaries via MIN/MAX/divide on the source PK, and
// runs N goroutines in lockstep. Each chunk has its own progressTicker
// and its own [ir.TableChunkProgress] entry within the table's
// progress row. Chunk-level cursor checkpoints write through the same
// [ir.MigrationStateStore] the single-chunk path uses, so a crash
// mid-table re-enters each chunk at its last cursor without
// re-copying completed chunks.
//
// Concurrency safety: the per-table state-row write happens from
// inside each chunk's goroutine via a small mutex guarding the
// table_progress map. The orchestrator's per-table loop is
// sequential, so no two parallel-copy phases write to the same
// state row at the same time. Within a phase, peer chunk goroutines
// take the mutex to mutate their own slot on Chunks, then deep-copy
// the state via [cloneStateForWrite] before releasing the lock so
// the JSON-encoding [writeState] call no longer shares the map and
// slice backing storage with concurrent mutators.
//
// Snapshot consistency: the cold-start (`sluice migrate`) path does
// not currently capture a source-side snapshot — each parallel reader
// opens its own connection and observes its own per-connection
// snapshot. For PG sources running OLTP traffic during the migration
// the small inconsistency window is the v1 trade-off; ADR-0019
// records this and proposes capturing a temporary replication-slot-
// based snapshot as a future enhancement. The snapshot-stream path
// (`sluice sync start` + cold-start branch) uses
// [ir.SnapshotImporter] to pin all N readers to the same exported
// snapshot when the engine implements the optional surface.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/redact"
)

// parallelBulkCopyDeps bundles the engine-level dependencies
// [copyTableParallel] needs to spin up N reader/writer pairs. The
// orchestrator constructs one of these per migration; each parallel-
// table-copy phase reuses it.
//
// The split lets [bulkCopyOneTable] hand off to the parallel path
// without growing its argument list every time a new dependency
// arrives. New fields land here, not on the bulk-copy dispatcher's
// signature.
type parallelBulkCopyDeps struct {
	// source is the source-side engine, used for OpenRowReader to
	// produce the N additional readers each chunk consumes from.
	source ir.Engine

	// target is the target-side engine, used for OpenRowWriter on
	// each chunk's writer connection.
	target ir.Engine

	// sourceDSN / targetDSN are the connection strings to feed the
	// engines. Stored verbatim from [Migrator] so the orchestrator
	// doesn't need to reopen the existing primary connections.
	sourceDSN string
	targetDSN string

	// parallelism is the configured --bulk-parallelism after
	// resolveBulkParallelism applied the "0 = min(8, NumCPU)" rule.
	// Always >= 1.
	parallelism int

	// minRows is the configured --bulk-parallel-min-rows after
	// resolveBulkParallelMinRows. Tables below this threshold use the
	// single-reader path regardless of parallelism.
	minRows int64

	// maxBufferBytes is the per-chunk soft byte cap on writer batch
	// accumulation (ADR-0028). Threaded through to each chunk's
	// writer via [ir.MaxBufferBytesSetter] when the engine
	// implements it. Zero means no cap (engines fall back to their
	// built-in default).
	maxBufferBytes int64

	// forceColdStart is the operator's --force-cold-start flag,
	// threaded verbatim from [Migrator.ForceColdStart]. It is gate
	// (3) of the ADR-0043 fast-loader selection: when set, the Bug 9
	// cold-start pre-flight was skipped, the target may already hold
	// rows, and a non-upsert WriteRows would collide on the primary
	// key — so the chunk must take the idempotent branch even on a
	// fresh, zero-progress run. See [useFastLoader].
	forceColdStart bool
}

// useFastLoader is the ADR-0043 gate: it decides whether a parallel
// chunk may stream through the engine-native fast loader
// ([ir.RowWriter.WriteRows] — PG COPY / MySQL LOAD DATA, with each
// engine's automatic fallback) instead of the generic
// [ir.IdempotentRowWriter.WriteRowsIdempotent] batched upsert.
//
// A chunk uses the fast loader IFF ALL of:
//
//   - (1) the migration is NOT a resume run, AND
//   - (2) the chunk has zero recorded prior progress
//     (LastPK == nil && RowsCopied == 0 && State != Complete), AND
//   - (3) the cold-start pre-flight was NOT bypassed via
//     --force-cold-start (the target is proven empty by Bug 9).
//
// Otherwise the chunk uses the idempotent upsert path exactly as
// before. All three gates are correctness-forced, not tunable: the
// fast path is non-upsert, so it is only ever taken where a primary-
// key collision is provably impossible. (1)+(2) give resume safety —
// a crash during a first-pass fast chunk is safe because the *next*
// invocation is a resume run, fails gate (1), and replays the chunk
// idempotently (the ADR-0043 load-bearing correctness claim). (3)
// covers the deliberate "bulk-copy into a populated target" override.
//
// Gate (4) in ADR-0043 ("NOT the mid-stream live-add path") is
// structurally vacuous for copyChunk and is deliberately NOT threaded
// as a parameter: the parallel-copy path is unreachable from live-add.
// runBulkCopyForAddTable copies via the single-table
// copyTableIdempotent and never enters tryParallelCopyTable /
// copyChunk (ADR-0036). Threading an always-false flag would be dead
// surface; per the clean-code tenet the invariant is documented here
// instead. If parallel copy is ever wired into the live-add path, the
// gate must be reintroduced as a real parameter at that point.
//
// Pure and table-unit-testable: no I/O, no state mutation.
func useFastLoader(resuming, forceColdStart bool, chunk ir.TableChunkProgress) bool {
	if resuming { // gate (1)
		return false
	}
	if forceColdStart { // gate (3)
		return false
	}
	// gate (2): zero recorded prior progress.
	return chunk.LastPK == nil &&
		chunk.RowsCopied == 0 &&
		chunk.State != ir.TableProgressComplete
}

// shouldParallelChunk decides whether a given table should take the
// parallel-copy path. Returns (true, "") to proceed; (false, reason)
// to fall back to single-reader. The reason string is logged at info
// level so operators can audit dispatch decisions.
//
// The check is split out from [canParallelChunkTable] because it adds
// the row-count threshold (orchestrator-level) to the table-shape
// (chunk-level) eligibility. Calling order: orchestrator dispatches
// → shouldParallelChunk → if true, computeChunkBoundaries (which
// re-runs the table-shape checks defensively).
func shouldParallelChunk(ctx context.Context, deps *parallelBulkCopyDeps, rows ir.RowReader, table *ir.Table) (chunk bool, reason string) {
	if deps == nil || deps.parallelism <= 1 {
		return false, "parallelism is 1; single-reader path"
	}
	if eligible, reason := canParallelChunkTable(table, deps.parallelism); !eligible {
		return false, reason
	}
	count, err := approximateRowCount(ctx, rows, table)
	if err != nil {
		// Best-effort fall-back: if the row-count probe fails (engine
		// doesn't implement RowCounter, table doesn't exist yet,
		// permission denied), default to single-reader and let the
		// operator see the warning. The data path is the load-bearing
		// thing; chunked optimisation is a perf detail.
		slog.WarnContext(ctx, "migration: row-count probe failed; falling back to single-reader copy",
			slog.String("table", table.Name),
			slog.String("err", err.Error()))
		return false, "row-count probe failed"
	}
	if count < deps.minRows {
		return false, fmt.Sprintf("table has ~%d rows; below --bulk-parallel-min-rows=%d", count, deps.minRows)
	}
	return true, ""
}

// approximateRowCount queries the row reader for an estimate of the
// table's row count via the optional [ir.RowCounter] surface.
// Returns (0, nil) when the reader doesn't implement RowCounter — the
// caller treats "0 rows" as "below any reasonable threshold" so we
// fall back to the single-reader path.
func approximateRowCount(ctx context.Context, rows ir.RowReader, table *ir.Table) (int64, error) {
	rc, ok := rows.(ir.RowCounter)
	if !ok {
		return 0, nil
	}
	return rc.CountRows(ctx, table)
}

// tryParallelCopyTable is the dispatcher entry point. Returns
// (ran=true, nil) when the parallel path handled the table; (ran=
// false, nil) when the table fell through to the single-reader path
// (eg. row count below threshold, no integer PK, parallelism is 1);
// (ran=false, err) on a parallel-path error that should be surfaced.
//
// On a resume run with previously-recorded chunks, the function honours
// the recorded chunk count even when the size threshold or eligibility
// checks would otherwise route to single-reader — finishing what was
// started keeps the state-row consistent.
func tryParallelCopyTable(
	ctx context.Context,
	rc resumeContext,
	state *ir.MigrationState,
	stateMu *sync.Mutex,
	primaryRows ir.RowReader,
	primaryRW ir.RowWriter,
	table *ir.Table,
	deps *parallelBulkCopyDeps,
	resuming bool,
	bulkBatchSize int,
	redactor *redact.Registry,
	shard ShardColumnSpec,
) (bool, error) {
	if deps == nil || deps.parallelism <= 1 {
		return false, nil
	}

	entry := state.TableProgress[table.Name]
	hasRecordedChunks := resuming && len(entry.Chunks) > 1

	if !hasRecordedChunks {
		shouldChunk, reason := shouldParallelChunk(ctx, deps, primaryRows, table)
		if !shouldChunk {
			slog.InfoContext(ctx, "migration: parallel copy not applicable; using single-reader path",
				slog.String("table", table.Name),
				slog.String("reason", reason))
			return false, nil
		}
	}

	// Compute or reload boundaries.
	chunks, err := resolveChunks(ctx, state, stateMu, rc, primaryRows, table, deps.parallelism, resuming)
	if err != nil {
		return false, err
	}
	if len(chunks) <= 1 {
		slog.InfoContext(ctx, "migration: table too small for parallel copy; using single-reader path",
			slog.String("table", table.Name),
			slog.Int("computed_chunks", len(chunks)))
		// Reset the table_progress entry so the single-reader path
		// sees a clean state (no spurious 1-chunk Chunks slice).
		stateMu.Lock()
		tp := state.TableProgress[table.Name]
		tp.Chunks = nil
		state.TableProgress[table.Name] = tp
		stateMu.Unlock()
		return false, nil
	}

	if err := runChunks(ctx, rc, state, stateMu, primaryRows, primaryRW, deps, table, chunks, bulkBatchSize, redactor, shard, resuming); err != nil {
		return false, err
	}

	// All chunks complete: record terminal state. The bare-string
	// "complete" wins on the next read; the verbose chunked entry is
	// no longer load-bearing.
	stateMu.Lock()
	state.TableProgress[table.Name] = ir.TableProgress{State: ir.TableProgressComplete}
	stateMu.Unlock()
	if err := writeState(ctx, rc, *state); err != nil {
		slog.WarnContext(ctx, "migration: terminal table-state write failed; continuing",
			slog.String("table", table.Name),
			slog.String("err", err.Error()))
	}
	return true, nil
}

// resolveChunks returns the chunk layout for this table. On a fresh
// table or an upgraded v0.4.0 row, it computes boundaries via
// MIN/MAX/divide and persists them to the state row before returning.
// On a resume run with recorded chunks, it returns the persisted layout
// unchanged so the boundary numbers stay stable across attempts.
func resolveChunks(
	ctx context.Context,
	state *ir.MigrationState,
	stateMu *sync.Mutex,
	rc resumeContext,
	primaryRows ir.RowReader,
	table *ir.Table,
	parallelism int,
	resuming bool,
) ([]ir.TableChunkProgress, error) {
	stateMu.Lock()
	entry := state.TableProgress[table.Name]
	stateMu.Unlock()

	if len(entry.Chunks) > 0 {
		if !resuming {
			// Cold-start that found a stale Chunks slice — should not
			// happen because cold-start either errors-on-existing-row
			// or fresh-writes the row. Defensive surface.
			return nil, fmt.Errorf("pipeline: table %q has chunks recorded but not resuming", table.Name)
		}
		slog.InfoContext(ctx, "migration: resuming parallel copy from per-chunk cursors",
			slog.String("table", table.Name),
			slog.Int("chunks", len(entry.Chunks)))
		return entry.Chunks, nil
	}

	// Fresh start (or upgrade from v0.4.0): compute boundaries via
	// MIN/MAX/divide.
	rangeQ, ok := primaryRows.(rangeQuerier)
	if !ok {
		return nil, errors.New("pipeline: primary row reader does not implement RangeBoundsQuerier")
	}
	bounds, err := computeChunkBoundaries(ctx, rangeQ, table, parallelism)
	if err != nil {
		return nil, fmt.Errorf("pipeline: compute chunks for %q: %w", table.Name, err)
	}
	chunks := make([]ir.TableChunkProgress, len(bounds))
	for i, b := range bounds {
		chunks[i] = ir.TableChunkProgress{
			ChunkIndex: b.chunkIndex,
			LowerPK:    b.lowerPK,
			UpperPK:    b.upperPK,
			State:      ir.TableProgressInProgress,
		}
	}
	stateMu.Lock()
	entry.State = ir.TableProgressInProgress
	entry.LastPK = nil
	entry.RowsCopied = 0
	entry.Chunks = chunks
	state.TableProgress[table.Name] = entry
	stateMu.Unlock()
	if err := writeState(ctx, rc, *state); err != nil {
		slog.WarnContext(ctx, "migration: chunk-boundary state write failed; continuing",
			slog.String("table", table.Name),
			slog.String("err", err.Error()))
	}
	return chunks, nil
}

// runChunks spawns one goroutine per chunk via errgroup. Chunk 0 reuses
// the orchestrator's already-open primary connections; chunks 1..N-1
// acquire their own reader/writer connections lazily, inside the
// goroutine, through a shared [copyParallelismGate] that caps how many
// chunk connections are concurrently open and applies the Phase 2b
// adaptive backoff on connection-slot exhaustion (SQLSTATE 53300). The
// first non-retryable error cancels the shared ctx so peers unwind
// cleanly.
//
// resuming (gate (1)) and deps.forceColdStart (gate (3)) are threaded
// to every chunk so [copyChunk] can run the ADR-0043 [useFastLoader]
// gate per chunk.
//
// Connection-resilience Phase 2b (closes the openChunkConnections
// TODO): the pre-Phase-2b code opened all N-1 extra connections eagerly
// up front inside this function and a single 53300 there failed the
// whole errgroup. The eager open is now replaced by per-chunk lazy
// acquisition with a retry loop (see [acquireChunkConn]); a transient
// mid-copy slot shortage multiplicatively shrinks parallelism and
// retries the failed chunk instead of aborting the migration. Phase 1's
// budget preflight still right-sizes parallelism up front, so this is
// defense-in-depth for the rare race where slots vanish *after* the
// preflight measured them as free.
//
// Double-copy safety on retry: the only retryable failure here is a
// connection-OPEN (ping) failure, which happens strictly before any
// COPY/WriteRows runs — so a retried chunk wrote zero rows on the failed
// attempt and re-runs [copyChunk]/[copyChunkFast] from its recorded
// cursor (or LowerPK when fresh). No partial write can precede a 53300,
// so a retry never double-copies. See the PR's safety argument.
func runChunks(
	ctx context.Context,
	rc resumeContext,
	state *ir.MigrationState,
	stateMu *sync.Mutex,
	primaryRows ir.RowReader,
	primaryRW ir.RowWriter,
	deps *parallelBulkCopyDeps,
	table *ir.Table,
	chunks []ir.TableChunkProgress,
	bulkBatchSize int,
	redactor *redact.Registry,
	shard ShardColumnSpec,
	resuming bool,
) error {
	pkCols := primaryKeyColumnNames(table)
	limit := bulkBatchSize
	if limit <= 0 {
		limit = defaultBulkBatchSize
	}

	// The gate caps concurrently-open chunk connections at the
	// post-preflight effective parallelism (= len(chunks)) and shrinks it
	// on a 53300. Chunk 0 reuses the primaries and never goes through the
	// gate, so the gate governs only the N-1 lazily-opened chunks.
	gate := newCopyParallelismGate(len(chunks), defaultCopyBackoffPolicy)

	// classifySlotExhaustion is the engine-supplied predicate that decides
	// whether an open error is the retryable slot-exhaustion class. A
	// target engine without the classifier (today: MySQL) makes every
	// open error non-retryable — the safe default (fail loudly).
	classifySlotExhaustion := func(error) bool { return false }
	if c, ok := deps.target.(ir.ConnectionSlotClassifier); ok {
		classifySlotExhaustion = c.IsConnectionSlotExhausted
	}

	g, gctx := errgroup.WithContext(ctx)
	for k := 0; k < len(chunks); k++ {
		k := k
		g.Go(func() error {
			if k == 0 {
				// Chunk 0 reuses the orchestrator's primary connections;
				// they are already open, so there is no acquire/retry and
				// no gate token to manage.
				return copyChunk(gctx, rc, state, stateMu, primaryRows, primaryRW, table, pkCols, k, limit, redactor, shard, resuming, deps.forceColdStart)
			}
			chunkRows, chunkRW, releaseConn, err := acquireChunkConn(gctx, deps, gate, classifySlotExhaustion, k)
			if err != nil {
				return err
			}
			defer releaseConn()
			return copyChunk(gctx, rc, state, stateMu, chunkRows, chunkRW, table, pkCols, k, limit, redactor, shard, resuming, deps.forceColdStart)
		})
	}
	return g.Wait()
}

// acquireChunkConn opens one source reader + one target writer for a
// non-zero chunk, retrying under the Phase 2b adaptive backoff when the
// open returns the connection-slot-exhaustion class (SQLSTATE 53300).
// It takes a gate token before each open attempt (capping concurrently-
// open connections) and returns a release function the caller defers to
// close the connections and return the token.
//
// Retry / double-copy safety: only a slot-exhaustion error on the OPEN
// (which fails at ping, before any COPY/WriteRows) is retried; every
// other error — bad DSN, permission denied, a real connection failure —
// surfaces immediately and loudly. Because a 53300 fails before any rows
// are written, a retry re-opens a fresh connection and re-runs the chunk
// from its recorded cursor with zero risk of duplicate rows.
//
// A slot-exhaustion that persists past the AIMD give-up bound surfaces a
// loud, bounded errCopySlotsExhausted — never an infinite spin.
func acquireChunkConn(
	ctx context.Context,
	deps *parallelBulkCopyDeps,
	gate *copyParallelismGate,
	isSlotExhausted func(error) bool,
	chunkIndex int,
) (ir.RowReader, ir.RowWriter, func(), error) {
	if err := gate.acquire(ctx); err != nil {
		return nil, nil, nil, err
	}
	// The token is held for the whole lifetime of this chunk's
	// connections; release() returns it (or swallows it if a shrink
	// retired it) when the caller's deferred releaseConn runs.
	release := func() { gate.release() }

	for {
		rdr, wr, err := openOneChunkConn(ctx, deps)
		if err == nil {
			closeConns := func() {
				if wr != nil {
					closeIf(wr)
				}
				if rdr != nil {
					closeIf(rdr)
				}
				release()
			}
			return rdr, wr, closeConns, nil
		}

		// Non-retryable: surface loudly and immediately. Return the token
		// so peers aren't starved while the errgroup unwinds.
		if !isSlotExhausted(err) {
			release()
			return nil, nil, nil, fmt.Errorf("open connections for chunk %d: %w", chunkIndex, err)
		}

		// Slot exhaustion: shrink parallelism + back off, then retry the
		// open. shrinkAndBackoff returns a give-up error once the AIMD
		// bound is exhausted.
		delay, giveErr := gate.shrinkAndBackoff(ctx, chunkIndex)
		if giveErr != nil {
			release()
			return nil, nil, nil, giveErr
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			release()
			return nil, nil, nil, ctx.Err()
		}
	}
}

// openOneChunkConn opens a single source reader + target writer pair for
// a parallel-copy chunk. On a writer-open failure the just-opened reader
// is closed so no connection leaks. Returns the pair, or the first open
// error verbatim (so [acquireChunkConn]'s classifier sees the engine's
// real error, including any SQLSTATE).
func openOneChunkConn(ctx context.Context, deps *parallelBulkCopyDeps) (ir.RowReader, ir.RowWriter, error) {
	rdr, err := deps.source.OpenRowReader(ctx, deps.sourceDSN)
	if err != nil {
		return nil, nil, err
	}
	wr, err := deps.target.OpenRowWriter(ctx, deps.targetDSN)
	if err != nil {
		closeIf(rdr)
		return nil, nil, err
	}
	applyMaxBufferBytes(wr, deps.maxBufferBytes)
	return rdr, wr, nil
}

// copyChunk runs the per-batch cursor loop for a single chunk. The
// shape mirrors [copyTableWithCursor] but bounds the WHERE predicate
// by the chunk's UpperPK and reports progress through a per-chunk
// section of [ir.TableProgress.Chunks].
//
// The implementation uses a `BoundedBatchedRowReader` (optional
// surface) when available and falls back to a manual filter on the
// returned rows otherwise. ADR-0019 records the rationale.
//
// ADR-0043: when [useFastLoader] returns true for this chunk (a fresh,
// zero-progress, non-resume, non-force-cold-start run), the chunk's
// whole PK-bounded range is drained through ONE
// [ir.RowWriter.WriteRows] call (PG COPY / MySQL LOAD DATA) via a
// memory-bounded streaming pump, and a single terminal per-chunk
// checkpoint is written on success — no per-batch checkpoint. A crash
// before that terminal checkpoint leaves the chunk with zero recorded
// progress, so the resume run replays the whole chunk under the
// idempotent branch (gate (1) fails). Otherwise the chunk uses the
// existing per-batch [ir.IdempotentRowWriter.WriteRowsIdempotent]
// cursor loop unchanged.
func copyChunk(
	ctx context.Context,
	rc resumeContext,
	state *ir.MigrationState,
	stateMu *sync.Mutex,
	rr ir.RowReader,
	rw ir.RowWriter,
	table *ir.Table,
	pkCols []string,
	chunkIndex int,
	limit int,
	redactor *redact.Registry,
	shard ShardColumnSpec,
	resuming bool,
	forceColdStart bool,
) (retErr error) {
	br, ok := rr.(ir.BatchedRowReader)
	if !ok {
		return errors.New("pipeline: copyChunk: row reader does not implement BatchedRowReader")
	}

	stateMu.Lock()
	tp := state.TableProgress[table.Name]
	if chunkIndex >= len(tp.Chunks) {
		stateMu.Unlock()
		return fmt.Errorf("pipeline: copyChunk: chunk index %d out of range (have %d chunks)", chunkIndex, len(tp.Chunks))
	}
	chunk := tp.Chunks[chunkIndex]
	stateMu.Unlock()

	if chunk.State == ir.TableProgressComplete {
		// Resume case: this chunk is already complete; skip.
		slog.InfoContext(ctx, "migration: skipping completed chunk",
			slog.String("table", table.Name),
			slog.Int("chunk", chunkIndex),
			slog.Int64("rows_copied", chunk.RowsCopied))
		return nil
	}

	// ADR-0043 fast-loader gate. Evaluated once at chunk entry from
	// the three correctness-forced gates (resume / prior-progress /
	// force-cold-start). When true the chunk takes the native bulk
	// loader; otherwise it falls through to the idempotent branch,
	// which additionally requires the IdempotentRowWriter surface.
	if useFastLoader(resuming, forceColdStart, chunk) {
		return copyChunkFast(ctx, rc, state, stateMu, br, rw, table, pkCols, chunkIndex, chunk, limit, redactor, shard)
	}

	iw, ok := rw.(ir.IdempotentRowWriter)
	if !ok {
		// Should never happen: both shipped engines implement
		// IdempotentRowWriter, and the fast path above is the only
		// non-idempotent route. Loud precondition rather than a
		// silent non-upsert write that could collide on resume.
		return errors.New("pipeline: copyChunk: idempotent branch selected but row writer does not implement IdempotentRowWriter")
	}

	cursor := chunk.LastPK
	if cursor == nil {
		// First batch: start at the chunk's lower bound (which is
		// nil for chunk 0 — meaning "from the start of the table").
		cursor = chunk.LowerPK
	}
	rowsCopied := chunk.RowsCopied

	// ADR-0042 Phase A — per-chunk wall-time instrumentation. DEBUG
	// level only; this is a permanent diagnostic artifact (same
	// disposition as the ADR-0033/0036 verify probes), gated behind
	// --log-level=debug so it never adds INFO+ noise for operators.
	// Comparing chunkStart across peer chunks (near-simultaneous?) and
	// whether [chunkStart,chunkEnd] intervals overlap answers H1
	// (writer serialisation); per-batch wall vs batchCount answers H2
	// (fixed per-batch overhead). The read+write are interleaved
	// through the streaming channel, so batch wall is read+redact+
	// write combined — that coupling is itself a Phase A finding.
	chunkStart := time.Now()
	var (
		totalBatchWall time.Duration
		batchN         int
	)
	slog.DebugContext(ctx, "adr0042: chunk start",
		slog.String("table", table.Name),
		slog.Int("chunk", chunkIndex),
		slog.Int("batch_limit", limit),
		slog.Time("t_start", chunkStart))

	pt := newProgressTickerForChunk(ctx, progressInterval, table.Name, chunkIndex)
	pt.rows.Store(rowsCopied)
	// Kick off an async row-count for this chunk's portion of the
	// table so the periodic progress lines carry an ETA. The query
	// runs on a separate connection (different *sql.DB pool) and
	// returns when it returns; the chunk loop is not blocked.
	kickOffRowCount(ctx, rr, table, pt)
	defer func() { pt.Stop(ctx, retErr) }()

	for {
		batchStart := time.Now() // ADR-0042 Phase A
		batchCtx, cancel := context.WithCancel(ctx)
		rowsCh, err := br.ReadRowsBatch(batchCtx, table, cursor, limit)
		if err != nil {
			cancel()
			return fmt.Errorf("read chunk %d batch: %w", chunkIndex, err)
		}

		var batchCount int64
		tracker := newPKTracker(pkCols)
		filtered := filterByUpperBound(batchCtx, rowsCh, pkCols, chunk.UpperPK)
		teed := teePKAndCount(batchCtx, filtered, tracker, &batchCount, pt.observeRow)
		// PII Phase 1: same redact-wrap as [copyTable].
		redacted, redactErrFn := redactRows(batchCtx, teed, redactor, table.Schema, table.Name, table.Columns, pkCols, "")
		// ADR-0048 Shape A: per-row discriminator stamp (see
		// copyTableWithCursor for the same shape). Zero-cost
		// passthrough when shard.Name is empty.
		stamped, _ := shardStampRows(batchCtx, redacted, shard.Name, shard.Value)
		if err := iw.WriteRowsIdempotent(batchCtx, table, stamped); err != nil {
			cancel()
			return fmt.Errorf("write chunk %d batch: %w", chunkIndex, err)
		}
		if err := redactErrFn(); err != nil {
			cancel()
			return fmt.Errorf("redact chunk %d batch: %w", chunkIndex, err)
		}
		cancel()

		// ADR-0042 Phase A — per-batch wall (read+redact+write
		// interleaved through the streaming channel). The terminal
		// batchCount==0 probe is logged too: its wall is pure
		// per-batch roundtrip overhead with zero payload, a direct
		// read on H2.
		batchWall := time.Since(batchStart)
		totalBatchWall += batchWall
		batchN++
		slog.DebugContext(ctx, "adr0042: batch done",
			slog.String("table", table.Name),
			slog.Int("chunk", chunkIndex),
			slog.Int("batch", batchN),
			slog.Int64("rows", batchCount),
			slog.Duration("wall", batchWall))

		// Loud-failure gate (Bug 68): the batched reader scans/decodes
		// on a background goroutine and aborts a batch by closing the
		// channel on a per-row failure — indistinguishable from a clean
		// short/empty batch. Each chunk owns its own reader instance
		// (see runChunks), so the sticky error is unambiguous here.
		// Check before interpreting batchCount so a decode failure
		// fails the migrate instead of looking like "end of chunk".
		if err := readerStreamErr(rr, table); err != nil {
			return err
		}

		if batchCount == 0 {
			// End of chunk (either hit upper bound or end of table).
			break
		}

		newCursor, ok := tracker.lastPK()
		if !ok {
			return errors.New("pipeline: copyChunk: batch produced rows but PK tracker captured none")
		}
		cursor = newCursor
		rowsCopied += batchCount

		// Chunk-level checkpoint write.
		stateMu.Lock()
		tp = state.TableProgress[table.Name]
		if chunkIndex < len(tp.Chunks) {
			tp.Chunks[chunkIndex].LastPK = cursor
			tp.Chunks[chunkIndex].RowsCopied = rowsCopied
			state.TableProgress[table.Name] = tp
		}
		// Deep-clone the state under the lock: writeState's JSON
		// encoding iterates TableProgress (a map shared with peer
		// chunk goroutines) and walks each entry's Chunks slice. A
		// shallow copy of *state would leave both pointing at the
		// same map and slice backing arrays, so peer goroutines
		// taking the lock to mutate their own chunk slot would race
		// the JSON encoder reading outside the lock.
		stateCopy := cloneStateForWrite(state)
		stateMu.Unlock()
		if err := writeState(ctx, rc, stateCopy); err != nil {
			slog.WarnContext(ctx, "migration: chunk cursor checkpoint write failed; continuing",
				slog.String("table", table.Name),
				slog.Int("chunk", chunkIndex),
				slog.Int64("rows_copied", rowsCopied),
				slog.String("err", err.Error()))
		}

		// Short batch indicates end-of-data within the chunk; same
		// logic as [copyTableWithCursor].
		if batchCount < int64(limit) {
			break
		}
	}

	stateMu.Lock()
	tp = state.TableProgress[table.Name]
	if chunkIndex < len(tp.Chunks) {
		tp.Chunks[chunkIndex].State = ir.TableProgressComplete
		state.TableProgress[table.Name] = tp
	}
	stateCopy := cloneStateForWrite(state)
	stateMu.Unlock()
	if err := writeState(ctx, rc, stateCopy); err != nil {
		slog.WarnContext(ctx, "migration: chunk completion state write failed; continuing",
			slog.String("table", table.Name),
			slog.Int("chunk", chunkIndex),
			slog.String("err", err.Error()))
	}

	// ADR-0042 Phase A — per-chunk summary. chunkEnd vs peer chunks'
	// [t_start,t_end] decides H1: overlapping intervals => true
	// parallelism; chunk N starting only after N-1's t_end =>
	// writer serialisation. rows_per_sec + batch_wall_share isolate
	// H2: a high non-batch remainder (chunk_wall - batch_wall_total)
	// points at fixed per-chunk overhead (rowcount kickoff,
	// checkpoint writes); a flat low rows_per_sec with full
	// batch_wall_share points at the read+write protocol path.
	chunkEnd := time.Now()
	chunkWall := chunkEnd.Sub(chunkStart)
	var rowsPerSec float64
	if s := chunkWall.Seconds(); s > 0 {
		rowsPerSec = float64(rowsCopied) / s
	}
	slog.DebugContext(ctx, "adr0042: chunk done",
		slog.String("table", table.Name),
		slog.Int("chunk", chunkIndex),
		slog.Int64("rows", rowsCopied),
		slog.Int("batches", batchN),
		slog.Time("t_start", chunkStart),
		slog.Time("t_end", chunkEnd),
		slog.Duration("chunk_wall", chunkWall),
		slog.Duration("batch_wall_total", totalBatchWall),
		slog.Duration("non_batch_wall", chunkWall-totalBatchWall),
		slog.Float64("rows_per_sec", rowsPerSec))

	return nil
}

// copyChunkFast is the ADR-0043 fast-loader branch for a fresh,
// zero-progress, non-resume, non-force-cold-start chunk (gated by
// [useFastLoader] in [copyChunk]). It drains the chunk's whole
// PK-bounded range (LowerPK exclusive .. UpperPK inclusive) through
// ONE [ir.RowWriter.WriteRows] call — one PG COPY / MySQL LOAD DATA
// for the entire chunk range — instead of the per-batch idempotent
// upsert loop.
//
// Memory-bounding (ADR-0028) is preserved: the whole chunk is NOT
// buffered. A streaming pump goroutine pages the reader with the same
// cursor-driven [ir.BatchedRowReader.ReadRowsBatch] + limit the
// idempotent loop uses, forwarding rows one-at-a-time into a single
// unbuffered channel that WriteRows consumes. At most one batch's
// worth of rows is ever in flight; the writer applies back-pressure
// through the channel exactly as the single-reader [copyTable] path
// does.
//
// Checkpoint coarsening (ADR-0043 design point b, locked): there is
// NO per-batch checkpoint on this path. On success a single terminal
// per-chunk checkpoint (RowsCopied = n, State = Complete) is written.
// A crash before that terminal write leaves the chunk with zero
// recorded progress, so the resume run fails useFastLoader gate (1)
// and replays the whole chunk under the idempotent branch — the
// load-bearing correctness property. The redact-wrap and PK/progress
// observation (teePKAndCount / pt.observeRow) match the idempotent
// loop so progress lines and PII redaction behave identically.
func copyChunkFast(
	ctx context.Context,
	rc resumeContext,
	state *ir.MigrationState,
	stateMu *sync.Mutex,
	br ir.BatchedRowReader,
	rw ir.RowWriter,
	table *ir.Table,
	pkCols []string,
	chunkIndex int,
	chunk ir.TableChunkProgress,
	limit int,
	redactor *redact.Registry,
	shard ShardColumnSpec,
) (retErr error) {
	// ADR-0042 Phase A — per-chunk wall-time instrumentation (kept;
	// permanent diagnostic). On the fast path there is one logical
	// "batch" (the single WriteRows stream), so batch_wall_total ≈
	// chunk_wall and the rows_per_sec line is directly comparable to
	// the idempotent path's per-chunk rate — that comparison is the
	// ADR-0043 "rate rises toward single-reader" acceptance signal.
	chunkStart := time.Now()
	slog.DebugContext(ctx, "adr0042: chunk start",
		slog.String("table", table.Name),
		slog.Int("chunk", chunkIndex),
		slog.Int("batch_limit", limit),
		slog.Bool("fast_loader", true),
		slog.Time("t_start", chunkStart))

	pt := newProgressTickerForChunk(ctx, progressInterval, table.Name, chunkIndex)
	// Async row-count for ETA; runs on its own connection, never
	// blocks the pump. Same disposition as the idempotent loop.
	kickOffRowCount(ctx, br, table, pt)
	defer func() { pt.Stop(ctx, retErr) }()

	// streamCtx bounds both the pump and the writer; cancelling it on
	// any error unwinds the reader goroutine cleanly (same shape as
	// copyTable's child-ctx + defer-cancel).
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	tracker := newPKTracker(pkCols)
	var rowCount int64

	// pump pages the reader cursor-by-cursor (memory-bounded: one
	// ReadRowsBatch of <= limit rows in flight at a time), bounds each
	// page by the chunk's UpperPK via the existing filterByUpperBound
	// gate, and forwards every row into the single `out` channel that
	// the one WriteRows call drains. The cursor starts at the chunk's
	// LowerPK (nil => table start) and advances by the last PK seen;
	// LastPK is guaranteed nil here (useFastLoader gate (2)) so there
	// is no mid-chunk resume cursor to honour. The downstream channel
	// is unbuffered, so the writer back-pressures the pump exactly as
	// the single-reader copyTable path does.
	out := make(chan ir.Row)
	pumpErr := make(chan error, 1)
	go func() {
		defer close(out)
		cursor := chunk.LowerPK
		for {
			rowsCh, err := br.ReadRowsBatch(streamCtx, table, cursor, limit)
			if err != nil {
				pumpErr <- fmt.Errorf("read chunk %d batch: %w", chunkIndex, err)
				return
			}
			var batchCount int64
			filtered := filterByUpperBound(streamCtx, rowsCh, pkCols, chunk.UpperPK)
			for row := range filtered {
				tracker.observe(row)
				pt.observeRow(row)
				batchCount++
				atomic.AddInt64(&rowCount, 1)
				select {
				case out <- row:
				case <-streamCtx.Done():
					pumpErr <- streamCtx.Err()
					return
				}
			}
			// Loud-failure gate (Bug 68): the batched reader scans/
			// decodes on a background goroutine and aborts a page by
			// closing its channel on a per-row failure — that looks
			// exactly like a clean short/empty page. Check the sticky
			// reader error after the page drains, before interpreting
			// batchCount, so a decode failure fails the migrate instead
			// of silently ending the chunk's stream.
			if err := readerStreamErr(br, table); err != nil {
				pumpErr <- err
				return
			}
			if batchCount == 0 {
				pumpErr <- nil // clean end of chunk range
				return
			}
			newCursor, ok := tracker.lastPK()
			if !ok {
				pumpErr <- errors.New("pipeline: copyChunkFast: batch produced rows but PK tracker captured none")
				return
			}
			cursor = newCursor
			if batchCount < int64(limit) {
				pumpErr <- nil // short page => end of data within chunk
				return
			}
		}
	}()

	// PII Phase 1: same redact-wrap as the idempotent loop / copyTable.
	redacted, redactErrFn := redactRows(streamCtx, out, redactor, table.Schema, table.Name, table.Columns, pkCols, "")
	// ADR-0048 Shape A: per-row discriminator stamp. Zero-cost
	// passthrough when shard.Name is empty.
	stamped, _ := shardStampRows(streamCtx, redacted, shard.Name, shard.Value)

	// One native bulk-load call for the whole chunk range. WriteRows
	// drains `stamped` until the pump closes `out`; PG runs a single
	// CopyFrom, MySQL a single LOAD DATA (each with its automatic
	// fallback to batched INSERT — still non-upsert, still safe on a
	// proven-empty cold chunk).
	writeErr := rw.WriteRows(streamCtx, table, stamped)

	// On a writer-side error WriteRows may have returned without
	// draining `out`, leaving the pump blocked on `out <- row`.
	// Cancel streamCtx first so the pump's select hits <-Done() and
	// posts its terminal status to the buffered pumpErr — otherwise
	// the <-pumpErr below would deadlock against the blocked pump.
	if writeErr != nil {
		cancel()
	}

	// Drain the pump's terminal status. On the success path WriteRows
	// returned because the pump closed `out`, so pumpErr is already
	// buffered; on the error path the cancel above guarantees the
	// pump posts within a scheduler tick. The cap-1 buffer makes the
	// receive race-free either way.
	pErr := <-pumpErr
	if writeErr != nil {
		return fmt.Errorf("write chunk %d (fast loader): %w", chunkIndex, writeErr)
	}
	if pErr != nil {
		cancel()
		return pErr
	}
	if err := redactErrFn(); err != nil {
		cancel()
		return fmt.Errorf("redact chunk %d (fast loader): %w", chunkIndex, err)
	}
	// Both the pump (writer side closed) and WriteRows have returned;
	// rowCount is now stable.
	finalRows := atomic.LoadInt64(&rowCount)

	// Single terminal per-chunk checkpoint (ADR-0043 design point b):
	// RowsCopied + State=Complete in one writeState. No LastPK is
	// recorded — the chunk is atomically "done" or (on crash) "never
	// started", never mid-cursor.
	stateMu.Lock()
	tp := state.TableProgress[table.Name]
	if chunkIndex < len(tp.Chunks) {
		tp.Chunks[chunkIndex].RowsCopied = finalRows
		tp.Chunks[chunkIndex].State = ir.TableProgressComplete
		state.TableProgress[table.Name] = tp
	}
	stateCopy := cloneStateForWrite(state)
	stateMu.Unlock()
	if err := writeState(ctx, rc, stateCopy); err != nil {
		slog.WarnContext(ctx, "migration: fast-loader chunk completion state write failed; continuing",
			slog.String("table", table.Name),
			slog.Int("chunk", chunkIndex),
			slog.String("err", err.Error()))
	}

	// ADR-0042 Phase A — per-chunk summary (one synthetic "batch").
	chunkEnd := time.Now()
	chunkWall := chunkEnd.Sub(chunkStart)
	var rowsPerSec float64
	if s := chunkWall.Seconds(); s > 0 {
		rowsPerSec = float64(finalRows) / s
	}
	slog.DebugContext(ctx, "adr0042: chunk done",
		slog.String("table", table.Name),
		slog.Int("chunk", chunkIndex),
		slog.Int64("rows", finalRows),
		slog.Int("batches", 1),
		slog.Bool("fast_loader", true),
		slog.Time("t_start", chunkStart),
		slog.Time("t_end", chunkEnd),
		slog.Duration("chunk_wall", chunkWall),
		slog.Duration("batch_wall_total", chunkWall),
		slog.Duration("non_batch_wall", 0),
		slog.Float64("rows_per_sec", rowsPerSec))

	return nil
}

// cloneStateForWrite returns a deep enough copy of state to be safe
// to read concurrently with peer chunk goroutines mutating the
// original under stateMu. Specifically: the TableProgress map is
// re-allocated, and each entry's Chunks slice is re-allocated so the
// encoder no longer shares slice backing storage with the chunk
// goroutines that write into [TableChunkProgress] slots under the
// lock.
//
// Other reference-typed fields on the entry (LastPK on the table-
// level entry, LowerPK/UpperPK/LastPK on each chunk) are not cloned:
// boundaries are written once during resolveChunks and per-chunk
// LastPK is replaced wholesale (not mutated in place) on every
// checkpoint, so swapping the slice header under the lock is enough
// to keep the encoder's view stable.
func cloneStateForWrite(state *ir.MigrationState) ir.MigrationState {
	cp := *state
	if state.TableProgress != nil {
		clone := make(map[string]ir.TableProgress, len(state.TableProgress))
		for k, v := range state.TableProgress {
			if len(v.Chunks) > 0 {
				chunks := make([]ir.TableChunkProgress, len(v.Chunks))
				copy(chunks, v.Chunks)
				v.Chunks = chunks
			}
			clone[k] = v
		}
		cp.TableProgress = clone
	}
	return cp
}

// filterByUpperBound wraps a row channel with a goroutine that drops
// rows whose PK exceeds the chunk's upper bound. Returns the
// downstream channel (forwarded as-is when upperPK is nil).
//
// The filter is necessary because the underlying ReadRowsBatch
// query has no notion of "upper bound" — it returns up to N rows with
// PK > cursor in PK order. Without this filter, chunk 0's batch could
// run past chunk 1's range and double-copy. The filter terminates
// the channel early (closes downstream and cancels via the parent
// ctx) when it sees a row beyond UpperPK so the reader doesn't keep
// scanning rows that won't be used.
//
// For composite PKs the comparison would need a row-comparison
// predicate; v1 supports single-column integer PKs only, so a
// straightforward int64 compare suffices.
func filterByUpperBound(ctx context.Context, src <-chan ir.Row, pkCols []string, upperPK []any) <-chan ir.Row {
	if upperPK == nil || len(pkCols) == 0 {
		// No upper bound (last chunk) or degenerate PK — pass through.
		return src
	}
	// v1 only handles single-column integer PKs. For other shapes
	// the parallel path is gated upstream; defensive pass-through
	// here keeps unexpected callers safe.
	if len(pkCols) != 1 || len(upperPK) != 1 {
		return src
	}
	upperInt, ok := coerceInt64(upperPK[0])
	if !ok {
		return src
	}
	pkCol := pkCols[0]

	out := make(chan ir.Row)
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
				rowPK, ok := coerceInt64(row[pkCol])
				if !ok {
					// Non-integer row PK on an integer-PK chunk: should
					// not happen given the eligibility checks. Forward
					// rather than silently drop to surface the
					// inconsistency in any subsequent integrity check.
					select {
					case out <- row:
					case <-ctx.Done():
						return
					}
					continue
				}
				if rowPK > upperInt {
					// Past the chunk's upper bound; drop the row and
					// drain the rest of the channel so the reader's
					// goroutine unwinds cleanly. The next ReadRowsBatch
					// returns zero rows and the chunk loop exits.
					return
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
