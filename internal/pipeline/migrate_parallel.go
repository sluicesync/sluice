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
// Concurrency safety: the per-table progress write happens from
// inside each chunk's goroutine via a small mutex guarding the
// table_progress map. The orchestrator's per-table loop is
// sequential, so no two parallel-copy phases write the same table's
// progress at the same time. Within a phase, peer chunk goroutines
// take the mutex to mutate their own slot on Chunks, then copy the
// table's entry via [cloneTableProgressForWrite] before releasing
// the lock so the JSON-encoding [writeTableProgress] call (one
// progress-row upsert — ADR-0082) no longer shares the Chunks
// backing storage with concurrent mutators.
//
// Snapshot consistency: when the source engine implements
// [ir.SnapshotExporter] + [ir.SnapshotImporterOpener] (PG), the
// migrate path pins ALL of its parallel readers — primary, per-table
// pool, per-chunk — to ONE plain-SQL exported snapshot (perf research
// delta 1; see migrate_snapshot.go), released at copy-phase end.
// Sources without the surfaces (MySQL, SQLite) and export failures
// fall back to the original shape: each parallel reader opens its own
// connection and observes its own per-connection snapshot — the
// ADR-0019 v1 window. The snapshot-stream path (`sluice sync start` +
// cold-start branch) uses the same [ir.SnapshotImporter] machinery
// pinned to its replication slot's exported snapshot (ADR-0079).

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

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/redact"
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
	// migcore.ResolveBulkParallelism applied the "0 = min(8, NumCPU)" rule.
	// Always >= 1.
	parallelism int

	// minRows is the configured --bulk-parallel-min-rows after
	// migcore.ResolveBulkParallelMinRows. Tables below this threshold use the
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

	// rawCopyOK is the run-level result of [rawCopyGate], computed once
	// at migrate setup. When true the same-engine raw-copy passthrough
	// lane (ADR-0078) is eligible — a chunk/table additionally requires
	// [identityProjection] to hold AND its reader/writer to implement the
	// raw surfaces before it byte-pipes. False forces the IR copy path
	// for every table (the gate found a transform that the byte-pipe
	// would silently skip).
	rawCopyOK bool

	// rawCopyFormat is the negotiated wire format for the raw-copy lane
	// ([negotiateRawCopyFormat]). Threaded verbatim into every
	// ExportRawCopy / ImportRawCopy so the exporter and importer never
	// disagree. Meaningful only when rawCopyOK is true.
	rawCopyFormat ir.RawCopyFormat

	// chunkReaderFactory mints the SOURCE-side reader for each parallel
	// chunk/table connection (ADR-0079). Two callers wire it, both to
	// snapshot-pinned minting:
	//
	//   - nil: [openOneChunkConn] opens an INDEPENDENT reader via
	//     source.OpenRowReader, each observing its own per-connection
	//     snapshot (the documented ADR-0019 v1 window) — a migrate whose
	//     source lacks the exporter/importer surfaces (MySQL, SQLite) or
	//     whose export fell back loudly.
	//   - sync cold-start (non-nil): each reader is minted via the source
	//     engine's [ir.SnapshotImporter] pinned to the ONE exported
	//     snapshot ([ir.SnapshotStream.SnapshotName]), so all N readers
	//     observe the SAME consistent_point view. The factory owns each
	//     reader's connection lifecycle; the caller closes it via the
	//     same migcore.CloseIf release path a normal chunk reader uses.
	//   - migrate with a shared exported snapshot (non-nil, perf research
	//     delta 1): identical shape, pinned to the plain-SQL
	//     [ir.ExportedSnapshot] instead of a slot-exported one (see
	//     migrate_snapshot.go).
	//
	// The factory returns ONE reader per call (lazy, matching the existing
	// per-chunk acquisition shape); it must be safe for concurrent calls
	// from peer chunk/table goroutines.
	chunkReaderFactory func(ctx context.Context) (ir.RowReader, error)

	// releaseSharedSnapshot, when non-nil, is invoked ONCE by
	// [runBulkCopyTablePool] the moment its errgroup drains cleanly — the
	// earliest point where every per-table / per-chunk reader has closed
	// and nothing needs the migrate shared exported snapshot anymore. It
	// commits the exporting transaction and closes the importer pool so
	// the snapshot never pins source vacuum through the index/constraint
	// phases (the long-pin source-bloat lesson; the Bug 21 class). Under
	// the ADR-0077 overlapped copy+index phase this fires on the copy
	// producer goroutine while index builds still run — deliberate: copy
	// end, not phase end, is the release point. nil for the sync
	// cold-start (its SnapshotStream.ReleaseRows owns that lifecycle) and
	// for migrate runs without the shared snapshot; error unwinds are
	// covered by the run-teardown close in runSingleDatabase.
	releaseSharedSnapshot func(ctx context.Context)

	// growGate is the run's shared cold-copy coordinated-pause primitive
	// (ADR-0110). Constructed ONCE per cold-copy run and shared across all
	// lanes: it is threaded onto every per-chunk/per-table writer
	// ([openOneChunkConn] via [migcore.ApplyGrowGate]) and into the source-read
	// retry ([bulkCopyOneTable]) so a classified grow-transient on any lane
	// — or a proactive storage-headroom telemetry signal — quiesces ALL
	// lanes together for the grow window. nil ⇒ pre-ADR-0110 behaviour: the
	// gate degrades to a no-op (Await instant, Trip no-op) and every lane
	// rides the grow independently via its own bounded retry budget, exactly
	// as before. It is the typed [ir.GrowGate] (set via [migcore.GrowGateOrNil]),
	// not the concrete *growGate, so a nil value stays a true nil interface.
	growGate ir.GrowGate

	// copyGate is the run's SINGLE shared connection-budget gate (ADR-0123).
	// Constructed ONCE in [runBulkCopyPhases], sized to the resolved budget
	// (tableParallelism × parallelism — the same product ceiling ADR-0076
	// enforced as two static caps). Every BASE table connection
	// ([runBulkCopyTablePool] worker) AND every within-table PK-range chunk
	// ([runChunks] chunks 1..M-1) draws one token, so the budget is
	// REDISTRIBUTED at runtime: when a peer table finishes and releases its
	// base token, an in-progress LARGE table's surplus chunks — blocked on the
	// same gate — steal it, keeping the copy budget-wide down to the tail
	// instead of pinned at --bulk-parallelism. Supersedes the per-table
	// fixed-width gate. nil ⇒ pre-ADR-0123 behaviour: the table pool does no
	// base gating and [runChunks] falls back to a per-table gate (serial
	// cold-start, a no-measured-budget target, or a unit test).
	copyGate *migcore.CopyParallelismGate

	// copyBudget is the resolved connection budget (tableParallelism ×
	// parallelism) the copyGate is sized to (ADR-0123). It bounds the finer
	// chunk count so even a budget > migcore.MaxChunksPerTable is fully fillable by a
	// single large table at the tail (the chunk cap is max(migcore.MaxChunksPerTable,
	// copyBudget) in [resolveParallelChunkCount]). 0 ⇒ no measured budget; the
	// cap falls back to migcore.MaxChunksPerTable.
	copyBudget int

	// reparentTracker collects the set of tables a writer reported as
	// reparent-touched during the cold-copy (ADR-0141 — the migrate analog of
	// ADR-0113's restore reconciliation). The grow-gate calms a storage-growing
	// PlanetScale target but cannot recover rows its reparent dropped BEFORE the
	// first transient was seen (committed-but-unreplicated rows lost when the
	// new primary is promoted behind the async-acked window). After the bulk
	// copy the migrate reconciliation phase re-derives exactly these tables from
	// the (replayable, static-precondition) SOURCE so each matches the source
	// regardless of what the reparent dropped. Constructed once per migrate run
	// in [Migrator.phaseBuildCopyDeps]; wired onto every cold-copy writer via
	// [migcore.ApplyReparentObserver] (the same observer the restore wires). nil ⇒ no
	// tracking (pre-ADR-0141 behaviour, byte-for-byte — e.g. the sync
	// cold-start deps, which build no tracker).
	reparentTracker *migcore.ReparentTracker
}

// reparentMark returns the observer callback to wire onto each cold-copy
// writer (ADR-0141), or nil when no tracker is constructed so
// [migcore.ApplyReparentObserver] no-ops. Mirrors [Restore.reparentMark].
func (d *parallelBulkCopyDeps) reparentMark() func(string) {
	if d == nil || d.reparentTracker == nil {
		return nil
	}
	return d.reparentTracker.Mark
}

// copyChunkLifecycleObserver is a TEST-ONLY seam (nil in production — a single
// nil check): it fires when a within-table PK-range chunk copy STARTS
// (active=true) and ENDS (active=false), so the skewed-corpus tail-reclaim
// integration test (ADR-0123) can measure the PEAK concurrent chunk reads of
// the large table and assert the budget-wide work-stealing reclaim — that the
// big table expands toward the full budget at the tail, not pinned at
// --bulk-parallelism — while never exceeding the budget. Completed (resume-
// skipped) chunks do NOT fire it. Mirrors the intraTableChunkObserver /
// concurrentCopyDispatchObserver disposition.
var copyChunkLifecycleObserver func(table string, chunkIndex int, active bool)

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
// The check is split out from [migcore.CanParallelChunkTable] because it adds
// the row-count threshold (orchestrator-level) to the table-shape
// (chunk-level) eligibility. Calling order: orchestrator dispatches
// → shouldParallelChunk → if true, migcore.ComputeChunkBoundaries (which
// re-runs the table-shape checks defensively).
func shouldParallelChunk(ctx context.Context, deps *parallelBulkCopyDeps, rows ir.RowReader, table *ir.Table) (chunk bool, reason string) {
	if deps == nil || deps.parallelism <= 1 {
		return false, "parallelism is 1; single-reader path"
	}
	eligible, strategy, reason := migcore.CanParallelChunkTable(table, deps.parallelism)
	if !eligible {
		return false, reason
	}
	// The reader must implement the surface the chosen strategy needs;
	// otherwise fall back cleanly here rather than erroring inside
	// resolveChunks. (ADR-0096: keyset needs KeysetSampler; MIN/MAX needs
	// RangeBoundsQuerier — both shipping engines implement both.)
	switch strategy {
	case migcore.StrategyMinMaxDivide:
		if _, ok := rows.(migcore.RangeQuerier); !ok {
			return false, "reader does not implement RangeBoundsQuerier; single-reader path"
		}
	case migcore.StrategyKeysetSample:
		if _, ok := rows.(migcore.KeysetSampler); !ok {
			return false, "reader does not implement KeysetSampler (non-integer/composite PK); single-reader path"
		}
		// ADR-0096 exactly-once: the keyset path covers string / varchar /
		// char / decimal PKs whose SQL ORDER BY uses the column's native
		// collation. The chunk's INCLUSIVE upper bound MUST be enforced in
		// SQL in that SAME collation (via ReadRowsBatchBounded); the old
		// bytewise Go clip diverges from a non-C collation and can drop a
		// boundary-straddling row into NO chunk (silent loss, the Bug-74
		// class). A reader without the bounded surface therefore CANNOT do
		// keyset chunking safely — route to single-reader.
		if _, ok := rows.(ir.BoundedBatchedRowReader); !ok {
			return false, "reader does not implement BoundedBatchedRowReader; keyset upper-bound clip would diverge from DB collation; single-reader path"
		}
	}
	count, err := migcore.ApproximateRowCount(ctx, rows, table)
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

	// Read under stateMu (ADR-0076): peer tables in the cross-table pool
	// write distinct keys of this shared map concurrently, and a Go map
	// read racing a write is undefined behaviour even on a different key.
	stateMu.Lock()
	entry := state.TableProgress[table.Name]
	stateMu.Unlock()
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
	chunks, err := resolveChunks(ctx, state, stateMu, rc, primaryRows, table, deps, resuming)
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
	// no longer load-bearing. Persist via setTableProgressAndWrite so
	// the in-memory map mutation happens UNDER stateMu — peer tables
	// in the cross-table pool mutate the shared map concurrently
	// (ADR-0076) — and only this table's progress row is upserted
	// (ADR-0082).
	setTableProgressAndWrite(ctx, rc, state, stateMu, table.Name,
		ir.TableProgress{State: ir.TableProgressComplete})
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
	deps *parallelBulkCopyDeps,
	resuming bool,
) ([]ir.TableChunkProgress, error) {
	parallelism := deps.parallelism
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

	// Fresh start (or upgrade from v0.4.0): compute boundaries. The
	// strategy is derived from the table's PK shape (ADR-0096):
	// single integer PK → MIN/MAX/divide; non-integer / composite
	// orderable PK → sampled-keyset.
	eligible, strategy, reason := migcore.CanParallelChunkTable(table, parallelism)
	if !eligible {
		return nil, fmt.Errorf("pipeline: table %q not chunk-eligible: %s", table.Name, reason)
	}
	// ADR-0123: derive a FINER chunk count from the row-count estimate so a
	// large table can fill the WHOLE budget at the tail, not just
	// --bulk-parallelism. Boundaries stay a deterministic pure function of the
	// sorted PK set (same migcore.ComputeChunkBoundaries / migcore.ComputeKeysetChunkBoundaries,
	// just a larger n) and are persisted below, so resume re-derives the
	// identical set. Pre-ADR-0123 this was exactly `parallelism` chunks.
	m := resolveParallelChunkCount(ctx, primaryRows, table, deps)
	var (
		bounds []migcore.ChunkBoundary
		err    error
	)
	switch strategy {
	case migcore.StrategyMinMaxDivide:
		rangeQ, ok := primaryRows.(migcore.RangeQuerier)
		if !ok {
			return nil, errors.New("pipeline: primary row reader does not implement RangeBoundsQuerier")
		}
		bounds, err = migcore.ComputeChunkBoundaries(ctx, rangeQ, table, m)
	case migcore.StrategyKeysetSample:
		sampler, ok := primaryRows.(migcore.KeysetSampler)
		if !ok {
			return nil, errors.New("pipeline: primary row reader does not implement KeysetSampler")
		}
		bounds, err = migcore.ComputeKeysetChunkBoundaries(ctx, sampler, table, m)
	default:
		return nil, fmt.Errorf("pipeline: table %q has unknown chunk strategy %d", table.Name, strategy)
	}
	if err != nil {
		return nil, fmt.Errorf("pipeline: compute chunks for %q: %w", table.Name, err)
	}
	chunks := make([]ir.TableChunkProgress, len(bounds))
	for i, b := range bounds {
		chunks[i] = ir.TableChunkProgress{
			ChunkIndex: b.ChunkIndex,
			LowerPK:    b.LowerPK,
			UpperPK:    b.UpperPK,
			State:      ir.TableProgressInProgress,
		}
	}
	entry.State = ir.TableProgressInProgress
	entry.LastPK = nil
	entry.RowsCopied = 0
	entry.Chunks = chunks
	// Set-under-lock persist (ADR-0076): peer tables mutate the shared
	// TableProgress map concurrently, so the set + entry clone must
	// happen under stateMu (setTableProgressAndWrite does exactly that,
	// then upserts this table's progress row — ADR-0082).
	setTableProgressAndWrite(ctx, rc, state, stateMu, table.Name, entry)
	return chunks, nil
}

// resolveParallelChunkCount derives how many PK-range chunks a large table is
// split into on a FRESH run (ADR-0123 Decision 2). Pre-ADR-0123 this was a
// fixed `parallelism` chunks, which pinned a single big table to
// --bulk-parallelism of the budget; the finer count lets an in-progress large
// table fill the WHOLE shared budget at the tail (the surplus chunks steal
// freed tokens off [parallelBulkCopyDeps.copyGate]).
//
//		M = clamp(ceil(est / threshold), parallelism, max(migcore.MaxChunksPerTable, budget))
//
//	  - threshold is the same migcore.ResolveBulkParallelMinRows the eligibility gate
//	    uses, so each chunk targets ~one threshold's worth of rows;
//	  - the FLOOR is `parallelism` (the old fixed M), so no eligible table ever
//	    gets FEWER chunks than pre-ADR-0123 — strictly >= the old width;
//	  - the CAP is max(migcore.MaxChunksPerTable, copyBudget) so even a budget larger
//	    than migcore.MaxChunksPerTable is fully fillable while bounding per-chunk
//	    overhead in the common case.
//
// Mirrors ADR-0119's chunkItemsFor clamp. Deterministic given (est, threshold,
// budget); resume does not call this (it reloads the persisted chunk set), so
// the only requirement is internal consistency within a fresh run.
func resolveParallelChunkCount(ctx context.Context, rows ir.RowReader, table *ir.Table, deps *parallelBulkCopyDeps) int {
	est, _ := migcore.ApproximateRowCount(ctx, rows, table)
	return migcore.ClampParallelChunkCount(est, deps.minRows, deps.parallelism, deps.copyBudget)
}

// runChunks spawns one goroutine per chunk via errgroup. Chunk 0 reuses
// the orchestrator's already-open primary connections; chunks 1..N-1
// acquire their own reader/writer connections lazily, inside the
// goroutine, through a shared [migcore.CopyParallelismGate] that caps how many
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
	pkCols := migcore.PrimaryKeyColumnNames(table)
	limit := bulkBatchSize
	if limit <= 0 {
		limit = migcore.DefaultBulkBatchSize
	}

	// ADR-0123: the run's SINGLE shared connection-budget gate caps
	// concurrently-open chunk connections RUN-WIDE (across all tables) and
	// shrinks on a 53300, so an in-progress large table's surplus chunks steal
	// budget freed by finished peer tables. Chunk 0 reuses the primaries
	// (covered by the table-pool worker's base token) and never goes through
	// the gate, so the gate governs only chunks 1..M-1. nil copyGate ⇒
	// pre-ADR-0123 per-table gate sized to this table's chunk count (serial
	// cold-start / no-budget target / a unit test that didn't wire one) —
	// byte-identical to the old behaviour.
	gate := deps.copyGate
	if gate == nil {
		gate = migcore.NewCopyParallelismGate(len(chunks), migcore.DefaultCopyBackoffPolicy)
	}

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
				return copyChunk(gctx, rc, state, stateMu, primaryRows, primaryRW, table, pkCols, k, limit, redactor, shard, resuming, deps.forceColdStart, deps.rawCopyOK, deps.rawCopyFormat)
			}
			chunkRows, chunkRW, releaseConn, err := acquireChunkConn(gctx, deps, gate, classifySlotExhaustion, k, table.Name)
			if err != nil {
				return err
			}
			defer releaseConn()
			return copyChunk(gctx, rc, state, stateMu, chunkRows, chunkRW, table, pkCols, k, limit, redactor, shard, resuming, deps.forceColdStart, deps.rawCopyOK, deps.rawCopyFormat)
		})
	}
	return g.Wait()
}

// acquireChunkConn opens one source reader + one target writer for a
// non-zero chunk, retrying under the Phase 2b adaptive backoff when the
// open returns the connection-slot-exhaustion class (SQLSTATE 53300), and
// (ADR-0146) under a bounded reconnect budget when the open hits a transient
// connection drop. It takes a gate token before the open (capping
// concurrently-open connections) and returns a release function the caller
// defers to close the connections and return the token.
//
// Retry / double-copy safety: an open failure — whether 53300 or a transient
// drop — fails at reader-open or writer-open, BEFORE any COPY/WriteRows (see
// openOneChunkConn, which closes any partial and returns (nil, nil, err)). So
// a retry re-opens a fresh connection and re-runs the chunk from its recorded
// chunk.LastPK cursor with zero risk of duplicate rows. Every other error —
// bad DSN, permission denied, an unknown fault — surfaces immediately and
// loudly. A slot-exhaustion that persists past the AIMD give-up bound, or a
// transient that persists past the reconnect wall-clock/attempt budget,
// surfaces a loud, bounded error — never an infinite spin.
//
// The retry loop itself lives in openChunkConnWithRetry (copy_chunk_open_
// retry.go); this wrapper only manages the gate token and builds the closer.
func acquireChunkConn(
	ctx context.Context,
	deps *parallelBulkCopyDeps,
	gate *migcore.CopyParallelismGate,
	isSlotExhausted func(error) bool,
	chunkIndex int,
	tableName string,
) (ir.RowReader, ir.RowWriter, func(), error) {
	if err := gate.Acquire(ctx); err != nil {
		return nil, nil, nil, err
	}
	// The token is held for the whole lifetime of this chunk's connections
	// (across every transient/slot retry — the same chunk keeps its budget
	// slot); release() returns it (or swallows it if a shrink retired it)
	// when the caller's deferred releaseConn runs, or on any error exit here.
	release := func() { gate.Release() }

	rdr, wr, err := openChunkConnWithRetry(
		ctx, chunkIndex, tableName,
		func(ctx context.Context) (ir.RowReader, ir.RowWriter, error) {
			return openOneChunkConn(ctx, deps)
		},
		isSlotExhausted,
		gate.ShrinkAndBackoff,
	)
	if err != nil {
		// Return the token so peers aren't starved while the errgroup unwinds.
		release()
		return nil, nil, nil, err
	}
	closeConns := func() {
		if wr != nil {
			migcore.CloseIf(wr)
		}
		if rdr != nil {
			migcore.CloseIf(rdr)
		}
		release()
	}
	return rdr, wr, closeConns, nil
}

// openChunkReader mints the source-side reader for one parallel chunk/table
// connection. It routes through [parallelBulkCopyDeps.chunkReaderFactory]
// when set (sync cold-start: a snapshot-pinned reader, ADR-0079) and falls
// back to an independent source.OpenRowReader otherwise (migrate, the
// pre-ADR-0079 behaviour). Centralising the choice here keeps every reader
// open-site — per-chunk and per-table (openTablePair delegates here too) —
// consistent by construction.
func openChunkReader(ctx context.Context, deps *parallelBulkCopyDeps) (ir.RowReader, error) {
	if deps.chunkReaderFactory != nil {
		return deps.chunkReaderFactory(ctx)
	}
	return deps.source.OpenRowReader(ctx, deps.sourceDSN)
}

// openOneChunkConn opens a single source reader + target writer pair for
// a parallel-copy chunk. On a writer-open failure the just-opened reader
// is closed so no connection leaks. Returns the pair, or the first open
// error verbatim (so [acquireChunkConn]'s classifier sees the engine's
// real error, including any SQLSTATE).
func openOneChunkConn(ctx context.Context, deps *parallelBulkCopyDeps) (ir.RowReader, ir.RowWriter, error) {
	rdr, err := openChunkReader(ctx, deps)
	if err != nil {
		return nil, nil, err
	}
	wr, err := deps.target.OpenRowWriter(ctx, deps.targetDSN)
	if err != nil {
		migcore.CloseIf(rdr)
		return nil, nil, err
	}
	migcore.ApplyMaxBufferBytes(wr, deps.maxBufferBytes)
	// ADR-0110: share the run's coordinated grow-pause gate with this
	// chunk/table writer so every cold-copy lane quiesces together for a
	// target storage-grow window. nil-safe (no-op when the run has no gate
	// or the engine doesn't implement the setter).
	migcore.ApplyGrowGate(wr, deps.growGate)
	// ADR-0141: wire the run's reparent observer onto this per-chunk/per-table
	// writer too, alongside the grow-gate — any writer that can hit
	// flushWithReparentRetry must report through the shared tracker so the
	// migrate reconciliation phase re-derives every reparent-touched table.
	// nil-safe (no-op when the run built no tracker — the migrate path always
	// does — or the engine doesn't implement ir.ReparentObserverSetter).
	migcore.ApplyReparentObserver(wr, deps.reparentMark())
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
	rawCopyOK bool,
	rawCopyFormat ir.RawCopyFormat,
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

	// ADR-0123 tail-reclaim observability (test-only). Wraps the whole active
	// chunk copy (fast / raw / idempotent) so the integration test can track
	// peak concurrent chunk reads per table.
	if obs := copyChunkLifecycleObserver; obs != nil {
		obs(table.Name, chunkIndex, true)
		defer obs(table.Name, chunkIndex, false)
	}

	// ADR-0043 fast-loader gate. Evaluated once at chunk entry from
	// the three correctness-forced gates (resume / prior-progress /
	// force-cold-start). When true the chunk takes the native bulk
	// loader; otherwise it falls through to the idempotent branch,
	// which additionally requires the IdempotentRowWriter surface.
	if useFastLoader(resuming, forceColdStart, chunk) {
		// ADR-0078 raw-copy passthrough: slotted INSIDE the fast-loader
		// branch so it inherits the same cold-start safety (a fresh,
		// zero-progress, proven-empty chunk — a byte-pipe COPY FROM is
		// non-upsert and can only run where a PK collision is impossible,
		// and a crash mid-pipe leaves zero recorded progress so resume
		// replays via the IR path). Engaged only when the run-level gate
		// holds AND this table's projection is identity-safe AND both
		// endpoints implement the raw surfaces; otherwise the regular IR
		// fast-loader runs.
		// Raw-copy bounds a chunk with bare integer SQL literals
		// (rawCopyChunkPredicate), so it is ONLY safe for a single integer
		// PK. A non-integer / composite keyset chunk must NOT take this
		// lane — it would either fail loudly on the int-literal check or,
		// worse, mis-bound — so route it to the IR fast loader, which
		// pushes the chunk's upper bound into SQL in the column's native
		// collation (ADR-0096 exactly-once).
		if rawCopyOK && identityProjection(table) && isIntegerSinglePK(table) {
			if exp, imp, ok := asRawCopyEndpoints(rr, rw); ok {
				return copyChunkRaw(ctx, rc, state, stateMu, exp, imp, table, pkCols, chunkIndex, chunk, rawCopyFormat)
			}
		}
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
		filtered, err := migcore.ReadChunkBatch(batchCtx, br, table, cursor, chunk.UpperPK, pkCols, limit)
		if err != nil {
			cancel()
			return fmt.Errorf("read chunk %d batch: %w", chunkIndex, err)
		}

		var batchCount int64
		tracker := migcore.NewPKTracker(pkCols)
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
		if err := migcore.ReaderStreamErr(rr, table); err != nil {
			return err
		}

		// CRITICAL (ADR-0109 sibling-cancel silent-loss fix): a
		// ctx-cancellation must NOT read as a clean end-of-chunk. A peer
		// chunk's retriable source-read drop cancels the shared errgroup ctx;
		// the reader then closes this chunk's batch channel early (batchCount==0
		// or short), and migcore.ReaderStreamErr filters ctx.Canceled to nil — so
		// without this check the chunk would break-out and be marked
		// State=Complete with a partial copy, and the whole-table retry would
		// SKIP it → silent loss of the unread tail. Returning the cancellation
		// keeps the chunk NOT-complete so the retry re-runs it from its durable
		// LastPK cursor. Mirrors the copyChunkFast pump guard.
		if err := ctx.Err(); err != nil {
			return err
		}

		if batchCount == 0 {
			// End of chunk (either hit upper bound or end of table).
			break
		}

		newCursor, ok := tracker.LastPK()
		if !ok {
			return errors.New("pipeline: copyChunk: batch produced rows but PK tracker captured none")
		}
		cursor = newCursor
		rowsCopied += batchCount

		// Chunk-level checkpoint write — one progress-row upsert for
		// THIS table (ADR-0082). Clone the entry under the lock:
		// writeTableProgress's JSON encoding walks the entry's Chunks
		// slice, whose backing array peer chunk goroutines mutate
		// under the lock — a shallow entry copy would race the
		// encoder reading outside it.
		stateMu.Lock()
		tp = state.TableProgress[table.Name]
		if chunkIndex < len(tp.Chunks) {
			tp.Chunks[chunkIndex].LastPK = cursor
			tp.Chunks[chunkIndex].RowsCopied = rowsCopied
			state.TableProgress[table.Name] = tp
		}
		entryCopy := cloneTableProgressForWrite(tp)
		stateMu.Unlock()
		if err := writeTableProgress(ctx, rc, table.Name, entryCopy); err != nil {
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
	entryCopy := cloneTableProgressForWrite(tp)
	stateMu.Unlock()
	if err := writeTableProgress(ctx, rc, table.Name, entryCopy); err != nil {
		slog.WarnContext(ctx, "migration: chunk completion state write failed; continuing",
			slog.String("table", table.Name),
			slog.Int("chunk", chunkIndex),
			slog.String("err", err.Error()))
	}

	logChunkSummary(ctx, table.Name, chunkIndex, chunkStart, rowsCopied, batchN, totalBatchWall)
	return nil
}

// logChunkSummary emits the ADR-0042 Phase A per-chunk timing summary at
// DEBUG. chunkEnd vs peer chunks' [t_start,t_end] decides H1 (overlapping
// intervals => true parallelism; chunk N starting only after N-1's t_end
// => writer serialisation); rows_per_sec + batch_wall_share isolate H2 (a
// high non-batch remainder — chunk_wall minus batch_wall_total — points
// at fixed per-chunk overhead like the rowcount kickoff and checkpoint
// writes; a flat low rows_per_sec with full batch_wall_share points at
// the read+write protocol path).
func logChunkSummary(ctx context.Context, tableName string, chunkIndex int, chunkStart time.Time, rowsCopied int64, batchN int, totalBatchWall time.Duration) {
	chunkEnd := time.Now()
	chunkWall := chunkEnd.Sub(chunkStart)
	var rowsPerSec float64
	if s := chunkWall.Seconds(); s > 0 {
		rowsPerSec = float64(rowsCopied) / s
	}
	slog.DebugContext(ctx, "adr0042: chunk done",
		slog.String("table", tableName),
		slog.Int("chunk", chunkIndex),
		slog.Int64("rows", rowsCopied),
		slog.Int("batches", batchN),
		slog.Time("t_start", chunkStart),
		slog.Time("t_end", chunkEnd),
		slog.Duration("chunk_wall", chunkWall),
		slog.Duration("batch_wall_total", totalBatchWall),
		slog.Duration("non_batch_wall", chunkWall-totalBatchWall),
		slog.Float64("rows_per_sec", rowsPerSec))
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

	tracker := migcore.NewPKTracker(pkCols)
	var rowCount int64

	// pump pages the reader cursor-by-cursor (memory-bounded: one
	// ReadRowsBatch of <= limit rows in flight at a time), bounds each
	// page by the chunk's UpperPK via the existing filterByUpperBound
	// gate, and forwards every row into the single `out` channel that
	// the one WriteRows call drains. The cursor starts at the chunk's
	// LowerPK (nil => table start) and advances by the last PK seen;
	// LastPK is guaranteed nil here (useFastLoader gate (2)) so there
	// is no mid-chunk resume cursor to honour. The downstream channel
	// carries the standard bounded buffer ([migcore.RowChanBuffer]) — the
	// writer still back-pressures the pump once it fills, but decode
	// and COPY overlap instead of rendezvous-alternating.
	out := make(chan ir.Row, migcore.RowChanBuffer)
	pumpErr := make(chan error, 1)
	go func() {
		defer close(out)
		cursor := chunk.LowerPK
		for {
			filtered, err := migcore.ReadChunkBatch(streamCtx, br, table, cursor, chunk.UpperPK, pkCols, limit)
			if err != nil {
				pumpErr <- fmt.Errorf("read chunk %d batch: %w", chunkIndex, err)
				return
			}
			var batchCount int64
			for row := range filtered {
				tracker.Observe(row)
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
			if err := migcore.ReaderStreamErr(br, table); err != nil {
				pumpErr <- err
				return
			}
			// CRITICAL (ADR-0109 sibling-cancel silent-loss fix): a
			// ctx-cancellation must NOT read as a clean end-of-chunk. The
			// reader closes its page channel early on cancellation (yielding
			// batchCount==0 or a short page), and migcore.ReaderStreamErr DELIBERATELY
			// filters ctx.Canceled/DeadlineExceeded to nil (benign-cancel) — so
			// without this check a chunk cancelled by a PEER chunk's retriable
			// source-read drop (errgroup cancels gctx) would post pumpErr<-nil,
			// be recorded State=Complete with a PARTIAL/EMPTY copy, and the
			// whole-table retry would then SKIP it (chunk already "complete") →
			// silent loss of the chunk's unread tail. Surfacing the cancellation
			// leaves the chunk NOT-complete so the retry re-runs it from its
			// durable cursor. (Checked before the batchCount/short-page verdicts,
			// which is where the benign-cancel close lands.)
			if err := streamCtx.Err(); err != nil {
				pumpErr <- err
				return
			}
			if batchCount == 0 {
				pumpErr <- nil // clean end of chunk range
				return
			}
			newCursor, ok := tracker.LastPK()
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
	// RowsCopied + State=Complete in one writeTableProgress. No LastPK
	// is recorded — the chunk is atomically "done" or (on crash)
	// "never started", never mid-cursor.
	stateMu.Lock()
	tp := state.TableProgress[table.Name]
	if chunkIndex < len(tp.Chunks) {
		tp.Chunks[chunkIndex].RowsCopied = finalRows
		tp.Chunks[chunkIndex].State = ir.TableProgressComplete
		state.TableProgress[table.Name] = tp
	}
	entryCopy := cloneTableProgressForWrite(tp)
	stateMu.Unlock()
	if err := writeTableProgress(ctx, rc, table.Name, entryCopy); err != nil {
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

// copyChunkRaw is the ADR-0078 raw-copy passthrough branch for a fresh,
// zero-progress chunk that passed the run-level [rawCopyGate], this
// table's [identityProjection], and whose endpoints implement the raw
// surfaces. It byte-pipes the chunk's PK range straight from source COPY
// TO STDOUT into target COPY FROM STDIN via [runRawCopyChunk] — no row
// is ever decoded.
//
// Checkpoint shape mirrors [copyChunkFast] exactly: NO per-batch
// checkpoint (a byte-pipe is one atomic logical operation), one terminal
// per-chunk checkpoint (RowsCopied + State=Complete) on success. A crash
// before that terminal write leaves the chunk with zero recorded
// progress, so the resume run fails [useFastLoader] gate (1) and replays
// the whole chunk through the IR path — the same load-bearing correctness
// property the fast loader relies on (raw lane never taken on resume).
//
// v1 supports a SINGLE integer PK column (the orchestrator's chunk
// machinery already restricts itself to that shape via
// migcore.CanParallelChunkTable); the chunk's [ir.TableChunkProgress] PK tuples
// are 1-wide, so we project the lone bound. A defensively-empty pkCols
// or a missing first chunk-PK element is a programming error (chunking
// would not have produced this chunk), surfaced loudly rather than
// silently copying the whole table.
func copyChunkRaw(
	ctx context.Context,
	rc resumeContext,
	state *ir.MigrationState,
	stateMu *sync.Mutex,
	exp ir.RawCopyExporter,
	imp ir.RawCopyImporter,
	table *ir.Table,
	pkCols []string,
	chunkIndex int,
	chunk ir.TableChunkProgress,
	format ir.RawCopyFormat,
) error {
	if len(pkCols) != 1 {
		return fmt.Errorf("pipeline: copyChunkRaw: expected exactly one PK column, got %d", len(pkCols))
	}
	rcChunk := &ir.RawCopyChunk{
		PKColumn: pkCols[0],
		LowerPK:  firstPKBound(chunk.LowerPK),
		UpperPK:  firstPKBound(chunk.UpperPK),
	}

	chunkStart := time.Now()
	slog.DebugContext(ctx, "adr0078: raw chunk start",
		slog.String("table", table.Name),
		slog.Int("chunk", chunkIndex),
		slog.Bool("raw_copy", true),
		slog.String("format", format.String()),
		slog.Time("t_start", chunkStart))

	rows, err := runRawCopyChunk(ctx, exp, imp, table, rcChunk, format)
	if err != nil {
		return err
	}

	// Single terminal per-chunk checkpoint (mirrors copyChunkFast).
	stateMu.Lock()
	tp := state.TableProgress[table.Name]
	if chunkIndex < len(tp.Chunks) {
		tp.Chunks[chunkIndex].RowsCopied = rows
		tp.Chunks[chunkIndex].State = ir.TableProgressComplete
		state.TableProgress[table.Name] = tp
	}
	entryCopy := cloneTableProgressForWrite(tp)
	stateMu.Unlock()
	if err := writeTableProgress(ctx, rc, table.Name, entryCopy); err != nil {
		slog.WarnContext(ctx, "migration: raw-copy chunk completion state write failed; continuing",
			slog.String("table", table.Name),
			slog.Int("chunk", chunkIndex),
			slog.String("err", err.Error()))
	}

	chunkEnd := time.Now()
	slog.DebugContext(ctx, "adr0078: raw chunk done",
		slog.String("table", table.Name),
		slog.Int("chunk", chunkIndex),
		slog.Bool("raw_copy", true),
		slog.Int64("rows", rows),
		slog.Duration("chunk_wall", chunkEnd.Sub(chunkStart)))
	return nil
}

// firstPKBound projects the single-column PK bound out of a chunk's PK
// tuple. Returns nil for an empty/nil tuple (an open bound — chunk 0 has
// no lower, the last chunk has no upper).
func firstPKBound(pk []any) any {
	if len(pk) == 0 {
		return nil
	}
	return pk[0]
}

// cloneTableProgressForWrite returns a deep enough copy of one
// table's progress entry to be safe to JSON-encode concurrently with
// peer chunk goroutines mutating the original under stateMu.
// Specifically: the entry's Chunks slice is re-allocated so the
// encoder no longer shares slice backing storage with the chunk
// goroutines that write into [ir.TableChunkProgress] slots under the
// lock. This is the per-entry successor of the pre-ADR-0082
// cloneStateForWrite (which had to re-allocate the WHOLE map because
// the store re-encoded the whole map per checkpoint — O(N) clone +
// encode at the 10k-table scale; the per-table store made the entry
// the unit of persistence).
//
// Other reference-typed fields (the table-level LastPK,
// LowerPK/UpperPK/LastPK on each chunk) are not cloned: boundaries
// are written once during resolveChunks and per-chunk LastPK is
// replaced wholesale (not mutated in place) on every checkpoint, so
// swapping the slice header under the lock is enough to keep the
// encoder's view stable.
func cloneTableProgressForWrite(p ir.TableProgress) ir.TableProgress {
	if len(p.Chunks) > 0 {
		chunks := make([]ir.TableChunkProgress, len(p.Chunks))
		copy(chunks, p.Chunks)
		p.Chunks = chunks
	}
	return p
}

// isIntegerSinglePK reports whether table has exactly one PK column and
// that column is an integer type. Used to gate the raw-copy lane, whose
// chunk predicate inlines bare integer literals
// ([postgres.rawCopyChunkPredicate]) and is therefore safe ONLY for a
// single integer PK; non-integer / composite keyset chunks must take the
// IR fast loader, which pushes the chunk's upper bound into SQL in the
// column's native collation (ADR-0096 exactly-once).
func isIntegerSinglePK(table *ir.Table) bool {
	if table == nil || table.PrimaryKey == nil || len(table.PrimaryKey.Columns) != 1 {
		return false
	}
	col := migcore.LookupColumn(table, table.PrimaryKey.Columns[0].Column)
	if col == nil {
		return false
	}
	_, ok := col.Type.(ir.Integer)
	return ok
}
