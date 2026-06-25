// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-table WRITE concurrency for the VStream cold-start copy (ADR-0100).
//
// ADR-0099 made the VStream cold-copy READ side concurrent: K independent
// vtgate VStreams, each over a DISJOINT subset of the in-scope tables, all
// filling one shared per-table row buffer. But the serial bulk-copy loop
// (runBulkCopyWithOpts) drains tables ONE AT A TIME, so only one table is
// ever WRITTEN at a time — the measured ~1.4× ceiling (the target
// PROCESSLIST showed exactly one table receiving rows over 28/28 polls).
//
// This file turns ADR-0099's K read streams into K end-to-end read→write
// PIPELINES: the engine surfaces the EXACT disjoint partition it gave the
// producers (ir.ConcurrentCopyPartitioner), and this driver runs one
// consumer goroutine per group, each looping its group's tables serially
// through the SAME per-table copy helper the serial loop uses
// (copyTableColdStartIdempotentMaybeParallel — so the ADR-0097 D-way write
// fan-out composes per table: W tables × D writers). Total write
// concurrency = W × D.
//
// Correctness invariants (silent-loss class — ADR-0100 §4/§5/§6):
//   - EXACTLY-ONCE: the partition is disjoint (ADR-0099, unit-pinned), so
//     each table is written by exactly one consumer — none dropped, none
//     double-written. The consumer reads the SAME groups the producers
//     used, so coverage/disjointness is inherited, never re-derived.
//   - POSITION-AFTER-ALL: this driver returns only after the W-way errgroup
//     joins (every table durably written); the engine records the stitched
//     CDC position only after all K producers join. The streamer reads
//     stream.Position strictly after this returns nil, so the global
//     position never advances past an un-written table (ADR-0007).
//   - LOUD ABORT: any consumer's error (or a reader Bug-68 stream error)
//     fails the whole copy via the errgroup; peers cancel; no position
//     advances.
//   - NO LEAKS on ctx-cancel: the errgroup's derived ctx cancels every
//     consumer goroutine deterministically.
//   - MID-COPY CHECKPOINT DISABLED: the durable-progress reporter is NOT
//     wired on this path (the caller skips it), and the engine pump records
//     no mid-COPY breadcrumb on the concurrent path — so a concurrent copy
//     persists no cursor that a resume could checkpoint past (ADR-0097 §3).

package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
)

// concurrentCopyDispatchObserver is a TEST-ONLY seam: when non-nil it
// fires with the cold-start concurrent-copy dispatch decision (the number
// of consumer groups, or 0 when the serial loop was taken) the moment
// [runConcurrentTableCopy] / the serial fallback chooses. It lets the
// unit tests assert WHICH path was taken without inferring it from timing.
// nil in production (a single nil check). Mirrors the
// coldStartDispatchObserver / onTableCopiedObserver disposition.
var concurrentCopyDispatchObserver func(groups int)

// intraTableChunkObserver is a TEST-ONLY seam (nil in production — a single nil
// check) that fires once per table during work-item construction with the
// number of work items that table was split into: 1 = a whole-table item (not
// chunked), M ≥ 2 = M intra-table PK-range chunks (ADR-0119, roadmap 21b). It
// lets the integration test assert that the skewed corpus's huge table was
// chunked (tail reclaimed) WITHOUT inferring it from timing. Mirrors the
// [concurrentCopyDispatchObserver] disposition.
var intraTableChunkObserver func(table string, chunks int)

// concurrentCopyGroups returns the engine-surfaced disjoint table
// partition the cold-start bulk copy may write CONCURRENTLY (ADR-0100),
// or nil when no cross-table write concurrency is engaged. It type-asserts
// the reader on [ir.ConcurrentCopyPartitioner] (the VStream concurrent
// cold-copy reader implements it; PG / vanilla MySQL / single-stream
// VStream do not) and returns its groups only when there are ≥2 of them —
// a single group is the serial path and is treated as "no concurrency" so
// the caller takes the byte-identical serial loop.
//
// Pure aside from the type-assert; safe to call before any goroutine
// spawns.
func concurrentCopyGroups(rows ir.RowReader) [][]string {
	p, ok := rows.(ir.ConcurrentCopyPartitioner)
	if !ok {
		return nil
	}
	groups := p.ConcurrentCopyGroups()
	if len(groups) < 2 {
		// nil, or a single group: no cross-table write concurrency. The
		// caller runs the serial table loop, byte-identical to today.
		return nil
	}
	return groups
}

// runConcurrentTableCopy copies schema.Tables through W = len(groups)
// CONCURRENT consumer pipelines (ADR-0100), one per disjoint group, each
// looping its group's tables serially through the per-table copy helper (so
// the ADR-0097 D-way write fan-out composes per table). It is the write-side
// companion to ADR-0099's K concurrent producer streams (W = K): each group's
// producer fills its tables' queues, each group's consumer (here)
// drains+writes them.
//
// needsIdempotent selects the per-table write path, EXACTLY mirroring the
// serial loop's dispatch:
//   - true  → [copyTableColdStartIdempotentMaybeParallel] (the upsert path —
//     the VStream COPY re-emits rows, Bug 125, ADR-0099/0100).
//   - false → [copyTablePlainMaybeParallel] (plain INSERT with the ADR-0097
//     D-way write fan-out — the native-MySQL binlog snapshot, ADR-0101/0102:
//     each table is read EXACTLY ONCE from a frozen REPEATABLE-READ view,
//     gap-free + overlap-free, so no upsert is needed and the disjoint
//     partition means each table is plain-INSERTed by exactly one pipeline;
//     within that pipeline the table's rows fan across D plain-INSERT workers
//     → W × D).
//
// The two readers that surface a concurrent partition are mutually exclusive
// on this axis (VStream is always idempotent; native binlog is never), so
// needsIdempotent is constant across a run.
//
// It returns only after the W-way errgroup joins — so when it returns nil,
// EVERY table in EVERY group is fully and durably written (the write
// barrier the streamer's post-copy position read depends on, ADR-0007). The
// first consumer error cancels the derived ctx so peers unwind, and that
// error is returned (loud abort, no partial silent success, no position
// advance).
//
// The schema-apply phases (CreateTables before, indexes/constraints/views
// after) stay in the caller's serial flow — only the per-table data sweep
// is parallelised across groups, exactly mirroring the cross-table pool
// (ADR-0076) on the migrate path.
//
// fanoutDegree is the resolved ADR-0097 per-table write fan-out degree,
// threaded into each per-table copy so W × D composes. The single shared
// writer rw is concurrency-safe for W × D callers: the MySQL RowWriter
// holds a *sql.DB pool, so each fan-out worker pins its own pooled
// connection; the mid-COPY durable watermark is never wired on this path
// (the caller skips it) and the fan-out path passes reportDurable=false, so
// no consumer touches the watermark concurrently.
func runConcurrentTableCopy(
	ctx context.Context,
	groups [][]string,
	schema *ir.Schema,
	rows ir.RowReader,
	rw ir.RowWriter,
	redactor *redact.Registry,
	shard ShardColumnSpec,
	fanoutDegree int,
	needsIdempotent bool,
	noIntraTableStealing bool,
) error {
	if concurrentCopyDispatchObserver != nil {
		concurrentCopyDispatchObserver(len(groups))
	}

	// Index the schema's tables by unqualified name so each group can
	// resolve its table names to the *ir.Table the per-table copy needs.
	// The partition names come from the same in-scope table set the schema
	// carries, so every group name MUST resolve — a miss is a programming
	// error (the engine surfaced a table the pipeline's schema doesn't
	// have) and is surfaced LOUDLY rather than silently skipped (which
	// would be a silently un-copied table — the worst silent-loss class).
	byName := make(map[string]*ir.Table, len(schema.Tables))
	for _, t := range schema.Tables {
		byName[t.Name] = t
	}

	// Work-stealing path (roadmap item 21a). When the reader's N connections
	// ALL observe the same consistent snapshot (native-MySQL FTWRL multi-
	// snapshot — ir.WorkStealingCopyReader), ANY connection can read ANY table,
	// so the static per-group drain is replaced with N pipelines pulling tables
	// from a SHARED queue, each reading on its OWN connection. This keeps the
	// copy N-wide to the tail instead of tapering as the lighter static groups
	// finish early and idle (the live Track-B/D tail observation). The VStream
	// reader does NOT implement the surface — each of its K streams is Match-
	// scoped to its group at the source, so a stealing consumer would have no
	// rows for an out-of-group table — so it stays on the static partition below.
	if ws, ok := rows.(ir.WorkStealingCopyReader); ok && ws.ConcurrentReaderCount() > 1 {
		return runWorkStealingTableCopy(ctx, groups, byName, ws, rw, redactor, shard, fanoutDegree, needsIdempotent, noIntraTableStealing)
	}

	tg, tctx := errgroup.WithContext(ctx)
	for _, group := range groups {
		group := group
		tg.Go(func() error {
			// One consumer pipeline: drain+write this group's tables
			// serially (its paired producer stream fills exactly these
			// tables' queues). Within a group the tables are written one at
			// a time — the cross-table concurrency is BETWEEN groups, so the
			// per-stream byte sub-budget (ADR-0099 §2, one consumer per
			// producer) stays correct.
			for _, name := range group {
				table, ok := byName[name]
				if !ok {
					return fmt.Errorf(
						"pipeline: concurrent copy: group table %q is not in the migration schema "+
							"(engine surfaced a table the pipeline does not have — a partition/scope mismatch)",
						name,
					)
				}
				var cerr error
				if needsIdempotent {
					cerr = copyTableColdStartIdempotentMaybeParallel(tctx, rows, rw, table, redactor, shard, fanoutDegree)
				} else {
					// Native-MySQL gap-free snapshot (ADR-0101/0102): plain
					// INSERT with the SAME ADR-0097 D-way write fan-out the
					// idempotent path uses, so each of the W group pipelines
					// fans its active table across D plain-INSERT workers →
					// W × D. Reuses partitionRowsByPK verbatim; degree==1 or a
					// no-PK table falls back to the single-writer copyTable.
					cerr = copyTablePlainMaybeParallel(tctx, rows, rw, table, redactor, shard, fanoutDegree)
				}
				if cerr != nil {
					return wrapWithHint(PhaseBulkCopy, fmt.Errorf("pipeline: copy table %q: %w", name, cerr))
				}
			}
			return nil
		})
	}
	return tg.Wait()
}

// maxChunksPerTable caps how many intra-table PK-range chunks one table is
// split into (ADR-0119 Decision 1). The split reclaims the tail; past a point
// extra chunks only add per-chunk overhead (a boundary tuple, a claim, a SQL
// page) without widening the copy beyond the N pinned readers. 64 is generous
// (a table N× the threshold still chunks fully up to the cap) while bounding the
// work list.
const maxChunksPerTable = 64

// copyWorkItem is the unit of stealable work the work-stealing copy claims off
// the shared cursor (ADR-0119 Decision 1). A whole-table item (chunkIndex < 0,
// both bounds nil) is byte-identical to tier (a); a chunk item carries the
// half-open (lowerPK, upperPK] bounds the engine reads within. table is the
// resolved *ir.Table (resolution happens at build time so a partition/scope
// mismatch fails loudly before any goroutine spawns).
type copyWorkItem struct {
	table      *ir.Table
	chunkIndex int // < 0 ⇒ whole table
	lowerPK    []any
	upperPK    []any
}

// runWorkStealingTableCopy is the work-stealing variant of the concurrent table
// copy, used when the reader's N connections all share one consistent snapshot
// (native-MySQL FTWRL — [ir.WorkStealingCopyReader]). It flattens the disjoint
// partition into ONE ordered list of WORK ITEMS and runs W = min(N, len(items))
// pipelines that PULL items from a shared atomic cursor, each reading its pulled
// item on its OWN reader index (connection). A fast pipeline naturally claims
// more items, so the copy stays W-wide until fewer than W items remain.
//
// Tier (a) (roadmap 21a) made the item a whole table, closing most of the
// static-partition taper. Tier (b) (roadmap 21b, ADR-0119) makes the item a
// (table, PK-range) CHUNK for large, chunk-eligible tables when the reader also
// implements [ir.ChunkedWorkStealingCopyReader] — so the tail is bounded by a
// chunk of the last big table, not the whole table. noIntraTableStealing forces
// every table back to one whole-table item (the tier-(a) behaviour) for
// operators who want it; the Go zero value (false) keeps intra-table stealing
// ON (the common default — every test/non-CLI caller gets stealing, the
// v0.99.51 opt-out-naming trap).
//
// Correctness mirrors the static path's invariants and adds the claim + tiling:
//   - EXACTLY-ONCE: each work item is claimed by exactly one pipeline — the
//     atomic fetch-add hands each index to one goroutine; coverage is total
//     (every index 0..len-1 claimed before any goroutine sees claim >= len).
//     The M chunks of a table tile its rows with no gap and no overlap (the
//     half-open bounds + the collation-correct SQL upper clip, Bug-74), and a
//     table is read EITHER whole OR as chunks, never both — so every source row
//     reaches the target exactly once.
//   - ONE QUERY PER CONNECTION: reader index i is owned by exactly one
//     pipeline, which copies one item at a time, so each pinned connection has
//     at most one in-flight read — the invariant [ir.WorkStealingCopyReader]
//     requires (unchanged by chunking).
//   - SEAM-SAFE: the native single recorded FTWRL position is independent of
//     WHICH connection read WHICH range, so stealing does not affect the
//     snapshot→CDC handoff (ADR-0101 §6 / ADR-0007 / ADR-0119 Decision 5).
//   - LOUD ABORT / NO LEAKS: same errgroup semantics as the static path — the
//     first error cancels peers via the derived ctx; no position advances.
//
// needsIdempotent threads through to the same per-item helper the static path
// uses (false for the native binlog snapshot → plain-INSERT + ADR-0097 D-way
// fan-out, so total concurrency stays W × D; disjoint chunks carry disjoint
// rows, so plain-INSERT per chunk stays exactly-once).
func runWorkStealingTableCopy(
	ctx context.Context,
	groups [][]string,
	byName map[string]*ir.Table,
	ws ir.WorkStealingCopyReader,
	rw ir.RowWriter,
	redactor *redact.Registry,
	shard ShardColumnSpec,
	fanoutDegree int,
	needsIdempotent bool,
	noIntraTableStealing bool,
) error {
	// Flatten the disjoint groups into one ordered table list. The partition is
	// a pure function of the sorted table set, so this order is deterministic
	// (stable across runs/resumes); order does not affect correctness here —
	// only complete coverage + the exactly-once claim do.
	var allTables []string
	for _, g := range groups {
		allTables = append(allTables, g...)
	}

	// Build the work list: one whole-table item per table, OR M chunk items for
	// a large, chunk-eligible table (when the reader is chunk-capable and
	// stealing is not opted out). Boundary computation runs on the reader's side
	// metadata pool BEFORE any pipeline spawns, so it cannot race a pinned read.
	items, err := buildCopyWorkItems(ctx, allTables, byName, ws, noIntraTableStealing)
	if err != nil {
		return err
	}

	w := ws.ConcurrentReaderCount()
	if w > len(items) {
		// Never spawn more pipelines than work items (a pipeline with no item to
		// claim would idle immediately); also keeps every reader index in range.
		w = len(items)
	}

	// cws is the chunked surface, non-nil only when the reader implements it;
	// chunk items exist only in that case, so the in-loop assertion is safe.
	cws, _ := ws.(ir.ChunkedWorkStealingCopyReader)

	var next atomic.Int64 // shared cursor into items; fetch-add to claim
	tg, tctx := errgroup.WithContext(ctx)
	for i := 0; i < w; i++ {
		readerIdx := i
		tg.Go(func() error {
			for {
				claim := next.Add(1) - 1
				if claim >= int64(len(items)) {
					return nil // queue drained — this pipeline is done
				}
				item := items[claim]
				// A whole-table item reads via the plain pinned adapter; a chunk
				// item reads its PK-range via the range adapter. Either way the
				// existing per-item copy helpers (which call ReadRows) drive the
				// work-stealing read unchanged, each on this pipeline's OWN
				// connection (reader index readerIdx).
				var src ir.RowReader
				if item.chunkIndex < 0 {
					src = pinnedRowReader{ws: ws, idx: readerIdx}
				} else {
					src = pinnedRangeRowReader{
						ws:         cws,
						idx:        readerIdx,
						chunkIndex: item.chunkIndex,
						lowerPK:    item.lowerPK,
						upperPK:    item.upperPK,
					}
				}
				var cerr error
				if needsIdempotent {
					cerr = copyTableColdStartIdempotentMaybeParallel(tctx, src, rw, item.table, redactor, shard, fanoutDegree)
				} else {
					cerr = copyTablePlainMaybeParallel(tctx, src, rw, item.table, redactor, shard, fanoutDegree)
				}
				if cerr != nil {
					return wrapWithHint(PhaseBulkCopy, fmt.Errorf("pipeline: copy table %q (chunk %d): %w", item.table.Name, item.chunkIndex, cerr))
				}
			}
		})
	}
	return tg.Wait()
}

// buildCopyWorkItems turns the flattened table list into the work list the
// work-stealing copy claims (ADR-0119 Decision 1). For each table it emits one
// whole-table item, OR M ≥ 2 chunk items when ALL of: the reader is
// chunk-capable ([ir.ChunkedWorkStealingCopyReader]); intra-table stealing is
// not opted out; the table is chunk-eligible ([canParallelChunkTable] — an
// orderable, non-sluice-injected single/composite PK, so keyless / no-PK tables
// stay whole and the keyless at-least-once contract is unchanged); and its
// CountRows estimate clears the within-table threshold.
//
// A table name absent from the schema is a partition/scope mismatch (the engine
// surfaced a table the pipeline does not have) and is refused LOUDLY here,
// before any goroutine spawns — the same silent-loss-class guard the per-table
// loop had, moved earlier. Any failure to COMPUTE boundaries (a boundary-query
// error, or a degenerate single-chunk result) is NOT fatal — boundaries are a
// partition hint (ADR-0119 Decision 2), so the table falls back to one
// whole-table item (logged at debug), never failing the copy.
func buildCopyWorkItems(
	ctx context.Context,
	allTables []string,
	byName map[string]*ir.Table,
	ws ir.WorkStealingCopyReader,
	noIntraTableStealing bool,
) ([]copyWorkItem, error) {
	cws, chunkable := ws.(ir.ChunkedWorkStealingCopyReader)
	rc, _ := ws.(ir.RowCounter)
	// The within-table threshold adapts to the table count exactly as the
	// migrate path's parallel chunker does (0 = the auto sentinel).
	threshold := resolveBulkParallelMinRows(0, len(allTables))

	var items []copyWorkItem
	for _, name := range allTables {
		table, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf(
				"pipeline: concurrent copy (work-stealing): table %q is not in the migration schema "+
					"(engine surfaced a table the pipeline does not have — a partition/scope mismatch)",
				name,
			)
		}
		tableItems := chunkItemsFor(ctx, table, cws, chunkable, rc, threshold, noIntraTableStealing)
		if intraTableChunkObserver != nil {
			intraTableChunkObserver(table.Name, len(tableItems))
		}
		items = append(items, tableItems...)
	}
	return items, nil
}

// chunkItemsFor returns the work items for one table: a single whole-table item
// (chunkIndex < 0), or M ≥ 2 chunk items when the table is large + chunk-eligible
// + the reader is chunk-capable + stealing is not opted out. It never errors:
// any reason not to chunk (or any boundary-compute failure) yields the
// whole-table item, because the chunk split is a throughput hint, never a
// correctness gate (ADR-0119 Decision 2).
func chunkItemsFor(
	ctx context.Context,
	table *ir.Table,
	cws ir.ChunkedWorkStealingCopyReader,
	chunkable bool,
	rc ir.RowCounter,
	threshold int64,
	noIntraTableStealing bool,
) []copyWorkItem {
	whole := []copyWorkItem{{table: table, chunkIndex: -1}}
	if noIntraTableStealing || !chunkable {
		return whole
	}
	// Eligibility: an orderable single/composite PK (keyless / sluice-injected /
	// non-orderable → whole, so the keyless at-least-once contract is unchanged).
	// Pass parallelism 2 — canParallelChunkTable only gates parallelism <= 1, and
	// the work-stealing reader always has N > 1 connections.
	eligible, strategy, _ := canParallelChunkTable(table, 2)
	if !eligible {
		return whole
	}
	// Large gate: only chunk past the within-table threshold (per-chunk overhead
	// dominates below it). A reader without a RowCounter estimate (est 0) stays
	// whole.
	var est int64
	if rc != nil {
		est, _ = rc.CountRows(ctx, table)
	}
	if est < threshold {
		return whole
	}
	// M = clamp(ceil(est/threshold), 2, maxChunksPerTable). threshold > 0
	// (resolveBulkParallelMinRows floors it), so the ceil-divide is safe.
	m := int((est + threshold - 1) / threshold)
	if m < 2 {
		m = 2
	}
	if m > maxChunksPerTable {
		m = maxChunksPerTable
	}

	// Compute boundaries on the side metadata pool via the SAME functions the
	// migrate parallel copy pins (ADR-0019 integer MIN/MAX/divide, ADR-0096
	// keyset), picking by the strategy canParallelChunkTable returned.
	var (
		boundaries []chunkBoundary
		berr       error
	)
	switch strategy {
	case strategyMinMaxDivide:
		boundaries, berr = computeChunkBoundaries(ctx, cws, table, m)
	case strategyKeysetSample:
		boundaries, berr = computeKeysetChunkBoundaries(ctx, cws, table, m)
	default:
		return whole
	}
	if berr != nil || len(boundaries) < 2 {
		// Boundaries are a HINT — a compute error or a degenerate single-chunk
		// result (empty/tiny table, too-few distinct keys) collapses to the
		// whole-table item rather than failing the copy.
		slog.DebugContext(ctx,
			"pipeline: concurrent copy (work-stealing): intra-table chunking fell back to a whole-table copy",
			slog.String("table", table.Name),
			slog.Int("requested_chunks", m),
			slog.Int("computed_boundaries", len(boundaries)),
			slog.Any("error", berr))
		return whole
	}

	items := make([]copyWorkItem, 0, len(boundaries))
	for _, b := range boundaries {
		items = append(items, copyWorkItem{
			table:      table,
			chunkIndex: b.chunkIndex,
			lowerPK:    b.lowerPK,
			upperPK:    b.upperPK,
		})
	}
	return items
}

// pinnedRowReader adapts an [ir.WorkStealingCopyReader] to a plain
// [ir.RowReader] that reads EVERY table on a FIXED reader index, so a
// work-stealing pipeline can drive the existing per-table copy helpers (which
// take an ir.RowReader and call ReadRows) while reading on its own pinned
// connection. Err delegates to the underlying reader (shared across all
// connections); CountRows forwards the underlying reader's row-count estimate
// (the native concurrent reader implements ir.RowCounter via a side metadata
// pool — collision-free with the pinned reads — so the work-stealing pipeline's
// progress ticker gets a per-table ETA).
type pinnedRowReader struct {
	ws  ir.WorkStealingCopyReader
	idx int
}

// Compile-time guarantee the adapter forwards the row-count surface so
// kickOffRowCount's ir.RowCounter assertion succeeds on the work-stealing path.
var _ ir.RowCounter = pinnedRowReader{}

func (p pinnedRowReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	return p.ws.ReadRowsOn(ctx, table, p.idx)
}

func (p pinnedRowReader) Err() error { return p.ws.Err() }

// CountRows forwards the underlying reader's per-table row-count ESTIMATE so the
// progress ticker can show a %/ETA on the work-stealing path. The native
// concurrent reader runs it on a side metadata pool (not the pinned snapshot
// connections), so it never collides with this pipeline's active read. A reader
// that doesn't implement ir.RowCounter yields (0, nil) — graceful no-ETA.
func (p pinnedRowReader) CountRows(ctx context.Context, table *ir.Table) (int64, error) {
	if rc, ok := p.ws.(ir.RowCounter); ok {
		return rc.CountRows(ctx, table)
	}
	return 0, nil
}

// pinnedRangeRowReader adapts an [ir.ChunkedWorkStealingCopyReader] to a plain
// [ir.RowReader] that reads ONE intra-table PK-range chunk on a FIXED reader
// index (ADR-0119, roadmap 21b). A work-stealing pipeline that claims a chunk
// item drives the EXISTING per-table copy helpers (which take an ir.RowReader
// and call ReadRows) through this adapter, so the chunk read needs no change to
// those helpers — ReadRows forwards the chunk's bounds + index to
// ReadRowsRangeOn. Err delegates to the underlying shared reader.
//
// It deliberately does NOT implement [ir.RowCounter]: the whole-table estimate
// would overstate a single chunk (and several chunks of one table would each
// report the full count), so the per-chunk progress shows rows-copied without a
// %/ETA — an honest "no estimate" rather than a wrong one. The whole-table
// items still get an ETA via [pinnedRowReader].
type pinnedRangeRowReader struct {
	ws         ir.ChunkedWorkStealingCopyReader
	idx        int
	chunkIndex int
	lowerPK    []any
	upperPK    []any
}

func (p pinnedRangeRowReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	return p.ws.ReadRowsRangeOn(ctx, table, p.lowerPK, p.upperPK, p.chunkIndex, p.idx)
}

func (p pinnedRangeRowReader) Err() error { return p.ws.Err() }
