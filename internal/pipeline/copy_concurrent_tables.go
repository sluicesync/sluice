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
		return runWorkStealingTableCopy(ctx, groups, byName, ws, rw, redactor, shard, fanoutDegree, needsIdempotent)
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

// runWorkStealingTableCopy is the roadmap-21a work-stealing variant of the
// concurrent table copy, used when the reader's N connections all share one
// consistent snapshot (native-MySQL FTWRL — [ir.WorkStealingCopyReader]). It
// flattens the disjoint partition into ONE ordered work list and runs W =
// min(N, len(tables)) pipelines that PULL tables from a shared atomic cursor,
// each reading its pulled table on its OWN reader index (connection). A fast
// pipeline naturally claims more tables, so the copy stays W-wide until fewer
// than W tables remain — closing the static partition's tail taper (the
// lighter groups would otherwise finish early and idle while a heavier group
// grinds its last large table).
//
// Correctness mirrors the static path's invariants and adds the claim:
//   - EXACTLY-ONCE: each table is claimed by exactly one pipeline — the atomic
//     fetch-add hands each index to one goroutine; coverage is total (every
//     index 0..len-1 is claimed before any goroutine sees claim >= len).
//   - ONE QUERY PER CONNECTION: reader index i is owned by exactly one
//     pipeline, which copies one table at a time, so each pinned connection has
//     at most one in-flight ReadRowsOn — the invariant the static partition
//     gave for free and [ir.WorkStealingCopyReader] requires.
//   - SEAM-SAFE: the native single recorded FTWRL position is independent of
//     WHICH connection read a table, so stealing does not affect the
//     snapshot→CDC handoff (ADR-0101 §6 / ADR-0007).
//   - LOUD ABORT / NO LEAKS: same errgroup semantics as the static path — the
//     first error cancels peers via the derived ctx; no position advances.
//
// needsIdempotent threads through to the same per-table helper the static path
// uses (false for the native binlog snapshot → plain-INSERT + ADR-0097 D-way
// fan-out, so total concurrency stays W × D).
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
) error {
	// Flatten the disjoint groups into one ordered work list. The partition is
	// a pure function of the sorted table set, so this order is deterministic
	// (stable across runs/resumes); order does not affect correctness here —
	// only complete coverage + the exactly-once claim do.
	var allTables []string
	for _, g := range groups {
		allTables = append(allTables, g...)
	}

	w := ws.ConcurrentReaderCount()
	if w > len(allTables) {
		// Never spawn more pipelines than tables (a pipeline with no table to
		// claim would idle immediately); also keeps every reader index in range.
		w = len(allTables)
	}

	var next atomic.Int64 // shared cursor into allTables; fetch-add to claim
	tg, tctx := errgroup.WithContext(ctx)
	for i := 0; i < w; i++ {
		readerIdx := i
		tg.Go(func() error {
			// This pipeline reads every table it claims on its OWN connection
			// (reader index readerIdx) via the pinned adapter, so the existing
			// per-table copy helpers (which call ReadRows) drive the
			// work-stealing read unchanged.
			src := pinnedRowReader{ws: ws, idx: readerIdx}
			for {
				claim := next.Add(1) - 1
				if claim >= int64(len(allTables)) {
					return nil // queue drained — this pipeline is done
				}
				name := allTables[claim]
				table, ok := byName[name]
				if !ok {
					return fmt.Errorf(
						"pipeline: concurrent copy (work-stealing): table %q is not in the migration schema "+
							"(engine surfaced a table the pipeline does not have — a partition/scope mismatch)",
						name,
					)
				}
				var cerr error
				if needsIdempotent {
					cerr = copyTableColdStartIdempotentMaybeParallel(tctx, src, rw, table, redactor, shard, fanoutDegree)
				} else {
					cerr = copyTablePlainMaybeParallel(tctx, src, rw, table, redactor, shard, fanoutDegree)
				}
				if cerr != nil {
					return wrapWithHint(PhaseBulkCopy, fmt.Errorf("pipeline: copy table %q: %w", name, cerr))
				}
			}
		})
	}
	return tg.Wait()
}

// pinnedRowReader adapts an [ir.WorkStealingCopyReader] to a plain
// [ir.RowReader] that reads EVERY table on a FIXED reader index, so a
// work-stealing pipeline can drive the existing per-table copy helpers (which
// take an ir.RowReader and call ReadRows) while reading on its own pinned
// connection. Err delegates to the underlying reader (shared across all
// connections); the native concurrent reader does not implement ir.RowCounter,
// so there is no ETA estimate to forward (parity with the static path).
type pinnedRowReader struct {
	ws  ir.WorkStealingCopyReader
	idx int
}

func (p pinnedRowReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	return p.ws.ReadRowsOn(ctx, table, p.idx)
}

func (p pinnedRowReader) Err() error { return p.ws.Err() }
