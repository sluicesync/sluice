// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// WRITE-side parallel fan-out for the idempotent VStream/CDC snapshot
// cold-start copy (ADR-0097).
//
// The PlanetScale-MySQL snapshot writer falls back to a single
// cross-region-RTT-bound batched-INSERT connection (vtgate blocks LOAD
// DATA LOCAL INFILE), and the READ side cannot be PK-range-chunked (a
// single un-splittable vtgate stream). The only lever is WRITE-side
// fan-out: one reader goroutine PK-hash-partitions the single incoming
// snapshot row stream to N per-worker channels, and the engine's
// [ir.ParallelIdempotentCopyWriter] runs N idempotent batched-INSERT
// workers, each on its own pinned connection.
//
// Correctness invariants (silent-loss class — see ADR-0097 §2/§3/§7):
//   - EXACTLY-ONCE routing: every row is sent to exactly one worker
//     channel (no drop, no dup). PK-hash (not round-robin) pins every
//     emission of a given PK to the SAME worker, so Bug-125 COPY
//     re-emissions of a PK serialize within one worker's batch stream
//     and can never race two concurrent upserts on the same row.
//   - FLUSH-BEFORE-POSITION: the writer's WriteRowsIdempotentParallel
//     returns only after every worker has durably committed; the
//     orchestrator advances no position until this whole function
//     returns nil (ADR-0007).
//   - LOUD ABORT: any worker error (or the reader's Bug-68 stream
//     error) fails the copy; no position is advanced.
//   - NO LEAKS on ctx-cancel: the child ctx + defer-close shape unwinds
//     the reader and every worker deterministically.

import (
	"context"
	"fmt"
	"hash/fnv"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/redact"
)

const (
	// defaultCopyFanoutDegree is the conservative default WRITE-side
	// fan-out degree. The live experiment (ADR-0097 Context) showed 3–4
	// concurrent INSERT streams already beat PG's single-stream COPY
	// with near-linear scaling and no target ingest ceiling; 4 is the
	// safe default. NOT aggressive — operators raise it explicitly for a
	// known-large cross-region copy, bounded by the connection budget.
	defaultCopyFanoutDegree = 4

	// maxCopyFanoutDegree caps an operator-supplied degree so a typo
	// can't request thousands of connections against a capped target.
	maxCopyFanoutDegree = 64
)

// resolveCopyFanoutDegree maps a raw degree field to the effective
// degree, ZERO-VALUE-SAFE for every constructor (the v0.99.51 trap):
//
//	n <= 0 → defaultCopyFanoutDegree  (the Go zero value is the safe,
//	                                   common default — NEVER "0 workers")
//	n == 1 → 1                        (serial; the no-fan-out path)
//	n  > 1 → min(n, maxCopyFanoutDegree)
//
// There is no input that resolves to "zero workers / copies nothing".
func resolveCopyFanoutDegree(n int) int {
	switch {
	case n <= 0:
		return defaultCopyFanoutDegree
	case n > maxCopyFanoutDegree:
		return maxCopyFanoutDegree
	default:
		return n
	}
}

// copyTablePlainMaybeParallel routes a cold-start PLAIN-INSERT table copy
// through the WRITE-side fan-out (ADR-0102) when it is both eligible and
// beneficial, and through the serial single-writer [copyTable] otherwise.
// It is the plain-INSERT mirror of
// [copyTableColdStartIdempotentMaybeParallel]: it reuses the SAME ADR-0097
// PK-hash partition ([partitionRowsByPK]) — only the per-worker write call
// differs (plain INSERT, not upsert). Eligibility (all required):
//
//   - degree > 1 (a degree of 1 is serial by definition);
//   - the writer implements [ir.ParallelCopyWriter];
//   - the table has a usable PRIMARY KEY (the partition key). A no-PK table
//     CANNOT be PK-hash-partitioned, so it routes to the single-writer
//     serial copy — fully copied, never refused (a gap-free plain-INSERT
//     snapshot has no re-emission to duplicate, unlike the idempotent case)
//     and never partially fanned out.
//
// Falling through to serial is always correct (same plain writer, same
// loud-failure gate via [copyTable]) — it just doesn't get the speedup.
//
// Plain INSERT (not upsert) is correct here because the only reader that
// drives this is the native-MySQL gap-free concurrent snapshot (ADR-0101)
// onto a FRESH target: each row is read exactly once, the disjoint partition
// means each table is owned by one pipeline, and the PK-hash routing means
// each row is written by exactly one worker — no overlap, nothing to absorb.
func copyTablePlainMaybeParallel(
	ctx context.Context,
	rr ir.RowReader,
	rw ir.RowWriter,
	table *ir.Table,
	redactor *redact.Registry,
	shard ShardColumnSpec,
	degree int,
) error {
	par, ok := rw.(ir.ParallelCopyWriter)
	if !ok || degree <= 1 || len(migcore.TablePKColumns(table)) == 0 {
		return copyTable(ctx, rr, rw, table, redactor, shard)
	}
	return copyTablePlainParallel(ctx, rr, par, table, redactor, shard, degree)
}

// copyTablePlainParallel is the fan-out variant of [copyTable]. It mirrors
// [copyTableColdStartIdempotentParallel]'s goroutine lifecycle, redaction,
// shard-stamping, and the Bug-68 loud-failure gate EXACTLY — only the writer
// call differs: instead of one WriteRows over the single channel, it spins
// up the SAME ADR-0097 PK-hash dispatcher over N per-worker channels and
// calls [ir.ParallelCopyWriter.WriteRowsParallel] (plain INSERT, not upsert).
//
// There is no mid-COPY durable watermark to disable here: the plain path
// carries no CopyDurableProgressReporter wiring, and the native concurrent
// path that drives this sets no CopyDurableProgressSink (ADR-0101 §7 /
// ADR-0102 §5). The whole-table join inside WriteRowsParallel is the SOLE
// durability guarantee, and the orchestrator advances no position until this
// returns nil (ADR-0007).
func copyTablePlainParallel(
	ctx context.Context,
	rr ir.RowReader,
	pw ir.ParallelCopyWriter,
	table *ir.Table,
	redactor *redact.Registry,
	shard ShardColumnSpec,
	degree int,
) (retErr error) {
	copyCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	rows, err := rr.ReadRows(copyCtx, table)
	if err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	pt := newProgressTicker(copyCtx, progressInterval, table.Name)
	kickOffRowCount(copyCtx, rr, table, pt)
	defer func() { pt.Stop(ctx, retErr) }()

	teed := teeRows(copyCtx, rows, pt.observeRow)
	redacted, redactErrFn := redactRows(copyCtx, teed, redactor, table.Schema, table.Name, table.Columns, migcore.TablePKColumns(table), "")
	stamped, _ := shardStampRows(copyCtx, redacted, shard.Name, shard.Value)

	// Partition the single (redacted, stamped) stream out to `degree`
	// per-worker channels, hashed by PK so every row lands on exactly one
	// worker — the SAME dispatcher the idempotent fan-out uses. The
	// dispatcher closes all worker channels when the source drains (or
	// copyCtx cancels), so each worker sees a clean close.
	workers := partitionRowsByPK(copyCtx, stamped, table, degree)

	if err := pw.WriteRowsParallel(copyCtx, table, workers); err != nil {
		return fmt.Errorf("write rows (plain, fan-out): %w", err)
	}
	if err := redactErrFn(); err != nil {
		return fmt.Errorf("redact rows: %w", err)
	}
	// The writers returned without error, but the reader may have aborted
	// mid-table on a scan/decode failure (Bug 68). Surface it loudly so a
	// silently-truncated table never reports success.
	return migcore.ReaderStreamErr(rr, table)
}

// copyTableColdStartIdempotentMaybeParallel routes a cold-start
// idempotent table copy through the WRITE-side fan-out when it is both
// eligible and beneficial, and through the serial
// [copyTableColdStartIdempotent] otherwise. Eligibility (all required):
//
//   - degree > 1 (a degree of 1 is serial by definition);
//   - the writer implements [ir.ParallelIdempotentCopyWriter];
//   - the table has a usable PRIMARY KEY (the partition key). A no-PK
//     table (the Bug-125 unique-key-upsert case) routes serial — the
//     fan-out is a pure additive optimization for PK'd tables and never
//     weakens the no-PK loud-refusal contract.
//
// Falling through to serial is always correct (same idempotent writer,
// same loud-failure gate) — it just doesn't get the speedup.
func copyTableColdStartIdempotentMaybeParallel(
	ctx context.Context,
	rr ir.RowReader,
	rw ir.RowWriter,
	table *ir.Table,
	redactor *redact.Registry,
	shard ShardColumnSpec,
	degree int,
) error {
	par, ok := rw.(ir.ParallelIdempotentCopyWriter)
	if !ok || degree <= 1 || len(migcore.TablePKColumns(table)) == 0 {
		return copyTableColdStartIdempotent(ctx, rr, rw, table, redactor, shard)
	}
	return copyTableColdStartIdempotentParallel(ctx, rr, par, table, redactor, shard, degree)
}

// copyTableColdStartIdempotentParallel is the fan-out variant of
// [copyTableColdStartIdempotent]. It mirrors that function's goroutine
// lifecycle, redaction, shard-stamping, and the Bug-68 loud-failure
// gate exactly — only the writer call differs: instead of one
// WriteRowsIdempotent over the single channel, it spins up the
// PK-hash dispatcher over N per-worker channels and calls
// WriteRowsIdempotentParallel.
//
// The mid-COPY durable-write watermark (ADR-0072 Phase B / v0.99.9) is
// DISABLED for the duration of a fan-out copy — not by this function, but
// inside the writer: [ir.ParallelIdempotentCopyWriter]'s
// WriteRowsIdempotentParallel runs every worker with durable-progress
// reporting suppressed (the MySQL writer passes reportDurable=false). It
// MUST be disabled, not merely unconsumed: under fan-out the flat
// durable-flushed-row count is NOT order-equivalent to the snapshot
// reader's enqueue-order breadcrumb frontier (rows flush in per-worker
// order with independent batch buffers), so a mid-COPY breadcrumb could be
// checkpointed past an early-enqueued row that a lagging worker has not yet
// flushed — a hard crash after that checkpoint would resume PAST the
// un-flushed row (silent loss; ADR-0097 §3). The SOLE durability guarantee
// for a fanned-out table is therefore the whole-table join:
// WriteRowsIdempotentParallel returns only after every worker has durably
// committed, and the orchestrator advances no position (and persists the
// final COPY_COMPLETED position) until this function returns nil (ADR-0007).
// Resume never fans out (ADR-0095 single-stream v1), so the disabled
// mid-COPY cursor is also one no resume path would consume.
func copyTableColdStartIdempotentParallel(
	ctx context.Context,
	rr ir.RowReader,
	pw ir.ParallelIdempotentCopyWriter,
	table *ir.Table,
	redactor *redact.Registry,
	shard ShardColumnSpec,
	degree int,
) (retErr error) {
	copyCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	rows, err := rr.ReadRows(copyCtx, table)
	if err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	pt := newProgressTicker(copyCtx, progressInterval, table.Name)
	kickOffRowCount(copyCtx, rr, table, pt)
	defer func() { pt.Stop(ctx, retErr) }()

	teed := teeRows(copyCtx, rows, pt.observeRow)
	redacted, redactErrFn := redactRows(copyCtx, teed, redactor, table.Schema, table.Name, table.Columns, migcore.TablePKColumns(table), "")
	stamped, _ := shardStampRows(copyCtx, redacted, shard.Name, shard.Value)

	// Partition the single (redacted, stamped) stream out to `degree`
	// per-worker channels, hashed by PK so every emission of a PK lands
	// on the same worker. The dispatcher closes all worker channels when
	// the source drains (or copyCtx cancels), so each worker sees a
	// clean close.
	workers := partitionRowsByPK(copyCtx, stamped, table, degree)

	if err := pw.WriteRowsIdempotentParallel(copyCtx, table, workers); err != nil {
		return fmt.Errorf("write rows (idempotent, fan-out): %w", err)
	}
	if err := redactErrFn(); err != nil {
		return fmt.Errorf("redact rows: %w", err)
	}
	// The writers returned without error, but the reader may have
	// aborted mid-table on a scan/decode failure (Bug 68). Surface it
	// loudly so a silently-truncated table never reports success.
	return migcore.ReaderStreamErr(rr, table)
}

// partitionRowsByPK launches one dispatcher goroutine that reads every
// row from src and routes it to EXACTLY ONE of `degree` per-worker
// channels by hashing the row's PK column values. It returns the N
// receive-only worker channels (as the []<-chan ir.Row the writer
// expects).
//
// Routing properties (the load-bearing exactly-once invariant):
//   - Each row is sent to exactly one channel — no drop, no dup, no
//     fan-to-many.
//   - The same PK always hashes to the same worker, so Bug-125 COPY
//     re-emissions of a PK serialize on one worker (never two
//     concurrent upserts on the same row).
//   - On ctx cancel the dispatcher stops and closes every channel in a
//     defer, so all workers unwind (no goroutine leak); a worker that
//     errored cancelled the shared ctx, which the dispatcher's selects
//     observe.
func partitionRowsByPK(ctx context.Context, src <-chan ir.Row, table *ir.Table, degree int) []<-chan ir.Row {
	pkCols := migcore.TablePKColumns(table)
	chans := make([]chan ir.Row, degree)
	out := make([]<-chan ir.Row, degree)
	for i := range chans {
		chans[i] = make(chan ir.Row, migcore.RowChanBuffer)
		out[i] = chans[i]
	}

	go func() {
		// Closing every worker channel on exit (drain, cancel, or a
		// worker-driven ctx cancel) lets each worker's loop observe a
		// clean close and return — the deterministic-shutdown guarantee.
		defer func() {
			for _, ch := range chans {
				close(ch)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case row, ok := <-src:
				if !ok {
					return
				}
				idx := pkWorkerIndex(row, pkCols, degree)
				select {
				case <-ctx.Done():
					return
				case chans[idx] <- row:
				}
			}
		}
	}()

	return out
}

// pkWorkerIndex returns the worker index in [0, degree) for a row,
// derived from an FNV-1a hash over its PK column values. The same PK
// value set always maps to the same worker within a run (which is all
// the partition needs — re-emissions carry the same PK value). degree
// is guaranteed > 0 by the caller (resolveCopyFanoutDegree). A row
// missing a PK column hashes its nil verbatim — deterministic, and the
// no-usable-PK table case never reaches here (it routes serial).
func pkWorkerIndex(row ir.Row, pkCols []string, degree int) int {
	h := fnv.New64a()
	for _, c := range pkCols {
		// A NUL separator between columns prevents composite-PK
		// concatenation ambiguity (e.g. ("a","bc") vs ("ab","c")).
		_, _ = fmt.Fprintf(h, "%v\x00", row[c])
	}
	return int(h.Sum64() % uint64(degree))
}
