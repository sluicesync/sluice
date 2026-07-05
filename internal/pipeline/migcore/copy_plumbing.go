// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
)

// PrimaryKeyColumnNames returns the PK column names in declaration
// order, or nil when the table has no PK. The orchestrator routes
// no-PK tables away from the cursor path before this gets called;
// the helper is defensive about a nil PK so future callers can use
// it safely.
func PrimaryKeyColumnNames(table *ir.Table) []string {
	if table == nil || table.PrimaryKey == nil {
		return nil
	}
	out := make([]string, len(table.PrimaryKey.Columns))
	for i, c := range table.PrimaryKey.Columns {
		out[i] = c.Column
	}
	return out
}

// ApplyMaxBufferBytes plumbs the orchestrator-side --max-buffer-bytes
// value to an engine-side surface that opts into byte-bounded
// batching via [ir.MaxBufferBytesSetter]. Engines that don't
// implement the setter retain their pre-v0.7.0 row-count-only
// behaviour. Zero or negative bytes is the no-cap value (engines
// fall back to their built-in default if they have one).
//
// Called immediately after each engine writer/applier opens, before
// any WriteRows / ApplyBatch dispatch. See ADR-0028.
func ApplyMaxBufferBytes(target any, bytes int64) {
	if bytes <= 0 {
		return
	}
	if setter, ok := target.(ir.MaxBufferBytesSetter); ok {
		setter.SetMaxBufferBytes(bytes)
	}
}

// MaxChunksPerTable caps how many intra-table PK-range chunks one table is
// split into (ADR-0119 Decision 1). The split reclaims the tail; past a point
// extra chunks only add per-chunk overhead (a boundary tuple, a claim, a SQL
// page) without widening the copy beyond the N pinned readers. 64 is generous
// (a table N× the threshold still chunks fully up to the cap) while bounding the
// work list.
const MaxChunksPerTable = 64

// ClampParallelChunkCount is the pure core of the ADR-0123 Decision 2 chunk-count
// clamp with no I/O — shared with the backup within-table read planner (ADR-0149),
// which derives its chunk count from the same (estimate, threshold, parallelism,
// budget) inputs so the two modes' chunking behaviour cannot drift apart.
func ClampParallelChunkCount(est, threshold int64, parallelism, copyBudget int) int {
	if threshold < 1 {
		threshold = 1
	}
	if est < 0 {
		est = 0
	}
	m := int((est + threshold - 1) / threshold) // ceiling of est over threshold
	if m < parallelism {
		m = parallelism
	}
	if m < 2 {
		m = 2
	}
	chunkCap := MaxChunksPerTable
	if copyBudget > chunkCap {
		chunkCap = copyBudget
	}
	if m > chunkCap {
		m = chunkCap
	}
	return m
}

// ApproximateRowCount queries the row reader for an estimate of the
// table's row count used to decide within-table parallel chunking. This
// is the chunk-DECISION path (caller A) and runs STRICTLY pre-stream, so
// it prefers the [ir.RowCountEstimator] surface — which a snapshot-pinned
// PG reader implements by reading reltuples off a FRESH conn, enabling
// within-table chunking on the sync fast cold-start without ever probing
// the pinned conn that the ETA path (kickOffRowCount) must keep clear
// (ADR-0079 v1.1). When the reader implements only [ir.RowCounter] (or
// neither), it falls back to CountRows / (0, nil); the caller treats "0
// rows" as below any threshold and routes to the single-reader path.
func ApproximateRowCount(ctx context.Context, rows ir.RowReader, table *ir.Table) (int64, error) {
	if est, ok := rows.(ir.RowCountEstimator); ok {
		return est.EstimateRowCount(ctx, table)
	}
	if rc, ok := rows.(ir.RowCounter); ok {
		return rc.CountRows(ctx, table)
	}
	return 0, nil
}
