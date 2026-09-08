// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Byte-targeted INSERT batching for the MySQL bulk-load path (ADR-0150)
//
// The batched-INSERT bulk paths (plain [RowWriter.writeBatchedConn] and
// idempotent [RowWriter.writeBatchedIdempotentConn], plus their ADR-0097/
// ADR-0102 fan-out callers) used to flush on a fixed 500-row cap. On
// Vitess/PlanetScale — which has no LOAD DATA, so batched INSERT IS the
// bulk-load path — narrow rows produced 50–100 KB statements, leaving
// 10–20× of round-trip amortization on the table for WAN imports
// (docs/research/perf-gap-analysis-2026-07.md §"PlanetScale-MySQL").
//
// This file holds the ADR-0150 batch composer: rows accumulate until the
// ESTIMATED statement value bytes reach ~1 MiB (the pscale dumper's
// battle-tested statement size — the same constant the CDC applier's
// ADR-0139 coalescer uses), with the row-count cap retained only as a
// safety ceiling and a placeholder bound protecting MySQL's 16-bit
// prepared-statement parameter limit. Statement COMPOSITION is the only
// thing that changes: every value still binds to a `?` through the same
// prepareValue codec, so the wire encoding of each value is byte-identical
// (the Bug-74 safety property) — there are only more placeholder groups
// per statement. Each flush remains one autocommit ExecContext, so a
// transaction is still exactly one statement and stays far inside
// Vitess's ~20 s transaction killer (ADR-0052 DP-2 / ADR-0116).

package mysql

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// defaultStatementByteTarget is the estimated per-statement value-byte
// budget the batched-INSERT bulk paths accumulate to before flushing —
// the PRIMARY flush trigger (ADR-0150). ~1 MiB is the pscale dumper's
// battle-tested statement-body size for PlanetScale (see the flavor
// capability comment in flavor.go), and matches the CDC applier's
// maxCoalescedStatementBytes (ADR-0139). It sits comfortably under any
// max_allowed_packet (MySQL 8.0 default 64 MiB, PlanetScale 16 MiB+),
// so even a generous under-estimate by ir.ApproximateRowBytes cannot
// push a statement anywhere near the packet limit.
const defaultStatementByteTarget int64 = 1 << 20 // 1 MiB

// maxBulkInsertPlaceholders caps the total bound `?`s (rows × columns)
// in one bulk INSERT statement: MySQL's prepared-statement parameter
// count is a 16-bit field (hard limit 65,535). Same bound, same
// headroom, same rationale as the CDC applier's
// maxCoalescedPlaceholders (ADR-0139); kept as a separate named
// constant because the two paths flush on different machinery.
const maxBulkInsertPlaceholders = 60000

// bulkFlushHookForTest, when non-nil, is invoked by the batched bulk
// flush closures with the row count and estimated value bytes of the
// just-flushed statement — so a pin can assert the byte-targeted
// composition is actually taken on the real write path (a flush with
// rows well past the old 500-row cap) without scraping server status.
// Production leaves it nil. Set only by single-test fixtures (set then
// reset in the same test, never in parallel tests), mirroring the
// package's other test seams (multiRowFlushHookForTest).
var bulkFlushHookForTest func(rows int, bytes int64)

// insertBatcher accumulates the pending rows for ONE multi-row INSERT /
// upsert statement and owns the flush decision for the batched bulk
// paths (ADR-0150). It is a pure accumulator — the callers own the SQL
// build, the exec, and the reset-after-flush — so the flush-boundary
// arithmetic is unit-testable without a connection.
//
// Rows are NEVER split and never refused for size: a single row whose
// estimate alone exceeds the byte target simply makes the batch full,
// so it ships as (at most) a one-row statement. The server's
// max_allowed_packet stays the loud upper bound for a genuinely
// oversized row, exactly as before.
type insertBatcher struct {
	rows  []ir.Row
	bytes int64

	// rowCeil is the safety ceiling on rows per statement: the
	// configured / default row cap, clamped by the placeholder bound
	// for the table's column count. Never below 1.
	rowCeil int

	// byteTarget is the primary flush trigger: the estimated value
	// bytes at which the pending statement is full. The operator's
	// --max-buffer-bytes can only LOWER it (see newInsertBatcher).
	byteTarget int64
}

// newInsertBatcher resolves the flush triggers for a batched bulk write
// into table and returns a ready accumulator (ADR-0150):
//
//   - byteTarget: defaultStatementByteTarget (~1 MiB), clamped DOWN by
//     an operator --max-buffer-bytes below it. A larger operator cap
//     does not raise the target — the ~1 MiB statement size is a
//     round-trip-amortization choice, not a memory bound, and it
//     already sits far under the ADR-0028 64 MiB accumulation default.
//   - rowCeil: the configured maxRowsPerBatch (default
//     defaultMaxRowsPerBatch), further clamped so rows × non-generated
//     columns never exceeds maxBulkInsertPlaceholders, and never below
//     1 so even a pathologically wide table still makes progress.
func (w *RowWriter) newInsertBatcher(table *ir.Table) *insertBatcher {
	rowCeil := w.maxRowsPerBatch
	if rowCeil <= 0 {
		rowCeil = defaultMaxRowsPerBatch
	}
	if cols := len(nonGeneratedColumns(table.Columns)); cols > 0 {
		if byPlaceholders := maxBulkInsertPlaceholders / cols; byPlaceholders < rowCeil {
			rowCeil = byPlaceholders
		}
	}
	if rowCeil < 1 {
		rowCeil = 1
	}
	byteTarget := defaultStatementByteTarget
	if w.maxBufferBytes > 0 && w.maxBufferBytes < byteTarget {
		byteTarget = w.maxBufferBytes
	}
	return &insertBatcher{
		rows:       make([]ir.Row, 0, rowCeil),
		rowCeil:    rowCeil,
		byteTarget: byteTarget,
	}
}

// add appends row to the pending statement, accumulating its estimated
// value bytes (O(value-length), no encoding — ir.ApproximateRowBytes).
func (b *insertBatcher) add(row ir.Row) {
	b.rows = append(b.rows, row)
	b.bytes += ir.ApproximateRowBytes(row)
}

// full reports whether the pending statement should flush: the byte
// target reached (the primary trigger) or the row safety ceiling hit.
func (b *insertBatcher) full() bool {
	return len(b.rows) >= b.rowCeil || b.bytes >= b.byteTarget
}

// empty reports whether there is nothing pending to flush.
func (b *insertBatcher) empty() bool { return len(b.rows) == 0 }

// reset clears the accumulator after a successful flush, retaining the
// backing array.
func (b *insertBatcher) reset() {
	b.rows = b.rows[:0]
	b.bytes = 0
}

// copyBottleneckMarker is the grep-stable marker on the once-per-writer
// bulk-load throughput hint. Operators reading this line are usually
// mid-copy and asking "why is this slow"; the marker is what they search
// their logs, and the docs, for.
const copyBottleneckMarker = "COPY-BOTTLENECK"

// noteTierCPUBoundTarget emits the once-per-writer ADR-0150 companion
// hint when the bulk-write path engages against a hosted-PlanetScale
// target. Gated on the flavor at OpenRowWriter (self-hosted vitess runs
// on the operator's own hardware and is deliberately excluded); the
// sync.Once keeps it to one line per writer — one line per run on the
// migrate path, which shares a single RowWriter.
//
// # Why this no longer names a lever (2026-09-08, from a user report)
//
// It used to say, flatly, that writes here are "tier-CPU-bound, not
// connection-bound", that "copy parallelism beyond the auto budget will
// not scale throughput linearly", and that "a larger tier (or Metal) is
// the real lever". Its evidence is ADR-0116's ground truth: a **PS-10** —
// the smallest tier — pins at 100% CPU under a 2-wide copy. That was
// measured at one point and asserted across the whole range.
//
// A user migrating AWS to GCP us-east4 followed it: they scaled the
// target from M-160 (2 vCPU) to M-640 (8 vCPU) and throughput moved
// 14k to 16k rows/s while target CPU sat at **11%**. The real bound was
// the single cross-region INSERT connection — a latency regime this hint
// had no model for at all — and the fix was the very thing the hint told
// them not to bother with: `--copy-fanout-degree 16
// --vstream-copy-table-parallelism 4` took the copy from ~18-36h to ~4h.
// So the hint cost a tier upgrade AND steered away from the 4.5-9x lever.
//
// The measured fact is kept and SCOPED to where it was measured. What is
// removed is the generalisation to every tier and the single named lever.
// In its place the line names the DISCRIMINATOR, because which regime you
// are in is cheap to observe and not cheap to guess: if target CPU is
// high you are tier-bound, and if it is low while throughput is flat you
// are bound by round-trips in flight, which is what the fan-out flags
// buy. sluice cannot decide this for the operator here — the hint fires
// at the first flush, before any throughput exists to measure, and the
// writer has no ir.TargetTelemetry handle — so it says how to tell
// rather than asserting which.
func (w *RowWriter) noteTierCPUBoundTarget(ctx context.Context) {
	if !w.tierCPUBoundTarget {
		return
	}
	w.tierHintOnce.Do(func() {
		slog.InfoContext(
			ctx, "mysql: cdc: "+copyBottleneckMarker+": bulk-loading a PlanetScale target. If the copy is "+
				"slower than you expect, read the TARGET's CPU before changing anything — it tells you which "+
				"of two bounds you are on, and they have opposite fixes. HIGH target CPU: the tier is the "+
				"bound (a PS-10 pins at 100% under a 2-wide copy, ADR-0116) and more copy parallelism will "+
				"not scale; a larger tier or Metal is the lever. LOW target CPU with flat throughput: the "+
				"bound is round-trips in flight, not the server — usual on a CROSS-REGION copy, where one "+
				"INSERT connection is latency-bound however large the tier — and the lever is "+
				"--copy-fanout-degree with --vstream-copy-table-parallelism. --planetscale-metrics-db and "+
				"--planetscale-metrics-branch surface the target CPU this decision needs",
		)
	})
}
