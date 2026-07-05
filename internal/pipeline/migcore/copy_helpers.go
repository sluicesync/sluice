// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"fmt"
	"io"

	"sluicesync.dev/sluice/internal/ir"
)

// DefaultBulkBatchSize is the per-batch row count when [Migrator]'s
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
const DefaultBulkBatchSize = 5000

// RowChanBuffer is the buffer size for the bulk-copy row channels
// (reader output, progress/PK tees, the fast-loader pump; mirrored by
// a same-named constant in each engine's row reader) and for the
// restore / chain-restore / broker replay hops (chunk decode → apply),
// which adopted the same discipline when perf-parity matrix gap 2
// closed (2026-07).
//
// Unbuffered channels force a rendezvous per row: source decode and
// target write strictly ALTERNATE, so the per-row cost is
// decode+write instead of max(decode, write). A small bounded buffer
// restores read/write overlap while preserving back-pressure — when
// the writer stalls, the buffer fills and the reader blocks exactly
// as before (bounded is not unbounded). 64 keeps the worst-case
// buffered footprint modest even on wide rows (64 × row size per
// pipeline stage).
//
// Resume correctness is unaffected by buffering: the per-batch resume
// cursor is persisted only AFTER the writer has consumed the entire
// batch (see the runBatchedCopy loop in migrate_bulk.go — checkpoint
// follows WriteRowsIdempotent's return), never from a tee's in-flight
// position, so buffered-but-unwritten rows can never advance a
// persisted cursor.
const RowChanBuffer = 64

// CloseIf calls Close on v if it implements [io.Closer]. Used to clean
// up the *sql.DB handles the engine readers/writers wrap.
func CloseIf(v any) {
	if c, ok := v.(io.Closer); ok {
		_ = c.Close()
	}
}

// ReaderStreamErr is the loud-failure gate for the bulk-copy paths
// (Bug 68). A streaming [ir.RowReader] scans and decodes rows on a
// background goroutine; a per-row scan/decode failure there closes
// the row channel exactly like a clean end-of-table would. Without
// observing [ir.RowReader.Err] after the channel drains, the
// orchestrator cannot tell "table fully read" from "a row failed and
// the stream aborted" — the writer simply sees fewer rows and the
// migrate exits 0 with the table silently truncated (the worst
// failure class under the project's loud-failure tenet). Every copy
// path MUST call this after the writer returns success and propagate
// a non-nil result as a hard failure. The error is wrapped so the
// table name and the originating reader error are both visible in
// the operator-facing message.
//
// context.Canceled / context.DeadlineExceeded are deliberately NOT
// treated as a stream failure. The batched + parallel copy paths
// cancel each batch's child context on purpose once the writer has
// drained it (the Bug-9 clean-unwind shape); the reader goroutine
// observes that cancel and stores it on its sticky error. That is a
// benign orchestrator-driven teardown, not a data-integrity failure.
// A genuine parent-context abort (operator Ctrl-C, deadline) is still
// surfaced — the writer returns the same ctx error and the
// orchestrator's own ctx checks fire — so suppressing it here cannot
// hide a real cancellation, only the self-inflicted per-batch one.
// The Bug-68 failure class (a scan/decode error) is never a context
// error; it is a `postgres: column …` / `mysql: scan: …` value, so
// this filter is precise.
func ReaderStreamErr(rr ir.RowReader, table *ir.Table) error {
	err := rr.Err()
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return fmt.Errorf("source row stream for table %q failed: %w", table.Name, err)
}

// TableNamesForPublication returns the bare table names from a
// post-filter schema, in declaration order. Used by the publication-
// scope step (Bug 13, ADR-0021) — schema-qualifying happens in the
// engine because schema is an engine-side concept (PG namespaces vs.
// MySQL databases vs. future engines).
func TableNamesForPublication(schema *ir.Schema) []string {
	if schema == nil {
		return nil
	}
	out := make([]string, 0, len(schema.Tables))
	for _, t := range schema.Tables {
		out = append(out, t.Name)
	}
	return out
}

// ReadChunkBatch reads one PK-ordered page of a chunk, clipped to the
// chunk's INCLUSIVE upper bound (UpperPK; nil for the last chunk).
//
// CRITICAL exactly-once contract (ADR-0096): the upper-bound clip MUST
// agree with the engine's ORDER BY total order, which for string / char /
// varchar / decimal PKs is the column's NATIVE DB COLLATION, not a byte
// order. So when the reader implements [ir.BoundedBatchedRowReader] we
// push the upper bound INTO the SQL WHERE (`(pk) <= upTo`) — same
// collation, same PK index as the cursor's lower bound and the ORDER BY —
// and do NO Go-side clip. This is the path BOTH shipping engines take.
//
// The Go-side bytewise [filterByUpperBound] is the FALLBACK for a reader
// that lacks the bounded surface. It is correct only for families whose
// byte order matches the collation order (integer, temporal, PG-native
// uuid/bytea); string/decimal keyset tables are kept off this path by
// shouldParallelChunk, which requires BoundedBatchedRowReader for the
// keyset strategy. The fallback never runs for those families, so it can
// never silently drop a boundary-straddling collated row.
func ReadChunkBatch(
	ctx context.Context,
	br ir.BatchedRowReader,
	table *ir.Table,
	cursor, upperPK []any,
	pkCols []string,
	limit int,
) (<-chan ir.Row, error) {
	if bb, ok := br.(ir.BoundedBatchedRowReader); ok {
		// SQL-side upper bound: collation-correct by construction, no Go
		// clip. upperPK nil (last chunk) => plain ReadRowsBatchBounded with
		// no upper predicate, identical to ReadRowsBatch.
		return bb.ReadRowsBatchBounded(ctx, table, cursor, upperPK, limit)
	}
	rowsCh, err := br.ReadRowsBatch(ctx, table, cursor, limit)
	if err != nil {
		return nil, err
	}
	return filterByUpperBound(ctx, rowsCh, pkCols, upperPK), nil
}

// filterByUpperBound wraps a row channel with a goroutine that drops
// rows whose PK exceeds the chunk's INCLUSIVE upper bound. Returns the
// downstream channel (forwarded as-is when upperPK is nil — the last
// chunk has no upper bound).
//
// FALLBACK ONLY (see [ReadChunkBatch]): this Go-side clip is used only
// for a reader that does NOT implement [ir.BoundedBatchedRowReader]. The
// preferred path pushes the upper bound into SQL so it uses the column's
// native collation. The comparison here is the full-tuple bytewise
// [ComparePKTuple], which matches the engine's ORDER BY total order ONLY
// for byte-ordered families (integer, temporal, PG-native uuid/bytea); it
// would DIVERGE from a non-C string/decimal collation, so the keyset
// strategy that covers those families requires the bounded surface and
// never reaches this filter (enforced in shouldParallelChunk).
//
// Without a clip, chunk 0's batch could run past chunk 1's range and
// double-copy. The filter terminates the channel early (closes downstream
// so the reader goroutine unwinds) the first time it sees a row strictly
// past upperPK; because rows arrive in PK order, every subsequent row is
// also past the bound, so stopping is safe and complete. upperPK is
// inclusive: a row whose PK equals upperPK belongs to THIS chunk, so only
// [ComparePKTuple] > 0 is dropped.
func filterByUpperBound(ctx context.Context, src <-chan ir.Row, pkCols []string, upperPK []any) <-chan ir.Row {
	if upperPK == nil || len(pkCols) == 0 {
		// No upper bound (last chunk) or degenerate PK — pass through.
		return src
	}
	if len(upperPK) != len(pkCols) {
		// Width mismatch should not happen (boundaries are width-checked
		// at computation). Defensive pass-through keeps an unexpected
		// caller safe rather than mis-clipping.
		return src
	}

	// Bounded buffer for the same decode/write-overlap reason as the
	// tees — see [RowChanBuffer].
	out := make(chan ir.Row, RowChanBuffer)
	go func() {
		defer close(out)
		rowPK := make([]any, len(pkCols))
		for {
			select {
			case <-ctx.Done():
				return
			case row, ok := <-src:
				if !ok {
					return
				}
				for i, c := range pkCols {
					rowPK[i] = row[c]
				}
				if ComparePKTuple(rowPK, upperPK) > 0 {
					// Strictly past the chunk's inclusive upper bound;
					// drop this row and stop. Rows arrive in PK order, so
					// nothing after it can be within the bound. Returning
					// closes `out` and the reader goroutine unwinds.
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
