// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"sluicesync.dev/sluice/internal/ir"
)

// ErrCopyInterrupted marks a copy that stopped before its source reached
// its natural end. A caller that records progress must never read it as a
// finished table; errors.Is also reaches the context error that caused it
// whenever one did.
var ErrCopyInterrupted = errors.New("copy interrupted before its source reached its end")

// SourceEnd is the source-end signal of one copy stream (GC-41 (i)): the
// one fact that makes "the writer returned nil" mean "the table is
// copied".
//
// A writer's nil does not carry that fact. Every stage between the source
// and the writer — the reader's goroutine, the tees, redaction, the
// shard stamp, the fan-out dispatcher — closes its output when its
// context ends, and that close is indistinguishable from end-of-table to
// whatever is downstream. The engine write loops then return nil whenever
// their "channel closed" select arm wins over ctx.Done on an empty buffer
// (MySQL writeBatchedConn / writeBatchedIdempotentConn; Postgres
// writeViaBatch / writeViaBatchIdempotent / writeViaCopyChunked — the
// race is random, and the pgx COPY and LOAD DATA sources end on a close
// the same way). So a stop landing mid-copy could read as success, and a
// caller then wrote the table COMPLETE with the rows it had so far.
//
// The signal does not try to make the writers deterministic; it makes
// their race irrelevant. The stage that reads the SOURCE records a
// natural end with [SourceEnd.Reached]; the copy entry point asks
// [SourceEnd.Confirm] after its writer returns nil, and returns what it
// says. TestCopyEntryPointRoster_EveryWriteConsultsTheSourceEnd holds
// every entry point in internal/pipeline to that.
type SourceEnd struct{ drained atomic.Bool }

// Reached records that the source stream ended NATURALLY — the reader
// closed its channel at end of data, or a batched loop read its terminal
// short page. (The batched loops read "short page" as "end of range" on
// the [ir.BatchedRowReader] full-page contract. The one code path that
// could short a page for another reason — [ReadChunkBatch]'s Go-side
// filterByUpperBound fallback, which clips client-side after the LIMIT —
// is unreachable today because every shipping batched reader implements
// [ir.BoundedBatchedRowReader]. UNVERIFIED PREMISE for a future reader
// that does not; nothing here would notice.) It counts only while ctx is live: a close observed after
// ctx ended may be the reader unwinding on that cancel, and the two are
// indistinguishable from here, so the ambiguous case is recorded as
// not-drained (a loud re-copy, never a skipped tail). Because cancellation
// is monotone, a live ctx at the close also proves the reader did not
// close BECAUSE of this ctx.
func (e *SourceEnd) Reached(ctx context.Context) {
	if ctx.Err() == nil {
		e.drained.Store(true)
	}
}

// Confirm is the completion verdict, asked once the writer has returned
// nil and before anything is recorded. handed is every channel the writer
// was given (one, or the fan-out's per-worker channels; none for the raw
// byte-pipe, whose importer is fed by the exporter's own end). It returns
// nil only when:
//
//   - the source reached its natural end ([SourceEnd.Reached]);
//   - ctx is still live, so no stage between the source and the writer
//     dropped rows on a cancel. Every such stage drops ONLY on ctx.Done
//     (an error-driven close is surfaced by its own errFn / the reader's
//     sticky Err), and cancellation is monotone, so a live ctx here means
//     none of them ever took that branch; and
//   - the writer consumed every handed channel to its close. A source end
//     says the rows were READ, not that the writer took them: a source
//     small enough to sit in the stage buffers ends before the writer has
//     consumed anything, so a writer that returns nil early would leave
//     them there unseen. Every in-tree writer drains to the close; this
//     makes that a checked fact rather than a promise.
//
// No one clause is enough. The first alone passes a stage that dropped
// rows after the source ended, and a writer that left buffered rows; the
// third alone passes a stream every stage closed on a cancel. Where a tee
// reads the source, the first is in fact implied by the other two — a tee
// that did not see the end either is still open (so its output is not
// consumed to a close) or exited on ctx (so ctx is not live) — and it is
// kept as the direct observation the other two are an argument for. Where
// a pump or exporter decides the end itself (copyChunkFast, the raw
// byte-pipe), it is the only clause that sees a pump that stopped early
// without an error. The ctx clause relies on one premise of the
// [ir.RowReader] contract: a reader closes its stream early only on the
// context it was handed, or reports why on Err.
//
// The second clause is a named wart. A stop landing in the microseconds
// between the writer's final commit and this check reads as interrupted
// although every row committed; the price is a loud re-copy of one table,
// never loss. It is the residue of the stop-time window GC-41 (i) was
// filed for, shrunk from "the whole progress-row write" to this call.
func (e *SourceEnd) Confirm(ctx context.Context, table string, handed ...<-chan ir.Row) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: table %q: %w", ErrCopyInterrupted, table, err)
	}
	if !e.drained.Load() {
		return fmt.Errorf("%w: table %q: the row stream closed before the source reported its end", ErrCopyInterrupted, table)
	}
	for _, ch := range handed {
		if !consumedToClose(ch) {
			return fmt.Errorf("%w: table %q: the writer returned with rows it was handed still unread", ErrCopyInterrupted, table)
		}
	}
	return nil
}

// consumedToClose reports whether ch is closed with nothing left in it,
// without blocking: a receive that yields a row means the writer left it
// behind, and one that would block means the stream never closed.
func consumedToClose(ch <-chan ir.Row) bool {
	select {
	case _, ok := <-ch:
		return !ok
	default:
		return false
	}
}
