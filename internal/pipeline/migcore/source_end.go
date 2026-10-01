// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
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
// short page. It counts only while ctx is live: a close observed after
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
// nil and before anything is recorded. It returns nil only when:
//
//   - the source reached its natural end ([SourceEnd.Reached]), so the
//     writer was handed the whole table and did not return early; and
//   - ctx is still live, so no stage between the source and the writer
//     dropped rows on a cancel. Every such stage drops ONLY on ctx.Done
//     (an error-driven close is surfaced by its own errFn / the reader's
//     sticky Err), and cancellation is monotone, so a live ctx here means
//     none of them ever took that branch.
//
// The second clause is a named wart. A stop landing in the microseconds
// between the writer's final commit and this check reads as interrupted
// although every row committed; the price is a loud re-copy of one table,
// never loss. It is the residue of the stop-time window GC-41 (i) was
// filed for, shrunk from "the whole progress-row write" to this call.
func (e *SourceEnd) Confirm(ctx context.Context, table string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: table %q: %w", ErrCopyInterrupted, table, err)
	}
	if !e.drained.Load() {
		return fmt.Errorf("%w: table %q: the row stream closed before the source reported its end", ErrCopyInterrupted, table)
	}
	return nil
}
