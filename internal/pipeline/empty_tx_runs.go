// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// liveEmptyTxMaxRun bounds how many empty source transactions one boundary
// write stands for on a live stream ([migcore.EmptyTxRuns.MaxRun]). A stream
// whose source is busy only elsewhere, and never quiet, would otherwise hold
// its position back for as long as that lasts — the GC-43 (r) shape, where a
// position older than the source's binlog retention cannot resume. At 1,000 a
// stream pays one position write per thousand foreign transactions, which is
// noise beside the 12 ms per transaction it paid before.
const liveEmptyTxMaxRun = 1000

// liveEmptyTxLinger is how long the live stage waits for the next change
// before it releases a held empty transaction. It is the serial applier's
// own idle-flush grace ([appliershared.DefaultIdleFlushPeriod]): a boundary
// that carries no data reaches the target at most that much later than it
// would have, which is the latency the applier already accepts for the rows
// it batches. Any run whose transactions arrive closer together than this
// collapses.
const liveEmptyTxLinger = appliershared.DefaultIdleFlushPeriod

// coalesceEmptyTxRuns is the live stream's empty-transaction stage (Bug 300):
// it collapses each run of empty source transactions to the run's last
// transaction and otherwise passes the stream through unchanged. See
// [migcore.EmptyTxRuns] for what is dropped and why that loses nothing.
//
// A capture or a replay knows its whole stream. A live stream does not know
// what comes next, so a held empty transaction is released once no change
// has arrived for linger — the stage never delays a change that carries
// data, only a boundary, and only by that much. A run therefore collapses
// exactly when the source is producing out-of-scope transactions faster than
// one per linger, which is the regime in which one position write apiece is
// what held the applier behind. (Releasing only when the input is
// momentarily EMPTY was the first cut, and measured no better than no stage
// at all: every hop of the intercept chain is an unbuffered channel, so the
// stage outran its own input between a transaction's begin and its commit.)
// A withheld begin is not released on a linger: it persists nothing, and its
// transaction's next event decides it. maxRun bounds a run that never
// pauses. A close of in flushes first, so a clean stop persists the final
// boundary it always did; a cancel drops what is held, which is what a crash
// at that point would have dropped.
func coalesceEmptyTxRuns(ctx context.Context, in <-chan ir.Change, maxRun int, linger time.Duration) <-chan ir.Change {
	out := make(chan ir.Change)
	go func() {
		defer close(out)
		runs := migcore.EmptyTxRuns{MaxRun: maxRun}
		send := migcore.ChannelEmit(ctx, out)
		timer := time.NewTimer(linger)
		defer timer.Stop()
		for {
			var lingered <-chan time.Time
			if runs.HoldsRun() {
				timer.Reset(linger)
				lingered = timer.C
			}
			select {
			case c, ok := <-in:
				if !ok {
					_ = runs.Flush(send)
					return
				}
				if runs.Push(c, send) != nil {
					return
				}
			case <-lingered:
				if runs.ReleaseRun(send) != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
