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

// liveEmptyTxPolicy is when the live stage releases a held empty
// transaction: whichever of its three bounds is reached first.
type liveEmptyTxPolicy struct {
	// maxRun bounds how many empty transactions one boundary write stands
	// for ([migcore.EmptyTxRuns.MaxRun]).
	maxRun int
	// linger is the idle grace: release once no change has arrived for this
	// long. The fast path, and the only one an idle stream takes.
	linger time.Duration
	// maxHold bounds how long a run may be held at all, measured from when
	// its FIRST transaction was held and never reset by a later one.
	maxHold time.Duration
	// now is the clock maxHold and linger are measured on; time.Now outside
	// tests.
	now func() time.Time

	// pushed, TEST-ONLY (nil in production), runs after each received
	// change has been pushed and timed, so a test driving a fake clock can
	// advance it knowing which events the stage has already stamped.
	pushed func()
}

// defaultLiveEmptyTxPolicy is the live stream's policy.
//
//   - linger is the serial applier's own idle-flush grace
//     ([appliershared.DefaultIdleFlushPeriod], 100 ms): a boundary that
//     carries no data reaches the target at most that much later on an idle
//     stream, the latency the applier already accepts for the rows it batches.
//   - maxHold is 1 s, the lane path's idle checkpoint period
//     (laneapply checkpointIdlePeriod): the DEFAULT apply path has always let
//     its persisted position lag the stream by up to that much, so holding a
//     run that long makes the serial paths no staler than the default, and
//     costs at most one position write a second under any foreign load. It is
//     what bounds a source whose foreign transactions keep arriving just
//     inside the linger — every one resets the linger, so on its own a steady
//     ~10 tx/s held the position back for the whole run (the pre-land review's
//     F1: ~100 s at the 1,000 cap), long enough to trip `sync health
//     --max-stale-seconds`, hold a PG 14 slot's WAL, or outlive a short binlog
//     retention.
//   - maxRun (1,000) still bounds a run by count, the cheaper bound at high
//     rates.
func defaultLiveEmptyTxPolicy() liveEmptyTxPolicy {
	return liveEmptyTxPolicy{
		maxRun:  1000,
		linger:  appliershared.DefaultIdleFlushPeriod,
		maxHold: time.Second,
		now:     time.Now,
	}
}

// coalesceEmptyTxRuns is the live stream's empty-transaction stage (Bug 300):
// it collapses each run of empty source transactions to the run's last
// transaction and otherwise passes the stream through unchanged. See
// [migcore.EmptyTxRuns] for what is dropped and why that loses nothing.
//
// A capture or a replay knows its whole stream. A live stream does not know
// what comes next, so a held empty transaction is released by the policy —
// after a linger with no change, after maxHold since its run began, or at
// maxRun — and the stage never delays a change that carries data, only a
// boundary, and only by that much. A run therefore collapses exactly when the
// source is producing out-of-scope transactions faster than one per linger,
// which is the regime in which one position write apiece is what held the
// applier behind. (Releasing only when the input is momentarily EMPTY was the
// first cut, and measured no better than no stage at all: every hop of the
// intercept chain is an unbuffered channel, so the stage outran its own input
// between a transaction's begin and its commit.) A withheld begin is never
// released by the clock: it persists nothing, and its transaction's next
// event decides it. A close of in flushes first, so a clean stop persists the
// final boundary it always did; a cancel drops what is held, which is what a
// crash at that point would have dropped.
func coalesceEmptyTxRuns(ctx context.Context, in <-chan ir.Change, p liveEmptyTxPolicy) <-chan ir.Change {
	out := make(chan ir.Change)
	go func() {
		defer close(out)
		runs := migcore.EmptyTxRuns{MaxRun: p.maxRun}
		send := migcore.ChannelEmit(ctx, out)
		timer := time.NewTimer(p.linger)
		defer timer.Stop()
		var runStart, lastEvent time.Time
		for {
			var due <-chan time.Time
			if runs.HoldsRun() {
				now := p.now()
				wait := min(p.linger-now.Sub(lastEvent), p.maxHold-now.Sub(runStart))
				if wait <= 0 {
					if runs.ReleaseRun(send) != nil {
						return
					}
					continue
				}
				timer.Reset(wait)
				due = timer.C
			}
			select {
			case c, ok := <-in:
				if !ok {
					_ = runs.Flush(send)
					return
				}
				held, released := runs.HoldsRun(), runs.Releases()
				if runs.Push(c, send) != nil {
					return
				}
				lastEvent = p.now()
				if runs.HoldsRun() && (!held || runs.Releases() != released) {
					runStart = lastEvent // this push began a new run
				}
				if p.pushed != nil {
					p.pushed()
				}
			case <-due:
				// Re-judged against the clock at the top of the loop.
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
