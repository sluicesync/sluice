// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Delayed-replica CDC apply (roadmap item 46, ADR-0121). `--apply-delay
// DURATION` holds each steady-state CDC change until its source commit
// timestamp + delay has elapsed on the local wall clock before forwarding it
// to the applier — the MySQL MASTER_DELAY "oops window" DR pattern. Off by
// default (0 = no delay = today's behaviour; the interceptor is wired only
// when ApplyDelay > 0, so the default apply path is byte-identical).
//
// The whole correctness story is the resume-safety invariant (ADR-0121 §2):
// this interceptor sits strictly UPSTREAM of the applier in the change
// pipeline, and the applier is the only thing that advances the durable resume
// position (ADR-0007, position written in the same tx as the data). A change
// held here has NOT been forwarded, so the applier has never seen it, so the
// position has not advanced past it. A crash (or ctx-cancel) while N changes
// are held therefore loses nothing: the persisted position is still behind the
// held window, and StreamChanges re-reads every held-but-unapplied change on
// resume (re-applied idempotently per ADR-0010). The delay window is never the
// SOLE home of an un-applied change — the source is.
//
// Memory is bounded by backpressure, not buffering (ADR-0121 §3): the
// interceptor holds at most ONE change (the one it is sleeping on) and blocks
// on the sleep BEFORE reading the next, so the upstream channels backpressure
// to the CDC reader. We deliberately do not read-ahead into an in-heap queue
// (which would be ~ delay × write-rate). See the ADR for the
// replication-idle-timeout tradeoff of backpressuring the reader.

import (
	"context"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// sleepFunc waits for d (or until ctx is cancelled). It returns true if the
// full duration elapsed, false if ctx was cancelled first. Injected so tests
// drive the hold deterministically without real time.
type sleepFunc func(ctx context.Context, d time.Duration) bool

// realSleep is the production [sleepFunc]: a ctx-cancellable timer.
func realSleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// delayChanges is the ADR-0121 delayed-replica gate: a pass-through
// interceptor that holds each change until SourceCommitTime + delay has
// elapsed (per now), then forwards it. A change with a zero commit time (a
// source/path that supplies none, or a non-row SchemaSnapshot) and a change
// whose release instant is already past forward immediately. now and sleep are
// injected for deterministic testing; production callers pass time.Now and
// [realSleep].
//
// Resume-safety (the load-bearing property): on ctx cancellation while holding
// a change, the function returns WITHOUT forwarding it — so a crash/stop
// mid-delay-window leaves that change un-applied and the position behind it,
// and resume re-reads it. The interceptor never advances the position itself;
// only the downstream applier does (ADR-0007).
//
// Because every row event in a source transaction carries that transaction's
// commit timestamp, gating each change at commitTs+delay releases the whole
// transaction at one instant — TxBegin → rows → TxCommit flow in order, all
// eligible together, so the batched applier groups them exactly as undelayed
// (a transaction is never split across the delay; ADR-0121 §4).
func delayChanges(
	ctx context.Context,
	in <-chan ir.Change,
	delay time.Duration,
	now func() time.Time,
	sleep sleepFunc,
) <-chan ir.Change {
	out := make(chan ir.Change)
	go func() {
		defer close(out)
		for {
			select {
			case c, ok := <-in:
				if !ok {
					return
				}
				if ts := c.SourceCommitTime(); !ts.IsZero() {
					if wait := ts.Add(delay).Sub(now()); wait > 0 {
						if !sleep(ctx, wait) {
							// ctx cancelled while holding: do NOT forward the
							// held change. It stays un-applied, the position
							// stays behind it, resume re-reads it (ADR-0121 §2).
							return
						}
					}
				}
				select {
				case out <- c:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
