// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Adaptive backoff on connection-slot exhaustion during the parallel
// bulk COPY (connection-resilience Phase 2b).
//
// Phase 1's budget preflight (connection_budget.go) right-sizes the
// parallelism up front against the target's slot accounting, so a
// correctly-sized run never asks for more slots than exist. Phase 2b is
// the defense-in-depth for the rare *mid-copy race*: after the preflight
// measured the budget and the pool started opening connections, another
// process (a peer migration, an operator psql, a leaked/orphaned backend)
// grabbed the slots the preflight had counted as free, so a chunk's
// connection-open returns SQLSTATE 53300 even though the preflight said
// there was room. Rather than abort the whole errgroup, the pool
// multiplicatively decreases the effective parallelism and retries the
// failed chunk — a transient slot shortage degrades to slower-but-correct
// instead of a failed migration.
//
// Two pieces, both kept pure and table-testable here; the pool plumbing
// in migrate_parallel.go wraps them:
//
//   - the retryable-error classification lives in the target engine
//     (ir.ConnectionSlotClassifier — Postgres SQLSTATE 53300), so the
//     pipeline never imports a driver and the "is this slot exhaustion?"
//     knowledge stays engine-side; and
//   - the AIMD decision (this file): given the current effective
//     parallelism + the retry attempt, what is the next parallelism, how
//     long to wait, and have we hit the give-up bound.
//
// Why multiplicative-decrease ONLY (no additive-increase within a copy):
// the applier's AIMD (change_applier_batch.go) both decreases on latency
// pressure and additively re-probes upward because its workload is a
// long-lived stream whose pressure ebbs and flows. A bulk copy is a
// bounded, one-shot phase: once the target proves it can't seat N
// writers, re-probing upward mid-copy would just re-trigger the same
// 53300 and waste a backoff cycle. Staying decreased for the rest of the
// copy is simpler and strictly safe — the only cost is finishing the copy
// at a lower parallelism than a (possibly) now-freed target could
// sustain, which the next migration's preflight re-measures from scratch.

package pipeline

import "time"

// copyBackoffPolicy parameterises the AIMD decision. The defaults
// ([defaultCopyBackoffPolicy]) are what the pool uses; tests construct
// their own to exercise the bounds without real waits.
type copyBackoffPolicy struct {
	// maxRetries bounds the total number of slot-exhaustion retries
	// across the whole parallel copy of one table. Once exceeded, the
	// decision gives up loudly. A permanently-saturated target therefore
	// fails after a bounded number of attempts instead of spinning
	// forever.
	maxRetries int

	// baseDelay is the wait before the first retry. Each subsequent
	// retry waits baseDelay * 2^(attempt-1), capped at maxDelay — a
	// standard bounded exponential backoff so a brief contention spike
	// clears on the first short wait while a sustained shortage backs off
	// to the cap.
	baseDelay time.Duration

	// maxDelay caps a single backoff wait so the exponential term can't
	// grow without bound.
	maxDelay time.Duration

	// maxTotalWait bounds the summed backoff waits across the whole copy.
	// It is the second give-up trigger (alongside maxRetries): even if
	// retries remain, once the accumulated wait reaches this ceiling the
	// decision gives up loudly so a permanently-saturated target can't
	// stall the migration for an unbounded wall-clock duration.
	maxTotalWait time.Duration
}

// defaultCopyBackoffPolicy is the policy the parallel-copy pool uses.
//
//   - 6 retries with a 250ms base doubling to a 4s cap covers a transient
//     contention spike (a peer run finishing, an operator psql closing)
//     within a few seconds while still terminating promptly on a target
//     that is genuinely out of slots.
//   - a 30s total-wait ceiling bounds the worst-case stall: the operator
//     gets the loud "slots stayed exhausted" error in well under a minute
//     rather than a migration that appears hung.
//
// Nothing in the design depends on the exact numbers; they are tuned for
// "clears a blip, fails a wall" and can move without changing the shape.
var defaultCopyBackoffPolicy = copyBackoffPolicy{
	maxRetries:   6,
	baseDelay:    250 * time.Millisecond,
	maxDelay:     4 * time.Second,
	maxTotalWait: 30 * time.Second,
}

// copyBackoffDecision is the verdict [nextCopyBackoff] returns for one
// slot-exhaustion event.
type copyBackoffDecision struct {
	// NextParallelism is the multiplicatively-decreased effective
	// parallelism the pool should enforce going forward (halve, floor at
	// 1). Meaningful only when GiveUp is false.
	NextParallelism int

	// Delay is how long the pool should wait before retrying the failed
	// chunk. Meaningful only when GiveUp is false.
	Delay time.Duration

	// GiveUp is true when the retry/total-wait bound is exhausted. The
	// pool surfaces the loud "slots stayed exhausted after N backoffs"
	// error and aborts — never an infinite spin.
	GiveUp bool
}

// nextCopyBackoff is the pure AIMD decision for a single
// connection-slot-exhaustion event during the parallel bulk copy.
//
// Inputs:
//
//   - currentParallelism: the effective parallelism in force when the
//     53300 fired (always >= 1).
//   - attempt: the 1-based retry attempt number (the first retry is
//     attempt 1).
//   - priorTotalWait: the summed backoff waits already spent on this
//     table's copy, so the total-wait ceiling is enforced across retries.
//   - p: the policy (bounds + delays).
//
// Decision:
//
//   - If attempt > p.maxRetries, or adding this attempt's delay would
//     push priorTotalWait past p.maxTotalWait, GiveUp = true.
//   - Otherwise NextParallelism = max(1, currentParallelism/2) and Delay
//     = min(p.baseDelay * 2^(attempt-1), p.maxDelay).
//
// Pure: no I/O, no clock read, no state mutation — the wait itself and
// the total-wait accumulation happen in the caller. Table-unit-testable.
func nextCopyBackoff(currentParallelism, attempt int, priorTotalWait time.Duration, p copyBackoffPolicy) copyBackoffDecision {
	if attempt > p.maxRetries {
		return copyBackoffDecision{GiveUp: true}
	}

	delay := backoffDelay(attempt, p)

	// Total-wait ceiling: if THIS wait would carry the accumulated wait
	// past the cap, give up now rather than wait-then-discover. A target
	// that is genuinely saturated should fail loudly and bounded, not
	// stall the operator for an open-ended duration.
	if priorTotalWait+delay > p.maxTotalWait {
		return copyBackoffDecision{GiveUp: true}
	}

	next := currentParallelism / 2
	if next < 1 {
		next = 1
	}
	return copyBackoffDecision{
		NextParallelism: next,
		Delay:           delay,
	}
}

// backoffDelay computes the bounded exponential wait for a 1-based
// attempt: baseDelay * 2^(attempt-1), capped at maxDelay. Split out so
// the doubling/cap is independently testable and the shift can't
// overflow (it saturates to maxDelay long before the exponent is large).
func backoffDelay(attempt int, p copyBackoffPolicy) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := p.baseDelay
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= p.maxDelay {
			return p.maxDelay
		}
	}
	if delay > p.maxDelay {
		return p.maxDelay
	}
	return delay
}
