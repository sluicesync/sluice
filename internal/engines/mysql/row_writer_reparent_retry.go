// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Cold-copy reparent-retry (ADR-0108)
//
// The COPY-phase analog of ADR-0038's apply-phase retry. A bulk
// cold-copy that runs for minutes can outlive a transient target
// PRIMARY REPARENT — e.g. a PlanetScale non-Metal storage auto-grow at
// the ~39 GB boundary triggers a primary reparent, and the in-flight
// INSERT connection dies with a "not serving" / vttablet
// `code = Unavailable` error. Pre-ADR-0108 the RowWriter returned that
// raw driver error unwrapped, the cold-copy process EXITED, and the
// supervisor crash-looped straight back into the still-in-progress
// reparent (the live Track-D finding: 9 relaunches, each re-hitting it).
//
// This file adds a bounded, observable retry around the per-batch flush.
// It deliberately does NOT import internal/pipeline (the writer lives in
// an engine package; the pipeline's ADR-0038 loop is apply-phase only) —
// the backoff shape is re-derived here, small and self-contained.
//
// The load-bearing requirement (see ADR-0108): after a reparent the
// PINNED connection is DEAD. A retry MUST re-acquire a FRESH connection
// from w.db — the pool reconnects to the new primary on the next
// db.Conn() — never reuse the bad one. The caller (the flush closure)
// re-runs the exec + the post-flush SHOW WARNINGS probe on that fresh
// conn.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Cold-copy reparent-retry bounds. Zero-value-safe by construction:
// these are package constants, not config fields, so there is no
// EnableX-defaulting-true trap (the v0.99.51 lesson) — every
// construction path (CLI, tests, broker, future callers) gets the same
// bounds. The envelope is sized to ride a MULTI-STEP PlanetScale storage
// auto-grow (12→39→62→214 GB), where a single big-table grow step can
// stall the write for several minutes:
//
//	100ms → 200ms → 400ms → 800ms → 1.6s → 3.2s → 6.4s → 12.8s →
//	25.6s → 30s (cap) → 30s → 30s → ... (24×)
//
// WALL-CLOCK BOUND (v0.99.103): the terminal bound is ELAPSED TIME
// (coldCopyReparentMaxWallVar, ~30 min), NOT an attempt count. The prior
// 24-attempt cap exhausted on a SINGLE batch during a prolonged grow: the
// ADR-0110 grow-gate adds fast probe cycles (the gate reopens, the lane
// attempts once, re-trips) so the attempt count gets consumed far faster
// than wall-clock, and PS-320-v11/v12 died with one `documents`/`bool_tiny`
// batch burning 24 attempts mid-grow. A wall-clock bound rides a prolonged
// multi-step grow regardless of probe cadence — the robust "don't get stuck
// on a storage threshold" guarantee — while STILL surfacing a genuinely-
// wedged target loudly after ~30 min. coldCopyReparentRetryAttemptsVar
// remains ONLY as a high runaway backstop (a tight-loop guard if backoff
// were ever zero); the wall-clock deadline is the real bound.
//
// Backoff shape is unchanged (100ms → 200ms → … → 30s cap), so the lane
// still backs off between attempts; at the 30s cap a ~30-min window is
// ~60 attempts, well under the backstop.
//
// These are package vars (not consts) ONLY so the unit tests can shrink
// the envelope to keep the suite fast — production NEVER mutates them, so
// the zero-value-safe-default reasoning is unaffected (there is no config
// field and no zero-value path; the values are baked at package init).
// Unlike the ADR-0110 grow-gate's backoff vars, these are read only inside
// the SYNCHRONOUS per-batch retry loop (never a long-lived background
// goroutine that could outlive a test's restore), so the -race lesson that
// drove the gate's per-instance snapshot does not apply here.
var (
	// coldCopyReparentMaxWallVar is the wall-clock ceiling on a single
	// batch's reparent-retry — the REAL terminal bound. ~30 min spans a
	// prolonged multi-step PlanetScale storage auto-grow (12→39→62→214 GB).
	coldCopyReparentMaxWallVar = 30 * time.Minute
	// coldCopyReparentRetryAttemptsVar is now a high RUNAWAY BACKSTOP only
	// (not the operational bound — the wall-clock deadline is). Kept large
	// so the backoff-rate-limited loop is bounded even in a pathological
	// zero-backoff test.
	coldCopyReparentRetryAttemptsVar = 100000
	coldCopyReparentBackoffBaseVar   = 100 * time.Millisecond
	coldCopyReparentBackoffCapVar    = 30 * time.Second
)

// coldCopyReparentBackoff returns the per-attempt backoff for the
// cold-copy reparent-retry loop: exponential doubling from
// coldCopyReparentBackoffBaseVar, capped at coldCopyReparentBackoffCapVar.
// attempt is 1-based (attempt 1 is the first RETRY, i.e. the wait BEFORE
// the second flush try). Mirrors pipeline.computeRetryBackoff's shape
// without the engine RetryHint plumbing (cold-copy has no hint source).
func coldCopyReparentBackoff(attempt int) time.Duration {
	b := coldCopyReparentBackoffBaseVar
	for i := 1; i < attempt; i++ {
		b *= 2
		if b > coldCopyReparentBackoffCapVar {
			return coldCopyReparentBackoffCapVar
		}
	}
	return b
}

// flushWithReparentRetry runs a single cold-copy batch flush with the
// ADR-0108 bounded reparent-retry around it. It is the ONE place the
// retry policy lives so the plain and idempotent paths (and their
// parallel-worker callers) share one shape, one log, one bound.
//
//   - tableName names the table for the WARN/terminal messages.
//   - rows is the row count of the batch being flushed (for the logs).
//   - attempt runs the flush against a CONNECTION. On the first try the
//     caller passes its already-pinned conn (isRetry=false); on every
//     retry this helper re-acquires a FRESH *sql.Conn from w.db (the pool
//     reconnects to the new primary) and passes it in with isRetry=true —
//     so the dead post-reparent conn is NEVER reused. attempt MUST run
//     the exec AND the post-flush warning probe on the conn it is handed.
//     The isRetry flag is load-bearing for the plain path's
//     1062-on-retry tolerance wart (see writeBatchedConn): the SAME
//     byte-identical atomic batch re-applied after a classified transient
//     may collide on a committed-but-unacked prior attempt; that 1062 is
//     provably "already landed" ONLY on a retry. A first-attempt 1062
//     (isRetry=false) stays terminal (real dup-key / dirty target).
//
// The first error is routed through classifyApplierError; the loop
// retries ONLY when it satisfies ir.RetriableError (the same transient
// set the CDC apply path retries — connection-reset / EOF / vttablet
// Unavailable / the new "not serving"/"reparent" text fallback). Any
// non-retriable (terminal) error returns unchanged, exactly as today.
//
// On retry the helper backs off (honoring ctx.Done() for prompt cancel),
// re-acquires a fresh conn, closes it after the attempt, and tries
// again. On budget exhaustion it returns a LOUD terminal error wrapping
// the most recent transient (never silent, never infinite).
func (w *RowWriter) flushWithReparentRetry(
	ctx context.Context,
	tableName string,
	rows int,
	attempt func(conn *sql.Conn, isRetry bool) error,
	firstConn *sql.Conn,
) error {
	// ADR-0110: quiesce with the run's other cold-copy lanes if a
	// coordinated grow-window pause is in effect. Await is a cheap open
	// read when no pause is active (the common case) and returns ctx.Err()
	// promptly on cancel (the no-deadlock contract). nil gate ⇒ instant
	// return (pre-ADR-0110 behaviour). It only changes WHEN this attempt
	// runs, never WHAT it does — the reparent-retry budget below is still
	// the authoritative loud-on-exhaustion floor.
	if err := w.awaitGrowGate(ctx); err != nil {
		return err
	}
	err := attempt(firstConn, false)
	if err == nil {
		return nil
	}

	// WALL-CLOCK BOUND (v0.99.103): the batch retries until it succeeds or
	// this deadline passes — NOT a fixed attempt count. The ADR-0110 gate's
	// fast probe cycles consume attempts faster than wall-clock, so an
	// attempt-count bound exhausted mid-grow (PS-320-v11/v12); a wall-clock
	// deadline rides a prolonged grow regardless of probe cadence.
	deadline := time.Now().Add(coldCopyReparentMaxWallVar)

	for try := 1; ; try++ {
		// Classify the MOST RECENT error. Only a transient (reparent /
		// connection-reset / vttablet-unavailable class) is retriable;
		// everything else — including a real terminal value-fidelity
		// failure or a first-attempt 1062 — returns unchanged.
		var re ir.RetriableError
		if !errors.As(classifyApplierError(err), &re) || !re.Retriable() {
			return err
		}
		// ADR-0110: this lane hit a classified grow-transient — TRIP the
		// shared gate so every sibling cold-copy lane quiesces together for
		// the grow window instead of independently hammering the struggling
		// target. Idempotent + coalescing: the ~W×D lanes that hit the
		// transient at once collapse into one pause window. This lane keeps
		// its own bounded retry below as the floor.
		w.tripGrowGate("mysql cold-copy flush transient: " + err.Error())
		// Terminal on the WALL-CLOCK deadline (the real bound) or the
		// runaway attempt backstop. A genuinely-wedged target surfaces
		// loudly after ~30 min; a transient grow is ridden regardless of
		// how many fast gate-probe cycles elapsed.
		if time.Now().After(deadline) || try >= coldCopyReparentRetryAttemptsVar {
			return fmt.Errorf(
				"mysql: cold-copy into %q: batch flush (%d rows) still failing after riding the reparent-retry window "+
					"(%s wall-clock, %d attempts; the target may be undergoing a prolonged reparent/failover or be genuinely down): %w",
				tableName, rows, coldCopyReparentMaxWallVar, try, err,
			)
		}

		backoff := coldCopyReparentBackoff(try)
		slog.WarnContext(
			ctx, "mysql: cold-copy batch flush hit a transient target error (likely a primary reparent / 'not serving'); "+
				"re-acquiring a fresh connection and retrying",
			slog.String("table", tableName),
			slog.Int("rows", rows),
			slog.Int("attempt", try),
			slog.Duration("elapsed", time.Since(deadline.Add(-coldCopyReparentMaxWallVar))),
			slog.Duration("max_wall", coldCopyReparentMaxWallVar),
			slog.Duration("backoff", backoff),
			slog.String("err", err.Error()),
		)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}

		// ADR-0110: before the retry attempt, Await the coordinated pause
		// again — if the gate is (still) closed for the grow window this
		// lane parks calmly here instead of re-acquiring a conn and
		// hammering the target. Returns promptly on ctx-cancel. nil gate ⇒
		// instant return.
		if aerr := w.awaitGrowGate(ctx); aerr != nil {
			return aerr
		}
		// Re-acquire a FRESH connection — the pinned conn is dead after a
		// reparent; the pool reconnects to the new primary here. NEVER
		// reuse firstConn. A re-acquire failure is itself classified on
		// the next loop iteration (a still-in-progress reparent surfaces
		// the same transient shape), so it rides the budget too.
		conn, acqErr := w.db.Conn(ctx)
		if acqErr != nil {
			err = acqErr
			continue
		}
		err = attempt(conn, true)
		_ = conn.Close()
		if err == nil {
			return nil
		}
	}
}

// awaitGrowGate blocks while the run's shared coordinated-pause gate
// (ADR-0110) is closed and returns ctx.Err() promptly on cancel. A nil
// gate ⇒ instant nil return (pre-ADR-0110 behaviour, byte-for-byte). It
// only changes WHEN a flush attempt runs, never WHAT it does.
func (w *RowWriter) awaitGrowGate(ctx context.Context) error {
	if w.growGate == nil {
		return nil
	}
	return w.growGate.Await(ctx)
}

// tripGrowGate trips the run's shared coordinated-pause gate so sibling
// cold-copy lanes quiesce together for a grow window. A nil gate ⇒ no-op.
// Idempotent + coalescing (see [ir.GrowGate.Trip]).
func (w *RowWriter) tripGrowGate(reason string) {
	if w.growGate == nil {
		return
	}
	w.growGate.Trip(reason)
}
