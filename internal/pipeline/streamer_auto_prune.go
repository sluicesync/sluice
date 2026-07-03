// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0137 Phase B — in-stream AUTO-PRUNE of a trigger-CDC source's change-log
// (Bug 165). The trigger engines (sqlite-trigger / d1-trigger / pgtrigger)
// capture every source change into `sluice_change_log` and never reap it, so
// the change-log grows unbounded for the life of a continuous sync (on D1 it is
// billable). Phase A shipped the operator-run `sluice trigger prune`; this
// sidecar makes it automatic so operators don't schedule a cron.
//
// The sidecar mirrors the failure-isolation shape of the ADR-0107 telemetry
// sidecars ([startTargetMetricsHistoryRecorder] / [startStorageHeadroomWatch]):
// a slow-tick goroutine that runs OFF the apply hot path and whose every error
// is logged at WARN and SWALLOWED — a prune failure must NEVER break or stall
// the sync. This is the one deliberate divergence from the Phase-A command,
// which fails LOUD: Phase A is an operator action, Phase B is background
// housekeeping.
//
// # The load-bearing safety rule (ADR-0137, same as Phase A)
//
// Each cadence reads the TARGET's durably-persisted CDC position (via the
// applier's [ir.ChangeApplier.ReadPosition]) and hands that token to the
// source's [ir.ChangeLogPruner], which decodes it with its OWN codec and reaps
// only `id <= appliedLastID - keep`. The durably-applied frontier is the ONLY
// safe lower bound: the source reader's read cursor runs AHEAD of it, so pruning
// on the read cursor (or MAX(id), or a TTL) would delete not-yet-applied rows →
// silent permanent loss on warm-resume. The engine-side decode refuses a FOREIGN
// token loudly, and a non-positive cut is a safe no-op.

const defaultAutoPruneInterval = 5 * time.Minute

// autoPruneGate bounds the auto-prune cadence to at most once per interval. The
// sidecar's base ticker already fires at the interval, so at steady state every
// tick is due; the gate makes the "at most once per interval, skip within"
// contract explicit and unit-testable with an injected clock, and keeps the
// cadence bounded if a future checkpoint-driven nudge is ever wired to prune
// sooner (an ADR-0137 Phase-B extension). It is not concurrency-safe; the single
// sidecar goroutine owns it.
type autoPruneGate struct {
	interval time.Duration
	last     time.Time
}

// due reports whether a prune is allowed at now, advancing the gate when it is.
// The first call (zero last) is always due, so the first prune happens on the
// first tick rather than after two intervals.
func (g *autoPruneGate) due(now time.Time) bool {
	if g.last.IsZero() || now.Sub(g.last) >= g.interval {
		g.last = now
		return true
	}
	return false
}

// startAutoPruneChangeLog spawns the ADR-0137 Phase-B auto-prune sidecar for the
// stream. It mirrors [startTargetMetricsHistoryRecorder]'s shape: a total no-op
// (returns without spawning) unless the operator opted in AND the source is a
// trigger-CDC engine, a single goroutine otherwise, exiting on ctx.Done. The
// caller does not track the goroutine.
//
// No-op when:
//   - AutoPruneChangeLog is not set (the default — pre-Phase-B behaviour), or
//   - the source didn't expose an [ir.ChangeLogPruner] (a non-trigger source:
//     vanilla PG/MySQL/vitess have no change-log ⇒ s.changeLogPruner is nil).
//
// Both preconditions make the zero-value construction (every test / broker /
// future caller that never sets the flag or streams a non-trigger source) a
// total no-op — the safe default by construction.
func (s *Streamer) startAutoPruneChangeLog(ctx context.Context, streamID string, applier ir.ChangeApplier) {
	if !s.AutoPruneChangeLog {
		return
	}
	pruner := s.changeLogPruner
	if pruner == nil {
		return
	}
	interval := s.AutoPruneInterval
	if interval <= 0 {
		interval = defaultAutoPruneInterval
	}
	keep := s.AutoPruneKeep
	if keep < 0 {
		keep = 0
	}
	logger := slog.Default()
	slog.InfoContext(
		ctx, "auto-prune: bounding source change-log growth on a cadence (ADR-0137 Phase B)",
		slog.String("stream_id", streamID),
		slog.Duration("interval", interval),
		slog.Int64("keep", keep),
	)
	gate := &autoPruneGate{interval: interval}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				runAutoPruneTick(ctx, logger, applier, pruner, streamID, keep, gate, now)
			}
		}
	}()
}

// runAutoPruneTick is one cadence of the auto-prune sidecar, pulled out so the
// cadence-gate + failure-isolation semantics are unit-testable without a live
// ticker. When the gate is due it reads the TARGET's durable position and, if a
// position has been persisted, hands the token to the source pruner. Every error
// (position read OR prune) is logged at WARN and SWALLOWED — the sync is never
// affected; the next cadence retries.
func runAutoPruneTick(
	ctx context.Context,
	logger *slog.Logger,
	applier ir.ChangeApplier,
	pruner ir.ChangeLogPruner,
	streamID string,
	keep int64,
	gate *autoPruneGate,
	now time.Time,
) {
	if !gate.due(now) {
		return
	}
	pos, found, err := applier.ReadPosition(ctx, streamID)
	if err != nil {
		logger.WarnContext(
			ctx, "auto-prune: could not read the target's durable position; skipping this cadence, will retry next interval (advisory, sync unaffected)",
			slog.String("stream_id", streamID),
			slog.String("error", err.Error()),
		)
		return
	}
	if !found {
		// No durable frontier persisted yet (cold-start, nothing applied) — there
		// is no safe lower bound, so nothing to prune. Not an error.
		return
	}
	deleted, err := pruner.PruneConsumedChangeLog(ctx, pos.Token, keep)
	if err != nil {
		logger.WarnContext(
			ctx, "auto-prune: change-log prune failed; will retry next interval (advisory, sync unaffected)",
			slog.String("stream_id", streamID),
			slog.Int64("keep", keep),
			slog.String("error", err.Error()),
		)
		return
	}
	if deleted > 0 {
		logger.InfoContext(
			ctx, "auto-prune: reaped durably-applied source change-log rows",
			slog.String("stream_id", streamID),
			slog.Int64("deleted", deleted),
			slog.Int64("keep", keep),
		)
	}
}
