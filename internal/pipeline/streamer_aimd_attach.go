// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/appliercontrol"
	"sluicesync.dev/sluice/internal/ir"
)

// warnIfApplyBatchSizeRisky emits a single WARN at startup when the
// operator's apply-batch-size + target combination is known to hit
// Vitess's 20s tx-killer under sustained load. GitHub #18 Phase 2:
// the validation-rig observations showed PS-MySQL cross-region
// failed at batch=100 (every batch hit tx-timeout, retry loop fired
// exhaustively), worked at 25-50.
//
// Triggers when the target declares [ir.Capabilities.TransactionKiller]
// (Vitess-backed flavors: planetscale, vitess) AND ApplyBatchSize > 50.
// The check is conservative — we don't try to detect cross-region from
// DSN host inspection (PS hostname formats vary; false negatives are
// better than the maintenance burden of a host-pattern table that
// grows stale). Operators on same-region PS-MySQL hit a benign WARN —
// better than missing the cross-region foot-gun entirely.
//
// Phase 3 (v0.46.0+) will replace this static rail with an AIMD
// controller that auto-discovers the right size per (source,
// target) pair from observed per-batch latency.
func warnIfApplyBatchSizeRisky(ctx context.Context, s *Streamer) {
	if s.Target == nil {
		return
	}
	maybeWarnApplyBatchSizeRisky(ctx, s.Target.Capabilities(), s.Target.Name(), s.ApplyBatchSize)
}

// maybeWarnApplyBatchSizeRisky is the testable core of
// [warnIfApplyBatchSizeRisky] — takes the target capabilities, engine
// name (for the WARN text only), and batch size directly so unit
// tests can exercise the policy without constructing a full Engine
// stub.
func maybeWarnApplyBatchSizeRisky(ctx context.Context, targetCaps ir.Capabilities, targetName string, batchSize int) {
	if !targetCaps.TransactionKiller {
		return
	}
	const riskyThreshold = 50
	if batchSize <= riskyThreshold {
		return
	}
	slog.WarnContext(
		ctx, fmt.Sprintf("apply-batch-size > 50 against a %s target may exceed Vitess's 20s transaction-killer timeout under sustained CDC load", targetName),
		slog.Int("apply_batch_size", batchSize),
		slog.Int("safe_threshold", riskyThreshold),
		slog.String("hint", "if you see frequent 'mysql: applier: batch rollback on error' with 'code = Aborted ... for tx killer rollback', reduce --apply-batch-size to 25-50. See GitHub #18 for the auto-tuning controller planned for a future release."),
	)
}

// maybeAttachAIMDController constructs an AIMD apply-batch-size
// controller (ADR-0052) and threads it onto the applier when:
//
//   - AutoTune is true (the v0.72.0 default; --no-auto-tune sets it
//     to false).
//   - ApplyBatchSize > 1 (the static-cap value; the controller
//     never exceeds this cap).
//   - The applier exposes both [ir.BatchSizeProviderSetter] and
//     [ir.BatchObserverSetter] (both shipping engines do after
//     ADR-0052).
//
// Returns the constructed controller for the metrics server to
// snapshot via AttachAIMDController, or nil when any of the above
// preconditions fails (the static --apply-batch-size cap remains
// the only flush trigger).
//
// Engines without the setters silently skip — the AIMD WARN is
// logged at DEBUG (not INFO) so a custom test stub doesn't drown
// out the operator-facing log surface; production engines all
// implement the setters by construction.
func (s *Streamer) maybeAttachAIMDController(ctx context.Context, applier ir.ChangeApplier, streamID string) *appliercontrol.Controller {
	if !s.AutoTune || s.ApplyBatchSize <= 1 {
		return nil
	}
	// ADR-0104/0105 concurrent key-hash apply: with --apply-concurrency W > 1
	// the applier runs W independent in-order lanes, each needing its OWN AIMD
	// controller (a tx-killer on a slow lane must shrink only that lane). When
	// the applier implements [ir.LaneAIMDSetter] we build W controllers, wire
	// them via SetLaneAIMDControllers, AND park them on s.laneAIMDControllers
	// for [phaseStartMetricsServer] to attach (each lane emitted as its own
	// `lane="N"`-labeled metric series). We return nil because there is no
	// single controller for the serial AttachAIMDController surface on this
	// path. The serial single-controller path below is UNCHANGED for W <= 1.
	// The lane count is the ADR-0106-resolved value (auto:N for an unset
	// --apply-concurrency), so the per-lane controllers match the lanes the
	// applier actually engaged.
	if s.resolvedApplyConcurrency > 1 {
		if laneSetter, ok := applier.(ir.LaneAIMDSetter); ok {
			return s.attachLaneAIMDControllers(ctx, laneSetter, streamID)
		}
		// W > 1 but the engine lacks the per-lane surface: fall through to
		// the single-controller path. (Both shipping engines implement
		// LaneAIMDSetter as of ADR-0105 — MySQL's key-hash lanes and
		// Postgres's lane router — so this is the safety fallback for a
		// future engine that adopts --apply-concurrency without the per-lane
		// surface, not a path the bundled engines take.)
	}
	provSetter, hasProv := applier.(ir.BatchSizeProviderSetter)
	obsSetter, hasObs := applier.(ir.BatchObserverSetter)
	if !hasProv || !hasObs {
		slog.DebugContext(
			ctx, "applier: AIMD controller skipped — engine lacks BatchSizeProviderSetter or BatchObserverSetter",
			slog.String("stream_id", streamID),
		)
		return nil
	}

	target := s.ApplyTuneTargetLatency
	if target <= 0 {
		target = resolveAIMDTargetLatency(s.targetCapsForAIMD())
	}

	// Resume from the shrunk size if a prior runOnce attempt this Run
	// already multiplicative-decreased (the v0.99.69 sustained-tx-killer
	// fix). Zero (the cold-start default) starts at the ceiling. The
	// stored value is always within [1, ceiling] by construction, but
	// clamp defensively so an unexpected value can never exceed the
	// operator's --apply-batch-size cap.
	initial := s.ApplyBatchSize
	if resume := int(s.aimdResumeSize.Load()); resume > 0 && resume < initial {
		initial = resume
		slog.InfoContext(
			ctx, "applier: AIMD resuming at shrunk batch size after a prior transaction-killer decrease this run",
			slog.String("stream_id", streamID),
			slog.Int("resume_size", initial),
			slog.Int("ceiling", s.ApplyBatchSize),
		)
	}

	cfg := appliercontrol.Config{
		StreamID:      streamID,
		EngineName:    s.engineNameForAIMD(),
		Floor:         1,
		Ceiling:       s.ApplyBatchSize,
		InitialSize:   initial,
		TargetLatency: target,
		// Persist every multiplicative decrease so the shrunk size
		// survives a runOnce restart driven by a tx-killer abort. See
		// the aimdResumeSize field doc for the cross-attempt lifecycle.
		OnShrink: func(newSize int) { s.aimdResumeSize.Store(int64(newSize)) },
	}
	ctrl, err := appliercontrol.New(cfg)
	if err != nil {
		slog.WarnContext(
			ctx, "applier: failed to construct AIMD controller; falling back to static apply-batch-size cap",
			slog.String("stream_id", streamID),
			slog.String("err", err.Error()),
		)
		return nil
	}

	provSetter.SetBatchSizeProvider(ctrl)
	obsSetter.SetBatchObserver(ctrl)
	slog.InfoContext(
		ctx, "applier: AIMD apply-batch-size controller engaged",
		slog.String("stream_id", streamID),
		slog.String("engine", cfg.EngineName),
		slog.Int("ceiling", cfg.Ceiling),
		slog.Duration("target_latency", cfg.TargetLatency),
	)
	return ctrl
}

// attachLaneAIMDControllers builds W AIMD controllers — one per ADR-0104/0105
// concurrent apply lane — and wires them onto the applier via
// [ir.LaneAIMDSetter]. Each controller carries the SAME Config as the
// serial single-controller path (Floor 1, Ceiling = --apply-batch-size,
// the resolved target latency) but its OWN OnShrink, so a tx-killer on one
// lane shrinks only that lane's controller. It also parks the controllers on
// s.laneAIMDControllers so [phaseStartMetricsServer] can attach them to the
// metrics server (each surfaced as its own `lane="N"`-labeled series).
// Returns nil: there is no single controller for the serial
// AttachAIMDController surface on this path.
//
// follow-up: per-lane resume-size persistence across a WHOLE-RUN restart is
// deliberately omitted. The in-lane shrink-and-retry (laneApplyLoop) absorbs
// a sustained tx-killer WITHOUT propagating out of runOnce, so the
// cross-runOnce resume-size carry (aimdResumeSize, the serial path's
// v0.99.69 fix) is not load-bearing here. If a future change makes a lane's
// failure escalate to a whole-run restart, add a []atomic.Int64 (one slot
// per lane) mirroring aimdResumeSize and thread each lane's OnShrink to its
// slot; each fresh controller would then resume from its slot rather than
// the ceiling.
func (s *Streamer) attachLaneAIMDControllers(ctx context.Context, setter ir.LaneAIMDSetter, streamID string) *appliercontrol.Controller {
	target := s.ApplyTuneTargetLatency
	if target <= 0 {
		target = resolveAIMDTargetLatency(s.targetCapsForAIMD())
	}
	w := s.resolvedApplyConcurrency
	// Keep the concrete controllers (for the metrics server's Snapshot) AND
	// the interface view (for the LaneAIMDSetter wiring) in lock-step by
	// index, so metric series lane="i" and the applier's lane i are the same
	// controller.
	controllers := make([]*appliercontrol.Controller, 0, w)
	laneSetters := make([]ir.BatchSizeController, 0, w)
	for i := 0; i < w; i++ {
		cfg := appliercontrol.Config{
			StreamID:      streamID,
			EngineName:    s.engineNameForAIMD(),
			Floor:         1,
			Ceiling:       s.ApplyBatchSize,
			InitialSize:   s.ApplyBatchSize,
			TargetLatency: target,
			// No OnShrink resume-size carry on the per-lane path (see the
			// follow-up note above); each controller is self-contained within
			// runOnce, which is sufficient because in-lane retry handles a
			// sustained tx-killer without a whole-run restart.
		}
		ctrl, err := appliercontrol.New(cfg)
		if err != nil {
			slog.WarnContext(
				ctx, "applier: failed to construct per-lane AIMD controller; lanes fall back to the static apply-batch-size cap",
				slog.String("stream_id", streamID),
				slog.Int("lane", i),
				slog.String("err", err.Error()),
			)
			return nil
		}
		controllers = append(controllers, ctrl)
		laneSetters = append(laneSetters, ctrl)
	}
	setter.SetLaneAIMDControllers(laneSetters)
	// Park the concrete controllers for phaseStartMetricsServer to snapshot
	// per lane. Without this the now-default concurrent path emits no AIMD
	// gauges at all (the serial AttachAIMDController surface never engages).
	s.laneAIMDControllers = controllers
	slog.InfoContext(
		ctx, "applier: per-lane AIMD apply-batch-size controllers engaged (ADR-0104 concurrent key-hash apply)",
		slog.String("stream_id", streamID),
		slog.String("engine", s.engineNameForAIMD()),
		slog.Int("lanes_W", w),
		slog.Int("ceiling", s.ApplyBatchSize),
		slog.Duration("target_latency", target),
	)
	return nil
}

// engineNameForAIMD returns the canonical engine name used to label
// the AIMD controller's log lines. Falls back to an empty string
// when the target engine is unset (test fixtures).
func (s *Streamer) engineNameForAIMD() string {
	if s.Target == nil {
		return ""
	}
	return s.Target.Name()
}

// targetCapsForAIMD returns the target engine's declared capabilities
// for the AIMD defaults lookup. Falls back to the zero Capabilities
// when the target engine is unset (test fixtures);
// resolveAIMDTargetLatency then picks the cross-engine default.
func (s *Streamer) targetCapsForAIMD() ir.Capabilities {
	if s.Target == nil {
		return ir.Capabilities{}
	}
	return s.Target.Capabilities()
}

// resolveAIMDTargetLatency returns the engine-default p95 target
// latency per ADR-0052 DP-2:
//
//   - targets declaring [ir.Capabilities.TransactionKiller]
//     (planetscale, vitess): 5s (Vitess 20s tx-killer + 4x headroom)
//   - mysql / postgres / any other target: 10s
//   - zero capabilities (unknown target — typically a test stub): 10s
func resolveAIMDTargetLatency(targetCaps ir.Capabilities) time.Duration {
	if targetCaps.TransactionKiller {
		return 5 * time.Second
	}
	return 10 * time.Second
}
