// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/appliercontrol"
	"sluicesync.dev/sluice/internal/ir"
)

// defaultApplyConcurrency is the conservative adaptive default LANE count W
// the CDC key-hash apply path resolves `--apply-concurrency 0` (unset) to
// (ADR-0106, item 31). It deliberately equals the cold-copy axes'
// auto:4 ([defaultTableParallelism] / [defaultCopyFanoutDegree]) so the whole
// pipeline has one mental model: "sluice fans out ~4-wide by default, bounded
// by the target's connection budget." NOT aggressive — the live PS-10 (1/8
// vCPU) evidence (ADR-0106 Context) showed 4 budget-bounded lanes stay safe
// on the worst-case tiny instance, with per-lane AIMD available to back off.
const defaultApplyConcurrency = 4

// resolveApplyConcurrency maps the operator's [Streamer.ApplyConcurrency]
// field to the effective LANE count for this attempt, applying the ADR-0106
// (item 31) `--table-parallelism`-style contract:
//
//	n  < 0 → 1                      (defensive against bad input)
//	n == 1 → 1                      (explicit serial opt-out, byte-identical
//	                                 to the pre-ADR-0106 default)
//	n  > 1 → n                      (operator override, honored as-is)
//	n == 0 → auto:N                 (unset → the adaptive default)
//
// The raw int alone distinguishes the unset/0 case (→ auto:N) from an
// explicit 1 (→ serial): the CLI passes the operator's value straight
// through, and this resolver — at the STREAMER level, not the CLI — is the
// single place the auto default materialises, so every construction path
// (CLI, tests, broker/chain replay, future programmatic callers) gets the
// same default rather than the Go zero value silently meaning serial (the
// v0.99.51 zero-value-safe-default trap).
//
// auto:N is connection-budget-aware:
//
//   - Postgres target: N = min(defaultApplyConcurrency, budget) where budget
//     comes from the existing connection-slot probe ([ir.TargetConnectionBudgetProber],
//     the same machinery --max-target-connections drives). On a constrained
//     instance the probe yields fewer lanes automatically; if the probe
//     refuses or fails (a managed-engine quirk, an exhausted budget), the
//     resolver degrades to serial (1) rather than refusing here — the
//     cold-start connection-budget preflight owns the loud refusal, and a
//     warm-resume that can't spare lane slots is correct to run serial.
//   - MySQL / PlanetScale-MySQL target (and any engine without a slot
//     probe): N = defaultApplyConcurrency, a fixed conservative ceiling.
//     --max-target-connections is documented inert against engines without a
//     connection-slot model, so there is no budget to probe; PlanetScale
//     per-branch connection limits are generous relative to 4 lanes + 4
//     dedicated backends across every tier.
//
// The resolver never RAISES an explicit operator value — `n > 1` is honored
// verbatim (the operator owns their target's budget), mirroring how
// --apply-concurrency behaved before this ADR. Only the unset case is
// bounded by the probe.
func (s *Streamer) resolveApplyConcurrency(ctx context.Context) int {
	switch n := s.ApplyConcurrency; {
	case n < 0:
		return 1
	case n == 1:
		return 1
	case n > 1:
		return n
	}
	// n == 0: the unset case — resolve the adaptive default.
	return s.autoApplyConcurrency(ctx)
}

// concurrencyHeadroomApproaching is the busiest-of-CPU/mem utilisation at
// which the auto:N lane count is HALVED at startup, a step below the
// high-water mark ([appliercontrol.DefaultTelemetryHighWater]) at which it is
// quartered. ADR-0107 Phase 3 (a): start fewer lanes when the target is
// already hot, rather than piling the default fan-out onto a saturated
// instance and relying purely on the reactive per-lane AIMD to claw it back.
const concurrencyHeadroomApproaching = 0.70

// autoApplyConcurrency computes the adaptive auto:N lane count for an unset
// --apply-concurrency. See [resolveApplyConcurrency] for the per-engine
// policy. Split out so [resolveApplyConcurrency]'s contract switch stays a
// pure mapping and the budget I/O lives in one place. The connection-budget
// base is then clamped by the target's live resource HEADROOM
// ([clampConcurrencyByHeadroom], ADR-0107 Phase 3) when telemetry is wired.
func (s *Streamer) autoApplyConcurrency(ctx context.Context) int {
	// On dry-run we do not open any target connection (no mutation, no probe);
	// report the fixed default so the dry-run plan reflects the policy without
	// I/O. The lanes never actually engage on a dry-run, and we skip the
	// headroom clamp too so the plan reflects the policy, not a transient load.
	if s.DryRun {
		return defaultApplyConcurrency
	}

	return s.clampConcurrencyByHeadroom(ctx, s.budgetBoundedAutoConcurrency(ctx))
}

// budgetBoundedAutoConcurrency is the connection-budget-aware auto:N base (the
// pre-ADR-0107-Phase-3 behaviour): the fixed conservative default on engines
// without a connection-slot model, or the probe-bounded count on Postgres.
func (s *Streamer) budgetBoundedAutoConcurrency(ctx context.Context) int {
	prober, ok := s.Target.(ir.TargetConnectionBudgetProber)
	if !ok {
		// No connection-slot model (today: MySQL / PlanetScale-MySQL). The
		// fixed conservative ceiling stands; --max-target-connections is inert
		// here, so there is nothing to probe.
		return defaultApplyConcurrency
	}

	// Probe for the desired lane count, bounded by --max-target-connections.
	// EffectiveParallelism is clamped to [1, min(CopyBudget, ceiling)], so a
	// constrained instance yields fewer lanes automatically.
	report, err := prober.ProbeTargetConnectionBudget(ctx, s.TargetDSN, defaultApplyConcurrency, s.MaxTargetConnections)
	if err != nil || report.ProbeFailed || report.Refuse {
		// A broken DSN surfaces loudly at the applier open immediately after
		// this; a degraded probe or an exhausted budget should not crash the
		// resolution — degrade to serial. The cold-start preflight owns the
		// loud connection-budget refusal; the apply path runs serial rather
		// than refusing here so a warm-resume into a tight target still runs.
		lanes := 1
		slog.DebugContext(
			ctx, "apply-concurrency auto: connection-budget probe unavailable; defaulting to serial apply",
			slog.String("stream_id", s.resolveStreamID()),
			slog.Int("lanes", lanes),
		)
		return lanes
	}

	lanes := report.EffectiveParallelism
	if lanes < 1 {
		lanes = 1
	}
	if lanes > defaultApplyConcurrency {
		lanes = defaultApplyConcurrency
	}
	return lanes
}

// clampConcurrencyByHeadroom reduces the auto:N base lane count when the
// target's LIVE resource headroom (CPU / memory) is already tight at startup
// (ADR-0107 Phase 3 (a)). It is a STARTUP-only bias: the CDC apply path
// partitions changes by PK-hash across a FIXED number of lanes, so the lane
// COUNT cannot change mid-stream without breaking the same-key→same-lane
// (exactly-once) invariant — the dynamic, per-lane sizing is already owned by
// the per-lane AIMD controller. This only sets the INITIAL count more
// conservatively when the target is hot, so sluice doesn't pile the full
// default fan-out onto a saturated instance and rely purely on the reactive
// back-off to claw it back. Because [resolveApplyConcurrency] re-runs each
// attempt, a transiently-hot target at one start yields more lanes on a later
// warm-resume once headroom recovers.
//
// It is ADVISORY and degrades exactly like every other telemetry consumer: no
// provider, or no FRESH snapshot, or neither CPU nor mem observed ⇒ the base
// is returned unchanged (today's behaviour). It never RAISES the base — an
// explicit operator --apply-concurrency never reaches here (only the unset
// auto:N path does), and a healthy target keeps the full budget-bounded count.
func (s *Streamer) clampConcurrencyByHeadroom(ctx context.Context, base int) int {
	if base <= 1 || s.TargetTelemetry == nil {
		return base
	}
	snap, ok := s.TargetTelemetry.Sample(ctx)
	if !ok {
		return base
	}
	util, known := maxKnownUtil(snap)
	if !known {
		return base
	}

	hw := appliercontrol.DefaultTelemetryHighWater
	var lanes int
	switch {
	case util >= hw:
		// Already saturated — start minimal; per-lane AIMD grows it back if
		// headroom opens up. quarter, floored at 1.
		lanes = base / 4
	case util >= concurrencyHeadroomApproaching:
		// Approaching the mark — halve, floored at 1.
		lanes = base / 2
	default:
		return base // healthy headroom: keep the full budget-bounded base.
	}
	if lanes < 1 {
		lanes = 1
	}
	slog.InfoContext(
		ctx, "apply-concurrency auto: reducing initial lane count — target resource headroom is tight (ADR-0107; advisory, per-lane AIMD still authoritative)",
		slog.String("stream_id", s.resolveStreamID()),
		slog.Int("base_lanes", base),
		slog.Int("lanes", lanes),
		slog.Float64("busiest_util", util),
		slog.Float64("high_water", hw),
	)
	return lanes
}

// maxKnownUtil returns the busiest of the snapshot's observed CPU / memory
// utilisations and whether AT LEAST ONE was known. An unobserved metric never
// counts as 0 (the honesty contract): known=false only when neither CPU nor
// mem was observed, so a partial snapshot still drives the clamp on whichever
// half is present.
func maxKnownUtil(snap ir.TargetHealthSnapshot) (float64, bool) {
	var (
		u     float64
		known bool
	)
	if snap.CPUKnown {
		u = snap.CPUUtil
		known = true
	}
	if snap.MemKnown && (!known || snap.MemUtil > u) {
		u = snap.MemUtil
		known = true
	}
	return u, known
}
