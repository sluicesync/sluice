// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"

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

// autoApplyConcurrency computes the adaptive auto:N lane count for an unset
// --apply-concurrency. See [resolveApplyConcurrency] for the per-engine
// policy. Split out so [resolveApplyConcurrency]'s contract switch stays a
// pure mapping and the budget I/O lives in one place.
func (s *Streamer) autoApplyConcurrency(ctx context.Context) int {
	// On dry-run we do not open any target connection (no mutation, no probe);
	// report the fixed default so the dry-run plan reflects the policy without
	// I/O. The lanes never actually engage on a dry-run.
	if s.DryRun {
		return defaultApplyConcurrency
	}

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
