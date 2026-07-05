// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"

	"sluicesync.dev/sluice/internal/appliercontrol"
	"sluicesync.dev/sluice/internal/ir"
)

// DefaultApplyConcurrency is the conservative adaptive default LANE count W
// the CDC key-hash apply path resolves `--apply-concurrency 0` (unset) to
// (ADR-0106, item 31). It deliberately equals the cold-copy axes'
// auto:4 ([DefaultTableParallelism]) so the whole pipeline has one mental
// model: "sluice fans out ~4-wide by default, bounded by the target's
// connection budget." NOT aggressive — the live PS-10 (1/8 vCPU) evidence
// (ADR-0106 Context) showed 4 budget-bounded lanes stay safe on the
// worst-case tiny instance, with per-lane AIMD available to back off.
const DefaultApplyConcurrency = 4

// concurrencyHeadroomApproaching is the busiest-of-CPU/mem utilisation at
// which an AUTO lane/parallelism count is HALVED at startup, a step below the
// high-water mark ([appliercontrol.DefaultTelemetryHighWater]) at which it is
// quartered. ADR-0107 Phase 3 (a): start fewer lanes when the target is
// already hot, rather than piling the default fan-out onto a saturated
// instance and relying purely on the reactive per-lane AIMD to claw it back.
const concurrencyHeadroomApproaching = 0.70

// ResolveReplayApplyConcurrency maps a replay-path operator apply-concurrency
// field to the effective LANE count. It is shared by the broker replay
// ([Streamer.ApplyConcurrency] via the broker) and the chain-restore
// incremental replay ([ChainRestore.ApplyConcurrency]) — both replay manifest
// change-chunks through an up-front applier, so both want the same mapping:
//
//	n  < 0 → 1                    (defensive against bad input)
//	n == 1 → 1                    (explicit serial opt-out)
//	n  > 1 → n                    (operator override, honored as-is)
//	n == 0 → DefaultApplyConcurrency (unset → the fast adaptive default)
//
// Unlike the streamer's resolver this does NOT run a connection-budget probe:
// these paths open a single applier up front and the concurrent path's per-lane
// AIMD backs each lane off independently if the target is tight, so the fixed
// conservative default (4) is safe here without the extra probe round-trip. The
// zero value resolving to the fast default — rather than the Go-zero-meaning-
// serial trap (v0.99.51) — is why every construction path (CLI, tests, future
// callers) gets concurrent replay unless it explicitly opts out with 1.
func ResolveReplayApplyConcurrency(n int) int {
	switch {
	case n < 0:
		return 1
	case n == 0:
		return DefaultApplyConcurrency
	default:
		return n
	}
}

// ApplyApplyConcurrency plumbs the streamer-side resolved apply-concurrency
// value to a target [ir.ChangeApplier] that opts into the ADR-0104 (item
// 23(c), MySQL) / ADR-0105 (item 26, Postgres) key-hash concurrent apply via
// [ir.ApplyConcurrencySetter]. Both shipping engines implement the setter;
// any future engine that doesn't inherits its existing serial apply path
// unchanged (type-assertion fails closed).
//
// lanes is the ADR-0106-resolved value (auto:N for an unset
// --apply-concurrency, an explicit 1 for serial opt-out, N>1 honored), not
// the operator's raw field. lanes <= 1 is a no-op (serial default kept); the
// setter is invoked ONLY for W > 1. Called immediately after each engine
// applier opens, before any ApplyBatch dispatch (sibling to applyExecTimeout
// / [ApplyMaxBufferBytes]).
func ApplyApplyConcurrency(target any, lanes int) {
	if lanes <= 1 {
		return
	}
	if setter, ok := target.(ir.ApplyConcurrencySetter); ok {
		setter.SetApplyConcurrency(lanes)
	}
}

// HeadroomDivisor returns the factor by which an AUTO concurrency / parallelism
// value should be reduced given the target's LIVE resource headroom: 1 =
// healthy (no reduction), 2 = approaching the high-water, 4 = at/over it. ok is
// false (divisor 1) when telemetry is absent / stale / neither CPU nor mem
// observed — the caller leaves its value unchanged (today's behaviour).
// busiestUtil is the deciding utilisation, returned for the caller's log.
//
// This is the SINGLE source of the headroom thresholds, shared by the CDC
// apply lane clamp ([Streamer.clampConcurrencyByHeadroom], ADR-0107 Phase 3)
// and the restore parallelism clamp ([Restore.clampRestoreParallelismByHeadroom],
// ADR-0115) so the two paths can never disagree on what "tight" means. It is
// the engine-neutral telemetry-consumer contract: a partial snapshot still
// drives the verdict on whichever of CPU/mem is present (maxKnownUtil), and an
// unobserved metric never counts as 0.
func HeadroomDivisor(ctx context.Context, tel ir.TargetTelemetry) (divisor int, busiestUtil float64, ok bool) {
	if tel == nil {
		return 1, 0, false
	}
	snap, sok := tel.Sample(ctx)
	if !sok {
		return 1, 0, false
	}
	util, known := maxKnownUtil(snap)
	if !known {
		return 1, 0, false
	}
	switch {
	case util >= appliercontrol.DefaultTelemetryHighWater:
		// Already saturated — quarter the auto fan-out; the reactive
		// controllers (per-lane AIMD / chunk retry) grow it back if headroom
		// opens up.
		return 4, util, true
	case util >= concurrencyHeadroomApproaching:
		// Approaching the mark — halve.
		return 2, util, true
	default:
		// Healthy headroom: no reduction.
		return 1, util, true
	}
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
