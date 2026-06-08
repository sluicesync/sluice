// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Connection-budget preflight (connection-resilience item 4). Before the
// bulk-copy connection pool is opened, probe the target's connection-slot
// budget and auto-cap the resolved parallelism so a wide
// --bulk-parallelism never exhausts a small target's slots mid-COPY (the
// opaque `FATAL: remaining connection slots are reserved for roles with
// the SUPERUSER attribute` failure load-testing surfaced against a
// single-node managed Postgres).
//
// The step is target-engine-agnostic at this layer: it type-asserts the
// target engine to [ir.TargetConnectionBudgetProber] and acts on the
// returned report. Engines without a connection-slot model (MySQL) don't
// implement the prober, so the step is a clean no-op for them. The PG
// catalog queries that produce the report live in the postgres engine —
// no PG specifics leak here beyond the small report struct.

package pipeline

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// resolveTargetCopyParallelism returns the bulk-copy parallelism to use
// after the connection-budget preflight. The contract:
//
//   - If the target engine doesn't implement the budget prober (MySQL),
//     return requested unchanged (no-op).
//   - If the probe refuses (budget < 1), return the refusal error — never
//     start a copy that can't finish.
//   - If the probe fails (catalog quirk / permission gap), log a WARN and
//     return requested unchanged — degrade to the blind pre-budget
//     behaviour rather than breaking a working migration.
//   - Otherwise return the capped parallelism, logging an INFO only when a
//     cap was actually applied (N→M).
//
// requested is the already-resolved --bulk-parallelism (post
// [resolveBulkParallelism], so the min(8, NumCPU) default is concrete).
// ceiling is the operator's --max-target-connections (0 = auto only).
//
// The returned [ir.ConnectionBudget] is the raw probe report (zero value
// on the no-prober / degraded / broken-DSN paths). The cross-table pool
// (ADR-0076) reads its CopyBudget to split the budget across the two
// concurrency axes via [resolveCopyParallelismBudget]; a zero-value
// report there reads as "no measured ceiling".
func resolveTargetCopyParallelism(
	ctx context.Context,
	target ir.Engine,
	targetDSN string,
	requested, ceiling int,
) (int, ir.ConnectionBudget, error) {
	prober, ok := target.(ir.TargetConnectionBudgetProber)
	if !ok {
		// Engine has no connection-slot model (today: MySQL). The budget
		// step is a no-op; the requested parallelism stands.
		return requested, ir.ConnectionBudget{}, nil
	}

	report, err := prober.ProbeTargetConnectionBudget(ctx, targetDSN, requested, ceiling)
	if err != nil {
		// A connection-open failure: the operator's target DSN is wrong.
		// Surface it the same as any other Open* — this is not the
		// safety check breaking a working migration, it's a broken DSN.
		return 0, ir.ConnectionBudget{}, wrapWithHint(PhaseConnect, err)
	}

	if report.ProbeFailed {
		slog.WarnContext(
			ctx, "connection-budget preflight degraded",
			slog.String("reason", report.Warning),
			slog.Int("bulk_parallelism", requested),
		)
		// CopyBudget is meaningless on a degraded probe; zero it so the
		// cross-table split reads "no measured ceiling" and honours the
		// operator's --table-parallelism unclamped (blind pre-budget
		// behaviour, symmetric with the within axis below).
		return requested, ir.ConnectionBudget{}, nil
	}

	if report.Refuse {
		// Loud refusal — never start a copy that can't finish.
		return 0, ir.ConnectionBudget{}, wrapWithHint(PhaseConnect, report.RefusalError)
	}

	if report.Capped {
		slog.InfoContext(
			ctx, "capping bulk parallelism: target connection budget",
			slog.Int("requested", requested),
			slog.Int("effective", report.EffectiveParallelism),
			slog.Int("max_connections", report.MaxConnections),
			slog.Int("reserved", report.Reserved),
			slog.Int("in_use", report.InUse),
			slog.Int("available", report.Available),
			slog.Int("copy_budget", report.CopyBudget),
		)
	} else {
		slog.DebugContext(
			ctx, "connection-budget preflight: within budget",
			slog.Int("bulk_parallelism", report.EffectiveParallelism),
			slog.Int("copy_budget", report.CopyBudget),
		)
	}
	return report.EffectiveParallelism, report, nil
}

// resolveCopyParallelismBudget splits a single connection budget across
// the TWO concurrency axes the cold migrate now drives — cross-table
// (roadmap item 3(a)) and within-table (--bulk-parallelism, ADR-0019) —
// such that the PRODUCT of the two factors never exceeds the budget
// (ADR-0076; roadmap item 3 gotcha 2). Enforcing the ceiling on the
// product, at the SINGLE budget chokepoint, is the load-bearing
// constraint: each table's own copyParallelismGate is seeded with
// <= withinP tokens and the table pool is capped at tableP, so
// tableP*withinP open target connections is the construction-time
// ceiling — no global shared runtime semaphore is needed.
//
// Inputs:
//   - resolvedWithin is the within-table factor after
//     [resolveTargetCopyParallelism]'s budget/ceiling cap (= the value
//     that function returns; always >= 1 in practice, clamped here
//     defensively).
//   - requestedTable is the operator's --table-parallelism after the
//     auto/disable resolution ([resolveTableParallelism]). Always >= 1.
//   - copyBudget is report.CopyBudget — the target's measured total
//     connection ceiling. 0 (or negative) means "no measured ceiling"
//     (a non-prober target like MySQL, or a degraded probe).
//   - ceiling is the operator's --max-target-connections (0 = no explicit
//     ceiling). It bounds the PRODUCT, not either axis alone.
//
// Policy (the conservative STATIC split — ADR-0076 option (i)): satisfy
// within-table FIRST (it has better locality and is the well-tuned
// shipped axis), then clamp the table factor to whole multiples of
// withinP that fit the effective product budget:
//
//	withinP = resolvedWithin
//	budget  = min(copyBudget, ceiling)   // 0-sentinels drop out
//	tableP  = clamp(requestedTable, 1, budget / withinP)
//
// Contract for a budget-less, ceiling-less call (budget resolves to 0,
// e.g. a MySQL target with no --max-target-connections): there is no
// measured ceiling to divide, so the operator's requestedTable stands
// unclamped — exactly the pre-(a) behaviour for the within axis, which
// also passes requested through untouched when the prober is absent.
//
// Returns (tableP, withinP); both >= 1, and tableP*withinP <= budget
// whenever budget >= 1.
func resolveCopyParallelismBudget(resolvedWithin, requestedTable, copyBudget, ceiling int) (tableP, withinP int) {
	withinP = resolvedWithin
	if withinP < 1 {
		withinP = 1
	}
	tableP = requestedTable
	if tableP < 1 {
		tableP = 1
	}

	// The effective product budget is the tighter of the measured copy
	// budget and the operator's explicit --max-target-connections. Each
	// 0 means "no limit from this source"; min() over the non-zero ones.
	budget := minNonZeroBudget(copyBudget, ceiling)
	if budget < 1 {
		// No measured ceiling and no explicit ceiling: honour the
		// operator's table request unclamped.
		return tableP, withinP
	}

	// Static split: within-table is satisfied first, the table axis gets
	// whatever whole multiples of withinP remain. Floor at 1 so a budget
	// smaller than withinP still copies one table at a time (budget=1 →
	// 1x1; the within factor was already clamped to copyBudget upstream by
	// resolveTargetCopyParallelism, so withinP <= copyBudget holds — and
	// when ceiling is the tighter bound it likewise clamped within).
	maxTable := budget / withinP
	if maxTable < 1 {
		maxTable = 1
	}
	if tableP > maxTable {
		tableP = maxTable
	}
	return tableP, withinP
}

// minNonZeroBudget returns the smaller of a and b treating 0 (or
// negative) as "no limit". Returns 0 when both are unlimited.
func minNonZeroBudget(a, b int) int {
	switch {
	case a < 1:
		return b
	case b < 1:
		return a
	case a < b:
		return a
	default:
		return b
	}
}
