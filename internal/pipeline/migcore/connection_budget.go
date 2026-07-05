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

package migcore

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// ResolveTargetCopyParallelism returns the bulk-copy parallelism to use
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
// [ResolveBulkParallelism], so the min(8, NumCPU) default is concrete).
// ceiling is the operator's --max-target-connections (0 = auto only).
//
// The returned [ir.ConnectionBudget] is the raw probe report (zero value
// on the no-prober / degraded / broken-DSN paths). The cross-table pool
// (ADR-0076) reads its CopyBudget to split the budget across the two
// concurrency axes via [ResolveCopyParallelismBudget]; a zero-value
// report there reads as "no measured ceiling".
func ResolveTargetCopyParallelism(
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
		return 0, ir.ConnectionBudget{}, WrapWithHint(PhaseConnect, err)
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
		return 0, ir.ConnectionBudget{}, WrapWithHint(PhaseConnect, report.RefusalError)
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

// ResolveCopyParallelismBudget splits a single connection budget across
// the TWO concurrency axes the cold migrate now drives — cross-table
// (roadmap item 3(a)) and within-table (--bulk-parallelism, ADR-0019) —
// such that the PRODUCT of the two factors never exceeds the budget
// (ADR-0076; roadmap item 3 gotcha 2). Enforcing the ceiling on the
// product, at the SINGLE budget chokepoint, is the load-bearing
// constraint: each table's own CopyParallelismGate is seeded with
// <= withinP tokens and the table pool is capped at tableP, so
// tableP*withinP open target connections is the construction-time
// ceiling — no global shared runtime semaphore is needed.
//
// Inputs:
//   - resolvedWithin is the within-table factor after
//     [ResolveTargetCopyParallelism]'s budget/ceiling cap (= the value
//     that function returns; always >= 1 in practice, clamped here
//     defensively).
//   - requestedTable is the operator's --table-parallelism after the
//     auto/disable resolution ([ResolveTableParallelism]). Always >= 1.
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
func ResolveCopyParallelismBudget(resolvedWithin, requestedTable, copyBudget, ceiling int) (tableP, withinP int) {
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
	// ResolveTargetCopyParallelism, so withinP <= copyBudget holds — and
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

// indexBudgetFraction is the share of the combined copy+index connection
// budget reserved for the overlapped index-build pool (ADR-0077). 0.25
// mirrors pgcopydb's default 4 table-jobs / 4 index-jobs balance from the
// at-scale comparison; the index pool's auto concurrency self-caps at
// indexBuildConcurrencyHardCap (8) anyway, so reserving more than that is
// wasted — hence the [1, IndexBudgetCeiling] clamp.
const (
	indexBudgetFraction = 0.25
	IndexBudgetCeiling  = 8 // == postgres.indexBuildConcurrencyHardCap
)

// SplitCopyAndIndexBudget carves the single measured connection budget
// into a slice for the overlapped index-build pool and the remainder for
// the copy pool, when index builds OVERLAP the bulk copy (ADR-0077,
// roadmap item 3b(a)). Both pools now hold connections open SIMULTANEOUSLY,
// so the combined ceiling has to be enforced at this ONE chokepoint — the
// same single-chokepoint discipline ADR-0076 uses for the copy axes — with
// no runtime semaphore.
//
// Inputs:
//   - copyBudget is report.CopyBudget — the target's measured total
//     connection ceiling for sluice's copy/index work. <1 means "no
//     measured ceiling" (a non-prober target like MySQL, or a degraded
//     probe).
//   - withinParallelism is the within-table copy factor AFTER the budget
//     cap (the value ResolveTargetCopyParallelism returned). It is the
//     minimum the copy axis needs to copy a single table, so the copy
//     slice is never trimmed below it.
//
// Returns (indexBudget, copyBudget'):
//
//   - copyBudget < 1 → (0, 0). There is no measured ceiling to divide, so
//     the overlap split does not engage: the caller keeps the existing
//     unclamped behaviour (MySQL — which overlaps via the post-copy
//     whole-schema fallback anyway — and the degraded-probe path).
//
//   - else: indexBudget = clamp(round(indexBudgetFraction × copyBudget),
//     1, IndexBudgetCeiling); copyBudget' = max(copyBudget − indexBudget,
//     withinParallelism), then indexBudget is trimmed to
//     copyBudget − copyBudget' so the INVARIANT holds:
//
//     indexBudget + copyBudget' <= copyBudget
//
//     i.e. the sum of simultaneously-open copy + index connections never
//     exceeds the measured budget. If trimming would drop indexBudget
//     below 1 (the copy axis alone needs the whole budget to copy one
//     table), the split returns (0, copyBudget) — no slot can be spared
//     for the index pool, so it falls back to the post-copy phase rather
//     than starving copy.
//
// The returned copyBudget' is fed back into ResolveCopyParallelismBudget
// so the copy axes only ever see their slice; indexBudget is handed to
// the SchemaWriter via SetIndexBuildBudget.
func SplitCopyAndIndexBudget(copyBudget, withinParallelism int) (indexBudget, copyBudgetRemaining int) {
	if copyBudget < 1 {
		// No measured ceiling: don't engage the split. Both axes stay
		// unclamped (the pre-ADR-0077 behaviour); MySQL overlaps via the
		// post-copy whole-schema CreateIndexes fallback regardless.
		return 0, 0
	}
	if withinParallelism < 1 {
		withinParallelism = 1
	}

	indexBudget = int(indexBudgetFraction*float64(copyBudget) + 0.5)
	if indexBudget < 1 {
		indexBudget = 1
	}
	if indexBudget > IndexBudgetCeiling {
		indexBudget = IndexBudgetCeiling
	}

	copyBudgetRemaining = copyBudget - indexBudget
	if copyBudgetRemaining < withinParallelism {
		copyBudgetRemaining = withinParallelism
	}

	// Enforce the invariant: copy is satisfied first (it floored at
	// withinParallelism above), so trim the index slice to whatever budget
	// is left. This can only shrink indexBudget, never grow it.
	if indexBudget+copyBudgetRemaining > copyBudget {
		indexBudget = copyBudget - copyBudgetRemaining
	}
	if indexBudget < 1 {
		// No slot can be spared for the index pool without starving copy
		// below one table's worth of connections. Don't overlap; the copy
		// pool keeps the full budget and indexes build in the post-copy
		// phase.
		return 0, copyBudget
	}
	return indexBudget, copyBudgetRemaining
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
