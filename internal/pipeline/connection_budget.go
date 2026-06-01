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

	"github.com/orware/sluice/internal/ir"
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
func resolveTargetCopyParallelism(
	ctx context.Context,
	target ir.Engine,
	targetDSN string,
	requested, ceiling int,
) (int, error) {
	prober, ok := target.(ir.TargetConnectionBudgetProber)
	if !ok {
		// Engine has no connection-slot model (today: MySQL). The budget
		// step is a no-op; the requested parallelism stands.
		return requested, nil
	}

	report, err := prober.ProbeTargetConnectionBudget(ctx, targetDSN, requested, ceiling)
	if err != nil {
		// A connection-open failure: the operator's target DSN is wrong.
		// Surface it the same as any other Open* — this is not the
		// safety check breaking a working migration, it's a broken DSN.
		return 0, wrapWithHint(PhaseConnect, err)
	}

	if report.ProbeFailed {
		slog.WarnContext(
			ctx, "connection-budget preflight degraded",
			slog.String("reason", report.Warning),
			slog.Int("bulk_parallelism", requested),
		)
		return requested, nil
	}

	if report.Refuse {
		// Loud refusal — never start a copy that can't finish.
		return 0, wrapWithHint(PhaseConnect, report.RefusalError)
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
	return report.EffectiveParallelism, nil
}
