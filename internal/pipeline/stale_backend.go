// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Stale-backend preflight (connection-resilience Phase 2, item 2). Before
// the cold-start DROP/CREATE and the connection-budget probe, detect
// sluice's OWN orphaned backends on the target — a SIGKILL'd / OOM-killed
// prior run whose server-side `COPY <table> FROM STDIN` backend kept
// running, still holding the target table's lock and consuming a
// connection slot. That orphan blocks the next run's cold-start and can
// exhaust a small target's slot budget.
//
// Detection runs ALWAYS and reports loudly; termination is opt-in via
// --reap-stale-backends (Migrator.ReapStaleBackends / Streamer.
// ReapStaleBackends). The default report-and-proceed posture is
// deliberate: a legitimately-running concurrent sluice process on the
// same target is a real possibility, so the report is shown first and the
// operator decides. The existing connection-budget refusal handles the
// genuinely-can't-proceed case; this preflight is advisory unless the
// operator opts in.
//
// The step is target-engine-agnostic here: it type-asserts the target
// engine to [ir.TargetStaleBackendReaper] and acts on the returned
// report. Engines without a backend model (MySQL) don't implement the
// reaper, so the step is a clean no-op for them. The PG catalog queries
// (pg_stat_activity / pg_locks) that produce the report live in the
// postgres engine — no PG specifics leak here beyond the small report
// struct.

package pipeline

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// preflightStaleBackends runs the stale-backend detector (and, on opt-in,
// the reaper) against the target before the cold-start path opens the
// bulk-copy pool. The contract:
//
//   - If the target engine doesn't implement the reaper (MySQL), no-op.
//   - If the probe fails (catalog quirk / permission gap), log a WARN and
//     proceed — the detector must never break a working migration.
//   - Detection is non-blocking by design: orphans found are reported at
//     WARN (with reaping=false) or INFO (each termination, with
//     reaping=true), then the run proceeds. The connection-budget refusal
//     is the gate that actually stops a run that can't finish; this is
//     advisory.
//   - A connection-open failure (the operator's DSN is wrong) surfaces as
//     an error, the same as every other Open*.
//
// schemas is the set of schema namespaces sluice is about to write into
// (the source-schema target tables plus the control/state schema). reap
// is the operator's --reap-stale-backends flag.
func preflightStaleBackends(
	ctx context.Context,
	target ir.Engine,
	targetDSN string,
	schemas []string,
	reap bool,
) error {
	reaper, ok := target.(ir.TargetStaleBackendReaper)
	if !ok {
		// Engine has no backend model (today: MySQL). No-op.
		return nil
	}

	report, err := reaper.DetectStaleBackends(ctx, targetDSN, schemas, reap)
	if err != nil {
		// A connection-open failure: the operator's target DSN is wrong.
		// Surface it the same as any other Open* — not the safety check
		// breaking a working migration, a broken DSN.
		return migcore.WrapWithHint(migcore.PhaseConnect, err)
	}

	if report.ProbeFailed {
		slog.WarnContext(
			ctx, "stale-backend preflight degraded",
			slog.String("reason", report.Warning),
		)
		return nil
	}

	if len(report.Backends) == 0 {
		slog.DebugContext(ctx, "stale-backend preflight: no orphaned sluice backends found")
		return nil
	}

	// A non-fatal reap failure rides through on the report's Warning even
	// though DetectStaleBackends returned nil error; surface it.
	if report.Warning != "" {
		slog.WarnContext(ctx, "stale-backend preflight", slog.String("warning", report.Warning))
	}

	if reap {
		slog.InfoContext(
			ctx, "stale-backend preflight: reaped orphaned sluice backends",
			slog.Int("reaped_count", len(report.Reaped)),
			slog.Int("detected_count", len(report.Backends)),
		)
		for _, b := range report.Backends {
			slog.InfoContext(
				ctx, "stale-backend detail",
				slog.Int("pid", b.PID),
				slog.String("application_name", b.ApplicationName),
				slog.String("state", b.State),
				slog.Duration("age", b.Age),
				slog.String("lock_relation", b.LockRelation),
				slog.String("lock_mode", b.LockMode),
				slog.Bool("terminated", containsInt(report.Reaped, b.PID)),
			)
		}
		return nil
	}

	// Report-only (default) path: loud WARN listing each orphan and how to
	// act on it. Never blocks — the budget refusal is the real gate.
	slog.WarnContext(
		ctx, "stale sluice backends detected on the target (run with --reap-stale-backends to terminate, "+
			"or clear them manually with pg_terminate_backend); these may be orphaned from a hard-killed prior run "+
			"and can hold a target-table lock or consume connection slots",
		slog.Int("count", len(report.Backends)),
	)
	for _, b := range report.Backends {
		slog.WarnContext(
			ctx, "stale-backend detail",
			slog.Int("pid", b.PID),
			slog.String("application_name", b.ApplicationName),
			slog.String("state", b.State),
			slog.Duration("age", b.Age),
			slog.String("lock_relation", b.LockRelation),
			slog.String("lock_mode", b.LockMode),
		)
	}
	return nil
}

// containsInt reports whether v is in s. Small linear helper for the
// per-backend "was this pid terminated?" log annotation.
func containsInt(s []int, v int) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// targetWriteSchemas collects the distinct schema namespaces sluice is
// about to write into, for the stale-backend detector's lock-scope. It is
// the union of every in-scope table's Schema and the operator's
// --target-schema override (when set). Empty entries (MySQL's flat scope)
// are dropped; the postgres engine folds in the control/state schema
// itself, so an empty result there still watches the control tables.
func targetWriteSchemas(schema *ir.Schema, targetSchema string) []string {
	seen := make(map[string]struct{})
	var out []string
	add := func(s string) {
		if s == "" {
			return
		}
		if _, dup := seen[s]; dup {
			return
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	add(targetSchema)
	if schema != nil {
		for _, t := range schema.Tables {
			add(t.Schema)
		}
	}
	return out
}
