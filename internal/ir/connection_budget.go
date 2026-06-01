// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "context"

// ConnectionBudget is a target engine's verdict on how many connections
// the bulk-copy pool may safely open, computed from the target's
// connection-slot accounting. The orchestrator uses it to auto-cap
// parallel COPY against a small target (e.g. a single-node managed
// Postgres) so a wide --bulk-parallelism never exhausts the slot budget
// mid-run — the opaque `FATAL: remaining connection slots are reserved
// for roles with the SUPERUSER attribute` failure the
// connection-resilience work targets.
//
// It is a plain report, not engine-specific: the probe (a set of
// catalog queries) is target-engine knowledge, but the orchestrator only
// reads these fields. Engines whose connection model has no equivalent
// (today: MySQL) simply don't implement [TargetConnectionBudgetProber];
// the orchestrator skips the step for them.
type ConnectionBudget struct {
	// MaxConnections, Reserved, and InUse are the global slot accounting
	// at probe time (Postgres: max_connections,
	// superuser_reserved_connections, count of pg_stat_activity). Carried
	// for the operator-facing log / refusal message.
	MaxConnections int
	Reserved       int
	InUse          int

	// RoleLimit and DatabaseLimit are the per-role / per-database
	// connection caps. A negative value means "unlimited" (the Postgres
	// catalog's -1 sentinel), carried verbatim so the report layer can
	// render it as "unlimited".
	RoleLimit     int
	DatabaseLimit int

	// Available is the raw number of free connections (min across the
	// global / role / database limits) before sluice's own reserve.
	Available int

	// CopyBudget is Available minus the engine's reserve (control + CDC
	// connection + operator headroom). It is the ceiling the bulk-copy
	// parallelism clamps to.
	CopyBudget int

	// RequestedCeiling echoes the operator's --max-target-connections
	// (0 = no explicit ceiling, auto only). Folded into the effective
	// budget as an upper bound the auto-cap further reduces.
	RequestedCeiling int

	// EffectiveParallelism is the resolved bulk-copy parallelism after
	// clamping the requested value to [1, min(CopyBudget, ceiling)].
	// Meaningful only when Refuse is false.
	EffectiveParallelism int

	// Capped reports whether EffectiveParallelism is below the requested
	// value because of the budget — the orchestrator logs the N→M
	// transition only when this is true.
	Capped bool

	// Refuse is true when the target has fewer than one connection free
	// for the COPY pool. The orchestrator surfaces RefusalError and
	// aborts before opening the pool — never start a copy that can't
	// finish (the loud-failure tenet).
	Refuse       bool
	RefusalError error

	// ProbeFailed is true when the budget probe itself failed (a managed
	// engine quirk, a permission gap on the catalog views). The
	// orchestrator logs Warning at WARN and proceeds with the blind,
	// pre-budget behaviour — the safety check must never be the thing
	// that breaks an otherwise-working migration.
	ProbeFailed bool
	Warning     string
}

// TargetConnectionBudgetProber is implemented by target engines that can
// report a [ConnectionBudget] for a DSN (today: Postgres, via catalog
// queries). The orchestrator discovers it structurally with a type
// assertion before opening the bulk-copy pool; engines without a
// connection-slot model (today: MySQL) omit it and the budget step is a
// no-op for them.
//
// requested is the operator's already-resolved bulk parallelism; ceiling
// is the explicit --max-target-connections cap (0 = auto only). The
// returned report's EffectiveParallelism is the clamped result. A probe
// failure is reported via ConnectionBudget.ProbeFailed with a nil error;
// only a connection-open failure (the operator's DSN is wrong) surfaces
// as a non-nil error.
type TargetConnectionBudgetProber interface {
	ProbeTargetConnectionBudget(ctx context.Context, dsn string, requested, ceiling int) (ConnectionBudget, error)
}
