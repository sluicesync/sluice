// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// connBudgetReserve is the number of connections sluice holds back from
// the COPY pool when it auto-caps parallelism against a target's
// connection budget. It covers the long-lived control/CDC connection a
// sync holds alongside the bulk-copy pool, plus operator headroom (a
// psql session to watch progress / intervene). Named so the slack is a
// single greppable source of truth rather than a magic number scattered
// across the formula. 4 mirrors the "reserve ~3-4" the resilience note
// settled on; it is intentionally conservative — over-reserving costs a
// little copy throughput, under-reserving risks the slot-exhaustion
// FATAL the whole feature exists to prevent.
const connBudgetReserve = 4

// errConnectionBudgetExhausted is the sentinel cause when a target has
// fewer than one connection free for the COPY pool. Wrapped with the
// concrete numbers; tests assert on it via errors.Is without coupling to
// the message text.
var errConnectionBudgetExhausted = errors.New("postgres: target connection budget exhausted")

// connectionBudgetProbe is the raw catalog reading [probeConnectionBudget]
// collects before [computeConnectionBudget] turns it into an effective
// parallelism. Split out so the math is a pure function unit-testable
// without a database.
//
// rolConnLimit and datConnLimit follow the Postgres catalog convention:
// a negative value (-1) means "no per-role / per-database limit". The
// global trio (maxConnections, reserved, currentTotal) is always a
// concrete count.
type connectionBudgetProbe struct {
	maxConnections int // SHOW max_connections
	reserved       int // SHOW superuser_reserved_connections
	currentTotal   int // SELECT count(*) FROM pg_stat_activity

	rolConnLimit int // pg_roles.rolconnlimit for current_user (-1 = unlimited)
	roleCurrent  int // count(*) FROM pg_stat_activity WHERE usename = current_user

	datConnLimit int // pg_database.datconnlimit for current_database (-1 = unlimited)
	dbCurrent    int // count(*) FROM pg_stat_activity WHERE datname = current_database()
}

// connectionBudget is the computed verdict [computeConnectionBudget]
// returns: how many connections are actually free for the COPY pool, and
// the intermediate numbers the operator-facing log / refusal message
// reports.
type connectionBudget struct {
	// CopyBudget is the number of connections available for the bulk-copy
	// pool after subtracting reserved/in-use slots and [connBudgetReserve].
	// It is the ceiling the effective parallelism clamps to. May be < 1,
	// in which case the caller refuses loudly.
	CopyBudget int

	// Available is CopyBudget + reserve — the raw min across the global /
	// role / database limits before sluice's own reserve. Surfaced in the
	// refusal message so the operator can see how close they are.
	Available int

	probe connectionBudgetProbe
}

// unlimited is the sentinel "no per-role / per-database limit" the
// Postgres catalog encodes as a negative rolconnlimit / datconnlimit.
const unlimited = -1

// computeConnectionBudget turns a raw probe into the connection budget,
// applying the formula from the connection-resilience note:
//
//	global_available = max_connections - superuser_reserved_connections - current_total
//	role_available   = rolconnlimit < 0 ? +inf : rolconnlimit - role_current
//	db_available     = datconnlimit  < 0 ? +inf : datconnlimit  - db_current
//	available        = min(global_available, role_available, db_available)
//	copy_budget      = available - reserve
//
// It is a pure function (no I/O) so the whole matrix — unlimited role
// limit, tight global, role-capped, db-capped, refuse-when-<1 — is
// table-unit-testable.
func computeConnectionBudget(p connectionBudgetProbe, reserve int) connectionBudget {
	globalAvailable := p.maxConnections - p.reserved - p.currentTotal

	available := globalAvailable
	if p.rolConnLimit != unlimited {
		if roleAvailable := p.rolConnLimit - p.roleCurrent; roleAvailable < available {
			available = roleAvailable
		}
	}
	if p.datConnLimit != unlimited {
		if dbAvailable := p.datConnLimit - p.dbCurrent; dbAvailable < available {
			available = dbAvailable
		}
	}

	return connectionBudget{
		CopyBudget: available - reserve,
		Available:  available,
		probe:      p,
	}
}

// clampParallelism bounds requested to [1, copyBudget]. It only ever
// reduces requested (the auto-cap is one-directional — it never raises
// the operator's resolved --bulk-parallelism). Returns the effective
// value and whether a cap was applied, so the caller can log the N→M
// transition only when something actually changed.
//
// Precondition: copyBudget >= 1 (the caller refuses loudly before
// calling this when the budget is exhausted).
func clampParallelism(requested, copyBudget int) (effective int, capped bool) {
	effective = requested
	if effective < 1 {
		effective = 1
	}
	if effective > copyBudget {
		return copyBudget, true
	}
	return effective, false
}

// probeConnectionBudget reads the six catalog values the budget formula
// needs. It is best-effort by contract: any individual probe failure
// (a managed-PG quirk, a permission gap on pg_roles, an engine variant
// that doesn't expose superuser_reserved_connections) returns a wrapped
// error so the caller can degrade to the blind pre-budget behaviour with
// a WARN rather than hard-failing a working migration. The safety check
// must never be the thing that breaks a migration.
func probeConnectionBudget(ctx context.Context, db *sql.DB) (connectionBudgetProbe, error) {
	var p connectionBudgetProbe

	if err := db.QueryRowContext(ctx, `SHOW max_connections`).Scan(&p.maxConnections); err != nil {
		return p, fmt.Errorf("probe max_connections: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SHOW superuser_reserved_connections`).Scan(&p.reserved); err != nil {
		return p, fmt.Errorf("probe superuser_reserved_connections: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity`).Scan(&p.currentTotal); err != nil {
		return p, fmt.Errorf("probe pg_stat_activity count: %w", err)
	}
	// rolconnlimit for the connecting role. COALESCE guards the
	// theoretically-impossible no-row case (current_user always has a
	// pg_roles entry) so a NULL scan never panics.
	if err := db.QueryRowContext(
		ctx,
		`SELECT COALESCE(max(rolconnlimit), -1) FROM pg_roles WHERE rolname = current_user`,
	).Scan(&p.rolConnLimit); err != nil {
		return p, fmt.Errorf("probe rolconnlimit: %w", err)
	}
	if err := db.QueryRowContext(
		ctx,
		`SELECT count(*) FROM pg_stat_activity WHERE usename = current_user`,
	).Scan(&p.roleCurrent); err != nil {
		return p, fmt.Errorf("probe role connection count: %w", err)
	}
	if err := db.QueryRowContext(
		ctx,
		`SELECT COALESCE(max(datconnlimit), -1) FROM pg_database WHERE datname = current_database()`,
	).Scan(&p.datConnLimit); err != nil {
		return p, fmt.Errorf("probe datconnlimit: %w", err)
	}
	if err := db.QueryRowContext(
		ctx,
		`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()`,
	).Scan(&p.dbCurrent); err != nil {
		return p, fmt.Errorf("probe database connection count: %w", err)
	}
	return p, nil
}

// ProbeTargetConnectionBudget implements the pipeline's
// TargetConnectionBudgetProber capability (discovered structurally, not
// imported). It opens a short-lived control connection, reads the
// catalog budget, computes the effective COPY parallelism for the
// operator's requested value + ceiling, and returns an
// [ir.ConnectionBudget] report the orchestrator can act on without any
// PG-specific knowledge.
//
// requested is the operator's already-resolved --bulk-parallelism;
// ceiling is the --max-target-connections explicit cap (0 = no explicit
// ceiling, auto only). The returned report's EffectiveParallelism is the
// min of (requested, ceiling-if-set, copy_budget), never below 1 unless
// the budget is exhausted (Refuse=true).
//
// A probe failure returns ProbeFailed=true with a populated Warning and a
// nil error: the orchestrator logs the warning and proceeds with the
// blind requested value. Only a connection-open failure surfaces as a
// non-nil error (the operator's own DSN is wrong — that's worth failing
// on, the same as every other Open*).
func (Engine) ProbeTargetConnectionBudget(ctx context.Context, dsn string, requested, ceiling int) (ir.ConnectionBudget, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return ir.ConnectionBudget{}, err
	}
	db, err := openDBAs(ctx, cfg, roleControl)
	if err != nil {
		return ir.ConnectionBudget{}, err
	}
	defer func() { _ = db.Close() }()

	probe, err := probeConnectionBudget(ctx, db)
	if err != nil {
		return ir.ConnectionBudget{
			ProbeFailed: true,
			Warning: fmt.Sprintf(
				"target connection-budget probe failed (%v); proceeding with the requested bulk parallelism unbounded — "+
					"if the target has a small max_connections, COPY may exhaust its slot budget mid-run",
				err,
			),
		}, nil
	}

	budget := computeConnectionBudget(probe, connBudgetReserve)

	// Fold the operator's explicit --max-target-connections ceiling into
	// the copy budget: it's an upper bound the auto-cap further reduces,
	// never a raise.
	effectiveBudget := budget.CopyBudget
	if ceiling > 0 && ceiling < effectiveBudget {
		effectiveBudget = ceiling
	}

	report := ir.ConnectionBudget{
		MaxConnections:   probe.maxConnections,
		Reserved:         probe.reserved,
		InUse:            probe.currentTotal,
		RoleLimit:        probe.rolConnLimit,
		DatabaseLimit:    probe.datConnLimit,
		Available:        budget.Available,
		CopyBudget:       budget.CopyBudget,
		RequestedCeiling: ceiling,
	}

	if effectiveBudget < 1 {
		report.Refuse = true
		report.RefusalError = fmt.Errorf(
			"%w: max_connections=%d reserved=%d in_use=%d role_limit=%s database_limit=%s available=%d reserve=%d need>=1; "+
				"free connections (close idle / orphaned sessions), raise max_connections, or lift the role/database connection limit",
			errConnectionBudgetExhausted,
			probe.maxConnections, probe.reserved, probe.currentTotal,
			connLimitText(probe.rolConnLimit), connLimitText(probe.datConnLimit),
			budget.Available, connBudgetReserve,
		)
		return report, nil
	}

	report.EffectiveParallelism, report.Capped = clampParallelism(requested, effectiveBudget)
	return report, nil
}

// connLimitText renders a rolconnlimit / datconnlimit for the operator:
// the catalog's -1 sentinel becomes "unlimited" rather than a confusing
// raw "-1".
func connLimitText(limit int) string {
	if limit == unlimited {
		return "unlimited"
	}
	return fmt.Sprintf("%d", limit)
}
