// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// connBudgetReserve is the number of connections sluice holds back from
// the COPY pool when it auto-caps parallelism against a target's
// connection budget (ADR-0116). It covers the long-lived control/CDC
// connection a sync holds alongside the bulk-copy pool, plus operator
// headroom (a `mysql` session to watch progress / intervene). Named so
// the slack is a single greppable source of truth rather than a magic
// number scattered across the formula. 4 mirrors the Postgres engine's
// connBudgetReserve so the two engines apply the same conservative slack;
// over-reserving costs a little copy throughput, under-reserving risks
// exhausting a small target's `max_connections` mid-run (MySQL Error 1040
// "Too many connections").
const connBudgetReserve = 4

// errConnectionBudgetExhausted is the sentinel cause when a target has
// fewer than one connection free for the COPY pool. Wrapped with the
// concrete numbers; tests assert on it via errors.Is without coupling to
// the message text.
var errConnectionBudgetExhausted = errors.New("mysql: target connection budget exhausted")

// connectionBudgetProbe is the raw accounting [probeConnectionBudget]
// collects before [computeConnectionBudget] turns it into an effective
// parallelism. Split out so the math is a pure function unit-testable
// without a database.
//
// roleLimit follows the convention this package adopts: the [unlimited]
// sentinel means "no per-user limit" (MySQL encodes that as 0; sluice
// normalises any <=0 limit to [unlimited] at probe time). maxConnections
// and inUse are always concrete counts.
//
// MySQL has no per-DATABASE connection cap (unlike Postgres'
// datconnlimit), so there is no datLimit field — the
// [ir.ConnectionBudget.DatabaseLimit] is reported as [unlimited].
type connectionBudgetProbe struct {
	maxConnections int // @@max_connections
	inUse          int // Threads_connected (SHOW GLOBAL STATUS)

	// roleLimit is the tighter of the per-user MAX_USER_CONNECTIONS (from
	// mysql.user, permission-gated on managed MySQL) and the global
	// @@max_user_connections default, normalised: <=0 ⇒ [unlimited].
	roleLimit int

	// bufferPoolBytes is @@innodb_buffer_pool_size, used as a
	// no-credential CPU/tier proxy (ADR-0116 Part B). 0 means "could not
	// read" ⇒ the tier cap is not applied.
	bufferPoolBytes int64
}

// connectionBudget is the computed verdict [computeConnectionBudget]
// returns: how many connections are actually free for the COPY pool, and
// the intermediate numbers the operator-facing log / refusal message
// reports.
type connectionBudget struct {
	// CopyBudget is the number of connections available for the bulk-copy
	// pool after subtracting in-use slots and [connBudgetReserve], then
	// folding the buffer-pool tier cap (ADR-0116 Part B). It is the
	// ceiling the effective parallelism clamps to. May be < 1, in which
	// case the caller refuses loudly.
	CopyBudget int

	// Available is the raw free-slot count (min across the global / role
	// limits) before sluice's own reserve. Surfaced in the refusal message
	// so the operator can see how close they are.
	Available int

	// tierCap is the buffer-pool-derived parallelism cap that was applied
	// (0 = not applied, e.g. @@innodb_buffer_pool_size unreadable). Carried
	// for the operator-facing log / refusal so the cap's provenance is
	// visible.
	tierCap int

	probe connectionBudgetProbe
}

// unlimited is the sentinel "no per-user limit" sluice normalises MySQL's
// 0 (and any non-positive MAX_USER_CONNECTIONS / @@max_user_connections)
// to. Mirrors the Postgres engine's -1 catalog sentinel so the shared
// [ir.ConnectionBudget.RoleLimit] convention ("negative ⇒ unlimited")
// holds across engines.
const unlimited = -1

// computeConnectionBudget turns a raw probe into the connection budget,
// applying the connection-budget formula plus the ADR-0116 Part-B tier
// cap:
//
//	global_available = max_connections - in_use
//	role_available   = roleLimit <= 0 ? +inf : roleLimit
//	available        = min(global_available, role_available)
//	copy_budget      = min(available - reserve, tier_cap)
//
// MySQL exposes no cheap per-user current-connection count comparable to
// Postgres' pg_stat_activity-by-usename, so the role term is the per-user
// LIMIT itself (a conservative upper bound on the role's free slots — it
// can only over-state availability when the user already holds many
// connections, which the global Threads_connected term already accounts
// for at the server level). The buffer-pool tier cap is the genuinely
// load-bearing bound on PlanetScale, where connections are abundant but
// CPU is the scarce small-tier resource (ADR-0116 Part B).
//
// It is a pure function (no I/O) so the whole matrix — unlimited role
// limit, tight global, role-capped, tier-capped, refuse-when-<1 — is
// table-unit-testable.
func computeConnectionBudget(p connectionBudgetProbe, reserve int, applyTierCap bool) connectionBudget {
	globalAvailable := p.maxConnections - p.inUse

	available := globalAvailable
	if p.roleLimit != unlimited && p.roleLimit < available {
		available = p.roleLimit
	}

	copyBudget := available - reserve

	// ADR-0116 Part B: fold the buffer-pool tier cap — but ONLY on the
	// PlanetScale flavor (applyTierCap). The buckets are calibrated to
	// PlanetScale's FIXED plan tiers (the live-measured PS-10→PS-160
	// @@innodb_buffer_pool_size sizes), where the buffer pool genuinely proxies
	// the instance's CPU tier. A vanilla MySQL — or a SELF-HOSTED Vitess
	// (FlavorVitess) — sizes its buffer pool to the operator's own hardware, so
	// @@innodb_buffer_pool_size is NOT a tier/CPU signal there and the cap
	// would wrongly throttle parallelism (a 128 MB default vanilla MySQL →
	// cap 2, collapsing parallel backup/restore). The v0.99.121 first cut
	// applied the cap to EVERY MySQL flavor; this gate (v0.99.122) restores
	// non-PlanetScale parallelism. The connection budget (Part A, above) still
	// applies to all flavors — it is a real slot bound, not a tier heuristic.
	// tierCap stays 0 (its "not applied" sentinel) on the non-PlanetScale path.
	tierCap := 0
	if applyTierCap {
		tierCap = bufferPoolParallelismCap(p.bufferPoolBytes)
		if tierCap > 0 && tierCap < copyBudget {
			copyBudget = tierCap
		}
	}

	return connectionBudget{
		CopyBudget: copyBudget,
		Available:  available,
		tierCap:    tierCap,
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

// probeConnectionBudget reads the connection accounting the budget
// formula needs. It is best-effort by contract: a per-user-limit probe
// failure (the common managed-MySQL / PlanetScale case — `mysql.user`
// access is denied) degrades that one term to [unlimited] inside
// [probeRoleLimit] rather than failing the whole probe, so the safety
// check never breaks an otherwise-working migration. Only the two
// always-available server variables (@@max_connections, Threads_connected)
// are hard requirements; a failure reading either returns a wrapped error
// so the caller WARNs and proceeds with the blind pre-budget behaviour.
//
// @@innodb_buffer_pool_size (the ADR-0116 Part-B tier proxy) is also
// best-effort: unreadable ⇒ 0 ⇒ the tier cap is not applied.
func probeConnectionBudget(ctx context.Context, db *sql.DB) (connectionBudgetProbe, error) {
	var p connectionBudgetProbe

	if err := db.QueryRowContext(ctx, `SELECT @@max_connections`).Scan(&p.maxConnections); err != nil {
		return p, fmt.Errorf("probe @@max_connections: %w", err)
	}

	// Threads_connected is a GLOBAL STATUS variable, not a session var, so
	// it is read via SHOW GLOBAL STATUS (a two-column Variable_name / Value
	// row), not SELECT @@. Scan the value into inUse.
	var statusName string
	if err := db.QueryRowContext(ctx, `SHOW GLOBAL STATUS LIKE 'Threads_connected'`).Scan(&statusName, &p.inUse); err != nil {
		return p, fmt.Errorf("probe Threads_connected: %w", err)
	}

	// Per-user connection limit. Best-effort: a failure leaves the term
	// [unlimited] rather than failing the probe.
	p.roleLimit = probeRoleLimit(ctx, db)

	// @@innodb_buffer_pool_size — the no-credential tier proxy (ADR-0116
	// Part B). Best-effort: unreadable ⇒ 0 ⇒ tier cap not applied.
	if err := db.QueryRowContext(ctx, `SELECT @@innodb_buffer_pool_size`).Scan(&p.bufferPoolBytes); err != nil {
		p.bufferPoolBytes = 0
	}

	return p, nil
}

// probeRoleLimit reads the tighter of the connecting user's per-user
// MAX_USER_CONNECTIONS and the server's global @@max_user_connections,
// normalised to the [unlimited] sentinel when no positive cap is found.
//
// MySQL semantics: @@max_user_connections is a non-negative integer where
// 0 means "no global per-user limit"; mysql.user.max_user_connections is a
// per-account override where 0 means "fall back to the global default".
// The effective per-user cap is the tighter positive value of the two.
// Reading mysql.user requires SELECT on the mysql schema, which managed
// MySQL / PlanetScale routinely deny — so that read is best-effort
// (denied ⇒ no per-account override). The global @@max_user_connections
// is always readable.
//
// Never returns an error: a probe that can't determine a tighter limit
// reports [unlimited], the safe (non-refusing) direction — the global
// Threads_connected term still bounds the budget.
func probeRoleLimit(ctx context.Context, db *sql.DB) int {
	limit := unlimited

	// Global default (@@max_user_connections); always readable. 0 ⇒ no
	// global per-user limit.
	var global int
	if err := db.QueryRowContext(ctx, `SELECT @@max_user_connections`).Scan(&global); err == nil && global > 0 {
		limit = global
	}

	// Per-account override (mysql.user.max_user_connections for the
	// CURRENT_USER()). Permission-gated on managed MySQL — a denied read
	// leaves `limit` at whatever the global term produced. CURRENT_USER()
	// returns 'user@host'; SUBSTRING_INDEX splits it so the WHERE matches
	// the catalog's separate User / Host columns.
	const q = `
		SELECT max_user_connections
		FROM mysql.user
		WHERE User = SUBSTRING_INDEX(CURRENT_USER(), '@', 1)
		  AND Host = SUBSTRING_INDEX(CURRENT_USER(), '@', -1)`
	var perUser int
	if err := db.QueryRowContext(ctx, q).Scan(&perUser); err == nil && perUser > 0 {
		if limit == unlimited || perUser < limit {
			limit = perUser
		}
	}

	return limit
}

// ProbeTargetConnectionBudget implements the pipeline's
// [ir.TargetConnectionBudgetProber] capability (discovered structurally,
// not imported; ADR-0116). It opens a short-lived control connection,
// reads the connection accounting, computes the effective COPY
// parallelism for the operator's requested value + ceiling, and returns
// an [ir.ConnectionBudget] report the orchestrator can act on without any
// MySQL-specific knowledge.
//
// requested is the operator's already-resolved --bulk-parallelism;
// ceiling is the --max-target-connections explicit cap (0 = no explicit
// ceiling, auto only). The returned report's EffectiveParallelism is the
// min of (requested, ceiling-if-set, copy_budget), never below 1 unless
// the budget is exhausted (Refuse=true).
//
// A probe failure (the always-available server variables can't be read)
// returns ProbeFailed=true with a populated Warning and a nil error: the
// orchestrator logs the warning and proceeds with the blind requested
// value — the safety check must never be the thing that breaks an
// otherwise-working migration. A per-user-limit denial (the common
// managed/PlanetScale case) is NOT a probe failure: it degrades that one
// term to unlimited inside [probeRoleLimit] and the budget proceeds. Only
// a connection-open failure surfaces as a non-nil error (the operator's
// own DSN is wrong — worth failing on).
func (e Engine) ProbeTargetConnectionBudget(ctx context.Context, dsn string, requested, ceiling int) (ir.ConnectionBudget, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return ir.ConnectionBudget{}, err
	}
	db, err := openDB(ctx, cfg, e.opts.sqlMode)
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
					"if the target has a small max_connections, a wide bulk copy may exhaust its slot budget mid-run",
				err,
			),
		}, nil
	}

	// The buffer-pool tier cap (Part B) is a PlanetScale-tier CPU proxy; it is
	// applied ONLY for the hosted PlanetScale flavor. Vanilla MySQL and
	// self-hosted Vitess size their buffer pool to their own hardware, so the
	// cap's PlanetScale-calibrated buckets don't apply to them (they get the
	// real connection budget, Part A, only). Gated on == FlavorPlanetScale
	// specifically, NOT usesVStream(), so self-hosted Vitess is excluded.
	budget := computeConnectionBudget(probe, connBudgetReserve, e.Flavor == FlavorPlanetScale)

	// Fold the operator's explicit --max-target-connections ceiling into
	// the copy budget: it's an upper bound the auto-cap further reduces,
	// never a raise.
	effectiveBudget := budget.CopyBudget
	if ceiling > 0 && ceiling < effectiveBudget {
		effectiveBudget = ceiling
	}

	report := ir.ConnectionBudget{
		MaxConnections:   probe.maxConnections,
		InUse:            probe.inUse,
		RoleLimit:        probe.roleLimit,
		DatabaseLimit:    unlimited, // MySQL has no per-database connection cap.
		Available:        budget.Available,
		CopyBudget:       budget.CopyBudget,
		RequestedCeiling: ceiling,
	}

	if effectiveBudget < 1 {
		report.Refuse = true
		report.RefusalError = fmt.Errorf(
			"%w: max_connections=%d in_use=%d role_limit=%s tier_cap=%s available=%d reserve=%d need>=1; "+
				"free connections (close idle / orphaned sessions), raise max_connections, or lift the per-user connection limit",
			errConnectionBudgetExhausted,
			probe.maxConnections, probe.inUse,
			connLimitText(probe.roleLimit), tierCapText(budget.tierCap),
			budget.Available, connBudgetReserve,
		)
		return report, nil
	}

	report.EffectiveParallelism, report.Capped = clampParallelism(requested, effectiveBudget)
	return report, nil
}

// connLimitText renders a per-user connection limit for the operator: the
// [unlimited] sentinel becomes "unlimited" rather than a confusing raw
// "-1".
func connLimitText(limit int) string {
	if limit == unlimited {
		return "unlimited"
	}
	return fmt.Sprintf("%d", limit)
}

// tierCapText renders the buffer-pool tier cap for the operator: 0 (not
// applied — @@innodb_buffer_pool_size unreadable) becomes "n/a".
func tierCapText(tierCap int) string {
	if tierCap <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("%d", tierCap)
}
