// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Stale-backend detection + opt-in reaping (connection-resilience Phase
// 2, item 2). A SIGKILL'd / OOM-killed / partitioned sluice run leaves
// its server-side backend running on the target: a `COPY <table> FROM
// STDIN` backend keeps blocking on the now-dead client socket, still
// holding the target table's lock and consuming a connection slot. That
// orphan blocks the next run's cold-start DROP/CREATE and can exhaust a
// small target's slot budget — the self-amplifying lockout the
// resilience note (docs/dev/notes/orphaned-backend-resilience.md)
// describes.
//
// This file detects those orphans in the target preflight and — only on
// the operator's explicit --reap-stale-backends opt-in — terminates
// them. Per the loud-failure / contain-Postgres-complexity tenets the
// default is detect-and-report; destruction is opt-in because a
// legitimately-running concurrent sluice process on the same target is a
// real possibility, and the report is shown first so the operator can
// tell the two apart.
//
// Safety bound (non-negotiable): every backend the detector reports — and
// every backend the reaper terminates — is scoped to ALL of
//
//	application_name LIKE 'sluice/%'   -- ours (the Phase 1 label)
//	AND usename = current_user         -- own-role (we can only reap our own)
//	AND pid <> pg_backend_pid()        -- never the current session
//
// A non-superuser may pg_terminate_backend its own backends with no extra
// grant, so the reaper needs no elevated privilege; the scoping never
// reaches another role's or a non-sluice backend.

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Compile-time assertion that the PG engine satisfies the structural
// stale-backend reaper capability the pipeline discovers via type
// assertion. Mirrors the implicit contract the budget prober relies on;
// making it explicit here catches a signature drift at build time rather
// than at the pipeline's runtime type-assert.
var _ ir.TargetStaleBackendReaper = Engine{}

// staleBackendScope is the WHERE predicate that bounds BOTH the detection
// scan and the termination to sluice's own, own-role, non-self backends.
// It is the load-bearing safety bound (see the file header) — defined
// once as a single greppable constant so the detect query and the reap
// query can never drift apart. Any backend this clause does not select is
// never reported and never terminated.
const staleBackendScope = `application_name LIKE 'sluice/%' AND usename = current_user AND pid <> pg_backend_pid()`

// idleInTxStates are the pg_stat_activity.state values that, on their own
// (no held lock required), mark a sluice backend as orphaned: a run died
// mid-transaction and the server hasn't noticed the client is gone. Held
// out as a slice so the detection query and the pure predicate share one
// source of truth.
var idleInTxStates = []string{"idle in transaction", "idle in transaction (aborted)"}

// staleBackendRow is the raw shape the detection scan yields per
// candidate backend, before [staleBackendFromRow] turns it into the
// engine-neutral [ir.StaleBackend]. Split out so the qualify/format logic
// is a pure function unit-testable without a database.
type staleBackendRow struct {
	pid             int
	applicationName string
	state           string          // "" when the catalog reported NULL
	ageSeconds      sql.NullFloat64 // now() - state_change; NULL when state_change is NULL
	lockRelation    string          // qualified schema.relation, "" when no in-scope lock
	lockMode        string          // "" when no in-scope lock
}

// qualifiesAsStale is the pure orphan-signal predicate. A row already
// passed the SQL [staleBackendScope] (ours / own-role / not-self); this
// applies the orphan signal on top: the backend is stale iff it is idle
// in a transaction OR it holds a lock on an in-scope relation. Keeping it
// a pure function lets the unit test exercise the own-role / sluice-label
// / exclude-self / signal matrix without a live catalog.
func qualifiesAsStale(r staleBackendRow) bool {
	for _, s := range idleInTxStates {
		if r.state == s {
			return true
		}
	}
	return r.lockRelation != ""
}

// staleBackendFromRow projects a scanned row onto the engine-neutral
// report shape, converting the float-seconds age to a Duration.
func staleBackendFromRow(r staleBackendRow) ir.StaleBackend {
	var age time.Duration
	if r.ageSeconds.Valid {
		age = time.Duration(r.ageSeconds.Float64 * float64(time.Second))
	}
	return ir.StaleBackend{
		PID:             r.pid,
		ApplicationName: r.applicationName,
		State:           r.state,
		Age:             age,
		LockRelation:    r.lockRelation,
		LockMode:        r.lockMode,
	}
}

// detectStaleBackendsQuery builds the catalog scan. It selects every
// backend matching [staleBackendScope], LEFT-JOINed to the single most
// significant lock it holds on a relation in one of `schemas` (the
// schemas sluice is about to write), and keeps only rows that qualify as
// orphaned (idle-in-transaction OR holding such a lock).
//
// The schema list is parameterised ($1 = text[]) rather than interpolated
// so the scan is injection-safe. An empty schema list degenerates to
// "lock signal never fires" — only the idle-in-transaction signal is
// then in play, which is the correct conservative behaviour (we don't
// know which relations to watch).
//
// The lock subquery joins pg_locks → pg_class → pg_namespace for
// relation-level locks (locktype='relation') the candidate backend holds,
// restricted to the in-scope schemas, and picks one row (highest lock
// mode by a coarse ordering, then relation name) so each backend reports
// at most one representative held lock.
func detectStaleBackendsQuery() string {
	// state_change can be NULL (a brand-new backend); EXTRACT(EPOCH FROM
	// now() - state_change) is then NULL and surfaces as a NULL age,
	// handled by the sql.NullFloat64 scan target.
	return `
SELECT
	a.pid,
	a.application_name,
	COALESCE(a.state, '')                                   AS state,
	EXTRACT(EPOCH FROM (now() - a.state_change))            AS age_seconds,
	COALESCE(l.relname, '')                                 AS lock_relation,
	COALESCE(l.mode, '')                                    AS lock_mode
FROM pg_stat_activity a
LEFT JOIN LATERAL (
	SELECT
		n.nspname || '.' || c.relname AS relname,
		lk.mode                        AS mode
	FROM pg_locks lk
	JOIN pg_class c     ON c.oid = lk.relation
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE lk.pid = a.pid
	  AND lk.locktype = 'relation'
	  AND lk.granted
	  AND n.nspname = ANY ($1)
	ORDER BY
		CASE lk.mode
			WHEN 'AccessExclusiveLock' THEN 8
			WHEN 'ExclusiveLock'       THEN 7
			WHEN 'ShareRowExclusiveLock' THEN 6
			WHEN 'ShareLock'           THEN 5
			WHEN 'ShareUpdateExclusiveLock' THEN 4
			WHEN 'RowExclusiveLock'    THEN 3
			WHEN 'RowShareLock'        THEN 2
			WHEN 'AccessShareLock'     THEN 1
			ELSE 0
		END DESC,
		relname
	LIMIT 1
) l ON true
WHERE ` + staleBackendScope + `
  AND (
		a.state = ANY ($2)
		OR l.relname IS NOT NULL
	)
ORDER BY a.pid
`
}

// scanStaleBackends runs the detection query and returns the orphaned
// backends. The pure [qualifiesAsStale] re-check is belt-and-suspenders
// against a future query edit — the SQL WHERE already filters to
// qualifying rows, but routing every row through the predicate keeps the
// "what counts as stale" decision in exactly one Go function the unit
// test pins.
func scanStaleBackends(ctx context.Context, db *sql.DB, schemas []string) ([]ir.StaleBackend, error) {
	// pq-style text[] bind: pgx accepts a []string for an array param.
	rows, err := db.QueryContext(ctx, detectStaleBackendsQuery(), pgTextArray(schemas), pgTextArray(idleInTxStates))
	if err != nil {
		return nil, fmt.Errorf("scan stale backends: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var found []ir.StaleBackend
	for rows.Next() {
		var r staleBackendRow
		if err := rows.Scan(&r.pid, &r.applicationName, &r.state, &r.ageSeconds, &r.lockRelation, &r.lockMode); err != nil {
			return nil, fmt.Errorf("scan stale-backend row: %w", err)
		}
		if !qualifiesAsStale(r) {
			continue
		}
		found = append(found, staleBackendFromRow(r))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stale-backend rows: %w", err)
	}
	return found, nil
}

// reapStaleBackends terminates each backend in pids via
// pg_terminate_backend, re-applying [staleBackendScope] inside the SAME
// statement so a backend that changed identity (a slot the OS recycled to
// a different role/process between the detect scan and the reap) can never
// be terminated outside the safety bound. The pid list is an additional
// filter, not the sole authority — the scope clause is.
//
// Returns the pids actually terminated (pg_terminate_backend returned
// true for a still-live, still-in-scope backend). A pid that vanished
// between detect and reap simply doesn't appear in the result.
// reapStaleBackendsQuery is the termination statement. pg_terminate_
// backend(pid) returns bool and is in the SELECT *projection*, not the
// WHERE — so it is evaluated only for rows that already passed the WHERE
// scope (WHERE is logically applied before the target list; a qual's
// evaluation order, by contrast, is not guaranteed, and pg_terminate_
// backend is VOLATILE — keeping it out of the qual list removes any risk
// the planner evaluates the kill before the safety predicates).
// [staleBackendScope] bounds the WHERE — the pid list is an additional
// filter, never the sole authority, so a recycled pid can't be terminated
// outside the safety bound. The returned bool reports per-pid success.
// Held as a package constant so the safety test can assert the scope is
// embedded.
const reapStaleBackendsQuery = `
SELECT a.pid, pg_terminate_backend(a.pid) AS terminated
FROM pg_stat_activity a
WHERE a.pid = ANY ($1)
  AND ` + staleBackendScope + `
`

func reapStaleBackends(ctx context.Context, db *sql.DB, pids []int) ([]int, error) {
	if len(pids) == 0 {
		return nil, nil
	}
	rows, err := db.QueryContext(ctx, reapStaleBackendsQuery, pgIntArray(pids))
	if err != nil {
		return nil, fmt.Errorf("reap stale backends: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var reaped []int
	for rows.Next() {
		var (
			pid        int
			terminated bool
		)
		if err := rows.Scan(&pid, &terminated); err != nil {
			return nil, fmt.Errorf("scan reaped pid: %w", err)
		}
		// pg_terminate_backend returns false if the signal couldn't be
		// sent (e.g. the backend vanished between detect and reap) — only
		// report pids actually signalled.
		if terminated {
			reaped = append(reaped, pid)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reaped pids: %w", err)
	}
	sort.Ints(reaped)
	return reaped, nil
}

// pgIntArray wraps a []int for use as a PG int[] bind value with the pgx
// driver via database/sql (sibling to [pgTextArray]). pgx accepts a []int
// directly as an int[] parameter; this names the intent at the call site.
func pgIntArray(i []int) any { return i }

// DetectStaleBackends implements the pipeline's
// [ir.TargetStaleBackendReaper] capability (discovered structurally, not
// imported). It opens a short-lived control connection, scans for
// sluice's own orphaned backends on the target, and — only when reap is
// true — terminates them. The returned report is what the orchestrator
// logs and acts on without any PG-specific knowledge.
//
// A probe failure (a catalog quirk, a permission gap on pg_stat_activity
// / pg_locks) returns ProbeFailed=true with a populated Warning and a nil
// error: the orchestrator logs the warning and proceeds. Only a
// connection-open failure surfaces as a non-nil error (the operator's own
// DSN is wrong — worth failing on, same as every other Open*).
func (Engine) DetectStaleBackends(ctx context.Context, dsn string, schemas []string, reap bool) (ir.StaleBackendReport, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return ir.StaleBackendReport{}, err
	}
	// The DSN's own schema (where the control/state tables live) is always
	// in scope alongside the caller-supplied target schemas, so a lock on
	// sluice_migrate_state / sluice_cdc_state counts as an orphan signal.
	schemas = withControlSchema(schemas, cfg.schema)

	db, err := openDBAs(ctx, cfg, roleControl)
	if err != nil {
		return ir.StaleBackendReport{}, err
	}
	defer func() { _ = db.Close() }()

	found, err := scanStaleBackends(ctx, db, schemas)
	if err != nil {
		return ir.StaleBackendReport{
			ProbeFailed: true,
			Warning: fmt.Sprintf(
				"stale-backend detection probe failed (%v); proceeding without it — "+
					"if a prior run was hard-killed, an orphaned backend may still hold a target-table lock",
				err,
			),
		}, nil
	}

	report := ir.StaleBackendReport{Backends: found}
	if !reap || len(found) == 0 {
		return report, nil
	}

	pids := make([]int, len(found))
	for i, b := range found {
		pids[i] = b.PID
	}
	reaped, err := reapStaleBackends(ctx, db, pids)
	if err != nil {
		// Detection already succeeded; a reap failure degrades to a
		// warning rather than failing the migration — the operator still
		// gets the (un-reaped) report and the budget refusal remains the
		// genuine can't-proceed gate.
		report.Warning = fmt.Sprintf("stale-backend reap failed (%v); the detected backends were left running", err)
		return report, nil
	}
	report.Reaped = reaped
	return report, nil
}

// withControlSchema returns schemas with control unconditionally
// included, de-duplicated. Order is preserved for the caller-supplied
// entries (so the report's lock matching is deterministic); the control
// schema is appended only if not already present.
func withControlSchema(schemas []string, control string) []string {
	out := make([]string, 0, len(schemas)+1)
	seen := make(map[string]struct{}, len(schemas)+1)
	for _, s := range schemas {
		if s == "" {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	if control == "" {
		control = "public"
	}
	if _, dup := seen[control]; !dup {
		out = append(out, control)
	}
	return out
}

// formatStaleBackend renders one orphan for the operator-facing preflight
// report, mirroring the example in the resilience note's item 2.
func formatStaleBackend(b ir.StaleBackend) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "pid=%d application_name=%q", b.PID, b.ApplicationName)
	if b.State != "" {
		fmt.Fprintf(&sb, " state=%q", b.State)
	}
	if b.Age > 0 {
		fmt.Fprintf(&sb, " age=%s", b.Age.Round(time.Second))
	}
	if b.LockRelation != "" {
		fmt.Fprintf(&sb, " holds %s on %s", b.LockMode, b.LockRelation)
	}
	return sb.String()
}
