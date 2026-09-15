//go:build integration || nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

// # The backend census, and why a paid run that stalls must take one
//
// Run 34932058458 (2026-09-15) spent 465 s inside [nekiBackupCoreFromPG]'s
// backup and died on the suite's shared deadline with
//
//	backup: table "nk_restored": read rows: postgres: ReadRows: query failed:
//	context deadline exceeded
//
// The identical read of the identical 20,000 rows across the identical two
// shards had taken 2.0 s in run 34928571469 forty minutes earlier (the
// backup's own `table complete` lines bracket it: 04:29:53.62 → 04:29:55.63).
// So the stall was real, and the log says NOTHING about what the server was
// doing — the fixture database is deleted when the run ends and nothing
// sampled it while it existed. That is the whole cost: a paid dispatch that
// produces a symptom and no evidence, and the next dispatch starts from the
// same place.
//
// [nekiReadBackends] is the sampler. It is deliberately NOT keyed to Neki: a
// router answers through `__neki.stat_get_activity()`, an ordinary PostgreSQL
// through `pg_catalog.pg_stat_activity`. The fallback is what lets the
// per-PR vanilla run exercise the code path rather than shipping a diagnostic
// that has only ever been compiled — see the census leg of
// [TestPostgresSuite_NekiBackupRestorePlumbing].
//
// WHAT THE ROUTER VIEW DOES NOT CARRY, said plainly rather than left to be
// discovered mid-incident: `__neki.stat_get_activity()` has no `wait_event` /
// `wait_event_type` column. Its columns are router_cell, router_uid, pid,
// datid, datname, usesysid, usename, application_name, client_addr,
// client_port, backend_start, xact_start, query_start, state_change, state,
// query, backend_type, ssl*, sidecar_backends. The nearest thing to a wait
// reason is the per-shard detail inside `sidecar_backends`, so that is what
// [nekiBackend.extra] carries on the router path; on vanilla PostgreSQL it
// carries the real `wait_event_type/wait_event` pair.
type nekiBackend struct {
	routerCell  string
	routerUID   string
	pid         int64
	state       string
	backendType string
	appName     string
	// querySecs is the age of the running statement in seconds, or -1 when
	// the backend is not running one.
	querySecs int64
	// xactSecs is the age of the open transaction in seconds, or -1 when the
	// backend is not in one. Paired with state, it is the field that grades
	// the leading hypothesis for the 2026-09-15 stall — see the note on
	// [nekiReadBackends].
	xactSecs int64
	query    string
	// extra is `sidecar_backends` on the router path and
	// `wait_event_type/wait_event` on the vanilla path. Which one it is, is
	// named by the source string [nekiReadBackends] returns.
	extra string
}

// nekiReadBackends samples every backend the target can see.
//
// It returns the rows, the name of the view that answered (so a reader is
// never left guessing whether a real `wait_event` was available), and an
// error only when NEITHER view could be read.
//
// # What to read off it first — the hypothesis this census exists to grade
//
// In run 34932058458 the backup's read of nk_restored hung, while THE SAME
// PROCESS had read every row of the same table out of the same router fifteen
// seconds earlier at normal speed (the independent expected-value reads span
// 05:41:05.74 → 05:41:20.85, against 04:29:34.02 → 04:29:51.27 in the healthy
// run). So the router was not starved and the table was not slow; something
// about the SECOND read differed.
//
// It does. The independent read is a plain autocommit `db.QueryContext`. The
// backup's read goes through [RowReader.ReadRows], which calls
// [pinReadSession] — and on a `*sql.DB` that opens an EXPLICIT TRANSACTION on
// a checked-out connection and runs `SET LOCAL statement_timeout = 0` in it.
// Against a two-shard router that is a scatter read inside a distributed
// transaction, a materially different router path from an autocommit scatter;
// this suite already records that the router treats explicit transactions
// specially (the DDL arm's "commit unexpectedly resulted in rollback"). And
// the pin removes the one thing that would have made the hang fast and loud:
// with `statement_timeout = 0` there is no server-side wall left, so the only
// bound is sluice's own context — which is exactly the shape observed.
//
// That is a HYPOTHESIS, not a finding; the healthy run took the identical path
// in 2.0 s. The census is what will settle it, and `state` + `xactSecs` are
// the two fields that do:
//
//   - state `active` with a long `querySecs` ⇒ the router is genuinely
//     executing the scatter SELECT and the cost is downstream.
//   - state `idle in transaction` with a long `xactSecs` ⇒ the transaction is
//     open and nothing is running on it, which is the distributed-transaction
//     hypothesis above and points at the pin rather than at the data.
//   - no backend of ours at all ⇒ the session was reaped and the client is
//     waiting on a socket nobody will write to.
func nekiReadBackends(ctx context.Context, db *sql.DB) ([]nekiBackend, string, error) {
	const routerQ = `
		SELECT coalesce(router_cell, ''), coalesce(router_uid, ''), pid,
		       coalesce(state, '?'), coalesce(backend_type, '?'), coalesce(application_name, ''),
		       coalesce(extract(epoch FROM (now() - query_start))::bigint, -1),
		       coalesce(extract(epoch FROM (now() - xact_start))::bigint, -1),
		       left(coalesce(query, ''), 120),
		       left(coalesce(sidecar_backends::text, ''), 400)
		FROM __neki.stat_get_activity()`
	const vanillaQ = `
		SELECT '', '', pid,
		       coalesce(state, '?'), coalesce(backend_type, '?'), coalesce(application_name, ''),
		       coalesce(extract(epoch FROM (now() - query_start))::bigint, -1),
		       coalesce(extract(epoch FROM (now() - xact_start))::bigint, -1),
		       left(coalesce(query, ''), 120),
		       coalesce(wait_event_type, '-') || '/' || coalesce(wait_event, '-')
		FROM pg_catalog.pg_stat_activity
		WHERE pid <> pg_catalog.pg_backend_pid()`

	backends, err := nekiScanBackends(ctx, db, routerQ)
	if err == nil {
		return backends, "__neki.stat_get_activity()", nil
	}
	routerErr := err
	backends, err = nekiScanBackends(ctx, db, vanillaQ)
	if err != nil {
		return nil, "", fmt.Errorf("neither backend view could be read: __neki.stat_get_activity(): %v; "+
			"pg_stat_activity: %w", routerErr, err)
	}
	return backends, fmt.Sprintf("pg_catalog.pg_stat_activity (the router view declined: %v)", routerErr), nil
}

func nekiScanBackends(ctx context.Context, db *sql.DB, query string) ([]nekiBackend, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []nekiBackend
	for rows.Next() {
		var b nekiBackend
		if err := rows.Scan(&b.routerCell, &b.routerUID, &b.pid, &b.state, &b.backendType,
			&b.appName, &b.querySecs, &b.xactSecs, &b.query, &b.extra); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// nekiRenderBackends renders a census for a test log.
func nekiRenderBackends(backends []nekiBackend, source string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "backend census from %s — %d backend(s)", source, len(backends))
	for _, s := range backends {
		fmt.Fprintf(&b, "\n  pid=%d state=%q type=%q app=%q query_age=%ds xact_age=%ds\n    query: %s\n    extra: %s",
			s.pid, s.state, s.backendType, s.appName, s.querySecs, s.xactSecs, s.query, s.extra)
	}
	return b.String()
}

// nekiLogBackendCensus samples and logs the census on a context of its OWN.
//
// Its own context is the point: every caller is reacting to something that has
// already blown a budget, so sampling on the spent context would return the
// same `context deadline exceeded` that provoked the sample and the run would
// learn nothing. Failure to sample is LOGGED, never fatal — a diagnostic that
// can fail a run is a diagnostic somebody deletes.
func nekiLogBackendCensus(t *testing.T, db *sql.DB, why string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	backends, source, err := nekiReadBackends(ctx, db)
	if err != nil {
		t.Logf("BACKEND CENSUS (%s): could not be taken: %v", why, err)
		return
	}
	t.Logf("BACKEND CENSUS (%s): %s", why, nekiRenderBackends(backends, source))
}

// nekiCountRouterCopySessions asks the ROUTER how many backends are running a
// COPY into table, and how many of those carry per-shard detail.
//
// This is the independent expected value for the burst's client-side
// inference: it is read from the platform's own activity view rather than
// deduced from what the client did not receive. The two numbers are logged
// side by side and neither is derived from the other.
//
// WHAT THE SECOND NUMBER IS FOR. A router-side session may be holding a client
// COPY it has not placed on any shard — that is precisely the state sessions
// 5–12 were in across three runs. `sidecar_backends` is the per-shard detail,
// and only a COPY that reached a shard can have it, so the split between the
// two counts is the discriminator. It is reported rather than graded, because
// nothing has yet established what the router puts in that column for a
// queued COPY; the first live run under this code is what establishes it.
//
// Failure to take the census is reported as such and never fails the arm — a
// cross-check that can fail a run is one somebody removes.
func nekiCountRouterCopySessions(t *testing.T, db *sql.DB, table string) (running, withSidecars int, note string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	backends, source, err := nekiReadBackends(ctx, db)
	if err != nil {
		return -1, -1, fmt.Sprintf("UNAVAILABLE (%v) — the burst count below has no independent cross-check "+
			"on this run", err)
	}

	// The sidecar count is meaningful ONLY on the router view. On the vanilla
	// fallback `extra` carries `wait_event_type/wait_event`, which is
	// populated for essentially every backend — counting it would report
	// "every COPY reached a shard" on a server that has no shards. Caught by
	// [TestPostgresSuite_NekiCopyBurstApparatus] reporting 1-of-1 against a
	// single-node PostgreSQL; -1 means "not applicable here", never zero,
	// because zero is a claim.
	routerView := strings.HasPrefix(source, "__neki.")
	withSidecars = -1
	if routerView {
		withSidecars = 0
	}

	marker := "COPY " + table
	for _, b := range backends {
		if !strings.Contains(b.query, marker) {
			continue
		}
		running++
		if !routerView {
			continue
		}
		if e := strings.TrimSpace(b.extra); e != "" && e != "null" && e != "[]" && e != "{}" {
			withSidecars++
		}
	}

	sidecarNote := fmt.Sprintf("%d of them carrying sidecar detail", withSidecars)
	if !routerView {
		sidecarNote = "sidecar detail NOT APPLICABLE on this view"
	}
	return running, withSidecars, fmt.Sprintf("%d backend(s) running %q, %s (from %s)",
		running, marker, sidecarNote, source)
}
