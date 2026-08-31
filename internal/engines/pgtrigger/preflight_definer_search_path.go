// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"log/slog"
	"sort"
	"strings"
)

// The insecure-SECURITY-DEFINER upgrade door (audit 2026-08-31, SEC-1).
//
// From v0.85.0 through v0.134.0 [renderCaptureDDLFunction] emitted the DDL
// capture function as SECURITY DEFINER with NO `SET search_path` clause,
// unlike its two siblings. `CREATE EVENT TRIGGER` requires superuser, so
// that function is necessarily owned by one — and an unpinned definer
// resolves its unqualified calls against the FIRING session's search_path,
// which belongs to whoever ran the DDL. `jsonb_build_object`'s built-in is
// `VARIADIC "any"` (zero exact argument-type matches); an attacker-created
// `jsonb_build_object(text,text,text,text)` in a schema they can write
// scores two, and a better match beats schema order, so their function wins
// resolution and runs as the superuser owner. One CREATE TABLE by any
// unprivileged user is arbitrary superuser code execution.
//
// The renderer is fixed, but **the fix only reaches a database when
// `sluice trigger setup` re-runs there** (CREATE OR REPLACE FUNCTION
// rewrites proconfig). Upgrading the binary alone leaves the vulnerable
// function installed and the event trigger live. This file is the door
// that tells the operator so.
//
// # WARN, not refuse — and why
//
// The check runs at every CDC open, which is also every warm resume, so a
// refusal would stop already-running syncs the moment the operator upgrades
// the binary — turning a latent privilege-escalation into an immediate
// outage on a source the operator may not be able to re-setup right then.
// The remedy is a seconds-long `sluice trigger setup` re-run that preserves
// the change log, its watermark and the consumer registry; an unmissable
// WARN naming it converges to the same end state without the outage. This
// is DELIBERATE and is the one place in this engine's open path where a
// security finding warns rather than refuses — recorded here rather than
// implied. (If the posture is ever revisited, note that the refusal would
// have to be scoped to installs whose event trigger is actually live: a
// polled-fingerprint install has no DDL function at all and is not exposed.)
//
// # What this door reaches (the gate-scope enumeration, per CLAUDE.md)
//
// All THREE sluice-owned capture functions in the schema — row, truncate
// and DDL — not just the one that shipped vulnerable. The row and truncate
// functions have carried the pin since the engine's first commit
// (`git log -S` on setup.go returns exactly one commit), so no released
// install should trip on them; checking them anyway means the door's name
// matches its coverage, and a future renderer that drops a pin is caught by
// the same probe. NOT reached: functions sluice does not own (an operator's
// own SECURITY DEFINER functions are their business), and the row/truncate
// functions' exposure on an install where they were hand-edited to drop the
// pin AND the body's pg_catalog qualification — the qualification is the
// second belt and the door cannot read a body it did not write.

// insecureDefinerMarker is the grep-stable prefix the WARN carries; the
// pins and the mutation run key on it.
const insecureDefinerMarker = "INSECURE-CAPTURE-FUNCTION"

// definerFunctionState is one sluice-owned function's security shape, as
// read from pg_proc.
type definerFunctionState struct {
	name             string
	securityDefiner  bool // pg_proc.prosecdef
	searchPathPinned bool // proconfig carries a search_path= entry
}

// hijackable reports the exact vulnerable shape: a SECURITY DEFINER
// function whose search_path is NOT pinned, so its unqualified calls
// resolve against the calling session's path. A SECURITY INVOKER function
// runs as the caller and escalates nothing; a pinned definer resolves
// against pg_catalog first with no attacker-writable schema in the path.
func (s definerFunctionState) hijackable() bool {
	return s.securityDefiner && !s.searchPathPinned
}

// warnInsecureCaptureFunctions WARNs when any sluice-owned capture function
// on the source carries the SEC-1 shape. It never fails the caller; a probe
// ERROR also WARNs ("cannot rule it out") rather than silently skipping —
// the SL-1 probe-error discipline shared with [warnDDLDetectionAbsent] and
// the plain-posture arm of [checkReplicaRoleCaptureShapes].
//
// Caller (the sibling-sweep list for this door): [openCDCReader] — the
// shared chokepoint of BOTH stream-open paths ([Engine.OpenCDCReader] warm
// resume and [Engine.OpenSnapshotStream] cold start construct the poller
// through it). `trigger setup` is deliberately NOT a caller: it REPLACES
// every one of these functions, so by the time it returns the shape is
// already repaired and a WARN there would be a false alarm.
func warnInsecureCaptureFunctions(ctx context.Context, db *sql.DB, schema string) {
	// Bounded per the open-path probe convention (audit 2026-08-27 A5;
	// rationale on [openProbeTimeout]): on expiry the error arm below
	// fires the "could not rule it out" WARN, not a silent skip.
	pctx, cancel := context.WithTimeout(ctx, openProbeTimeout)
	defer cancel()
	states, err := loadCaptureFunctionSecurity(pctx, db, schema)
	if err != nil {
		slog.WarnContext(ctx,
			"pgtrigger: "+insecureDefinerMarker+": cannot read the capture functions' SECURITY DEFINER search_path settings from pg_proc; "+
				"if this install predates the SEC-1 fix its superuser-owned DDL capture function can be hijacked by any user who can create a "+
				"function on this database, and this open could not rule that out. Re-run `sluice trigger setup --dsn=...` to replace the "+
				"capture functions with the fixed definitions",
			slog.Any("err", err))
		return
	}
	var bad []string
	for _, s := range states {
		if s.hijackable() {
			bad = append(bad, s.name)
		}
	}
	if len(bad) == 0 {
		return
	}
	sort.Strings(bad)
	slog.WarnContext(ctx,
		"pgtrigger: "+insecureDefinerMarker+": this install predates the SEC-1 fix — its SECURITY DEFINER capture function(s) carry NO "+
			"`SET search_path`, so their unqualified calls resolve against the search_path of whatever session fires them. The DDL capture "+
			"function is owned by a SUPERUSER (CREATE EVENT TRIGGER requires one) and fires on ANY user's DDL, so an unprivileged user who can "+
			"create a function in a reachable schema (the default PUBLIC grant on `public` before PG 15, or any schema they own) can shadow a "+
			"built-in it calls and execute arbitrary SQL AS THE SUPERUSER by running one CREATE TABLE. Upgrading the sluice binary does NOT fix "+
			"an already-installed function: re-run `sluice trigger setup --dsn=... --tables=...` to replace them (CREATE OR REPLACE; the change "+
			"log, its resume watermark and the consumer registry are all preserved, and the stream resumes where it left off)",
		slog.String("schema", schema),
		slog.String("functions", strings.Join(bad, ", ")))
}

// loadCaptureFunctionSecurity reads prosecdef + the search_path proconfig
// entry for whichever of the three sluice-owned capture functions exist in
// the schema. Absent functions simply yield no row (a polled-fingerprint
// install has no DDL function; teardown --keep-data leaves none at all).
func loadCaptureFunctionSecurity(ctx context.Context, db *sql.DB, schema string) ([]definerFunctionState, error) {
	const q = `
SELECT p.proname,
       p.prosecdef,
       EXISTS (
           SELECT 1
             FROM pg_catalog.unnest(COALESCE(p.proconfig, '{}'::text[])) AS cfg
            WHERE cfg LIKE 'search_path=%'
       )
  FROM pg_catalog.pg_proc      p
  JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
 WHERE n.nspname = $1
   AND p.proname IN ($2, $3, $4)
 ORDER BY p.proname`
	rows, err := db.QueryContext(ctx, q, schema, CaptureFunctionRow, CaptureFunctionTruncate, CaptureFunctionDDL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []definerFunctionState
	for rows.Next() {
		var s definerFunctionState
		if err := rows.Scan(&s.name, &s.securityDefiner, &s.searchPathPinned); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
