// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
)

// warnCaptureFunctionsExecutableByPublic reports a capture function that
// still carries PostgreSQL's default PUBLIC EXECUTE grant.
//
// THE GAP IT CLOSES (audit 2026-09-06 PRE-TAG-4). v0.145.0 revokes that
// grant at setup, because the capture functions are SECURITY DEFINER and
// any source role able to create a table and a trigger could otherwise
// attach one to a table of its own — named after a synced table — and
// have every row it wrote applied to the target's real table.
//
// An install created BEFORE that release keeps the grant until setup
// re-runs, and nothing said so. The capture-shape door's body arm grades
// `prosrc`, `proconfig` and `prosecdef` and never reads `proacl`, so
// this was a FIFTH reason to re-run `trigger setup` and the only one
// with no signal at all — while the operator docs enumerated four and
// presented the set as closed. A closed enumeration that is missing a
// member is worse than no enumeration, because it stops the reader
// looking.
//
// WARN AND NEVER REFUSE, for two reasons that both matter:
//
//   - An operator may have widened the grant deliberately. Halting a
//     working stream over a posture choice is the false-refusal shape
//     this project spends its time removing.
//   - The reader-side scope check already drops any captured row outside
//     this stream's namespace, so an un-revoked install is not exposed
//     to the write primitive itself. It is exposed to someone being ABLE
//     to write such rows — worth telling the operator, not worth
//     stopping their sync for.
//
// Best-effort by design: a probe error is logged at DEBUG and the open
// proceeds. This arm is advisory, so failing closed would convert an
// advisory into an outage — the opposite of the trade the refusing arms
// beside it make.
//
// The `grantee = 0` test is PostgreSQL's encoding of PUBLIC in an
// aclitem; a NULL `proacl` is the default, which also means PUBLIC holds
// EXECUTE.
func warnCaptureFunctionsExecutableByPublic(ctx, pctx context.Context, db *sql.DB, schema string) {
	const q = `
SELECT p.proname
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
 WHERE n.nspname = $1
   AND p.proname LIKE 'sluice\_capture%'
   AND (p.proacl IS NULL OR EXISTS (
         SELECT 1 FROM pg_catalog.aclexplode(p.proacl) a
          WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE'))
 ORDER BY p.proname`
	rows, err := db.QueryContext(pctx, q, schema)
	if err != nil {
		slog.DebugContext(ctx, "pgtrigger: could not read capture-function ACLs; skipping the PUBLIC-grant advisory",
			slog.String("error", err.Error()))
		return
	}
	defer func() { _ = rows.Close() }()

	var open []string
	for rows.Next() {
		var name string
		if serr := rows.Scan(&name); serr != nil {
			slog.DebugContext(ctx, "pgtrigger: scanning capture-function ACLs failed; skipping the advisory",
				slog.String("error", serr.Error()))
			return
		}
		open = append(open, name)
	}
	if rows.Err() != nil || len(open) == 0 {
		return
	}
	slog.WarnContext(
		ctx, "pgtrigger: CAPTURE-FUNCTION-PUBLIC-EXECUTE: sluice capture functions are still EXECUTable by PUBLIC; re-run `sluice trigger setup --dsn=... --tables=...` to revoke it",
		slog.String("schema", schema),
		slog.String("functions", strings.Join(open, ", ")),
		slog.String("why", "the capture functions are SECURITY DEFINER, so any source role that can create a table and a trigger could attach one to a table of its own named after a synced table and have its rows applied to the target"),
		slog.String("mitigation_in_place", "the reader drops every captured row whose schema is not this stream's (CAPTURE-OUT-OF-SCOPE), so rows written that way are never applied"),
		slog.String("action", "re-run `sluice trigger setup --dsn=... --tables=...`, which revokes EXECUTE from PUBLIC and grants it back to the setup role"),
	)
}
