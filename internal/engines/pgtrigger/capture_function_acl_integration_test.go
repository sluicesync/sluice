//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"net/url"
	"strings"
	"testing"
)

// TestCaptureFunctionACL_TakesTheFunctionsOffPublic is audit 2026-09-06
// S-2 layer 2, on a real server.
//
// THE ATTACK IT CLOSES. Every capture function is SECURITY DEFINER and
// PostgreSQL grants EXECUTE on a new function to PUBLIC by default. So a
// low-privilege source role could create a table of its own, attach
// sluice's capture function to it, and have every row it wrote applied
// to the TARGET's real table of that name — a cross-privilege write
// primitive sluice manufactures rather than inherits.
//
// THE TWO FACTS THIS PINS were measured before the fix was written, and
// they are the whole reason the revoke is safe:
//
//  1. An EXISTING capture trigger keeps firing for a role with no
//     EXECUTE. PostgreSQL checks EXECUTE at CREATE TRIGGER time, not at
//     fire time. Without this cell the change would be indistinguishable
//     from one that silently stops capturing.
//  2. That same role can no longer CREATE a trigger on the function.
//
// Cell 1 is the more important of the two: cell 2 failing means the
// attack is open, but cell 1 failing means sluice has broken capture for
// every operator whose application writes as a non-owner role.
func TestCaptureFunctionACL_TakesTheFunctionsOffPublic(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()
	ctx := context.Background()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mustExec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}

	mustExec(`CREATE TABLE acl_t (id int primary key, v text)`)
	mustExec(`CREATE ROLE acl_app LOGIN PASSWORD 'x'`)
	mustExec(`GRANT USAGE, CREATE ON SCHEMA public TO acl_app`)
	mustExec(`GRANT INSERT, SELECT ON acl_t TO acl_app`)

	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"acl_t"}, Schema: "public"}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	// The ACL actually landed. Without this the two behavioural cells
	// below could both pass for unrelated reasons.
	var acl sql.NullString
	if err := db.QueryRowContext(
		ctx,
		`SELECT array_to_string(proacl, ',') FROM pg_proc p
		   JOIN pg_namespace n ON n.oid = p.pronamespace
		  WHERE n.nspname = 'public' AND p.proname = $1`, CaptureFunctionRow,
	).Scan(&acl); err != nil {
		t.Fatalf("read proacl: %v", err)
	}
	if !acl.Valid || acl.String == "" {
		t.Fatal("the capture function still carries PostgreSQL's default ACL (NULL proacl = PUBLIC has " +
			"EXECUTE); setup did not take it off PUBLIC")
	}
	// A PUBLIC grant renders with an EMPTY grantee ("=X/owner"); the
	// owner grant renders as "owner=X/owner". Substring-matching "=X/"
	// therefore matches BOTH, which is how the first cut of this
	// assertion failed against a correctly-revoked function.
	for _, entry := range strings.Split(acl.String, ",") {
		if strings.HasPrefix(strings.TrimSpace(entry), "=") {
			t.Errorf("proacl still carries a PUBLIC execute grant (empty grantee) in %q", acl.String)
		}
	}

	t.Run("an existing capture trigger still fires for a role with no EXECUTE", func(t *testing.T) {
		appDSN := asRole(t, dsn, "acl_app", "x")
		appDB, err := sql.Open("pgx", appDSN)
		if err != nil {
			t.Fatalf("open as acl_app: %v", err)
		}
		defer func() { _ = appDB.Close() }()

		var before int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM `+ChangeLogTable).Scan(&before); err != nil {
			t.Fatalf("count change log: %v", err)
		}
		if _, err := appDB.ExecContext(ctx, `INSERT INTO acl_t VALUES (1, 'a')`); err != nil {
			t.Fatalf("the unprivileged role could not insert at all, so this cell proves nothing "+
				"about capture: %v", err)
		}
		var after int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM `+ChangeLogTable).Scan(&after); err != nil {
			t.Fatalf("count change log: %v", err)
		}
		if after != before+1 {
			t.Fatalf("the capture trigger did NOT fire after the revoke (change log %d -> %d): sluice has "+
				"broken capture for every operator whose application writes as a non-owner role. "+
				"PostgreSQL is supposed to check EXECUTE at CREATE TRIGGER time, not at fire time.",
				before, after)
		}
	})

	t.Run("the same role can no longer attach the capture function", func(t *testing.T) {
		appDSN := asRole(t, dsn, "acl_app", "x")
		appDB, err := sql.Open("pgx", appDSN)
		if err != nil {
			t.Fatalf("open as acl_app: %v", err)
		}
		defer func() { _ = appDB.Close() }()

		if _, err := appDB.ExecContext(ctx, `CREATE TABLE acl_mine (id int primary key, v text)`); err != nil {
			t.Fatalf("acl_app could not create its own table, so the attack shape is not reproduced "+
				"and this cell proves nothing: %v", err)
		}
		_, err = appDB.ExecContext(ctx,
			`CREATE TRIGGER evil AFTER INSERT ON acl_mine FOR EACH ROW EXECUTE FUNCTION `+
				rowFunctionRef("public")+`('["id"]')`)
		if err == nil {
			t.Fatal("an unprivileged role attached sluice's SECURITY DEFINER capture function to its own " +
				"table; every row it writes there becomes a change sluice applies to the target's real " +
				"table of that name")
		}
		if !strings.Contains(strings.ToLower(err.Error()), "permission denied") {
			t.Errorf("CREATE TRIGGER failed, but not on the EXECUTE privilege — so the block may be "+
				"incidental rather than the ACL: %v", err)
		}
	})
}

// asRole rewrites a connection string to authenticate as a different
// role, preserving host, port, database and options.
//
// NOT a string replace of the username: the first cut of this file did
// that and produced "user=orwar database=" — the harness DSN does not
// contain the literal it assumed, and the resulting connection failed
// for a reason unrelated to anything under test, which would have read
// as a passing cell if the assertions had been weaker.
func asRole(t *testing.T, dsn, user, pass string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn %q: %v", dsn, err)
	}
	u.User = url.UserPassword(user, pass)
	return u.String()
}

// TestCaptureFunctionACL_WarnsWhenPublicStillHoldsExecute pins audit
// 2026-09-06 PRE-TAG-4: the advisory that tells an operator their
// pre-v0.145.0 install still carries PUBLIC's EXECUTE grant.
//
// Before this arm existed there was no signal at all. The capture-shape
// door grades the function's body, config and security flag and never
// reads its ACL, so re-running `trigger setup` was a fifth reason with
// no warning attached — while the operator docs enumerated four and
// called the set closed.
//
// Both directions are graded because only one of them is the defect: a
// probe that always warns would be noise on every current install, and a
// probe that never warns is the gap itself.
func TestCaptureFunctionACL_WarnsWhenPublicStillHoldsExecute(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()
	ctx := context.Background()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, `CREATE TABLE acl_warn_t (id int primary key, v text)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"acl_warn_t"}, Schema: "public"}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	openFns := func(t *testing.T) []string {
		t.Helper()
		rows, qerr := db.QueryContext(ctx, `
SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
 WHERE n.nspname = 'public' AND p.proname LIKE 'sluice\_capture%'
   AND (p.proacl IS NULL OR EXISTS (
         SELECT 1 FROM pg_catalog.aclexplode(p.proacl) a
          WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE'))`)
		if qerr != nil {
			t.Fatalf("query acls: %v", qerr)
		}
		defer func() { _ = rows.Close() }()
		var out []string
		for rows.Next() {
			var n string
			if serr := rows.Scan(&n); serr != nil {
				t.Fatalf("scan: %v", serr)
			}
			out = append(out, n)
		}
		return out
	}

	// A CURRENT install: setup just revoked, so nothing should be open to
	// PUBLIC. This is the no-false-fire direction, and it is the one that
	// would make the advisory noise on every healthy stream.
	if open := openFns(t); len(open) != 0 {
		t.Fatalf("a freshly-set-up install still has %v EXECUTable by PUBLIC; the advisory would fire on "+
			"every current install, which is noise rather than signal", open)
	}

	// Now recreate the pre-v0.145.0 posture by handing the grant back,
	// and confirm the probe SEES it. Without this cell the check above
	// would pass against a probe that can never detect anything.
	if _, err := db.ExecContext(ctx,
		`GRANT EXECUTE ON FUNCTION public.`+CaptureFunctionRow+`() TO PUBLIC`); err != nil {
		t.Fatalf("re-grant to PUBLIC: %v", err)
	}
	open := openFns(t)
	if len(open) == 0 {
		t.Fatal("after handing EXECUTE back to PUBLIC the probe still reports nothing open — it cannot " +
			"detect the very posture it exists to report, so the advisory is inert")
	}
	found := false
	for _, n := range open {
		if n == CaptureFunctionRow {
			found = true
		}
	}
	if !found {
		t.Errorf("the probe reports %v but not the row capture function, which is the one just re-granted", open)
	}
}
