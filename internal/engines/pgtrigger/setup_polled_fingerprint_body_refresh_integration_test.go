//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// C-4 (audit 2026-08-31, sharpened 2026-09-01; CONFIRMED on postgres:16.15
// before it was fixed). A `--allow-polled-fingerprint` re-run is what an
// operator reaches for when the role can no longer create event triggers —
// a demoted owner, a managed provider. The plan then rendered NO capture
// functions, so an event trigger created by an EARLIER privileged install
// kept firing an OLD body: one that predates the SEC-2 evidence protocol
// and suppresses on the bare marker 'on', while setup now arms a random
// nonce. It did not recognise setup's own transaction and recorded op='X'
// markers for sluice's own DDL — including on the change log and the meta
// table themselves — so the next resume refuses sluice's statements as
// operator DDL. That is Bug 257 returning by a different route.
//
// Measured, not assumed: the fix's first cut rendered the bodies but left
// the EMISSION gated on the old condition, and this test is what caught
// that (6 markers -> 3 -> 0 as each half landed).

package pgtrigger

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestSetup_RefreshesCaptureBodiesWhenAnEventTriggerIsLive(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	mustExec := func(stmt string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}

	mustExec(`CREATE TABLE public.t1 (id int PRIMARY KEY, v text)`)
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"t1"}}); err != nil {
		t.Fatalf("privileged Setup: %v", err)
	}

	// Confirm the privileged install really did create the event triggers,
	// or the whole probe is vacuous.
	var ets int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM pg_catalog.pg_event_trigger WHERE evtname IN ($1,$2)`,
		CaptureTriggerDDL, CaptureTriggerDrop).Scan(&ets); err != nil {
		t.Fatalf("count event triggers: %v", err)
	}
	if ets != 2 {
		t.Fatalf("premise gone: the privileged install created %d event triggers, want 2", ets)
	}

	// Simulate the vintage half: replace the DDL capture function body with
	// a PRE-NONCE rendering — the shape shipped before the SEC-2 evidence
	// protocol, which suppressed on the bare marker value 'on'. This is what
	// an install created by an older sluice still carries.
	mustExec(`
CREATE OR REPLACE FUNCTION public.` + CaptureFunctionDDL + `()
RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $legacy$
DECLARE r RECORD;
BEGIN
    IF pg_catalog.current_setting('` + setupSessionGUC + `', true) = 'on' THEN
        RETURN;
    END IF;
    FOR r IN SELECT * FROM pg_catalog.pg_event_trigger_ddl_commands() LOOP
        IF r.object_identity IS NULL THEN
            CONTINUE;
        END IF;
        INSERT INTO public.sluice_change_log
            (txid, schema_name, table_name, op, pk_jsonb, before_jsonb, after_jsonb)
        VALUES (pg_catalog.pg_current_xact_id()::text::bigint,
                COALESCE(r.schema_name,'public'), COALESCE(r.object_identity,'unknown'), 'X',
                pg_catalog.jsonb_build_object('command_tag', r.command_tag), NULL, NULL);
    END LOOP;
END
$legacy$;`)

	// The re-run must happen as a role that genuinely CANNOT create event
	// triggers — the flag is inert for a superuser, which is what made the
	// first cut of this probe measure nothing. Own the objects so the plan
	// can still ALTER them, but stay non-superuser.
	mustExec("CREATE ROLE lowowner LOGIN PASSWORD " + quoteSQLString("lowpw"))
	mustExec("GRANT ALL ON SCHEMA public TO lowowner")
	mustExec("ALTER TABLE public.t1 OWNER TO lowowner")
	mustExec("ALTER TABLE public.sluice_change_log OWNER TO lowowner")
	mustExec("ALTER TABLE public.sluice_change_log_meta OWNER TO lowowner")
	mustExec("ALTER SEQUENCE public.sluice_change_log_id_seq OWNER TO lowowner")
	// The plan CREATE OR REPLACEs every capture function it renders, which
	// requires ownership of each.
	for _, fn := range []string{CaptureFunctionRow, CaptureFunctionTruncate, CaptureFunctionDDL, CaptureFunctionDrop} {
		mustExec("ALTER FUNCTION public." + fn + "() OWNER TO lowowner")
	}
	// The harness boots the container with user/password "test"/"test".
	lowDSN := strings.Replace(dsn, "://test:test@", "://lowowner:lowpw@", 1)
	if lowDSN == dsn {
		t.Fatalf("could not rewrite the DSN to the low-privilege role: %q", dsn)
	}
	var isSuper bool
	lowDB, err := sql.Open("pgx", lowDSN)
	if err != nil {
		t.Fatalf("open as lowowner: %v", err)
	}
	defer func() { _ = lowDB.Close() }()
	if err := lowDB.QueryRowContext(ctx, "SELECT usesuper FROM pg_user WHERE usename = current_user").Scan(&isSuper); err != nil {
		t.Fatalf("lowowner connect: %v (dsn rewrite may have failed: %q)", err, lowDSN)
	}
	if isSuper {
		t.Fatalf("premise gone: the re-run role is a superuser, so --allow-polled-fingerprint is inert")
	}

	// Reset the markers HERE, not earlier: the ownership statements above are
	// themselves TAG-watched ALTER TABLEs, run by the test with the legacy body
	// live, so counting from before them attributes the TEST's own DDL to setup
	// — which is exactly what the first cut of this probe did.
	mustExec(`DELETE FROM public.sluice_change_log WHERE op = 'X'`)

	plan, err := Setup(ctx, lowDSN, SetupOptions{Tables: []string{"t1"}, AllowPolledFingerprint: true})
	if err != nil {
		t.Fatalf("polled-fingerprint Setup: %v", err)
	}
	t.Logf("re-run plan: %d statements, EventTriggerSupported=%v", len(plan.Statements), plan.EventTriggerSupported)

	n := countXRows(t, ctx, db)
	detail := strings.Join(xRowDetail(t, ctx, db), " | ")
	t.Logf("MEASURED: setup's own DDL recorded %d op='X' marker(s): %s", n, detail)
	if n > 0 {
		t.Errorf("C-4 CONFIRMED: a --allow-polled-fingerprint re-run recorded %d marker(s) for sluice's OWN setup DDL "+
			"against a legacy capture body — the next resume refuses sluice's statements as operator DDL (Bug 257's shape). "+
			"Markers: %s", n, detail)
	}
}
