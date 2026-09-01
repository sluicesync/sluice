//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The capture-shape door's BODY arm, on a real server (audit 2026-08-31
// SL-5). Three things only a real PostgreSQL can settle:
//
//   - The PREMISE the whole comparison rests on — that `pg_proc.prosrc`
//     comes back byte-identical to the body sluice rendered, and that
//     `proconfig` comes back in the normalized form the door builds from
//     the `SET` clauses. Neither is sluice's behaviour to guarantee, so per
//     the premise-naming rule it is CHECKED here rather than asserted in a
//     comment. It runs for every capture function, because the four have
//     different clause sets.
//   - The ATTACK, executed rather than simulated: `CREATE OR REPLACE` the
//     capture function with a body that records nothing, leaving every
//     trigger in place, and require the open to refuse.
//   - The UPGRADE case it must not break: an install whose definitions are
//     older than the binary but still match what its own setup recorded
//     keeps streaming, with the WARN.
package pgtrigger

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestCaptureFunctionBodyDoor(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE body_t (id BIGINT PRIMARY KEY, note TEXT)`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	setup := func(t *testing.T) {
		t.Helper()
		if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"body_t"}}); err != nil {
			t.Fatalf("Setup: %v", err)
		}
	}
	openWantClean := func(t *testing.T) string {
		t.Helper()
		var logs string
		logs = captureWarnLogs(t, func() {
			r, err := openCDCReader(ctx, dsn, "")
			if err != nil {
				t.Fatalf("CDC open refused: %v", err)
			}
			_ = r.(*CDCReader).Close()
		})
		return logs
	}
	openWantRefusal := func(t *testing.T, wantAll ...string) {
		t.Helper()
		r, err := openCDCReader(ctx, dsn, "")
		if err == nil {
			_ = r.(*CDCReader).Close()
			t.Fatalf("CDC open succeeded; want a refusal containing %q", wantAll)
		}
		for _, want := range wantAll {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal missing %q:\n%v", want, err)
			}
		}
	}

	setup(t)

	t.Run("PREMISE: pg_proc gives back exactly what the renderer emitted, for every capture function", func(t *testing.T) {
		installed, err := loadInstalledCaptureFunctionShapes(ctx, db, "public")
		if err != nil {
			t.Fatalf("read installed shapes: %v", err)
		}
		expected := expectedCaptureFunctionShapes("public")
		if len(installed) != 4 {
			t.Fatalf("read %d capture functions; want 4 (row, truncate, ddl, drop) — the premise check would be vacuous", len(installed))
		}
		for name, got := range installed {
			if !matchesAnyShape(got, expected[name]) {
				want := expected[name][0]
				t.Errorf("%s: the server's stored definition does not match this binary's render.\n"+
					"  body equal: %t\n  definer got=%t want=%t\n  settings got=%v want=%v",
					name, got.body == want.body, got.definer, want.definer, got.settings, want.settings)
			}
		}
		// Anti-vacuity for the proconfig half specifically: the GUC pins
		// must have SURVIVED the round trip, not merely compared equal
		// because both sides were empty.
		row := installed[CaptureFunctionRow]
		for _, want := range []string{"bytea_output=hex", "extra_float_digits=3", "search_path=pg_catalog, pg_temp"} {
			if !containsString(row.settings, want) {
				t.Errorf("%s proconfig = %v; missing %q — the normalization the door applies does not match the server's storage form", CaptureFunctionRow, row.settings, want)
			}
		}
	})

	t.Run("a healthy install opens clean and silent", func(t *testing.T) {
		if logs := openWantClean(t); strings.Contains(logs, staleCaptureFunctionMarker) {
			t.Errorf("a freshly set-up install WARNed about its own capture functions:\n%s", logs)
		}
	})

	t.Run("THE ATTACK: a capture function replaced with a body that records nothing refuses", func(t *testing.T) {
		// Every trigger stays in place, correctly named, correctly shaped,
		// pointing at a function of the right name — the exact state the
		// door passed before this arm existed.
		applyPGSQL(t, dsn, `
CREATE OR REPLACE FUNCTION public.sluice_capture_change()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $evil$
BEGIN
    RETURN NULL;
END
$evil$;`)
		openWantRefusal(t, CaptureFunctionRow, "records NOTHING")

		// Ground truth that the refusal is not theoretical: with the gutted
		// function installed, a real INSERT records no change-log row.
		applyPGSQL(t, dsn, `INSERT INTO body_t VALUES (1, 'lost')`)
		if ops := readCapturedOps(t, ctx, db, "body_t"); len(ops) != 0 {
			t.Fatalf("captured %v; the gutted function should record nothing — the fixture is not reproducing the defect", ops)
		}

		// The prescribed remedy runs.
		setup(t)
		if logs := openWantClean(t); strings.Contains(logs, staleCaptureFunctionMarker) {
			t.Errorf("after the repair re-run the door still reports drift:\n%s", logs)
		}
	})

	t.Run("THE ATTACK, class member 2: a gutted function hidden behind a same-named OVERLOAD still refuses", func(t *testing.T) {
		// The subtest above executes the REPRESENTATIVE attack. This is the
		// class member one CREATE FUNCTION away from it, and it defeated the
		// door as originally shipped: PostgreSQL permits overloading, so
		// after gutting the real 0-arg function an adversary plants a
		// same-named 1-arg decoy carrying a healthy body. A read scoped by
		// proname alone returned both rows and the map collapse kept the
		// last one, so the door graded the DECOY, saw a body that records,
		// and passed — while the trigger went on executing the gutted 0-arg
		// function. Found by the pre-publish value-fidelity review of
		// v0.137.0. The arity scope on the read is what closes it.
		//
		// check_function_bodies=off is what lets the decoy be stored: it is
		// a RETURNS void function, so its body would not otherwise validate.
		applyPGSQL(t, dsn, `
CREATE OR REPLACE FUNCTION public.sluice_capture_change()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $evil$
BEGIN
    RETURN NULL;
END
$evil$;
SET check_function_bodies = off;
CREATE OR REPLACE FUNCTION public.sluice_capture_change(decoy text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
SET bytea_output = hex
SET extra_float_digits = 3
AS $decoy$
BEGIN
    INSERT INTO public.sluice_change_log
        (txid, schema_name, table_name, op, pk_jsonb, before_jsonb, after_jsonb)
    VALUES
        (pg_catalog.pg_current_xact_id()::text::bigint, TG_TABLE_SCHEMA, TG_TABLE_NAME, 'I', '{}'::jsonb, NULL, NULL);
    RETURN;
END
$decoy$;
RESET check_function_bodies;`)

		// Anti-vacuity: the decoy must really be installed alongside the
		// real function, or this is just the previous subtest again.
		var overloads int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_catalog.pg_proc p
  JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
 WHERE n.nspname = 'public' AND p.proname = $1`, CaptureFunctionRow).Scan(&overloads); err != nil {
			t.Fatalf("count overloads: %v", err)
		}
		if overloads != 2 {
			t.Fatalf("pg_proc carries %d definitions of %s; want 2 (the gutted real one and the decoy) — "+
				"the fixture is not reproducing the bypass", overloads, CaptureFunctionRow)
		}

		// The read must resolve to the ONE the trigger executes.
		installed, err := loadInstalledCaptureFunctionShapes(ctx, db, "public")
		if err != nil {
			t.Fatalf("read installed shapes: %v", err)
		}
		if got := installed[CaptureFunctionRow]; got.recordsIntoChangeLog() {
			t.Errorf("the door read a definition of %s that records into the change log; the 0-arg "+
				"function the trigger actually calls was gutted, so it read the decoy", CaptureFunctionRow)
		}
		openWantRefusal(t, CaptureFunctionRow, "records NOTHING")

		// And the same ground truth as the representative attack: the
		// gutted function really does drop the write.
		applyPGSQL(t, dsn, `INSERT INTO body_t VALUES (2, 'lost behind the decoy')`)
		if ops := readCapturedOps(t, ctx, db, "body_t"); len(ops) != 0 {
			t.Fatalf("captured %v; the gutted function should record nothing", ops)
		}

		// Clean up the decoy before the remedy: `trigger setup` rewrites the
		// 0-arg function and CANNOT remove an overload, which is exactly why
		// the bypass was permanent once planted. Asserted, not assumed.
		setup(t)
		installed, err = loadInstalledCaptureFunctionShapes(ctx, db, "public")
		if err != nil {
			t.Fatalf("read installed shapes after repair: %v", err)
		}
		if got := installed[CaptureFunctionRow]; !got.recordsIntoChangeLog() {
			t.Error("the repair re-run did not restore a recording definition")
		}
		applyPGSQL(t, dsn, `DROP FUNCTION public.sluice_capture_change(text)`)
		if logs := openWantClean(t); strings.Contains(logs, staleCaptureFunctionMarker) {
			t.Errorf("after the repair and decoy removal the door still reports drift:\n%s", logs)
		}
	})

	t.Run("a definition changed AFTER setup recorded it refuses (provenance armed)", func(t *testing.T) {
		// Still records — so the capture-defeat arm does not fire — but it
		// is not what setup installed. This is the subtle-tamper class the
		// recorded digest exists for.
		applyPGSQL(t, dsn, `
CREATE OR REPLACE FUNCTION public.sluice_capture_truncate_fn()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $edited$
BEGIN
    IF current_setting('application_name', true) <> 'skip_me' THEN
        INSERT INTO public.sluice_change_log
            (txid, schema_name, table_name, op, pk_jsonb, before_jsonb, after_jsonb)
        VALUES
            (pg_catalog.pg_current_xact_id()::text::bigint, TG_TABLE_SCHEMA, TG_TABLE_NAME, 'T', '{}'::jsonb, NULL, NULL);
    END IF;
    RETURN NULL;
END
$edited$;`)
		openWantRefusal(t, CaptureFunctionTruncate, "CHANGED after setup")
		setup(t)
		openWantClean(t)
	})

	t.Run("an OLDER install that still matches its own provenance WARNs and keeps streaming", func(t *testing.T) {
		// The upgrade case: the body is the one THAT install recorded, and
		// this binary renders a different one. Reproduced by installing an
		// older-shaped definition and re-recording the digest for it, which
		// is exactly the state an older sluice's setup leaves behind.
		older := strings.Replace(
			renderCaptureRowFunction("public", `"public"."sluice_change_log"`, CapturePayloadFull),
			"SET bytea_output = hex\n", "", 1,
		)
		applyPGSQL(t, dsn, older)
		digest := captureFunctionDigests(map[string]string{
			CaptureFunctionRow:      older,
			CaptureFunctionTruncate: renderCaptureTruncateFunction("public", `"public"."sluice_change_log"`),
			CaptureFunctionDDL:      renderCaptureDDLFunction("public", `"public"."sluice_change_log"`, `"public"."sluice_change_log_meta"`),
			CaptureFunctionDrop:     renderCaptureDropFunction("public", `"public"."sluice_change_log"`, `"public"."sluice_change_log_meta"`),
		})
		if _, err := db.ExecContext(ctx,
			"UPDATE public.sluice_change_log_meta SET capture_fn_digest = $1 WHERE singleton_pk", digest); err != nil {
			t.Fatalf("re-record provenance: %v", err)
		}

		logs := openWantClean(t)
		if !strings.Contains(logs, staleCaptureFunctionMarker) || !strings.Contains(logs, CaptureFunctionRow) {
			t.Errorf("an older-but-untampered install should WARN naming the function:\n%s", logs)
		}
		if !strings.Contains(logs, "DIFFERENT sluice binary") {
			t.Errorf("the WARN should say the install is older, not edited:\n%s", logs)
		}
	})

	t.Run("a PRE-v5 install (no provenance) WARNs rather than refusing, and still refuses a gutted body", func(t *testing.T) {
		// Simulate the whole shipped population: an install whose meta row
		// predates the digest column.
		if _, err := db.ExecContext(ctx,
			"UPDATE public.sluice_change_log_meta SET capture_fn_digest = NULL WHERE singleton_pk"); err != nil {
			t.Fatalf("clear provenance: %v", err)
		}
		logs := openWantClean(t)
		if !strings.Contains(logs, staleCaptureFunctionMarker) || !strings.Contains(logs, "CANNOT be told apart") {
			t.Errorf("a pre-v5 drift should WARN and say the provenance is unknown:\n%s", logs)
		}

		// The capture-defeat arm does NOT depend on provenance — which is
		// what protects every install that exists today.
		applyPGSQL(t, dsn, `
CREATE OR REPLACE FUNCTION public.sluice_capture_change()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $evil$
BEGIN
    RETURN NULL;
END
$evil$;`)
		openWantRefusal(t, CaptureFunctionRow, "records NOTHING")
		setup(t)
		openWantClean(t)
	})
}
