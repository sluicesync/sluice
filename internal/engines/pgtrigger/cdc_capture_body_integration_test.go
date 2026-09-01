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
