//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The capture-shape door against the OTHER-SCHEMA decoy, on a real server
// (audit 2026-09-01 SLP-1). The attack the audit observed: a function with
// the capture function's exact name planted in a second schema and bound to
// a capture trigger. Every arm of the door compared names, and the body arm
// read definitions by name WITHIN the sluice schema — so it graded the
// pristine original while the trigger executed the decoy. 3 INSERTs + 1
// UPDATE recorded 0 change-log rows at a healthy stream; a decoy-bound event
// trigger recorded no `X` for an ALTER TABLE.
//
// One stage per capture TIER — the row trigger, the TRUNCATE trigger, the
// `ddl_command_end` arm and the `sql_drop` arm — because a refusal that
// reaches one tier and not its siblings is the sibling-miss shape, and each
// stage carries its own ground truth that the decoy really drops the write
// (the fixture reproduces the defect, not a look-alike). The last stage is
// the no-false-refuse floor for the new arm: a trigger rebound BY HAND to
// the real same-schema function opens clean, so the door keys on identity,
// not on "setup was the last writer".
//
// The stages share one container and run in order; each decoy stage repairs
// the source (re-setup, which is the prescribed remedy) before the next.

package pgtrigger

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestCDCOpen_CaptureShapeDoor_DecoySchema(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `
CREATE TABLE decoy_t (id BIGINT PRIMARY KEY, v TEXT);
CREATE TABLE decoy_drop_t (id BIGINT PRIMARY KEY);
CREATE SCHEMA decoy;`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	setup := func(t *testing.T, tables ...string) {
		t.Helper()
		if _, err := Setup(ctx, dsn, SetupOptions{Tables: tables}); err != nil {
			t.Fatalf("Setup: %v", err)
		}
	}
	openWantRefusal := func(t *testing.T, wantAll ...string) {
		t.Helper()
		r, err := openCDCReader(ctx, dsn, "")
		if err == nil {
			_ = r.(*CDCReader).Close()
			t.Fatalf("CDC open succeeded on a decoy-bound install; want a refusal containing %q", wantAll)
		}
		for _, want := range wantAll {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal missing %q:\n%v", want, err)
			}
		}
	}
	openWantClean := func(t *testing.T) {
		t.Helper()
		r, err := openCDCReader(ctx, dsn, "")
		if err != nil {
			t.Fatalf("CDC open refused (false refuse): %v", err)
		}
		_ = r.(*CDCReader).Close()
	}
	// boundSchemaOf is the anti-vacuity check for every decoy stage: the
	// trigger must REALLY execute a function in the decoy schema, or the
	// stage is grading the healthy install again.
	boundSchemaOf := func(t *testing.T, q string, args ...any) string {
		t.Helper()
		var ns string
		if err := db.QueryRowContext(ctx, q, args...).Scan(&ns); err != nil {
			t.Fatalf("read the bound function's schema: %v", err)
		}
		return ns
	}
	const rowTriggerNS = `
SELECT pn.nspname FROM pg_trigger t
  JOIN pg_class c ON c.oid = t.tgrelid
  JOIN pg_proc p ON p.oid = t.tgfoid
  JOIN pg_namespace pn ON pn.oid = p.pronamespace
 WHERE c.relname = $1 AND t.tgname = $2`
	const eventTriggerNS = `
SELECT pn.nspname FROM pg_event_trigger e
  JOIN pg_proc p ON p.oid = e.evtfoid
  JOIN pg_namespace pn ON pn.oid = p.pronamespace
 WHERE e.evtname = $1`
	countX := func(t *testing.T) int {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM public.sluice_change_log WHERE op = 'X'`).Scan(&n); err != nil {
			t.Fatalf("count X rows: %v", err)
		}
		return n
	}

	setup(t, "decoy_t", "decoy_drop_t")
	openWantClean(t)

	t.Run("row trigger bound to decoy.sluice_capture_change refuses (the observed shape)", func(t *testing.T) {
		applyPGSQL(t, dsn, `
CREATE FUNCTION decoy.sluice_capture_change() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
DROP TRIGGER sluice_capture ON decoy_t;
CREATE TRIGGER sluice_capture AFTER INSERT OR UPDATE OR DELETE ON decoy_t
    FOR EACH ROW EXECUTE FUNCTION decoy.sluice_capture_change('["id"]');`)
		if ns := boundSchemaOf(t, rowTriggerNS, "decoy_t", CaptureTriggerRow); ns != "decoy" {
			t.Fatalf("row trigger executes a function in %q; the fixture did not bind the decoy", ns)
		}
		// Ground truth, exactly the audit's: DML at a "healthy" install
		// records nothing.
		applyPGSQL(t, dsn, `INSERT INTO decoy_t VALUES (2,'b'),(3,'c'),(4,'d'); UPDATE decoy_t SET v='zz' WHERE id=2;`)
		if ops := readCapturedOps(t, ctx, db, "decoy_t"); len(ops) != 0 {
			t.Fatalf("captured %v with the decoy bound; the fixture is not reproducing the defect", ops)
		}
		openWantRefusal(t, "decoy_t", CaptureTriggerRow, `"decoy"."`+CaptureFunctionRow+`"`, "OUTSIDE the sluice schema", "trigger setup")

		// Both stream-open paths reach the door (the moved-door caller
		// list, as in TestCDCOpen_CaptureShapeDoor).
		if stream, err := (Engine{}).OpenSnapshotStream(ctx, dsn); err == nil {
			_ = stream.Close()
			t.Fatal("OpenSnapshotStream succeeded on a decoy-bound source; want the capture-shape refusal")
		} else if !strings.Contains(err.Error(), "OUTSIDE the sluice schema") {
			t.Errorf("OpenSnapshotStream refusal should carry the decoy message; got %v", err)
		}

		// The prescribed remedy rebinds (DROP IF EXISTS + CREATE), and the
		// repaired trigger captures again — the control that proves the
		// silence above was the decoy's doing.
		setup(t, "decoy_t", "decoy_drop_t")
		applyPGSQL(t, dsn, `INSERT INTO decoy_t VALUES (5,'e')`)
		if ops := readCapturedOps(t, ctx, db, "decoy_t"); len(ops) != 1 || ops[0] != "I" {
			t.Fatalf("after repair captured %v; want [I]", ops)
		}
		openWantClean(t)
		applyPGSQL(t, dsn, `DROP FUNCTION decoy.sluice_capture_change()`)
	})

	t.Run("truncate trigger bound to decoy.sluice_capture_truncate_fn refuses", func(t *testing.T) {
		applyPGSQL(t, dsn, `
CREATE FUNCTION decoy.sluice_capture_truncate_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
DROP TRIGGER sluice_capture_truncate ON decoy_t;
CREATE TRIGGER sluice_capture_truncate AFTER TRUNCATE ON decoy_t
    FOR EACH STATEMENT EXECUTE FUNCTION decoy.sluice_capture_truncate_fn();`)
		if ns := boundSchemaOf(t, rowTriggerNS, "decoy_t", CaptureTriggerTruncate); ns != "decoy" {
			t.Fatalf("truncate trigger executes a function in %q; the fixture did not bind the decoy", ns)
		}
		before := readCapturedOps(t, ctx, db, "decoy_t")
		applyPGSQL(t, dsn, `TRUNCATE decoy_t`)
		if after := readCapturedOps(t, ctx, db, "decoy_t"); len(after) != len(before) {
			t.Fatalf("TRUNCATE recorded %v (was %v) with the decoy bound; the fixture is not reproducing the defect", after, before)
		}
		openWantRefusal(t, "decoy_t", CaptureTriggerTruncate, `"decoy"."`+CaptureFunctionTruncate+`"`, "OUTSIDE the sluice schema", "TRUNCATE")

		setup(t, "decoy_t", "decoy_drop_t")
		openWantClean(t)
		applyPGSQL(t, dsn, `DROP FUNCTION decoy.sluice_capture_truncate_fn()`)
	})

	t.Run("ddl_command_end event trigger bound to decoy.sluice_capture_ddl refuses", func(t *testing.T) {
		applyPGSQL(t, dsn, `
CREATE FUNCTION decoy.sluice_capture_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN RETURN; END $$;
DROP EVENT TRIGGER sluice_capture_ddl_trg;
CREATE EVENT TRIGGER sluice_capture_ddl_trg ON ddl_command_end
    WHEN TAG IN ('ALTER TABLE','CREATE TABLE','CREATE INDEX')
    EXECUTE FUNCTION decoy.sluice_capture_ddl();`)
		if ns := boundSchemaOf(t, eventTriggerNS, CaptureTriggerDDL); ns != "decoy" {
			t.Fatalf("DDL event trigger executes a function in %q; the fixture did not bind the decoy", ns)
		}
		// Ground truth, the audit's event-tier observation: an ALTER TABLE
		// on a captured table records no `X` row.
		xBefore := countX(t)
		applyPGSQL(t, dsn, `ALTER TABLE decoy_t ADD COLUMN extra INT DEFAULT 7`)
		if xAfter := countX(t); xAfter != xBefore {
			t.Fatalf("ALTER TABLE recorded %d X row(s) with the decoy bound; the fixture is not reproducing the defect", xAfter-xBefore)
		}
		openWantRefusal(t, CaptureTriggerDDL, `"decoy"."`+CaptureFunctionDDL+`"`, "OUTSIDE the sluice schema", "trigger setup")

		setup(t, "decoy_t", "decoy_drop_t")
		openWantClean(t)
		applyPGSQL(t, dsn, `DROP FUNCTION decoy.sluice_capture_ddl()`)
	})

	t.Run("sql_drop event trigger bound to decoy.sluice_capture_drop refuses", func(t *testing.T) {
		applyPGSQL(t, dsn, `
CREATE FUNCTION decoy.sluice_capture_drop() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN RETURN; END $$;
DROP EVENT TRIGGER sluice_capture_drop_trg;
CREATE EVENT TRIGGER sluice_capture_drop_trg ON sql_drop EXECUTE FUNCTION decoy.sluice_capture_drop();`)
		if ns := boundSchemaOf(t, eventTriggerNS, CaptureTriggerDrop); ns != "decoy" {
			t.Fatalf("drop event trigger executes a function in %q; the fixture did not bind the decoy", ns)
		}
		// Ground truth: dropping a captured table records no `X` row.
		xBefore := countX(t)
		applyPGSQL(t, dsn, `DROP TABLE decoy_drop_t`)
		if xAfter := countX(t); xAfter != xBefore {
			t.Fatalf("DROP TABLE recorded %d X row(s) with the decoy bound; the fixture is not reproducing the defect", xAfter-xBefore)
		}
		openWantRefusal(t, CaptureTriggerDrop, `"decoy"."`+CaptureFunctionDrop+`"`, "OUTSIDE the sluice schema", "trigger setup")

		setup(t, "decoy_t")
		openWantClean(t)
		applyPGSQL(t, dsn, `DROP FUNCTION decoy.sluice_capture_drop()`)
	})

	t.Run("CONTROL: a trigger rebound by hand to the real same-schema function opens clean", func(t *testing.T) {
		// The door keys on the function's identity, not on who bound it:
		// the statement setup itself renders, run by hand, is a healthy
		// install.
		applyPGSQL(t, dsn, `
DROP TRIGGER sluice_capture ON decoy_t;
CREATE TRIGGER sluice_capture AFTER INSERT OR UPDATE OR DELETE ON decoy_t
    FOR EACH ROW EXECUTE FUNCTION public.sluice_capture_change('["id"]');`)
		if ns := boundSchemaOf(t, rowTriggerNS, "decoy_t", CaptureTriggerRow); ns != "public" {
			t.Fatalf("row trigger executes a function in %q; the control did not rebind to the sluice schema", ns)
		}
		openWantClean(t)
	})
}
