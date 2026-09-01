//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// D-1 integration pins (audit 2026-08-31): the §7 DDL tier's `sql_drop`
// arm. Two halves, on a real server:
//
//   - The PREMISE, measured rather than asserted (CLAUDE.md's
//     premise-naming rule): `ddl_command_end` alone CANNOT see a drop, so
//     the second event trigger is load-bearing rather than belt-and-braces.
//     TestCaptureDropTier_DDLCommandEndIsBlindToDrops disables the sql_drop
//     trigger, drops a captured table, and demands ZERO op='X' rows — i.e.
//     it reproduces the shipped v0.134.x blindness — then re-enables it and
//     demands exactly one.
//   - The CONSEQUENCE: a dropped captured table refuses at the next poll
//     with a message that names the relation, and the no-false-fire floors
//     (an unrelated table in another schema, an uncaptured table in the
//     same schema, a DROP INDEX on a captured table, a bare DROP TRIGGER)
//     record nothing at all.

package pgtrigger

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// xRowDetail returns "table_name|command_tag|dropped_relation" for every
// marker in id order — the evidence a failure message needs.
func xRowDetail(t *testing.T, ctx context.Context, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, `
SELECT table_name, COALESCE(pk_jsonb->>'command_tag',''), COALESCE(pk_jsonb->>'`+ddlMarkerDroppedRelationKey+`','')
  FROM public.sluice_change_log WHERE op = 'X' ORDER BY id`)
	if err != nil {
		t.Fatalf("read X rows: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var tbl, tag, dropped string
		if err := rows.Scan(&tbl, &tag, &dropped); err != nil {
			t.Fatalf("scan X row: %v", err)
		}
		out = append(out, tbl+"|"+tag+"|"+dropped)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate X rows: %v", err)
	}
	return out
}

// TestCaptureDropTier_DDLCommandEndIsBlindToDrops is the premise pin. The
// whole sql_drop arm rests on one environmental fact —
// pg_event_trigger_ddl_commands() reports nothing for a DROP — and a fact
// a safety argument cites owes a check on the same server the suite runs.
// Disabling the sql_drop trigger reconstructs the exact v0.134.x install,
// so the first sub-assertion IS the shipped defect, reproduced.
func TestCaptureDropTier_DDLCommandEndIsBlindToDrops(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `
		CREATE TABLE blind_a (id BIGINT PRIMARY KEY, note TEXT);
		CREATE TABLE blind_b (id BIGINT PRIMARY KEY, note TEXT);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"blind_a", "blind_b"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	// The control: ALTER TABLE is recordable through ddl_command_end, so
	// the tagged arm is demonstrably alive in this install.
	applyPGSQL(t, dsn, `ALTER TABLE blind_a ADD COLUMN extra TEXT`)
	if n := countXRows(t, ctx, db); n != 1 {
		t.Fatalf("control ALTER TABLE recorded %d op='X' row(s), want 1 — the ddl_command_end arm is not working, so the rest of this test proves nothing: %v",
			n, xRowDetail(t, ctx, db))
	}

	// v0.134.x, reconstructed: only the ddl_command_end trigger fires.
	applyPGSQL(t, dsn, `ALTER EVENT TRIGGER `+CaptureTriggerDrop+` DISABLE`)
	applyPGSQL(t, dsn, `DROP TABLE public.blind_a`)
	if n := countXRows(t, ctx, db); n != 1 {
		t.Fatalf("with the sql_drop arm disabled, DROP TABLE moved the marker count to %d (want it unchanged at 1): %v — "+
			"if this fails because a marker APPEARED, ddl_command_end can see drops after all and the sql_drop arm's premise is wrong",
			n, xRowDetail(t, ctx, db))
	}

	// The fix, on the same install and the same server.
	applyPGSQL(t, dsn, `ALTER EVENT TRIGGER `+CaptureTriggerDrop+` ENABLE`)
	applyPGSQL(t, dsn, `DROP TABLE public.blind_b`)
	got := xRowDetail(t, ctx, db)
	if len(got) != 2 {
		t.Fatalf("DROP TABLE with the sql_drop arm enabled recorded %d marker(s), want 2 (the control ALTER + the drop): %v", len(got), got)
	}
	if want := "public.blind_b|DROP TABLE|public.blind_b"; got[1] != want {
		t.Fatalf("drop marker = %q, want %q", got[1], want)
	}

	t.Run("PREMISE: the two context functions are NOT symmetrically exclusive", func(t *testing.T) {
		// ADR-0066 said they were ("calling either from the other's event
		// raises"), which was asserted rather than measured. Only ONE
		// direction raises, and the other is the one that matters here:
		// calling pg_event_trigger_ddl_commands() from a sql_drop function
		// returns ZERO ROWS AND NO ERROR — the same silent nothing this
		// whole arm exists to fix, one move further on. Someone relocating
		// the capture arm to sql_drop and keeping the ddl_commands() call
		// would reproduce the defect exactly, with nothing to say so.
		//
		// Corrected and pinned 2026-09-01 after a real PG 16.14/17.11
		// measurement contradicted the ADR. Measured here per run rather
		// than restated, per the premise-naming rule.
		applyPGSQL(t, dsn, `
CREATE FUNCTION premise_drop_probe() RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE n int;
BEGIN
    SELECT count(*) INTO n FROM pg_catalog.pg_event_trigger_ddl_commands();
    INSERT INTO premise_probe_log (what, n) VALUES ('ddl_commands_from_sql_drop', n);
END $$;
CREATE TABLE premise_probe_log (what TEXT, n INT);
CREATE EVENT TRIGGER premise_drop_trg ON sql_drop EXECUTE FUNCTION premise_drop_probe();
CREATE TABLE premise_victim (id BIGINT PRIMARY KEY);`)
		applyPGSQL(t, dsn, `DROP TABLE public.premise_victim`)

		var n sql.NullInt64
		if err := db.QueryRowContext(ctx,
			`SELECT n FROM premise_probe_log WHERE what = 'ddl_commands_from_sql_drop'`).Scan(&n); err != nil {
			t.Fatalf("the sql_drop probe did not run, so the premise is unmeasured: %v", err)
		}
		if !n.Valid || n.Int64 != 0 {
			t.Errorf("pg_event_trigger_ddl_commands() from a sql_drop function returned %v rows; want 0. "+
				"If this now RAISES instead, the ADR's original symmetry claim has become true on this "+
				"server version and the correction needs revisiting; if it returns rows, the sql_drop arm "+
				"could have been written the other way after all", n)
		}

		// The direction that DOES raise, pinned so the asymmetry is a
		// measured pair rather than one half plus an assumption.
		_, err := db.ExecContext(ctx, `
CREATE OR REPLACE FUNCTION premise_end_probe() RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE n int;
BEGIN
    SELECT count(*) INTO n FROM pg_catalog.pg_event_trigger_dropped_objects();
END $$;
CREATE EVENT TRIGGER premise_end_trg ON ddl_command_end EXECUTE FUNCTION premise_end_probe();
CREATE TABLE premise_raises (id BIGINT PRIMARY KEY);`)
		if err == nil {
			t.Error("pg_event_trigger_dropped_objects() from a ddl_command_end function did NOT raise; " +
				"the asymmetry this test pins has changed and ADR-0066's correction needs re-deriving")
		} else if !strings.Contains(err.Error(), "sql_drop") {
			t.Errorf("it raised, but not with the expected sql_drop-context message: %v", err)
		}
	})
}

// TestCaptureDropTier_DroppedCapturedTableRefusesAtResume drives the whole
// consequence: a captured table dropped mid-stream refuses at the next poll
// naming the relation, and four no-false-fire shapes record nothing.
func TestCaptureDropTier_DroppedCapturedTableRefusesAtResume(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `
		CREATE TABLE drop_kept (id BIGINT PRIMARY KEY, note TEXT);
		CREATE TABLE drop_gone (id BIGINT PRIMARY KEY, note TEXT);
		CREATE TABLE drop_uncaptured (id BIGINT PRIMARY KEY);
		CREATE SCHEMA elsewhere;
		CREATE TABLE elsewhere.far (id BIGINT PRIMARY KEY);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"drop_kept", "drop_gone"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	applyPGSQL(t, dsn, `CREATE INDEX drop_gone_note_idx ON drop_gone (note)`)
	// The CREATE INDEX is itself recordable (it is on the tagged arm's
	// list), so start the stream AFTER it: the position anchors past every
	// marker written so far and the drop is the only one the poll can see.
	if n := countXRows(t, ctx, db); n != 1 {
		t.Fatalf("setup + CREATE INDEX recorded %d marker(s), want 1 (the CREATE INDEX): %v", n, xRowDetail(t, ctx, db))
	}

	r, err := openCDCReader(ctx, dsn, "")
	if err != nil {
		t.Fatalf("openCDCReader: %v", err)
	}
	reader := r.(*CDCReader)
	defer func() { _ = reader.Close() }()
	out, err := reader.StreamChanges(ctx, ir.Position{}) // zero position anchors "from now"
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}

	// The floors first, while the stream is live: none of these may write a
	// marker, and the stream must still be serving.
	applyPGSQL(t, dsn, `DROP INDEX public.drop_gone_note_idx`)
	applyPGSQL(t, dsn, `DROP TABLE public.drop_uncaptured`)
	applyPGSQL(t, dsn, `DROP SCHEMA elsewhere CASCADE`)
	applyPGSQL(t, dsn, `DROP TRIGGER `+CaptureTriggerTruncate+` ON public.drop_kept`)
	if n := countXRows(t, ctx, db); n != 1 {
		t.Fatalf("the no-false-fire shapes (DROP INDEX on a captured table, DROP of an uncaptured table, DROP SCHEMA of an "+
			"unrelated schema, bare DROP TRIGGER) moved the marker count to %d, want it unchanged at 1: %v", n, xRowDetail(t, ctx, db))
	}
	applyPGSQL(t, dsn, `INSERT INTO drop_kept (id, note) VALUES (1, 'still streaming')`)
	if got := drainEvents(t, out, 1, 20*time.Second); len(got) != 1 {
		t.Fatalf("stream consumed %d event(s) after the no-false-fire shapes, want the INSERT; reader.Err() = %v", len(got), reader.Err())
	}

	// The consequence.
	applyPGSQL(t, dsn, `DROP TABLE public.drop_gone`)
	if got := drainEvents(t, out, 1, 20*time.Second); len(got) != 0 {
		t.Fatalf("stream emitted %d event(s) after a captured table was dropped, want the refusal: %+v", len(got), got)
	}
	err = reader.Err()
	if err == nil {
		t.Fatal("dropping a captured table mid-stream did not refuse — the stream kept exiting 0 while the target retains the dropped table's rows (D-1)")
	}
	for _, want := range []string{"public.drop_gone", "DROPPED", "--restart-from-scratch"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("drop refusal missing %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "run `sluice migrate` on the target to land the schema change") {
		t.Errorf("drop refusal steers to `sluice migrate`, which reads the SOURCE schema and cannot land a drop: %v", err)
	}
}
