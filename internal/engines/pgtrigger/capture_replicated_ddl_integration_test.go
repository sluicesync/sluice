//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// A-1 integration pins (audit 2026-08-31): under
// `--capture-replicated-writes`, the DDL tier carries the same posture as
// the row tier — one flag, one coherent capture posture.
//
// The MECHANISM is measured on the server rather than assumed, because the
// ADR shipped a safety argument that cited an environmental fact ("logical
// replication does not replicate DDL") which was true of a native apply
// worker and did not cover the write class the opt-in actually admits:
// `SET session_replication_role = replica` is an ordinary operator/ETL
// idiom, and PostgreSQL filters EVENT triggers by evtenabled exactly as it
// filters row triggers. The plain-posture arm here reproduces that
// blindness on the same container as the control, so a green run means the
// mechanism was exercised, not merely modelled.

package pgtrigger

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// eventTriggerEnablement returns pg_event_trigger.evtenabled for a trigger.
func eventTriggerEnablement(t *testing.T, ctx context.Context, db *sql.DB, name string) string {
	t.Helper()
	var enabled string
	if err := db.QueryRowContext(ctx, `SELECT evtenabled::text FROM pg_event_trigger WHERE evtname = $1`, name).Scan(&enabled); err != nil {
		t.Fatalf("read evtenabled of %s: %v", name, err)
	}
	return enabled
}

// replicaRoleExec runs stmts on ONE pinned connection under
// session_replication_role=replica — the shape of a subscription apply
// worker, of sluice's own privileged applier, and of the bulk-load idiom
// operators use to suppress FK and user triggers.
func replicaRoleExec(t *testing.T, ctx context.Context, db *sql.DB, stmts ...string) {
	t.Helper()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SET session_replication_role = replica"); err != nil {
		t.Fatalf("SET replica role: %v", err)
	}
	for _, s := range stmts {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatalf("replica-role %q: %v", s, err)
		}
	}
}

func TestCaptureReplicatedWrites_ReplicaRoleDDLIsCaptured(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `
		CREATE TABLE crwddl_t (id BIGINT PRIMARY KEY, note TEXT);
		CREATE TABLE crwddl_dropme (id BIGINT PRIMARY KEY);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// tables shrinks when the drop cell below removes one — a re-setup
	// naming a table that no longer exists refuses, which is correct
	// behaviour and not what these cells are about.
	tables := []string{"crwddl_t", "crwddl_dropme"}
	setup := func(t *testing.T, optIn bool) {
		t.Helper()
		if _, err := Setup(ctx, dsn, SetupOptions{
			Tables:                  tables,
			CaptureReplicatedWrites: optIn,
		}); err != nil {
			t.Fatalf("Setup(optIn=%t): %v", optIn, err)
		}
	}
	truncateLog := func(t *testing.T) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `TRUNCATE public.sluice_change_log`); err != nil {
			t.Fatalf("truncate change log: %v", err)
		}
	}

	t.Run("plain posture: the event triggers are 'O' and replica-role DDL records NOTHING (the F1 shape, reproduced)", func(t *testing.T) {
		setup(t, false)
		for _, trg := range []string{CaptureTriggerDDL, CaptureTriggerDrop} {
			if got := eventTriggerEnablement(t, ctx, db, trg); got != "O" {
				t.Errorf("evtenabled of %s = %q under the default posture; want \"O\"", trg, got)
			}
		}
		truncateLog(t)
		replicaRoleExec(t, ctx, db, `ALTER TABLE crwddl_t ADD COLUMN plain_col TEXT`)
		if n := countXRows(t, ctx, db); n != 0 {
			t.Fatalf("a plain install recorded %d marker(s) for replica-role DDL, want 0 — if this fails, evtenabled 'O' DOES fire under "+
				"replica role and the A-1 fix rests on a false premise: %v", n, xRowDetail(t, ctx, db))
		}
		// The control on the same install: origin-role DDL still records,
		// so the tier is demonstrably alive.
		applyPGSQL(t, dsn, `ALTER TABLE crwddl_t ADD COLUMN origin_col TEXT`)
		if n := countXRows(t, ctx, db); n != 1 {
			t.Fatalf("origin-role DDL recorded %d marker(s), want 1 — the DDL tier is not working, so the arm above proves nothing: %v",
				n, xRowDetail(t, ctx, db))
		}
	})

	t.Run("opt-in posture: every capture trigger is ENABLE ALWAYS", func(t *testing.T) {
		setup(t, true)
		for _, tbl := range []string{"crwddl_t", "crwddl_dropme"} {
			for _, trg := range []string{CaptureTriggerRow, CaptureTriggerTruncate} {
				if got := triggerEnablement(t, ctx, db, tbl, trg); got != "A" {
					t.Errorf("tgenabled of %s on %s = %q; want \"A\"", trg, tbl, got)
				}
			}
		}
		for _, trg := range []string{CaptureTriggerDDL, CaptureTriggerDrop} {
			if got := eventTriggerEnablement(t, ctx, db, trg); got != "A" {
				t.Errorf("evtenabled of %s = %q under --capture-replicated-writes; want \"A\" — replica-role DDL would be invisible while "+
					"replica-role DML is captured (A-1)", trg, got)
			}
		}
	})

	t.Run("opt-in posture: replica-role ALTER records an X row BEFORE the replica-role INSERT it reshapes", func(t *testing.T) {
		truncateLog(t)
		replicaRoleExec(
			t, ctx, db,
			`ALTER TABLE crwddl_t ADD COLUMN replicated_col TEXT`,
			`INSERT INTO crwddl_t (id, note, replicated_col) VALUES (7, 'x', 'y')`,
		)
		if got := strings.Join(readCapturedOps(t, ctx, db, "crwddl_t"), ""); got != "I" {
			t.Fatalf("captured ops for crwddl_t = %q; want \"I\" (the row tier still captures the replicated INSERT)", got)
		}
		detail := xRowDetail(t, ctx, db)
		if len(detail) != 1 || !strings.Contains(detail[0], "ALTER TABLE") {
			t.Fatalf("replica-role ALTER recorded %v; want exactly one ALTER TABLE marker — without it the applier writes a post-DDL-shaped "+
				"row into an unchanged target with no refusal (A-1)", detail)
		}
		// Ordering is what makes the refusal useful: PG takes ACCESS
		// EXCLUSIVE for the ALTER, so its marker is allocated ahead of the
		// INSERT's row and the poll refuses BEFORE emitting the reshaped row.
		var xID, insID int64
		if err := db.QueryRowContext(
			ctx,
			`SELECT (SELECT min(id) FROM public.sluice_change_log WHERE op = 'X'), (SELECT min(id) FROM public.sluice_change_log WHERE op = 'I')`,
		).Scan(&xID, &insID); err != nil {
			t.Fatalf("read marker ordering: %v", err)
		}
		if xID >= insID {
			t.Fatalf("the DDL marker (id=%d) does not precede the INSERT it reshapes (id=%d) — the stream would emit the reshaped row first", xID, insID)
		}
	})

	t.Run("opt-in posture: a replica-role DROP of a captured table is captured too (D-1 x A-1)", func(t *testing.T) {
		truncateLog(t)
		replicaRoleExec(t, ctx, db, `DROP TABLE public.crwddl_dropme`)
		tables = []string{"crwddl_t"}
		detail := xRowDetail(t, ctx, db)
		if len(detail) != 1 || !strings.Contains(detail[0], "public.crwddl_dropme") {
			t.Fatalf("replica-role DROP TABLE recorded %v; want one marker naming public.crwddl_dropme — the sql_drop arm needs the same "+
				"ENABLE ALWAYS treatment as its sibling", detail)
		}
	})

	t.Run("the posture door grades the event triggers in BOTH directions", func(t *testing.T) {
		// Hand-flip the DDL event trigger back to plain on an opt-in
		// install: the two tiers now disagree, which is exactly the state
		// A-1 describes, and the open must refuse.
		applyPGSQL(t, dsn, `ALTER EVENT TRIGGER `+CaptureTriggerDDL+` ENABLE`)
		r, err := openCDCReader(ctx, dsn, "")
		if err == nil {
			_ = r.(*CDCReader).Close()
			t.Fatal("an opt-in install whose DDL event trigger was flipped back to plain opened clean — the posture door does not reach the event tier")
		}
		for _, want := range []string{CaptureTriggerDDL, "--capture-replicated-writes"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal missing %q: %v", want, err)
			}
		}
		// The remedy really runs.
		setup(t, true)
		r, err = openCDCReader(ctx, dsn, "")
		if err != nil {
			t.Fatalf("re-running `trigger setup --capture-replicated-writes` did not repair the posture: %v", err)
		}
		_ = r.(*CDCReader).Close()

		// The other direction: a PLAIN install whose event trigger was
		// hand-flipped to ENABLE ALWAYS.
		setup(t, false)
		applyPGSQL(t, dsn, `ALTER EVENT TRIGGER `+CaptureTriggerDrop+` ENABLE ALWAYS`)
		r, err = openCDCReader(ctx, dsn, "")
		if err == nil {
			_ = r.(*CDCReader).Close()
			t.Fatal("a plain install whose sql_drop event trigger was flipped to ENABLE ALWAYS opened clean — hand-flipped drift is what this door is for")
		}
		for _, want := range []string{CaptureTriggerDrop, "ORIGIN-ONLY"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal missing %q: %v", want, err)
			}
		}
	})
}
