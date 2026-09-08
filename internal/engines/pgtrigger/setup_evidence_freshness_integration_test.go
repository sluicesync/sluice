//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// A `--capture-replicated-writes` setup that STALLS must not record its own
// ALTERs as source-side DDL (audit SLP-4).
//
// # The defect
//
// The strict DDL-suppression arm recognises sluice's own DDL only while the
// armed evidence is FRESH: `setup_at > clock_timestamp() - '1 hour'`. That is
// a real clock, not the transaction's — clock_timestamp() advances inside a
// transaction — and the plan's `DROP TRIGGER` / `CREATE TRIGGER` statements
// take ACCESS EXCLUSIVE, so they queue behind any long-running transaction on
// a busy table. A wait longer than the window means the ENABLE ALWAYS ALTERs
// that follow are recorded as `X` rows, and the NEXT stream open refuses with
// "observed source-side DDL (ALTER TABLE)" and a drain / --restart-from-scratch
// remedy. The operator is told to rebuild, for DDL sluice wrote.
//
// # Why the constants are scaled
//
// Reproducing this at the shipped constants needs an hour of held lock. The
// mechanism is one predicate against one clock, so the test rewrites the
// rendered plan's window to '1 second' and injects a 3-second stall — the same
// statement, the same predicate, a scale a test can afford. What is NOT scaled
// is the fix: the plan under test is the one `renderSetupDDL` produces.
//
// # Why the negative control is load-bearing
//
// "Zero X rows" on its own is the answer a plan that recorded nothing at all
// would give, and a plan whose stall injection missed would give it too. The
// control removes the re-arm from the same mutated plan and asserts the rows
// DO appear: that is what makes the passing cell evidence about the re-arm
// rather than about the injection having silently not applied.
func TestSetupEvidenceFreshness_StalledOptInDoesNotRecordItsOwnALTERs(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// scaleAndStall rewrites the freshness window down to a second and puts
	// a 3-second sleep where a lock wait would land: immediately before the
	// first ENABLE ALWAYS, which is where the trigger DROP/CREATE work ends.
	scaleAndStall := func(t *testing.T, stmts []string, keepRearm bool) []string {
		t.Helper()
		var (
			out             []string
			scaled, stalled int
			armsDropped     int
		)
		for _, s := range stmts {
			if strings.Contains(s, "'"+setupEvidenceFreshness+"'") {
				s = strings.ReplaceAll(s, "'"+setupEvidenceFreshness+"'", "'1 second'")
				scaled++
			}
			if strings.Contains(s, "ENABLE ALWAYS TRIGGER") && stalled == 0 {
				// The re-arm is the statement immediately before this one
				// (pinned by TestRenderSetupDDL_SelfDDLSuppression). Drop it
				// for the control, and put the stall where the lock wait is.
				if !keepRearm && len(out) > 0 &&
					strings.Contains(out[len(out)-1], "pg_catalog.set_config('"+setupSessionGUC+"'") {
					out = out[:len(out)-1]
					armsDropped++
				}
				out = append(out, "SELECT pg_sleep(3)")
				stalled++
			}
			out = append(out, s)
		}
		// The mutation-applied check, as its own assertion rather than as a
		// hope: a rewrite that matched nothing produces a plan that behaves
		// exactly like the unmutated one, and the cell would then pass or
		// fail for reasons that have nothing to do with the fix.
		if scaled == 0 {
			t.Fatalf("the freshness rewrite matched no statement — the window is no longer spelled %q in the plan, "+
				"so this test is exercising the shipped one-hour window and can never reach the defect",
				setupEvidenceFreshness)
		}
		if stalled == 0 {
			t.Fatal("the plan rendered no ENABLE ALWAYS statement, so no stall was injected — " +
				"CaptureReplicatedWrites did not reach renderSetupDDL")
		}
		if !keepRearm && armsDropped == 0 {
			t.Fatal("the control could not find a re-arm immediately before the first ENABLE ALWAYS to remove — " +
				"either the adjacency broke (see TestRenderSetupDDL_SelfDDLSuppression) or this helper is looking " +
				"for the wrong statement shape; either way the control proves nothing")
		}
		return out
	}

	apply := func(t *testing.T, stmts []string) {
		t.Helper()
		// One connection, because the plan is one transaction and the
		// evidence is keyed on pg_backend_pid().
		conn, cerr := db.Conn(ctx)
		if cerr != nil {
			t.Fatalf("conn: %v", cerr)
		}
		defer func() { _ = conn.Close() }()
		for i, s := range stmts {
			if _, eerr := conn.ExecContext(ctx, s); eerr != nil {
				t.Fatalf("statement %d failed: %v\n%s", i, eerr, firstLine(s))
			}
		}
	}

	renderPlan := func(t *testing.T, table string) []string {
		t.Helper()
		plan, perr := Setup(ctx, dsn, SetupOptions{
			Tables:                  []string{table},
			CaptureReplicatedWrites: true,
			DryRun:                  true,
		})
		if perr != nil {
			t.Fatalf("render plan: %v", perr)
		}
		return plan.Statements
	}

	t.Run("the re-arm holds across a stall longer than the window", func(t *testing.T) {
		applyPGSQL(t, dsn, `DROP TABLE IF EXISTS slp4_fixed; CREATE TABLE slp4_fixed (id BIGINT PRIMARY KEY, note TEXT)`)
		apply(t, scaleAndStall(t, renderPlan(t, "slp4_fixed"), true))

		if n := countXRows(t, ctx, db); n != 0 {
			t.Fatalf("a stalled opt-in setup recorded %d source-side-DDL marker(s) for its OWN ALTERs, want 0.\n"+
				"  The next stream open would refuse with \"observed source-side DDL (ALTER TABLE)\" and tell the "+
				"operator to drain or --restart-from-scratch, for DDL sluice itself wrote: %v",
				n, xRowDetail(t, ctx, db))
		}
	})

	t.Run("control: without the re-arm the same stall DOES poison the log", func(t *testing.T) {
		if _, err := db.ExecContext(ctx, `TRUNCATE public.sluice_change_log`); err != nil {
			t.Fatalf("truncate change log: %v", err)
		}
		applyPGSQL(t, dsn, `DROP TABLE IF EXISTS slp4_control; CREATE TABLE slp4_control (id BIGINT PRIMARY KEY, note TEXT)`)
		apply(t, scaleAndStall(t, renderPlan(t, "slp4_control"), false))

		if n := countXRows(t, ctx, db); n == 0 {
			t.Fatal("the control recorded ZERO markers, so the cell above proves nothing about the re-arm.\n" +
				"  Either the stall injection is not reaching the freshness predicate, or the DDL tier is not " +
				"recording ALTER TABLE at all on this install — check before trusting the passing cell.")
		}
	})
}
