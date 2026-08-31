//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Real-PG pins for the DDL-suppression privilege boundary (audit 2026-08-31
// SEC-2) and for its transaction scoping (audit C-1).
//
// SEC-2 was observed on PG 16.14: `sluice.setup_in_progress` is an ordinary
// dotted placeholder GUC, PostgreSQL puts no privilege on SETting one, and
// v0.133.1's capture function graded that value and nothing else — so an
// unprivileged role SET it, ran ADD COLUMN / DROP COLUMN / CREATE INDEX, and
// recorded ZERO op='X' rows while identical control ALTERs before and after
// recorded normally. Since the pgtrigger engine has no second DDL-detection
// tier, that was an off-switch for the whole thing.
//
// TestSetup_UnprivilegedSessionCannotSuppressDDL is that repro INVERTED: the
// same statements must now RECORD. TestSetupPlan_AppliedByHand is C-1's half
// — a hand-applied `--dry-run` plan must suppress its own DDL and must leave
// nothing behind afterwards, whether it commits or is abandoned.
//
// Scope, stated rather than implied: these grade the pgtrigger event-trigger
// tier only. The polled-fingerprint tier records no op='X' rows at all (it
// was never implemented — preflight_ddl_detection.go), and the sqlite-family
// trigger engines have no session-GUC suppression to bind.

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

// countXRows returns the number of op='X' marker rows in the change log.
func countXRows(t *testing.T, ctx context.Context, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sluice_change_log WHERE op = 'X'`).Scan(&n); err != nil {
		t.Fatalf("count X rows: %v", err)
	}
	return n
}

// TestSetup_UnprivilegedSessionCannotSuppressDDL is the SEC-2 repro,
// inverted. Every arm here sets the marker exactly as the audit did and
// asserts the DDL is RECORDED anyway, because the suppression now demands
// evidence the firing session cannot produce.
func TestSetup_UnprivilegedSessionCannotSuppressDDL(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	applyPGSQL(t, dsn, `CREATE TABLE guc_t (id BIGINT PRIMARY KEY, note TEXT, amount INT)`)
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"guc_t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// The actor from the audit: an ordinary LOGIN role, NOSUPERUSER, that
	// owns a replicated table (it must, to run DDL on it at all) and holds
	// no privilege on sluice's own bookkeeping.
	applyPGSQL(t, dsn, `
		CREATE ROLE lowpriv LOGIN PASSWORD 'lowpriv' NOSUPERUSER NOCREATEROLE NOCREATEDB;
		GRANT CREATE, USAGE ON SCHEMA public TO lowpriv;
		ALTER TABLE public.guc_t OWNER TO lowpriv;
	`)

	// Precondition the whole boundary rests on: setup renders no GRANTs, so
	// the meta table is owner-only. If a future change granted an ordinary
	// writer SELECT on it, the nonce would stop being unreadable and this
	// test would be grading a weaker property than its name claims.
	if err := applyPGSQLAs(t, dsn, "lowpriv", "lowpriv", `SELECT 1 FROM `+ChangeLogMetaTable); err == nil {
		t.Fatalf("an unprivileged role can SELECT %s — the armed nonce is readable and the strict arm's secrecy assumption is void", ChangeLogMetaTable)
	}
	if err := applyPGSQLAs(t, dsn, "lowpriv", "lowpriv",
		`UPDATE `+ChangeLogMetaTable+` SET `+metaSetupPIDCol+` = pg_backend_pid()`); err == nil {
		t.Fatalf("an unprivileged role can UPDATE %s — it can arm its own suppression evidence", ChangeLogMetaTable)
	}

	t.Run("the audit's repro: unprivileged session sets the marker and runs DDL", func(t *testing.T) {
		before := countXRows(t, ctx, db)
		// One session, exactly the observed sequence: two controls around
		// three statements that used to vanish.
		if err := applyPGSQLAs(t, dsn, "lowpriv", "lowpriv", `
			ALTER TABLE public.guc_t ADD COLUMN control_before INT;
			SET `+setupSessionGUC+` = '`+setupBootstrapMarker+`';
			ALTER TABLE public.guc_t ADD COLUMN hidden1 INT;
			ALTER TABLE public.guc_t DROP COLUMN note;
			CREATE INDEX guc_hidden_idx ON public.guc_t (amount);
			RESET `+setupSessionGUC+`;
			ALTER TABLE public.guc_t ADD COLUMN control_after INT;
		`); err != nil {
			t.Fatalf("unprivileged DDL script: %v", err)
		}
		// 2 controls + 3 previously-suppressed statements = 5.
		if got := countXRows(t, ctx, db) - before; got != 5 {
			t.Fatalf("an unprivileged session with %s set recorded %d op='X' row(s), want 5 "+
				"(2 controls + the 3 statements SEC-2 observed vanishing) — the marker is an off-switch again",
				setupSessionGUC, got)
		}
	})

	t.Run("the marker alone does not suppress for the install-owning role either", func(t *testing.T) {
		// Steady state: setup disarmed the evidence to a non-NULL sentinel,
		// so the bootstrap arm is closed and a stray marker in a superuser's
		// psql session — SEC-2's quieter reading — suppresses nothing.
		before := countXRows(t, ctx, db)
		applyPGSQL(t, dsn, `
			SET `+setupSessionGUC+` = '`+setupBootstrapMarker+`';
			ALTER TABLE public.guc_t ADD COLUMN owner_marker_only INT;
		`)
		if got := countXRows(t, ctx, db) - before; got != 1 {
			t.Fatalf("the install-owning role suppressed DDL with the marker alone (recorded %d, want 1) — "+
				"the bootstrap arm is still open in steady state", got)
		}
	})

	t.Run("fail-safe: an evidence read that raises RECORDS rather than suppresses", func(t *testing.T) {
		// Retype the PID column so the strict arm's comparison raises
		// undefined_function (text = integer), and NULL the nonce so the
		// BOOTSTRAP arm would otherwise fire for this owner session. Only
		// the WHEN OTHERS fail-safe stands between that and a suppressed
		// row — and if the handler were missing entirely the exception
		// would propagate and BLOCK the ALTER, so "the ALTER succeeded AND
		// recorded" is the exact shape being pinned.
		applyPGSQL(t, dsn, fmt.Sprintf(
			`ALTER TABLE %s ALTER COLUMN %s TYPE TEXT; UPDATE %s SET %s = NULL`,
			ChangeLogMetaTable, metaSetupPIDCol, ChangeLogMetaTable, metaSetupNonceCol,
		))
		before := countXRows(t, ctx, db)
		applyPGSQL(t, dsn, `
			SET `+setupSessionGUC+` = '`+setupBootstrapMarker+`';
			ALTER TABLE public.guc_t ADD COLUMN after_broken_evidence INT;
		`)
		if got := countXRows(t, ctx, db) - before; got != 1 {
			t.Fatalf("DDL fired against an unreadable evidence row recorded %d op='X' row(s), want 1 — "+
				"an evidence read that fails must RECORD (suppression is the privilege)", got)
		}
	})
}

// TestSetupPlan_AppliedByHand is audit C-1's half: the `--dry-run` plan is
// documented as inspectable and is applied by hand in the field, so it must
// (1) still suppress its own DDL when a human pastes it into psql, and
// (2) leave NOTHING set afterwards — neither on the happy path, where the
// COMMIT reverts the SET LOCAL, nor on the abandoned path, where the
// rollback does. Before this change the off-state rode a trailing `RESET`
// that an abandoned plan never reached, and the operator's own next ALTER
// went unrecorded (observed).
func TestSetupPlan_AppliedByHand(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	applyPGSQL(t, dsn, `CREATE TABLE hand_t (id BIGINT PRIMARY KEY, note TEXT)`)
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"hand_t"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	plan, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"hand_t"}, DryRun: true})
	if err != nil {
		t.Fatalf("Setup(dry-run): %v", err)
	}
	if plan.Statements[0] != "BEGIN" || plan.Statements[len(plan.Statements)-1] != "COMMIT" {
		t.Fatalf("the dry-run plan is not transaction-scoped: first=%q last=%q",
			firstLine(plan.Statements[0]), firstLine(plan.Statements[len(plan.Statements)-1]))
	}

	// One psql session, modelled: a pinned connection the operator pastes
	// into. markerOn reports what that session's marker currently is.
	pasteInto := func(t *testing.T, body func(conn *sql.Conn)) {
		t.Helper()
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("pin a session: %v", err)
		}
		defer func() { _ = conn.Close() }()
		body(conn)
	}
	markerOf := func(t *testing.T, conn *sql.Conn) string {
		t.Helper()
		var v sql.NullString
		if err := conn.QueryRowContext(ctx,
			`SELECT current_setting('`+setupSessionGUC+`', true)`).Scan(&v); err != nil {
			t.Fatalf("read marker: %v", err)
		}
		return v.String
	}

	t.Run("abandoned mid-plan: the marker does not leak into the rest of the session", func(t *testing.T) {
		before := countXRows(t, ctx, db)
		pasteInto(t, func(conn *sql.Conn) {
			// Paste up to and including the first evidence ARM — the point
			// at which suppression is live — then hit a failing statement
			// and give up, exactly the ON_ERROR_STOP shape C-1 observed.
			armed := false
			for _, stmt := range plan.Statements {
				if _, err := conn.ExecContext(ctx, stmt); err != nil {
					t.Fatalf("hand-apply %q: %v", firstLine(stmt), err)
				}
				if strings.Contains(stmt, "PERFORM pg_catalog.set_config('"+setupSessionGUC+"'") {
					armed = true
					break
				}
			}
			if !armed {
				t.Fatal("never reached an ARM statement in the plan")
			}
			if markerOf(t, conn) == "" {
				t.Fatal("the plan did not arm the marker on the pasted session — the hand-application arm is vacuous")
			}
			// The operator's failing statement, then abandonment.
			if _, err := conn.ExecContext(ctx, `ALTER TABLE public.no_such_table ADD COLUMN x INT`); err == nil {
				t.Fatal("the deliberately-failing statement succeeded")
			}
			if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
				t.Fatalf("rollback: %v", err)
			}
			if got := markerOf(t, conn); got != "" {
				t.Fatalf("after the abandoned plan the session still carries %s=%q — "+
					"the operator's own DDL from here on would be silently suppressed (C-1)", setupSessionGUC, got)
			}
			// C-1's observed failure, live: the operator's real change,
			// typed into the same session.
			if _, err := conn.ExecContext(ctx, `ALTER TABLE public.hand_t ADD COLUMN after_abandon TEXT`); err != nil {
				t.Fatalf("operator DDL after the abandoned plan: %v", err)
			}
		})
		if got := countXRows(t, ctx, db) - before; got != 1 {
			t.Fatalf("operator DDL typed after an abandoned hand-applied plan recorded %d op='X' row(s), want 1 — "+
				"the stream would keep applying past a schema change (C-1)", got)
		}
		// And the abort took the armed evidence with it.
		var armed bool
		if err := db.QueryRowContext(
			ctx,
			`SELECT `+metaSetupNonceCol+` IS NOT NULL AND `+metaSetupNonceCol+` <> '' FROM `+ChangeLogMetaTable+` WHERE singleton_pk`,
		).Scan(&armed); err != nil {
			t.Fatalf("read evidence: %v", err)
		}
		if armed {
			t.Fatal("the aborted plan left the suppression evidence ARMED — it would outlive its own transaction")
		}
	})

	t.Run("applied to completion: suppresses its own DDL, leaves nothing set", func(t *testing.T) {
		before := countXRows(t, ctx, db)
		pasteInto(t, func(conn *sql.Conn) {
			for _, stmt := range plan.Statements {
				if _, err := conn.ExecContext(ctx, stmt); err != nil {
					t.Fatalf("hand-apply %q: %v", firstLine(stmt), err)
				}
			}
			if got := markerOf(t, conn); got != "" {
				t.Fatalf("after COMMIT the session still carries %s=%q — SET LOCAL was not reverted", setupSessionGUC, got)
			}
			// The operator's next real change, same session.
			if _, err := conn.ExecContext(ctx, `ALTER TABLE public.hand_t ADD COLUMN after_apply TEXT`); err != nil {
				t.Fatalf("operator DDL after the applied plan: %v", err)
			}
		})
		if got := countXRows(t, ctx, db) - before; got != 1 {
			t.Fatalf("a hand-applied plan + one operator ALTER recorded %d op='X' row(s), want exactly 1 "+
				"(the plan's own DDL must be suppressed, the operator's must not)", got)
		}
	})
}
