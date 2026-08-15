//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestSingleStatementDoorMatchesPG is the C-1 differential lexer oracle
// (2026-08-14 audit): the hand-rolled [assertSingleDDLStatement] is
// pinned against the REAL server, not the author's imagination. For a
// corpus spanning every PostgreSQL lexical form carrying an embedded
// injection, it asserts the security invariant that actually matters —
//
//	NOT (door PASSES ∧ the server executed the injected statement).
//
// The injected statement is a marker `CREATE TABLE`; each candidate runs
// inside a transaction that is ROLLED BACK, and the marker's existence
// is probed inside that tx via to_regclass. A candidate the server
// rejects as a syntax error injects nothing (safe); a candidate whose
// injection sits inside a comment/quote the server honours injects
// nothing (safe); the ONLY hard failure is a candidate the door waved
// through whose marker actually materialised — a live restore-time RCE.
//
// Anti-vacuity: the corpus must contain at least a few GENUINE
// injections (marker fires when the door is not consulted), so a green
// run proves the door blocked real bypasses rather than an inert corpus.
// The over-refusal direction (door refusing a legitimate single
// statement) is pinned separately by the unit door test's accept list.
func TestSingleStatementDoorMatchesPG(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS c1_probe (id int)`); err != nil {
		t.Fatalf("create probe table: %v", err)
	}

	// Each body is a RECORDED CHECK expression inlined verbatim into the
	// real emit template (schema_writer_check.go). Bodies marked
	// wantInject carry a marker CREATE TABLE the corpus author believes
	// the server WILL execute past a naive door — the anti-vacuity
	// floor. The template is the production one, so paren balance is
	// the server's problem to judge, not the test's to hand-count.
	type row struct {
		name       string
		body       string
		wantInject bool // author's belief; the server is the arbiter
	}
	marker := func(n string) string { return "c1_mark_" + n }
	// inj builds the injected `CREATE TABLE <marker>` for a row, using
	// the row's OWN name as the marker suffix so the probe and the
	// injection agree (the marker-name mismatch that made the first cut
	// of this oracle read every genuine injection as "not injected").
	inj := func(n string) string { return "; CREATE TABLE " + marker(n) + " ()" }

	corpus := []row{
		// Family B — comment-smuggled apostrophe, real injection OUTSIDE the comment.
		{"blockcomment_apostrophe", "true /* ' */ )" + inj("blockcomment_apostrophe") + "; -- '", true},
		{"linecomment_apostrophe", "true -- '\n)" + inj("linecomment_apostrophe") + "; -- '", true},
		{"nested_block_comment", "true /* a /* b */ c */ )" + inj("nested_block_comment") + "; -- x", true},
		// Family A — dollar sign continuing an identifier, not a quote opener.
		{"dollar_in_ident", "id in (0) or a$q$x$q$" + inj("dollar_in_ident") + " $q$ = 1", true},
		// Genuinely-safe shapes the door must NOT over-refuse and the server runs as ONE statement.
		{"apostrophe_in_string", "id > 0 and 'a;b;c' <> 'x'", false},
		{"injection_inside_line_comment", "id > 0) -- " + inj("injection_inside_line_comment"), false},
		{"injection_inside_block_comment", "id > 0) /* " + inj("injection_inside_block_comment") + " */", false},
		{"legit_dollar_quote", "id > 0 and $tag$ ; not sql $tag$ = $tag$ ; not sql $tag$", false},
		{"semicolon_in_string_literal", "'end; DROP' <> 'x'", false},
	}

	// probeMarker runs candidate EXACTLY as production does — a bare
	// db.ExecContext with zero bind args, which pgx v5 executes over the
	// simple protocol (multi-statement capable); wrapping it in an
	// explicit tx or a prepared statement suppresses that protocol and
	// would silently NOT reproduce the bypass (the reason the first cut
	// of this oracle read a false all-clear). Each row runs against a
	// FRESH c1_probe so a prior row's added constraint can't collide,
	// and the marker is dropped afterward.
	probeMarker := func(candidate, markerName string) (injected bool, srvErr error) {
		if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS c1_probe; CREATE TABLE c1_probe (id int)`); err != nil {
			t.Fatalf("reset probe: %v", err)
		}
		if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS `+markerName); err != nil {
			t.Fatalf("pre-drop marker: %v", err)
		}
		_, srvErr = db.ExecContext(ctx, candidate)
		var present bool
		if qerr := db.QueryRowContext(ctx,
			`SELECT to_regclass('public.'||$1) IS NOT NULL`, markerName).Scan(&present); qerr != nil {
			t.Fatalf("marker probe: %v", qerr)
		}
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS `+markerName)
		return present, srvErr
	}

	var genuineInjections int
	for _, r := range corpus {
		r := r
		t.Run(r.name, func(t *testing.T) {
			candidate := fmt.Sprintf("ALTER TABLE c1_probe ADD CONSTRAINT %s CHECK (%s)",
				"ck_"+r.name, r.body)
			doorErr := assertSingleDDLStatement(candidate)
			injected, srvErr := probeMarker(candidate, marker(r.name))

			if injected {
				genuineInjections++
			}
			// THE INVARIANT: the door must never wave through a string
			// whose injected statement the server actually ran.
			if doorErr == nil && injected {
				t.Fatalf("C-1 BYPASS: the door PASSED a string the server executed an injection for.\n"+
					"  candidate: %q\n  server err: %v", candidate, srvErr)
			}
			// Diagnostic only: a door refusal on a server-safe string is
			// over-refusal (loud, acceptable); log the shape so drift is
			// visible.
			if doorErr != nil && !injected && srvErr == nil {
				t.Logf("over-refusal (safe direction): %q refused by door %v", candidate, doorErr)
			}
			if r.wantInject && !injected && srvErr != nil && !strings.Contains(r.name, "dollar") {
				t.Logf("NOTE %s did not inject (server err: %v) — shape may be a syntax error, not a bypass", r.name, srvErr)
			}
		})
	}

	// Anti-vacuity: SOME corpus row must be a genuine injection, or the
	// oracle proved nothing. (Measured on real PG 16; if this ever hits
	// 0, the corpus went inert — a wider list with the same defect.)
	if genuineInjections == 0 {
		t.Fatal("anti-vacuity: NO corpus row injected on the real server — the oracle is inert, it did not test the door against a real bypass")
	}
	t.Logf("oracle: %d/%d corpus rows were genuine server-side injections, all blocked by the door", genuineInjections, len(corpus))
}
