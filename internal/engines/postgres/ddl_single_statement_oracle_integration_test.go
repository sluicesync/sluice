//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
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
	probeMarker := func(candidate, markerName string, cleanupExtra ...string) (injected bool, srvErr error) {
		if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS c1_probe; CREATE TABLE c1_probe (id int)`); err != nil {
			t.Fatalf("reset probe: %v", err)
		}
		for _, tbl := range append([]string{markerName}, cleanupExtra...) {
			if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS `+tbl); err != nil {
				t.Fatalf("pre-drop %s: %v", tbl, err)
			}
		}
		_, srvErr = db.ExecContext(ctx, candidate)
		var present bool
		if qerr := db.QueryRowContext(ctx,
			`SELECT to_regclass('public.'||$1) IS NOT NULL`, markerName).Scan(&present); qerr != nil {
			t.Fatalf("marker probe: %v", qerr)
		}
		for _, tbl := range append([]string{markerName}, cleanupExtra...) {
			_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS `+tbl)
		}
		return present, srvErr
	}

	// Two emission TEMPLATES, because the door protects both real restore
	// emission shapes and the 2026-08-14 regression cycle found the live
	// RCE rode the INLINE one, not the ALTER wrapper this oracle first
	// used (the "gate must cover the real path, not a representative"
	// discipline applied to the gate itself). The ALTER corpus bodies
	// close ONE paren; the inline template wraps the body in
	// `CREATE TABLE <t> (id int, CONSTRAINT ck CHECK (<body>))`, so its
	// injecting body must close TWO and re-open to balance — the shape
	// the cycle proved live. The door is template-agnostic (it scans the
	// string), so the invariant holds for both; paren balance is the
	// server's to judge.
	type tmpl struct {
		name  string
		build func(rowName, body string) (candidate string, cleanupExtra []string)
	}
	inlineTable := func(rowName string) string { return "c1_inl_" + rowName }
	templates := []tmpl{
		{"alter", func(rowName, body string) (string, []string) {
			return fmt.Sprintf("ALTER TABLE c1_probe ADD CONSTRAINT %s CHECK (%s)", "ck_"+rowName, body), nil
		}},
		{"inline", func(rowName, body string) (string, []string) {
			t := inlineTable(rowName)
			return fmt.Sprintf("CREATE TABLE %s (id int, CONSTRAINT %s CHECK (%s))", t, "ck_"+rowName, body), []string{t}
		}},
	}
	// The inline template's own genuine-injection body (cycle-proven
	// shape): close the CHECK paren and the CREATE-TABLE paren, inject,
	// then a fresh valid CREATE to balance the trailing `)` the template
	// appends. Added to the corpus flagged wantInject; it is a syntax
	// error under the ALTER template (harmless — just does not inject
	// there), and a live injection under the inline template.
	inlineInj := "true)) /* ' */ ; CREATE TABLE " + marker("inline_twoparen") +
		" (); /* ' */ CREATE TABLE " + inlineTable("inline_twoparen_pad") + " (y int CHECK (true"
	corpus = append(corpus, row{"inline_twoparen", inlineInj, true})

	perTemplate := map[string]int{}
	for _, tp := range templates {
		for _, r := range corpus {
			tp, r := tp, r
			t.Run(tp.name+"/"+r.name, func(t *testing.T) {
				candidate, cleanupExtra := tp.build(r.name, r.body)
				doorErr := assertSingleDDLStatement(candidate)
				injected, srvErr := probeMarker(candidate, marker(r.name),
					append(cleanupExtra, inlineTable("inline_twoparen_pad"))...)

				if injected {
					perTemplate[tp.name]++
				}
				// THE INVARIANT: the door must never wave through a string
				// whose injected statement the server actually ran.
				if doorErr == nil && injected {
					t.Fatalf("C-1 BYPASS (%s template): the door PASSED a string the server executed an injection for.\n"+
						"  candidate: %q\n  server err: %v", tp.name, candidate, srvErr)
				}
				if doorErr != nil && !injected && srvErr == nil {
					t.Logf("over-refusal (safe direction): %q refused by door %v", candidate, doorErr)
				}
			})
		}
	}

	// Anti-vacuity, PER TEMPLATE: each real emission shape must have
	// exercised the door against a genuine server-side injection, or that
	// template's coverage is inert (a gate that does not cover the real
	// path — the exact defect the 2026-08-14 audit's C-1 was).
	for _, tp := range templates {
		if perTemplate[tp.name] == 0 {
			t.Fatalf("anti-vacuity: the %s template produced NO genuine server-side injection — its door coverage is inert", tp.name)
		}
	}
	t.Logf("oracle: genuine server-side injections blocked by the door, per template: %v", perTemplate)
}
