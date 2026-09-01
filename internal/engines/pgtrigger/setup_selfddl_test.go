// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the self-DDL suppression: a `trigger setup` re-run over an
// existing event-trigger install must never record its OWN statements as
// op='X' rows (Bug 257), the suppression must be bound to evidence an
// ordinary writer cannot forge (audit SEC-2), and its off-state must be
// structural rather than a trailing statement (audit C-1). Four render/body
// properties carry that, pinned here without a database; the live
// differentials — zero X rows after a re-setup over a STREAMED install, an
// unprivileged session that CANNOT suppress, an aborted plan that leaves
// nothing set — are setup_selfddl_integration_test.go.

package pgtrigger

import (
	"regexp"
	"strings"
	"testing"
)

// TestRenderSetupDDL_SelfDDLSuppression pins the load-bearing render
// properties across the posture × event-trigger matrix:
//
//  1. The plan is ONE transaction — BEGIN first, COMMIT last — and the
//     marker rides SET LOCAL, so PostgreSQL reverts it at both commit and
//     rollback (audit C-1). No bare `SET`/`RESET` of the marker survives.
//  2. The DDL capture function body opens with the suppression check.
//  3. The function's CREATE OR REPLACE precedes EVERY statement whose
//     command tag the event trigger watches — the ordering that makes the
//     FIRST re-setup over an old-function install clean (the old body
//     ignores the marker, so it must be replaced before the first statement
//     it would record). The watched tags are parsed out of the rendered
//     CREATE EVENT TRIGGER itself, so a grown TAG filter grows this pin.
//  4. An evidence ARM precedes every watched-tag statement too, and the
//     DISARM is the last statement before COMMIT (SEC-2): a watched
//     statement that ran before any arm would fall through to the bootstrap
//     arm, and an armed row that outlived the plan would be presentable
//     evidence.
func TestRenderSetupDDL_SelfDDLSuppression(t *testing.T) {
	t.Parallel()
	specs := []tableTriggerSpec{
		{Name: "orders", PKCols: []string{"id"}},
		{Name: "line_items", PKCols: []string{"tenant_id", "order_id"}},
	}
	for _, tc := range []struct {
		name            string
		canEventTrigger bool
		captureRepl     bool
	}{
		{"plain, event-trigger tier", true, false},
		{"opt-in, event-trigger tier", true, true},
		{"plain, polled tier", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stmts := renderSetupDDL("public", specs, tc.canEventTrigger, CapturePayloadFull, tc.captureRepl, nil)

			if got := stmts[0]; got != "BEGIN" {
				t.Errorf("plan does not OPEN with BEGIN (got %q) — the marker's off-state would ride a trailing statement again (C-1)", got)
			}
			if got := stmts[len(stmts)-1]; got != "COMMIT" {
				t.Errorf("plan does not CLOSE with COMMIT (got %q)", got)
			}
			if got := stmts[len(stmts)-2]; got != renderDisarmSetupEvidence(`"public"."`+ChangeLogMetaTable+`"`) {
				t.Errorf("the statement before COMMIT is not the evidence DISARM (got %q) — an armed row would outlive the plan", firstLine(got))
			}

			markerIdx := -1
			for i, s := range stmts {
				switch {
				case s == "SET LOCAL "+setupSessionGUC+" = '"+setupBootstrapMarker+"'":
					markerIdx = i
				case strings.HasPrefix(s, "SET "+setupSessionGUC),
					strings.HasPrefix(s, "RESET "+setupSessionGUC):
					t.Errorf("statement %d sets the marker with SESSION scope (%q) — an abandoned hand-applied plan leaks it into the operator's session (C-1)", i, s)
				}
			}
			if markerIdx < 0 {
				t.Fatal("render never sets the suppression marker with SET LOCAL")
			}

			armIdxs := []int{}
			for i, s := range stmts {
				if strings.Contains(s, "PERFORM pg_catalog.set_config('"+setupSessionGUC+"'") {
					armIdxs = append(armIdxs, i)
				}
			}
			// Two arms: the early one covers a v4 install's meta ADD COLUMN
			// no-ops (which DO fire ddl_command_end); the second, after the
			// migration, is strict on every install vintage.
			if len(armIdxs) != 2 {
				t.Fatalf("render emits %d evidence ARM statement(s) %v, want 2 (early + post-migration)", len(armIdxs), armIdxs)
			}
			if armIdxs[0] < markerIdx {
				t.Errorf("the first ARM (%d) precedes the SET LOCAL marker (%d) — the arm adopts the nonce, so it must follow", armIdxs[0], markerIdx)
			}

			if !tc.canEventTrigger {
				// Polled tier: no DDL capture function in the plan at all
				// (nothing records, so nothing to order against).
				for _, s := range stmts {
					if strings.Contains(s, CaptureFunctionDDL) {
						t.Errorf("polled-tier render emits the DDL capture function:\n%s", s)
					}
				}
				return
			}

			fnIdx := -1
			for i, s := range stmts {
				if strings.HasPrefix(s, "CREATE OR REPLACE FUNCTION "+ddlFnRef("public")) {
					fnIdx = i
					break
				}
			}
			if fnIdx < 0 {
				t.Fatal("render is missing the DDL capture function")
			}
			if !strings.Contains(stmts[fnIdx], captureDDLSuppressionCheck(`"public"."`+ChangeLogMetaTable+`"`)) {
				t.Errorf("DDL capture function body is missing the suppression check:\n%s", stmts[fnIdx])
			}

			dropFnIdx := -1
			for i, s := range stmts {
				if strings.HasPrefix(s, "CREATE OR REPLACE FUNCTION "+dropFnRef("public")) {
					dropFnIdx = i
					break
				}
			}
			if dropFnIdx < 0 {
				t.Fatal("render is missing the sql_drop capture function (D-1)")
			}
			if !strings.Contains(stmts[dropFnIdx], captureDDLSuppressionCheck(`"public"."`+ChangeLogMetaTable+`"`)) {
				t.Errorf("sql_drop capture function body is missing the suppression check:\n%s", stmts[dropFnIdx])
			}
			// The sql_drop arm is UNFILTERED, so every DROP the plan issues
			// reaches it — the same ordering rule as the tagged arm, for the
			// same Bug 257 reason.
			for i, s := range stmts {
				if strings.HasPrefix(s, "DROP ") && i < dropFnIdx {
					t.Errorf("statement %d (%q) is a DROP preceding the sql_drop capture function at %d", i, firstLine(s), dropFnIdx)
				}
			}

			// Derive the watched tag set from the rendered event trigger so
			// this pin tracks the TAG filter instead of a hand-copied list.
			// The floor is 3, not the original 5: D-1 moved 'DROP TABLE' and
			// 'DROP INDEX' off this arm, because pg_event_trigger_ddl_commands()
			// returns zero rows for a DROP and the two tags could never record
			// here. A regression that puts them back is caught below.
			tags := watchedCommandTags(t, stmts)
			if len(tags) < 3 {
				t.Fatalf("parsed only %d watched tags %v from the CREATE EVENT TRIGGER (anti-vacuity: the parser has probably stopped matching)", len(tags), tags)
			}
			for _, tag := range tags {
				if strings.HasPrefix(tag, "DROP ") {
					t.Errorf("ddl_command_end watches %q, which pg_event_trigger_ddl_commands() can never report — the TAG list reads broader than the truth (D-1); drops belong to the sql_drop arm", tag)
				}
			}
			recordable := 0
			for i, s := range stmts {
				for _, tag := range tags {
					if !strings.HasPrefix(s, tag+" ") {
						continue
					}
					recordable++
					if i < fnIdx {
						t.Errorf("statement %d (%q) carries watched tag %q but precedes the DDL capture function at %d — "+
							"the first re-setup over an old-function install records it (Bug 257's ordering half)",
							i, firstLine(s), tag, fnIdx)
					}
					if i < armIdxs[0] {
						t.Errorf("statement %d (%q) carries watched tag %q but precedes the first evidence ARM at %d — "+
							"it would be covered by the bootstrap arm alone (SEC-2)",
							i, firstLine(s), tag, armIdxs[0])
					}
				}
			}
			// Anti-vacuity floor: the two meta ADD COLUMNs + the change-log /
			// meta / consumers CREATEs are all watched, so a render where
			// nothing matched means the prefix match broke, not that the plan
			// went quiet. (Was 7 while the two index-diet DROP INDEXes counted;
			// D-1 took the DROP tags off this arm — those statements are now
			// covered by the sql_drop ordering check above.)
			if recordable < 5 {
				t.Errorf("only %d statements matched a watched tag; the ordering pin is not exercising the plan", recordable)
			}
		})
	}
}

// TestCaptureDDLSuppressionCheck_BindsToUnforgeableEvidence is the SEC-2
// gate at the render layer: the emitted body must never accept the marker
// VALUE on its own. Every path that returns early has to additionally
// demand something the firing session cannot manufacture — the armed
// (PID, nonce, freshness) row in the owner-only meta table, or, in the
// bootstrap window, that the session is authenticated as the install's own
// role.
//
// Scope, stated rather than implied: this grades the rendered SQL text. That
// the SERVER agrees — that an unprivileged role setting the marker really
// does get recorded — is TestSetup_UnprivilegedSessionCannotSuppressDDL, on
// real PG. Neither is sufficient alone: this one catches a body that stops
// asking, that one catches a body that asks the wrong thing.
func TestCaptureDDLSuppressionCheck_BindsToUnforgeableEvidence(t *testing.T) {
	t.Parallel()
	body := captureDDLSuppressionCheck(`"public"."` + ChangeLogMetaTable + `"`)

	for _, want := range []struct {
		frag string
		why  string
	}{
		{"m." + metaSetupPIDCol + " = pg_catalog.pg_backend_pid()", "no role can choose its own backend PID; without this the marker alone suppresses"},
		{"m." + metaSetupNonceCol + " = v_marker", "the marker must equal the nonce Setup armed, not a literal an attacker can type"},
		{"m." + metaSetupAtCol + " > pg_catalog.clock_timestamp() - '" + setupEvidenceFreshness + "'::interval", "a stale armed row must not authorize suppression forever"},
		{"session_user = current_user", "the bootstrap arm's unforgeable half — SET SESSION AUTHORIZATION needs superuser"},
		{"m." + metaSetupNonceCol + " IS NULL", "the bootstrap arm must be closed once any v4-aware setup has disarmed (which writes a non-NULL sentinel)"},
		{"WHEN OTHERS THEN\n                v_suppress := FALSE", "fail-safe direction: an unreadable evidence read RECORDS"},
		{"IF v_suppress IS TRUE THEN", "IS TRUE, not a bare truthiness test: SELECT INTO over zero rows yields NULL"},
	} {
		if !strings.Contains(body, want.frag) {
			t.Errorf("suppression check does not require %q — %s\n\nbody:\n%s", want.frag, want.why, body)
		}
	}

	// The v0.133.1 shape, verbatim: grading the marker against a literal and
	// returning. Its absence is the whole point of SEC-2.
	if regexp.MustCompile(`current_setting\('` + regexp.QuoteMeta(setupSessionGUC) + `', true\) = '[^']*' THEN\s*\n\s*RETURN;`).MatchString(body) {
		t.Errorf("suppression check still returns on the marker value alone — SEC-2's off-switch is back:\n%s", body)
	}

	// The disarm must not write NULL: NULL is the bootstrap arm's key.
	if strings.Contains(renderDisarmSetupEvidence("m"), metaSetupNonceCol+" = NULL") {
		t.Errorf("the disarm writes NULL into %s — that re-opens the bootstrap arm permanently", metaSetupNonceCol)
	}
}

var eventTriggerTagRe = regexp.MustCompile(`WHEN TAG IN \(([^)]+)\)`)

// watchedCommandTags parses the quoted tag list out of the rendered
// CREATE EVENT TRIGGER statement.
func watchedCommandTags(t *testing.T, stmts []string) []string {
	t.Helper()
	for _, s := range stmts {
		if !strings.HasPrefix(s, "CREATE EVENT TRIGGER ") {
			continue
		}
		m := eventTriggerTagRe.FindStringSubmatch(s)
		if m == nil {
			t.Fatalf("CREATE EVENT TRIGGER has no parseable TAG IN list: %s", s)
		}
		raws := strings.Split(m[1], ",")
		tags := make([]string, 0, len(raws))
		for _, raw := range raws {
			tags = append(tags, strings.Trim(strings.TrimSpace(raw), "'"))
		}
		return tags
	}
	t.Fatal("render has no CREATE EVENT TRIGGER statement")
	return nil
}
