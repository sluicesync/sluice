// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the Bug 257 self-DDL suppression: a `trigger setup` re-run
// over an existing event-trigger install must never record its OWN
// statements as op='X' rows (the meta ADD COLUMN migration fires
// ddl_command_end even as an IF-NOT-EXISTS no-op; the ADR-0185 opt-in adds
// two ALTER TABLE statements per table), or the next warm resume refuses
// "observed source-side DDL" for DDL no operator ran. Three render
// properties carry the fix, pinned here without a database; the live
// differential (zero X rows after a re-setup over a STREAMED install, warm
// resume consumes) is setup_selfddl_integration_test.go.

package pgtrigger

import (
	"regexp"
	"strings"
	"testing"
)

// TestRenderSetupDDL_SelfDDLSuppression pins the three load-bearing render
// properties across the posture × event-trigger matrix:
//
//  1. The plan is bracketed by SET/RESET of the session marker, so every
//     statement between them runs suppressed (Setup pins one session; a
//     hand-applied dry-run plan gets the same from psql's single session).
//  2. The DDL capture function body opens with the suppression check.
//  3. The function's CREATE OR REPLACE precedes EVERY statement whose
//     command tag the event trigger watches — the ordering that makes the
//     FIRST re-setup over an old-function install clean (the old body
//     ignores the GUC, so it must be replaced before the first statement
//     it would record). The watched tags are parsed out of the rendered
//     CREATE EVENT TRIGGER itself, so a grown TAG filter grows this pin.
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stmts := renderSetupDDL("public", specs, tc.canEventTrigger, CapturePayloadFull, tc.captureRepl)

			if got := stmts[0]; got != "SET "+setupSessionGUC+" = 'on'" {
				t.Errorf("plan does not OPEN with the suppression SET (got %q) — statements before it run unsuppressed", got)
			}
			if got := stmts[len(stmts)-1]; got != "RESET "+setupSessionGUC {
				t.Errorf("plan does not CLOSE with the suppression RESET (got %q) — a hand-applied plan would leave the session marked", got)
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
			if !strings.Contains(stmts[fnIdx], captureDDLSuppressionCheck) {
				t.Errorf("DDL capture function body is missing the Bug 257 suppression check:\n%s", stmts[fnIdx])
			}

			// Derive the watched tag set from the rendered event trigger so
			// this pin tracks the TAG filter instead of a hand-copied list.
			tags := watchedCommandTags(t, stmts)
			if len(tags) < 5 {
				t.Fatalf("parsed only %d watched tags %v from the CREATE EVENT TRIGGER (anti-vacuity: the parser has probably stopped matching)", len(tags), tags)
			}
			recordable := 0
			for i, s := range stmts {
				for _, tag := range tags {
					if strings.HasPrefix(s, tag+" ") {
						recordable++
						if i < fnIdx {
							t.Errorf("statement %d (%q) carries watched tag %q but precedes the DDL capture function at %d — "+
								"the first re-setup over an old-function install records it (Bug 257's ordering half)",
								i, firstLine(s), tag, fnIdx)
						}
					}
				}
			}
			// Anti-vacuity floor: the meta ADD COLUMN + the change-log /
			// meta / consumers CREATEs + the two index-diet DROPs are all
			// watched, so a render where nothing matched means the prefix
			// match broke, not that the plan went quiet.
			if recordable < 6 {
				t.Errorf("only %d statements matched a watched tag; the ordering pin is not exercising the plan", recordable)
			}
		})
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
