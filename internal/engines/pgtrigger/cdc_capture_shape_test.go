// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the capture-shape door's grading half
// ([gradeCaptureShape], audit 2026-08-26 F2; posture match ADR-0185).
// Every defect class the door names gets a cell, plus the accept cells
// (healthy install under BOTH postures; the polled-fingerprint exemption)
// so the door cannot drift into false-refusing a legitimate source. The
// posture-mismatch cells pin BOTH directions — 'A' where origin-only was
// recorded and 'O' where the --capture-replicated-writes opt-in was — on
// each trigger of the pair. The catalog-reading half and the end-to-end
// refusals run against real PG in cdc_capture_shape_integration_test.go
// and capture_replicated_integration_test.go.

package pgtrigger

import (
	"database/sql"
	"encoding/base64"
	"strings"
	"testing"
)

// healthyTriggers returns a correctly-installed plain-posture pair for
// one table.
//
// nargs: 1 is not decoration — setup passes the PK column list as the ROW
// trigger`s one argument, and the door refuses a zero-arg row trigger
// (audit 2026-09-06 S-2 layer 3). A fixture that omits it is modelling a
// trigger whose captured rows would carry the wrong key.
func healthyTriggers(table string) []installedCaptureTrigger {
	return []installedCaptureTrigger{
		{table: table, name: CaptureTriggerRow, enabled: "O", fn: CaptureFunctionRow, fnSchema: "public", tgtype: expectedRowTgType, nargs: 1},
		{table: table, name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, fnSchema: "public", tgtype: expectedTruncateTgType},
	}
}

// alwaysTriggers returns a correctly-installed --capture-replicated-writes
// (ENABLE ALWAYS) pair for one table.
func alwaysTriggers(table string) []installedCaptureTrigger {
	return []installedCaptureTrigger{
		{table: table, name: CaptureTriggerRow, enabled: "A", fn: CaptureFunctionRow, fnSchema: "public", tgtype: expectedRowTgType, nargs: 1},
		{table: table, name: CaptureTriggerTruncate, enabled: "A", fn: CaptureFunctionTruncate, fnSchema: "public", tgtype: expectedTruncateTgType},
	}
}

// healthyEvt / healthyDropEvt are the two event-trigger arms' states in a
// full (event-trigger-tier) install.
var (
	healthyEvt     = eventTriggerState{present: true, enabled: "O", fn: CaptureFunctionDDL, fnSchema: "public"}
	healthyDropEvt = eventTriggerState{present: true, enabled: "O", fn: CaptureFunctionDrop, fnSchema: "public"}
	// The ENABLE ALWAYS ('A') shapes the --capture-replicated-writes opt-in
	// installs (ADR-0185 + audit A-1).
	alwaysEvt     = eventTriggerState{present: true, enabled: "A", fn: CaptureFunctionDDL, fnSchema: "public"}
	alwaysDropEvt = eventTriggerState{present: true, enabled: "A", fn: CaptureFunctionDrop, fnSchema: "public"}
)

// tiers builds the event-trigger-tier state with BOTH arms' functions
// installed and the given trigger rows. The zero ddlCaptureState is the
// polled-fingerprint shape (no functions, no triggers).
func tiers(ddlEvt, dropEvt eventTriggerState) ddlCaptureState {
	return ddlCaptureState{
		fnPresent: map[string]bool{CaptureFunctionDDL: true, CaptureFunctionDrop: true},
		triggers:  map[string]eventTriggerState{CaptureTriggerDDL: ddlEvt, CaptureTriggerDrop: dropEvt},
	}
}

// healthyTiers is the full event-trigger install both arms healthy.
func healthyTiers() ddlCaptureState { return tiers(healthyEvt, healthyDropEvt) }

// preDropArmTiers is an install made before v0.136.0: the ddl_command_end arm
// exists, the sql_drop one was never created. Graded as EXEMPT (the WARN
// carries it — warnDropCaptureAbsent), never as a refusal, so upgrading the
// binary cannot strand a running sync.
func preDropArmTiers() ddlCaptureState {
	return ddlCaptureState{
		fnPresent: map[string]bool{CaptureFunctionDDL: true},
		triggers:  map[string]eventTriggerState{CaptureTriggerDDL: healthyEvt},
	}
}

func TestGradeCaptureShape(t *testing.T) {
	cases := []struct {
		name              string
		installed         []installedCaptureTrigger
		ddl               ddlCaptureState
		captureReplicated bool     // the recorded ADR-0185 posture
		wantErr           []string // all must appear; empty = accept
	}{
		{
			name:      "healthy full install accepts",
			installed: healthyTriggers("t"),
			ddl:       healthyTiers(),
		},
		{
			name:      "polled-fingerprint install (no DDL function, no event trigger) accepts",
			installed: healthyTriggers("t"),
			// zero ddlCaptureState: no capture function, so no event
			// trigger of either arm was ever expected — requiring one would
			// false-refuse every --allow-polled-fingerprint source.
		},
		{
			name:              "healthy ENABLE ALWAYS install accepts under the opt-in posture",
			installed:         alwaysTriggers("t"),
			ddl:               tiers(alwaysEvt, alwaysDropEvt),
			captureReplicated: true,
		},
		{
			// Pre-ADR-0185 the door accepted 'A' blindly as "strictly-more
			// capture"; the posture match narrows that — hand-flipped
			// ENABLE ALWAYS captures replica-role writes without the
			// echo-loop vetting.
			name:      "ENABLE ALWAYS under a recorded origin-only posture refuses (hand-flipped drift)",
			installed: alwaysTriggers("t"),
			ddl:       healthyTiers(),
			wantErr:   []string{"ENABLE ALWAYS", "ORIGIN-ONLY", "--capture-replicated-writes"},
		},
		{
			name: "plain row trigger under the opt-in posture refuses (replicated writes silently uncaptured)",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerRow, enabled: "O", fn: CaptureFunctionRow, fnSchema: "public", tgtype: expectedRowTgType, nargs: 1},
				{table: "t", name: CaptureTriggerTruncate, enabled: "A", fn: CaptureFunctionTruncate, fnSchema: "public", tgtype: expectedTruncateTgType},
			},
			ddl:               tiers(alwaysEvt, alwaysDropEvt),
			captureReplicated: true,
			wantErr:           []string{CaptureTriggerRow, "--capture-replicated-writes", "NOT being captured"},
		},
		{
			name: "plain truncate trigger under the opt-in posture refuses too (both members graded)",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerRow, enabled: "A", fn: CaptureFunctionRow, fnSchema: "public", tgtype: expectedRowTgType, nargs: 1},
				{table: "t", name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, fnSchema: "public", tgtype: expectedTruncateTgType},
			},
			ddl:               tiers(alwaysEvt, alwaysDropEvt),
			captureReplicated: true,
			wantErr:           []string{CaptureTriggerTruncate, "--capture-replicated-writes", "TRUNCATE"},
		},
		{
			name: "disabled trigger still refuses under the opt-in posture",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerRow, enabled: "D", fn: CaptureFunctionRow, fnSchema: "public", tgtype: expectedRowTgType, nargs: 1},
				{table: "t", name: CaptureTriggerTruncate, enabled: "A", fn: CaptureFunctionTruncate, fnSchema: "public", tgtype: expectedTruncateTgType},
			},
			ddl:               tiers(alwaysEvt, alwaysDropEvt),
			captureReplicated: true,
			wantErr:           []string{"DISABLED"},
		},
		{
			// A-1: the event triggers now carry the posture too, in BOTH
			// directions. 'A' under a recorded origin-only install is
			// hand-flipped drift (and makes the two tiers disagree the other
			// way round).
			name:      "event trigger ENABLE ALWAYS under a recorded origin-only posture refuses",
			installed: healthyTriggers("t"),
			ddl:       tiers(eventTriggerState{present: true, enabled: "A", fn: CaptureFunctionDDL, fnSchema: "public"}, healthyDropEvt),
			wantErr:   []string{CaptureTriggerDDL, "ENABLE ALWAYS", "ORIGIN-ONLY"},
		},
		{
			name:              "plain DDL event trigger under the opt-in posture refuses (A-1's exact shape)",
			installed:         alwaysTriggers("t"),
			ddl:               tiers(healthyEvt, alwaysDropEvt),
			captureReplicated: true,
			wantErr:           []string{CaptureTriggerDDL, "--capture-replicated-writes", "NOT detected"},
		},
		{
			name:              "plain sql_drop event trigger under the opt-in posture refuses too (both arms graded)",
			installed:         alwaysTriggers("t"),
			ddl:               tiers(alwaysEvt, healthyDropEvt),
			captureReplicated: true,
			wantErr:           []string{CaptureTriggerDrop, "--capture-replicated-writes", "DROP of a captured table"},
		},
		{
			name:              "both event triggers ENABLE ALWAYS accept under the opt-in posture",
			installed:         alwaysTriggers("t"),
			ddl:               tiers(alwaysEvt, alwaysDropEvt),
			captureReplicated: true,
		},
		{
			// The D-1 upgrade shape: the sql_drop arm was never installed.
			// EXEMPT here by design — a refusal would strand every running
			// sync at the moment the operator upgrades the binary, for a gap
			// that is bounded and static. warnDropCaptureAbsent is what makes
			// it loud (pinned in preflight_ddl_detection_integration_test.go).
			name:      "install predating the sql_drop arm accepts (WARN carries it, not a refusal)",
			installed: healthyTriggers("t"),
			ddl:       preDropArmTiers(),
		},
		{
			name:      "drop function present but its event trigger dropped refuses",
			installed: healthyTriggers("t"),
			ddl:       tiers(healthyEvt, eventTriggerState{}),
			wantErr:   []string{CaptureTriggerDrop, "MISSING", "DROP of a captured table"},
		},
		{
			name:      "disabled drop event trigger refuses",
			installed: healthyTriggers("t"),
			ddl:       tiers(healthyEvt, eventTriggerState{present: true, enabled: "D", fn: CaptureFunctionDrop, fnSchema: "public"}),
			wantErr:   []string{CaptureTriggerDrop, "not enabled", "DROP of a captured table"},
		},
		{
			name:      "drop event trigger bound to a foreign function refuses",
			installed: healthyTriggers("t"),
			ddl:       tiers(healthyEvt, eventTriggerState{present: true, enabled: "O", fn: "somebody_elses_drop_hook", fnSchema: "public"}),
			wantErr:   []string{CaptureTriggerDrop, "somebody_elses_drop_hook"},
		},
		{
			name:    "zero triggers anywhere refuses (the dropped-everything floor)",
			wantErr: []string{"NO capture trigger", "trigger setup"},
		},
		{
			name: "missing row trigger refuses naming it",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, fnSchema: "public", tgtype: expectedTruncateTgType},
			},
			wantErr: []string{`"t"`, CaptureTriggerRow, "MISSING", "trigger setup"},
		},
		{
			name: "missing truncate trigger refuses naming it",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerRow, enabled: "O", fn: CaptureFunctionRow, fnSchema: "public", tgtype: expectedRowTgType, nargs: 1},
			},
			wantErr: []string{CaptureTriggerTruncate, "MISSING", "TRUNCATE"},
		},
		{
			name: "disabled trigger refuses",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerRow, enabled: "D", fn: CaptureFunctionRow, fnSchema: "public", tgtype: expectedRowTgType, nargs: 1},
				{table: "t", name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, fnSchema: "public", tgtype: expectedTruncateTgType},
			},
			wantErr: []string{"DISABLED", "DISABLE TRIGGER"},
		},
		{
			name: "ENABLE REPLICA trigger refuses (fires for no origin write)",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerRow, enabled: "R", fn: CaptureFunctionRow, fnSchema: "public", tgtype: expectedRowTgType, nargs: 1},
				{table: "t", name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, fnSchema: "public", tgtype: expectedTruncateTgType},
			},
			wantErr: []string{"ENABLE REPLICA", "session_replication_role"},
		},
		{
			name: "foreign bound function refuses",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerRow, enabled: "O", fn: "audit_everything", fnSchema: "public", tgtype: expectedRowTgType, nargs: 1},
				{table: "t", name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, fnSchema: "public", tgtype: expectedTruncateTgType},
			},
			wantErr: []string{"audit_everything", CaptureFunctionRow, "not what this sluice installs"},
		},
		{
			name: "wrong trigger shape (tgtype) refuses",
			installed: []installedCaptureTrigger{
				// BEFORE instead of AFTER: tgtype carries the BEFORE bit.
				{table: "t", name: CaptureTriggerRow, enabled: "O", fn: CaptureFunctionRow, fnSchema: "public", tgtype: expectedRowTgType | 1<<1},
				{table: "t", name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, fnSchema: "public", tgtype: expectedTruncateTgType},
			},
			wantErr: []string{"tgtype", "trigger setup"},
		},
		{
			name:      "DDL function present but event trigger dropped refuses",
			installed: healthyTriggers("t"),
			ddl:       tiers(eventTriggerState{}, healthyDropEvt),
			wantErr:   []string{CaptureTriggerDDL, "MISSING", "DROP EVENT TRIGGER"},
		},
		{
			name:      "disabled event trigger refuses",
			installed: healthyTriggers("t"),
			ddl:       tiers(eventTriggerState{present: true, enabled: "D", fn: CaptureFunctionDDL, fnSchema: "public"}, healthyDropEvt),
			wantErr:   []string{CaptureTriggerDDL, "not enabled"},
		},
		{
			name:      "event trigger bound to a foreign function refuses",
			installed: healthyTriggers("t"),
			ddl:       tiers(eventTriggerState{present: true, enabled: "O", fn: "somebody_elses_ddl_hook", fnSchema: "public"}, healthyDropEvt),
			wantErr:   []string{CaptureTriggerDDL, "somebody_elses_ddl_hook"},
		},
		// SLP-1 (audit 2026-09-01): a SAME-NAMED function in ANOTHER schema
		// bound to a capture trigger. Every arm compared names, so all four
		// tiers passed it while the body arm graded the untouched original;
		// on real PG 16 the row tier recorded 0 change-log rows for 3
		// INSERTs + 1 UPDATE and the DDL tier recorded no `X` for an ALTER
		// TABLE. One cell per tier, because a refusal that reached one and
		// not its siblings is the sibling-miss shape.
		{
			name: "row trigger bound to a same-named function in another schema refuses (SLP-1)",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerRow, enabled: "O", fn: CaptureFunctionRow, fnSchema: "decoy", tgtype: expectedRowTgType, nargs: 1},
				{table: "t", name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, fnSchema: "public", tgtype: expectedTruncateTgType},
			},
			ddl:     healthyTiers(),
			wantErr: []string{CaptureTriggerRow, `"decoy"."` + CaptureFunctionRow + `"`, "OUTSIDE the sluice schema", "INSERT/UPDATE/DELETE"},
		},
		{
			name: "truncate trigger bound to a same-named function in another schema refuses (SLP-1)",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerRow, enabled: "O", fn: CaptureFunctionRow, fnSchema: "public", tgtype: expectedRowTgType, nargs: 1},
				{table: "t", name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, fnSchema: "decoy", tgtype: expectedTruncateTgType},
			},
			ddl:     healthyTiers(),
			wantErr: []string{CaptureTriggerTruncate, `"decoy"."` + CaptureFunctionTruncate + `"`, "OUTSIDE the sluice schema", "TRUNCATE"},
		},
		{
			name:      "ddl_command_end event trigger bound to a same-named function in another schema refuses (SLP-1)",
			installed: healthyTriggers("t"),
			ddl:       tiers(eventTriggerState{present: true, enabled: "O", fn: CaptureFunctionDDL, fnSchema: "decoy"}, healthyDropEvt),
			wantErr:   []string{CaptureTriggerDDL, `"decoy"."` + CaptureFunctionDDL + `"`, "OUTSIDE the sluice schema", "ALTER/CREATE DDL"},
		},
		{
			name:      "sql_drop event trigger bound to a same-named function in another schema refuses (SLP-1)",
			installed: healthyTriggers("t"),
			ddl:       tiers(healthyEvt, eventTriggerState{present: true, enabled: "O", fn: CaptureFunctionDrop, fnSchema: "decoy"}),
			wantErr:   []string{CaptureTriggerDrop, `"decoy"."` + CaptureFunctionDrop + `"`, "OUTSIDE the sluice schema", "DROP of a captured table"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := gradeCaptureShape("public", tc.installed, tc.ddl, tc.captureReplicated)
			if len(tc.wantErr) == 0 {
				if err != nil {
					t.Fatalf("want accept, got refusal: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want a refusal containing %q, got nil", tc.wantErr)
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal missing %q:\n%v", want, err)
				}
			}
		})
	}
}

// TestGradeCaptureShape_GradesTheTriggerWiring covers audit 2026-09-06
// S-2 layer 3: the door graded a trigger's IDENTITY (name, function,
// namespace, OID) and its SHAPE (tgtype, tgenabled) and nothing about
// its WIRING, so the two edits below passed every check.
//
// Both are quiet by construction. The trigger is present, enabled, bound
// to the right function with the right tgtype; only what reaches the
// function changes.
func TestGradeCaptureShape_GradesTheTriggerWiring(t *testing.T) {
	t.Run("a WHEN clause refuses", func(t *testing.T) {
		// The quietest edit available: rows the predicate excludes are
		// never captured, so they are simply missing from the target and
		// nothing else reports it. Setup renders no WHEN clause, so any
		// value here is an edit.
		trigs := healthyTriggers("t")
		trigs[0].whenClause = sql.NullString{String: "(new.tenant_id <> 42)", Valid: true}
		err := gradeCaptureShape("public", trigs, healthyTiers(), false)
		if err == nil {
			t.Fatal("a capture trigger carrying a WHEN clause was accepted; rows the predicate excludes " +
				"are silently never captured")
		}
		if !strings.Contains(err.Error(), "WHEN clause") || !strings.Contains(err.Error(), "tenant_id") {
			t.Errorf("refusal does not name the clause it found: %v", err)
		}
	})

	t.Run("a zero-arg row trigger refuses", func(t *testing.T) {
		// Setup passes the PK column list as the ROW trigger's one
		// argument and the capture function keys every change-log row
		// from it, so a zero-arg reinstall mis-keys everything.
		trigs := healthyTriggers("t")
		trigs[0].nargs = 0
		err := gradeCaptureShape("public", trigs, healthyTiers(), false)
		if err == nil {
			t.Fatal("a row capture trigger with no arguments was accepted; the capture function keys " +
				"every change-log row from that argument")
		}
		if !strings.Contains(err.Error(), "NO arguments") {
			t.Errorf("refusal does not name the missing argument: %v", err)
		}
	})

	t.Run("the TRUNCATE trigger takes no arguments and must not be refused", func(t *testing.T) {
		// The no-false-fire floor for the cell above: setup renders the
		// truncate trigger with an empty arg list, so the nargs check is
		// deliberately scoped to the ROW trigger. Without this, the fix
		// would refuse every healthy install.
		trigs := healthyTriggers("t")
		trigs[1].nargs = 0
		if err := gradeCaptureShape("public", trigs, healthyTiers(), false); err != nil {
			t.Fatalf("a healthy install was refused because its TRUNCATE trigger takes no arguments, "+
				"which is exactly how setup renders it: %v", err)
		}
	})
}

// TestGradeCaptureShape_GradesThePKArgumentValue closes the residual S-2
// layer 3 left open: the door checked that the ROW trigger HAD an
// argument and never what it said.
//
// WHY IT NEEDED NO SCHEMA BUMP. The layer-3 note said closing this
// wanted a triggerdef digest stamped into the install meta at setup — a
// version bump and a new column. It does not. The table's own PRIMARY
// KEY is independent evidence of what that argument should say, sitting
// in a catalog the door already reads, and it is strictly better than a
// recorded expectation: it also catches the case where the PK CHANGED
// after setup, which a recorded digest would have called healthy.
//
// The harm either way is that the capture function keys every
// change-log row for the table from that argument, so the applier
// matches the wrong target row.
func TestGradeCaptureShape_GradesThePKArgumentValue(t *testing.T) {
	// withPK builds the fixture the way POSTGRESQL does, not the way the
	// parser wishes it would.
	//
	// This helper is the fix for a HIGH that shipped inert (v0.146.0 pre-tag,
	// found by the value-fidelity review). The door read
	// `encode(tgargs,'escape')` and trimmed a Go NUL byte, and the fixture
	// handed it a Go NUL byte — so the test proved the parser agreed with the
	// test's own idea of the input, which was never in doubt. Measured on real
	// PostgreSQL 16, `escape` renders the terminator as the four literal
	// characters `\000` (a healthy `'["id"]'` comes back as `["id"]\000`, 10
	// chars over 7 raw bytes), so the trim removed nothing, the JSON parse
	// failed, and the ENTIRE primary-key grading was skipped on every real
	// install. The query now uses base64, and this helper encodes exactly what
	// the server returns: the argument bytes, NUL-terminated, base64'd.
	//
	// Every cell below therefore exercises a server-shaped value. Do not add a
	// cell that hand-writes a base64 string unless it came off a server.
	pgTgArgs := func(args string) string {
		return base64.StdEncoding.EncodeToString(append([]byte(args), 0))
	}
	withPK := func(args, livePK string) []installedCaptureTrigger {
		trigs := healthyTriggers("t")
		trigs[0].args = pgTgArgs(args)
		trigs[0].livePK = livePK
		return trigs
	}
	// withRawPK bypasses the encoder for the cells that are ABOUT malformed
	// server output rather than about a wrong key.
	withRawPK := func(rawArgs, livePK string) []installedCaptureTrigger {
		trigs := healthyTriggers("t")
		trigs[0].args = rawArgs
		trigs[0].livePK = livePK
		return trigs
	}

	t.Run("an argument naming the wrong column refuses", func(t *testing.T) {
		err := gradeCaptureShape("public", withPK(`["tenant_id"]`, `["id"]`), healthyTiers(), false)
		if err == nil {
			t.Fatal("a capture trigger keyed on a column that is not the table's primary key was " +
				"accepted; every captured change for that table carries the wrong key and the applier " +
				"matches the wrong target row")
		}
		for _, want := range []string{"tenant_id", "id", "primary key"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal missing %q; got: %v", want, err)
			}
		}
	})

	t.Run("a PK changed after setup refuses too", func(t *testing.T) {
		// The shape a recorded digest could NOT have caught: the trigger
		// is exactly as setup installed it, and the TABLE moved.
		if err := gradeCaptureShape("public", withPK(`["id"]`, `["id","tenant_id"]`), healthyTiers(), false); err == nil {
			t.Fatal("a table whose PRIMARY KEY gained a column since setup was accepted; the trigger " +
				"still keys on the old list, so captured rows are keyed by a stale key")
		}
	})

	t.Run("the matching case passes, on real server-shaped bytes", func(t *testing.T) {
		if err := gradeCaptureShape("public", withPK(`["id"]`, `["id"]`), healthyTiers(), false); err != nil {
			t.Fatalf("a correctly-installed trigger was refused: %v", err)
		}
	})

	t.Run("VERBATIM server output parses — the anti-vacuity floor", func(t *testing.T) {
		// The cell that would have caught the inert door, and the reason it
		// hard-codes a literal: these two strings were COPIED OFF a real
		// PostgreSQL 16, not constructed by the same helper the other cells
		// use. If parsePKColumnList ever stops understanding what the server
		// actually sends, every other cell here keeps passing (they agree
		// with the encoder) and only this one fails.
		//
		//   CREATE TRIGGER tr ... EXECUTE FUNCTION f('["id"]');
		//   SELECT encode(tgargs,'base64') -> WyJpZCJdAA==
		//
		// The second is a NON-ASCII column name, the family `escape` also
		// mangled (it octal-escapes every byte >= 0x7F, so `café` came back
		// as ["caf\303\251"]\000 and failed the parse identically):
		//
		//   CREATE TRIGGER tru ... EXECUTE FUNCTION f('["café"]');
		//   SELECT encode(tgargs,'base64') -> WyJjYWbDqSJdAA==
		for _, tc := range []struct{ name, b64, livePK string }{
			{"ascii key", "WyJpZCJdAA==", `["id"]`},
			{"non-ASCII key", "WyJjYWbDqSJdAA==", `["café"]`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				// Assert the PARSE succeeded, not merely that grading passed
				// — a parse failure also "passes" by grading nothing, which
				// is exactly how the inert door looked green.
				cols, ok := parsePKColumnList(tc.b64)
				if !ok {
					t.Fatalf("verbatim PostgreSQL output %q did not parse. The door grades NOTHING in "+
						"this state, on every real install, healthy or tampered — which is how it "+
						"shipped inert the first time.", tc.b64)
				}
				if len(cols) != 1 {
					t.Fatalf("parsed %v, want one column", cols)
				}
				if err := gradeCaptureShape("public", withRawPK(tc.b64, tc.livePK), healthyTiers(), false); err != nil {
					t.Fatalf("a correctly-installed trigger was refused: %v", err)
				}
			})
		}
	})

	t.Run("server-shaped bytes still catch a WRONG key", func(t *testing.T) {
		// The pair to the floor above: parsing correctly is only useful if
		// the grade then fires. "WyJ0ZW5hbnRfaWQiXQA=" is '["tenant_id"]'
		// NUL-terminated and base64'd, the same shape the server sends.
		if err := gradeCaptureShape("public", withRawPK("WyJ0ZW5hbnRfaWQiXQA=", `["id"]`), healthyTiers(), false); err == nil {
			t.Fatal("a trigger keyed on the wrong column passed when its argument arrived in the real " +
				"server encoding — the parse works but the grade does not fire")
		}
	})

	t.Run("a composite key in a different ORDER passes", func(t *testing.T) {
		// The floor that keeps this from refusing correct installs. The
		// capture function builds pk_jsonb with jsonb_object_agg, which
		// is order-independent, so column order carries no meaning — and
		// json_agg over the index may well emit a different order than
		// the Go slice setup marshalled.
		if err := gradeCaptureShape("public", withPK(`["a","b"]`, `["b","a"]`), healthyTiers(), false); err != nil {
			t.Fatalf("a composite key listed in a different order was refused, but the capture function "+
				"aggregates by name and does not care about order: %v", err)
		}
	})

	t.Run("an unparseable argument grades nothing", func(t *testing.T) {
		// An argument this cannot read is a DIFFERENT finding from a
		// wrong one; reporting it as "wrong columns" would be a
		// confident error. The zero-arg case is caught by its own check.
		if err := gradeCaptureShape("public", withRawPK("not-json", `["id"]`), healthyTiers(), false); err != nil {
			t.Errorf("an unparseable argument was reported as a wrong key: %v", err)
		}
	})

	t.Run("a keyless table grades nothing", func(t *testing.T) {
		// live_pk is '[]' for a table with no primary key. Refusing here
		// would break every keyless-table install, which pgtrigger
		// supports.
		if err := gradeCaptureShape("public", withPK(`[]`, `[]`), healthyTiers(), false); err != nil {
			t.Errorf("a keyless table was refused: %v", err)
		}
	})
}
