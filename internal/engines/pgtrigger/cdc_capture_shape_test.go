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
	"strings"
	"testing"
)

// healthyTriggers returns a correctly-installed plain-posture pair for
// one table.
func healthyTriggers(table string) []installedCaptureTrigger {
	return []installedCaptureTrigger{
		{table: table, name: CaptureTriggerRow, enabled: "O", fn: CaptureFunctionRow, tgtype: expectedRowTgType},
		{table: table, name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, tgtype: expectedTruncateTgType},
	}
}

// alwaysTriggers returns a correctly-installed --capture-replicated-writes
// (ENABLE ALWAYS) pair for one table.
func alwaysTriggers(table string) []installedCaptureTrigger {
	return []installedCaptureTrigger{
		{table: table, name: CaptureTriggerRow, enabled: "A", fn: CaptureFunctionRow, tgtype: expectedRowTgType},
		{table: table, name: CaptureTriggerTruncate, enabled: "A", fn: CaptureFunctionTruncate, tgtype: expectedTruncateTgType},
	}
}

// healthyEvt / healthyDropEvt are the two event-trigger arms' states in a
// full (event-trigger-tier) install.
var (
	healthyEvt     = eventTriggerState{present: true, enabled: "O", fn: CaptureFunctionDDL}
	healthyDropEvt = eventTriggerState{present: true, enabled: "O", fn: CaptureFunctionDrop}
	// The ENABLE ALWAYS ('A') shapes the --capture-replicated-writes opt-in
	// installs (ADR-0185 + audit A-1).
	alwaysEvt     = eventTriggerState{present: true, enabled: "A", fn: CaptureFunctionDDL}
	alwaysDropEvt = eventTriggerState{present: true, enabled: "A", fn: CaptureFunctionDrop}
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

// preDropArmTiers is an install made before v0.135: the ddl_command_end arm
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
				{table: "t", name: CaptureTriggerRow, enabled: "O", fn: CaptureFunctionRow, tgtype: expectedRowTgType},
				{table: "t", name: CaptureTriggerTruncate, enabled: "A", fn: CaptureFunctionTruncate, tgtype: expectedTruncateTgType},
			},
			ddl:               tiers(alwaysEvt, alwaysDropEvt),
			captureReplicated: true,
			wantErr:           []string{CaptureTriggerRow, "--capture-replicated-writes", "NOT being captured"},
		},
		{
			name: "plain truncate trigger under the opt-in posture refuses too (both members graded)",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerRow, enabled: "A", fn: CaptureFunctionRow, tgtype: expectedRowTgType},
				{table: "t", name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, tgtype: expectedTruncateTgType},
			},
			ddl:               tiers(alwaysEvt, alwaysDropEvt),
			captureReplicated: true,
			wantErr:           []string{CaptureTriggerTruncate, "--capture-replicated-writes", "TRUNCATE"},
		},
		{
			name: "disabled trigger still refuses under the opt-in posture",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerRow, enabled: "D", fn: CaptureFunctionRow, tgtype: expectedRowTgType},
				{table: "t", name: CaptureTriggerTruncate, enabled: "A", fn: CaptureFunctionTruncate, tgtype: expectedTruncateTgType},
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
			ddl:       tiers(eventTriggerState{present: true, enabled: "A", fn: CaptureFunctionDDL}, healthyDropEvt),
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
			ddl:       tiers(healthyEvt, eventTriggerState{present: true, enabled: "D", fn: CaptureFunctionDrop}),
			wantErr:   []string{CaptureTriggerDrop, "not enabled", "DROP of a captured table"},
		},
		{
			name:      "drop event trigger bound to a foreign function refuses",
			installed: healthyTriggers("t"),
			ddl:       tiers(healthyEvt, eventTriggerState{present: true, enabled: "O", fn: "somebody_elses_drop_hook"}),
			wantErr:   []string{CaptureTriggerDrop, "somebody_elses_drop_hook"},
		},
		{
			name:    "zero triggers anywhere refuses (the dropped-everything floor)",
			wantErr: []string{"NO capture trigger", "trigger setup"},
		},
		{
			name: "missing row trigger refuses naming it",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, tgtype: expectedTruncateTgType},
			},
			wantErr: []string{`"t"`, CaptureTriggerRow, "MISSING", "trigger setup"},
		},
		{
			name: "missing truncate trigger refuses naming it",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerRow, enabled: "O", fn: CaptureFunctionRow, tgtype: expectedRowTgType},
			},
			wantErr: []string{CaptureTriggerTruncate, "MISSING", "TRUNCATE"},
		},
		{
			name: "disabled trigger refuses",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerRow, enabled: "D", fn: CaptureFunctionRow, tgtype: expectedRowTgType},
				{table: "t", name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, tgtype: expectedTruncateTgType},
			},
			wantErr: []string{"DISABLED", "DISABLE TRIGGER"},
		},
		{
			name: "ENABLE REPLICA trigger refuses (fires for no origin write)",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerRow, enabled: "R", fn: CaptureFunctionRow, tgtype: expectedRowTgType},
				{table: "t", name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, tgtype: expectedTruncateTgType},
			},
			wantErr: []string{"ENABLE REPLICA", "session_replication_role"},
		},
		{
			name: "foreign bound function refuses",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerRow, enabled: "O", fn: "audit_everything", tgtype: expectedRowTgType},
				{table: "t", name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, tgtype: expectedTruncateTgType},
			},
			wantErr: []string{"audit_everything", CaptureFunctionRow, "not what this sluice installs"},
		},
		{
			name: "wrong trigger shape (tgtype) refuses",
			installed: []installedCaptureTrigger{
				// BEFORE instead of AFTER: tgtype carries the BEFORE bit.
				{table: "t", name: CaptureTriggerRow, enabled: "O", fn: CaptureFunctionRow, tgtype: expectedRowTgType | 1<<1},
				{table: "t", name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, tgtype: expectedTruncateTgType},
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
			ddl:       tiers(eventTriggerState{present: true, enabled: "D", fn: CaptureFunctionDDL}, healthyDropEvt),
			wantErr:   []string{CaptureTriggerDDL, "not enabled"},
		},
		{
			name:      "event trigger bound to a foreign function refuses",
			installed: healthyTriggers("t"),
			ddl:       tiers(eventTriggerState{present: true, enabled: "O", fn: "somebody_elses_ddl_hook"}, healthyDropEvt),
			wantErr:   []string{CaptureTriggerDDL, "somebody_elses_ddl_hook"},
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
