// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the capture-shape door's grading half
// ([gradeCaptureShape], audit 2026-08-26 F2). Every defect class the door
// names gets a cell, plus the two accept cells (healthy install; the
// polled-fingerprint exemption) so the door cannot drift into
// false-refusing a legitimate source. The catalog-reading half and the
// end-to-end refusals run against real PG in
// cdc_capture_shape_integration_test.go.

package pgtrigger

import (
	"strings"
	"testing"
)

// healthyTriggers returns a correctly-installed pair for one table.
func healthyTriggers(table string) []installedCaptureTrigger {
	return []installedCaptureTrigger{
		{table: table, name: CaptureTriggerRow, enabled: "O", fn: CaptureFunctionRow, tgtype: expectedRowTgType},
		{table: table, name: CaptureTriggerTruncate, enabled: "O", fn: CaptureFunctionTruncate, tgtype: expectedTruncateTgType},
	}
}

// healthyEvt is the event-trigger state a full (event-trigger-tier) install
// leaves behind.
var healthyEvt = eventTriggerState{present: true, enabled: "O", fn: CaptureFunctionDDL}

func TestGradeCaptureShape(t *testing.T) {
	cases := []struct {
		name         string
		installed    []installedCaptureTrigger
		ddlFnPresent bool
		evt          eventTriggerState
		wantErr      []string // all must appear; empty = accept
	}{
		{
			name:         "healthy full install accepts",
			installed:    healthyTriggers("t"),
			ddlFnPresent: true,
			evt:          healthyEvt,
		},
		{
			name:      "polled-fingerprint install (no DDL function, no event trigger) accepts",
			installed: healthyTriggers("t"),
			// ddlFnPresent=false, evt absent: no event trigger was ever
			// expected — requiring one would false-refuse every
			// --allow-polled-fingerprint source.
		},
		{
			name: "ENABLE ALWAYS accepts (strictly-more capture)",
			installed: []installedCaptureTrigger{
				{table: "t", name: CaptureTriggerRow, enabled: "A", fn: CaptureFunctionRow, tgtype: expectedRowTgType},
				{table: "t", name: CaptureTriggerTruncate, enabled: "A", fn: CaptureFunctionTruncate, tgtype: expectedTruncateTgType},
			},
			ddlFnPresent: true,
			evt:          healthyEvt,
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
			name:         "DDL function present but event trigger dropped refuses",
			installed:    healthyTriggers("t"),
			ddlFnPresent: true,
			wantErr:      []string{CaptureTriggerDDL, "MISSING", "DROP EVENT TRIGGER"},
		},
		{
			name:         "disabled event trigger refuses",
			installed:    healthyTriggers("t"),
			ddlFnPresent: true,
			evt:          eventTriggerState{present: true, enabled: "D", fn: CaptureFunctionDDL},
			wantErr:      []string{CaptureTriggerDDL, "not enabled"},
		},
		{
			name:         "event trigger bound to a foreign function refuses",
			installed:    healthyTriggers("t"),
			ddlFnPresent: true,
			evt:          eventTriggerState{present: true, enabled: "O", fn: "somebody_elses_ddl_hook"},
			wantErr:      []string{CaptureTriggerDDL, "somebody_elses_ddl_hook"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := gradeCaptureShape("public", tc.installed, tc.ddlFnPresent, tc.evt)
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
