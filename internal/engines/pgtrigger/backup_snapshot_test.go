// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// TestOpenBackupSnapshot_RefusesChainSlotBeforeConnecting pins the
// `--chain-slot` refusal (roadmap item 163): there is no slot to persist
// on a trigger source, and the refusal fires before any connection is
// attempted — the DSN below points nowhere.
func TestOpenBackupSnapshot_RefusesChainSlotBeforeConnecting(t *testing.T) {
	_, err := (Engine{}).OpenBackupSnapshot(context.Background(),
		"postgres://nobody:nothing@127.0.0.1:1/nowhere?sslmode=disable",
		irbackup.SnapshotOptions{PersistChainSlot: true})
	if err == nil || !strings.Contains(err.Error(), "--chain-slot") {
		t.Fatalf("OpenBackupSnapshot with PersistChainSlot = %v; want a refusal naming --chain-slot", err)
	}
}

// TestPreflightPositionFromManifest_GradesTheTokenFamily pins the
// re-pointed manifest preflight on the trigger reader: a trigger-CDC
// terminal position passes with no slot inspection (the composed reader's
// preflight would refuse a missing `sluice_slot`), and a pgoutput
// {slot, lsn} from a vanilla-postgres chain is refused before the stream
// opens.
func TestPreflightPositionFromManifest_GradesTheTokenFamily(t *testing.T) {
	r := &SchemaReader{} // the method touches neither the embedded reader nor a connection
	for _, tc := range []struct {
		name       string
		terminal   ir.Position
		wantRefuse bool
	}{
		{"trigger position passes", ir.Position{Engine: EngineName, Token: `{"last_id":42}`}, false},
		{"empty (from now) passes", ir.Position{}, false},
		{"pgoutput position is refused", ir.Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/100"}`}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report, err := r.PreflightPositionFromManifest(context.Background(), tc.terminal, "sluice_slot")
			if err != nil {
				t.Fatalf("PreflightPositionFromManifest: %v", err)
			}
			if got := report.Refusal != ""; got != tc.wantRefuse {
				t.Errorf("refusal = %q; want refuse=%v", report.Refusal, tc.wantRefuse)
			}
			if len(report.Warnings) != 0 {
				t.Errorf("warnings = %v; want none (no slot to warn about)", report.Warnings)
			}
		})
	}
}
