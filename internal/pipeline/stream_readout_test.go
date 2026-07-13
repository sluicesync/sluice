// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/progress"
)

// TestPushStreamReadout pins the ADR-0156 backup-stream readout shape:
// lifetime incrementals rolled, current chain position (backup id, falling
// back to the EndPosition token), rollover cadence, and the poll instant. A
// nil hook is a no-op; a nil parent renders "—".
func TestPushStreamReadout(t *testing.T) {
	now := time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC)

	// nil Readout: must not panic.
	(&BackupStream{}).pushStreamReadout(&irbackup.Manifest{BackupID: "incr-1"}, 1, time.Minute, now)

	var got []progress.Field
	b := &BackupStream{Readout: func(f []progress.Field) { got = f }}

	// BackupID wins for the position label.
	b.pushStreamReadout(&irbackup.Manifest{BackupID: "incr-9"}, 4, 5*time.Minute, now)
	assertFields(t, got, map[string]string{
		"incrementals": "4",
		"position":     "incr-9",
		"cadence":      "5m0s",
		"last poll":    "2026-07-13T00:00:00Z",
	})

	// No BackupID → fall back to the EndPosition token.
	b.pushStreamReadout(&irbackup.Manifest{EndPosition: ir.Position{Token: "0/16B7400"}}, 0, time.Minute, now)
	if got[1].Label != "position" || got[1].Value != "0/16B7400" {
		t.Errorf("position should fall back to the EndPosition token, got %+v", got[1])
	}

	// nil parent → em-dash.
	b.pushStreamReadout(nil, 0, time.Minute, now)
	if got[1].Value != "—" {
		t.Errorf("nil parent should render em-dash position, got %q", got[1].Value)
	}
}
