// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/progress"
)

// TestPushBrokerReadout pins the ADR-0156 broker readout shape: last-applied
// chain position, cumulative incrementals + chunks, and the poll instant. A
// nil hook is a no-op (the non-TTY path), and an empty position renders "—".
func TestPushBrokerReadout(t *testing.T) {
	now := time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC)

	// nil Readout: must not panic.
	(&SyncFromBackup{}).pushBrokerReadout("incr-1", 1, 1, now)

	var got []progress.Field
	b := &SyncFromBackup{Readout: func(f []progress.Field) { got = f }}
	b.pushBrokerReadout("incr-9", 3, 5, now)

	want := map[string]string{
		"position":     "incr-9",
		"incrementals": "3",
		"chunks":       "5",
		"last poll":    "2026-07-13T00:00:00Z",
	}
	assertFields(t, got, want)

	// Empty position (no incremental applied yet) renders the honest em-dash.
	b.pushBrokerReadout("", 0, 0, now)
	if got[0].Label != "position" || got[0].Value != "—" {
		t.Errorf("empty position should render em-dash, got %+v", got[0])
	}
}

// assertFields checks an ordered readout field list against an expected
// label→value map (exact length + values).
func assertFields(t *testing.T, got []progress.Field, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("field count = %d, want %d: %+v", len(got), len(want), got)
	}
	for _, f := range got {
		if w, ok := want[f.Label]; !ok || w != f.Value {
			t.Errorf("field %q = %q, want %q", f.Label, f.Value, w)
		}
	}
}
