// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"testing"
	"time"
)

// TestPgTriggerCommitTime pins the units conversion for the sync-lag metric
// (roadmap item 45): the change-log committed_at is projected as UTC
// seconds-since-epoch, and a non-positive value (NULL/0) maps to the zero
// time (unknown), never to the epoch.
func TestPgTriggerCommitTime(t *testing.T) {
	const epoch int64 = 1_750_000_000
	got := pgTriggerCommitTime(epoch)
	if want := time.Unix(epoch, 0).UTC(); !got.Equal(want) {
		t.Errorf("pgTriggerCommitTime = %v; want %v", got, want)
	}
	if got.Location() != time.UTC {
		t.Errorf("pgTriggerCommitTime location = %v; want UTC", got.Location())
	}
	if !pgTriggerCommitTime(0).IsZero() {
		t.Error("zero epoch did not map to the zero time")
	}
	if !pgTriggerCommitTime(-1).IsZero() {
		t.Error("negative epoch did not map to the zero time")
	}
}
