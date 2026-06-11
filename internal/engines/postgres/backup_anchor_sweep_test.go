// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for backup_anchor_sweep.go — the name-shape classifier
// the resume-time orphan sweep gates every drop on. Live-server
// coverage (real slots, real drops) lives in
// backup_anchor_slot_integration_test.go.

package postgres

import (
	"testing"
	"time"
)

// TestBackupAnchorTimestamp pins the classifier the sweep's "is this
// provably a sluice anchor?" decision rests on. Anything the parser
// rejects is never dropped, so the rejection cases are as load-bearing
// as the accepting ones.
func TestBackupAnchorTimestamp(t *testing.T) {
	cases := []struct {
		name     string
		slot     string
		wantOK   bool
		wantTime time.Time
	}{
		{
			name:     "canonical anchor name parses to its embedded instant",
			slot:     "sluice_backup_anchor_123",
			wantOK:   true,
			wantTime: time.Unix(0, 123),
		},
		{
			name:     "realistic unix-nano timestamp",
			slot:     "sluice_backup_anchor_1781171588306531000",
			wantOK:   true,
			wantTime: time.Unix(0, 1781171588306531000),
		},
		{
			name:   "bare prefix without a timestamp is rejected",
			slot:   "sluice_backup_anchor_",
			wantOK: false,
		},
		{
			name:   "non-numeric suffix is rejected",
			slot:   "sluice_backup_anchor_bogus",
			wantOK: false,
		},
		{
			name:   "trailing garbage after the digits is rejected",
			slot:   "sluice_backup_anchor_123x",
			wantOK: false,
		},
		{
			name:   "negative timestamp is rejected",
			slot:   "sluice_backup_anchor_-5",
			wantOK: false,
		},
		{
			name:   "unrelated slot name is rejected",
			slot:   "sluice_slot",
			wantOK: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := backupAnchorTimestamp(c.slot)
			if ok != c.wantOK {
				t.Fatalf("backupAnchorTimestamp(%q) ok = %v; want %v", c.slot, ok, c.wantOK)
			}
			if ok && !got.Equal(c.wantTime) {
				t.Errorf("backupAnchorTimestamp(%q) = %v; want %v", c.slot, got, c.wantTime)
			}
		})
	}
}
