// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import "testing"

// TestComputePruneCut pins the prune-bound math: cut = appliedLastID - keep, and
// a non-positive cut is a no-op (prune=false). This is the load-bearing
// computation — getting it wrong either over-prunes (silent loss) or never
// prunes.
func TestComputePruneCut(t *testing.T) {
	cases := []struct {
		name          string
		appliedLastID int64
		keep          int64
		wantCut       int64
		wantPrune     bool
	}{
		{"frontier well above margin", 10_000, 1000, 9000, true},
		{"keep zero prunes through frontier", 500, 0, 500, true},
		{"cut exactly zero is a no-op", 1000, 1000, 0, false},
		{"cut negative is a no-op", 500, 1000, -500, false},
		{"nothing applied yet", 0, 1000, -1000, false},
		{"small margin", 3, 2, 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cut, prune := computePruneCut(tc.appliedLastID, tc.keep)
			if cut != tc.wantCut || prune != tc.wantPrune {
				t.Errorf("computePruneCut(%d, %d) = (%d, %v); want (%d, %v)",
					tc.appliedLastID, tc.keep, cut, prune, tc.wantCut, tc.wantPrune)
			}
		})
	}
}

// TestDecodeAppliedLastID covers the source-driver-dispatched token decode for
// all three trigger engines plus the refuse-on-garbled-token path.
func TestDecodeAppliedLastID(t *testing.T) {
	for _, driver := range []string{triggerDriverSQLite, triggerDriverD1, triggerDriverPostgres} {
		got, err := decodeAppliedLastID(driver, `{"last_id":7}`)
		if err != nil {
			t.Errorf("decodeAppliedLastID(%q, valid) error: %v", driver, err)
			continue
		}
		if got != 7 {
			t.Errorf("decodeAppliedLastID(%q) = %d; want 7", driver, got)
		}
		// An empty/garbled token must refuse loudly — never prune blind.
		if _, err := decodeAppliedLastID(driver, ""); err == nil {
			t.Errorf("decodeAppliedLastID(%q, empty) returned nil; want a loud error", driver)
		}
	}

	if _, err := decodeAppliedLastID("mystery-engine", `{"last_id":7}`); err == nil {
		t.Error("decodeAppliedLastID(unknown driver) returned nil; want an error")
	}
}
