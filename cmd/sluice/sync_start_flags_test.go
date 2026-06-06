// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
)

// TestSyncStartCmd_validateFlagCombos pins the mutually-exclusive flag
// validation for `sync start`. The regression motivating the dedicated test:
// the --reset-target-data destructive confirmation prompt used to fire BEFORE
// these checks, so combining --restart-from-scratch with --reset-target-data
// asked the operator to confirm a target-table DROP and only THEN reported the
// flags were mutually exclusive. validateFlagCombos is pure and is now called
// ahead of the prompt; this test asserts each rejected combination fails loud
// and each valid combination passes — independent of any prompt/I/O.
func TestSyncStartCmd_validateFlagCombos(t *testing.T) {
	cases := []struct {
		name        string
		cmd         SyncStartCmd
		wantErr     bool
		wantSubstrs []string // all must appear in the error message
	}{
		{
			name:        "restart-from-scratch + reset-target-data → rejected",
			cmd:         SyncStartCmd{RestartFromScratch: true, ResetTargetData: true},
			wantErr:     true,
			wantSubstrs: []string{"--restart-from-scratch", "--reset-target-data", "mutually exclusive"},
		},
		{
			name:        "restart-from-scratch + position-from-manifest → rejected",
			cmd:         SyncStartCmd{RestartFromScratch: true, PositionFromManifest: "m.json"},
			wantErr:     true,
			wantSubstrs: []string{"--restart-from-scratch", "--position-from-manifest", "mutually exclusive"},
		},
		{
			name:        "position-from-manifest + reset-target-data → rejected",
			cmd:         SyncStartCmd{PositionFromManifest: "m.json", ResetTargetData: true},
			wantErr:     true,
			wantSubstrs: []string{"--position-from-manifest", "--reset-target-data", "mutually exclusive"},
		},
		{
			name:    "restart-from-scratch alone → ok",
			cmd:     SyncStartCmd{RestartFromScratch: true},
			wantErr: false,
		},
		{
			name:    "reset-target-data alone → ok",
			cmd:     SyncStartCmd{ResetTargetData: true},
			wantErr: false,
		},
		{
			name:    "position-from-manifest alone → ok",
			cmd:     SyncStartCmd{PositionFromManifest: "m.json"},
			wantErr: false,
		},
		{
			name:    "no recovery flags → ok",
			cmd:     SyncStartCmd{},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cmd.validateFlagCombos()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("validateFlagCombos() = nil; want error")
				}
				for _, sub := range tc.wantSubstrs {
					if !strings.Contains(err.Error(), sub) {
						t.Errorf("error %q missing substring %q", err.Error(), sub)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("validateFlagCombos() = %v; want nil", err)
			}
		})
	}
}
