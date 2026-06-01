// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import "testing"

// TestComputeConnectionBudget pins the pure budget formula across the
// family of limit shapes the connection-resilience note enumerates:
// unlimited role/db, tight global, role-capped, db-capped, and the
// refuse-when-<1 boundary. The math is I/O-free so the whole matrix is
// table-driven without a database.
func TestComputeConnectionBudget(t *testing.T) {
	const reserve = 4
	tests := []struct {
		name          string
		probe         connectionBudgetProbe
		wantAvailable int
		wantCopy      int
	}{
		{
			name: "unlimited role and db, roomy global",
			probe: connectionBudgetProbe{
				maxConnections: 100, reserved: 3, currentTotal: 10,
				rolConnLimit: unlimited, roleCurrent: 5,
				datConnLimit: unlimited, dbCurrent: 5,
			},
			// global = 100-3-10 = 87; role/db unlimited.
			wantAvailable: 87,
			wantCopy:      83,
		},
		{
			name: "tight global dominates",
			probe: connectionBudgetProbe{
				maxConnections: 20, reserved: 3, currentTotal: 8,
				rolConnLimit: unlimited, roleCurrent: 2,
				datConnLimit: unlimited, dbCurrent: 2,
			},
			// global = 20-3-8 = 9.
			wantAvailable: 9,
			wantCopy:      5,
		},
		{
			name: "role limit dominates",
			probe: connectionBudgetProbe{
				maxConnections: 100, reserved: 3, currentTotal: 10, // global = 87
				rolConnLimit: 12, roleCurrent: 4, // role = 8
				datConnLimit: unlimited, dbCurrent: 0,
			},
			wantAvailable: 8,
			wantCopy:      4,
		},
		{
			name: "database limit dominates",
			probe: connectionBudgetProbe{
				maxConnections: 100, reserved: 3, currentTotal: 10, // global = 87
				rolConnLimit: unlimited, roleCurrent: 0,
				datConnLimit: 10, dbCurrent: 4, // db = 6
			},
			wantAvailable: 6,
			wantCopy:      2,
		},
		{
			name: "all three present, min wins (role)",
			probe: connectionBudgetProbe{
				maxConnections: 50, reserved: 3, currentTotal: 10, // global = 37
				rolConnLimit: 6, roleCurrent: 4, // role = 2
				datConnLimit: 30, dbCurrent: 10, // db = 20
			},
			wantAvailable: 2,
			wantCopy:      -2,
		},
		{
			name: "exhausted: global leaves nothing after reserve",
			probe: connectionBudgetProbe{
				maxConnections: 20, reserved: 3, currentTotal: 18, // global = -1
				rolConnLimit: unlimited,
				datConnLimit: unlimited,
			},
			wantAvailable: -1,
			wantCopy:      -5,
		},
		{
			name: "boundary: copy budget exactly 1",
			probe: connectionBudgetProbe{
				maxConnections: 20, reserved: 3, currentTotal: 12, // global = 5
				rolConnLimit: unlimited,
				datConnLimit: unlimited,
			},
			wantAvailable: 5,
			wantCopy:      1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := computeConnectionBudget(tc.probe, reserve)
			if got.Available != tc.wantAvailable {
				t.Errorf("Available = %d, want %d", got.Available, tc.wantAvailable)
			}
			if got.CopyBudget != tc.wantCopy {
				t.Errorf("CopyBudget = %d, want %d", got.CopyBudget, tc.wantCopy)
			}
		})
	}
}

// TestClampParallelism pins the one-directional clamp: it only ever
// reduces the requested value, never raises it, and floors at 1.
func TestClampParallelism(t *testing.T) {
	tests := []struct {
		name          string
		requested     int
		copyBudget    int
		wantEffective int
		wantCapped    bool
	}{
		{name: "within budget, no cap", requested: 4, copyBudget: 10, wantEffective: 4, wantCapped: false},
		{name: "over budget, capped down", requested: 8, copyBudget: 3, wantEffective: 3, wantCapped: true},
		{name: "exactly at budget", requested: 5, copyBudget: 5, wantEffective: 5, wantCapped: false},
		{name: "requested below 1 floors to 1", requested: 0, copyBudget: 10, wantEffective: 1, wantCapped: false},
		{name: "budget of 1 caps a wide request", requested: 8, copyBudget: 1, wantEffective: 1, wantCapped: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			eff, capped := clampParallelism(tc.requested, tc.copyBudget)
			if eff != tc.wantEffective {
				t.Errorf("effective = %d, want %d", eff, tc.wantEffective)
			}
			if capped != tc.wantCapped {
				t.Errorf("capped = %v, want %v", capped, tc.wantCapped)
			}
		})
	}
}

// TestConnLimitText pins the -1 → "unlimited" rendering used in the
// refusal message.
func TestConnLimitText(t *testing.T) {
	if got := connLimitText(unlimited); got != "unlimited" {
		t.Errorf("connLimitText(-1) = %q, want %q", got, "unlimited")
	}
	if got := connLimitText(12); got != "12" {
		t.Errorf("connLimitText(12) = %q, want %q", got, "12")
	}
}
