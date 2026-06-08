// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"
	"testing"
)

// TestResolveCopyParallelismBudget pins the ADR-0076 static-split policy:
// within-table is satisfied first, the table axis gets whatever whole
// multiples of withinP fit the effective budget, and the PRODUCT
// (tableP × withinP) never exceeds min(copyBudget, ceiling) whenever that
// is >= 1. The matrix includes the load-bearing corners: copyBudget=1
// (→ 1×1), --max-target-connections (ceiling) bounding the product below
// the copy budget, and the budget-less target (copyBudget=0, no ceiling)
// where the operator's request stands unclamped.
func TestResolveCopyParallelismBudget(t *testing.T) {
	tests := []struct {
		name           string
		resolvedWithin int
		requestedTable int
		copyBudget     int
		ceiling        int
		wantTable      int
		wantWithin     int
	}{
		{
			name:           "ample budget honours both requests",
			resolvedWithin: 4, requestedTable: 4, copyBudget: 32, ceiling: 0,
			wantTable: 4, wantWithin: 4, // product 16 <= 32
		},
		{
			name:           "budget caps the table axis to whole multiples of within",
			resolvedWithin: 8, requestedTable: 4, copyBudget: 20, ceiling: 0,
			wantTable: 2, wantWithin: 8, // 20/8 = 2; product 16 <= 20
		},
		{
			name:           "copyBudget=1 collapses to 1x1",
			resolvedWithin: 1, requestedTable: 4, copyBudget: 1, ceiling: 0,
			wantTable: 1, wantWithin: 1,
		},
		{
			name:           "budget smaller than within floors table at 1",
			resolvedWithin: 8, requestedTable: 4, copyBudget: 4, ceiling: 0,
			// within was already clamped to copyBudget upstream in practice,
			// but defend the corner: 4/8 = 0 → floored to 1.
			wantTable: 1, wantWithin: 8,
		},
		{
			name:           "max-target-connections ceiling bounds the product below copyBudget",
			resolvedWithin: 4, requestedTable: 8, copyBudget: 64, ceiling: 12,
			wantTable: 3, wantWithin: 4, // min(64,12)=12; 12/4=3; product 12 <= 12
		},
		{
			name:           "ceiling tighter than budget and not a clean multiple",
			resolvedWithin: 4, requestedTable: 8, copyBudget: 64, ceiling: 10,
			wantTable: 2, wantWithin: 4, // 10/4 = 2 (floor); product 8 <= 10
		},
		{
			name:           "no measured budget, no ceiling: request stands (MySQL target)",
			resolvedWithin: 8, requestedTable: 6, copyBudget: 0, ceiling: 0,
			wantTable: 6, wantWithin: 8,
		},
		{
			name:           "no budget but explicit ceiling still bounds the product",
			resolvedWithin: 2, requestedTable: 9, copyBudget: 0, ceiling: 10,
			wantTable: 5, wantWithin: 2, // 10/2 = 5; product 10 <= 10
		},
		{
			name:           "table=1 disables cross-table regardless of budget",
			resolvedWithin: 4, requestedTable: 1, copyBudget: 64, ceiling: 0,
			wantTable: 1, wantWithin: 4,
		},
		{
			name:           "defensive: zero/negative inputs clamp to 1",
			resolvedWithin: 0, requestedTable: 0, copyBudget: 0, ceiling: 0,
			wantTable: 1, wantWithin: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			gotTable, gotWithin := resolveCopyParallelismBudget(
				tt.resolvedWithin, tt.requestedTable, tt.copyBudget, tt.ceiling,
			)
			if gotTable != tt.wantTable || gotWithin != tt.wantWithin {
				t.Errorf("resolveCopyParallelismBudget(within=%d,table=%d,budget=%d,ceiling=%d) = (%d,%d); want (%d,%d)",
					tt.resolvedWithin, tt.requestedTable, tt.copyBudget, tt.ceiling,
					gotTable, gotWithin, tt.wantTable, tt.wantWithin)
			}
			// Both factors must always be >= 1.
			if gotTable < 1 || gotWithin < 1 {
				t.Errorf("factors must be >= 1; got table=%d within=%d", gotTable, gotWithin)
			}
			// The load-bearing invariant: the product fits the effective
			// budget whenever a budget is in force AND within fits the
			// budget. The within factor is PINNED (the well-tuned axis is
			// never lowered by the split — that is resolveTargetCopyParallelism's
			// job, and it guarantees withinP <= copyBudget upstream); so in
			// the degenerate within > budget case the best the split can do
			// is floor the table axis at 1, which this guard excludes.
			effective := minNonZeroBudget(tt.copyBudget, tt.ceiling)
			if effective >= gotWithin && gotTable*gotWithin > effective {
				t.Errorf("product %d×%d=%d exceeds effective budget %d",
					gotTable, gotWithin, gotTable*gotWithin, effective)
			}
		})
	}
}

// TestResolveCopyParallelismBudget_ProductBoundExhaustive sweeps a matrix
// of inputs and asserts the product invariant holds for every cell — the
// gotcha-2 ceiling that, if violated, lets a wide schema exhaust the
// target's connection slots.
func TestResolveCopyParallelismBudget_ProductBoundExhaustive(t *testing.T) {
	for within := 1; within <= 8; within++ {
		for table := 1; table <= 8; table++ {
			for budget := 0; budget <= 32; budget++ {
				for _, ceiling := range []int{0, 4, 12, 24} {
					gotTable, gotWithin := resolveCopyParallelismBudget(within, table, budget, ceiling)
					effective := minNonZeroBudget(budget, ceiling)
					// Product bound holds whenever within fits the budget —
					// the contract resolveTargetCopyParallelism guarantees
					// upstream (withinP <= copyBudget). The degenerate
					// within > budget case can't arise in production; the
					// split floors table at 1 there.
					if effective >= gotWithin && gotTable*gotWithin > effective {
						t.Fatalf("product bound violated: within=%d table=%d budget=%d ceiling=%d → (%d,%d) product=%d > effective=%d",
							within, table, budget, ceiling, gotTable, gotWithin, gotTable*gotWithin, effective)
					}
					if gotTable < 1 || gotWithin < 1 {
						t.Fatalf("factor < 1: within=%d table=%d budget=%d ceiling=%d → (%d,%d)",
							within, table, budget, ceiling, gotTable, gotWithin)
					}
					// within is never lowered by the split (it's pinned).
					if gotWithin != maxInt(within, 1) {
						t.Fatalf("within mutated: in=%d out=%d (budget=%d ceiling=%d)", within, gotWithin, budget, ceiling)
					}
				}
			}
		}
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// TestResolveTableParallelism pins the 0=auto / 1=disable / explicit
// resolution.
func TestResolveTableParallelism(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{0, defaultTableParallelism},
		{1, 1},
		{4, 4},
		{16, 16},
		{-3, 1},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("in=%d", c.in), func(t *testing.T) {
			if got := resolveTableParallelism(c.in); got != c.want {
				t.Errorf("resolveTableParallelism(%d) = %d; want %d", c.in, got, c.want)
			}
		})
	}
}
