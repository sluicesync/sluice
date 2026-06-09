// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import "testing"

// TestSplitCopyAndIndexBudget pins the ADR-0077 combined copy+index budget
// split. The load-bearing invariant is indexBudget + copyBudget' <=
// copyBudget — the sum of simultaneously-open copy + index connections
// never exceeds the measured budget. The matrix covers the contract
// corners: no measured ceiling (→ no split), small budgets where the
// within floor protects copy and trims the index slice, and large budgets
// where the index slice is clamped to the [1, 8] ceiling.
func TestSplitCopyAndIndexBudget(t *testing.T) {
	tests := []struct {
		name              string
		copyBudget        int
		withinParallelism int
		wantIndex         int
		wantCopy          int
	}{
		{
			name:       "no measured ceiling: don't split (MySQL / degraded)",
			copyBudget: 0, withinParallelism: 8,
			wantIndex: 0, wantCopy: 0,
		},
		{
			name:       "negative budget: don't split",
			copyBudget: -5, withinParallelism: 4,
			wantIndex: 0, wantCopy: 0,
		},
		{
			name:       "ample budget: index gets 25% clamped, copy the rest",
			copyBudget: 32, withinParallelism: 4,
			wantIndex: 8, wantCopy: 24, // round(0.25*32)=8 (==ceiling); 32-8=24
		},
		{
			name:       "index slice clamped to ceiling 8 on a huge budget",
			copyBudget: 100, withinParallelism: 4,
			wantIndex: 8, wantCopy: 92, // round(25)=25 → clamp 8; 100-8=92
		},
		{
			name:       "small budget: 25% rounds to at least 1",
			copyBudget: 8, withinParallelism: 2,
			wantIndex: 2, wantCopy: 6, // index rounds to 2, copy keeps the rest
		},
		{
			name:       "budget 4 within 2: index 1, copy 3",
			copyBudget: 4, withinParallelism: 2,
			wantIndex: 1, wantCopy: 3, // index rounds to 1, copy keeps the rest
		},
		{
			name:       "within floor protects copy, trims index slice",
			copyBudget: 10, withinParallelism: 8,
			// round(2.5)=3 index; copy' = max(10-3, 8) = 8; invariant
			// 3+8=11 > 10 → trim index to 10-8 = 2.
			wantIndex: 2, wantCopy: 8,
		},
		{
			name:       "within == budget: no slot to spare → no overlap",
			copyBudget: 6, withinParallelism: 6,
			// round(1.5)=2; copy'=max(6-2,6)=6; 2+6=8>6 → trim index=6-6=0
			// → can't spare → return (0, copyBudget).
			wantIndex: 0, wantCopy: 6,
		},
		{
			name:       "budget 1: index can't be carved → no overlap",
			copyBudget: 1, withinParallelism: 1,
			// round(0.25)=0 → floor 1; copy'=max(1-1,1)=1; 1+1=2>1 →
			// trim index=1-1=0 → no overlap → (0, 1).
			wantIndex: 0, wantCopy: 1,
		},
		{
			name:       "within < 1 clamps to 1",
			copyBudget: 8, withinParallelism: 0,
			wantIndex: 2, wantCopy: 6, // within floored to 1; round(2)=2; 8-2=6
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			gotIndex, gotCopy := splitCopyAndIndexBudget(tt.copyBudget, tt.withinParallelism)
			if gotIndex != tt.wantIndex || gotCopy != tt.wantCopy {
				t.Errorf("splitCopyAndIndexBudget(budget=%d, within=%d) = (%d,%d); want (%d,%d)",
					tt.copyBudget, tt.withinParallelism, gotIndex, gotCopy, tt.wantIndex, tt.wantCopy)
			}
		})
	}
}

// TestSplitCopyAndIndexBudget_Invariant sweeps a matrix and asserts the
// load-bearing invariant on EVERY cell with a measured budget:
//
//	indexBudget + copyBudget' <= copyBudget
//
// plus the structural guarantees: index slice in [0, 8], copy slice never
// below the within floor when overlap engages, and overlap only declines
// (index==0) when there is genuinely no slot to spare.
func TestSplitCopyAndIndexBudget_Invariant(t *testing.T) {
	for budget := 0; budget <= 64; budget++ {
		for within := 0; within <= 16; within++ {
			idx, cpy := splitCopyAndIndexBudget(budget, within)

			if budget < 1 {
				if idx != 0 || cpy != 0 {
					t.Fatalf("no-ceiling must return (0,0): budget=%d within=%d → (%d,%d)", budget, within, idx, cpy)
				}
				continue
			}

			// THE invariant: simultaneous copy + index connections fit.
			if idx+cpy > budget {
				t.Fatalf("invariant violated: budget=%d within=%d → index=%d copy=%d sum=%d > budget",
					budget, within, idx, cpy, idx+cpy)
			}
			// Index slice bounds.
			if idx < 0 || idx > indexBudgetCeiling {
				t.Fatalf("index slice out of [0,%d]: budget=%d within=%d → index=%d", indexBudgetCeiling, budget, within, idx)
			}
			// When overlap engages (idx>0), copy keeps at least the within
			// floor (a single table's worth of connections).
			if idx > 0 {
				wantFloor := within
				if wantFloor < 1 {
					wantFloor = 1
				}
				if cpy < wantFloor {
					t.Fatalf("copy below within floor: budget=%d within=%d → copy=%d < floor=%d", budget, within, cpy, wantFloor)
				}
			}
			// When overlap declines, copy keeps the full budget.
			if idx == 0 && cpy != budget {
				t.Fatalf("declined overlap must hand copy the full budget: budget=%d within=%d → copy=%d", budget, within, cpy)
			}
		}
	}
}
