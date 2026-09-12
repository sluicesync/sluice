// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestResolveCopyAxes_ProductRespectsCopyConcurrencyCeiling pins the bound a
// real defect walked straight through.
//
// THE DEFECT, because the shape is what matters rather than the arithmetic:
// a target that refuses more than N concurrent COPY statements had its limit
// applied to the WITHIN-table axis upstream, while the product bound was
// still derived from the uncapped CONNECTION budget. The table axis then
// multiplied the capped within value back up — a limit of 4 produced 4x4.
//
// It was invisible for as long as the connection budget was the tighter of
// the two (a small max_connections left room for only one table worker), and
// surfaced the moment the operator raised max_connections: a live PlanetScale
// Neki router refused the copy with SQLSTATE 53300 roughly forty seconds in.
//
// WHY THIS TEST EXISTS RATHER THAN THE ONE THAT WAS ALREADY THERE. The Neki
// cap did have a unit test. That test pinned the pigeonhole argument behind
// the CONSTANT — that N concurrent COPYs are admissible iff N <= the limit —
// and its own header said, accurately, that it tested the argument and not
// the wiring, leaving the wiring to a suite that needs a live router. The
// argument was never wrong. The wiring was. A gate that grades the reasoning
// behind a number cannot see a number that is correct and applied in the
// wrong place, so this one grades the applied RESULT: whatever ceilings are
// declared, the product of the two axes must honour the tightest of them.
func TestResolveCopyAxes_ProductRespectsCopyConcurrencyCeiling(t *testing.T) {
	t.Parallel()

	const copyConcurrencyLimit = 4

	cases := []struct {
		name string

		resolvedWithin    int
		requestedTable    int
		copyBudgetForAxes int
		maxTargetConns    int
		report            ir.ConnectionBudget

		wantProductAtMost int
	}{
		{
			// The exact shape of the live failure: a generous connection
			// budget, a COPY-concurrency ceiling of 4, and a table axis that
			// would happily take 4 workers of 4 streams each.
			name:              "generous slots, tight COPY ceiling",
			resolvedWithin:    4,
			requestedTable:    4,
			copyBudgetForAxes: 29,
			report:            ir.ConnectionBudget{CopyConcurrencyCeiling: copyConcurrencyLimit},
			wantProductAtMost: copyConcurrencyLimit,
		},
		{
			// Slots are abundant and unbounded by the operator; the COPY
			// ceiling is the only thing standing between the run and a
			// refusal. This is the arm that most needs to hold.
			name:              "no operator ceiling at all",
			resolvedWithin:    4,
			requestedTable:    8,
			copyBudgetForAxes: 100,
			maxTargetConns:    0,
			report:            ir.ConnectionBudget{CopyConcurrencyCeiling: copyConcurrencyLimit},
			wantProductAtMost: copyConcurrencyLimit,
		},
		{
			// The operator's ceiling is TIGHTER than the target's. The
			// tighter one wins; the fold must not let a declared ceiling
			// loosen an operator's explicit bound.
			name:              "operator ceiling tighter than the target's",
			resolvedWithin:    2,
			requestedTable:    8,
			copyBudgetForAxes: 100,
			maxTargetConns:    2,
			report:            ir.ConnectionBudget{CopyConcurrencyCeiling: copyConcurrencyLimit},
			wantProductAtMost: 2,
		},
		{
			// The zero-value-safe default (the v0.99.51 trap): an engine
			// that declares no COPY ceiling must leave the axes governed by
			// the slot budget exactly as before, never clamped to zero.
			name:              "no ceiling declared: slot budget still governs",
			resolvedWithin:    2,
			requestedTable:    4,
			copyBudgetForAxes: 8,
			report:            ir.ConnectionBudget{},
			wantProductAtMost: 8,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tableP, withinP := ResolveCopyAxes(
				tc.resolvedWithin, tc.requestedTable,
				tc.copyBudgetForAxes, tc.maxTargetConns,
				tc.report,
			)
			if tableP < 1 || withinP < 1 {
				t.Fatalf("resolved axes must both be >= 1, got table=%d within=%d", tableP, withinP)
			}
			if product := tableP * withinP; product > tc.wantProductAtMost {
				t.Fatalf(
					"resolved %d table workers x %d within-table streams = %d concurrent COPYs, "+
						"which exceeds the ceiling of %d. A target that refuses the (N+1)th COPY will "+
						"refuse this run — the ceiling has to bound the PRODUCT of the axes, not one of them.",
					tableP, withinP, product, tc.wantProductAtMost,
				)
			}
		})
	}
}

// TestResolveCopyAxes_CeilingNeverRaises is the direction a future edit is
// most likely to get backwards: a declared ceiling is an upper bound, so it
// must never widen axes that a tighter slot budget had already narrowed.
func TestResolveCopyAxes_CeilingNeverRaises(t *testing.T) {
	t.Parallel()

	for _, slots := range []int{1, 2, 3, 4, 8} {
		withCeiling, withinA := ResolveCopyAxes(2, 4, slots, 0,
			ir.ConnectionBudget{CopyConcurrencyCeiling: 64})
		without, withinB := ResolveCopyAxes(2, 4, slots, 0, ir.ConnectionBudget{})

		if got, want := withCeiling*withinA, without*withinB; got > want {
			t.Errorf(
				"slot budget %d: declaring a LOOSE COPY ceiling raised the product from %d to %d — "+
					"a ceiling only ever reduces",
				slots, want, got,
			)
		}
	}
}
