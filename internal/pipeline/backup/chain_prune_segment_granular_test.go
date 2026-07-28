// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"fmt"
	"testing"
)

// Roadmap item 100's rounding rule, pinned as a table over CHAIN SHAPES
// rather than one representative chain.
//
// [segmentAlignedKeep] dispatches on the shape of the lineage — where
// the segment boundaries fall — and the shapes behave genuinely
// differently: a never-rotated chain has NO boundary (so every
// keep-count rounds to "retain everything", which the caller refuses), a
// rotated chain has one per segment, a request above every boundary has
// nothing to round up TO, and a leading segment holding no incrementals
// contributes no boundary while still being droppable. Pinning one of
// those proves nothing about the others, which is the Bug-74 discipline
// applied to a shape-dispatched function instead of a type-dispatched
// one.
//
// The two counts that are boundary-aligned WITHOUT appearing in
// [segmentBoundaryKeepCounts] — keep 0 (retire everything, the
// --keep-duration-past-all shape) and keep len(flat) (retain everything)
// — are cells here too, because they are exactly the ones a "look it up
// in the list" implementation would get wrong.
func TestSegmentAlignedKeep_RoundsUpAcrossEveryChainShape(t *testing.T) {
	// shape lists incrementals-per-segment, oldest segment first.
	cases := []struct {
		name  string
		shape []int
		keep  int
		want  int
	}{
		// Never-rotated: no boundary exists, so every positive request
		// rounds up to the whole chain (and PruneChain refuses that).
		{"single-segment/keep-1-of-5", []int{5}, 1, 5},
		{"single-segment/keep-4-of-5", []int{5}, 4, 5},
		{"single-segment/retire-all", []int{5}, 0, 0},

		// Rotated 2/2: the only boundary is keep=2.
		{"two-segment/keep-1-rounds-to-2", []int{2, 2}, 1, 2},
		{"two-segment/keep-2-is-already-aligned", []int{2, 2}, 2, 2},
		{"two-segment/keep-3-has-no-boundary-above", []int{2, 2}, 3, 4},
		{"two-segment/retire-all", []int{2, 2}, 0, 0},

		// Rotated 2/2/2: boundaries at 2 and 4.
		{"three-segment/keep-1-rounds-to-2", []int{2, 2, 2}, 1, 2},
		{"three-segment/keep-3-rounds-to-4", []int{2, 2, 2}, 3, 4},
		{"three-segment/keep-4-is-already-aligned", []int{2, 2, 2}, 4, 4},
		{"three-segment/keep-5-has-no-boundary-above", []int{2, 2, 2}, 5, 6},

		// Ragged segments: the boundary positions are not multiples of
		// anything, which is the case a modulo-based implementation gets
		// wrong.
		{"ragged-3-1-2/keep-1-rounds-to-2", []int{3, 1, 2}, 1, 2},
		{"ragged-3-1-2/keep-2-is-already-aligned", []int{3, 1, 2}, 2, 2},
		{"ragged-3-1-2/keep-3-rounds-to-3", []int{3, 1, 2}, 3, 3},
		{"ragged-3-1-2/keep-4-has-no-boundary-above", []int{3, 1, 2}, 4, 6},

		// A leading segment with NO incrementals contributes no boundary
		// of its own. Rounding to "retain everything" is still the right
		// answer here — PruneChain then notices the empty segment is
		// droppable anyway and does NOT refuse (pinned by
		// TestPruneLineage_LeadingIncrementalLessSegmentStillDrops in
		// internal/pipeline).
		{"empty-leading-segment/keep-1", []int{0, 2}, 1, 2},
		{"empty-leading-segment/keep-2-aligned", []int{0, 2}, 2, 2},

		// One incremental per segment: every count is a boundary.
		{"one-per-segment/keep-1", []int{1, 1, 1}, 1, 1},
		{"one-per-segment/keep-2", []int{1, 1, 1}, 2, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			flat := flatFromShape(tc.shape)
			if got := segmentAlignedKeep(flat, tc.keep); got != tc.want {
				t.Fatalf("segmentAlignedKeep(shape=%v, keep=%d) = %d; want %d", tc.shape, tc.keep, got, tc.want)
			}
		})
	}
}

// TestSegmentAlignedKeep_NeverRetainsFewerThanAsked is the invariant
// underneath every cell above, stated once and checked exhaustively:
// rounding is UP, so the result is never below the request (except the
// deliberate keep<=0 case, which asks for nothing) and always lands on a
// shape a whole-segment prune can express.
func TestSegmentAlignedKeep_NeverRetainsFewerThanAsked(t *testing.T) {
	for _, shape := range [][]int{{5}, {2, 2}, {2, 2, 2}, {3, 1, 2}, {0, 2}, {1, 1, 1}} {
		flat := flatFromShape(shape)
		for keep := 1; keep <= len(flat)+1; keep++ {
			// A request beyond the chain is capped at the chain — there is
			// nothing more to retain. (PruneChain never gets here with such
			// a request: keep >= the incremental count is its no-op early
			// return.)
			want := min(keep, len(flat))
			got := segmentAlignedKeep(flat, keep)
			if got < want {
				t.Errorf("shape=%v keep=%d rounded DOWN to %d — retention must only ever grow", shape, keep, got)
			}
			if got > len(flat) {
				t.Errorf("shape=%v keep=%d rounded to %d, past the %d incrementals that exist", shape, keep, got, len(flat))
			}
			// Boundary-aligned means: the survivor set starts at a
			// segment's first incremental, or there is no survivor set.
			if drop := len(flat) - got; drop > 0 && flat[drop].inSegIdx != 0 {
				t.Errorf("shape=%v keep=%d → drop %d, whose first survivor is at in-segment index %d; that is a within-segment trim",
					shape, keep, drop, flat[drop].inSegIdx)
			}
		}
	}
}

// flatFromShape builds the lineage-flattened incremental list
// [segmentAlignedKeep] reads, from a per-segment incremental count. Only
// the two index fields matter to the rounding.
func flatFromShape(perSegment []int) []lineageIncr {
	var flat []lineageIncr
	for si, n := range perSegment {
		for ii := 0; ii < n; ii++ {
			flat = append(flat, lineageIncr{
				segIdx: si, inSegIdx: ii,
				path: fmt.Sprintf("manifests/seg%d-incr%d.json", si, ii),
			})
		}
	}
	return flat
}
