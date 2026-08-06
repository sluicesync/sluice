// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "testing"

// The roadmap item 144 gates: the WRITE-side copy fan-out ceiling derived
// from the ADR-0116 Part-B buffer-pool probe.
//
// SCOPE, stated so the name cannot be read as broader than the truth:
// these pin the DERIVATION and its plumbing into [ir.ConnectionBudget].
// They say nothing about whether the pipeline honours the ceiling — that is
// TestBulkCopyHonoursTheTargetFanoutCeiling in internal/pipeline, and it is
// the half that actually matters, because item 144's defect was a tier
// verdict that was computed correctly and then thrown away.

const (
	mib = int64(1) << 20
	gib = int64(1) << 30

	// planetScaleDevBranchPoolBytes is what a real PlanetScale DEV branch
	// reports, measured three times against aws.connect.psdb.cloud
	// (2026-08-05 on a PS-10 and on the field report's database, 2026-08-06
	// on a fresh PS-10) — always 32 MiB, on databases whose PRODUCTION
	// branch reports its tier correctly.
	planetScaleDevBranchPoolBytes = 32 * mib

	// planetScaleProductionPS10PoolBytes is a real PRODUCTION PS-10 branch,
	// measured the same three times: EXACTLY the floor, never a byte less.
	// It is the item-123 regression control — this reading must NOT take
	// the unknown-tier path.
	planetScaleProductionPS10PoolBytes = 134217728
)

// TestCopyFanoutCeiling_ReadingClassTimesFlavor pins every cell of the
// matrix the decision dispatches on — reading CLASS x flavor gate — rather
// than one representative reading (the Bug 74 discipline). The two axes are
// independent in the code, so a green cell on one flavor says nothing about
// the other.
func TestCopyFanoutCeiling_ReadingClassTimesFlavor(t *testing.T) {
	readings := []struct {
		name  string
		bytes int64
		// wantPlanetScale is the ceiling on the flavor the cap is gated to.
		wantPlanetScale int
	}{
		// Unreadable: an absent reading is not evidence of anything, so no
		// ceiling is declared on either flavor.
		{name: "unreadable (0)", bytes: 0, wantPlanetScale: 0},
		{name: "unreadable (negative, defensive)", bytes: -1, wantPlanetScale: 0},

		// Sub-floor: the tier is UNKNOWN ⇒ conservative fan-out.
		{name: "1 byte", bytes: 1, wantPlanetScale: copyFanoutCeilingUnknownTier},
		{name: "32 MiB (a real PlanetScale DEV branch)", bytes: planetScaleDevBranchPoolBytes, wantPlanetScale: copyFanoutCeilingUnknownTier},
		{name: "one byte under the PS-10 floor", bytes: 128*mib - 1, wantPlanetScale: copyFanoutCeilingUnknownTier},

		// On the tier scale ⇒ NO fan-out ceiling. The first row is the
		// item-123 control and the tightest boundary in the whole design:
		// one byte lower and a real production PS-10 would be held to the
		// dev-branch fan-out.
		{name: "EXACTLY the PS-10 floor (a real production branch)", bytes: planetScaleProductionPS10PoolBytes, wantPlanetScale: 0},
		{name: "PS-20 (~0.83 GB)", bytes: 891289600, wantPlanetScale: 0},
		{name: "PS-40 (~1.64 GB)", bytes: 1761607680, wantPlanetScale: 0},
		{name: "PS-80 (~4.91 GB)", bytes: 5272799232, wantPlanetScale: 0},
		{name: "PS-160 (~9.80 GB)", bytes: 10522669056, wantPlanetScale: 0},
		{name: "128 GB self-hosted-sized", bytes: 128 * gib, wantPlanetScale: 0},
	}

	for _, r := range readings {
		t.Run(r.name+"/planetscale", func(t *testing.T) {
			if got := copyFanoutCeiling(r.bytes, true); got != r.wantPlanetScale {
				t.Fatalf("copyFanoutCeiling(%d, planetscale) = %d; want %d", r.bytes, got, r.wantPlanetScale)
			}
		})
		// The OTHER half of the matrix. Vanilla MySQL and self-hosted Vitess
		// size their buffer pool to the operator's own hardware, so the value
		// is not a tier signal at all there and NO reading may produce a
		// ceiling — including the sub-floor ones, which on a self-hosted box
		// just mean "a small dev MySQL", the exact case computeConnectionBudget's
		// flavor gate exists to protect (v0.99.122).
		t.Run(r.name+"/non-planetscale", func(t *testing.T) {
			if got := copyFanoutCeiling(r.bytes, false); got != 0 {
				t.Fatalf("copyFanoutCeiling(%d, non-planetscale) = %d; want 0 — the fan-out ceiling is "+
					"PlanetScale-flavor-gated exactly as the tier cap is", r.bytes, got)
			}
		})
	}
}

// TestCopyFanoutCeiling_SharesOnePredicateWithTheTierCap is the ANTI-DRIFT
// gate, and it is why the ceiling is derived from [bufferPoolParallelismCap]
// rather than re-deriving the floor comparison.
//
// The two decisions are deliberately different — one bounds a CONNECTION
// BUDGET (and must stay off a sub-floor reading, item 123), the other bounds
// a WRITE FAN-OUT (and must engage on exactly that reading, item 144) — but
// they must agree on WHEN a reading is a tier answer, forever. If someone
// moves the floor, adds a tier, or changes the bucket boundaries, this fails
// unless both halves move together.
func TestCopyFanoutCeiling_SharesOnePredicateWithTheTierCap(t *testing.T) {
	// Walk a range that crosses every boundary, including both sides of the
	// floor and each bucket edge.
	readings := []int64{
		-1, 0, 1, 4 * mib, planetScaleDevBranchPoolBytes, 128*mib - 1,
		planetScaleProductionPS10PoolBytes, 256 * mib, 891289600, 2 * gib,
		5272799232, 8 * gib, 10522669056, 128 * gib,
	}
	sawCeiling, sawNoCeiling := 0, 0
	for _, r := range readings {
		tierCap := bufferPoolParallelismCap(r)
		ceiling := copyFanoutCeiling(r, true)

		wantCeiling := r > 0 && tierCap == 0
		if wantCeiling && ceiling == 0 {
			t.Errorf("reading %d: the tier cap does not place it on the tier scale (cap=0) but no fan-out "+
				"ceiling was declared — the tier signal fails OPEN, which is item 144", r)
		}
		if !wantCeiling && ceiling != 0 {
			t.Errorf("reading %d: the tier cap placed it at %d (or it is unreadable) yet a fan-out ceiling "+
				"of %d was declared — the two decisions have drifted apart", r, tierCap, ceiling)
		}
		if ceiling > 0 {
			sawCeiling++
		} else {
			sawNoCeiling++
		}
	}
	// Anti-vacuity in BOTH directions: a predicate that never fires, and one
	// that always fires, would both satisfy the loop above.
	if sawCeiling == 0 || sawNoCeiling == 0 {
		t.Fatalf("vacuous: %d readings declared a ceiling and %d did not; the sweep must exercise both sides",
			sawCeiling, sawNoCeiling)
	}
}

// TestComputeConnectionBudget_CarriesTheFanoutCeiling pins the plumbing from
// the probe to the budget verdict — and, critically, that declaring a fan-out
// ceiling does NOT move the connection budget. That separation is the whole
// reason item 144 does not re-open item 123: a sub-floor reading still leaves
// the copy budget at the connection-derived value, so migrate's table x
// within-table product is untouched.
func TestComputeConnectionBudget_CarriesTheFanoutCeiling(t *testing.T) {
	probe := connectionBudgetProbe{maxConnections: 250, inUse: 10, roleLimit: unlimited}

	t.Run("sub-floor reading declares a ceiling and leaves the budget alone", func(t *testing.T) {
		p := probe
		p.bufferPoolBytes = planetScaleDevBranchPoolBytes
		got := computeConnectionBudget(p, connBudgetReserve, true)
		if got.fanoutCeiling != copyFanoutCeilingUnknownTier {
			t.Errorf("fanoutCeiling = %d; want %d", got.fanoutCeiling, copyFanoutCeilingUnknownTier)
		}
		if got.tierCap != 0 {
			t.Errorf("tierCap = %d; want 0 — item 123 requires a sub-floor reading NOT to cap the budget", got.tierCap)
		}
		if want := 250 - 10 - connBudgetReserve; got.CopyBudget != want {
			t.Errorf("CopyBudget = %d; want the un-capped connection-derived %d — the fan-out ceiling must not "+
				"leak into the connection budget", got.CopyBudget, want)
		}
	})

	t.Run("a real production PS-10 declares NO ceiling and still tier-caps", func(t *testing.T) {
		p := probe
		p.bufferPoolBytes = planetScaleProductionPS10PoolBytes
		got := computeConnectionBudget(p, connBudgetReserve, true)
		if got.fanoutCeiling != 0 {
			t.Errorf("fanoutCeiling = %d; want 0 — a genuine smallest-tier reading must not be driven at the "+
				"unknown-tier fan-out (the item-123 regression this fix is most likely to cause)", got.fanoutCeiling)
		}
		if got.tierCap != bufferPoolCapSmall {
			t.Errorf("tierCap = %d; want %d — item 123 kept the cap for a genuine PS-10", got.tierCap, bufferPoolCapSmall)
		}
	})

	t.Run("non-PlanetScale flavor declares nothing at any reading", func(t *testing.T) {
		for _, bytes := range []int64{0, planetScaleDevBranchPoolBytes, planetScaleProductionPS10PoolBytes, 128 * gib} {
			p := probe
			p.bufferPoolBytes = bytes
			got := computeConnectionBudget(p, connBudgetReserve, false)
			if got.fanoutCeiling != 0 || got.tierCap != 0 {
				t.Errorf("bytes=%d: fanoutCeiling=%d tierCap=%d; want 0/0 on a non-PlanetScale flavor",
					bytes, got.fanoutCeiling, got.tierCap)
			}
		}
	})

	t.Run("unreadable probe declares nothing", func(t *testing.T) {
		p := probe
		p.bufferPoolBytes = 0
		got := computeConnectionBudget(p, connBudgetReserve, true)
		if got.fanoutCeiling != 0 {
			t.Errorf("fanoutCeiling = %d; want 0 — an unreadable probe is not evidence of anything", got.fanoutCeiling)
		}
	})
}
