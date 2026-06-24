// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "testing"

// TestComputeConnectionBudget pins the pure budget formula across the
// family of limit shapes the MySQL connection-budget prober enumerates
// (ADR-0116): unlimited per-user limit, tight global, role-capped, the
// refuse-when-<1 boundary, and the Part-B buffer-pool tier cap dominating.
// The math is I/O-free so the whole matrix is table-driven without a
// database.
//
// Each case sets bufferPoolBytes to a value whose tier cap is wide enough
// NOT to dominate (>= 8 GB ⇒ cap 8) unless the case is specifically
// exercising the tier cap, so the connection-derived budget is isolated.
func TestComputeConnectionBudget(t *testing.T) {
	const reserve = 4
	const wideBufferPool = int64(16) << 30 // 16 GiB ⇒ tier cap 8 (won't dominate the small budgets below)

	tests := []struct {
		name          string
		probe         connectionBudgetProbe
		wantAvailable int
		wantCopy      int
	}{
		{
			name: "unlimited per-user, roomy global",
			probe: connectionBudgetProbe{
				maxConnections: 100, inUse: 10,
				roleLimit:       unlimited,
				bufferPoolBytes: wideBufferPool,
			},
			// global = 100-10 = 90; role unlimited; tier cap 8 dominates.
			wantAvailable: 90,
			wantCopy:      8, // min(90-4=86, tierCap 8)
		},
		{
			name: "tight global dominates (below tier cap)",
			probe: connectionBudgetProbe{
				maxConnections: 20, inUse: 8,
				roleLimit:       unlimited,
				bufferPoolBytes: wideBufferPool,
			},
			// global = 20-8 = 12; copy = 12-4 = 8; tier cap 8 ties → 8.
			wantAvailable: 12,
			wantCopy:      8,
		},
		{
			name: "very tight global below tier cap",
			probe: connectionBudgetProbe{
				maxConnections: 14, inUse: 4,
				roleLimit:       unlimited,
				bufferPoolBytes: wideBufferPool,
			},
			// global = 14-4 = 10; copy = 10-4 = 6 (< tier cap 8) → 6.
			wantAvailable: 10,
			wantCopy:      6,
		},
		{
			name: "per-user role limit dominates",
			probe: connectionBudgetProbe{
				maxConnections: 100, inUse: 10, // global = 90
				roleLimit:       9,
				bufferPoolBytes: wideBufferPool,
			},
			// available = min(90, 9) = 9; copy = 9-4 = 5 (< tier cap 8).
			wantAvailable: 9,
			wantCopy:      5,
		},
		{
			name: "exhausted: global leaves nothing after reserve",
			probe: connectionBudgetProbe{
				maxConnections: 20, inUse: 18, // global = 2
				roleLimit:       unlimited,
				bufferPoolBytes: wideBufferPool,
			},
			// available = 2; copy = 2-4 = -2.
			wantAvailable: 2,
			wantCopy:      -2,
		},
		{
			name: "boundary: copy budget exactly 1",
			probe: connectionBudgetProbe{
				maxConnections: 20, inUse: 15, // global = 5
				roleLimit:       unlimited,
				bufferPoolBytes: wideBufferPool,
			},
			// available = 5; copy = 5-4 = 1.
			wantAvailable: 5,
			wantCopy:      1,
		},
		{
			name: "buffer-pool tier cap dominates a roomy connection budget",
			probe: connectionBudgetProbe{
				maxConnections: 250, inUse: 6, // global = 244 (the PlanetScale conns=6/250 shape)
				roleLimit:       unlimited,
				bufferPoolBytes: 134217728, // PS-10 = 0.125 GB ⇒ tier cap 2
			},
			// available = 244; connection copy = 240, but PS-10 tier cap 2
			// dominates — the Part-B CPU proxy is the load-bearing bound
			// exactly where connections are abundant.
			wantAvailable: 244,
			wantCopy:      2,
		},
		{
			name: "buffer pool unreadable ⇒ tier cap no-op",
			probe: connectionBudgetProbe{
				maxConnections: 100, inUse: 10, // global = 90
				roleLimit:       unlimited,
				bufferPoolBytes: 0, // unreadable ⇒ cap not applied
			},
			// copy = 90-4 = 86, no tier cap folded in.
			wantAvailable: 90,
			wantCopy:      86,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// This matrix is the PlanetScale path (applyTierCap=true) — it
			// exercises the buffer-pool tier cap. The non-PlanetScale path is
			// pinned separately in TestComputeConnectionBudget_TierCapPlanetScaleOnly.
			got := computeConnectionBudget(tc.probe, reserve, true)
			if got.Available != tc.wantAvailable {
				t.Errorf("Available = %d, want %d", got.Available, tc.wantAvailable)
			}
			if got.CopyBudget != tc.wantCopy {
				t.Errorf("CopyBudget = %d, want %d", got.CopyBudget, tc.wantCopy)
			}
		})
	}
}

// TestComputeConnectionBudget_TierCapPlanetScaleOnly pins the v0.99.122
// regression fix: the buffer-pool tier cap (Part B) applies ONLY on the
// PlanetScale flavor. v0.99.121's first cut folded it on every MySQL flavor,
// so a vanilla MySQL (or self-hosted Vitess) with the common 128 MB default
// innodb_buffer_pool_size was throttled to parallelism 2 — collapsing parallel
// backup/restore (caught by the -race integration shard, not unit tests). With
// applyTierCap=false the connection-derived budget stands unchanged and the
// recorded tierCap is its 0 "not applied" sentinel.
func TestComputeConnectionBudget_TierCapPlanetScaleOnly(t *testing.T) {
	const reserve = 4
	// A roomy connection budget + a PS-10-sized buffer pool (128 MiB) whose
	// tier cap WOULD be 2 if applied. This is the exact vanilla-MySQL shape
	// that regressed: max_connections 151 (MySQL default), a few in use.
	probe := connectionBudgetProbe{
		maxConnections:  151,
		inUse:           6,
		roleLimit:       unlimited,
		bufferPoolBytes: 134217728, // 128 MiB ⇒ tier cap 2 if applied
	}

	// PlanetScale flavor: the tier cap binds (the CPU-proxy is the point).
	ps := computeConnectionBudget(probe, reserve, true)
	if ps.CopyBudget != 2 || ps.tierCap != 2 {
		t.Errorf("PlanetScale: CopyBudget=%d tierCap=%d; want 2/2 (tier cap binds)", ps.CopyBudget, ps.tierCap)
	}

	// Vanilla MySQL / self-hosted Vitess: the cap is NOT applied — the full
	// connection budget stands (151-6-4 = 141), tierCap recorded as 0.
	nonPS := computeConnectionBudget(probe, reserve, false)
	if nonPS.CopyBudget != 141 {
		t.Errorf("non-PlanetScale: CopyBudget=%d; want 141 (no tier cap — parallelism preserved)", nonPS.CopyBudget)
	}
	if nonPS.tierCap != 0 {
		t.Errorf("non-PlanetScale: tierCap=%d; want 0 (not applied)", nonPS.tierCap)
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

// TestConnLimitText pins the unlimited → "unlimited" rendering used in the
// refusal message.
func TestConnLimitText(t *testing.T) {
	if got := connLimitText(unlimited); got != "unlimited" {
		t.Errorf("connLimitText(unlimited) = %q, want %q", got, "unlimited")
	}
	if got := connLimitText(12); got != "12" {
		t.Errorf("connLimitText(12) = %q, want %q", got, "12")
	}
}

// TestTierCapText pins the 0 → "n/a" rendering (the tier cap not applied)
// used in the refusal message.
func TestTierCapText(t *testing.T) {
	if got := tierCapText(0); got != "n/a" {
		t.Errorf("tierCapText(0) = %q, want %q", got, "n/a")
	}
	if got := tierCapText(-1); got != "n/a" {
		t.Errorf("tierCapText(-1) = %q, want %q", got, "n/a")
	}
	if got := tierCapText(4); got != "4" {
		t.Errorf("tierCapText(4) = %q, want %q", got, "4")
	}
}
