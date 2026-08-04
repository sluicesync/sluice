// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The address renderer, pinned in BOTH directions (audit 2026-08-04 C1).
//
// The defect this closes was not that a rendering was wrong. It was that the
// gate only ever asked one question — "is this literal canonical as a Go
// value?" — when the question that decides correctness is "is this what the
// server will send?". The two agree for most addresses, which is why one
// direction of testing kept passing.
//
// So the table below carries the DIVERGING shapes explicitly, with netip's
// answer written out beside the server's. If a future Go release changes
// netip's rendering, the `netipDiffers` column is what fails.

package ir

import (
	"net/netip"
	"testing"
)

func TestRenderNetworkAddr_MatchesTheServerNotNetip(t *testing.T) {
	for _, tc := range []struct {
		in           string
		want         string // what PG 16 and MariaDB 11.4 deliver
		netipDiffers bool
	}{
		// The divergent class: leading 96 bits zero, word 6 non-zero. Both
		// servers print a dotted quad here; netip prints hex groups.
		{"::1.2.3.4", "::1.2.3.4", true},
		{"::10.0.0.1", "::10.0.0.1", true},
		{"::255.255.255.255", "::255.255.255.255", true},
		{"::1:0", "::0.1.0.0", true},

		// Short forms: word 6 is zero, so the zero run swallows it and the
		// server prints the short spelling too.
		{"::", "::", false},
		{"::1", "::1", false},
		{"::2", "::2", false},

		// IPv4-mapped: netip already prints these dotted, and so does the
		// server. This is the shape the original MariaDB fixture used, which
		// is why that fixture could not have caught the bug.
		{"::ffff:10.0.0.1", "::ffff:10.0.0.1", false},

		// Ordinary v6 and v4: unaffected.
		{"2001:db8::1", "2001:db8::1", false},
		{"10.0.0.1", "10.0.0.1", false},
		{"0.0.0.0", "0.0.0.0", false},
	} {
		t.Run(tc.in, func(t *testing.T) {
			a := netip.MustParseAddr(tc.in)
			got := RenderNetworkAddr(a)
			if got != tc.want {
				t.Errorf("RenderNetworkAddr(%s) = %q, want %q — this is the spelling the change stream "+
					"delivers, and a mismatch means a --where literal on this address matches nothing",
					tc.in, got, tc.want)
			}
			// The non-vacuity half: assert that the cases claimed to diverge
			// ACTUALLY diverge from netip. Without this, a change that made
			// RenderNetworkAddr a pass-through to netip.String would keep the
			// table green for every row whose want happens to equal netip's
			// answer — which is most of them.
			if diff := a.String() != tc.want; diff != tc.netipDiffers {
				t.Errorf("netip.String()=%q vs server %q: divergence is %v, table says %v. The table's "+
					"whole purpose is to record where Go and the servers disagree; if that set has "+
					"changed, re-derive it before editing this column.",
					a.String(), tc.want, diff, tc.netipDiffers)
			}
		})
	}
}

// TestRenderNetworkAddr_HasDivergentCases is the anti-vacuity floor on the
// table itself. A table that accidentally lost every diverging row would pass
// every assertion above and prove nothing — the exact shape of the MariaDB
// fixture this finding came from, which claimed to cover the divergence and
// used the one v6 form where there is none.
func TestRenderNetworkAddr_HasDivergentCases(t *testing.T) {
	divergent := 0
	for _, s := range []string{"::1.2.3.4", "::10.0.0.1", "::255.255.255.255", "::1:0"} {
		a := netip.MustParseAddr(s)
		if RenderNetworkAddr(a) != a.String() {
			divergent++
		}
	}
	if divergent < 4 {
		t.Fatalf("only %d of 4 known-divergent addresses still render differently from netip; either Go "+
			"changed or RenderNetworkAddr has become a pass-through, and in both cases the --where gate "+
			"needs re-deriving against a live server", divergent)
	}
}

// TestRenderNetworkPrefix_CarriesTheMask checks the masked form uses the same
// address rendering — the prefix path is a separate call site and was a
// separate bug.
func TestRenderNetworkPrefix_CarriesTheMask(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"::1.2.3.4/128", "::1.2.3.4/128"},
		{"::10.0.0.1/96", "::10.0.0.1/96"},
		{"10.0.0.1/32", "10.0.0.1/32"},
		{"2001:db8::/32", "2001:db8::/32"},
	} {
		if got := RenderNetworkPrefix(netip.MustParsePrefix(tc.in)); got != tc.want {
			t.Errorf("RenderNetworkPrefix(%s) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
