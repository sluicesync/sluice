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
	"strings"
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
			got := RenderNetworkAddr(a, NetworkLiteralRenderingHostBare)
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
		if RenderNetworkAddr(a, NetworkLiteralRenderingHostBare) != a.String() {
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
		if got := RenderNetworkPrefix(netip.MustParsePrefix(tc.in), NetworkLiteralRenderingHostBare); got != tc.want {
			t.Errorf("RenderNetworkPrefix(%s) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestRenderNetworkAddr_MariaDBCompressesASingleZeroHextet pins the axis on
// which the two servers DISAGREE — the thing the original version of this file
// asserted did not exist.
//
// MariaDB compresses a zero run of length one; RFC 5952 §4.2.2 forbids it, and
// Postgres and netip obey the RFC. Ground-truthed on mariadb:11.4 and 10.11.
// Rendering a MariaDB literal under the Postgres rule produces a spelling that
// column can never deliver, so the predicate matches nothing.
func TestRenderNetworkAddr_MariaDBCompressesASingleZeroHextet(t *testing.T) {
	for _, tc := range []struct {
		in       string
		mariadb  string // what a MariaDB inet6 column delivers
		postgres string // what a PG inet column delivers
	}{
		{"2001:db8:0:1:2:3:4:5", "2001:db8::1:2:3:4:5", "2001:db8:0:1:2:3:4:5"},
		{"0:1:2:3:4:5:6:7", "::1:2:3:4:5:6:7", "0:1:2:3:4:5:6:7"},
		{"1:2:3:4:5:6:7:0", "1:2:3:4:5:6:7::", "1:2:3:4:5:6:7:0"},
		// Runs of two or more: both compress, so both agree.
		{"2001:db8::1", "2001:db8::1", "2001:db8::1"},
		{"::1", "::1", "::1"},
		// The dotted-quad rule is shared, so it must survive the split.
		{"::1.2.3.4", "::1.2.3.4", "::1.2.3.4"},
	} {
		t.Run(tc.in, func(t *testing.T) {
			a := netip.MustParseAddr(tc.in)
			if got := RenderNetworkAddr(a, NetworkLiteralRenderingAddressOnly); got != tc.mariadb {
				t.Errorf("MariaDB rendering of %s = %q, want %q — a --where literal on this address would "+
					"never equal what the column delivers", tc.in, got, tc.mariadb)
			}
			if got := RenderNetworkAddr(a, NetworkLiteralRenderingHostBare); got != tc.postgres {
				t.Errorf("Postgres rendering of %s = %q, want %q", tc.in, got, tc.postgres)
			}
		})
	}
}

// TestRenderNetworkAddr_ServersActuallyDisagree is the anti-vacuity floor on
// the split: if the two renderings ever produce identical output for every
// case above, the per-engine branch is dead and the table proves nothing.
func TestRenderNetworkAddr_ServersActuallyDisagree(t *testing.T) {
	diverged := 0
	for _, s := range []string{"2001:db8:0:1:2:3:4:5", "0:1:2:3:4:5:6:7", "1:2:3:4:5:6:7:0"} {
		a := netip.MustParseAddr(s)
		if RenderNetworkAddr(a, NetworkLiteralRenderingAddressOnly) != RenderNetworkAddr(a, NetworkLiteralRenderingHostBare) {
			diverged++
		}
	}
	if diverged < 3 {
		t.Fatalf("only %d of 3 single-zero-hextet addresses render differently between the MariaDB and "+
			"Postgres rules; the per-engine split has collapsed and one of the two engines is now being "+
			"handed a spelling it never sends", diverged)
	}
}

// TestRenderNetworkAddr_ZoneScopedIsLeftIntact pins that a zone is never
// silently dropped. Neither server accepts a zone-scoped literal, so the
// caller must be able to see the zone in order to refuse it — the dotted-quad
// branch would otherwise format only the trailing four bytes and discard it.
func TestRenderNetworkAddr_ZoneScopedIsLeftIntact(t *testing.T) {
	for _, s := range []string{"fe80::1%eth0", "::1.2.3.4%eth0"} {
		a, err := netip.ParseAddr(s)
		if err != nil {
			t.Fatalf("fixture %q does not parse: %v", s, err)
		}
		// The property that matters is that the ZONE survives, not that the
		// input round-trips byte-for-byte — netip normalises the address part
		// (`::1.2.3.4%eth0` → `::102:304%eth0`), which is fine because the
		// caller refuses the literal outright rather than comparing it.
		for _, r := range []NetworkLiteralRendering{NetworkLiteralRenderingHostBare, NetworkLiteralRenderingAddressOnly} {
			got := RenderNetworkAddr(a, r)
			if !strings.Contains(got, "%eth0") {
				t.Errorf("rendering %q under %v = %q — the zone was DROPPED. The dotted-quad branch formats "+
					"only the trailing four bytes, so a zone-scoped literal would silently become a "+
					"different address that the caller then treats as valid", s, r, got)
			}
		}
	}
}
