// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The inet ADDRESS-FAMILY axis of a network literal (roadmap item 133).
//
// [ir.NetworkLiteralRendering] answers "is a prefix length shown?".
// [ir.InetFamily] answers a SECOND, independent question: was the address
// itself coerced into another family before it was rendered? MariaDB's INET4
// and INET6 share one rendering (AddressOnly) and disagree on the second — an
// INET6 column stores and delivers `10.0.0.1` as `::ffff:10.0.0.1` while the
// server still matches the bare literal — so the rendering axis alone could
// not have caught this, and a per-representative pin on one family could not
// either.
//
// The matrix below is therefore FAMILY × SHAPE × RENDERING rather than one
// representative per family: every family (unspecified / any / v4 / v6) against
// every address shape the two servers distinguish (bare v4, bare v6,
// IPv4-mapped, IPv4-compatible, the all-zero forms, a full-width prefix, a
// narrower prefix), under every rendering that family can occur with.
//
// The ground truth for the v6 rows is mariadb:11.4 — see [coerceToInetFamily]
// for the captured HEX/SELECT/WHERE evidence.

package rowpredicate

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestNetworkLiteral_InetFamilyCoercion(t *testing.T) {
	cases := []struct {
		name      string
		family    ir.InetFamily
		rendering ir.NetworkLiteralRendering
		in        string
		want      string
		wantOK    bool
	}{
		// ---- IPv6-constrained (MariaDB INET6) ----
		//
		// THE HEADLINE. The literal an operator naturally writes is not the
		// spelling the column delivers, so it must canonicalise UP to the
		// IPv4-mapped form; before the family existed it round-tripped to
		// itself and the filter silently matched nothing on the CDC leg.
		{
			"v6col/bare v4 widens to IPv4-mapped", ir.InetFamilyIPv6, ir.NetworkLiteralRenderingAddressOnly,
			"10.0.0.1", "::ffff:10.0.0.1", true,
		},
		{
			"v6col/all-zero v4 widens too", ir.InetFamilyIPv6, ir.NetworkLiteralRenderingAddressOnly,
			"0.0.0.0", "::ffff:0.0.0.0", true,
		},
		{
			"v6col/broadcast v4 widens too", ir.InetFamilyIPv6, ir.NetworkLiteralRenderingAddressOnly,
			"255.255.255.255", "::ffff:255.255.255.255", true,
		},
		{
			"v6col/zero-padded v4 normalises then widens", ir.InetFamilyIPv6, ir.NetworkLiteralRenderingAddressOnly,
			"010.000.000.001", "::ffff:10.0.0.1", true,
		},
		{
			"v6col/already IPv4-mapped is canonical", ir.InetFamilyIPv6, ir.NetworkLiteralRenderingAddressOnly,
			"::ffff:10.0.0.1", "::ffff:10.0.0.1", true,
		},
		// IPv4-COMPATIBLE is NOT re-mapped by the server (`::1.2.3.4` stores
		// 16 zero-then-quad bytes and comes back `::1.2.3.4`), so widening
		// must not touch an address that is already v6.
		{
			"v6col/IPv4-compatible is left alone", ir.InetFamilyIPv6, ir.NetworkLiteralRenderingAddressOnly,
			"::1.2.3.4", "::1.2.3.4", true,
		},
		{
			"v6col/ordinary v6 is left alone", ir.InetFamilyIPv6, ir.NetworkLiteralRenderingAddressOnly,
			"2001:db8::1", "2001:db8::1", true,
		},
		{
			"v6col/:: is left alone", ir.InetFamilyIPv6, ir.NetworkLiteralRenderingAddressOnly,
			"::", "::", true,
		},
		// A full-width v4 PREFIX reduces to its address and then widens: the
		// /32 has to travel to /128 with it, or the mapped address reads as a
		// 32-bit network of a 128-bit value and loses full-width status.
		{
			"v6col/full-width v4 prefix reduces then widens", ir.InetFamilyIPv6, ir.NetworkLiteralRenderingAddressOnly,
			"10.0.0.1/32", "::ffff:10.0.0.1", true,
		},
		{
			"v6col/full-width v6 prefix reduces", ir.InetFamilyIPv6, ir.NetworkLiteralRenderingAddressOnly,
			"2001:db8::1/128", "2001:db8::1", true,
		},
		// A genuine network still cannot be held by an address-only column.
		{
			"v6col/narrower prefix still refuses", ir.InetFamilyIPv6, ir.NetworkLiteralRenderingAddressOnly,
			"10.0.0.0/24", "", false,
		},

		// ---- IPv4-constrained (MariaDB INET4) ----
		//
		// The server REFUSES an IPv6 literal on INSERT (errno 1292) and
		// matches nothing for one in a WHERE, so every v6 shape is refused
		// rather than unmapped — unmapping would rewrite a predicate the
		// server rejects into one it accepts.
		{
			"v4col/bare v4 is canonical", ir.InetFamilyIPv4, ir.NetworkLiteralRenderingAddressOnly,
			"10.0.0.1", "10.0.0.1", true,
		},
		{
			"v4col/zero-padded v4 normalises", ir.InetFamilyIPv4, ir.NetworkLiteralRenderingAddressOnly,
			"010.000.000.001", "10.0.0.1", true,
		},
		{
			"v4col/full-width v4 prefix reduces", ir.InetFamilyIPv4, ir.NetworkLiteralRenderingAddressOnly,
			"10.0.0.1/32", "10.0.0.1", true,
		},
		{
			"v4col/IPv4-mapped v6 refuses", ir.InetFamilyIPv4, ir.NetworkLiteralRenderingAddressOnly,
			"::ffff:10.0.0.1", "", false,
		},
		{
			"v4col/ordinary v6 refuses", ir.InetFamilyIPv4, ir.NetworkLiteralRenderingAddressOnly,
			"2001:db8::1", "", false,
		},
		{
			"v4col/IPv4-compatible v6 refuses", ir.InetFamilyIPv4, ir.NetworkLiteralRenderingAddressOnly,
			"::1.2.3.4", "", false,
		},
		{
			"v4col/v6 prefix refuses", ir.InetFamilyIPv4, ir.NetworkLiteralRenderingAddressOnly,
			"2001:db8::1/128", "", false,
		},
		{
			"v4col/narrower v4 prefix still refuses", ir.InetFamilyIPv4, ir.NetworkLiteralRenderingAddressOnly,
			"10.0.0.0/24", "", false,
		},

		// ---- Unconstrained (Postgres inet / cidr) ----
		//
		// The family must change NOTHING here, on BOTH Postgres renderings:
		// PG stores what it is given, so a coercion leaking into this arm
		// would be the mirror-image silent bug.
		{
			"anycol/hostbare bare v4 untouched", ir.InetFamilyAny, ir.NetworkLiteralRenderingHostBare,
			"10.0.0.1", "10.0.0.1", true,
		},
		{
			"anycol/hostbare IPv4-mapped untouched", ir.InetFamilyAny, ir.NetworkLiteralRenderingHostBare,
			"::ffff:10.0.0.1", "::ffff:10.0.0.1", true,
		},
		{
			"anycol/hostbare full-width mask drops", ir.InetFamilyAny, ir.NetworkLiteralRenderingHostBare,
			"10.0.0.1/32", "10.0.0.1", true,
		},
		{
			"anycol/hostbare narrower mask kept", ir.InetFamilyAny, ir.NetworkLiteralRenderingHostBare,
			"10.0.0.0/24", "10.0.0.0/24", true,
		},
		{
			"anycol/alwaysmasked bare v4 gains /32", ir.InetFamilyAny, ir.NetworkLiteralRenderingAlwaysMasked,
			"10.0.0.1", "10.0.0.1/32", true,
		},
		{
			"anycol/alwaysmasked IPv4-mapped gains /128", ir.InetFamilyAny, ir.NetworkLiteralRenderingAlwaysMasked,
			"::ffff:10.0.0.1", "::ffff:10.0.0.1/128", true,
		},
		{
			"anycol/alwaysmasked network kept", ir.InetFamilyAny, ir.NetworkLiteralRenderingAlwaysMasked,
			"10.0.0.0/24", "10.0.0.0/24", true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := canonicalIdentifierLiteral(tc.in, ColumnInfo{
				Identifier: identifierNetwork, NetworkRendering: tc.rendering, InetFamily: tc.family,
			})
			if ok != tc.wantOK || got != tc.want {
				t.Errorf("canonicalIdentifierLiteral(%q, family=%d, rendering=%v) = %q, %v; want %q, %v",
					tc.in, tc.family, tc.rendering, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

// TestNetworkLiteral_UndeclaredFamilyRefuses pins the fail-closed zero value on
// the family axis, exactly as TestNetworkLiteral_UndeclaredRenderingRefuses
// does on the rendering axis.
//
// The two axes need SEPARATE pins: a resolver that declares a rendering says
// nothing about the family, and the engine whose family matters (MariaDB) has
// a perfectly well-declared rendering. So this drives a resolver that DOES
// declare the rendering and leaves the column type's family unset.
func TestNetworkLiteral_UndeclaredFamilyRefuses(t *testing.T) {
	infos := ColumnInfosFromIR(
		fakeNetworkResolver{rendering: ir.NetworkLiteralRenderingAddressOnly},
		[]*ir.Column{{Name: "ip", Type: ir.Inet{}}, {Name: "net", Type: ir.Cidr{}}}, false,
	)

	for _, col := range []string{"ip", "net"} {
		for _, spelling := range []string{"10.0.0.1", "2001:db8::1", "10.0.0.1/32"} {
			_, err := Compile("t", col+" = '"+spelling+"'", infos)
			if err == nil {
				t.Errorf("%s = %q compiled against a column whose address family nobody declared; a MariaDB "+
					"INET6 column would deliver a different spelling, so this must refuse rather than guess",
					col, spelling)
				continue
			}
			// Assert the REASON, not merely that something refused. Every one
			// of these spellings is a valid network value under the declared
			// AddressOnly rendering, so a refusal from anywhere else would
			// mean the fail-closed branch never ran — and this gate would stay
			// green with that branch deleted.
			if !strings.Contains(err.Error(), "has not declared which address family") {
				t.Errorf("%s = %q was refused, but not for the undeclared-family reason — the fail-closed "+
					"branch did not run. got: %v", col, spelling, err)
			}
		}
	}
}

// TestNetworkLiteral_MariaDBInet6NamesTheMappedSpelling is the operator-facing
// half: the refusal must not merely refuse, it must NAME the spelling to write.
// A refusal that says "not canonical" without the alternative sends the
// operator back to a value their own database shows them.
func TestNetworkLiteral_MariaDBInet6NamesTheMappedSpelling(t *testing.T) {
	infos := ColumnInfosFromIR(
		fakeNetworkResolver{rendering: ir.NetworkLiteralRenderingAddressOnly},
		[]*ir.Column{{Name: "ip", Type: ir.Inet{Family: ir.InetFamilyIPv6}}}, false,
	)

	_, err := Compile("t", "ip = '10.0.0.1'", infos)
	if err == nil {
		t.Fatal("`ip = '10.0.0.1'` compiled against a MariaDB INET6 column. The column delivers " +
			"`::ffff:10.0.0.1`, so the cold copy matches the row server-side and the CDC leg then drops " +
			"every change to it — the sync freezes, caught up, at exit 0 (roadmap item 133)")
	}
	if !strings.Contains(err.Error(), "::ffff:10.0.0.1") {
		t.Errorf("the refusal must name the spelling to write (`::ffff:10.0.0.1`); got: %v", err)
	}

	// And the named spelling must actually compile, or the remedy is a
	// dead end.
	if _, err := Compile("t", "ip = '::ffff:10.0.0.1'", infos); err != nil {
		t.Errorf("the spelling the refusal names must itself compile: %v", err)
	}
}
