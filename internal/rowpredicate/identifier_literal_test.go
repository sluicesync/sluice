// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The family × spelling matrix for audit 2026-07-26 SL-3.
//
// Pinning ONE representative spelling of ONE family would be the exact mistake
// this project has a standing rule against ("pin the class, not the
// representative"): the dispatch is per-family and each family has its own
// canonicalisation, so a green UUID case says nothing about macaddr. Every
// spelling below was OBSERVED to compare TRUE server-side on real PG 16 /
// MySQL 8.0 during the audit — they are the shapes an operator naturally
// writes, not contrived ones.
package rowpredicate

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestIdentifierLiteral_NonCanonicalSpellingsAreRefused(t *testing.T) {
	cases := []struct {
		name    string
		colType ir.Type
		// canonical compiles and must keep working, byte-for-byte as before.
		canonical string
		// divergent are accepted by the SOURCE but never match the decoded
		// value client-side; each must be refused, naming the canonical form.
		divergent []string
		// rendering is the source engine's inet/cidr rendering. Meaningful
		// only for the network cases, and REQUIRED there — under the
		// undeclared zero value every network literal refuses for that reason
		// alone, which would make these cases vacuous (audit 2026-08-01 S2).
		rendering ir.NetworkLiteralRendering
	}{
		{
			name:      "uuid",
			colType:   ir.UUID{},
			canonical: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
			divergent: []string{
				"A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", // uppercase
				"a0eebc999c0b4ef8bb6d6bb9bd380a11",     // unhyphenated
				"{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
			},
		},
		{
			name:      "inet/bare rendering (MariaDB inet4)",
			colType:   ir.Inet{},
			rendering: ir.NetworkLiteralRenderingBare,
			canonical: "10.0.0.1",
			divergent: []string{
				"010.000.000.001", // zero-padded octets
				"10.0.0.1/32",     // a mask the bare rendering never delivers
			},
		},
		{
			// The S2 case: under Postgres's rendering the canonical spelling
			// is the MASKED one, and the bare address — what an operator
			// copies out of psql — is the divergent spelling that used to
			// compile and then match nothing.
			name:      "inet/masked rendering (Postgres)",
			colType:   ir.Inet{},
			rendering: ir.NetworkLiteralRenderingMasked,
			canonical: "10.0.0.1/32",
			divergent: []string{
				"10.0.0.1",        // the bare spelling psql prints
				"010.000.000.001", // zero-padded octets
			},
		},
		{
			name:      "cidr",
			colType:   ir.Cidr{},
			rendering: ir.NetworkLiteralRenderingMasked,
			canonical: "10.0.0.0/24",
			divergent: []string{
				"010.000.000.000/24",
			},
		},
		{
			name:      "macaddr",
			colType:   ir.Macaddr{},
			canonical: "08:00:2b:01:02:03",
			divergent: []string{
				"08-00-2B-01-02-03", // dashes + uppercase
				"0800.2b01.0203",    // Cisco dotted
				"08:00:2B:01:02:03", // uppercase
			},
		},
		{
			name:      "time(0)",
			colType:   ir.Time{Precision: 0},
			canonical: "08:30:00",
			divergent: []string{
				"08:30", // what an operator naturally writes
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			infos := ColumnInfosFromIR(fakeNetworkResolver{rendering: tc.rendering},
				[]*ir.Column{{Name: "v", Type: tc.colType}}, false)

			// The canonical spelling must still compile — a fix that refuses
			// everything is not a fix.
			if _, err := Compile("t", "v = '"+tc.canonical+"'", infos); err != nil {
				t.Fatalf("canonical spelling %q was refused: %v", tc.canonical, err)
			}

			for _, spelling := range tc.divergent {
				_, err := Compile("t", "v = '"+spelling+"'", infos)
				if err == nil {
					t.Errorf("%q compiled. The source ACCEPTS this spelling (it coerces the literal to the "+
						"column type), so the cold-start copy succeeds — and then the CDC leg compares it against "+
						"the canonical decoded value, scores every change NOT in scope, and silently drops them. "+
						"The target row goes permanently stale at exit 0 (audit SL-3).", spelling)
					continue
				}
				// The refusal is only useful if it tells the operator what to
				// write instead.
				if !strings.Contains(err.Error(), tc.canonical) {
					t.Errorf("%q was refused but the message does not name the canonical spelling %q; got: %v",
						spelling, tc.canonical, err)
				}
			}
		})
	}
}

// TestIdentifierLiteral_FractionalTimeIsRefusedOutright pins the one family
// where no literal is correct: the value renders differently on the snapshot
// and binlog legs, so a single compiled predicate would classify the same row
// two ways.
func TestIdentifierLiteral_FractionalTimeIsRefusedOutright(t *testing.T) {
	for _, colType := range []ir.Type{
		ir.Time{Precision: 6},
		ir.Time{Precision: 3},
		ir.Time{PrecisionUnspecified: true},
	} {
		infos := ColumnInfosFromIR(ir.ByteExactCollationResolver{},
			[]*ir.Column{{Name: "v", Type: colType}}, false)
		for _, spelling := range []string{"08:30:00", "08:30:00.000000", "08:30"} {
			if _, err := Compile("t", "v = '"+spelling+"'", infos); err == nil {
				t.Errorf("%v with literal %q compiled; a fractional TIME renders as 08:30:00.000000 on the "+
					"snapshot leg and 08:30:00 on the binlog leg, so no literal is correct on both and the "+
					"comparison must be refused", colType, spelling)
			}
		}
	}
}

// TestIdentifierLiteral_InvalidValuesAreRefused: a literal that is not a valid
// value of the type at all was previously compared byte-exactly and simply
// never matched.
func TestIdentifierLiteral_InvalidValuesAreRefused(t *testing.T) {
	cases := []struct {
		colType ir.Type
		lit     string
	}{
		{ir.UUID{}, "not-a-uuid"},
		{ir.Inet{}, "999.999.999.999"},
		{ir.Macaddr{}, "zz:00:2b:01:02:03"},
		{ir.Time{Precision: 0}, "25:99"},
	}
	for _, tc := range cases {
		// A declared network rendering keeps the inet row exercising the
		// INVALID-LITERAL path; under the undeclared zero value it would
		// refuse for the unrelated no-rendering reason and go vacuous.
		infos := ColumnInfosFromIR(fakeNetworkResolver{rendering: ir.NetworkLiteralRenderingBare},
			[]*ir.Column{{Name: "v", Type: tc.colType}}, false)
		if _, err := Compile("t", "v = '"+tc.lit+"'", infos); err == nil {
			t.Errorf("%v compared to invalid literal %q compiled; it can never match, so the filter would "+
				"silently select nothing", tc.colType, tc.lit)
		}
	}
}

// TestCanonicalIdentifierLiteral_MatchesDecoderOutput guards the premise the
// refusal rests on: the canonical form this package computes must be the form
// the ENGINE DECODERS produce, or the gate would refuse correct literals and
// accept wrong ones. The decoders render UUIDs via hex, networks via netip,
// and MACs via net.HardwareAddr.String — so these expectations are that
// contract written down.
func TestCanonicalIdentifierLiteral_MatchesDecoderOutput(t *testing.T) {
	cases := []struct {
		kind identifierKind
		in   string
		want string
		// network is meaningful only for identifierNetwork; the network
		// rendering matrix lives in
		// TestCanonicalNetworkLiteral_FollowsTheEngineRendering.
		network ir.NetworkLiteralRendering
	}{
		{kind: identifierUUID, in: "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", want: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
		{kind: identifierUUID, in: "a0eebc999c0b4ef8bb6d6bb9bd380a11", want: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
		{kind: identifierNetwork, in: "010.000.000.001", want: "10.0.0.1", network: ir.NetworkLiteralRenderingBare},
		{kind: identifierNetwork, in: "10.0.0.0/24", want: "10.0.0.0/24", network: ir.NetworkLiteralRenderingMasked},
		{kind: identifierNetwork, in: "2001:0db8:0000:0000:0000:0000:0000:0001", want: "2001:db8::1", network: ir.NetworkLiteralRenderingBare},
		{kind: identifierMAC, in: "08-00-2B-01-02-03", want: "08:00:2b:01:02:03"},
		{kind: identifierMAC, in: "0800.2b01.0203", want: "08:00:2b:01:02:03"},
		{kind: identifierTime, in: "08:30:00", want: "08:30:00"},
	}
	for _, tc := range cases {
		got, ok := canonicalIdentifierLiteral(tc.kind, tc.in, tc.network)
		if !ok || got != tc.want {
			t.Errorf("canonicalIdentifierLiteral(%v, %q, %v) = %q, %v; want %q, true",
				tc.kind, tc.in, tc.network, got, ok, tc.want)
		}
	}
}

// fakeNetworkResolver is a byte-exact collation resolver that also declares a
// network rendering, so tests can exercise each engine's inet spelling without
// importing an engine package.
type fakeNetworkResolver struct {
	ir.ByteExactCollationResolver
	rendering ir.NetworkLiteralRendering
}

func (r fakeNetworkResolver) ResolveNetworkLiteralRendering() ir.NetworkLiteralRendering {
	return r.rendering
}

// TestCanonicalNetworkLiteral_FollowsTheEngineRendering pins the audit-2026-08-01
// S2 contract: the canonical spelling of an inet/cidr literal is decided by the
// SOURCE engine's delivered rendering, not by netip. Postgres delivers every
// value through pgx's InetCodec as a netip.Prefix, so a host address arrives
// masked ("10.0.0.1/32") and a bare literal must be refused-and-corrected;
// MariaDB's native inet4/inet6 arrive bare, so the opposite holds. Getting it
// backwards is SILENT — the literal simply never equals the delivered value —
// which is why both directions are pinned rather than one representative.
func TestCanonicalNetworkLiteral_FollowsTheEngineRendering(t *testing.T) {
	cases := []struct {
		name      string
		rendering ir.NetworkLiteralRendering
		in        string
		want      string
		wantOK    bool
	}{
		// Postgres: the mask is always present, so a bare literal
		// canonicalises UP to its full-width prefix. This is the case that
		// was silently dropping every change before the fix.
		{"masked/bare v4 gains /32", ir.NetworkLiteralRenderingMasked, "10.0.0.1", "10.0.0.1/32", true},
		{"masked/bare v6 gains /128", ir.NetworkLiteralRenderingMasked, "2001:db8::1", "2001:db8::1/128", true},
		{"masked/full-width prefix is already canonical", ir.NetworkLiteralRenderingMasked, "10.0.0.1/32", "10.0.0.1/32", true},
		{"masked/network prefix is already canonical", ir.NetworkLiteralRenderingMasked, "10.0.0.0/24", "10.0.0.0/24", true},
		{"masked/zero-padded octets normalise and gain the mask", ir.NetworkLiteralRenderingMasked, "010.000.000.001", "10.0.0.1/32", true},

		// MariaDB: the value is an address, never a network.
		{"bare/bare v4 is already canonical", ir.NetworkLiteralRenderingBare, "10.0.0.1", "10.0.0.1", true},
		{"bare/bare v6 is already canonical", ir.NetworkLiteralRenderingBare, "2001:db8::1", "2001:db8::1", true},
		{"bare/full-width mask reduces to the address", ir.NetworkLiteralRenderingBare, "10.0.0.1/32", "10.0.0.1", true},
		{"bare/a real network can never match an address column", ir.NetworkLiteralRenderingBare, "10.0.0.0/24", "", false},

		// Undeclared: no spelling is knowable, so nothing canonicalises.
		{"unknown/bare refuses", ir.NetworkLiteralRenderingUnknown, "10.0.0.1", "", false},
		{"unknown/prefix refuses", ir.NetworkLiteralRenderingUnknown, "10.0.0.0/24", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := canonicalIdentifierLiteral(identifierNetwork, tc.in, tc.rendering)
			if ok != tc.wantOK || got != tc.want {
				t.Errorf("canonicalIdentifierLiteral(network, %q, %v) = %q, %v; want %q, %v",
					tc.in, tc.rendering, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

// TestNetworkLiteral_BareSpellingRefusedOnPostgresRendering is the end-to-end
// half: the S2 defect was that `ip = '10.0.0.1'` COMPILED against a Postgres
// source and then matched nothing, so the pin is that it now refuses, and that
// the masked spelling still compiles. A compile success here would be the
// silent-loss regression.
func TestNetworkLiteral_BareSpellingRefusedOnPostgresRendering(t *testing.T) {
	infos := ColumnInfosFromIR(
		fakeNetworkResolver{rendering: ir.NetworkLiteralRenderingMasked},
		[]*ir.Column{{Name: "ip", Type: ir.Inet{}}}, false,
	)

	if _, err := Compile("t", "ip = '10.0.0.1'", infos); err == nil {
		t.Fatal("bare literal compiled against a masked-rendering source; the change stream delivers " +
			"`10.0.0.1/32`, so the filter would score every row out of scope and silently drop every change")
	}
	if _, err := Compile("t", "ip = '10.0.0.1/32'", infos); err != nil {
		t.Fatalf("masked literal must still compile against a masked-rendering source: %v", err)
	}
}

// TestNetworkLiteral_UndeclaredRenderingRefuses pins the fail-closed zero
// value: an engine that has not named its rendering gets a refusal, not a
// guess, because both concrete renderings are silently wrong for the other.
func TestNetworkLiteral_UndeclaredRenderingRefuses(t *testing.T) {
	infos := ColumnInfosFromIR(ir.ByteExactCollationResolver{},
		[]*ir.Column{{Name: "ip", Type: ir.Inet{}}}, false)
	for _, spelling := range []string{"10.0.0.1", "10.0.0.1/32", "10.0.0.0/24"} {
		_, err := Compile("t", "ip = '"+spelling+"'", infos)
		if err == nil {
			t.Errorf("literal %q compiled against a source with no declared network rendering; "+
				"no spelling is knowable, so this must refuse rather than guess", spelling)
			continue
		}
		// Assert the REASON, not just that something refused. Every one of
		// these spellings is a perfectly valid network value, so a bare
		// "not a valid value" would mean the fail-closed branch never ran and
		// the refusal came from the canonicaliser falling through — which is
		// the same message a genuinely malformed literal gets, and would let
		// the branch be deleted with this test still green. (Found by
		// mutation-running this gate; it passed with the branch disabled.)
		if !strings.Contains(err.Error(), "has not declared how its change stream renders network values") {
			t.Errorf("literal %q was refused, but not for the undeclared-rendering reason — the fail-closed "+
				"branch did not run. got: %v", spelling, err)
		}
	}
}
