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
			name:      "inet",
			colType:   ir.Inet{},
			canonical: "10.0.0.1",
			divergent: []string{
				"010.000.000.001", // zero-padded octets
			},
		},
		{
			name:      "cidr",
			colType:   ir.Cidr{},
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
			infos := ColumnInfosFromIR(ir.ByteExactCollationResolver{},
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
		infos := ColumnInfosFromIR(ir.ByteExactCollationResolver{},
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
	}{
		{identifierUUID, "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
		{identifierUUID, "a0eebc999c0b4ef8bb6d6bb9bd380a11", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
		{identifierNetwork, "010.000.000.001", "10.0.0.1"},
		{identifierNetwork, "10.0.0.0/24", "10.0.0.0/24"},
		{identifierNetwork, "2001:0db8:0000:0000:0000:0000:0000:0001", "2001:db8::1"},
		{identifierMAC, "08-00-2B-01-02-03", "08:00:2b:01:02:03"},
		{identifierMAC, "0800.2b01.0203", "08:00:2b:01:02:03"},
		{identifierTime, "08:30:00", "08:30:00"},
	}
	for _, tc := range cases {
		got, ok := canonicalIdentifierLiteral(tc.kind, tc.in)
		if !ok || got != tc.want {
			t.Errorf("canonicalIdentifierLiteral(%v, %q) = %q, %v; want %q, true", tc.kind, tc.in, got, ok, tc.want)
		}
	}
}
