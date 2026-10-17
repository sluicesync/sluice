// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCollationLoosening_Determinism pins the Postgres resolver over each
// character type family × each determinism: a nondeterministic collation
// reports case and accent loosening, bpchar always reports pad, and an
// unestablished determinism is not classified.
func TestCollationLoosening_Determinism(t *testing.T) {
	const (
		c = ir.LooseCase
		a = ir.LooseAccent
		p = ir.LoosePad
	)
	det, non := ir.CollationDeterministic, ir.CollationNonDeterministic
	cases := []struct {
		name string
		typ  ir.Type
		want ir.CollationLoosening
		ok   bool
	}{
		{"varchar deterministic", ir.Varchar{Length: 9, Determinism: det}, 0, true},
		{"varchar nondeterministic", ir.Varchar{Length: 9, Collation: "ci", Determinism: non}, c | a, true},
		{"text deterministic", ir.Text{Size: ir.TextLong, Determinism: det}, 0, true},
		{"text nondeterministic", ir.Text{Size: ir.TextLong, Collation: "ci", Determinism: non}, c | a, true},
		{"bpchar deterministic", ir.Char{Length: 9, Determinism: det}, p, true},
		{"bpchar nondeterministic", ir.Char{Length: 9, Collation: "ci", Determinism: non}, c | a | p, true},
		{"determinism unknown", ir.Varchar{Length: 9}, 0, false},
		{"bpchar determinism unknown", ir.Char{Length: 9}, 0, false},
		{"citext extension type", ir.ExtensionType{Extension: "citext", Name: "citext"}, c, true},
		{"citext verbatim, schema-qualified", ir.VerbatimType{Definition: "public.citext"}, c, true},
		{"another verbatim type", ir.VerbatimType{Definition: "ltree"}, 0, false},
		{"citext array", ir.Array{Element: ir.ExtensionType{Extension: "citext", Name: "citext"}}, c, true},
		{"not a character type", ir.Integer{Width: 32}, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := Engine{}.CollationLoosening(tc.typ)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("CollationLoosening(%v) = %03b, %v; want %03b, %v", tc.typ, got, ok, tc.want, tc.ok)
			}
		})
	}
}
