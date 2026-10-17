// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCollationLoosening_NameMatrix pins the MySQL-family resolver over
// every collation shape the door meets: each axis (case, accent, pad) on
// and off, legacy names with no accent token, MariaDB's uca1400 PAD and
// NO PAD forms, the byte collations, each character type family, and the
// unknowns.
func TestCollationLoosening_NameMatrix(t *testing.T) {
	const (
		c = ir.LooseCase
		a = ir.LooseAccent
		p = ir.LoosePad
	)
	cases := []struct {
		name string
		typ  ir.Type
		want ir.CollationLoosening
		ok   bool
	}{
		{"0900_ai_ci", ir.Varchar{Length: 9, Collation: "utf8mb4_0900_ai_ci"}, c | a, true},
		{"0900_as_ci", ir.Varchar{Length: 9, Collation: "utf8mb4_0900_as_ci"}, c, true},
		{"0900_as_cs", ir.Varchar{Length: 9, Collation: "utf8mb4_0900_as_cs"}, 0, true},
		{"0900_bin", ir.Varchar{Length: 9, Collation: "utf8mb4_0900_bin"}, 0, true},
		{"utf8mb4_bin is PAD SPACE", ir.Varchar{Length: 9, Collation: "utf8mb4_bin"}, p, true},
		{"general_ci legacy folds accents", ir.Varchar{Length: 9, Collation: "utf8mb4_general_ci"}, c | a | p, true},
		{"latin1_swedish_ci", ir.Varchar{Length: 9, Collation: "latin1_swedish_ci"}, c | a | p, true},
		{"latin1_general_cs", ir.Varchar{Length: 9, Collation: "latin1_general_cs"}, p, true},
		{"binary", ir.Varchar{Length: 9, Collation: "binary"}, 0, true},
		{"mariadb uca1400_ai_ci PAD", ir.Varchar{Length: 9, Collation: "utf8mb4_uca1400_ai_ci"}, c | a | p, true},
		{"mariadb uca1400_nopad_ai_ci", ir.Varchar{Length: 9, Collation: "utf8mb4_uca1400_nopad_ai_ci"}, c | a, true},
		{"mariadb uca1400_as_cs PAD", ir.Varchar{Length: 9, Collation: "utf8mb4_uca1400_as_cs"}, p, true},
		{"mariadb nopad_bin", ir.Varchar{Length: 9, Collation: "utf8mb4_nopad_bin"}, 0, true},
		{"mariadb general_nopad_ci", ir.Varchar{Length: 9, Collation: "utf8mb4_general_nopad_ci"}, c | a, true},
		{"upper case name", ir.Varchar{Length: 9, Collation: "UTF8MB4_0900_AS_CI"}, c, true},
		{"CHAR pads under NO PAD", ir.Char{Length: 9, Collation: "utf8mb4_0900_bin"}, p, true},
		{"CHAR ai_ci", ir.Char{Length: 9, Collation: "utf8mb4_0900_ai_ci"}, c | a | p, true},
		{"TEXT general_ci", ir.Text{Size: ir.TextMedium, Collation: "utf8mb4_general_ci"}, c | a | p, true},
		{"TEXT 0900_as_cs", ir.Text{Size: ir.TextMedium, Collation: "utf8mb4_0900_as_cs"}, 0, true},
		{"mariadb weight-level suffix is unknown", ir.Varchar{Length: 9, Collation: "utf8mb4_thai_520_w2"}, 0, false},
		{"kana-sensitive is unknown", ir.Varchar{Length: 9, Collation: "utf8mb4_ja_0900_as_cs_ks"}, 0, false},
		{"a name with no case token is unknown", ir.Varchar{Length: 9, Collation: "utf8mb4_unknown_0900"}, 0, false},
		{"no collation recorded", ir.Varchar{Length: 9}, 0, false},
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
