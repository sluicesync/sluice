// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestResolveCollation pins that real MySQL collation names resolve and
// garbage does not (the loud-refusal floor: an unresolvable collation must
// NOT silently fall through to a byte compare).
func TestResolveCollation(t *testing.T) {
	for _, name := range []string{
		"utf8mb4_0900_ai_ci", "utf8mb4_general_ci", "utf8mb4_bin",
		"utf8_general_ci", "latin1_swedish_ci",
	} {
		if _, ok := resolveCollation(name); !ok {
			t.Errorf("resolveCollation(%q) = not-ok; want a resolved collation", name)
		}
	}
	for _, name := range []string{"", "not_a_collation", "utf8mb4_made_up"} {
		if id, ok := resolveCollation(name); ok {
			t.Errorf("resolveCollation(%q) = (%d, ok); want not-ok so the caller refuses", name, id)
		}
	}
}

// TestCollationEqual pins that collationEqual reproduces MySQL's own `=` —
// case-insensitivity, accent-insensitivity, and the case-sensitive _bin
// baseline — using Vitess's comparator. This is the fidelity the ADR-0174
// design rests on: the client-side classification equals the source's.
func TestCollationEqual(t *testing.T) {
	mustID := func(name string) (id collationID) {
		t.Helper()
		got, ok := resolveCollation(name)
		if !ok {
			t.Fatalf("collation %q did not resolve", name)
		}
		return got
	}
	cases := []struct {
		name      string
		collation string
		a, b      string
		want      bool
	}{
		// utf8mb4_0900_ai_ci — case AND accent insensitive (MySQL 8 default).
		{"0900 case-fold", "utf8mb4_0900_ai_ci", "EU", "eu", true},
		{"0900 mixed case", "utf8mb4_0900_ai_ci", "Eu", "eU", true},
		{"0900 accent-fold", "utf8mb4_0900_ai_ci", "cafe", "café", true},
		{"0900 distinct", "utf8mb4_0900_ai_ci", "EU", "US", false},
		// utf8mb4_general_ci — case insensitive.
		{"general case-fold", "utf8mb4_general_ci", "EU", "eu", true},
		{"general distinct", "utf8mb4_general_ci", "EU", "US", false},
		// utf8mb4_bin — binary: case-SENSITIVE (the byte-exact baseline).
		{"bin case-distinct", "utf8mb4_bin", "EU", "eu", false},
		{"bin exact", "utf8mb4_bin", "EU", "EU", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := collationEqual(tc.a, tc.b, mustID(tc.collation)); got != tc.want {
				t.Errorf("collationEqual(%q,%q,%s) = %v; want %v", tc.a, tc.b, tc.collation, got, tc.want)
			}
		})
	}
}

// TestFaithfulCICompileEval is the end-to-end proof of ADR-0174 Piece 1: a
// `region = 'EU'` predicate on a case-insensitive column matches rows whose
// value differs only by case/accent — classified exactly as the source's
// own `WHERE region = 'EU'` would — instead of being refused.
func TestFaithfulCICompileEval(t *testing.T) {
	infos := ColumnInfosFromIR("mysql", []*ir.Column{
		{Name: "region", Type: ir.Varchar{Collation: "utf8mb4_0900_ai_ci"}},
	}, false)
	p, err := Compile("t", "region = 'EU'", infos)
	if err != nil {
		t.Fatalf("Compile refused a faithful ci comparison: %v", err)
	}
	for _, tc := range []struct {
		val  string
		want bool
	}{
		{"EU", true}, {"eu", true}, {"Eu", true}, {"US", false}, {"eux", false},
	} {
		if got := p.Eval(ir.Row{"region": tc.val}); got != tc.want {
			t.Errorf("region=%q: Eval = %v; want %v", tc.val, got, tc.want)
		}
	}

	// Strict mode refuses the same predicate.
	strictInfos := ColumnInfosFromIR("mysql", []*ir.Column{
		{Name: "region", Type: ir.Varchar{Collation: "utf8mb4_0900_ai_ci"}},
	}, true)
	if _, err := Compile("t", "region = 'EU'", strictInfos); err == nil {
		t.Error("strict mode: Compile should refuse a ci-collation string comparison")
	}
}
