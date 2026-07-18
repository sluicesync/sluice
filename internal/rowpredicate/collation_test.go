// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// These tests drive the collation-driven compile + eval END TO END through the
// engine's [ir.CollationResolver] (audit M2.1 — the Vitess comparator now lives
// in internal/engines/mysql; the resolver-internal pins moved there too). The
// oracle for the case/accent fold is the ground-truth real-MySQL family matrix
// (collation_realmysql_integration_test.go), not these unit pins.

// TestFaithfulCICompileEval is the end-to-end proof of ADR-0174 Piece 1: a
// `region = 'EU'` predicate on a case-insensitive column matches rows whose
// value differs only by case/accent — classified exactly as the source's
// own `WHERE region = 'EU'` would — instead of being refused.
func TestFaithfulCICompileEval(t *testing.T) {
	infos := ColumnInfosFromIR(testMySQLResolver, []*ir.Column{
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
	strictInfos := ColumnInfosFromIR(testMySQLResolver, []*ir.Column{
		{Name: "region", Type: ir.Varchar{Collation: "utf8mb4_0900_ai_ci"}},
	}, true)
	if _, err := Compile("t", "region = 'EU'", strictInfos); err == nil {
		t.Error("strict mode: Compile should refuse a ci-collation string comparison")
	}
}

// TestPadSpaceFix_F01 pins the audit-F0-1 fix: a PAD SPACE collation's `=`
// ignores TRAILING spaces, so region='EU' must match a stored 'EU '/'EU  ' —
// while a NO-PAD collation must NOT. Reproduces the exact Critical scenario.
func TestPadSpaceFix_F01(t *testing.T) {
	mk := func(coll string) *Predicate {
		t.Helper()
		infos := ColumnInfosFromIR(testMySQLResolver, []*ir.Column{{Name: "region", Type: ir.Varchar{Collation: coll}}}, false)
		p, err := Compile("t", "region = 'EU'", infos)
		if err != nil {
			t.Fatalf("Compile refused %s: %v", coll, err)
		}
		return p
	}
	cases := []struct {
		coll string
		val  string
		want bool
	}{
		// PAD SPACE ci: trailing-space AND case fold both match (= real MySQL).
		{"utf8mb4_general_ci", "EU", true},
		{"utf8mb4_general_ci", "EU ", true},
		{"utf8mb4_general_ci", "EU  ", true},
		{"utf8mb4_general_ci", "eu", true},
		{"utf8mb4_general_ci", "Eu", true},
		{"utf8mb4_general_ci", "US", false},
		{"utf8mb4_general_ci", "EUX", false},
		// PAD SPACE byte-exact (_bin): trailing-space matches, case does NOT.
		{"utf8mb4_bin", "EU", true},
		{"utf8mb4_bin", "EU ", true},
		{"utf8mb4_bin", "eu", false},
		// NO PAD (0900): case folds, trailing space does NOT match.
		{"utf8mb4_0900_ai_ci", "EU", true},
		{"utf8mb4_0900_ai_ci", "eu", true},
		{"utf8mb4_0900_ai_ci", "EU ", false},
	}
	for _, tc := range cases {
		if got := mk(tc.coll).Eval(ir.Row{"region": tc.val}); got != tc.want {
			t.Errorf("%s region=%q: Eval=%v want %v", tc.coll, tc.val, got, tc.want)
		}
	}
}

// TestCollationRefusals_F03_F06 pins the loud-refusal fences: a non-UTF-8
// charset (F0-6) and a Postgres NAMED (possibly non-deterministic) collation
// (F0-3) refuse rather than silently compare wrongly; the PG default works.
func TestCollationRefusals_F03_F06(t *testing.T) {
	refuse := func(resolver ir.CollationResolver, coll string) {
		t.Helper()
		infos := ColumnInfosFromIR(resolver, []*ir.Column{{Name: "c", Type: ir.Varchar{Collation: coll}}}, false)
		if _, err := Compile("t", "c = 'x'", infos); err == nil {
			t.Errorf("resolver=%T coll=%q: expected a loud refusal, got nil", resolver, coll)
		}
	}
	refuse(testMySQLResolver, "latin1_swedish_ci") // F0-6 non-UTF-8 charset
	refuse(testMySQLResolver, "gbk_chinese_ci")    // F0-6 non-UTF-8 charset
	refuse(testMySQLResolver, "")                  // unknown collation on MySQL
	refuse(testPGResolver, "nd_icu")               // F0-3 PG named collation (can't prove deterministic)
	// PG default (empty) is deterministic -> byte-exact, allowed.
	infos := ColumnInfosFromIR(testPGResolver, []*ir.Column{{Name: "c", Type: ir.Varchar{}}}, false)
	if _, err := Compile("t", "c = 'x'", infos); err != nil {
		t.Errorf("postgres default collation should compile: %v", err)
	}
}
