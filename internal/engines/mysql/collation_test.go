// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"vitess.io/vitess/go/mysql/collations"

	"sluicesync.dev/sluice/internal/ir"
)

// These pins moved here from internal/rowpredicate when the Vitess-backed
// collation comparator was contained behind the [ir.CollationResolver] seam
// (audit 2026-07-18 M2.1 / M-A1): the evaluator is now engine-neutral and this
// engine owns the MySQL collation lens.

// TestResolveCollation pins that real MySQL collation names resolve and
// garbage does not (the loud-refusal floor: an unresolvable collation must NOT
// silently fall through to a byte compare).
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
	mustID := func(name string) (id collations.ID) {
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

// TestMySQLCollationResolver_Policies pins the engine resolver's classification
// of the string-comparison policy per collation family: byte-exact (_bin/_cs),
// collation-fold (ci/ai), PAD SPACE trim, and the loud-refusal fences (empty /
// non-UTF-8 charset / strict mode). This is the [ir.StringEquality] contract the
// engine-neutral evaluator consumes.
func TestMySQLCollationResolver_Policies(t *testing.T) {
	r := mysqlCollationResolver{}
	det := ir.CollationDeterminismUnknown

	// Byte-exact, PAD SPACE (_bin): faithful, no comparator, pad-space on.
	if eq := r.ResolveStringEquality("utf8mb4_bin", det, false, false); !eq.Faithful || eq.Compare != nil || !eq.PadSpace {
		t.Errorf("utf8mb4_bin: got %+v; want faithful byte-exact PAD SPACE", eq)
	}
	// UCA case+accent-sensitive (_as_cs 0900): NOT byte-exact — MySQL's `=`
	// folds canonical equivalence (NFC/NFD) and UCA-ignorables, so it routes
	// through the Vitess FOLD comparator (faithful, Compare != nil), NO PAD
	// (audit 2026-07-19 A1). Only `_bin`/binary is byte-exact.
	if eq := r.ResolveStringEquality("utf8mb4_0900_as_cs", det, false, false); !eq.Faithful || eq.Compare == nil || eq.PadSpace {
		t.Errorf("utf8mb4_0900_as_cs: got %+v; want faithful FOLD NO PAD (audit A1)", eq)
	}
	// ci fold, PAD SPACE: faithful WITH comparator, pad-space on.
	if eq := r.ResolveStringEquality("utf8mb4_general_ci", det, false, false); !eq.Faithful || eq.Compare == nil || !eq.PadSpace {
		t.Errorf("utf8mb4_general_ci: got %+v; want faithful fold PAD SPACE", eq)
	}
	// ci fold, NO PAD (0900): faithful WITH comparator, pad-space off.
	if eq := r.ResolveStringEquality("utf8mb4_0900_ai_ci", det, false, false); !eq.Faithful || eq.Compare == nil || eq.PadSpace {
		t.Errorf("utf8mb4_0900_ai_ci: got %+v; want faithful fold NO PAD", eq)
	}
	// Strict mode refuses the fold path (ci/ai AND the UCA _as_cs) but keeps
	// byte-exact.
	if eq := r.ResolveStringEquality("utf8mb4_0900_ai_ci", det, true, false); eq.Faithful {
		t.Errorf("utf8mb4_0900_ai_ci under strict: got %+v; want refuse", eq)
	}
	if eq := r.ResolveStringEquality("utf8mb4_0900_as_cs", det, true, false); eq.Faithful {
		t.Errorf("utf8mb4_0900_as_cs under strict: got %+v; want refuse (UCA fold, not byte-exact)", eq)
	}
	if eq := r.ResolveStringEquality("utf8mb4_bin", det, true, false); !eq.Faithful || eq.Compare != nil {
		t.Errorf("utf8mb4_bin under strict: got %+v; want still byte-exact", eq)
	}
	// fixedChar is IGNORED on MySQL: a CHAR shares its collation's PAD_ATTRIBUTE
	// with VARCHAR (Postgres bpchar's collation-independent pad is a PG-only
	// override, not applied here), so CHAR and VARCHAR give identical policy.
	if eq := r.ResolveStringEquality("utf8mb4_bin", det, false, true); !eq.Faithful || eq.Compare != nil || !eq.PadSpace {
		t.Errorf("utf8mb4_bin as CHAR (fixedChar): got %+v; want identical to VARCHAR (byte-exact PAD SPACE)", eq)
	}
	// Refusal fences: empty + non-UTF-8 charset.
	for _, coll := range []string{"", "latin1_swedish_ci", "gbk_chinese_ci"} {
		if eq := r.ResolveStringEquality(coll, det, false, false); eq.Faithful {
			t.Errorf("%q: got faithful %+v; want refuse (empty/non-UTF-8 charset)", coll, eq)
		}
	}
	// The comparator actually folds (spot check the ci path is wired).
	eq := r.ResolveStringEquality("utf8mb4_0900_ai_ci", det, false, false)
	if !eq.Compare("EU", "eu") || eq.Compare("EU", "US") {
		t.Error("ci comparator did not fold case as expected")
	}
}
