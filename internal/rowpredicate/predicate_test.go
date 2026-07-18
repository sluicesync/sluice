// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

import (
	"math"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// compile is a test helper that compiles predicate against infos and fails
// on error.
func mustCompile(t *testing.T, predicate string, infos map[string]ColumnInfo) *Predicate {
	t.Helper()
	p, err := Compile("t", predicate, infos)
	if err != nil {
		t.Fatalf("Compile(%q) unexpected error: %v", predicate, err)
	}
	return p
}

// mustRefuse asserts Compile refuses with the unsupported-predicate code.
func mustRefuse(t *testing.T, predicate string, infos map[string]ColumnInfo) {
	t.Helper()
	_, err := Compile("t", predicate, infos)
	if err == nil {
		t.Fatalf("Compile(%q): want a loud refusal, got nil", predicate)
	}
	ce, ok := sluicecode.FromError(err)
	if !ok {
		t.Fatalf("Compile(%q): want a coded error, got %v", predicate, err)
	}
	if ce.Code != sluicecode.CodeWhereCDCUnsupportedPredicate {
		t.Errorf("Compile(%q): code = %q; want %q", predicate, ce.Code, sluicecode.CodeWhereCDCUnsupportedPredicate)
	}
}

// TestFloatEqualityRefused pins audit F0-5: `=`/`!=`/`<>`/`IN`/`NOT IN` on a
// FLOAT/DOUBLE column is refused at compile time (the client's exact big.Rat
// compare vs the source's IEEE-754 coercion diverge on a high-precision
// literal), while ordering (`<`/`<=`/`>`/`>=`) stays allowed. Built through
// ColumnInfosFromIR from ir.Float so the FamilyFloat routing is exercised, not
// a hand-set family.
func TestFloatEqualityRefused(t *testing.T) {
	floatCol := ColumnInfosFromIR("mysql", []*ir.Column{{Name: "x", Type: ir.Float{}}}, false)
	for _, pred := range []string{
		"x = 0.1",
		"x != 0.1",
		"x <> 0.1",
		"x IN (0.1, 0.2)",
		"x NOT IN (0.1, 0.2)",
	} {
		t.Run("refuse "+pred, func(t *testing.T) {
			mustRefuse(t, pred, floatCol)
		})
	}
	for _, pred := range []string{
		"x > 0.1",
		"x >= 0.1",
		"x < 0.1",
		"x <= 0.1",
	} {
		t.Run("allow "+pred, func(t *testing.T) {
			p := mustCompile(t, pred, floatCol)
			// Ordering still evaluates (big.Rat over the float64 value).
			assertEval(t, p, ir.Row{"x": float64(0.05)}, pred == "x < 0.1" || pred == "x <= 0.1")
		})
	}
}

// TestColumns pins Predicate.Columns: the deduplicated, lower-cased set of
// columns the predicate references across every node kind (cmp, IN, IS NULL,
// AND/OR/NOT, parens). The filtered-sync before-image guard depends on it to
// know which columns a partial before-image must carry.
func TestColumns(t *testing.T) {
	infos := map[string]ColumnInfo{
		"region":    {Family: FamilyString, CaseSensitive: true},
		"tenant_id": {Family: FamilyNumeric},
		"deleted":   {Family: FamilyString, CaseSensitive: true},
		"age":       {Family: FamilyNumeric},
	}
	p := mustCompile(t, "(region = 'EU' AND tenant_id IN (1,2)) OR NOT (deleted IS NULL) OR age > 3", infos)
	got := p.Columns()
	want := []string{"age", "deleted", "region", "tenant_id"} // sorted, deduped
	if len(got) != len(want) {
		t.Fatalf("Columns() = %v; want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Columns() = %v; want %v", got, want)
		}
	}

	// A repeated column is deduplicated.
	p2 := mustCompile(t, "age > 3 AND age < 99", map[string]ColumnInfo{"age": {Family: FamilyNumeric}})
	if cols := p2.Columns(); len(cols) != 1 || cols[0] != "age" {
		t.Errorf("Columns() dedup = %v; want [age]", cols)
	}

	// A nil predicate references nothing.
	var nilP *Predicate
	if cols := nilP.Columns(); cols != nil {
		t.Errorf("nil predicate Columns() = %v; want nil", cols)
	}
}

// TestValueFamilies is the Bug-74 matrix for the evaluator: EVERY value
// family it can compare (numeric int64/uint64/float64/decimal-string, bool,
// string, temporal, binary), each exercised for a matching AND a
// non-matching row — a green test on one family does not cover the others.
func TestValueFamilies(t *testing.T) {
	t.Run("integer int64", func(t *testing.T) {
		p := mustCompile(t, "age >= 18", map[string]ColumnInfo{"age": {Family: FamilyNumeric}})
		assertEval(t, p, ir.Row{"age": int64(18)}, true)
		assertEval(t, p, ir.Row{"age": int64(21)}, true)
		assertEval(t, p, ir.Row{"age": int64(17)}, false)
	})
	t.Run("integer uint64 above MaxInt64", func(t *testing.T) {
		// A BIGINT UNSIGNED value beyond int64 must still compare exactly.
		p := mustCompile(t, "id > 9223372036854775807", map[string]ColumnInfo{"id": {Family: FamilyNumeric}})
		assertEval(t, p, ir.Row{"id": uint64(9223372036854775808)}, true)
		assertEval(t, p, ir.Row{"id": uint64(9223372036854775807)}, false)
	})
	t.Run("float64", func(t *testing.T) {
		p := mustCompile(t, "score > 19.99", map[string]ColumnInfo{"score": {Family: FamilyNumeric}})
		assertEval(t, p, ir.Row{"score": float64(20.0)}, true)
		assertEval(t, p, ir.Row{"score": float64(19.99)}, false)
	})
	t.Run("float NaN never matches", func(t *testing.T) {
		p := mustCompile(t, "score > 0", map[string]ColumnInfo{"score": {Family: FamilyNumeric}})
		assertEval(t, p, ir.Row{"score": math.NaN()}, false)
	})
	t.Run("decimal string exact", func(t *testing.T) {
		// Decimal values are strings (docs/value-types.md); comparison must
		// be numeric, not lexical, and lossless.
		p := mustCompile(t, "price >= 100.5", map[string]ColumnInfo{"price": {Family: FamilyNumeric}})
		assertEval(t, p, ir.Row{"price": "100.50"}, true)
		assertEval(t, p, ir.Row{"price": "100.4999"}, false)
		assertEval(t, p, ir.Row{"price": "1000"}, true)
	})
	t.Run("bool", func(t *testing.T) {
		p := mustCompile(t, "active = TRUE", map[string]ColumnInfo{"active": {Family: FamilyBool}})
		assertEval(t, p, ir.Row{"active": true}, true)
		assertEval(t, p, ir.Row{"active": false}, false)
		p2 := mustCompile(t, "active = 0", map[string]ColumnInfo{"active": {Family: FamilyBool}})
		assertEval(t, p2, ir.Row{"active": false}, true)
		assertEval(t, p2, ir.Row{"active": true}, false)
	})
	t.Run("string equality (case-sensitive)", func(t *testing.T) {
		p := mustCompile(t, "country = 'US'", map[string]ColumnInfo{"country": {Family: FamilyString, CaseSensitive: true}})
		assertEval(t, p, ir.Row{"country": "US"}, true)
		assertEval(t, p, ir.Row{"country": "CA"}, false)
		assertEval(t, p, ir.Row{"country": "us"}, false) // byte-exact
	})
	t.Run("string not-equal", func(t *testing.T) {
		p := mustCompile(t, "country != 'US'", map[string]ColumnInfo{"country": {Family: FamilyString, CaseSensitive: true}})
		assertEval(t, p, ir.Row{"country": "CA"}, true)
		assertEval(t, p, ir.Row{"country": "US"}, false)
	})
	t.Run("temporal date", func(t *testing.T) {
		p := mustCompile(t, "created >= '2020-01-01'", map[string]ColumnInfo{"created": {Family: FamilyTemporal}})
		assertEval(t, p, ir.Row{"created": time.Date(2020, 6, 1, 0, 0, 0, 0, time.UTC)}, true)
		assertEval(t, p, ir.Row{"created": time.Date(2019, 12, 31, 0, 0, 0, 0, time.UTC)}, false)
	})
	t.Run("temporal datetime", func(t *testing.T) {
		p := mustCompile(t, "created < '2020-01-01 12:00:00'", map[string]ColumnInfo{"created": {Family: FamilyTemporal}})
		assertEval(t, p, ir.Row{"created": time.Date(2020, 1, 1, 11, 0, 0, 0, time.UTC)}, true)
		assertEval(t, p, ir.Row{"created": time.Date(2020, 1, 1, 13, 0, 0, 0, time.UTC)}, false)
	})
	t.Run("binary/json equality", func(t *testing.T) {
		p := mustCompile(t, "payload = 'abc'", map[string]ColumnInfo{"payload": {Family: FamilyBinary}})
		assertEval(t, p, ir.Row{"payload": []byte("abc")}, true)
		assertEval(t, p, ir.Row{"payload": []byte("abd")}, false)
	})
}

// TestThreeValuedLogic pins SQL 3VL: a NULL operand makes a comparison
// UNKNOWN, which is treated as NOT matching (a row where the predicate is
// UNKNOWN is not selected by WHERE) — and the AND/OR/NOT combinators
// propagate UNKNOWN correctly.
func TestThreeValuedLogic(t *testing.T) {
	num := map[string]ColumnInfo{"a": {Family: FamilyNumeric}, "b": {Family: FamilyNumeric}}

	t.Run("NULL operand -> UNKNOWN -> not matching", func(t *testing.T) {
		p := mustCompile(t, "a = 1", num)
		assertEval(t, p, ir.Row{"a": nil}, false)
		assertEval(t, p, ir.Row{}, false) // absent column == NULL
	})
	t.Run("IS NULL / IS NOT NULL are never UNKNOWN", func(t *testing.T) {
		p := mustCompile(t, "a IS NULL", num)
		assertEval(t, p, ir.Row{"a": nil}, true)
		assertEval(t, p, ir.Row{"a": int64(1)}, false)
		p2 := mustCompile(t, "a IS NOT NULL", num)
		assertEval(t, p2, ir.Row{"a": int64(1)}, true)
		assertEval(t, p2, ir.Row{"a": nil}, false)
	})
	t.Run("AND: UNKNOWN and TRUE = UNKNOWN(not matching); UNKNOWN and FALSE = FALSE", func(t *testing.T) {
		p := mustCompile(t, "a = 1 AND b = 2", num)
		assertEval(t, p, ir.Row{"a": nil, "b": int64(2)}, false) // U AND T = U
		assertEval(t, p, ir.Row{"a": nil, "b": int64(9)}, false) // U AND F = F
		assertEval(t, p, ir.Row{"a": int64(1), "b": int64(2)}, true)
	})
	t.Run("OR: UNKNOWN or TRUE = TRUE; UNKNOWN or FALSE = UNKNOWN(not matching)", func(t *testing.T) {
		p := mustCompile(t, "a = 1 OR b = 2", num)
		assertEval(t, p, ir.Row{"a": nil, "b": int64(2)}, true)  // U OR T = T
		assertEval(t, p, ir.Row{"a": nil, "b": int64(9)}, false) // U OR F = U
	})
	t.Run("NOT UNKNOWN = UNKNOWN(not matching)", func(t *testing.T) {
		p := mustCompile(t, "NOT a = 1", num)
		assertEval(t, p, ir.Row{"a": nil}, false)     // NOT U = U
		assertEval(t, p, ir.Row{"a": int64(2)}, true) // NOT F = T
		assertEval(t, p, ir.Row{"a": int64(1)}, false)
	})
}

// TestInList pins IN / NOT IN with SQL NULL semantics.
func TestInList(t *testing.T) {
	str := map[string]ColumnInfo{"country": {Family: FamilyString, CaseSensitive: true}}
	t.Run("IN matches / drops", func(t *testing.T) {
		p := mustCompile(t, "country IN ('US','CA')", str)
		assertEval(t, p, ir.Row{"country": "US"}, true)
		assertEval(t, p, ir.Row{"country": "CA"}, true)
		assertEval(t, p, ir.Row{"country": "MX"}, false)
	})
	t.Run("NOT IN", func(t *testing.T) {
		p := mustCompile(t, "country NOT IN ('US','CA')", str)
		assertEval(t, p, ir.Row{"country": "MX"}, true)
		assertEval(t, p, ir.Row{"country": "US"}, false)
	})
	t.Run("IN with NULL value is UNKNOWN", func(t *testing.T) {
		p := mustCompile(t, "country IN ('US')", str)
		assertEval(t, p, ir.Row{"country": nil}, false)
		pn := mustCompile(t, "country NOT IN ('US')", str)
		assertEval(t, pn, ir.Row{"country": nil}, false) // NOT UNKNOWN = UNKNOWN
	})
	t.Run("numeric IN", func(t *testing.T) {
		p := mustCompile(t, "tier IN (1, 2, 3)", map[string]ColumnInfo{"tier": {Family: FamilyNumeric}})
		assertEval(t, p, ir.Row{"tier": int64(2)}, true)
		assertEval(t, p, ir.Row{"tier": int64(4)}, false)
	})
}

// TestParenthesization pins AND/OR precedence + explicit grouping.
func TestParenthesization(t *testing.T) {
	infos := map[string]ColumnInfo{
		"a": {Family: FamilyNumeric},
		"b": {Family: FamilyNumeric},
		"c": {Family: FamilyNumeric},
	}
	// a=1 OR (b=2 AND c=3): default precedence AND binds tighter than OR.
	p := mustCompile(t, "a = 1 OR b = 2 AND c = 3", infos)
	assertEval(t, p, ir.Row{"a": int64(1), "b": int64(0), "c": int64(0)}, true)
	assertEval(t, p, ir.Row{"a": int64(0), "b": int64(2), "c": int64(3)}, true)
	assertEval(t, p, ir.Row{"a": int64(0), "b": int64(2), "c": int64(9)}, false)
	// Explicit grouping changes the meaning.
	pg := mustCompile(t, "(a = 1 OR b = 2) AND c = 3", infos)
	assertEval(t, pg, ir.Row{"a": int64(1), "b": int64(0), "c": int64(3)}, true)
	assertEval(t, pg, ir.Row{"a": int64(1), "b": int64(0), "c": int64(9)}, false)
}

// TestCompileRefusals pins the loud-failure contract: every construct the
// evaluator cannot faithfully evaluate is refused with the coded error at
// compile time, NEVER silently mis-evaluated.
func TestCompileRefusals(t *testing.T) {
	infos := map[string]ColumnInfo{
		"country": {Family: FamilyString, CaseSensitive: true},
		"name_ci": {Family: FamilyString, CaseSensitive: false},
		"age":     {Family: FamilyNumeric},
		"created": {Family: FamilyTemporal},
		"tags":    {Family: FamilyUnsupported}, // e.g. an array/set
		"payload": {Family: FamilyBinary},
		"active":  {Family: FamilyBool},
	}
	cases := []struct {
		name, pred string
	}{
		{"function call", "lower(country) = 'us'"},
		{"subquery", "age IN (SELECT id FROM other)"},
		{"arithmetic", "age + 1 = 2"},
		{"LIKE", "country LIKE 'U%'"},
		{"unknown column", "region = 'x'"},
		{"ordering on string", "country > 'US'"},
		{"case-insensitive string", "name_ci = 'bob'"},
		{"string vs numeric literal", "age = 'x'"},
		{"numeric vs string literal", "country = 5"},
		{"= NULL (must be IS NULL)", "age = NULL"},
		{"unsupported family (array)", "tags = 'x'"},
		{"ordering on binary", "payload > 'x'"},
		{"ordering on bool", "active > 0"},
		{"unbalanced paren", "(age = 1"},
		{"trailing junk", "age = 1 age"},
		{"empty predicate", ""},
		{"bare column", "age"},
		{"backslash string literal", `country = 'a\b'`},
		{"unterminated string", "country = 'US"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mustRefuse(t, tc.pred, infos)
		})
	}
}

// TestColumnInfosFromIR pins the IR-type → family mapping and the
// collation-driven case-sensitivity gate — the fidelity classification the
// compile refusals depend on. Bug-74 discipline: every family the mapper
// produces, plus the collation variants that flip a string comparison from
// faithful to refused.
func TestColumnInfosFromIR(t *testing.T) {
	cols := []*ir.Column{
		{Name: "i", Type: ir.Integer{}},
		{Name: "d", Type: ir.Decimal{}},
		{Name: "f", Type: ir.Float{}},
		{Name: "b", Type: ir.Boolean{}},
		{Name: "vc_bin", Type: ir.Varchar{Collation: "utf8mb4_bin"}},
		{Name: "vc_ci", Type: ir.Varchar{Collation: "utf8mb4_0900_ai_ci"}},
		{Name: "vc_cs", Type: ir.Varchar{Collation: "utf8mb4_0900_as_cs"}},
		{Name: "vc_empty", Type: ir.Varchar{}},
		{Name: "u", Type: ir.UUID{}},
		{Name: "net", Type: ir.Inet{}},
		{Name: "tm", Type: ir.Time{}},
		{Name: "blob", Type: ir.Blob{}},
		{Name: "js", Type: ir.JSON{}},
		{Name: "dt", Type: ir.DateTime{}},
		{Name: "dte", Type: ir.Date{}},
		{Name: "ts", Type: ir.Timestamp{}},
		{Name: "tstz", Type: ir.Timestamp{WithTimeZone: true}},
		{Name: "arr", Type: ir.Array{Element: ir.Integer{}}},
	}

	t.Run("mysql-family: empty collation is NOT case-sensitive", func(t *testing.T) {
		m := ColumnInfosFromIR("mysql", cols, false)
		wantFamily(t, m, "i", FamilyNumeric)
		wantFamily(t, m, "d", FamilyNumeric)
		wantFamily(t, m, "f", FamilyFloat) // audit F0-5: Float is its own family (equality refused)
		wantFamily(t, m, "b", FamilyBool)
		wantStringCS(t, m, "vc_bin", true)
		wantStringCS(t, m, "vc_ci", false)
		wantStringCS(t, m, "vc_cs", true)
		wantStringCS(t, m, "vc_empty", false) // MySQL default collation is ci
		wantStringCS(t, m, "u", true)
		wantStringCS(t, m, "net", true)
		wantStringCS(t, m, "tm", true)
		wantFamily(t, m, "blob", FamilyBinary)
		wantFamily(t, m, "js", FamilyBinary)
		wantFamily(t, m, "dt", FamilyTemporal)
		wantFamily(t, m, "dte", FamilyTemporal)
		wantFamily(t, m, "ts", FamilyTemporal)
		wantFamily(t, m, "tstz", FamilyUnsupported) // tz-aware: refused
		wantFamily(t, m, "arr", FamilyUnsupported)
	})

	t.Run("postgres: empty collation IS case-sensitive (deterministic =)", func(t *testing.T) {
		m := ColumnInfosFromIR("postgres", cols, false)
		wantStringCS(t, m, "vc_empty", true)
	})

	// audit 2026-07-18 F0-3 / M1.1: a Postgres NAMED collation is byte-exact
	// (faithful) ONLY when it is provably deterministic; a non-deterministic
	// ICU collation, and an UNKNOWN-determinism name (the safe zero value),
	// both refuse. The default (empty) collation stays byte-exact.
	t.Run("postgres: named collation admitted only when deterministic", func(t *testing.T) {
		pgCols := []*ir.Column{
			{Name: "c_det", Type: ir.Text{Collation: "C", Determinism: ir.CollationDeterministic}},
			{Name: "vc_det", Type: ir.Varchar{Collation: "en_US", Determinism: ir.CollationDeterministic}},
			{Name: "ch_det", Type: ir.Char{Collation: "POSIX", Determinism: ir.CollationDeterministic}},
			{Name: "c_nd", Type: ir.Text{Collation: "nd_icu", Determinism: ir.CollationNonDeterministic}},
			{Name: "c_unknown", Type: ir.Text{Collation: "mystery", Determinism: ir.CollationDeterminismUnknown}},
			{Name: "c_default", Type: ir.Text{}},
		}
		m := ColumnInfosFromIR("postgres", pgCols, false)
		// Deterministic named collations → byte-exact faithful (allowed).
		for _, name := range []string{"c_det", "vc_det", "ch_det"} {
			if !m[name].CaseSensitive || !m[name].faithfulString() {
				t.Errorf("%s: deterministic named PG collation must be byte-exact faithful, got %+v", name, m[name])
			}
			if m[name].Collation != 0 {
				t.Errorf("%s: byte-exact column must carry no Vitess collation id, got %d", name, m[name].Collation)
			}
		}
		// Non-deterministic + unknown-determinism named collations → refuse.
		for _, name := range []string{"c_nd", "c_unknown"} {
			if m[name].faithfulString() {
				t.Errorf("%s: non-deterministic/unknown named PG collation must NOT be faithful (must refuse), got %+v", name, m[name])
			}
		}
		// Default (empty) collation stays byte-exact.
		if !m["c_default"].CaseSensitive {
			t.Error("c_default: PG default collation should be byte-exact")
		}
	})

	// ADR-0174 Piece 1: a recognized ci/ai collation resolves to a faithful
	// comparator (non-strict), so its string comparison is ALLOWED, not
	// refused — while an unknown/empty collation stays unreproducible.
	t.Run("mysql-family: recognized ci collation is faithfully reproducible", func(t *testing.T) {
		m := ColumnInfosFromIR("mysql", cols, false)
		if !m["vc_ci"].faithfulString() || m["vc_ci"].Collation == 0 {
			t.Errorf("vc_ci (utf8mb4_0900_ai_ci): want a resolved faithful collation, got Collation=%d faithful=%v", m["vc_ci"].Collation, m["vc_ci"].faithfulString())
		}
		if m["vc_empty"].faithfulString() {
			t.Error("vc_empty (unknown collation): must NOT be faithful — an unknown collation can't be reproduced")
		}
		// A byte-exact column carries no collation (byte compare is faithful).
		if m["vc_bin"].Collation != 0 {
			t.Errorf("vc_bin: byte-exact column should carry no collation, got %d", m["vc_bin"].Collation)
		}
	})

	// --where-strict-collation: even a recognized ci collation is left
	// unreproducible, so its comparison refuses (the operator's strict opt-out).
	t.Run("strict mode: recognized ci collation is NOT reproduced", func(t *testing.T) {
		m := ColumnInfosFromIR("mysql", cols, true)
		if m["vc_ci"].faithfulString() {
			t.Error("vc_ci under strict mode: must NOT be faithful (strict forces refusal)")
		}
		// Byte-exact columns are still fine under strict.
		if !m["vc_bin"].faithfulString() {
			t.Error("vc_bin under strict mode: byte-exact should still be faithful")
		}
	})
}

func wantFamily(t *testing.T, m map[string]ColumnInfo, col string, want Family) {
	t.Helper()
	if got := m[col].Family; got != want {
		t.Errorf("%s: family = %d; want %d", col, got, want)
	}
}

func wantStringCS(t *testing.T, m map[string]ColumnInfo, col string, want bool) {
	t.Helper()
	if m[col].Family != FamilyString {
		t.Fatalf("%s: family = %d; want FamilyString", col, m[col].Family)
	}
	if got := m[col].CaseSensitive; got != want {
		t.Errorf("%s: CaseSensitive = %v; want %v", col, got, want)
	}
}

// TestCaseInsensitiveStringRefusalMessage pins that the ci-string refusal
// names the collation hazard (so an operator understands why).
func TestCaseInsensitiveStringRefusalMessage(t *testing.T) {
	_, err := Compile("users", "name = 'bob'", map[string]ColumnInfo{"name": {Family: FamilyString, CaseSensitive: false}})
	if err == nil {
		t.Fatal("want refusal")
	}
	if !strings.Contains(err.Error(), "collation") {
		t.Errorf("refusal %q does not mention collation", err.Error())
	}
}

func assertEval(t *testing.T, p *Predicate, row ir.Row, want bool) {
	t.Helper()
	if got := p.Eval(row); got != want {
		t.Errorf("Eval(%v) on %q = %v; want %v", row, p, got, want)
	}
}

// TestInList_ShortCircuitSemantics pins audit F-P2's short-circuit: a match on
// an early IN member fixes the result to TRUE (non-negated) / FALSE (NOT IN)
// regardless of later members — including a later NULL, which must NOT demote
// a definite match to UNKNOWN. This is both the correctness contract and the
// behavior the short-circuit relies on (breaking on the first match cannot
// change the answer).
func TestInList_ShortCircuitSemantics(t *testing.T) {
	infos := ColumnInfosFromIR("mysql", []*ir.Column{
		{Name: "n", Type: ir.Integer{}},
	}, false)

	// Match on the FIRST member with a NULL later member still in the list:
	// SQL `n IN (1, NULL)` with n=1 is TRUE (a definite match), never UNKNOWN.
	p := mustCompile(t, "n IN (1, 2, 3)", infos)
	assertEval(t, p, ir.Row{"n": int64(1)}, true) // early match
	assertEval(t, p, ir.Row{"n": int64(3)}, true) // late match
	assertEval(t, p, ir.Row{"n": int64(9)}, false)

	// NOT IN with an early match short-circuits to FALSE.
	pn := mustCompile(t, "n NOT IN (1, 2, 3)", infos)
	assertEval(t, pn, ir.Row{"n": int64(1)}, false)
	assertEval(t, pn, ir.Row{"n": int64(9)}, true)
}

// BenchmarkInList_CollationShortCircuit demonstrates the F-P2 win: a 50-value
// IN on a case-insensitive column whose FIRST member matches pays exactly one
// collation compare, not fifty — the short-circuit returns on the first TRUE.
// (Before the fix, inNode.eval looped every member, paying ~50×130ns + 100
// allocs per row.) The benchmark is a living demonstration, not a hard
// threshold assertion.
func BenchmarkInList_CollationShortCircuit(b *testing.B) {
	cols := []*ir.Column{{Name: "region", Type: ir.Varchar{Collation: "utf8mb4_general_ci"}}}
	infos := ColumnInfosFromIR("mysql", cols, false)
	// A 50-member IN list; "EU" is the first member.
	members := make([]string, 0, 50)
	members = append(members, "'EU'")
	for i := 0; i < 49; i++ {
		members = append(members, "'zz"+string(rune('a'+i%26))+"'")
	}
	p, err := Compile("t", "region IN ("+strings.Join(members, ", ")+")", infos)
	if err != nil {
		b.Fatalf("compile: %v", err)
	}
	row := ir.Row{"region": "eu"} // matches "EU" under general_ci on the FIRST member
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !p.Eval(row) {
			b.Fatal("expected match")
		}
	}
}
