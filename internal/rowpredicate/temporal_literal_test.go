// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

import (
	"fmt"
	"math"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines/mysql"
	"sluicesync.dev/sluice/internal/ir"
)

// testMariaDBResolver is the REAL MariaDB-flavor resolver: the string lens is
// shared with vanilla MySQL, but the temporal-literal lens truncates where
// MySQL rounds (audit 2026-07-23 D0-5 ground truth).
var testMariaDBResolver = mysql.Engine{Flavor: mysql.FlavorMariaDB}.CollationResolver()

// TestColumnInfosFromIR_TemporalSemantics pins the resolver → ColumnInfo
// threading of the engine temporal-literal lens (audit 2026-07-23 D0-5 / Q1):
// each engine's declared coercion rule lands on every FamilyTemporal column,
// DATE columns carry the date-only marker, and a resolver WITHOUT the
// optional surface (the generic byte-exact lens) leaves the safe zero value
// (ClientExact — no normalization, push-down belt engaged).
func TestColumnInfosFromIR_TemporalSemantics(t *testing.T) {
	cols := []*ir.Column{
		{Name: "d", Type: ir.Date{}},
		{Name: "ts", Type: ir.DateTime{}},
		{Name: "xts", Type: ir.Timestamp{}}, // cross-engine naive spelling
	}
	cases := []struct {
		name     string
		resolver ir.CollationResolver
		want     ir.TemporalLiteralSemantics
	}{
		{"postgres casts to column", testPGResolver, ir.TemporalLiteralCastToColumn},
		{"mysql promotes + rounds half-up", testMySQLResolver, ir.TemporalLiteralPromoteRoundHalfUp},
		{"mariadb promotes + truncates", testMariaDBResolver, ir.TemporalLiteralPromoteTruncate},
		{"generic byte-exact lens has no temporal surface (zero value)", ir.ByteExactCollationResolver{}, ir.TemporalLiteralClientExact},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := ColumnInfosFromIR(tc.resolver, cols, false)
			for _, col := range []string{"d", "ts", "xts"} {
				if got := m[col].TemporalSemantics; got != tc.want {
					t.Errorf("%s: TemporalSemantics = %d; want %d", col, got, tc.want)
				}
			}
			if !m["d"].TemporalDateOnly {
				t.Error("d: TemporalDateOnly = false; want true (ir.Date)")
			}
			if m["ts"].TemporalDateOnly || m["xts"].TemporalDateOnly {
				t.Error("ts/xts: TemporalDateOnly = true; want false (datetime family)")
			}
		})
	}
}

// TestTemporalLiteralNormalization pins the Q1 owner call (audit 2026-07-23
// D0-5): Compile normalizes a temporal literal to the SOURCE ENGINE's own
// comparison semantics, so the client evaluator classifies a row exactly as
// the engine's snapshot SELECT / pushed stream filter does. Every expected
// verdict below is the REAL server's, observed 2026-07-23 (PG 16.14, MySQL
// 8.0.46, MariaDB 11.8.8 — see ir.TemporalLiteralSemantics) and permanently
// re-ground-truthed by the temporal_realdb integration matrix. The three
// engine modes and the ClientExact zero value are each pinned per literal
// shape — the flavor-divergent .1234565 half-boundary is the discriminator
// cell (PG's double-mediated rint keeps .123456 there, MySQL half-up gives
// .123457, MariaDB truncates to .123456), and the .0001255/.0001265 pair
// pins that PG's rule is the DOUBLE-mediated one, not exact decimal
// half-even (review F1).
func TestTemporalLiteralNormalization(t *testing.T) {
	date := func(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }
	ts := func(ns int) time.Time { return time.Date(2026, 1, 15, 8, 30, 0, ns, time.UTC) }

	pgInfos := ColumnInfosFromIR(testPGResolver, []*ir.Column{
		{Name: "d", Type: ir.Date{}},
		{Name: "ts", Type: ir.DateTime{}},
	}, false)
	myInfos := ColumnInfosFromIR(testMySQLResolver, []*ir.Column{
		{Name: "d", Type: ir.Date{}},
		{Name: "dt", Type: ir.DateTime{}},
	}, false)
	mdbInfos := ColumnInfosFromIR(testMariaDBResolver, []*ir.Column{
		{Name: "d", Type: ir.Date{}},
		{Name: "dt", Type: ir.DateTime{}},
	}, false)
	exactInfos := map[string]ColumnInfo{
		"d":  {Family: FamilyTemporal, TemporalDateOnly: true},
		"dt": {Family: FamilyTemporal},
	}

	cases := []struct {
		name  string
		infos map[string]ColumnInfo
		pred  string
		row   ir.Row
		want  bool
	}{
		// ---- Postgres: literal CAST to the DATE column (time truncated) ----
		{"pg date eq time-bearing literal matches (truncated)", pgInfos, "d = '2026-01-15 08:30:00'", ir.Row{"d": date(2026, 1, 15)}, true},
		{"pg date lt noon literal is date-vs-date (not lt)", pgInfos, "d < '2026-01-15 12:00:00'", ir.Row{"d": date(2026, 1, 15)}, false},
		{"pg date ne noon literal is equal after truncation", pgInfos, "d != '2026-01-15 12:00:00'", ir.Row{"d": date(2026, 1, 15)}, false},
		{"pg date NOT(ge noon literal) — 3VL negation", pgInfos, "NOT (d >= '2026-01-15 12:00:00')", ir.Row{"d": date(2026, 1, 15)}, false},
		{"pg date IN with a time-bearing member (truncated to match)", pgInfos, "d IN ('2026-01-15 08:30', '2026-02-01')", ir.Row{"d": date(2026, 1, 15)}, true},
		{"pg date pure-date literal untouched", pgInfos, "d = '2026-01-15'", ir.Row{"d": date(2026, 1, 15)}, true},
		{"pg date earlier day stays lt", pgInfos, "d < '2026-01-15 12:00:00'", ir.Row{"d": date(2026, 1, 14)}, true},

		// ---- Postgres: fractional seconds round DOUBLE-MEDIATED to µs ----
		// PG's rule is rint(strtod(fraction)·10⁶) (datetime.c) — nominally
		// round-half-even, but computed through a binary double, so the
		// decimal-exact half rounds the way the DOUBLE of the fraction
		// lands. The .0001255/.0001265 pair is the divergence pin: exact
		// decimal half-even would give .000126/.000126, PG gives
		// .000125/.000127 (OBSERVED live, 2026-07-23).
		{"pg ts .1234565 half lands below (→ .123456)", pgInfos, "ts = '2026-01-15 08:30:00.1234565'", ir.Row{"ts": ts(123456000)}, true},
		{"pg ts .1234575 half lands above (→ .123458)", pgInfos, "ts = '2026-01-15 08:30:00.1234575'", ir.Row{"ts": ts(123457000)}, false},
		{"pg ts .1234575 matches .123458", pgInfos, "ts = '2026-01-15 08:30:00.1234575'", ir.Row{"ts": ts(123458000)}, true},
		{"pg ts .0001255 double lands BELOW the half (→ .000125, not exact-half-even's .000126)", pgInfos, "ts = '2026-01-15 08:30:00.0001255'", ir.Row{"ts": ts(125000)}, true},
		{"pg ts .0001255 does not match .000126", pgInfos, "ts = '2026-01-15 08:30:00.0001255'", ir.Row{"ts": ts(126000)}, false},
		{"pg ts .0001265 double lands ABOVE the half (→ .000127, not exact-half-even's .000126)", pgInfos, "ts = '2026-01-15 08:30:00.0001265'", ir.Row{"ts": ts(127000)}, true},
		{"pg ts .0001265 does not match .000126", pgInfos, "ts = '2026-01-15 08:30:00.0001265'", ir.Row{"ts": ts(126000)}, false},
		{"pg ts 7-digit rounds up (.1234567 → .123457)", pgInfos, "ts = '2026-01-15 08:30:00.1234567'", ir.Row{"ts": ts(123457000)}, true},
		{"pg ts 7-digit does not match the truncation", pgInfos, "ts = '2026-01-15 08:30:00.1234567'", ir.Row{"ts": ts(123456000)}, false},
		{"pg ts rounding carries into seconds (.9999995 → +1s)", pgInfos, "ts = '2026-01-15 08:30:00.9999995'", ir.Row{"ts": time.Date(2026, 1, 15, 8, 30, 1, 0, time.UTC)}, true},
		{"pg ts 8-digit fraction (.12345650 → .123456)", pgInfos, "ts = '2026-01-15 08:30:00.12345650'", ir.Row{"ts": ts(123456000)}, true},
		{"pg ts 9-digit fraction (.123456501 → .123457)", pgInfos, "ts = '2026-01-15 08:30:00.123456501'", ir.Row{"ts": ts(123457000)}, true},
		{"pg ts 6-digit literal untouched", pgInfos, "ts = '2026-01-15 08:30:00.123456'", ir.Row{"ts": ts(123456000)}, true},

		// ---- MySQL: DATE PROMOTED to datetime (full instant compared) ----
		{"mysql date eq time-bearing literal does NOT match (promoted)", myInfos, "d = '2026-01-15 08:30:00'", ir.Row{"d": date(2026, 1, 15)}, false},
		{"mysql date lt noon literal (midnight < noon)", myInfos, "d < '2026-01-15 12:00:00'", ir.Row{"d": date(2026, 1, 15)}, true},
		{"mysql date eq midnight literal matches", myInfos, "d = '2026-01-15 00:00:00'", ir.Row{"d": date(2026, 1, 15)}, true},

		// ---- MySQL: fractional seconds round HALF-UP to µs ----
		{"mysql dt half-boundary rounds up (.1234565 → .123457)", myInfos, "dt = '2026-01-15 08:30:00.1234565'", ir.Row{"dt": ts(123457000)}, true},
		{"mysql dt half-boundary does not match the floor", myInfos, "dt = '2026-01-15 08:30:00.1234565'", ir.Row{"dt": ts(123456000)}, false},
		{"mysql dt rounding carries into seconds", myInfos, "dt = '2026-01-15 08:30:00.9999995'", ir.Row{"dt": time.Date(2026, 1, 15, 8, 30, 1, 0, time.UTC)}, true},
		{"mysql dt ge 7-digit literal rounds down to the stored µs", myInfos, "dt >= '2026-01-15 08:30:00.1234561'", ir.Row{"dt": ts(123456000)}, true},

		// ---- MariaDB: fractional seconds TRUNCATED to µs (no carry) ----
		{"mariadb dt half-boundary truncates (.1234565 → .123456)", mdbInfos, "dt = '2026-01-15 08:30:00.1234565'", ir.Row{"dt": ts(123456000)}, true},
		{"mariadb dt 7-digit truncates (.1234567 → .123456)", mdbInfos, "dt = '2026-01-15 08:30:00.1234567'", ir.Row{"dt": ts(123456000)}, true},
		{"mariadb dt .9999995 does NOT carry (→ .999999)", mdbInfos, "dt = '2026-01-15 08:30:00.9999995'", ir.Row{"dt": time.Date(2026, 1, 15, 8, 30, 0, 999999000, time.UTC)}, true},
		{"mariadb dt .9999995 does not match the next second", mdbInfos, "dt = '2026-01-15 08:30:00.9999995'", ir.Row{"dt": time.Date(2026, 1, 15, 8, 30, 1, 0, time.UTC)}, false},
		{"mariadb date promotes like mysql", mdbInfos, "d = '2026-01-15 08:30:00'", ir.Row{"d": date(2026, 1, 15)}, false},

		// ---- ClientExact zero value: engine-granular literals still
		// compile and compare at full (exact) precision ----
		{"clientexact datetime time-bearing literal is exact", exactInfos, "dt = '2026-01-15 08:30:00.123456'", ir.Row{"dt": ts(123456000)}, true},
		{"clientexact pure-date literal on a date column", exactInfos, "d = '2026-01-15'", ir.Row{"d": date(2026, 1, 15)}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := mustCompile(t, tc.pred, tc.infos)
			assertEval(t, p, tc.row, tc.want)
		})
	}
}

// TestTemporalLiteralClientExactRefusals pins review F4: under the
// ClientExact zero value (no engine temporal-literal lens) a literal
// FINER-grained than the column REFUSES at compile — the engines resolve
// the mismatch three different ways, so a full-precision client compare is
// a guess, and Compile's other unfaithful-comparison refusals set the
// pattern. This makes "Compile output is always engine-granular or
// engine-normalized" code, not a doc comment: no compiled predicate can
// carry a granularity mismatch the evaluator would mis-compare.
func TestTemporalLiteralClientExactRefusals(t *testing.T) {
	exactInfos := map[string]ColumnInfo{
		"d":  {Family: FamilyTemporal, TemporalDateOnly: true},
		"dt": {Family: FamilyTemporal},
	}
	for _, pred := range []string{
		"d = '2026-01-15 08:30:00'",               // time-bearing on DATE
		"d < '2026-01-15 12:00'",                  // minute form on DATE
		"d IN ('2026-01-15', '2026-01-16 08:30')", // one time-bearing member
		"dt >= '2026-01-15 08:30:00.1234561'",     // sub-µs fraction
		"dt IN ('2026-01-15 08:30:00.1234567')",   // sub-µs IN member
	} {
		t.Run(pred, func(t *testing.T) {
			mustRefuse(t, pred, exactInfos)
		})
	}
	// Engine-granular shapes stay compilable under ClientExact.
	for _, pred := range []string{
		"d = '2026-01-15'",
		"dt = '2026-01-15 08:30:00.123456'",
		"dt < '2026-01-15 12:00:00'", // time-bearing is fine on a NON-date temporal
		"d IS NULL",
	} {
		t.Run("allow "+pred, func(t *testing.T) {
			mustCompile(t, pred, exactInfos)
		})
	}
}

// TestCompareNumeric_NonFiniteTotalOrder pins the FamilyNumeric sibling of
// the Q4 owner call (review F2): PG's NUMERIC type also stores NaN — and,
// since PG 14, ±Infinity — ordered with the same total order as float (NaN
// above everything, Infinity included). ir.Decimal values travel as STRINGS
// per docs/value-types.md, so "NaN"/"Infinity"/"-Infinity" arrive at the
// numeric comparator, where the exact big.Rat parse fails — pre-F2 that
// meant UNKNOWN→drop, while ir.Decimal sits INSIDE the push-down envelope:
// the server delivered a NaN row's changes and route() dropped them under
// the benign-direction DEBUG log. Equality is ALLOWED on numeric (unlike
// float), so =, !=, IN, and NOT IN are pinned alongside the orderings.
// float64/float32 non-finite carriers get the same order (a value-contract
// deviation would otherwise silently flip verdicts).
func TestCompareNumeric_NonFiniteTotalOrder(t *testing.T) {
	numCol := ColumnInfosFromIR(testPGResolver, []*ir.Column{{Name: "x", Type: ir.Decimal{Precision: 12, Scale: 4}}}, false)
	cases := []struct {
		pred string
		val  any
		want bool
	}{
		// NaN: above every finite literal.
		{"x > 10.5", "NaN", true},
		{"x >= 10.5", "NaN", true},
		{"x < 10.5", "NaN", false},
		{"x <= 10.5", "NaN", false},
		{"x = 10.5", "NaN", false},
		{"x != 10.5", "NaN", true},
		{"x IN (10.5, 20)", "NaN", false},
		{"x NOT IN (10.5, 20)", "NaN", true},
		// +Infinity: above every finite literal.
		{"x > 10.5", "Infinity", true},
		{"x < 10.5", "Infinity", false},
		{"x = 10.5", "Infinity", false},
		{"x != 10.5", "Infinity", true},
		// -Infinity: below every finite literal.
		{"x < 10.5", "-Infinity", true},
		{"x > 10.5", "-Infinity", false},
		{"x != 10.5", "-Infinity", true},
		// float64 carriers order identically.
		{"x > 10.5", math.NaN(), true},
		{"x > 10.5", math.Inf(1), true},
		{"x < 10.5", math.Inf(-1), true},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/%v", tc.pred, tc.val), func(t *testing.T) {
			p := mustCompile(t, tc.pred, numCol)
			assertEval(t, p, ir.Row{"x": tc.val}, tc.want)
		})
	}

	// A finite-but-unparseable string stays UNKNOWN (contract violation →
	// no guessed verdict), and a normal decimal string is untouched.
	p := mustCompile(t, "x > 10.5", numCol)
	assertEval(t, p, ir.Row{"x": "not-a-number"}, false)
	assertEval(t, p, ir.Row{"x": "10.5001"}, true)
}

// TestCompareFloat_NaNTotalOrder pins the Q4 owner call (audit 2026-07-23
// D0-6): the FamilyFloat ordering comparator uses Postgres's float TOTAL
// order — NaN greater than every non-NaN value, NaN equal to NaN (observed
// PG 16.14: `'NaN'::float8 > 0.1` → true) — instead of mapping NaN to
// UNKNOWN. UNKNOWN made the snapshot leg (server-evaluated WHERE) include a
// NaN row and the client CDC leg then drop its every change: stale target
// rows and orphaned deletes at exit 0. PG is the only supported source that
// can deliver a float NaN (MySQL/MariaDB cannot store one; SQLite stores NaN
// as NULL), so the engine-neutral comparator applies the total order
// universally — no other engine can ever present the case. ±Inf already
// ordered correctly (IEEE-754 comparison) and is pinned alongside.
func TestCompareFloat_NaNTotalOrder(t *testing.T) {
	floatCol := ColumnInfosFromIR(testPGResolver, []*ir.Column{{Name: "x", Type: ir.Float{}}}, false)
	cases := []struct {
		pred string
		val  float64
		want bool
	}{
		// NaN sorts LAST: greater than every finite value.
		{"x > 0.1", math.NaN(), true},
		{"x >= 0.1", math.NaN(), true},
		{"x < 0.1", math.NaN(), false},
		{"x <= 0.1", math.NaN(), false},
		// NaN is above +Inf in PG's total order, so > any literal holds; the
		// grammar cannot spell an Inf/NaN literal, so finite literals are the
		// whole reachable space.
		{"x > 99999", math.NaN(), true},
		// ±Inf: plain IEEE-754 ordering.
		{"x > 0.1", math.Inf(1), true},
		{"x < 0.1", math.Inf(1), false},
		{"x < 0.1", math.Inf(-1), true},
		{"x > 0.1", math.Inf(-1), false},
	}
	for _, tc := range cases {
		t.Run(tc.pred, func(t *testing.T) {
			p := mustCompile(t, tc.pred, floatCol)
			assertEval(t, p, ir.Row{"x": tc.val}, tc.want)
		})
	}

	// The literal side of the total order (NaN == NaN) is unreachable through
	// the grammar — lexNumber cannot produce "NaN" — but the comparator is
	// total anyway; pin it directly so a refactor cannot half-implement it.
	if got := compareFloat(math.NaN(), opGe, literal{kind: litNumber, text: "NaN"}); got != truthTrue {
		t.Errorf("compareFloat(NaN >= NaN) = %d; want truthTrue (NaN == NaN in the total order)", got)
	}
	if got := compareFloat(math.NaN(), opGt, literal{kind: litNumber, text: "NaN"}); got != truthFalse {
		t.Errorf("compareFloat(NaN > NaN) = %d; want truthFalse (NaN == NaN in the total order)", got)
	}
	if got := compareFloat(float64(0.5), opLt, literal{kind: litNumber, text: "NaN"}); got != truthTrue {
		t.Errorf("compareFloat(0.5 < NaN) = %d; want truthTrue (NaN sorts last)", got)
	}
}
