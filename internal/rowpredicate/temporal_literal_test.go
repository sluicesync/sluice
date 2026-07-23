// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

import (
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
// cell (PG half-even keeps .123456, MySQL half-up gives .123457, MariaDB
// truncates to .123456).
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

		// ---- Postgres: fractional seconds round HALF-EVEN to µs ----
		{"pg ts half-boundary rounds to even (.1234565 → .123456)", pgInfos, "ts = '2026-01-15 08:30:00.1234565'", ir.Row{"ts": ts(123456000)}, true},
		{"pg ts half-boundary odd floor rounds up (.1234575 → .123458)", pgInfos, "ts = '2026-01-15 08:30:00.1234575'", ir.Row{"ts": ts(123457000)}, false},
		{"pg ts .1234575 matches .123458", pgInfos, "ts = '2026-01-15 08:30:00.1234575'", ir.Row{"ts": ts(123458000)}, true},
		{"pg ts 7-digit rounds up (.1234567 → .123457)", pgInfos, "ts = '2026-01-15 08:30:00.1234567'", ir.Row{"ts": ts(123457000)}, true},
		{"pg ts 7-digit does not match the truncation", pgInfos, "ts = '2026-01-15 08:30:00.1234567'", ir.Row{"ts": ts(123456000)}, false},
		{"pg ts rounding carries into seconds (.9999995 → +1s)", pgInfos, "ts = '2026-01-15 08:30:00.9999995'", ir.Row{"ts": time.Date(2026, 1, 15, 8, 30, 1, 0, time.UTC)}, true},
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

		// ---- ClientExact zero value: NO normalization (pre-Q1 behavior;
		// the push-down classifiers keep these out as the fail-closed belt) ----
		{"clientexact date compares the full instant", exactInfos, "d = '2026-01-15 08:30:00'", ir.Row{"d": date(2026, 1, 15)}, false},
		{"clientexact keeps sub-µs precision (no rounding)", exactInfos, "dt >= '2026-01-15 08:30:00.1234561'", ir.Row{"dt": ts(123456000)}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := mustCompile(t, tc.pred, tc.infos)
			assertEval(t, p, tc.row, tc.want)
		})
	}
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
