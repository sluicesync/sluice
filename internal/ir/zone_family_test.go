// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"strings"
	"testing"
)

// TestSessionDependentZoneSwap_FamilyMatrix pins the class, not a
// representative: every member type × every member type, plus the
// non-members, with the expected verdict derived from the rule (same
// family, opposite zone flag, same array depth — and, for the time family
// only, the ADD direction) rather than from the code. Precision is varied
// on purpose — it must never enter the verdict.
func TestSessionDependentZoneSwap_FamilyMatrix(t *testing.T) {
	type member struct {
		name   string
		typ    Type
		family string
		zoned  bool
	}
	members := []member{
		{"timestamptz", Timestamp{WithTimeZone: true, Precision: 6}, "timestamp", true},
		{"timestamptz(3)", Timestamp{WithTimeZone: true, Precision: 3}, "timestamp", true},
		{"timestamp-naive-Timestamp", Timestamp{}, "timestamp", false},
		{"datetime", DateTime{Precision: 6}, "timestamp", false},
		{"datetime-bare", DateTime{PrecisionUnspecified: true}, "timestamp", false},
		{"timetz", Time{WithTimeZone: true}, "time", true},
		{"time", Time{Precision: 3}, "time", false},
		{"timestamptz[]", Array{Element: Timestamp{WithTimeZone: true}}, "array:timestamp", true},
		{"timestamp[]", Array{Element: DateTime{}}, "array:timestamp", false},
		{"timetz[]", Array{Element: Time{WithTimeZone: true}}, "array:time", true},
		{"time[]", Array{Element: Time{}}, "array:time", false},
		{"timestamp[][]", Array{Element: Array{Element: DateTime{}}}, "array:array:timestamp", false},
	}
	nonMembers := []Type{Date{}, Interval{}, Text{}, Integer{Width: 64}, Array{Element: Text{}}, nil}

	for _, m := range members {
		family, zoned, ok := ZoneFamily(m.typ)
		if !ok || family != m.family || zoned != m.zoned {
			t.Errorf("ZoneFamily(%s) = (%q, %v, %v); want (%q, %v, true)", m.name, family, zoned, ok, m.family, m.zoned)
		}
	}
	for _, n := range nonMembers {
		if _, _, ok := ZoneFamily(n); ok {
			t.Errorf("ZoneFamily(%v) reports a member; want none", n)
		}
	}
	pairs, timePairs := 0, 0
	for _, a := range members {
		for _, b := range members {
			want := a.family == b.family && a.zoned != b.zoned
			// The time family is the ADD direction only (SLM-5b). The
			// suffix test is deliberately independent of the production
			// baseFamily helper — "timestamp" does not end in "time", so
			// this separates the two families without calling the code
			// under test to decide what the answer should be.
			isTimeFamily := strings.HasSuffix(a.family, "time")
			if want && isTimeFamily {
				want = !a.zoned && b.zoned
			}
			if got := SessionDependentZoneSwap(a.typ, b.typ); got != want {
				t.Errorf("SessionDependentZoneSwap(%s → %s) = %v; want %v", a.name, b.name, got, want)
			}
			if want {
				pairs++
				if isTimeFamily {
					timePairs++
				}
			}
		}
		for _, n := range nonMembers {
			if SessionDependentZoneSwap(a.typ, n) || SessionDependentZoneSwap(n, a.typ) {
				t.Errorf("SessionDependentZoneSwap pairs member %s with non-member %v", a.name, n)
			}
		}
	}
	// Anti-vacuity: the timestamp family contributes 2 zoned × 3 naive × 2
	// directions scalar plus 2 in the array family; the time family
	// contributes exactly one ADD cell per shape.
	if pairs < 12 {
		t.Fatalf("%d swap cells; floor 12 — the matrix lost its members", pairs)
	}
	// A second floor, because the first one is dominated by the timestamp
	// family and would stay green if the time family stopped matching
	// entirely — which is the failure the SLM-5b narrowing could plausibly
	// cause. Scalar `time`→`timetz` and `time[]`→`timetz[]`: exactly 2.
	if timePairs != 2 {
		t.Fatalf("%d time-family swap cells; want exactly 2 (scalar and array, ADD direction only) — either the drop direction came back or the add direction stopped refusing", timePairs)
	}
}

// TestSessionDependentZoneSwap_DirectionMatrix is the SLM-5b pin the
// predicate's own doc cites: all four directions of both families, scalar
// and array, each with the measurement that decided it. It exists
// separately from the family matrix above because that one derives its
// expectations from a RULE, and a rule stated wrongly in two places agrees
// with itself. These are written out literally.
func TestSessionDependentZoneSwap_DirectionMatrix(t *testing.T) {
	var (
		tsz   Type = Timestamp{WithTimeZone: true}
		tsn   Type = Timestamp{}
		timez Type = Time{WithTimeZone: true}
		timen Type = Time{}
	)
	arr := func(t Type) Type { return Array{Element: t} }

	for _, tc := range []struct {
		name     string
		prev     Type
		cur      Type
		want     bool
		measured string
	}{
		// The timestamp family is symmetric: timestamptz is stored
		// normalised to UTC, so BOTH directions consult the session.
		{"timestamptz -> timestamp", tsz, tsn, true, "renders UTC through the session zone (SLM-5, pg16)"},
		{"timestamp -> timestamptz", tsn, tsz, true, "interprets a naive value in the session zone (SLM-5, pg16)"},
		{"timestamptz[] -> timestamp[]", arr(tsz), arr(tsn), true, "same mechanism one level down"},
		{"timestamp[] -> timestamptz[]", arr(tsn), arr(tsz), true, "same mechanism one level down"},

		// The time family is NOT. Only the ADD direction invents an offset.
		{"time -> timetz", timen, timez, true, "stamps the session offset onto a naive value (SLM-5b, pg16: +09 vs +00)"},
		{"timetz -> time", timez, timen, false, "offset travels with the value; byte-identical under UTC and Asia/Tokyo (SLM-5b)"},
		{"time[] -> timetz[]", arr(timen), arr(timez), true, "same mechanism one level down"},
		{"timetz[] -> time[]", arr(timez), arr(timen), false, "same mechanism one level down"},

		// Neither a swap in either family.
		{"timetz -> timetz", timez, timez, false, "identity"},
		{"time -> time", timen, timen, false, "identity"},
		{"timetz -> timestamptz", timez, tsz, false, "different family — not this predicate's class (SessionZoneCast covers it)"},
		{"time -> timestamp", timen, tsn, false, "different family"},
		{"timetz -> time[]", timez, arr(timen), false, "scalar to array is a dimension change, not a re-cast in place"},
		{"time -> timetz[]", timen, arr(timez), false, "scalar to array is a dimension change, not a re-cast in place"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := SessionDependentZoneSwap(tc.prev, tc.cur); got != tc.want {
				t.Errorf("SessionDependentZoneSwap(%v, %v) = %v; want %v — measured: %s",
					tc.prev, tc.cur, got, tc.want, tc.measured)
			}
		})
	}
}
