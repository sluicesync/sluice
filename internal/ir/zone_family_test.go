// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "testing"

// TestZoneSiblingSwap_FamilyMatrix pins the class, not a representative:
// every member type × every member type, plus the non-members, with the
// expected verdict derived from the rule (same family, opposite zone
// flag, same array depth) rather than from the code. Precision is varied
// on purpose — it must never enter the verdict.
func TestZoneSiblingSwap_FamilyMatrix(t *testing.T) {
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
	pairs := 0
	for _, a := range members {
		for _, b := range members {
			want := a.family == b.family && a.zoned != b.zoned
			if got := ZoneSiblingSwap(a.typ, b.typ); got != want {
				t.Errorf("ZoneSiblingSwap(%s → %s) = %v; want %v", a.name, b.name, got, want)
			}
			if want {
				pairs++
			}
		}
		for _, n := range nonMembers {
			if ZoneSiblingSwap(a.typ, n) || ZoneSiblingSwap(n, a.typ) {
				t.Errorf("ZoneSiblingSwap pairs member %s with non-member %v", a.name, n)
			}
		}
	}
	// Anti-vacuity: the four scalar pairs × 2 directions × the precision
	// variants, plus the array pairs — well above the floor.
	if pairs < 12 {
		t.Fatalf("%d swap cells; floor 12 — the matrix lost its members", pairs)
	}
}
