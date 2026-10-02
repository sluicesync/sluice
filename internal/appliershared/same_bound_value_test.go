// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"math"
	"testing"
	"time"
)

// TestSameBoundValue_EveryFamily pins the trim equality across every value
// family docs/value-types.md lets a Row carry: equal values are the same,
// and every pair that binds a different argument — including the signed-zero
// pair == cannot see — is not.
func TestSameBoundValue_EveryFamily(t *testing.T) {
	negZero := math.Copysign(0, -1)
	utc := time.Date(2026, 10, 2, 10, 0, 0, 123456000, time.UTC)
	cases := []struct {
		name string
		a, b any
		want bool
	}{
		{"int64 equal", int64(7), int64(7), true},
		{"int64 vs int32", int64(7), int32(7), false},
		{"uint64 equal", uint64(1 << 63), uint64(1 << 63), true},
		{"bool", true, false, false},
		{"string case", "abc", "ABC", false},
		{"string trailing space", "abc", "abc ", false},
		{"bytes equal", []byte{0, 1}, []byte{0, 1}, true},
		{"bytes differ", []byte{0, 1}, []byte{0, 2}, false},
		{"set", []string{"a", "b"}, []string{"a", "b"}, true},
		{"time.Time equal", utc, utc, true},
		{"time.Time same instant other location", utc, utc.In(time.FixedZone("X", 7200)), false},
		{"float64 equal", 1.5, 1.5, true},
		{"float64 +0 vs -0", 0.0, negZero, false},
		{"float64 -0 vs -0", negZero, negZero, true},
		{"float32 +0 vs -0", float32(0), float32(negZero), false},
		{"float32 vs float64", float32(1), float64(1), false},
		{"float64 NaN same bits", math.NaN(), math.NaN(), true},
		{"[]any equal", []any{int64(1), 2.0}, []any{int64(1), 2.0}, true},
		{"[]any signed-zero element", []any{0.0}, []any{negZero}, false},
		{"[]any nested", []any{[]any{0.0}}, []any{[]any{negZero}}, false},
		{"[]any length", []any{1.0}, []any{1.0, 1.0}, false},
		{"[]any nil vs empty", []any(nil), []any{}, false},
		{"nil", nil, nil, true},
	}
	for _, c := range cases {
		if got := SameBoundValue(c.a, c.b); got != c.want {
			t.Errorf("%s: SameBoundValue(%#v, %#v) = %v, want %v", c.name, c.a, c.b, got, c.want)
		}
	}
}
