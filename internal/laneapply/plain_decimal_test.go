// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

// TestPlainDecimal pins the exponent expansion the float and json.Number
// key arms share. The expected values are Postgres numeric's rendering of
// the same text (numeric_in keeps every mantissa digit and moves the
// point), which is what the postgres-trigger stream carries for a float
// key; text without an exponent must come back byte-identical, which is
// what keeps every integer and every numeric key on its existing lane.
func TestPlainDecimal(t *testing.T) {
	for in, want := range map[string]string{
		"1.5e-07":     "0.00000015",
		"1.5e-7":      "0.00000015",
		"1e+21":       "1000000000000000000000",
		"1E21":        "1000000000000000000000",
		"-2.5e-3":     "-0.0025",
		"1.234e+2":    "123.4",
		"1.25e+2":     "125",
		"5e-324":      "0." + strings.Repeat("0", 323) + "5",
		"10.50":       "10.50",
		"42":          "42",
		"-0":          "-0",
		"007":         "007",
		"1.5":         "1.5",
		"k-42":        "k-42",
		"1e-99999999": "1e-99999999",
		"e5":          "e5",
		"1.5e":        "1.5e",
		"1.x5e2":      "1.x5e2",
	} {
		if got := plainDecimal(in); got != want {
			t.Errorf("plainDecimal(%q) = %q; want %q", in, got, want)
		}
	}
}

// TestCanonicalKey_NegativeZeroNumberIsTheIntegralZero pins the json.Number
// arm's negative-zero rule (v0.156.4 review): a float key's -0 carried by an
// ADD COLUMN fill as json.Number("-0") must encode exactly as the stream's
// integral 0 and as a float64 -0, or the fill and a later update of that row
// can take different lanes. Other zero spellings keep their text ("0.00" is
// a numeric key's own rendering and must still match its copy-read string).
func TestCanonicalKey_NegativeZeroNumberIsTheIntegralZero(t *testing.T) {
	enc := func(v any) string {
		var b strings.Builder
		WriteCanonicalKeyValue(&b, v)
		return b.String()
	}
	zero := enc(int64(0))
	for _, v := range []any{json.Number("-0"), json.Number("0"), math.Copysign(0, -1), float64(0)} {
		if got := enc(v); got != zero {
			t.Errorf("key %#v encodes %q; want the integral zero's %q", v, got, zero)
		}
	}
	if got, want := enc(json.Number("0.00")), enc("0.00"); got != want {
		t.Errorf("json.Number(\"0.00\") encodes %q; want the copy-read string's %q", got, want)
	}
}
