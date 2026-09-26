// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
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
