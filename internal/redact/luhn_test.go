// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import "testing"

// TestLuhnValid covers the standard PAN / SIN / IMEI checksum
// algorithm across a representative set of inputs.
func TestLuhnValid(t *testing.T) {
	cases := []struct {
		name string
		s    string
		want bool
	}{
		// Real-world test PANs from the major card brands' published
		// test suites (these are not real card numbers).
		{"Visa test", "4111111111111111", true},
		{"Mastercard test", "5555555555554444", true},
		{"AmEx test", "378282246310005", true},
		{"Discover test", "6011111111111117", true},
		// Same numbers with spaces / hyphens — non-digit chars skipped.
		{"Visa with hyphens", "4111-1111-1111-1111", true},
		{"Visa with spaces", "4111 1111 1111 1111", true},
		// Off by one in the check digit.
		{"Visa wrong checksum", "4111111111111112", false},
		// Common single-digit / repeated-digit shapes.
		{"all zeros pass Luhn", "0000000000000000", true},
		{"single zero", "0", true},
		// Other ISO/IEC 7812 examples (Wikipedia).
		{"79927398713", "79927398713", true},
		{"79927398710", "79927398710", false},
		{"79927398711", "79927398711", false},
		// Boundary / empty cases.
		{"empty string", "", false},
		{"all non-digits", "abc-def", false},
		// Single digit cases (only "0" is divisible by 10).
		{"digit 0 alone", "0", true},
		{"digit 5 alone", "5", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := luhnValid(c.s); got != c.want {
				t.Errorf("luhnValid(%q) = %v; want %v", c.s, got, c.want)
			}
		})
	}
}

// TestLuhnCheckDigit covers the inverse — given a digit prefix,
// produce the check digit that completes a valid number. Each
// computed result must satisfy [luhnValid] when concatenated.
func TestLuhnCheckDigit(t *testing.T) {
	cases := []struct {
		name   string
		prefix string
		want   int
	}{
		// Known canonical example from Wikipedia: prefix "7992739871"
		// has check digit 3 (resulting in "79927398713").
		{"79927398713 base", "7992739871", 3},
		// AmEx-15 test number ends with 5.
		{"AmEx test base", "37828224631000", 5},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := luhnCheckDigit(c.prefix)
			if got != c.want {
				t.Errorf("luhnCheckDigit(%q) = %d; want %d", c.prefix, got, c.want)
			}
			// Round-trip: prefix + got must be Luhn-valid.
			complete := c.prefix + string(rune('0'+got))
			if !luhnValid(complete) {
				t.Errorf("computed completion %q is not Luhn-valid", complete)
			}
		})
	}

	t.Run("range invariant: every 14-digit prefix yields a valid completion", func(t *testing.T) {
		// Sweep a handful of 14-digit prefixes to confirm the helper
		// always produces a digit in [0,9] that closes the Luhn sum.
		prefixes := []string{
			"00000000000000",
			"12345678901234",
			"99999999999999",
			"41111111111111",
			"55555555555555",
		}
		for _, p := range prefixes {
			d := luhnCheckDigit(p)
			if d < 0 || d > 9 {
				t.Errorf("check digit out of range for %q: %d", p, d)
			}
			if !luhnValid(p + string(rune('0'+d))) {
				t.Errorf("computed completion for %q is not Luhn-valid", p)
			}
		}
	})
}
