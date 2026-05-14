// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

// Luhn-checksum helpers for PII Phase 2 format-preserving / random
// strategies (PAN, Canadian SIN — both use Luhn). Pulled out as a
// standalone file so future strategies (`mask:pan`, `mask:ca-sin`,
// `randomize:pan`, `randomize:ca-sin`) can share the implementation
// without duplicating it.

// luhnValid reports whether s is a numeric string that passes the
// Luhn checksum. Non-digit characters are skipped (operator-friendly
// for "4111-1111-1111-1111" style inputs); the result is determined
// by the remaining digits only. An empty digit-set returns false.
//
// The Luhn algorithm:
//
//  1. Walk the digits right-to-left.
//  2. Double every second digit; if the doubled value exceeds 9,
//     subtract 9.
//  3. Sum all digits.
//  4. The total must be divisible by 10.
//
// This is the standard ISO/IEC 7812 checksum used by credit-card
// PANs, Canadian SINs, IMEIs, and several national IDs.
func luhnValid(s string) bool {
	sum := 0
	digits := 0
	double := false
	for i := len(s) - 1; i >= 0; i-- {
		c := s[i]
		if c < '0' || c > '9' {
			continue
		}
		d := int(c - '0')
		if double {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		double = !double
		digits++
	}
	if digits == 0 {
		return false
	}
	return sum%10 == 0
}

// luhnCheckDigit returns the check digit that, appended to digits,
// produces a Luhn-valid string. Used by randomize:pan and
// randomize:ca-sin to generate synthetic-yet-valid identifiers.
//
// digits must be a numeric string of length N (no separators).
// Returns the digit value 0-9 such that `digits + string(digit)`
// passes [luhnValid].
//
// The math: compute the Luhn sum as if `digits` were the leading
// part of a valid number (so the FIRST doubled digit is the
// rightmost of `digits` — same parity as if a check digit had been
// appended). The check digit is `(10 - sum%10) % 10`.
func luhnCheckDigit(digits string) int {
	sum := 0
	// As if a check digit will be appended: walk `digits`
	// right-to-left starting with double=true (the next-to-last
	// position of a complete N+1-length Luhn string).
	double := true
	for i := len(digits) - 1; i >= 0; i-- {
		c := digits[i]
		if c < '0' || c > '9' {
			// Caller misuse — non-digit input. Skip the character
			// rather than panic; downstream callers (random PAN
			// generators) feed us digit-only strings so this branch
			// is defensive.
			continue
		}
		d := int(c - '0')
		if double {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		double = !double
	}
	return (10 - sum%10) % 10
}
