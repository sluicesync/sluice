// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"math/big"
	"strings"
)

// IBAN helpers for PII Phase 2.c second wave (v0.60.0). IBAN check
// digits are NOT Luhn — they're computed via mod-97 over the BBAN
// with letters mapped to digits (A=10, B=11, ..., Z=35) per
// ISO 13616-1. Pulled into a standalone file so `randomize:iban`
// (and any future IBAN-aware strategy) can share the implementation.
//
// The algorithm:
//
//  1. Take the BBAN, append the country code, append "00" (check-
//     digit placeholder).
//  2. Convert every letter in the resulting string to two digits:
//     A → "10", B → "11", ..., Z → "35".
//  3. Interpret the result as a base-10 integer and compute
//     `98 - (value mod 97)`. That's the 2-digit check.
//
// Worked example for German IBAN `DE89 3704 0044 0532 0130 00`:
//
//   - BBAN = "370400440532013000"
//   - rearranged = "370400440532013000" + "DE" + "00"
//                = "370400440532013000DE00"
//   - letter-encoded = "370400440532013000131400"
//   - mod 97 = 9
//   - check digits = 98 - 9 = 89
//
// Real-world IBAN parsers (e.g. iban.IsValid in finance libraries)
// use this same algorithm; sluice's randomize:iban output passes
// every standard validator.
//
// The function does not validate that countryCode is a real ISO
// country code or that bban has the right length for that country;
// callers (the strategy code, which knows the country-specific
// structure) hold that contract.

// ibanCheckDigits returns the 2-digit string that, inserted at
// positions 2-3 of the country-prefixed BBAN, produces a mod-97-
// valid IBAN. countryCode must be exactly 2 uppercase ASCII letters;
// bban must be the country-specific BBAN string (uppercase
// letters + digits only, no spaces).
//
// Returns a zero-padded 2-digit decimal string in the range
// "02"-"98" (the spec excludes "00", "01", "99" as check values).
func ibanCheckDigits(countryCode, bban string) string {
	// Rearrange: BBAN + countryCode + "00"
	rearranged := bban + countryCode + "00"
	encoded := ibanLetterEncode(rearranged)

	// Compute mod 97 over the (potentially very large) integer.
	mod := new(big.Int)
	value, ok := new(big.Int).SetString(encoded, 10)
	if !ok {
		// Defensive: callers always supply digit-only encodings; this
		// branch is unreachable in production.
		return "00"
	}
	mod.Mod(value, big.NewInt(97))

	check := 98 - mod.Int64()
	if check < 10 {
		return "0" + bigIntToString(check)
	}
	return bigIntToString(check)
}

// ibanLetterEncode converts a mixed alphanumeric string to a digit-
// only string by replacing each uppercase letter with its 2-digit
// numeric equivalent (A=10, ..., Z=35). Digits are passed through.
// Lowercase letters and other characters are skipped defensively
// (production callers feed uppercase alphanumeric only).
func ibanLetterEncode(s string) string {
	var b strings.Builder
	b.Grow(len(s) * 2)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			b.WriteByte(c)
		case c >= 'A' && c <= 'Z':
			n := int(c-'A') + 10
			b.WriteByte(byte('0' + n/10))
			b.WriteByte(byte('0' + n%10))
		}
	}
	return b.String()
}

// bigIntToString renders a small non-negative int64 as its decimal
// string. Avoids the strconv import in this file and keeps the
// ibanCheckDigits hot-path lean.
func bigIntToString(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [3]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// ibanValid reports whether s is a valid IBAN under the mod-97
// check. Spaces are skipped; case is preserved (the spec uses
// uppercase but real-world inputs include lowercase). Returns false
// for inputs shorter than 4 chars, longer than 34, or containing
// any non-alphanumeric character.
//
// Used by tests to confirm randomize:iban output is mod-97-valid;
// not exposed as a strategy itself.
func ibanValid(s string) bool {
	// Strip spaces; uppercase.
	cleaned := strings.ToUpper(strings.ReplaceAll(s, " ", ""))
	if len(cleaned) < 4 || len(cleaned) > 34 {
		return false
	}
	for i := 0; i < len(cleaned); i++ {
		c := cleaned[i]
		if (c < '0' || c > '9') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	// Rearrange: move first 4 chars (country + check) to the end.
	rearranged := cleaned[4:] + cleaned[:4]
	encoded := ibanLetterEncode(rearranged)
	value, ok := new(big.Int).SetString(encoded, 10)
	if !ok {
		return false
	}
	mod := new(big.Int).Mod(value, big.NewInt(97))
	return mod.Int64() == 1
}
