// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// This file holds PII Phase 2.b country/format-specific mask
// presets. Each preset is a no-options Strategy that validates the
// input shape and applies a fixed-form masking pattern. Operators
// use them when the column has a known PII shape (SSN, PAN, email)
// and want a one-liner instead of stacking generic mask:inner with
// careful margin counting.
//
// Inspired by MySQL Enterprise's data-masking-component functions
// (mask_ssn, mask_pan, mask_pan_relaxed) plus a sluice-native
// mask:email which Enterprise doesn't ship. See the Phase 2 prep
// doc (docs/dev/notes/prep-pii-redaction-phase-2-strategy-catalog.md)
// for the full catalog mapping.
//
// Each preset refuses non-conforming input at runtime with an
// operator-actionable error. The strict refusal matches Phase 1
// strategies' style: misconfiguration fails loudly, never silently.

// MaskSSN is the US Social Security Number preset. Accepts input
// in the canonical `XXX-XX-XXXX` shape (9 digits split by two
// hyphens); outputs `XXX-XX-NNNN` with the first 5 digits masked
// to `X` and the last 4 preserved verbatim. Hyphens are preserved.
//
// Refuses input that doesn't match the 3-2-4 dash pattern or that
// has non-digit characters in the digit positions. Operators with
// SSN columns stored without dashes should use a generic
// `mask:inner:0,4` (which is rune-counted and length-preserving)
// instead. The strict shape check exists because real-world SSN
// columns are usually stored either dashed (`XXX-XX-XXXX`) or
// undashed (`XXXXXXXXX`) consistently; mixing the two in the same
// column is rare enough that refusing the wrong shape catches
// operator misconfiguration faster than silently producing weird
// output.
type MaskSSN struct{}

// Name returns "mask:ssn".
func (MaskSSN) Name() string { return "mask:ssn" }

// Redact applies the SSN mask. Refuses non-string input or
// shape-mismatched input.
func (MaskSSN) Redact(col *ir.Column, val any) (any, error) {
	if val == nil {
		return nil, nil
	}
	s, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("redact: column %s has unsupported type %T for mask:ssn strategy (only string is supported)", colIdentity(col), val)
	}
	// Validate canonical XXX-XX-XXXX shape: exactly 11 characters,
	// hyphens at positions 3 and 6, digits elsewhere.
	if len(s) != 11 || s[3] != '-' || s[6] != '-' {
		return nil, fmt.Errorf("redact: column %s value does not match SSN format 'XXX-XX-XXXX' (got %d chars); use mask:inner if the column stores SSNs without dashes", colIdentity(col), len(s))
	}
	for i, r := range s {
		if i == 3 || i == 6 {
			continue
		}
		if r < '0' || r > '9' {
			return nil, fmt.Errorf("redact: column %s value has non-digit at SSN position %d; expected XXX-XX-XXXX", colIdentity(col), i)
		}
	}
	// Preserve the last 4 (positions 7-10); mask positions 0-1-2 and
	// 4-5 with 'X'. The hyphens stay at 3 and 6.
	return "XXX-XX-" + s[7:], nil
}

// MaskPAN is the strict payment-card PAN preset. Validates the
// Luhn checksum first (using [luhnValid] which skips non-digit
// characters); then masks all digits except the first 6 (BIN) and
// last 4 (account suffix). Non-digit characters (spaces, hyphens)
// are preserved at their original positions.
//
// PAN length per ISO/IEC 7812 ranges from 12 (older Maestro cards)
// to 19 digits (some bank-card variants). The strict variant
// refuses Luhn-invalid inputs because real PAN columns must satisfy
// the checksum; a Luhn-invalid value is either operator-supplied
// synthetic test data (use `mask:pan-relaxed` instead) or
// upstream-corrupt data (worth catching at masking-time, not
// silently passing through).
//
// Examples (defaulting char to 'X'):
//
//   - "4111111111111111" → "411111XXXXXX1111"
//   - "4111 1111 1111 1111" → "411111 XXXX XXXX 1111" (spaces preserved)
//   - "4111-1111-1111-1112" → refused (bad Luhn checksum)
type MaskPAN struct{}

// Name returns "mask:pan".
func (MaskPAN) Name() string { return "mask:pan" }

// Redact applies the strict PAN mask. Refuses non-string input,
// Luhn-invalid input, or input with fewer than 12 / more than 19
// digits.
func (MaskPAN) Redact(col *ir.Column, val any) (any, error) {
	return maskPAN(col, val, true)
}

// MaskPANRelaxed is the lenient PAN preset. Same masking shape as
// [MaskPAN] (preserves first 6 + last 4 digits; preserves non-digit
// chars at their positions) but skips Luhn validation. Useful for:
//
//   - Synthetic test data that doesn't satisfy the checksum.
//   - PAN columns containing tokenized values that look like PANs
//     but use a different validation scheme.
//   - Operator confidence in upstream data quality (skipping the
//     check shaves a few ns per row at scale).
//
// Real production PAN columns should use [MaskPAN] (the strict
// variant) so checksum-broken data is caught at masking time.
type MaskPANRelaxed struct{}

// Name returns "mask:pan-relaxed".
func (MaskPANRelaxed) Name() string { return "mask:pan-relaxed" }

// Redact applies the lenient PAN mask. Refuses non-string input
// and inputs with fewer than 12 / more than 19 digits but does NOT
// check the Luhn checksum.
func (MaskPANRelaxed) Redact(col *ir.Column, val any) (any, error) {
	return maskPAN(col, val, false)
}

// maskPAN implements the shared masking logic for [MaskPAN] /
// [MaskPANRelaxed]. validateLuhn=true gates the strict variant's
// extra check.
func maskPAN(col *ir.Column, val any, validateLuhn bool) (any, error) {
	if val == nil {
		return nil, nil
	}
	s, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("redact: column %s has unsupported type %T for mask:pan strategy (only string is supported)", colIdentity(col), val)
	}
	// Count digits to validate length range (12-19 per ISO/IEC 7812).
	digitCount := 0
	for _, r := range s {
		if r >= '0' && r <= '9' {
			digitCount++
		}
	}
	if digitCount < 12 || digitCount > 19 {
		return nil, fmt.Errorf("redact: column %s value has %d digits; PAN requires 12-19 digits per ISO/IEC 7812", colIdentity(col), digitCount)
	}
	if validateLuhn && !luhnValid(s) {
		return nil, fmt.Errorf("redact: column %s value fails Luhn checksum; use mask:pan-relaxed if the column intentionally stores Luhn-invalid (e.g., synthetic test) PANs", colIdentity(col))
	}
	// Walk the runes; preserve non-digit chars at their positions;
	// for digits, preserve the first 6 (BIN) and last 4 (suffix),
	// mask middle digits with 'X'.
	runes := []rune(s)
	out := make([]rune, len(runes))
	seenDigit := 0
	digitsToPreserveAtEnd := 4
	preservedFromStart := 6
	middleDigitCount := digitCount - preservedFromStart - digitsToPreserveAtEnd
	for i, r := range runes {
		if r < '0' || r > '9' {
			out[i] = r
			continue
		}
		seenDigit++
		switch {
		case seenDigit <= preservedFromStart:
			out[i] = r
		case seenDigit <= preservedFromStart+middleDigitCount:
			out[i] = 'X'
		default:
			out[i] = r
		}
	}
	return string(out), nil
}

// MaskEmail is the sluice-native email preset (MySQL Enterprise
// does not ship an equivalent). Preserves the first character of
// the local part (mailbox), masks the rest of the mailbox with
// `X`, and preserves the entire `@domain` suffix verbatim.
//
// Examples:
//
//   - "alice@example.com" → "aXXXX@example.com"
//   - "a@x.test"          → "a@x.test" (single-char mailbox; nothing to mask)
//   - "support+billing@example.com" → "sXXXXXXXXXXXXXXX@example.com"
//
// Refuses input without an `@` (not a valid email shape; operator
// likely wanted a different strategy). The first-char-preserved
// shape matches operator intent for log-friendly email surrogates
// — the mailbox initial helps with grouping/correlation without
// exposing the full address. Operators wanting larger preserved
// prefixes can stack the generic `mask:inner` on local-part-only
// columns instead.
type MaskEmail struct{}

// Name returns "mask:email".
func (MaskEmail) Name() string { return "mask:email" }

// Redact applies the email mask. Refuses non-string input or input
// without an `@` separator.
func (MaskEmail) Redact(col *ir.Column, val any) (any, error) {
	if val == nil {
		return nil, nil
	}
	s, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("redact: column %s has unsupported type %T for mask:email strategy (only string is supported)", colIdentity(col), val)
	}
	// Use LAST '@' so quoted-local-part addresses ("a@b"@example.com)
	// resolve at the right boundary. RFC 5322 technically allows
	// multiple '@' inside quotes, but the rightmost '@' always
	// separates local-part from domain.
	at := strings.LastIndex(s, "@")
	if at < 0 {
		return nil, fmt.Errorf("redact: column %s value has no '@' character; not a valid email shape (use a different strategy for non-email columns)", colIdentity(col))
	}
	if at == 0 {
		return nil, fmt.Errorf("redact: column %s value has empty local part (starts with '@'); cannot apply mask:email", colIdentity(col))
	}
	local := s[:at]
	domain := s[at:] // includes the '@'
	// Preserve first rune of the local part; mask the rest with 'X'.
	// Rune-counted to handle non-ASCII mailboxes safely.
	localRunes := []rune(local)
	if len(localRunes) <= 1 {
		// Single-char mailbox — nothing to mask; the first char IS
		// the whole mailbox. Return unchanged (the domain still
		// gets preserved, which is the intent).
		return s, nil
	}
	out := make([]rune, len(localRunes))
	out[0] = localRunes[0]
	for i := 1; i < len(localRunes); i++ {
		out[i] = 'X'
	}
	return string(out) + domain, nil
}

// MaskCASIN is the Canadian Social Insurance Number preset. Accepts
// input in the canonical `XXX-XXX-XXX` (dashed) or `XXXXXXXXX`
// (undashed) shape — both are common in real-world Canadian PII
// columns. Validates the Luhn checksum (CA SINs MUST satisfy it
// per the Government of Canada's SIN structure rules); preserves
// the last 3 digits; masks the first 6 digits with `X`. Dashes,
// when present, stay at their original positions.
//
// Examples:
//
//   - "046-454-286" → "XXX-XXX-286" (real Luhn-valid SIN format from Wikipedia)
//   - "046454286"   → "XXXXXX286"
//   - "046-454-287" → refused (bad Luhn checksum)
//
// Refuses non-conforming length, non-digit body, or Luhn-invalid
// inputs. Operators with synthetic test data that doesn't satisfy
// Luhn should use generic `mask:inner:0,3` instead.
type MaskCASIN struct{}

// Name returns "mask:ca-sin".
func (MaskCASIN) Name() string { return "mask:ca-sin" }

// Redact applies the CA SIN mask.
func (MaskCASIN) Redact(col *ir.Column, val any) (any, error) {
	if val == nil {
		return nil, nil
	}
	s, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("redact: column %s has unsupported type %T for mask:ca-sin strategy (only string is supported)", colIdentity(col), val)
	}
	// Accept dashed (`XXX-XXX-XXX`, 11 chars) or undashed (9 digits).
	hasDashes := len(s) == 11 && s[3] == '-' && s[7] == '-'
	if !hasDashes && len(s) != 9 {
		return nil, fmt.Errorf("redact: column %s value does not match SIN format 'XXX-XXX-XXX' (dashed) or 'XXXXXXXXX' (undashed); got %d chars", colIdentity(col), len(s))
	}
	// All non-dash positions must be digits.
	for i, r := range s {
		if hasDashes && (i == 3 || i == 7) {
			continue
		}
		if r < '0' || r > '9' {
			return nil, fmt.Errorf("redact: column %s value has non-digit at SIN position %d", colIdentity(col), i)
		}
	}
	if !luhnValid(s) {
		return nil, fmt.Errorf("redact: column %s value fails Luhn checksum; CA SINs must satisfy Luhn (use mask:inner:0,3 for synthetic non-Luhn data)", colIdentity(col))
	}
	if hasDashes {
		return "XXX-XXX-" + s[8:], nil
	}
	return "XXXXXX" + s[6:], nil
}

// MaskUKNIN is the UK National Insurance Number preset. Accepts
// the canonical `AA999999A` 9-char shape — 2 uppercase prefix
// letters + 6 digits + 1 uppercase suffix letter. Preserves the
// first 2 letters + the suffix letter; masks the 6 middle digits
// with `X`.
//
// Examples:
//
//   - "AB123456C" → "ABXXXXXXC"
//
// Refuses non-conforming length, wrong-position letter/digit, or
// suffix-letter outside the valid set A/B/C/D. (HMRC reserves
// other suffix letters for administrative use — we treat them as
// shape violations to surface operator misconfiguration.) Prefix
// letter validation is NOT enforced here because the valid prefix
// set is large and changes over time; HMRC's authoritative list is
// out of scope.
type MaskUKNIN struct{}

// Name returns "mask:uk-nin".
func (MaskUKNIN) Name() string { return "mask:uk-nin" }

// Redact applies the UK NIN mask.
func (MaskUKNIN) Redact(col *ir.Column, val any) (any, error) {
	if val == nil {
		return nil, nil
	}
	s, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("redact: column %s has unsupported type %T for mask:uk-nin strategy (only string is supported)", colIdentity(col), val)
	}
	if len(s) != 9 {
		return nil, fmt.Errorf("redact: column %s value is %d chars; UK NIN requires exactly 9 chars (AA999999A)", colIdentity(col), len(s))
	}
	// Positions 0,1: uppercase letters (prefix).
	// Positions 2-7: digits.
	// Position 8: uppercase A/B/C/D (suffix).
	for i := 0; i < 2; i++ {
		if s[i] < 'A' || s[i] > 'Z' {
			return nil, fmt.Errorf("redact: column %s value position %d is not an uppercase letter; UK NIN prefix must be 2 letters", colIdentity(col), i)
		}
	}
	for i := 2; i < 8; i++ {
		if s[i] < '0' || s[i] > '9' {
			return nil, fmt.Errorf("redact: column %s value position %d is not a digit; UK NIN must have 6 digits between prefix and suffix", colIdentity(col), i)
		}
	}
	switch s[8] {
	case 'A', 'B', 'C', 'D':
		// ok
	default:
		return nil, fmt.Errorf("redact: column %s value suffix %q is not one of A/B/C/D; UK NIN suffix is restricted", colIdentity(col), s[8])
	}
	// Preserve positions 0,1,8; mask digits at 2-7.
	return s[:2] + "XXXXXX" + s[8:], nil
}

// MaskIBAN is the International Bank Account Number preset.
// IBANs are 15-34 chars: 2 uppercase country-code letters + 2
// numeric check digits + a country-specific BBAN (Basic Bank
// Account Number) of variable length. Preserves the country code
// (positions 0-1) + the check digits (positions 2-3) + the first
// 2 chars of the BBAN (positions 4-5) + the last 4 chars; masks
// everything else with `X`.
//
// Example: German IBAN `DE89370400440532013000` (22 chars) →
// `DE8937XXXXXXXXXXXX3000`. The pattern preserves enough
// information to confirm bank routing (country + bank code prefix)
// + the account-suffix slice useful for support-case lookups,
// while hiding the bulk of the account identifier.
//
// Refuses non-conforming length (< 15 or > 34), missing country-
// code letters at positions 0-1, or non-digit check digits at
// positions 2-3.
//
// Per-country structural validation (e.g., German IBANs are
// always 22 chars, UK 22 chars, etc.) is NOT enforced here —
// the ISO 13616 spec allows lengths 15-34 and country-specific
// length rules change over time. Operators wanting strict
// per-country length checks should preflight at ingest time.
type MaskIBAN struct{}

// Name returns "mask:iban".
func (MaskIBAN) Name() string { return "mask:iban" }

// Redact applies the IBAN mask.
func (MaskIBAN) Redact(col *ir.Column, val any) (any, error) {
	if val == nil {
		return nil, nil
	}
	s, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("redact: column %s has unsupported type %T for mask:iban strategy (only string is supported)", colIdentity(col), val)
	}
	// Strip any spaces (some IBAN representations group in 4s with
	// spaces); validate against the compact form. Output preserves
	// spaces if present.
	compact := strings.ReplaceAll(s, " ", "")
	if len(compact) < 15 || len(compact) > 34 {
		return nil, fmt.Errorf("redact: column %s value has %d chars (compact); IBAN length must be 15-34 per ISO 13616", colIdentity(col), len(compact))
	}
	// Country code (positions 0-1): uppercase letters.
	if compact[0] < 'A' || compact[0] > 'Z' || compact[1] < 'A' || compact[1] > 'Z' {
		return nil, fmt.Errorf("redact: column %s value does not start with 2 uppercase country-code letters; not an IBAN shape", colIdentity(col))
	}
	// Check digits (positions 2-3): digits.
	if compact[2] < '0' || compact[2] > '9' || compact[3] < '0' || compact[3] > '9' {
		return nil, fmt.Errorf("redact: column %s value positions 2-3 are not numeric check digits; not an IBAN shape", colIdentity(col))
	}
	// Mask in compact form: preserve [0:6] + last 4; mask middle.
	keepFromStart := 6 // country code + check digits + 2 BBAN
	keepFromEnd := 4
	masked := make([]byte, len(compact))
	for i := 0; i < len(compact); i++ {
		switch {
		case i < keepFromStart:
			masked[i] = compact[i]
		case i >= len(compact)-keepFromEnd:
			masked[i] = compact[i]
		default:
			masked[i] = 'X'
		}
	}
	// If input had spaces, re-insert them at original positions so
	// the output is the same shape as the input.
	if strings.Contains(s, " ") {
		return reinsertSpaces(s, string(masked)), nil
	}
	return string(masked), nil
}

// reinsertSpaces takes original (with spaces) + compact (with the
// masking applied) and produces a string of the same length as
// original, with spaces re-inserted at the original positions.
// Used by [MaskIBAN] to preserve operator-supplied space grouping.
func reinsertSpaces(original, compact string) string {
	out := make([]byte, len(original))
	ci := 0
	for i := 0; i < len(original); i++ {
		if original[i] == ' ' {
			out[i] = ' '
		} else {
			out[i] = compact[ci]
			ci++
		}
	}
	return string(out)
}

// MaskUUID is the UUID preset. Accepts the canonical 36-char
// 8-4-4-4-12 hyphenated form (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`).
// Preserves the 4 hyphens + the first 4 chars + the last 4 chars;
// masks all other hex digits with `X`.
//
// Example: `550e8400-e29b-41d4-a716-446655440000` →
// `550eXXXX-XXXX-XXXX-XXXX-XXXXXXXX0000`.
//
// Refuses non-36-char input, hyphens at wrong positions, or
// non-hex characters in the digit positions. The strict shape
// check matches operator intent — a UUID column that doesn't hold
// canonical UUIDs is usually a configuration mistake worth
// catching loudly.
type MaskUUID struct{}

// Name returns "mask:uuid".
func (MaskUUID) Name() string { return "mask:uuid" }

// Redact applies the UUID mask.
func (MaskUUID) Redact(col *ir.Column, val any) (any, error) {
	if val == nil {
		return nil, nil
	}
	s, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("redact: column %s has unsupported type %T for mask:uuid strategy (only string is supported)", colIdentity(col), val)
	}
	if len(s) != 36 {
		return nil, fmt.Errorf("redact: column %s value is %d chars; UUID canonical form requires 36 chars (8-4-4-4-12 hyphenated)", colIdentity(col), len(s))
	}
	// Hyphens at positions 8, 13, 18, 23.
	for _, pos := range []int{8, 13, 18, 23} {
		if s[pos] != '-' {
			return nil, fmt.Errorf("redact: column %s value missing hyphen at position %d; UUID canonical form is 8-4-4-4-12", colIdentity(col), pos)
		}
	}
	// All other positions must be hex digits (0-9, a-f, A-F).
	for i := 0; i < 36; i++ {
		switch i {
		case 8, 13, 18, 23:
			continue
		}
		c := s[i]
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
		if !isHex {
			return nil, fmt.Errorf("redact: column %s value position %d (%q) is not a hex digit; UUID requires hex chars in all non-hyphen positions", colIdentity(col), i, c)
		}
	}
	// Mask: preserve positions 0-3 (first 4 hex) + 32-35 (last 4 hex)
	// + the 4 hyphens. Mask everything else with 'X'.
	out := make([]byte, 36)
	for i := 0; i < 36; i++ {
		switch {
		case i == 8 || i == 13 || i == 18 || i == 23:
			out[i] = '-'
		case i < 4:
			out[i] = s[i]
		case i >= 32:
			out[i] = s[i]
		default:
			out[i] = 'X'
		}
	}
	return string(out), nil
}
