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
