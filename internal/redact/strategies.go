// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/orware/sluice/internal/ir"
)

// Null replaces the value with NULL. Refuses at runtime if the
// target column is NOT NULL — the operator should pick a different
// strategy (e.g. `static:` with an empty-equivalent value) for
// non-nullable PII columns. The refusal is loud (returns an error)
// rather than silent so the operator notices misconfiguration
// before data flows.
type Null struct{}

// Name returns the stable identifier "null".
func (Null) Name() string { return "null" }

// Redact returns (nil, nil) when col is nullable; otherwise returns
// (nil, error) with the column identity in the message.
func (Null) Redact(col *ir.Column, _ any, _ []byte) (any, error) {
	if col != nil && !col.Nullable {
		return nil, fmt.Errorf("redact: column %s is NOT NULL; refusing to redact via 'null' (use 'static:<empty-equivalent>' instead)", colIdentity(col))
	}
	return nil, nil
}

// Static replaces every value with a literal constant. The Value
// is stored as a string; the per-engine prepareValue downstream
// handles type coercion to the column's IR type. Phase 1 keeps the
// coercion at the engine layer (where the existing prepareValue
// already does it for ir.Row's untyped values).
type Static struct {
	Value string
}

// Name returns "static:<elided>" — the actual replacement value is
// NOT included in the strategy name to avoid leaking PII into log
// lines that record the audit configuration. (The strategy name
// is logged at stream startup; the operator can read the YAML or
// the CLI flag if they need to confirm the exact replacement.)
func (Static) Name() string { return "static:<elided>" }

// Redact returns the configured Value.
func (s Static) Redact(_ *ir.Column, _ any, _ []byte) (any, error) {
	return s.Value, nil
}

// Hash applies a one-way hash. Algo is "sha256" (stateless,
// deterministic) or "hmac-sha256" (keyed, deterministic per
// (Key, input) pair). Output is hex-encoded — width-stable for
// SHA-256 (64 hex chars) which simplifies target schema sizing.
//
// The Phase 1 acceptable input shapes:
//
//   - string: hashed directly via the UTF-8 byte representation.
//   - []byte: hashed directly.
//   - nil: passed through verbatim (Hash on NULL is a no-op; the
//     value stays NULL on the target). Operators wanting NULL to
//     produce a specific surrogate should use `static:` instead.
//
// All other input shapes (numeric types, booleans, etc.) return
// an error naming the column + the value's Go type. Real-world PII
// columns are almost always string-typed (email, name, address,
// phone, SSN); the strict-type check catches operator
// misconfiguration early.
type Hash struct {
	Algo string
	Key  []byte // empty for sha256; non-empty for hmac-sha256
}

// Name returns "hash:<algo>".
func (h Hash) Name() string { return "hash:" + h.Algo }

// Redact hashes the input and returns the hex-encoded digest as a
// string. See the type's doc-comment for the accepted input shapes.
func (h Hash) Redact(col *ir.Column, val any, _ []byte) (any, error) {
	if val == nil {
		return nil, nil
	}
	var data []byte
	switch v := val.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		return nil, fmt.Errorf("redact: column %s has unsupported type %T for hash strategy (only string and []byte are supported)", colIdentity(col), val)
	}
	switch h.Algo {
	case "sha256":
		sum := sha256.Sum256(data)
		return hex.EncodeToString(sum[:]), nil
	case "hmac-sha256":
		if len(h.Key) == 0 {
			return nil, fmt.Errorf("redact: column %s declared with 'hash:hmac-sha256' but Key is empty; ensure --redact-key-source provides a non-empty key", colIdentity(col))
		}
		m := hmac.New(sha256.New, h.Key)
		_, _ = m.Write(data)
		return hex.EncodeToString(m.Sum(nil)), nil
	default:
		return nil, fmt.Errorf("redact: column %s declared with unknown hash algorithm %q (supported: sha256, hmac-sha256)", colIdentity(col), h.Algo)
	}
}

// Truncate keeps the first N runes (not bytes) of a string value.
// Returns the input verbatim if it's already shorter than N runes
// or if it's nil. Refuses non-string input at runtime; the typical
// real-world misuse is to apply truncate to an integer or BLOB
// column, which should fail loudly.
//
// Rune-based (not byte-based) truncation matters for non-ASCII
// content — a UTF-8 email "ñ@example.com" truncated to 4 should
// produce "ñ@ex", not "ñ@e" (which is what naive byte slicing of
// a 2-byte ñ would yield).
type Truncate struct {
	N int
}

// Name returns "truncate:<n>".
func (t Truncate) Name() string { return fmt.Sprintf("truncate:%d", t.N) }

// Redact returns the first t.N runes of the input.
func (t Truncate) Redact(col *ir.Column, val any, _ []byte) (any, error) {
	if val == nil {
		return nil, nil
	}
	s, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("redact: column %s has unsupported type %T for truncate strategy (only string is supported)", colIdentity(col), val)
	}
	if t.N <= 0 {
		// truncate:0 is "empty out"; truncate:-N is operator error
		// caught at CLI parse time. Treat negatives as 0 defensively.
		return "", nil
	}
	runes := []rune(s)
	if len(runes) <= t.N {
		return s, nil
	}
	return string(runes[:t.N]), nil
}

// colIdentity formats the column identity for error messages.
// Returns "<unknown>" when col is nil (defensive — shouldn't
// happen in production but tests can pass nil to exercise the
// strategy in isolation).
func colIdentity(col *ir.Column) string {
	if col == nil {
		return "<unknown>"
	}
	return col.Name
}

// MaskForm enumerates Mask's two variants. The values mirror MySQL
// Enterprise's `mask_inner` / `mask_outer` function names per the
// PII Phase 2 strategy catalog.
type MaskForm uint8

// Mask form constants.
const (
	// MaskInner keeps the first M1 + last M2 characters and masks
	// the middle. Used for "show last 4 of an SSN/PAN" style
	// redaction.
	MaskInner MaskForm = iota
	// MaskOuter masks the first M1 + last M2 characters and keeps
	// the middle. Inverse of MaskInner; less commonly used in
	// real-world PII workflows but matches MySQL Enterprise.
	MaskOuter
)

// String returns the textual form ("inner" / "outer") used in
// [Mask.Name].
func (f MaskForm) String() string {
	switch f {
	case MaskInner:
		return "inner"
	case MaskOuter:
		return "outer"
	default:
		return "unknown"
	}
}

// Mask applies format-preserving redaction by replacing characters
// with a mask character while preserving the original length and
// non-masked chars verbatim. Two forms (PII Phase 2.a, roadmap item
// 15a Phase 2):
//
//   - [MaskInner] keeps the first M1 + last M2 characters and masks
//     the middle with [Mask.Char] (default `X`). Real-world use:
//     redact a credit card to `4111XXXXXXXXXXX1111`, an SSN to
//     `XXX-XX-1234` (with M1=0, M2=4), or an email's mailbox while
//     keeping its domain (M1=1, M2=len(@domain), char=`X`).
//   - [MaskOuter] masks the first M1 + last M2 characters and keeps
//     the middle. Less common; included for parity with MySQL
//     Enterprise.
//
// Mask preserves the input's rune-length: a 16-char PAN stays
// 16-char on output. Operators wanting variable-length output
// should use [Truncate] or [Static] instead.
//
// Accepted input shapes:
//
//   - string: masked rune-wise (UTF-8 + emoji safe).
//   - nil: passed through verbatim.
//
// All other input shapes (numeric types, []byte, etc.) return an
// error naming the column + the value's Go type. Real-world PII
// columns for masking are almost always string-typed (PAN, SSN,
// phone, email); strict-type check catches operator
// misconfiguration early.
//
// Boundary cases:
//
//   - M1 + M2 >= rune-length: the entire value is preserved
//     (nothing to mask). No-op return.
//   - M1 or M2 < 0: treated as 0 defensively.
//   - Char is one rune; multi-rune Char defaults to the first rune.
//     Empty Char defaults to `X`.
type Mask struct {
	// Form is MaskInner or MaskOuter; see the type doc.
	Form MaskForm
	// M1 is the "first N chars" margin. Operator-supplied via
	// `mask:inner:4,4` or `mask:outer:4,4`.
	M1 int
	// M2 is the "last N chars" margin.
	M2 int
	// Char is the mask character. Defaults to `X` when empty. Only
	// the first rune is consumed.
	Char string
}

// Name returns "mask:<form>:<M1>,<M2>" — the audit-log identifier.
// The mask character is NOT included in the name since it's
// uninteresting from an audit perspective (the strategy still
// fully describes what was applied: form + margins).
func (m Mask) Name() string {
	return "mask:" + m.Form.String() + ":" + strconv.Itoa(m.M1) + "," + strconv.Itoa(m.M2)
}

// Redact applies the mask. See the type doc for boundary semantics.
func (m Mask) Redact(col *ir.Column, val any, _ []byte) (any, error) {
	if val == nil {
		return nil, nil
	}
	s, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("redact: column %s has unsupported type %T for mask:%s strategy (only string is supported)", colIdentity(col), val, m.Form)
	}
	runes := []rune(s)
	m1 := m.M1
	m2 := m.M2
	if m1 < 0 {
		m1 = 0
	}
	if m2 < 0 {
		m2 = 0
	}
	mc := maskRune(m.Char)
	switch m.Form {
	case MaskInner:
		// Keep first m1 + last m2; mask middle.
		if m1+m2 >= len(runes) {
			return s, nil // nothing to mask
		}
		out := make([]rune, len(runes))
		copy(out, runes[:m1])
		for i := m1; i < len(runes)-m2; i++ {
			out[i] = mc
		}
		copy(out[len(runes)-m2:], runes[len(runes)-m2:])
		return string(out), nil
	case MaskOuter:
		// Mask first m1 + last m2; keep middle.
		if m1+m2 >= len(runes) {
			// Whole value is masked.
			out := make([]rune, len(runes))
			for i := range out {
				out[i] = mc
			}
			return string(out), nil
		}
		out := make([]rune, len(runes))
		for i := 0; i < m1; i++ {
			out[i] = mc
		}
		copy(out[m1:len(runes)-m2], runes[m1:len(runes)-m2])
		for i := len(runes) - m2; i < len(runes); i++ {
			out[i] = mc
		}
		return string(out), nil
	default:
		return nil, fmt.Errorf("redact: column %s has unknown mask form %v", colIdentity(col), m.Form)
	}
}

// maskRune extracts the first rune from char (defaulting to 'X' on
// empty). Multi-rune values silently consume only the first rune;
// the CLI parser should refuse multi-rune values at parse time, so
// this is defensive.
func maskRune(char string) rune {
	if char == "" {
		return 'X'
	}
	for _, r := range char {
		return r
	}
	return 'X'
}
