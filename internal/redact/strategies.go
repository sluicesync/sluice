// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

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
func (Null) Redact(col *ir.Column, _ any) (any, error) {
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
func (s Static) Redact(_ *ir.Column, _ any) (any, error) {
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
func (h Hash) Redact(col *ir.Column, val any) (any, error) {
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
func (t Truncate) Redact(col *ir.Column, val any) (any, error) {
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
