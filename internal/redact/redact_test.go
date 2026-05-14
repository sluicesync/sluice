// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// stringColumn returns a [ir.Column] with the named string type,
// nullability defaulting to the second arg. Convenience for the
// strategy tests below.
func stringColumn(name string, nullable bool) *ir.Column {
	return &ir.Column{
		Name:     name,
		Type:     ir.Varchar{Length: 255},
		Nullable: nullable,
	}
}

func intColumn(name string, nullable bool) *ir.Column {
	return &ir.Column{
		Name:     name,
		Type:     ir.Integer{Width: 64},
		Nullable: nullable,
	}
}

// TestNull covers the Null strategy's three behaviours:
//   - NULL input on a nullable column → (nil, nil)
//   - non-NULL input on a nullable column → (nil, nil)
//   - any input on a NOT NULL column → (nil, error naming the column)
func TestNull(t *testing.T) {
	t.Run("Name is 'null'", func(t *testing.T) {
		if got := (Null{}).Name(); got != "null" {
			t.Errorf("Null.Name = %q; want %q", got, "null")
		}
	})

	t.Run("nullable column: returns nil, nil for any input", func(t *testing.T) {
		col := stringColumn("email", true)
		for _, in := range []any{"alice@example.com", nil, []byte("raw"), 42} {
			got, err := (Null{}).Redact(col, in)
			if err != nil {
				t.Errorf("input %v: unexpected error %v", in, err)
			}
			if got != nil {
				t.Errorf("input %v: got %v; want nil", in, got)
			}
		}
	})

	t.Run("NOT NULL column: refuses with informative error", func(t *testing.T) {
		col := stringColumn("ssn", false)
		_, err := (Null{}).Redact(col, "111-22-3333")
		if err == nil {
			t.Fatal("expected refusal; got nil error")
		}
		if !strings.Contains(err.Error(), "ssn") {
			t.Errorf("error %q should name the column 'ssn'", err.Error())
		}
		if !strings.Contains(err.Error(), "NOT NULL") {
			t.Errorf("error %q should mention NOT NULL", err.Error())
		}
		if !strings.Contains(err.Error(), "static:") {
			t.Errorf("error %q should suggest 'static:' alternative", err.Error())
		}
	})

	t.Run("nil column: passes through (defensive)", func(t *testing.T) {
		// Defensive: redactRow always passes a non-nil col in
		// production. Tests can pass nil to exercise strategies in
		// isolation; the strategy must not panic.
		got, err := (Null{}).Redact(nil, "x")
		if err != nil {
			t.Errorf("nil col: unexpected error %v", err)
		}
		if got != nil {
			t.Errorf("nil col: got %v; want nil", got)
		}
	})
}

// TestStatic covers the literal-replacement strategy.
func TestStatic(t *testing.T) {
	t.Run("Name elides the replacement value", func(t *testing.T) {
		s := Static{Value: "secret-but-don't-log-me"}
		if got := s.Name(); got != "static:<elided>" {
			t.Errorf("Static.Name = %q; want %q", got, "static:<elided>")
		}
	})

	t.Run("returns the configured value regardless of input", func(t *testing.T) {
		col := stringColumn("email", true)
		s := Static{Value: "REDACTED"}
		for _, in := range []any{"alice@example.com", nil, 42, []byte("raw")} {
			got, err := s.Redact(col, in)
			if err != nil {
				t.Errorf("input %v: unexpected error %v", in, err)
			}
			if got != "REDACTED" {
				t.Errorf("input %v: got %v; want %q", in, got, "REDACTED")
			}
		}
	})

	t.Run("empty Value is fine (operator-explicit empty replacement)", func(t *testing.T) {
		got, err := (Static{Value: ""}).Redact(stringColumn("x", true), "anything")
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}
		if got != "" {
			t.Errorf("got %v; want empty string", got)
		}
	})
}

// TestHash covers SHA-256 + HMAC-SHA256 paths. Determinism is the
// key property: same input → same hex output across calls.
func TestHash(t *testing.T) {
	t.Run("Name is 'hash:<algo>'", func(t *testing.T) {
		if got := (Hash{Algo: "sha256"}).Name(); got != "hash:sha256" {
			t.Errorf("Hash.Name = %q; want %q", got, "hash:sha256")
		}
		if got := (Hash{Algo: "hmac-sha256"}).Name(); got != "hash:hmac-sha256" {
			t.Errorf("Hash.Name = %q; want %q", got, "hash:hmac-sha256")
		}
	})

	t.Run("sha256: string input → hex digest", func(t *testing.T) {
		col := stringColumn("email", true)
		got, err := (Hash{Algo: "sha256"}).Redact(col, "alice@example.com")
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		// Compute the expected digest independently so the test
		// catches output-format regressions (e.g. accidental
		// base64 switch).
		want := sha256.Sum256([]byte("alice@example.com"))
		wantHex := hex.EncodeToString(want[:])
		if got != wantHex {
			t.Errorf("got %v; want %s", got, wantHex)
		}
		// Sanity: SHA-256 hex output is always 64 chars.
		if s, ok := got.(string); !ok || len(s) != 64 {
			t.Errorf("expected 64-char hex string; got %T %q", got, got)
		}
	})

	t.Run("sha256: []byte input → hex digest", func(t *testing.T) {
		col := stringColumn("pii", true)
		got, err := (Hash{Algo: "sha256"}).Redact(col, []byte("raw-bytes"))
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		want := sha256.Sum256([]byte("raw-bytes"))
		if got != hex.EncodeToString(want[:]) {
			t.Errorf("byte-input hash mismatch")
		}
	})

	t.Run("sha256: nil input passes through", func(t *testing.T) {
		got, err := (Hash{Algo: "sha256"}).Redact(stringColumn("email", true), nil)
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}
		if got != nil {
			t.Errorf("nil input should pass through; got %v", got)
		}
	})

	t.Run("sha256: deterministic across calls", func(t *testing.T) {
		h := Hash{Algo: "sha256"}
		col := stringColumn("email", true)
		a, _ := h.Redact(col, "alice@example.com")
		b, _ := h.Redact(col, "alice@example.com")
		if a != b {
			t.Errorf("not deterministic: %v != %v", a, b)
		}
	})

	t.Run("sha256: unsupported type refuses with informative error", func(t *testing.T) {
		_, err := (Hash{Algo: "sha256"}).Redact(intColumn("id", false), int64(42))
		if err == nil {
			t.Fatal("expected error for int input; got nil")
		}
		if !strings.Contains(err.Error(), "id") {
			t.Errorf("error should name the column: %q", err.Error())
		}
		if !strings.Contains(err.Error(), "int64") {
			t.Errorf("error should name the input type: %q", err.Error())
		}
	})

	t.Run("hmac-sha256: requires Key", func(t *testing.T) {
		_, err := (Hash{Algo: "hmac-sha256", Key: nil}).Redact(stringColumn("email", true), "alice@example.com")
		if err == nil {
			t.Fatal("expected error for empty Key; got nil")
		}
		if !strings.Contains(err.Error(), "Key") || !strings.Contains(err.Error(), "key-source") {
			t.Errorf("error should mention Key + --redact-key-source: %q", err.Error())
		}
	})

	t.Run("hmac-sha256: keyed digest", func(t *testing.T) {
		key := []byte("my-secret-key-32-bytes-or-more!!")
		got, err := (Hash{Algo: "hmac-sha256", Key: key}).Redact(stringColumn("email", true), "alice@example.com")
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		m := hmac.New(sha256.New, key)
		m.Write([]byte("alice@example.com"))
		want := hex.EncodeToString(m.Sum(nil))
		if got != want {
			t.Errorf("got %v; want %s", got, want)
		}
	})

	t.Run("hmac-sha256: different key → different digest (keying matters)", func(t *testing.T) {
		k1 := []byte("key-one")
		k2 := []byte("key-two")
		a, _ := (Hash{Algo: "hmac-sha256", Key: k1}).Redact(stringColumn("email", true), "alice@example.com")
		b, _ := (Hash{Algo: "hmac-sha256", Key: k2}).Redact(stringColumn("email", true), "alice@example.com")
		if a == b {
			t.Errorf("different keys should produce different digests; got identical %v", a)
		}
	})

	t.Run("unknown algorithm refuses with informative error", func(t *testing.T) {
		_, err := (Hash{Algo: "md5"}).Redact(stringColumn("email", true), "x")
		if err == nil {
			t.Fatal("expected error for unknown algorithm; got nil")
		}
		if !strings.Contains(err.Error(), "md5") {
			t.Errorf("error should name the unknown algorithm: %q", err.Error())
		}
		if !strings.Contains(err.Error(), "supported:") {
			t.Errorf("error should list supported algorithms: %q", err.Error())
		}
	})
}

// TestTruncate covers rune-based truncation + the refusal on
// non-string types.
func TestTruncate(t *testing.T) {
	t.Run("Name is 'truncate:<n>'", func(t *testing.T) {
		if got := (Truncate{N: 4}).Name(); got != "truncate:4" {
			t.Errorf("Truncate.Name = %q; want %q", got, "truncate:4")
		}
	})

	t.Run("ASCII string: first N chars", func(t *testing.T) {
		got, err := (Truncate{N: 4}).Redact(stringColumn("phone", true), "555-1234")
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if got != "555-" {
			t.Errorf("got %v; want %q", got, "555-")
		}
	})

	t.Run("shorter than N: returns verbatim", func(t *testing.T) {
		got, err := (Truncate{N: 100}).Redact(stringColumn("phone", true), "555-1234")
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if got != "555-1234" {
			t.Errorf("got %v; want %q (unchanged)", got, "555-1234")
		}
	})

	t.Run("multi-byte UTF-8: rune-counted, not byte-counted", func(t *testing.T) {
		// 'ñ' is 2 bytes in UTF-8. Truncate to 4 RUNES of "ñ@ex.com"
		// must produce "ñ@ex" (4 runes), NOT "ñ@e" (which is what
		// byte-truncating the leading 2-byte ñ to 4 would do).
		got, err := (Truncate{N: 4}).Redact(stringColumn("email", true), "ñ@example.com")
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if got != "ñ@ex" {
			t.Errorf("got %v; want %q (rune truncation)", got, "ñ@ex")
		}
	})

	t.Run("emoji: 1 rune is preserved as 1 emoji", func(t *testing.T) {
		got, err := (Truncate{N: 1}).Redact(stringColumn("note", true), "🔒secret")
		if err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if got != "🔒" {
			t.Errorf("got %v; want %q", got, "🔒")
		}
	})

	t.Run("nil input passes through", func(t *testing.T) {
		got, err := (Truncate{N: 4}).Redact(stringColumn("phone", true), nil)
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}
		if got != nil {
			t.Errorf("nil input should pass through; got %v", got)
		}
	})

	t.Run("non-string input refuses with informative error", func(t *testing.T) {
		_, err := (Truncate{N: 4}).Redact(intColumn("age", false), int64(42))
		if err == nil {
			t.Fatal("expected error for int input; got nil")
		}
		if !strings.Contains(err.Error(), "age") {
			t.Errorf("error should name the column: %q", err.Error())
		}
	})

	t.Run("N <= 0 produces empty string defensively", func(t *testing.T) {
		got, err := (Truncate{N: 0}).Redact(stringColumn("note", true), "anything")
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}
		if got != "" {
			t.Errorf("got %v; want empty string", got)
		}
		got, err = (Truncate{N: -5}).Redact(stringColumn("note", true), "anything")
		if err != nil {
			t.Errorf("unexpected error %v", err)
		}
		if got != "" {
			t.Errorf("negative N should produce empty string; got %v", got)
		}
	})
}

// TestRegistry covers the Set / Get / Empty / Rules round-trip
// plus the case-folding policy.
func TestRegistry(t *testing.T) {
	t.Run("empty registry is Empty + Get returns nil", func(t *testing.T) {
		r := New()
		if !r.Empty() {
			t.Error("fresh Registry should be Empty")
		}
		if s := r.Get("public", "users", "email"); s != nil {
			t.Errorf("Get on empty Registry should be nil; got %v", s)
		}
		if rules := r.Rules(); rules != nil {
			t.Errorf("Rules on empty Registry should be nil; got %v", rules)
		}
	})

	t.Run("nil registry pointer is Empty + Get returns nil (defensive)", func(t *testing.T) {
		var r *Registry
		if !r.Empty() {
			t.Error("nil Registry should be Empty")
		}
		if s := r.Get("any", "thing", "here"); s != nil {
			t.Errorf("Get on nil Registry should be nil; got %v", s)
		}
		if rules := r.Rules(); rules != nil {
			t.Errorf("Rules on nil Registry should be nil; got %v", rules)
		}
	})

	t.Run("Set then Get: round-trip", func(t *testing.T) {
		r := New()
		r.Set("public", "users", "email", Hash{Algo: "sha256"})
		got := r.Get("public", "users", "email")
		if got == nil {
			t.Fatal("Get returned nil after Set")
		}
		if got.Name() != "hash:sha256" {
			t.Errorf("got Strategy %v; want hash:sha256", got.Name())
		}
	})

	t.Run("Set then Get: case-insensitive (Phase 1 policy)", func(t *testing.T) {
		r := New()
		r.Set("Public", "Users", "Email", Hash{Algo: "sha256"})
		// Look up with different case; should match.
		got := r.Get("public", "USERS", "Email")
		if got == nil {
			t.Fatal("Get should match case-insensitively")
		}
	})

	t.Run("Set with nil strategy panics", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Error("expected panic when Set called with nil strategy")
			}
		}()
		New().Set("schema", "table", "col", nil)
	})

	t.Run("Set duplicate: last write wins", func(t *testing.T) {
		r := New()
		r.Set("public", "users", "email", Hash{Algo: "sha256"})
		r.Set("public", "users", "email", Static{Value: "REDACTED"})
		got := r.Get("public", "users", "email")
		if got.Name() != "static:<elided>" {
			t.Errorf("last-write-wins: got %v; want static:<elided>", got.Name())
		}
	})

	t.Run("Rules returns all registered + sorted by lowercased key", func(t *testing.T) {
		r := New()
		r.Set("public", "users", "phone", Truncate{N: 4})
		r.Set("public", "users", "email", Hash{Algo: "sha256"})
		r.Set("billing", "accounts", "ssn", Null{})

		rules := r.Rules()
		if len(rules) != 3 {
			t.Fatalf("len(rules) = %d; want 3", len(rules))
		}
		// Sorted by key: "billing.accounts.ssn" < "public.users.email" < "public.users.phone"
		want := []string{"billing.accounts.ssn", "public.users.email", "public.users.phone"}
		for i, r := range rules {
			got := r.Schema + "." + r.Table + "." + r.Column
			if got != want[i] {
				t.Errorf("rules[%d] key = %q; want %q (sorted lexicographically)", i, got, want[i])
			}
		}
	})

	t.Run("empty schema is allowed (some engines resolve implicitly)", func(t *testing.T) {
		r := New()
		r.Set("", "users", "email", Hash{Algo: "sha256"})
		got := r.Get("", "users", "email")
		if got == nil {
			t.Error("Get with empty schema should match the same Set")
		}
	})
}
