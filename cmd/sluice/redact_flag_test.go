// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/redact"
)

// TestParseRedactFlags_Empty pins the no-op default: empty slice
// returns nil registry, no error.
func TestParseRedactFlags_Empty(t *testing.T) {
	reg, err := parseRedactFlags(nil, "", "")
	if err != nil {
		t.Errorf("empty: unexpected error %v", err)
	}
	if reg != nil {
		t.Errorf("empty: expected nil registry; got %+v", reg)
	}
}

// TestParseRedactFlags_AllStrategies covers each Phase 1 strategy
// via the CLI flag value form. Each strategy must round-trip from
// flag string → Registry entry → Strategy.Name().
func TestParseRedactFlags_AllStrategies(t *testing.T) {
	values := []string{
		"public.users.email=hash:sha256",
		"users.phone=truncate:4",
		"public.users.ssn=null",
		"billing.accounts.credit_card=static:REDACTED",
	}
	reg, err := parseRedactFlags(values, "", "")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	rules := reg.Rules()
	if len(rules) != 4 {
		t.Fatalf("got %d rules; want 4", len(rules))
	}
	// Verify each rule by looking up via Get.
	cases := []struct {
		schema, table, col, wantStrategy string
	}{
		{"public", "users", "email", "hash:sha256"},
		{"", "users", "phone", "truncate:4"},
		{"public", "users", "ssn", "null"},
		{"billing", "accounts", "credit_card", "static:<elided>"},
	}
	for _, c := range cases {
		s := reg.Get(c.schema, c.table, c.col)
		if s == nil {
			t.Errorf("%s.%s.%s: no rule registered", c.schema, c.table, c.col)
			continue
		}
		if s.Name() != c.wantStrategy {
			t.Errorf("%s.%s.%s: got %q; want %q", c.schema, c.table, c.col, s.Name(), c.wantStrategy)
		}
	}
}

// TestParseRedactFlags_HashHMAC covers the HMAC keyed path with
// the derive: key source.
func TestParseRedactFlags_HashHMAC_Derive(t *testing.T) {
	reg, err := parseRedactFlags(
		[]string{"users.email=hash:hmac-sha256"},
		"derive:test-salt",
		"test-stream",
	)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	s := reg.Get("", "users", "email")
	if s == nil {
		t.Fatal("expected strategy registered")
	}
	if s.Name() != "hash:hmac-sha256" {
		t.Errorf("got %q; want hash:hmac-sha256", s.Name())
	}
	// Verify the derived key matches what the helper produces.
	got, err := s.Redact(&ir.Column{Name: "email"}, "alice@example.com")
	if err != nil {
		t.Fatalf("Redact failed: %v", err)
	}
	wantKey := sha256.Sum256([]byte("test-stream:test-salt"))
	m := hmac.New(sha256.New, wantKey[:])
	m.Write([]byte("alice@example.com"))
	want := hex.EncodeToString(m.Sum(nil))
	if got != want {
		t.Errorf("HMAC digest mismatch: got %v; want %s", got, want)
	}
}

// TestParseRedactFlags_HashHMAC_Env covers the env-var key source.
func TestParseRedactFlags_HashHMAC_Env(t *testing.T) {
	const envVar = "TEST_REDACT_HMAC_KEY"
	t.Setenv(envVar, "test-key-from-env")
	reg, err := parseRedactFlags(
		[]string{"users.email=hash:hmac-sha256"},
		"env:"+envVar,
		"",
	)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	s := reg.Get("", "users", "email")
	got, err := s.Redact(&ir.Column{Name: "email"}, "alice@example.com")
	if err != nil {
		t.Fatalf("Redact failed: %v", err)
	}
	m := hmac.New(sha256.New, []byte("test-key-from-env"))
	m.Write([]byte("alice@example.com"))
	want := hex.EncodeToString(m.Sum(nil))
	if got != want {
		t.Errorf("env-keyed HMAC digest mismatch")
	}
}

// TestParseRedactFlags_HashHMAC_File covers the file-based key source.
func TestParseRedactFlags_HashHMAC_File(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "hmac.key")
	if err := os.WriteFile(keyPath, []byte("file-key-content\n"), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	reg, err := parseRedactFlags(
		[]string{"users.email=hash:hmac-sha256"},
		"file:"+keyPath,
		"",
	)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	s := reg.Get("", "users", "email")
	got, err := s.Redact(&ir.Column{Name: "email"}, "alice@example.com")
	if err != nil {
		t.Fatalf("Redact failed: %v", err)
	}
	m := hmac.New(sha256.New, []byte("file-key-content"))
	m.Write([]byte("alice@example.com"))
	want := hex.EncodeToString(m.Sum(nil))
	if got != want {
		t.Errorf("file-keyed HMAC digest mismatch")
	}
}

// TestParseRedactFlags_RefusalPaths covers every documented
// malformed input. Each case must return an error naming the
// offending input.
func TestParseRedactFlags_RefusalPaths(t *testing.T) {
	cases := []struct {
		name, raw, wantSubstring string
	}{
		{"missing =", "users.email", "missing '='"},
		{"empty triple", "=hash:sha256", "column triple is empty"},
		{"empty strategy", "users.email=", "strategy is empty"},
		{"too few parts", "email=hash:sha256", "must be either 'table.column' or 'schema.table.column'"},
		{"too many parts", "a.b.c.d=hash:sha256", "must be either"},
		{"unknown strategy", "users.email=foo", "unknown strategy"},
		{"null with options", "users.email=null:foo", "takes no options"},
		{"hash no algo", "users.email=hash:", "hash' requires an algorithm"},
		{"hash unknown algo", "users.email=hash:md5", "not supported"},
		{"truncate no length", "users.phone=truncate", "requires a length"},
		{"truncate non-integer", "users.phone=truncate:abc", "must be an integer"},
		{"truncate negative", "users.phone=truncate:-5", "non-negative"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := parseRedactFlags([]string{c.raw}, "", "")
			if err == nil {
				t.Fatal("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.wantSubstring) {
				t.Errorf("error %q should contain %q", err.Error(), c.wantSubstring)
			}
		})
	}
}

// TestParseRedactFlags_HMACNoKeySource covers the specific refusal
// when hmac-sha256 is declared without --redact-key-source.
func TestParseRedactFlags_HMACNoKeySource(t *testing.T) {
	_, err := parseRedactFlags(
		[]string{"users.email=hash:hmac-sha256"},
		"",
		"",
	)
	if err == nil {
		t.Fatal("expected refusal; got nil")
	}
	if !strings.Contains(err.Error(), "--redact-key-source") {
		t.Errorf("error %q should reference --redact-key-source", err.Error())
	}
}

// TestParseRedactFlags_KeySourceMalformed covers the key-source
// parsing's error paths.
func TestParseRedactFlags_KeySourceMalformed(t *testing.T) {
	cases := []struct {
		source, wantSubstring string
	}{
		{"no-colon", "expected 'env:VAR'"},
		{"unknown:foo", "unknown scheme"},
		{"env:NONEXISTENT_VAR_XYZ", "environment variable is empty"},
		{"file:/no/such/path", ""}, // OS-dependent message; just ensure error
	}
	for _, c := range cases {
		c := c
		t.Run(c.source, func(t *testing.T) {
			_, err := parseRedactFlags(
				[]string{"users.email=hash:hmac-sha256"},
				c.source,
				"",
			)
			if err == nil {
				t.Fatal("expected error; got nil")
			}
			if c.wantSubstring != "" && !strings.Contains(err.Error(), c.wantSubstring) {
				t.Errorf("error %q should contain %q", err.Error(), c.wantSubstring)
			}
		})
	}
}

// TestLogRedactionConfig is a smoke test for the audit log line.
// We don't assert on log output here (slog handler is the default);
// just verify the function doesn't panic on nil/empty/populated
// registries.
func TestLogRedactionConfig(_ *testing.T) {
	logRedactionConfig(nil, "test")
	logRedactionConfig(redact.New(), "test")
	r := redact.New()
	r.Set("public", "users", "email", redact.Hash{Algo: "sha256"})
	r.Set("public", "users", "phone", redact.Truncate{N: 4})
	r.Set("public", "users", "another_email", redact.Hash{Algo: "sha256"}) // dedup test
	logRedactionConfig(r, "migrate")
}
