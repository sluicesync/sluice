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

	"github.com/orware/sluice/internal/config"
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

// TestMergeYAMLRedactions_AllStrategies covers the PII Phase 1.5
// YAML round-trip: config.Redaction entries → redact.Registry.
func TestMergeYAMLRedactions_AllStrategies(t *testing.T) {
	entries := []config.Redaction{
		{Table: "public.users.email", Strategy: "hash", Algo: "sha256"},
		{Table: "users.phone", Strategy: "truncate", Length: 4},
		{Table: "public.users.ssn", Strategy: "static", Value: "REDACTED"},
		{Table: "users.middle_name", Strategy: "null"},
	}
	reg, err := mergeYAMLRedactions(nil, entries, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rules := reg.Rules()
	if len(rules) != 4 {
		t.Fatalf("got %d rules; want 4", len(rules))
	}
	// Spot-check each
	if reg.Get("public", "users", "email").Name() != "hash:sha256" {
		t.Errorf("email strategy mismatch")
	}
	if reg.Get("", "users", "phone").Name() != "truncate:4" {
		t.Errorf("phone strategy mismatch")
	}
	if reg.Get("public", "users", "ssn").Name() != "static:<elided>" {
		t.Errorf("ssn strategy mismatch")
	}
	if reg.Get("", "users", "middle_name").Name() != "null" {
		t.Errorf("middle_name strategy mismatch")
	}
}

// TestMergeYAMLRedactions_AppendsToCLIRegistry covers the
// "YAML extends CLI" semantics: the merged Registry has both sets.
func TestMergeYAMLRedactions_AppendsToCLIRegistry(t *testing.T) {
	cli, err := parseRedactFlags([]string{"users.email=hash:sha256"}, "", "")
	if err != nil {
		t.Fatalf("CLI parse failed: %v", err)
	}
	yaml := []config.Redaction{
		{Table: "users.phone", Strategy: "truncate", Length: 4},
	}
	reg, err := mergeYAMLRedactions(cli, yaml, "", "")
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}
	if reg.Get("", "users", "email") == nil {
		t.Errorf("CLI rule lost after merge")
	}
	if reg.Get("", "users", "phone") == nil {
		t.Errorf("YAML rule not registered")
	}
}

// TestMergeYAMLRedactions_RefusalPaths covers malformed YAML
// entries.
func TestMergeYAMLRedactions_RefusalPaths(t *testing.T) {
	cases := []struct {
		name          string
		entry         config.Redaction
		wantSubstring string
	}{
		{"unknown strategy", config.Redaction{Table: "users.email", Strategy: "foo"}, "unknown strategy"},
		{"empty strategy", config.Redaction{Table: "users.email"}, "'strategy' field is required"},
		{"hash no algo", config.Redaction{Table: "users.email", Strategy: "hash"}, "requires 'algo' field"},
		{"hash unknown algo", config.Redaction{Table: "users.email", Strategy: "hash", Algo: "md5"}, "not supported"},
		{"truncate negative", config.Redaction{Table: "users.email", Strategy: "truncate", Length: -1}, "non-negative"},
		{"empty table", config.Redaction{Strategy: "null"}, "'table' field is empty"},
		{"single-segment table", config.Redaction{Table: "users", Strategy: "null"}, "must be"},
		{"too many segments", config.Redaction{Table: "a.b.c.d", Strategy: "null"}, "must be"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := mergeYAMLRedactions(nil, []config.Redaction{c.entry}, "", "")
			if err == nil {
				t.Fatal("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.wantSubstring) {
				t.Errorf("error %q should contain %q", err.Error(), c.wantSubstring)
			}
		})
	}
}

// TestParseRedactFlags_Mask covers the PII Phase 2.a CLI form of
// mask:inner and mask:outer including the optional char argument.
func TestParseRedactFlags_Mask(t *testing.T) {
	values := []string{
		"users.pan=mask:inner:4,4",    // default char
		"users.ssn=mask:inner:0,4,*",  // custom char
		"users.token=mask:outer:2,2",  // outer form
		"users.code=mask:outer:1,1,#", // outer + custom char
	}
	reg, err := parseRedactFlags(values, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cases := []struct {
		schema, table, col, wantStrategy string
	}{
		{"", "users", "pan", "mask:inner:4,4"},
		{"", "users", "ssn", "mask:inner:0,4"},
		{"", "users", "token", "mask:outer:2,2"},
		{"", "users", "code", "mask:outer:1,1"},
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

	// Confirm the parsed Mask actually masks correctly end-to-end.
	pan := reg.Get("", "users", "pan")
	got, err := pan.Redact(&ir.Column{Name: "pan"}, "4111111111111111")
	if err != nil {
		t.Fatalf("Redact failed: %v", err)
	}
	if got != "4111XXXXXXXX1111" {
		t.Errorf("PAN mask got %q; want %q", got, "4111XXXXXXXX1111")
	}
	ssn := reg.Get("", "users", "ssn")
	got, err = ssn.Redact(&ir.Column{Name: "ssn"}, "123456789")
	if err != nil {
		t.Fatalf("Redact failed: %v", err)
	}
	if got != "*****6789" {
		t.Errorf("SSN mask got %q; want %q", got, "*****6789")
	}
}

// TestParseRedactFlags_MaskRefusalPaths covers every documented
// refusal in parseMaskStrategy.
func TestParseRedactFlags_MaskRefusalPaths(t *testing.T) {
	cases := []struct {
		name, raw, wantSubstring string
	}{
		{"empty opts", "users.pan=mask", "requires a form"},
		{"no margins", "users.pan=mask:inner", "expected 'mask:<form>:<m1>,<m2>"},
		{"unknown form", "users.pan=mask:middle:4,4", "unknown form"},
		{"missing m2", "users.pan=mask:inner:4", "expected '<m1>,<m2>"},
		{"too many args", "users.pan=mask:inner:1,2,3,4", "expected '<m1>,<m2>"},
		{"non-int m1", "users.pan=mask:inner:abc,4", "m1 must be an integer"},
		{"non-int m2", "users.pan=mask:inner:4,xyz", "m2 must be an integer"},
		{"negative m1", "users.pan=mask:inner:-1,4", "m1 must be non-negative"},
		{"negative m2", "users.pan=mask:inner:4,-1", "m2 must be non-negative"},
		{"multi-rune char", "users.pan=mask:inner:4,4,XY", "single rune"},
		{"empty char arg", "users.pan=mask:inner:4,4,", "char argument is empty"},
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

// TestMergeYAMLRedactions_Mask covers the YAML form of mask.
func TestMergeYAMLRedactions_Mask(t *testing.T) {
	entries := []config.Redaction{
		{Table: "users.pan", Strategy: "mask", Form: "inner", M1: 4, M2: 4},
		{Table: "users.ssn", Strategy: "mask", Form: "inner", M1: 0, M2: 4, Char: "*"},
		{Table: "users.token", Strategy: "mask", Form: "outer", M1: 2, M2: 2},
	}
	reg, err := mergeYAMLRedactions(nil, entries, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := reg.Get("", "users", "pan").Name(); got != "mask:inner:4,4" {
		t.Errorf("pan strategy mismatch: %q", got)
	}
	if got := reg.Get("", "users", "ssn").Name(); got != "mask:inner:0,4" {
		t.Errorf("ssn strategy mismatch: %q", got)
	}
	if got := reg.Get("", "users", "token").Name(); got != "mask:outer:2,2" {
		t.Errorf("token strategy mismatch: %q", got)
	}

	// Confirm custom char round-trips through YAML.
	ssn := reg.Get("", "users", "ssn")
	got, err := ssn.Redact(&ir.Column{Name: "ssn"}, "123456789")
	if err != nil {
		t.Fatalf("Redact failed: %v", err)
	}
	if got != "*****6789" {
		t.Errorf("SSN mask via YAML got %q; want %q", got, "*****6789")
	}
}

// TestMergeYAMLRedactions_MaskRefusalPaths covers malformed YAML
// mask entries.
func TestMergeYAMLRedactions_MaskRefusalPaths(t *testing.T) {
	cases := []struct {
		name          string
		entry         config.Redaction
		wantSubstring string
	}{
		{"missing form", config.Redaction{Table: "users.pan", Strategy: "mask", M1: 4, M2: 4}, "requires 'form' field"},
		{"unknown form", config.Redaction{Table: "users.pan", Strategy: "mask", Form: "middle", M1: 4, M2: 4}, "unknown form"},
		{"negative m1", config.Redaction{Table: "users.pan", Strategy: "mask", Form: "inner", M1: -1, M2: 4}, "non-negative 'm1'"},
		{"negative m2", config.Redaction{Table: "users.pan", Strategy: "mask", Form: "inner", M1: 4, M2: -1}, "non-negative 'm2'"},
		{"multi-rune char", config.Redaction{Table: "users.pan", Strategy: "mask", Form: "inner", M1: 4, M2: 4, Char: "XY"}, "single rune"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := mergeYAMLRedactions(nil, []config.Redaction{c.entry}, "", "")
			if err == nil {
				t.Fatal("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.wantSubstring) {
				t.Errorf("error %q should contain %q", err.Error(), c.wantSubstring)
			}
		})
	}
}

// TestMergeYAMLRedactions_HMACWithKeySource covers the YAML form
// of hmac-sha256 + key-source resolution.
func TestMergeYAMLRedactions_HMACWithKeySource(t *testing.T) {
	t.Setenv("TEST_YAML_HMAC_KEY", "yaml-hmac-key")
	entries := []config.Redaction{
		{Table: "users.email", Strategy: "hash", Algo: "hmac-sha256"},
	}
	reg, err := mergeYAMLRedactions(nil, entries, "env:TEST_YAML_HMAC_KEY", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s := reg.Get("", "users", "email")
	got, err := s.Redact(&ir.Column{Name: "email"}, "alice@example.com")
	if err != nil {
		t.Fatalf("Redact failed: %v", err)
	}
	m := hmac.New(sha256.New, []byte("yaml-hmac-key"))
	m.Write([]byte("alice@example.com"))
	want := hex.EncodeToString(m.Sum(nil))
	if got != want {
		t.Errorf("YAML hmac mismatch")
	}
}
