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

	"github.com/alecthomas/kong"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/redact"
)

// TestRedactFlag_KongCommaPreservation pins Bug 59's regression
// surface across all four `--redact` declaration sites. Before v0.56.1,
// kong's default sep:"," split a value like `users.pan=mask:inner:4,4`
// into two list entries ([`users.pan=mask:inner:3`, `4`]), so the
// parser saw only "mask:inner:3" and rejected with a misleading
// "got 1 args" error. The fix is `sep:"none"` on each Redact field.
// Each subtest builds a kong parser, parses a comma-containing
// `--redact` value, and confirms the Redact slice has exactly one
// element with the comma intact.
func TestRedactFlag_KongCommaPreservation(t *testing.T) {
	want := "users.pan=mask:inner:4,4"
	cases := []struct {
		name string
		args []string
		get  func(*CLI) []string
	}{
		{
			name: "migrate",
			args: []string{
				"migrate",
				"--source-driver=mysql", "--source=u:p@/db",
				"--target-driver=mysql", "--target=u:p@/db",
				"--redact=" + want,
			},
			get: func(c *CLI) []string { return c.Migrate.Redact },
		},
		{
			name: "sync-start",
			args: []string{
				"sync", "start",
				"--source-driver=mysql", "--source=u:p@/db",
				"--target-driver=mysql", "--target=u:p@/db",
				"--redact=" + want,
			},
			get: func(c *CLI) []string { return c.Sync.Start.Redact },
		},
		{
			name: "backup-full",
			args: []string{
				"backup", "full",
				"--source-driver=mysql", "--source=u:p@/db",
				"--output-dir=/tmp/b",
				"--redact=" + want,
			},
			get: func(c *CLI) []string { return c.Backup.Full.Redact },
		},
		{
			name: "schema-preview",
			args: []string{
				"schema", "preview",
				"--source-driver=mysql", "--source=u:p@/db",
				"--target-driver=mysql", "--target=u:p@/db",
				"--redact=" + want,
			},
			get: func(c *CLI) []string { return c.Schema.Preview.Redact },
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			cli := &CLI{}
			parser, err := kong.New(cli,
				kong.Vars{"version": "test"},
				kong.Exit(func(int) {}),
			)
			if err != nil {
				t.Fatalf("kong.New: %v", err)
			}
			if _, err := parser.Parse(c.args); err != nil {
				t.Fatalf("Parse: %v", err)
			}
			got := c.get(cli)
			if len(got) != 1 {
				t.Fatalf("Redact len = %d; want 1 (kong split on comma — Bug 59 regression). values: %q", len(got), got)
			}
			if got[0] != want {
				t.Errorf("Redact[0] = %q; want %q", got[0], want)
			}
		})
	}
}

// TestParseRedactFlags_Empty pins the no-op default: empty slice
// returns nil registry, no error.
func TestParseRedactFlags_Empty(t *testing.T) {
	reg, err := parseRedactFlags(nil, "", "", nil)
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
	reg, err := parseRedactFlags(values, "", "", nil)
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
		nil,
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
	got, err := s.Redact(&ir.Column{Name: "email"}, "alice@example.com", nil)
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
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	s := reg.Get("", "users", "email")
	got, err := s.Redact(&ir.Column{Name: "email"}, "alice@example.com", nil)
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
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	s := reg.Get("", "users", "email")
	got, err := s.Redact(&ir.Column{Name: "email"}, "alice@example.com", nil)
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
			_, err := parseRedactFlags([]string{c.raw}, "", "", nil)
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
		nil,
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
				nil,
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
	reg, err := mergeYAMLRedactions(nil, entries, "", "", nil)
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
	cli, err := parseRedactFlags([]string{"users.email=hash:sha256"}, "", "", nil)
	if err != nil {
		t.Fatalf("CLI parse failed: %v", err)
	}
	yaml := []config.Redaction{
		{Table: "users.phone", Strategy: "truncate", Length: 4},
	}
	reg, err := mergeYAMLRedactions(cli, yaml, "", "", nil)
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
			_, err := mergeYAMLRedactions(nil, []config.Redaction{c.entry}, "", "", nil)
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
	reg, err := parseRedactFlags(values, "", "", nil)
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
	got, err := pan.Redact(&ir.Column{Name: "pan"}, "4111111111111111", nil)
	if err != nil {
		t.Fatalf("Redact failed: %v", err)
	}
	if got != "4111XXXXXXXX1111" {
		t.Errorf("PAN mask got %q; want %q", got, "4111XXXXXXXX1111")
	}
	ssn := reg.Get("", "users", "ssn")
	got, err = ssn.Redact(&ir.Column{Name: "ssn"}, "123456789", nil)
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
		{"no margins", "users.pan=mask:inner", "requiring margins"},
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
			_, err := parseRedactFlags([]string{c.raw}, "", "", nil)
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
	reg, err := mergeYAMLRedactions(nil, entries, "", "", nil)
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
	got, err := ssn.Redact(&ir.Column{Name: "ssn"}, "123456789", nil)
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
			_, err := mergeYAMLRedactions(nil, []config.Redaction{c.entry}, "", "", nil)
			if err == nil {
				t.Fatal("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.wantSubstring) {
				t.Errorf("error %q should contain %q", err.Error(), c.wantSubstring)
			}
		})
	}
}

// TestParseRedactFlags_MaskPresets covers the PII Phase 2.b CLI
// form of the country/format-specific mask presets.
func TestParseRedactFlags_MaskPresets(t *testing.T) {
	values := []string{
		"users.ssn=mask:ssn",
		"users.pan=mask:pan",
		"users.test_pan=mask:pan-relaxed",
		"users.email=mask:email",
	}
	reg, err := parseRedactFlags(values, "", "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cases := []struct {
		schema, table, col, wantStrategy string
	}{
		{"", "users", "ssn", "mask:ssn"},
		{"", "users", "pan", "mask:pan"},
		{"", "users", "test_pan", "mask:pan-relaxed"},
		{"", "users", "email", "mask:email"},
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

	// End-to-end: each preset masks correctly through the resolved registry.
	got, err := reg.Get("", "users", "ssn").Redact(&ir.Column{Name: "ssn"}, "123-45-6789", nil)
	if err != nil || got != "XXX-XX-6789" {
		t.Errorf("ssn end-to-end: got %v err %v; want XXX-XX-6789", got, err)
	}
	got, err = reg.Get("", "users", "pan").Redact(&ir.Column{Name: "pan"}, "4111111111111111", nil)
	if err != nil || got != "411111XXXXXX1111" {
		t.Errorf("pan end-to-end: got %v err %v; want 411111XXXXXX1111", got, err)
	}
	got, err = reg.Get("", "users", "email").Redact(&ir.Column{Name: "email"}, "alice@example.com", nil)
	if err != nil || got != "aXXXX@example.com" {
		t.Errorf("email end-to-end: got %v err %v; want aXXXX@example.com", got, err)
	}
}

// TestParseRedactFlags_MaskPresetsSecondWave covers the PII Phase
// 2.b second-wave preset names (ca-sin, uk-nin, iban, uuid)
// through the CLI parser dispatch.
func TestParseRedactFlags_MaskPresetsSecondWave(t *testing.T) {
	values := []string{
		"users.sin=mask:ca-sin",
		"users.nin=mask:uk-nin",
		"users.iban=mask:iban",
		"users.id=mask:uuid",
	}
	reg, err := parseRedactFlags(values, "", "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cases := []struct {
		table, col, wantStrategy string
	}{
		{"users", "sin", "mask:ca-sin"},
		{"users", "nin", "mask:uk-nin"},
		{"users", "iban", "mask:iban"},
		{"users", "id", "mask:uuid"},
	}
	for _, c := range cases {
		s := reg.Get("", c.table, c.col)
		if s == nil {
			t.Errorf("%s.%s: no rule", c.table, c.col)
			continue
		}
		if s.Name() != c.wantStrategy {
			t.Errorf("%s.%s: got %q; want %q", c.table, c.col, s.Name(), c.wantStrategy)
		}
	}

	// End-to-end through the registry for each preset.
	got, err := reg.Get("", "users", "sin").Redact(&ir.Column{Name: "sin"}, "046-454-286", nil)
	if err != nil || got != "XXX-XXX-286" {
		t.Errorf("ca-sin end-to-end: got %v err %v", got, err)
	}
	got, err = reg.Get("", "users", "nin").Redact(&ir.Column{Name: "nin"}, "AB123456C", nil)
	if err != nil || got != "ABXXXXXXC" {
		t.Errorf("uk-nin end-to-end: got %v err %v", got, err)
	}
	got, err = reg.Get("", "users", "iban").Redact(&ir.Column{Name: "iban"}, "DE89370400440532013000", nil)
	if err != nil || got != "DE8937XXXXXXXXXXXX3000" {
		t.Errorf("iban end-to-end: got %v err %v", got, err)
	}
	got, err = reg.Get("", "users", "id").Redact(&ir.Column{Name: "id"}, "550e8400-e29b-41d4-a716-446655440000", nil)
	if err != nil || got != "550eXXXX-XXXX-XXXX-XXXX-XXXXXXXX0000" {
		t.Errorf("uuid end-to-end: got %v err %v", got, err)
	}
}

// TestMergeYAMLRedactions_MaskPresetsSecondWave covers the YAML
// form for the four new presets.
func TestMergeYAMLRedactions_MaskPresetsSecondWave(t *testing.T) {
	entries := []config.Redaction{
		{Table: "users.sin", Strategy: "mask", Form: "ca-sin"},
		{Table: "users.nin", Strategy: "mask", Form: "uk-nin"},
		{Table: "users.iban", Strategy: "mask", Form: "iban"},
		{Table: "users.id", Strategy: "mask", Form: "uuid"},
	}
	reg, err := mergeYAMLRedactions(nil, entries, "", "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wants := map[string]string{
		"sin":  "mask:ca-sin",
		"nin":  "mask:uk-nin",
		"iban": "mask:iban",
		"id":   "mask:uuid",
	}
	for col, want := range wants {
		if got := reg.Get("", "users", col).Name(); got != want {
			t.Errorf("%s: got %q; want %q", col, got, want)
		}
	}
}

// TestParseRedactFlags_MaskPresetRefusalPaths covers every preset-
// specific CLI refusal.
func TestParseRedactFlags_MaskPresetRefusalPaths(t *testing.T) {
	cases := []struct {
		name, raw, wantSubstring string
	}{
		{"unknown preset", "users.x=mask:zip", "unknown form/preset"},
		{"preset with spurious options", "users.ssn=mask:ssn:foo", "preset 'mask:ssn' takes no options"},
		{"second-wave preset with options", "users.x=mask:uuid:foo", "preset 'mask:uuid' takes no options"},
		{"inner without margins", "users.x=mask:inner", "requiring margins"},
		{"outer without margins", "users.x=mask:outer", "requiring margins"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := parseRedactFlags([]string{c.raw}, "", "", nil)
			if err == nil {
				t.Fatal("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.wantSubstring) {
				t.Errorf("error %q should contain %q", err.Error(), c.wantSubstring)
			}
		})
	}
}

// TestMergeYAMLRedactions_MaskPresets covers the YAML form of the
// PII Phase 2.b presets.
func TestMergeYAMLRedactions_MaskPresets(t *testing.T) {
	entries := []config.Redaction{
		{Table: "users.ssn", Strategy: "mask", Form: "ssn"},
		{Table: "users.pan", Strategy: "mask", Form: "pan"},
		{Table: "users.test_pan", Strategy: "mask", Form: "pan-relaxed"},
		{Table: "users.email", Strategy: "mask", Form: "email"},
	}
	reg, err := mergeYAMLRedactions(nil, entries, "", "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := reg.Get("", "users", "ssn").Name(); got != "mask:ssn" {
		t.Errorf("ssn: %q", got)
	}
	if got := reg.Get("", "users", "pan").Name(); got != "mask:pan" {
		t.Errorf("pan: %q", got)
	}
	if got := reg.Get("", "users", "test_pan").Name(); got != "mask:pan-relaxed" {
		t.Errorf("pan-relaxed: %q", got)
	}
	if got := reg.Get("", "users", "email").Name(); got != "mask:email" {
		t.Errorf("email: %q", got)
	}
}

// TestMergeYAMLRedactions_MaskPresetRefusalPaths covers the YAML
// refusals (spurious M1/M2/Char on a preset, unknown preset).
func TestMergeYAMLRedactions_MaskPresetRefusalPaths(t *testing.T) {
	cases := []struct {
		name          string
		entry         config.Redaction
		wantSubstring string
	}{
		{"spurious m1 on preset", config.Redaction{Table: "users.ssn", Strategy: "mask", Form: "ssn", M1: 4}, "takes no other fields"},
		{"spurious char on preset", config.Redaction{Table: "users.pan", Strategy: "mask", Form: "pan", Char: "*"}, "takes no other fields"},
		{"unknown preset/form", config.Redaction{Table: "users.x", Strategy: "mask", Form: "zip"}, "unknown form"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := mergeYAMLRedactions(nil, []config.Redaction{c.entry}, "", "", nil)
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
	reg, err := mergeYAMLRedactions(nil, entries, "env:TEST_YAML_HMAC_KEY", "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s := reg.Get("", "users", "email")
	got, err := s.Redact(&ir.Column{Name: "email"}, "alice@example.com", nil)
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

// TestParseRedactFlags_Randomize covers the v0.59.0 PII Phase 2.c
// randomize:* parser shapes. Each happy path produces the right
// Strategy concrete type with the expected Name().
func TestParseRedactFlags_Randomize(t *testing.T) {
	cases := []struct {
		name, raw, wantName string
	}{
		{"int with bounds", "users.age=randomize:int:18,90", "randomize:int:18,90"},
		{"int with negative min", "users.score=randomize:int:-100,100", "randomize:int:-100,100"},
		{"email", "users.email=randomize:email", "randomize:email"},
		{"us-phone", "users.phone=randomize:us-phone", "randomize:us-phone"},
		{"uuid", "users.id=randomize:uuid", "randomize:uuid"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			reg, err := parseRedactFlags([]string{c.raw}, "", "", nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			rules := reg.Rules()
			if len(rules) != 1 {
				t.Fatalf("got %d rules; want 1", len(rules))
			}
			if got := rules[0].Strategy.Name(); got != c.wantName {
				t.Errorf("Strategy.Name() = %q; want %q", got, c.wantName)
			}
		})
	}
}

// TestParseRedactFlags_RandomizeRefusalPaths covers every documented
// malformed randomize:* input.
func TestParseRedactFlags_RandomizeRefusalPaths(t *testing.T) {
	cases := []struct {
		name, raw, wantSubstring string
	}{
		{"missing form", "users.x=randomize", "requires a form"},
		{"unknown form", "users.x=randomize:foo", "unknown form"},
		{"int missing bounds", "users.x=randomize:int", "requires bounds"},
		{"int single arg", "users.x=randomize:int:5", "expected '<min>,<max>'"},
		{"int three args", "users.x=randomize:int:5,10,99", "expected '<min>,<max>'"},
		{"int non-integer min", "users.x=randomize:int:abc,99", "min must be an integer"},
		{"int non-integer max", "users.x=randomize:int:5,xyz", "max must be an integer"},
		{"int min > max", "users.x=randomize:int:99,5", "must not exceed max"},
		{"email with options", "users.x=randomize:email:foo", "takes no options"},
		{"us-phone with options", "users.x=randomize:us-phone:foo", "takes no options"},
		{"uuid with options", "users.x=randomize:uuid:foo", "takes no options"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := parseRedactFlags([]string{c.raw}, "", "", nil)
			if err == nil {
				t.Fatal("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.wantSubstring) {
				t.Errorf("error %q should contain %q", err.Error(), c.wantSubstring)
			}
		})
	}
}

// TestMergeYAMLRedactions_Randomize covers the YAML form of every
// randomize:* shape.
func TestMergeYAMLRedactions_Randomize(t *testing.T) {
	cases := []struct {
		name     string
		entry    config.Redaction
		wantName string
	}{
		{
			name:     "int",
			entry:    config.Redaction{Table: "users.age", Strategy: "randomize", Form: "int", Min: 18, Max: 90},
			wantName: "randomize:int:18,90",
		},
		{
			name:     "email",
			entry:    config.Redaction{Table: "users.email", Strategy: "randomize", Form: "email"},
			wantName: "randomize:email",
		},
		{
			name:     "us-phone",
			entry:    config.Redaction{Table: "users.phone", Strategy: "randomize", Form: "us-phone"},
			wantName: "randomize:us-phone",
		},
		{
			name:     "uuid",
			entry:    config.Redaction{Table: "users.id", Strategy: "randomize", Form: "uuid"},
			wantName: "randomize:uuid",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			reg, err := mergeYAMLRedactions(nil, []config.Redaction{c.entry}, "", "", nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			rules := reg.Rules()
			if len(rules) != 1 {
				t.Fatalf("got %d rules; want 1", len(rules))
			}
			if got := rules[0].Strategy.Name(); got != c.wantName {
				t.Errorf("Strategy.Name() = %q; want %q", got, c.wantName)
			}
		})
	}
}

// TestParseRedactFlags_RandomizeSecondWave covers the v0.60.0 PII
// Phase 2.c second-wave checksum-aware generators. Each happy-path
// case produces the right Strategy concrete type with the expected
// Name() — including brand / country-code parsing.
func TestParseRedactFlags_RandomizeSecondWave(t *testing.T) {
	cases := []struct {
		name, raw, wantName string
	}{
		{"ssn", "users.ssn=randomize:ssn", "randomize:ssn"},
		{"pan no brand", "p.pan=randomize:pan", "randomize:pan"},
		{"pan visa", "p.pan=randomize:pan:visa", "randomize:pan:visa"},
		{"pan mastercard", "p.pan=randomize:pan:mastercard", "randomize:pan:mastercard"},
		{"pan amex", "p.pan=randomize:pan:amex", "randomize:pan:amex"},
		{"ca-sin", "c.sin=randomize:ca-sin", "randomize:ca-sin"},
		{"uk-nin", "u.nin=randomize:uk-nin", "randomize:uk-nin"},
		{"iban no country", "c.iban=randomize:iban", "randomize:iban"},
		{"iban DE", "c.iban=randomize:iban:DE", "randomize:iban:DE"},
		{"iban GB", "c.iban=randomize:iban:GB", "randomize:iban:GB"},
		{"iban FR", "c.iban=randomize:iban:FR", "randomize:iban:FR"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			reg, err := parseRedactFlags([]string{c.raw}, "", "", nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			rules := reg.Rules()
			if len(rules) != 1 {
				t.Fatalf("got %d rules; want 1", len(rules))
			}
			if got := rules[0].Strategy.Name(); got != c.wantName {
				t.Errorf("Strategy.Name() = %q; want %q", got, c.wantName)
			}
		})
	}
}

// TestParseRedactFlags_RandomizeSecondWaveRefusals covers every
// documented refusal path on the new generators: unknown brand,
// unknown country, spurious options on no-options forms.
func TestParseRedactFlags_RandomizeSecondWaveRefusals(t *testing.T) {
	cases := []struct {
		name, raw, wantSubstring string
	}{
		{"ssn with options", "u.x=randomize:ssn:foo", "takes no options"},
		{"ca-sin with options", "u.x=randomize:ca-sin:foo", "takes no options"},
		{"uk-nin with options", "u.x=randomize:uk-nin:foo", "takes no options"},
		{"pan unknown brand", "u.x=randomize:pan:discover", "unknown brand"},
		{"pan empty brand after colon", "u.x=randomize:pan:", "brand is empty"},
		{"iban unknown country", "u.x=randomize:iban:US", "unknown country"},
		{"iban lowercase country (case-sensitive)", "u.x=randomize:iban:de", "unknown country"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := parseRedactFlags([]string{c.raw}, "", "", nil)
			if err == nil {
				t.Fatal("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.wantSubstring) {
				t.Errorf("error %q should contain %q", err.Error(), c.wantSubstring)
			}
		})
	}
}

// TestMergeYAMLRedactions_RandomizeSecondWave covers the YAML form
// of the v0.60.0 generators including brand / country_code parsing.
func TestMergeYAMLRedactions_RandomizeSecondWave(t *testing.T) {
	cases := []struct {
		name     string
		entry    config.Redaction
		wantName string
	}{
		{
			name:     "ssn",
			entry:    config.Redaction{Table: "users.ssn", Strategy: "randomize", Form: "ssn"},
			wantName: "randomize:ssn",
		},
		{
			name:     "pan no brand",
			entry:    config.Redaction{Table: "p.pan", Strategy: "randomize", Form: "pan"},
			wantName: "randomize:pan",
		},
		{
			name:     "pan visa",
			entry:    config.Redaction{Table: "p.pan", Strategy: "randomize", Form: "pan", Brand: "visa"},
			wantName: "randomize:pan:visa",
		},
		{
			name:     "ca-sin",
			entry:    config.Redaction{Table: "c.sin", Strategy: "randomize", Form: "ca-sin"},
			wantName: "randomize:ca-sin",
		},
		{
			name:     "uk-nin",
			entry:    config.Redaction{Table: "u.nin", Strategy: "randomize", Form: "uk-nin"},
			wantName: "randomize:uk-nin",
		},
		{
			name:     "iban no country",
			entry:    config.Redaction{Table: "c.iban", Strategy: "randomize", Form: "iban"},
			wantName: "randomize:iban",
		},
		{
			name:     "iban DE",
			entry:    config.Redaction{Table: "c.iban", Strategy: "randomize", Form: "iban", CountryCode: "DE"},
			wantName: "randomize:iban:DE",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			reg, err := mergeYAMLRedactions(nil, []config.Redaction{c.entry}, "", "", nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			rules := reg.Rules()
			if len(rules) != 1 {
				t.Fatalf("got %d rules; want 1", len(rules))
			}
			if got := rules[0].Strategy.Name(); got != c.wantName {
				t.Errorf("Strategy.Name() = %q; want %q", got, c.wantName)
			}
		})
	}
}

// TestMergeYAMLRedactions_RandomizeSecondWaveRefusals covers YAML
// refusal paths: bad brand / country, spurious bounds/brand/country
// on forms that don't accept them.
func TestMergeYAMLRedactions_RandomizeSecondWaveRefusals(t *testing.T) {
	cases := []struct {
		name          string
		entry         config.Redaction
		wantSubstring string
	}{
		{
			name:          "pan with unknown brand",
			entry:         config.Redaction{Table: "u.x", Strategy: "randomize", Form: "pan", Brand: "discover"},
			wantSubstring: "unknown brand",
		},
		{
			name:          "iban with unknown country",
			entry:         config.Redaction{Table: "u.x", Strategy: "randomize", Form: "iban", CountryCode: "US"},
			wantSubstring: "unknown country",
		},
		{
			name:          "ssn with spurious brand",
			entry:         config.Redaction{Table: "u.x", Strategy: "randomize", Form: "ssn", Brand: "visa"},
			wantSubstring: "takes no brand",
		},
		{
			name:          "ssn with spurious country",
			entry:         config.Redaction{Table: "u.x", Strategy: "randomize", Form: "ssn", CountryCode: "DE"},
			wantSubstring: "takes no country_code",
		},
		{
			name:          "ssn with spurious min",
			entry:         config.Redaction{Table: "u.x", Strategy: "randomize", Form: "ssn", Min: 5},
			wantSubstring: "takes no min/max",
		},
		{
			name:          "pan with spurious country",
			entry:         config.Redaction{Table: "u.x", Strategy: "randomize", Form: "pan", CountryCode: "DE"},
			wantSubstring: "takes no country_code",
		},
		{
			name:          "pan with spurious min",
			entry:         config.Redaction{Table: "u.x", Strategy: "randomize", Form: "pan", Min: 5},
			wantSubstring: "takes no min/max",
		},
		{
			name:          "iban with spurious brand",
			entry:         config.Redaction{Table: "u.x", Strategy: "randomize", Form: "iban", Brand: "visa"},
			wantSubstring: "takes no brand",
		},
		{
			name:          "iban with spurious max",
			entry:         config.Redaction{Table: "u.x", Strategy: "randomize", Form: "iban", Max: 5},
			wantSubstring: "takes no min/max",
		},
		{
			name:          "ca-sin with spurious brand",
			entry:         config.Redaction{Table: "u.x", Strategy: "randomize", Form: "ca-sin", Brand: "visa"},
			wantSubstring: "takes no brand",
		},
		{
			name:          "uk-nin with spurious country",
			entry:         config.Redaction{Table: "u.x", Strategy: "randomize", Form: "uk-nin", CountryCode: "DE"},
			wantSubstring: "takes no country_code",
		},
		{
			name:          "int with spurious brand",
			entry:         config.Redaction{Table: "u.x", Strategy: "randomize", Form: "int", Brand: "visa", Min: 1, Max: 10},
			wantSubstring: "takes no brand",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := mergeYAMLRedactions(nil, []config.Redaction{c.entry}, "", "", nil)
			if err == nil {
				t.Fatal("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.wantSubstring) {
				t.Errorf("error %q should contain %q", err.Error(), c.wantSubstring)
			}
		})
	}
}

// TestMergeYAMLRedactions_RandomizeRefusalPaths covers the YAML
// refusal paths: missing form, unknown form, spurious min/max on
// no-options forms, etc.
func TestMergeYAMLRedactions_RandomizeRefusalPaths(t *testing.T) {
	cases := []struct {
		name          string
		entry         config.Redaction
		wantSubstring string
	}{
		{
			name:          "missing form",
			entry:         config.Redaction{Table: "users.x", Strategy: "randomize"},
			wantSubstring: "requires 'form'",
		},
		{
			name:          "unknown form",
			entry:         config.Redaction{Table: "users.x", Strategy: "randomize", Form: "weird"},
			wantSubstring: "unknown form",
		},
		{
			name:          "int with min > max",
			entry:         config.Redaction{Table: "users.x", Strategy: "randomize", Form: "int", Min: 100, Max: 5},
			wantSubstring: "min <= max",
		},
		{
			name:          "email with spurious min",
			entry:         config.Redaction{Table: "users.x", Strategy: "randomize", Form: "email", Min: 5},
			wantSubstring: "takes no min/max",
		},
		{
			name:          "uuid with spurious max",
			entry:         config.Redaction{Table: "users.x", Strategy: "randomize", Form: "uuid", Max: 5},
			wantSubstring: "takes no min/max",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := mergeYAMLRedactions(nil, []config.Redaction{c.entry}, "", "", nil)
			if err == nil {
				t.Fatal("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.wantSubstring) {
				t.Errorf("error %q should contain %q", err.Error(), c.wantSubstring)
			}
		})
	}
}

// TestParseRedactFlags_TokenizeDict covers the v0.61.0 PII Phase 3
// CLI form for tokenize:dict and randomize:dict.
func TestParseRedactFlags_TokenizeDict(t *testing.T) {
	dicts := map[string][]string{
		"first_names": {"Alice", "Bob", "Carol"},
		"cities":      {"Boston", "Denver"},
	}
	cases := []struct {
		name, raw, wantName string
	}{
		{"tokenize:dict", "u.name=tokenize:dict:first_names", "tokenize:dict:first_names"},
		{"randomize:dict", "u.name=randomize:dict:first_names", "randomize:dict:first_names"},
		{"tokenize:dict different dict", "u.city=tokenize:dict:cities", "tokenize:dict:cities"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			reg, err := parseRedactFlags([]string{c.raw}, "", "stream-x", dicts)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			rules := reg.Rules()
			if len(rules) != 1 {
				t.Fatalf("got %d rules; want 1", len(rules))
			}
			if got := rules[0].Strategy.Name(); got != c.wantName {
				t.Errorf("Strategy.Name() = %q; want %q", got, c.wantName)
			}
		})
	}
}

// TestParseRedactFlags_TokenizeDictRefusals covers documented CLI
// refusal paths for the dict strategies.
func TestParseRedactFlags_TokenizeDictRefusals(t *testing.T) {
	dicts := map[string][]string{"first_names": {"Alice", "Bob"}}
	cases := []struct {
		name, raw, wantSubstring string
		dicts                    map[string][]string
	}{
		{"tokenize empty", "u.x=tokenize", "requires a form", dicts},
		{"tokenize unknown form", "u.x=tokenize:other", "unknown form", dicts},
		{"tokenize:dict no name", "u.x=tokenize:dict", "dictionary name is empty", dicts},
		{"tokenize:dict empty name", "u.x=tokenize:dict:", "dictionary name is empty", dicts},
		{"tokenize:dict unknown dict", "u.x=tokenize:dict:nope", "not declared", dicts},
		{"tokenize:dict no dicts loaded", "u.x=tokenize:dict:any", "no dictionaries are loaded", nil},
		{"randomize:dict no name", "u.x=randomize:dict", "requires a dictionary name", dicts},
		{"randomize:dict empty name after colon", "u.x=randomize:dict:", "dictionary name is empty", dicts},
		{"randomize:dict unknown dict", "u.x=randomize:dict:nope", "not declared", dicts},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := parseRedactFlags([]string{c.raw}, "", "", c.dicts)
			if err == nil {
				t.Fatal("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.wantSubstring) {
				t.Errorf("error %q should contain %q", err.Error(), c.wantSubstring)
			}
		})
	}
}

// TestParseRedactFlags_TokenizeDictStreamIDThreaded pins that the
// CLI parser threads the streamID arg into the TokenizeDict
// strategy. Two different streamIDs (likely) produce different
// outputs for the same input through the same dict.
func TestParseRedactFlags_TokenizeDictStreamIDThreaded(t *testing.T) {
	dicts := map[string][]string{
		"names": {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"},
	}
	regA, err := parseRedactFlags([]string{"u.n=tokenize:dict:names"}, "", "stream-A", dicts)
	if err != nil {
		t.Fatalf("regA: %v", err)
	}
	regB, err := parseRedactFlags([]string{"u.n=tokenize:dict:names"}, "", "stream-B", dicts)
	if err != nil {
		t.Fatalf("regB: %v", err)
	}
	differs := false
	for _, in := range []string{"alice", "bob", "carol", "dave", "eve", "frank", "grace", "heidi"} {
		a, _ := regA.Get("", "u", "n").Redact(&ir.Column{Name: "n"}, in, nil)
		b, _ := regB.Get("", "u", "n").Redact(&ir.Column{Name: "n"}, in, nil)
		if a != b {
			differs = true
			break
		}
	}
	if !differs {
		t.Errorf("streamID was not threaded through CLI parser to TokenizeDict")
	}
}

// TestMergeYAMLRedactions_TokenizeDict covers the YAML round-trip
// for dict declaration.
func TestMergeYAMLRedactions_TokenizeDict(t *testing.T) {
	dicts := map[string][]string{
		"first_names": {"Alice", "Bob"},
		"cities":      {"Boston"},
	}
	cases := []struct {
		name     string
		entry    config.Redaction
		wantName string
	}{
		{
			name:     "tokenize form omitted (defaults to dict)",
			entry:    config.Redaction{Table: "u.n", Strategy: "tokenize", Dict: "first_names"},
			wantName: "tokenize:dict:first_names",
		},
		{
			name:     "tokenize form: dict explicit",
			entry:    config.Redaction{Table: "u.n", Strategy: "tokenize", Form: "dict", Dict: "first_names"},
			wantName: "tokenize:dict:first_names",
		},
		{
			name:     "randomize form: dict",
			entry:    config.Redaction{Table: "u.n", Strategy: "randomize", Form: "dict", Dict: "first_names"},
			wantName: "randomize:dict:first_names",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			reg, err := mergeYAMLRedactions(nil, []config.Redaction{c.entry}, "", "stream-x", dicts)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			rules := reg.Rules()
			if len(rules) != 1 {
				t.Fatalf("got %d rules; want 1", len(rules))
			}
			if got := rules[0].Strategy.Name(); got != c.wantName {
				t.Errorf("Strategy.Name() = %q; want %q", got, c.wantName)
			}
		})
	}
}

// TestMergeYAMLRedactions_TokenizeDictRefusals covers documented
// YAML refusal paths.
func TestMergeYAMLRedactions_TokenizeDictRefusals(t *testing.T) {
	dicts := map[string][]string{"first_names": {"Alice"}}
	cases := []struct {
		name          string
		entry         config.Redaction
		wantSubstring string
	}{
		{
			name:          "tokenize without dict field",
			entry:         config.Redaction{Table: "u.n", Strategy: "tokenize"},
			wantSubstring: "requires 'dict' field",
		},
		{
			name:          "tokenize unknown form",
			entry:         config.Redaction{Table: "u.n", Strategy: "tokenize", Form: "weird", Dict: "first_names"},
			wantSubstring: "unknown form",
		},
		{
			name:          "tokenize unknown dict",
			entry:         config.Redaction{Table: "u.n", Strategy: "tokenize", Dict: "nope"},
			wantSubstring: "not declared",
		},
		{
			name:          "tokenize with spurious min",
			entry:         config.Redaction{Table: "u.n", Strategy: "tokenize", Dict: "first_names", Min: 5},
			wantSubstring: "takes no min/max",
		},
		{
			name:          "tokenize with spurious brand",
			entry:         config.Redaction{Table: "u.n", Strategy: "tokenize", Dict: "first_names", Brand: "visa"},
			wantSubstring: "takes no brand",
		},
		{
			name:          "tokenize with spurious country_code",
			entry:         config.Redaction{Table: "u.n", Strategy: "tokenize", Dict: "first_names", CountryCode: "DE"},
			wantSubstring: "takes no country_code",
		},
		{
			name:          "randomize:dict without dict field",
			entry:         config.Redaction{Table: "u.n", Strategy: "randomize", Form: "dict"},
			wantSubstring: "requires 'dict' field",
		},
		{
			name:          "randomize:dict unknown dict",
			entry:         config.Redaction{Table: "u.n", Strategy: "randomize", Form: "dict", Dict: "nope"},
			wantSubstring: "not declared",
		},
		{
			name:          "randomize:dict with spurious brand",
			entry:         config.Redaction{Table: "u.n", Strategy: "randomize", Form: "dict", Dict: "first_names", Brand: "visa"},
			wantSubstring: "takes no brand",
		},
		{
			name:          "randomize:int with spurious dict",
			entry:         config.Redaction{Table: "u.n", Strategy: "randomize", Form: "int", Min: 1, Max: 10, Dict: "first_names"},
			wantSubstring: "takes no dict",
		},
		{
			name:          "hash with spurious dict",
			entry:         config.Redaction{Table: "u.n", Strategy: "hash", Algo: "sha256", Dict: "first_names"},
			wantSubstring: "takes no 'dict' field",
		},
		{
			name:          "static with spurious dict",
			entry:         config.Redaction{Table: "u.n", Strategy: "static", Value: "X", Dict: "first_names"},
			wantSubstring: "takes no 'dict' field",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := mergeYAMLRedactions(nil, []config.Redaction{c.entry}, "", "", dicts)
			if err == nil {
				t.Fatal("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.wantSubstring) {
				t.Errorf("error %q should contain %q", err.Error(), c.wantSubstring)
			}
		})
	}
}

// TestLoadDictionariesAndParse_EndToEnd pins the realistic flow:
// declare a dict in config, load it, parse a CLI rule against it,
// invoke the strategy.
func TestLoadDictionariesAndParse_EndToEnd(t *testing.T) {
	cfg := map[string]config.Dictionary{
		"first_names": {Entries: []string{"Alice", "Bob", "Carol"}},
	}
	loaded, err := redact.LoadDictionaries(cfg)
	if err != nil {
		t.Fatalf("LoadDictionaries: %v", err)
	}
	reg, err := parseRedactFlags([]string{"u.name=tokenize:dict:first_names"}, "", "stream", loaded)
	if err != nil {
		t.Fatalf("parseRedactFlags: %v", err)
	}
	got, err := reg.Get("", "u", "name").Redact(&ir.Column{Name: "name"}, "Alice", nil)
	if err != nil {
		t.Fatalf("Redact: %v", err)
	}
	out, ok := got.(string)
	if !ok {
		t.Fatalf("Redact returned non-string: %T %v", got, got)
	}
	found := false
	for _, e := range []string{"Alice", "Bob", "Carol"} {
		if out == e {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("output %q not in dict; mapping broken", out)
	}
}
