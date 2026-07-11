// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

func TestLoadEmptyPath(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatalf(`Load(""): %v`, err)
	}
	if c == nil {
		t.Fatal("Load returned nil Config; want empty")
	}
	if len(c.Mappings) != 0 {
		t.Errorf("expected no mappings; got %#v", c.Mappings)
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load("/nonexistent/sluice.yaml")
	if err == nil {
		t.Error("expected error for missing file; got nil")
	}
	// Load errors are typed as sluicecode.ConfigError so the CLI exit
	// boundary maps them to exit code 2 (config error) — this package
	// is the single wrapping chokepoint.
	var ce *sluicecode.ConfigError
	if !errors.As(err, &ce) {
		t.Fatalf("Load error %v does not carry a sluicecode.ConfigError", err)
	}
	if got := ce.ExitCode(); got != sluicecode.ExitConfig {
		t.Errorf("ExitCode() = %d; want %d", got, sluicecode.ExitConfig)
	}
}

func TestLoadYAML(t *testing.T) {
	yamlContent := `
mappings:
  - table: orders
    column: status
    target_type: text
  - table: events
    column: payload
    target_type: jsonb
    target_type_options:
      binary: true

extensions:
  allow:
    - citext
    - pg_trgm
`
	dir := t.TempDir()
	path := filepath.Join(dir, "sluice.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0o600); err != nil {
		t.Fatal(err)
	}

	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(c.Mappings) != 2 {
		t.Fatalf("got %d mappings; want 2", len(c.Mappings))
	}
	if c.Mappings[0].Table != "orders" || c.Mappings[0].Column != "status" || c.Mappings[0].TargetType != "text" {
		t.Errorf("Mappings[0] = %#v; unexpected", c.Mappings[0])
	}
	if c.Mappings[1].TargetType != "jsonb" {
		t.Errorf("Mappings[1].TargetType = %q; want jsonb", c.Mappings[1].TargetType)
	}
	if !reflect.DeepEqual(c.Extensions.Allow, []string{"citext", "pg_trgm"}) {
		t.Errorf("Extensions.Allow = %v; want [citext pg_trgm]", c.Extensions.Allow)
	}
}

// TestLoadYAML_Redactions covers the PII Phase 1.5 YAML block.
// Each strategy form should parse into the corresponding
// Redaction struct.
func TestLoadYAML_Redactions(t *testing.T) {
	yamlContent := `
redactions:
  - table: users.email
    strategy: hash
    algo: sha256
  - table: users.phone
    strategy: truncate
    length: 4
  - table: billing.accounts.ssn
    strategy: static
    value: REDACTED
  - table: users.middle_name
    strategy: "null"
keyset_source: file:/etc/sluice/keyset.yaml
`
	dir := t.TempDir()
	path := filepath.Join(dir, "sluice.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0o600); err != nil {
		t.Fatal(err)
	}

	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(c.Redactions) != 4 {
		t.Fatalf("got %d redactions; want 4", len(c.Redactions))
	}
	if c.KeysetSource != "file:/etc/sluice/keyset.yaml" {
		t.Errorf("KeysetSource = %q; want %q", c.KeysetSource, "file:/etc/sluice/keyset.yaml")
	}

	cases := []struct {
		idx                          int
		table, strategy, algo, value string
		length                       int
	}{
		{0, "users.email", "hash", "sha256", "", 0},
		{1, "users.phone", "truncate", "", "", 4},
		{2, "billing.accounts.ssn", "static", "", "REDACTED", 0},
		{3, "users.middle_name", "null", "", "", 0},
	}
	for _, c2 := range cases {
		r := c.Redactions[c2.idx]
		if r.Table != c2.table {
			t.Errorf("Redactions[%d].Table = %q; want %q", c2.idx, r.Table, c2.table)
		}
		if r.Strategy != c2.strategy {
			t.Errorf("Redactions[%d].Strategy = %q; want %q", c2.idx, r.Strategy, c2.strategy)
		}
		if r.Algo != c2.algo {
			t.Errorf("Redactions[%d].Algo = %q; want %q", c2.idx, r.Algo, c2.algo)
		}
		if r.Value != c2.value {
			t.Errorf("Redactions[%d].Value = %q; want %q", c2.idx, r.Value, c2.value)
		}
		if r.Length != c2.length {
			t.Errorf("Redactions[%d].Length = %d; want %d", c2.idx, r.Length, c2.length)
		}
	}
}

// TestLoadYAML_UnknownKeyRejected pins N-10: an unknown/typo'd YAML key is a
// LOUD load failure, not a silent drop. The trap this closes: a mistyped
// `redactions:` block (or a misspelled field inside one) was silently ignored,
// so the operator believed PII was being redacted while it was not — a
// compliance-grade silent-loss. Every documented config key is a Config-struct
// field, so this only rejects genuinely-unsupported keys.
func TestLoadYAML_UnknownKeyRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		yaml string
	}{
		{"top-level typo: `redaction` vs `redactions` (the PII-leak trap)", "redaction:\n  - table: users.email\n    strategy: hash\n"},
		{"nested field typo: `tabel` vs `table`", "redactions:\n  - tabel: users.email\n    strategy: hash\n"},
		{"unknown top-level key", "frobnicate: true\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "sluice.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err == nil {
				t.Fatal("Load accepted an unknown/typo'd YAML key — a typo'd redactions block would silently drop PII rules (N-10)")
			}
		})
	}
}

// TestLoadIncludeExcludeTables checks the table-filter YAML fields
// round-trip through the loader. Operators put these alongside
// mappings in sluice.yaml; the orchestrator builds a TableFilter
// from them when no CLI flag overrides.
func TestLoadIncludeExcludeTables(t *testing.T) {
	yamlContent := `
include_tables:
  - users
  - orders
exclude_tables:
  - "audit_*"
  - sessions
`
	dir := t.TempDir()
	path := filepath.Join(dir, "sluice.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !reflect.DeepEqual(c.IncludeTables, []string{"users", "orders"}) {
		t.Errorf("IncludeTables = %v; want [users orders]", c.IncludeTables)
	}
	if !reflect.DeepEqual(c.ExcludeTables, []string{"audit_*", "sessions"}) {
		t.Errorf("ExcludeTables = %v; want [audit_* sessions]", c.ExcludeTables)
	}
}

// TestEnvVarsLayer verifies the documented example still works after
// the audit N-10 mapping rework: SLUICE_EXTENSIONS_ALLOW is a NESTED
// key (extensions.allow), and koanf's default unmarshal splits the
// comma-separated env value into the slice.
func TestEnvVarsLayer(t *testing.T) {
	t.Setenv("SLUICE_EXTENSIONS_ALLOW", "citext,uuid-ossp")

	c, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !reflect.DeepEqual(c.Extensions.Allow, []string{"citext", "uuid-ossp"}) {
		t.Errorf("Extensions.Allow = %v; want [citext uuid-ossp] (the documented SLUICE_EXTENSIONS_ALLOW example)", c.Extensions.Allow)
	}
}

// TestEnvVars_UnderscoreKeyedFields pins the audit N-10 fix: pre-fix
// the env mapping replaced EVERY underscore with a dot, so
// SLUICE_KEYSET_SOURCE resolved to keyset.source ≠ koanf:"keyset_source"
// and was SILENTLY dropped — for every underscore-keyed field,
// including keyset_source (a secrets pointer operators put in env by
// design). Post-fix flat keys keep their underscores.
func TestEnvVars_UnderscoreKeyedFields(t *testing.T) {
	t.Setenv("SLUICE_KEYSET_SOURCE", "env:SLUICE_KEYSET")
	t.Setenv("SLUICE_INCLUDE_TABLES", "users,orders")
	t.Setenv("SLUICE_NAMESPACE_MAP_APP", "app_prod")

	c, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.KeysetSource != "env:SLUICE_KEYSET" {
		t.Errorf("KeysetSource = %q; want %q (SLUICE_KEYSET_SOURCE was silently dropped pre-fix)", c.KeysetSource, "env:SLUICE_KEYSET")
	}
	if !reflect.DeepEqual(c.IncludeTables, []string{"users", "orders"}) {
		t.Errorf("IncludeTables = %v; want [users orders]", c.IncludeTables)
	}
	if got := c.NamespaceMap["app"]; got != "app_prod" {
		t.Errorf("NamespaceMap[app] = %q; want app_prod (map-valued env key)", got)
	}
}

// TestEnvVars_PrecedenceOverYAML pins that the env overlay still wins
// over the YAML file for the newly-reachable underscore keys.
func TestEnvVars_PrecedenceOverYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sluice.yaml")
	if err := os.WriteFile(path, []byte("keyset_source: file:/from-yaml.yaml\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SLUICE_KEYSET_SOURCE", "file:/from-env.yaml")

	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.KeysetSource != "file:/from-env.yaml" {
		t.Errorf("KeysetSource = %q; want the env value to override YAML", c.KeysetSource)
	}
}

// TestEnvVars_UnknownVarWarnsLoudly pins the unknown-variable posture:
// a SLUICE_ variable that resolves to no config key is skipped from
// the overlay with a WARN naming the variable and the valid keys —
// never silently dropped (the N-10 class), and never a hard error
// (SLUICE_ names are also legitimate as kong env bindings and
// operator-chosen secret holders, e.g. env:SLUICE_BACKUP_PASS).
func TestEnvVars_UnknownVarWarnsLoudly(t *testing.T) {
	t.Setenv("SLUICE_TYPO_KEY", "1")
	// A kong-bound process var must NOT warn (it is consumed by the CLI).
	t.Setenv("SLUICE_SOURCE", "postgres://u:p@h/db")

	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	c, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.KeysetSource != "" || len(c.IncludeTables) != 0 {
		t.Errorf("typo var mutated the config: %+v", c)
	}
	logs := logBuf.String()
	if !strings.Contains(logs, "SLUICE_TYPO_KEY") {
		t.Errorf("WARN log %q missing the offending variable name", logs)
	}
	if !strings.Contains(logs, "keyset_source") {
		t.Errorf("WARN log %q should list the valid keys", logs)
	}
	if strings.Contains(logs, "SLUICE_SOURCE") {
		t.Errorf("WARN log %q flagged the kong-bound SLUICE_SOURCE — process-level vars are exempt", logs)
	}
}

// TestEnvKeyIndex_Resolve pins the resolver's mapping table shape:
// flat, nested, map-valued, and YAML-only block keys.
func TestEnvKeyIndex_Resolve(t *testing.T) {
	idx := buildEnvKeyIndex()
	cases := []struct {
		env     string
		wantKey string
		wantOK  bool
	}{
		{"SLUICE_KEYSET_SOURCE", "keyset_source", true},
		{"SLUICE_INCLUDE_TABLES", "include_tables", true},
		{"SLUICE_EXCLUDE_TABLES", "exclude_tables", true},
		{"SLUICE_EXTENSIONS_ALLOW", "extensions.allow", true},
		{"SLUICE_NAMESPACE_MAP_APP", "namespace_map.app", true},
		// Preserves underscores inside the map key.
		{"SLUICE_NAMESPACE_MAP_MY_APP", "namespace_map.my_app", true},
		// Block-structured keys are YAML-only: not resolvable from env.
		{"SLUICE_MAPPINGS", "", false},
		{"SLUICE_REDACTIONS", "", false},
		{"SLUICE_DICTIONARIES_FIRST_NAMES", "", false},
		{"SLUICE_TYPO_KEY", "", false},
	}
	for _, c := range cases {
		key, ok := idx.resolve(c.env)
		if key != c.wantKey || ok != c.wantOK {
			t.Errorf("resolve(%s) = (%q, %v); want (%q, %v)", c.env, key, ok, c.wantKey, c.wantOK)
		}
	}
}
