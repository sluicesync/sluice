// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"net/url"
	"strings"
	"testing"
)

// TestIsSystemSchema pins the system-schema exclusion set AND the
// non-system-lookalike battery (ADR-0075). A false exclusion silently
// drops a USER schema (the inverse of the MySQL `_vt_*` over-broad-match
// lesson), so a schema literally named `information_schema_data` /
// `pg_catalogue` / `pg_temporary` MUST NOT be excluded.
func TestIsSystemSchema(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		// Real system schemas — excluded.
		{"pg_catalog", true},
		{"information_schema", true},
		{"pg_toast", true},
		// Session-temp namespaces (per-backend numeric suffix) — excluded.
		{"pg_temp_1", true},
		{"pg_temp_42", true},
		{"pg_toast_temp_1", true},
		{"pg_toast_temp_99", true},
		// Non-system lookalikes — MUST be kept (user data).
		{"information_schema_data", false},
		{"information_schemas", false},
		{"pg_catalogue", false},
		{"pg_catalog_user", false},
		{"pg_temporary", false}, // not pg_temp_<n>
		{"pg_toast_user", false},
		{"public", false},
		{"app_sales", false},
		{"Sales", false},
		{"my_pg_temp_1", false}, // prefix match must be anchored at start
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := isSystemSchema(c.name); got != c.want {
				t.Errorf("isSystemSchema(%q) = %v; want %v", c.name, got, c.want)
			}
		})
	}
}

// TestWithDatabaseURI verifies the URI-form schema rebind sets the
// sluice-custom `schema` query parameter and leaves the rest intact.
func TestWithDatabaseURI(t *testing.T) {
	const base = "postgres://u:p@host:5432/appdb?sslmode=disable"
	got, err := Engine{}.WithDatabase(base, "sales")
	if err != nil {
		t.Fatalf("WithDatabase: %v", err)
	}
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse result: %v", err)
	}
	if u.Query().Get("schema") != "sales" {
		t.Errorf("schema = %q; want sales (DSN=%q)", u.Query().Get("schema"), got)
	}
	if u.Query().Get("sslmode") != "disable" {
		t.Errorf("sslmode lost; DSN=%q", got)
	}
	if u.Path != "/appdb" {
		t.Errorf("database path changed = %q; want /appdb", u.Path)
	}
	// parseDSN must round-trip the schema back out.
	cfg, err := Engine{}.parseDSN(got)
	if err != nil {
		t.Fatalf("parseDSN(result): %v", err)
	}
	if cfg.schema != "sales" {
		t.Errorf("parseDSN schema = %q; want sales", cfg.schema)
	}
}

// TestWithDatabaseURI_OverridesExisting confirms an existing schema
// parameter is replaced, not duplicated.
func TestWithDatabaseURI_OverridesExisting(t *testing.T) {
	const base = "postgres://u:p@host:5432/appdb?schema=public&sslmode=disable"
	got, err := Engine{}.WithDatabase(base, "billing")
	if err != nil {
		t.Fatalf("WithDatabase: %v", err)
	}
	u, _ := url.Parse(got)
	schemas := u.Query()["schema"]
	if len(schemas) != 1 || schemas[0] != "billing" {
		t.Errorf("schema params = %v; want exactly [billing]", schemas)
	}
}

// TestWithDatabaseKV verifies the libpq KV form replaces / appends the
// schema= token and round-trips through parseDSN.
func TestWithDatabaseKV(t *testing.T) {
	cases := []struct {
		name string
		base string
	}{
		{"no existing schema", "host=localhost user=u dbname=appdb sslmode=disable"},
		{"existing schema replaced", "host=localhost user=u dbname=appdb schema=public sslmode=disable"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := Engine{}.WithDatabase(c.base, "warehouse")
			if err != nil {
				t.Fatalf("WithDatabase: %v", err)
			}
			cfg, err := Engine{}.parseDSN(got)
			if err != nil {
				t.Fatalf("parseDSN(result %q): %v", got, err)
			}
			if cfg.schema != "warehouse" {
				t.Errorf("parseDSN schema = %q; want warehouse (DSN=%q)", cfg.schema, got)
			}
			// Exactly one schema= token in the derived DSN.
			if n := strings.Count(got, "schema="); n != 1 {
				t.Errorf("schema= token count = %d; want 1 (DSN=%q)", n, got)
			}
			// dbname (connection boundary) untouched.
			if !strings.Contains(got, "dbname=appdb") {
				t.Errorf("dbname lost; DSN=%q", got)
			}
		})
	}
}

// TestWithDatabaseRejectsEmpty pins the loud-failure on empty inputs.
func TestWithDatabaseRejectsEmpty(t *testing.T) {
	if _, err := (Engine{}).WithDatabase("postgres://h/db", ""); err == nil {
		t.Error("expected error for empty schema; got nil")
	}
	if _, err := (Engine{}).WithDatabase("", "sales"); err == nil {
		t.Error("expected error for empty DSN; got nil")
	}
}
