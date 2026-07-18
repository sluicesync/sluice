// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
)

func TestParseWhereFilters(t *testing.T) {
	t.Run("empty returns nil", func(t *testing.T) {
		got, err := parseWhereFilters(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Fatalf("want nil map, got %v", got)
		}
	})

	t.Run("splits at first equals so predicate may contain equals", func(t *testing.T) {
		got, err := parseWhereFilters([]string{"users=country = 'US'"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got["users"] != "country = 'US'" {
			t.Fatalf("want predicate %q, got %q", "country = 'US'", got["users"])
		}
	})

	t.Run("multiple tables + disjunctive predicate", func(t *testing.T) {
		got, err := parseWhereFilters([]string{
			"users=country IN ('US','CA') OR vip = 1",
			"orders=total > 100",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got["users"] != "country IN ('US','CA') OR vip = 1" {
			t.Errorf("users predicate wrong: %q", got["users"])
		}
		if got["orders"] != "total > 100" {
			t.Errorf("orders predicate wrong: %q", got["orders"])
		}
	})

	t.Run("trims surrounding whitespace on key and predicate", func(t *testing.T) {
		got, err := parseWhereFilters([]string{"  users  =  country = 'US'  "})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := got["users"]; !ok {
			t.Fatalf("key not trimmed: %v", got)
		}
		if got["users"] != "country = 'US'" {
			t.Fatalf("predicate not trimmed: %q", got["users"])
		}
	})

	refusals := []struct {
		name  string
		value string
	}{
		{"no equals", "users"},
		{"empty table", "=country = 'US'"},
		{"empty predicate", "users="},
	}
	for _, r := range refusals {
		t.Run("refuses "+r.name, func(t *testing.T) {
			if _, err := parseWhereFilters([]string{r.value}); err == nil {
				t.Fatalf("parseWhereFilters(%q) err = nil; want a loud refusal", r.value)
			}
		})
	}

	t.Run("refuses duplicate table key", func(t *testing.T) {
		_, err := parseWhereFilters([]string{"users=a = 1", "users=b = 2"})
		if err == nil {
			t.Fatal("duplicate table key err = nil; want a loud refusal")
		}
	})
}

// TestAllowDegradedFKsFlagSpelling pins that the flag surfaced by the --where
// FK-orphan path parses under the spelling the docs, --help, and the
// SLUICE-E-WHERE-FK-ORPHAN hint all recommend. Without an explicit name: tag,
// kong auto-kebabs the AllowDegradedFKs field to --allow-degraded-f-ks (it
// splits the FKs capital run), so the documented --allow-degraded-fks would be
// rejected. The v0.99.276 regression cycle caught that mismatch; this pins the
// tag so a future dropped name: tag re-breaks loudly rather than silently.
func TestAllowDegradedFKsFlagSpelling(t *testing.T) {
	baseArgs := "migrate --source-driver=mysql --source=src --target-driver=postgres --target=tgt"
	cli := parseInto(t, append(strings.Fields(baseArgs), "--allow-degraded-fks")...)
	if !cli.Migrate.AllowDegradedFKs {
		t.Fatal("--allow-degraded-fks did not bind AllowDegradedFKs=true")
	}
}
