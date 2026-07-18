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

// TestWhereFlagKongBinding pins --where through the REAL kong parser on all
// three subcommands (migrate / sync start / verify) — the Bug-180 "pin through
// the CLI layer" lesson (audit F-T3). The load-bearing property is the
// sep:"none" tag: a repeatable flag defaults to comma-splitting, which would
// shred a comma-bearing IN-list ("region IN ('EU','US')") into corrupt
// fragments — a filtered migrate/verify silently scoping the wrong rows, and it
// would ship green because where_filter_test.go only exercises pre-split
// []string. Dropping sep:"none" (or a mis-kebab, the class that broke
// --allow-degraded-fks in v0.99.276) fails this test.
func TestWhereFlagKongBinding(t *testing.T) {
	const whereVal = "orders=region IN ('EU','US')"
	cases := []struct {
		name string
		base string
		get  func(*CLI) []string
	}{
		{"migrate", "migrate --source-driver=mysql --source=src --target-driver=postgres --target=tgt", func(c *CLI) []string { return c.Migrate.Where }},
		{"sync start", "sync start --source-driver=mysql --source=src --target-driver=postgres --target=tgt", func(c *CLI) []string { return c.Sync.Start.Where }},
		{"verify", "verify --source-driver=mysql --source=src --target-driver=postgres --target=tgt", func(c *CLI) []string { return c.Verify.Where }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Append the --where value as a SINGLE argv element (it contains
			// spaces); only the base is field-split.
			args := append(strings.Fields(tc.base), "--where="+whereVal)
			cli := parseInto(t, args...)
			got := tc.get(cli)
			if len(got) != 1 {
				t.Fatalf("--where bound %d values (%q); want 1 — sep:\"none\" must keep the comma-bearing IN-list intact", len(got), got)
			}
			if got[0] != whereVal {
				t.Fatalf("--where[0] = %q; want %q (the comma inside IN(...) was split)", got[0], whereVal)
			}
		})
	}
}

// TestWhereStrictCollationFlag pins that --where-strict-collation parses onto
// the sync-start field and defaults OFF (faithful mode) — the zero-value-safe
// default (ADR-0174). Pinned through the real kong parser (the Bug-180
// lesson) so a dropped tag or an inverted default is caught loudly.
func TestWhereStrictCollationFlag(t *testing.T) {
	base := "sync start --source-driver=mysql --source=src --target-driver=postgres --target=tgt"

	t.Run("defaults off (faithful mode)", func(t *testing.T) {
		cli := parseInto(t, strings.Fields(base)...)
		if cli.Sync.Start.WhereStrictCollation {
			t.Fatal("WhereStrictCollation defaulted true; the faithful default must be the zero value")
		}
	})
	t.Run("flag sets it", func(t *testing.T) {
		cli := parseInto(t, append(strings.Fields(base), "--where-strict-collation")...)
		if !cli.Sync.Start.WhereStrictCollation {
			t.Fatal("--where-strict-collation did not bind WhereStrictCollation=true")
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
