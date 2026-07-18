// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import "testing"

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
