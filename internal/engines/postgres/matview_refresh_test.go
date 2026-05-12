// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"
)

// TestBuildRefreshStatement pins the SQL shape across the four
// argument permutations. The case set isn't exhaustive of identifier
// quoting (covered by quoteIdent's own tests) — just the matview-
// refresh-specific concerns: concurrently flag, schema qualification,
// case-preserving names.
func TestBuildRefreshStatement(t *testing.T) {
	cases := []struct {
		name         string
		schema       string
		matview      string
		concurrently bool
		want         string
	}{
		{
			"plain refresh in public schema",
			"public", "user_stats", false,
			`REFRESH MATERIALIZED VIEW "public"."user_stats"`,
		},
		{
			"concurrent refresh in public schema",
			"public", "user_stats", true,
			`REFRESH MATERIALIZED VIEW CONCURRENTLY "public"."user_stats"`,
		},
		{
			"plain refresh in non-default schema",
			"analytics", "daily_signups", false,
			`REFRESH MATERIALIZED VIEW "analytics"."daily_signups"`,
		},
		{
			"concurrent refresh in non-default schema",
			"analytics", "daily_signups", true,
			`REFRESH MATERIALIZED VIEW CONCURRENTLY "analytics"."daily_signups"`,
		},
		{
			"mixed-case matview name preserved",
			"public", "UserDashboard", false,
			`REFRESH MATERIALIZED VIEW "public"."UserDashboard"`,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := buildRefreshStatement(c.schema, c.matview, c.concurrently)
			if got != c.want {
				t.Errorf("buildRefreshStatement(%q, %q, %v) =\n  got  %q\n  want %q",
					c.schema, c.matview, c.concurrently, got, c.want)
			}
		})
	}
}

// TestFilterMatviewsByName pins the filter shape — empty filter
// returns the input unchanged; non-empty filter narrows to matching
// names. Validates the order is preserved (alphabetical by input).
func TestFilterMatviewsByName(t *testing.T) {
	available := []string{"alpha", "beta", "delta", "gamma"}

	t.Run("empty filter returns all", func(t *testing.T) {
		got := filterMatviewsByName(available, nil)
		if !equalStrings(got, available) {
			t.Errorf("empty filter: got %v; want %v", got, available)
		}
	})

	t.Run("single match", func(t *testing.T) {
		got := filterMatviewsByName(available, []string{"beta"})
		want := []string{"beta"}
		if !equalStrings(got, want) {
			t.Errorf("got %v; want %v", got, want)
		}
	})

	t.Run("multi match preserves order", func(t *testing.T) {
		got := filterMatviewsByName(available, []string{"gamma", "alpha"})
		// Output is in `available`'s order (alphabetical), not the
		// filter's order — the filter is a set, not a sequence.
		want := []string{"alpha", "gamma"}
		if !equalStrings(got, want) {
			t.Errorf("got %v; want %v", got, want)
		}
	})

	t.Run("non-existent filter returns empty", func(t *testing.T) {
		got := filterMatviewsByName(available, []string{"missing"})
		if len(got) != 0 {
			t.Errorf("got %v; want empty", got)
		}
	})
}

// TestValidateMatviewFilter pins the loud-failure-on-typo path: a
// requested matview that doesn't exist surfaces as an actionable
// error naming the missing name(s), not a silent no-op.
func TestValidateMatviewFilter(t *testing.T) {
	available := []string{"alpha", "beta", "gamma"}

	t.Run("empty request is no-op", func(t *testing.T) {
		if err := validateMatviewFilter(nil, available); err != nil {
			t.Errorf("err = %v; want nil for empty request", err)
		}
	})

	t.Run("all requested present is OK", func(t *testing.T) {
		if err := validateMatviewFilter([]string{"alpha", "beta"}, available); err != nil {
			t.Errorf("err = %v; want nil for all-present", err)
		}
	})

	t.Run("missing matview surfaces error", func(t *testing.T) {
		err := validateMatviewFilter([]string{"alpha", "delta"}, available)
		if err == nil {
			t.Fatal("err = nil; want error for missing matview")
		}
		if !strings.Contains(err.Error(), "delta") {
			t.Errorf("err = %v; want missing name 'delta' in message", err)
		}
		if !strings.Contains(err.Error(), "not found") {
			t.Errorf("err = %v; want 'not found' wording", err)
		}
	})

	t.Run("multiple missing surfaces all names", func(t *testing.T) {
		err := validateMatviewFilter([]string{"delta", "epsilon"}, available)
		if err == nil {
			t.Fatal("err = nil; want error")
		}
		if !strings.Contains(err.Error(), "delta") || !strings.Contains(err.Error(), "epsilon") {
			t.Errorf("err = %v; want both missing names", err)
		}
	})
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
