// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestBuildSelect_WhereFilter pins the ADR-0173 Phase 1 row filter on the
// MySQL full-scan read: the predicate is ANDed as the sole (parenthesized)
// WHERE clause; a disjunctive predicate stays whole.
func TestBuildSelect_WhereFilter(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "country", Type: ir.Varchar{Length: 2}},
		},
	}
	cases := []struct {
		name      string
		predicate string
		want      string
	}{
		{
			name:      "simple",
			predicate: "country = 'US'",
			want:      "SELECT `id`, `country` FROM `users` WHERE (country = 'US')",
		},
		{
			name:      "disjunctive stays parenthesized",
			predicate: "country = 'US' OR country = 'CA'",
			want:      "SELECT `id`, `country` FROM `users` WHERE (country = 'US' OR country = 'CA')",
		},
		{
			name:      "empty predicate is byte-identical",
			predicate: "",
			want:      "SELECT `id`, `country` FROM `users`",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := buildSelect(table, false, c.predicate); got != c.want {
				t.Errorf("buildSelect:\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestBuildBatchedSelect_WhereFilter pins that the row filter is added as
// one more parenthesized conjunct in the keyset WHERE — before ORDER
// BY/LIMIT — on the MySQL cursor-paged read.
func TestBuildBatchedSelect_WhereFilter(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "country", Type: ir.Varchar{Length: 2}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	cases := []struct {
		name                string
		hasCursor, hasUpper bool
		predicate           string
		want                string
	}{
		{
			name:      "first batch + filter",
			predicate: "country = 'US'",
			want:      "SELECT `id`, `country` FROM `users` WHERE (country = 'US') ORDER BY `users`.`id` LIMIT 5000",
		},
		{
			name:      "bounded chunk + disjunctive filter",
			hasCursor: true,
			hasUpper:  true,
			predicate: "country = 'US' OR country = 'CA'",
			want:      "SELECT `id`, `country` FROM `users` WHERE (`users`.`id`) > (?) AND (`users`.`id`) <= (?) AND (country = 'US' OR country = 'CA') ORDER BY `users`.`id` LIMIT 5000",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := buildBatchedSelect(table, 5000, c.hasCursor, c.hasUpper, c.predicate)
			if got != c.want {
				t.Errorf("buildBatchedSelect:\n got  %q\n want %q", got, c.want)
			}
		})
	}
}
