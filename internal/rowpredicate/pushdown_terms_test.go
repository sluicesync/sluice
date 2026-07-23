// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

import (
	"reflect"
	"testing"
)

// TestPushdownTerms pins the flattened leaf-comparison surface the
// ADR-0176 push-down classifier consumes: one term per leaf in AST walk
// order, columns lower-cased, and the bool-vs-0/1-literal flag set for
// exactly the comparisons Postgres SQL would reject.
func TestPushdownTerms(t *testing.T) {
	infos := map[string]ColumnInfo{
		"id":      {Family: FamilyNumeric},
		"name":    {Family: FamilyString, Faithful: true},
		"active":  {Family: FamilyBool},
		"deleted": {Family: FamilyBool},
		"d":       {Family: FamilyTemporal},
	}

	tests := []struct {
		name      string
		predicate string
		want      []PushdownTerm
	}{
		{
			name:      "compound walk order, IS NULL and IN included",
			predicate: "NOT (id = 5 OR name IN ('a', 'b')) AND id IS NULL",
			want: []PushdownTerm{
				{Column: "id"},
				{Column: "name"},
				{Column: "id"},
			},
		},
		{
			name:      "bool vs TRUE literal is NOT flagged",
			predicate: "active = TRUE",
			want:      []PushdownTerm{{Column: "active"}},
		},
		{
			name:      "bool vs 0/1 numeric literal IS flagged (invalid PG SQL)",
			predicate: "active = 1",
			want:      []PushdownTerm{{Column: "active", BoolNumericLiteral: true}},
		},
		{
			name:      "bool IN with a numeric member IS flagged",
			predicate: "active IN (1)",
			want:      []PushdownTerm{{Column: "active", BoolNumericLiteral: true}},
		},
		{
			name:      "bool IN with only keyword literals is NOT flagged",
			predicate: "active IN (TRUE, FALSE)",
			want:      []PushdownTerm{{Column: "active"}},
		},
		{
			name:      "column casing is normalized like Compile's",
			predicate: `"Active" = FALSE AND ID < 3`,
			want: []PushdownTerm{
				{Column: "active"},
				{Column: "id"},
			},
		},

		// ---- Temporal literal-granularity flags (audit 2026-07-23 D0-5) ----
		{
			name:      "pure-date temporal literal carries no granularity flag",
			predicate: "d = '2026-01-15'",
			want:      []PushdownTerm{{Column: "d"}},
		},
		{
			name:      "time-bearing literal (space form, no seconds) is flagged",
			predicate: "d < '2026-01-15 08:30'",
			want:      []PushdownTerm{{Column: "d", TemporalLiteralTimeBearing: true}},
		},
		{
			name:      "time-bearing literal (T form) is flagged",
			predicate: "d != '2026-01-15T08:30:00'",
			want:      []PushdownTerm{{Column: "d", TemporalLiteralTimeBearing: true}},
		},
		{
			name:      "midnight is still time-bearing (the flag keys on the TEXT, conservatively)",
			predicate: "d = '2026-01-15 00:00:00'",
			want:      []PushdownTerm{{Column: "d", TemporalLiteralTimeBearing: true}},
		},
		{
			name:      "exactly 6 fractional digits is time-bearing but NOT sub-microsecond",
			predicate: "d >= '2026-01-15 08:30:00.123456'",
			want:      []PushdownTerm{{Column: "d", TemporalLiteralTimeBearing: true}},
		},
		{
			name:      "7 fractional digits is sub-microsecond (PG rounds to µs, the client keeps ns)",
			predicate: "d >= '2026-01-15 08:30:00.1234567'",
			want:      []PushdownTerm{{Column: "d", TemporalLiteralTimeBearing: true, TemporalLiteralSubMicrosecond: true}},
		},
		{
			name:      "IN list flags fold across members (one time-bearing member flags the term)",
			predicate: "d IN ('2026-01-15', '2026-01-16 08:30:00.1234567')",
			want:      []PushdownTerm{{Column: "d", TemporalLiteralTimeBearing: true, TemporalLiteralSubMicrosecond: true}},
		},
		{
			name:      "IN list of pure dates carries no granularity flag",
			predicate: "d NOT IN ('2026-01-15', '2026-01-16')",
			want:      []PushdownTerm{{Column: "d"}},
		},
		{
			name:      "granularity flags survive NOT/AND composition",
			predicate: "NOT (d >= '2026-01-15 12:00:00') AND id < 3",
			want: []PushdownTerm{
				{Column: "d", TemporalLiteralTimeBearing: true},
				{Column: "id"},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := Compile("t", tc.predicate, infos)
			if err != nil {
				t.Fatalf("Compile(%q): %v", tc.predicate, err)
			}
			if got := p.PushdownTerms(); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("PushdownTerms(%q) = %+v, want %+v", tc.predicate, got, tc.want)
			}
		})
	}

	var nilPred *Predicate
	if got := nilPred.PushdownTerms(); got != nil {
		t.Errorf("nil predicate PushdownTerms() = %v, want nil", got)
	}
}
