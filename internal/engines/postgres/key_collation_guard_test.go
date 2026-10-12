// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestPlanPGKeyCollationGuard is the Postgres arming truth table.
func TestPlanPGKeyCollationGuard(t *testing.T) {
	src := func(typ ir.Type) *ir.Table {
		return &ir.Table{Name: "kd", Columns: []*ir.Column{{Name: "k", Type: typ}}}
	}
	ciTarget := map[string]targetKeyCollation{"k": {name: "ci", deterministic: false}}
	cases := []struct {
		name   string
		source *ir.Table
		target map[string]targetKeyCollation
		want   string // "" = not armed
	}{
		{"deterministic target never merges", src(ir.Text{Collation: "ci", Determinism: ir.CollationNonDeterministic}), map[string]targetKeyCollation{"k": {name: "", deterministic: true}}, ""},
		{"source C against a nondeterministic key", src(ir.Text{Collation: "C", Determinism: ir.CollationDeterministic}), ciTarget, "C"},
		{"source database default (no determinism at the boundary): residual, unarmed", src(ir.Text{}), ciTarget, ""},
		{"the same nondeterministic collation", src(ir.Text{Collation: "ci", Determinism: ir.CollationNonDeterministic}), ciTarget, ""},
		{"another nondeterministic collation", src(ir.Text{Collation: "ci_ai", Determinism: ir.CollationNonDeterministic}), ciTarget, "ci_ai"},
		{"a MySQL-family source", src(ir.Varchar{Charset: "utf8mb4", Collation: "utf8mb4_bin"}), ciTarget, ""},
		{"a source collation the stream did not carry", src(ir.Text{}), ciTarget, ""},
		{"no source shape", nil, ciTarget, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := planPGKeyCollationGuard(tc.source, []string{"k"}, tc.target)
			got := ""
			if p != nil {
				got = p.compare["k"]
			}
			if got != tc.want {
				t.Fatalf("compare under %q; want %q", got, tc.want)
			}
		})
	}
}

// TestBuildInsertSQL_KeyCollationGuard pins the guarded statement shapes:
// the aliased target, the CASE whose else-branch raises with the marker, and
// an all-key table's guarded DO UPDATE in place of DO NOTHING.
func TestBuildInsertSQL_KeyCollationGuard(t *testing.T) {
	plan := &pgKeyCollationPlan{compare: map[string]string{"k": "C"}, marker: "k"}
	stmt, _, err := buildInsertSQL("public", "kd", ir.Row{"k": "a", "d": int64(1)}, []string{"k"}, nil, nil, plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`INSERT INTO "public"."kd" AS "sluice_existing"`, `DO UPDATE SET "d" = EXCLUDED."d" WHERE CASE WHEN`,
		`"sluice_existing"."k" COLLATE "C" IS NOT DISTINCT FROM EXCLUDED."k" COLLATE "C" THEN true ELSE`, "KEY-COLLATION-MERGE",
	} {
		if !strings.Contains(stmt, want) {
			t.Errorf("statement lacks %q:\n%s", want, stmt)
		}
	}
	allKey, _, err := buildInsertSQL("public", "ak", ir.Row{"k": "a"}, []string{"k"}, nil, nil, plan)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(allKey, "DO NOTHING") || !strings.Contains(allKey, `DO UPDATE SET "k" = "sluice_existing"."k" WHERE CASE WHEN`) ||
		!strings.Contains(allKey, "THEN false ELSE") {
		t.Errorf("all-key guarded statement:\n%s", allKey)
	}
}
