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
		refuse bool   // the guard cannot be written: KEY-COLLATION-GUARD-UNSUPPORTED
	}{
		{"deterministic target never merges", src(ir.Text{Collation: "ci", Determinism: ir.CollationNonDeterministic}), map[string]targetKeyCollation{"k": {name: "", deterministic: true}}, "", false},
		{"source C against a nondeterministic key", src(ir.Text{Collation: "C", Determinism: ir.CollationDeterministic}), ciTarget, "C", false},
		{"source C through a domain", src(ir.Domain{Name: "code", BaseType: ir.Text{Collation: "C", Determinism: ir.CollationDeterministic}}), ciTarget, "C", false},
		// The PG then-retype warm cells: a key moved to the database default
		// while the stream lagged. The readers stamp the default
		// deterministic, so it compares bytes against a nondeterministic key.
		{"source database default, stamped deterministic", src(ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic}), ciTarget, "C", false},
		{"no collation carried at all (a schema from an older binary): unarmed", src(ir.Text{}), ciTarget, "", false},
		{"the same nondeterministic collation", src(ir.Text{Collation: "ci", Determinism: ir.CollationNonDeterministic}), ciTarget, "", false},
		{"another nondeterministic collation", src(ir.Text{Collation: "ci_ai", Determinism: ir.CollationNonDeterministic}), ciTarget, "ci_ai", false},
		// KEY-COLLATION-PG-LOOSENED-REPLAY: the reader read the collation from
		// a catalog row newer than the rows, so it stamps the name with no
		// determinism. Even the target's own name proves nothing then: bytes.
		{"the target's collation, not established for the rows", src(ir.Text{Collation: "ci"}), ciTarget, "C", false},
		{"another named collation, not established for the rows", src(ir.Text{Collation: "ci_ai"}), ciTarget, "C", false},
		{"not established, on a deterministic key: nothing to guard", src(ir.Text{Collation: "ci"}), map[string]targetKeyCollation{"k": {name: "", deterministic: true}}, "", false},
		// Review LOW: a MySQL-family source on a nondeterministic target key
		// was skipped silently; Postgres has no collation that compares as
		// it does, so the guard refuses instead.
		{"a MySQL-family source on a nondeterministic key: refused", src(ir.Varchar{Charset: "utf8mb4", Collation: "utf8mb4_bin"}), ciTarget, "", true},
		{"a MySQL-family source on a deterministic key: nothing to guard", src(ir.Varchar{Charset: "utf8mb4", Collation: "utf8mb4_bin"}), map[string]targetKeyCollation{"k": {name: "", deterministic: true}}, "", false},
		{"a source collation the stream did not carry", src(ir.Varchar{Length: 20}), ciTarget, "", false},
		{"no source shape", nil, ciTarget, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, unexpressible := planPGKeyCollationGuard(tc.source, []string{"k"}, tc.target)
			got := ""
			if p != nil {
				got = p.compare["k"]
			}
			if got != tc.want {
				t.Fatalf("compare under %q; want %q", got, tc.want)
			}
			if (len(unexpressible) > 0) != tc.refuse {
				t.Fatalf("unexpressible = %v; want refused=%v", unexpressible, tc.refuse)
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
