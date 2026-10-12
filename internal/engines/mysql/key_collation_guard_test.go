// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func kcgCols(kt ir.Type) map[string]*ir.Column {
	return map[string]*ir.Column{
		"id": {Name: "id", Type: ir.Integer{Width: 32}},
		"k":  {Name: "k", Type: kt},
		"d":  {Name: "d", Type: ir.Integer{Width: 32}},
	}
}

func kcgSource(kt ir.Type) *ir.Table {
	return &ir.Table{Name: "kd", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}}, {Name: "k", Type: kt}, {Name: "d", Type: ir.Integer{Width: 32}},
	}}
}

// TestPlanKeyCollationGuard is the arming truth table (operator's choice
// (b): armed by the SOURCE's collation). Expected values from the design.
func TestPlanKeyCollationGuard(t *testing.T) {
	ci := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"}
	bin := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_bin"}
	cases := []struct {
		name        string
		source      *ir.Table
		target      map[string]*ir.Column
		pk          []string
		uniques     [][]string
		wantArmed   bool
		wantBytes   bool
		wantKeyLens []int
		remap       map[string]string
		wantColl    string
	}{
		// The cold start remaps a MySQL 8 collation onto MariaDB's
		// equivalent: no change, no guard, and no probe for a collation the
		// target does not have.
		{
			"cross-flavor remap, unchanged", kcgSource(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}),
			kcgCols(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_uca1400_ai_ci"}),
			[]string{"k"},
			nil, false, false, nil,
			mariadbTargetCollations, "",
		},
		{
			"cross-flavor remap, changed: compared under the target's name", kcgSource(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_bin"}),
			kcgCols(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_uca1400_ai_ci"}),
			[]string{"k"},
			nil, true, false,
			[]int{1},
			mariadbTargetCollations, "utf8mb4_nopad_bin",
		},
		{"no source shape", nil, kcgCols(ci), []string{"k"}, nil, false, false, nil, nil, ""},
		{"source and target agree: no guard, no cost", kcgSource(ci), kcgCols(ci), []string{"k"}, nil, false, false, nil, nil, ""},
		{"same collation in another case", kcgSource(ir.Varchar{Length: 20, Charset: "UTF8MB4", Collation: "UTF8MB4_GENERAL_CI"}), kcgCols(ci), []string{"k"}, nil, false, false, nil, nil, ""},
		{"source _bin, target _ci, primary key", kcgSource(bin), kcgCols(ci), []string{"k"}, nil, true, false, []int{1}, nil, ""},
		{"source _bin, target _ci, unique key beside a primary key", kcgSource(bin), kcgCols(ci), []string{"id"}, [][]string{{"k"}}, true, false, []int{1}, nil, ""},
		{"source _bin, target _ci, no key on k", kcgSource(bin), kcgCols(ci), []string{"id"}, nil, false, false, nil, nil, ""},
		{"source collation unknown (VStream)", kcgSource(ir.Varchar{Length: 20}), kcgCols(ci), []string{"k"}, nil, false, false, nil, nil, ""},
		{"Postgres deterministic source", kcgSource(ir.Text{Size: ir.TextLong, Collation: "C", Determinism: ir.CollationDeterministic}), kcgCols(ci), []string{"k"}, nil, true, true, []int{1}, nil, ""},
		{"Postgres nondeterministic source", kcgSource(ir.Text{Size: ir.TextLong, Collation: "ci", Determinism: ir.CollationNonDeterministic}), kcgCols(ci), []string{"k"}, nil, false, false, nil, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := planKeyCollationGuard(tc.source, tc.target, tc.pk, tc.uniques, tc.remap)
			if (p != nil) != tc.wantArmed {
				t.Fatalf("armed = %v; want %v (%+v)", p != nil, tc.wantArmed, p)
			}
			if p == nil {
				return
			}
			if p.armed["k"].bytes != tc.wantBytes {
				t.Fatalf("bytes = %v; want %v", p.armed["k"].bytes, tc.wantBytes)
			}
			if len(p.keys) != len(tc.wantKeyLens) {
				t.Fatalf("keys = %v; want %d", p.keys, len(tc.wantKeyLens))
			}
			if tc.wantColl != "" && p.armed["k"].collation != tc.wantColl {
				t.Fatalf("compared under %q; want %q", p.armed["k"].collation, tc.wantColl)
			}
		})
	}
}

// TestKeyCollationGuard_RendersFirstAndOnce pins the clause shape the
// integration matrix measured: the guard is the FIRST assignment (MySQL
// evaluates the list against the row as updated so far), it replaces its
// column's plain assignment, and the raise carries the marker.
func TestKeyCollationGuard_RendersFirstAndOnce(t *testing.T) {
	p := planKeyCollationGuard(kcgSource(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_bin"}),
		kcgCols(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"}), []string{"id"}, [][]string{{"k"}}, nil)
	for _, upsert := range []upsertSpelling{upsertRowAlias, upsertValuesFunc} {
		assign, hang, ok := p.render("s", "kd", []string{"d", "id", "k"}, upsert)
		if !ok || hang != "d" {
			t.Fatalf("render: ok=%v hang=%q", ok, hang)
		}
		clause := onDuplicateKeyUpdateClause([]string{"d", "id", "k"}, []string{"id"}, upsert, assign, hang)
		body := strings.TrimPrefix(clause, upsert.clauseOpen())
		if !strings.HasPrefix(body, assign) {
			t.Fatalf("the guard is not the first assignment: %s", clause)
		}
		if strings.Count(clause, "`d` =") != 1 {
			t.Fatalf("the hang column is assigned twice: %s", clause)
		}
		for _, want := range []string{"KEY-COLLATION-MERGE", "18446744073709551615", "COLLATE utf8mb4_bin", "`s`.`kd`.`k`"} {
			if !strings.Contains(assign, want) {
				t.Errorf("guard %q lacks %q", assign, want)
			}
		}
	}
	if _, _, ok := p.render("s", "kd", []string{"d", "id"}, upsertRowAlias); ok {
		t.Fatal("a guard whose key column is not in the insert rendered")
	}
}
