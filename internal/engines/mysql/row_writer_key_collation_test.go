// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// TestPlanCopyKeyCollationGuard_ArmsThePostgresDefaultInsideThePrimaryKey
// pins the copy writer's one departure from the applier's arming rule
// (KEY-COLLATION-ROWWRITER): a Postgres default-collation source key on a
// `_ci` target is armed inside the primary key, and with no primary key,
// because a copy re-sends only rows of a snapshot, never a re-delivered
// change of a key. Every other cell arms exactly as the applier does.
func TestPlanCopyKeyCollationGuard_ArmsThePostgresDefaultInsideThePrimaryKey(t *testing.T) {
	ci := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"}
	pgDefault := ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic}
	pgNondet := ir.Varchar{Length: 20, Collation: "ci_level2", Determinism: ir.CollationNonDeterministic}
	bin := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_bin"}
	cases := []struct {
		name            string
		source          ir.Type
		pk              []string
		uniques         [][]string
		applier, copier bool
	}{
		{"postgres default, primary key", pgDefault, []string{"k"}, nil, false, true},
		{"postgres default, unique with no primary key", pgDefault, nil, [][]string{{"k"}}, false, true},
		{"postgres default, unique beside a primary key", pgDefault, []string{"id"}, [][]string{{"k"}}, true, true},
		{"postgres nondeterministic, primary key", pgNondet, []string{"k"}, nil, false, true},
		{"postgres nondeterministic, unique with no primary key", pgNondet, nil, [][]string{{"k"}}, false, true},
		{"postgres nondeterministic, unique beside a primary key", pgNondet, []string{"id"}, [][]string{{"k"}}, true, true},
		{"_bin, primary key", bin, []string{"k"}, nil, true, true},
		{"_bin, no key on the column", bin, []string{"id"}, nil, false, false},
		{"agreeing collations", ci, []string{"k"}, nil, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src, tgt := kcgSource(tc.source), kcgCols(ci)
			compareAs := FlavorVanilla.keyGuardCollations()
			if got := planKeyCollationGuard(src, tgt, tc.pk, tc.uniques, compareAs) != nil; got != tc.applier {
				t.Errorf("applier armed = %v; want %v", got, tc.applier)
			}
			if got := planCopyKeyCollationGuard(src, tgt, tc.pk, tc.uniques, compareAs) != nil; got != tc.copier {
				t.Errorf("copy armed = %v; want %v", got, tc.copier)
			}
		})
	}
}

// TestBuildBatchUpsert_KeyCollationGuard pins the copy statement's shape in
// both upsert spellings: unarmed is byte-identical to the unguarded
// statement; armed puts the guard FIRST, drops the plain assignment of the
// column it assigns, and references the existing row through the
// unqualified table the INSERT names; a table whose every column is the key
// carries the guard in place of the no-op self-assignment.
func TestBuildBatchUpsert_KeyCollationGuard(t *testing.T) {
	bin := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_bin"}
	ci := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"}
	table := &ir.Table{
		Name: "kd",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "k", Type: bin},
			{Name: "d", Type: ir.Integer{Width: 32}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}},
	}
	allKey := &ir.Table{Name: "ak", Columns: []*ir.Column{{Name: "k", Type: bin}}, PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}}}
	guardFor := func(t *testing.T, tbl *ir.Table, upsert upsertSpelling) copyKeyCollationGuard {
		t.Helper()
		target := map[string]*ir.Column{}
		names := make([]string, 0, len(tbl.Columns))
		for _, c := range tbl.Columns {
			typ := c.Type
			if c.Name == "k" {
				typ = ci
			}
			target[c.Name] = &ir.Column{Name: c.Name, Type: typ}
			names = append(names, c.Name)
		}
		plan := planCopyKeyCollationGuard(tbl, target, []string{"k"}, nil, nil)
		if plan == nil {
			t.Fatal("not armed")
		}
		assign, hang, ok := plan.renderFor(quoteIdent(tbl.Name), names, upsert)
		if !ok {
			t.Fatal("render refused")
		}
		return copyKeyCollationGuard{plan: plan, schema: "s", assign: assign, hang: hang}
	}
	for _, upsert := range []upsertSpelling{upsertRowAlias, upsertValuesFunc} {
		plain := buildBatchUpsert(table, 2, []string{"k"}, upsert, copyKeyCollationGuard{})
		if strings.Contains(plain, keyCollationMergeMarker) {
			t.Fatalf("an unarmed table carries the guard: %s", plain)
		}
		g := guardFor(t, table, upsert)
		got := buildBatchUpsert(table, 2, []string{"k"}, upsert, g)
		head, body, found := strings.Cut(got, upsert.clauseOpen())
		plainHead, _, _ := strings.Cut(plain, upsert.clauseOpen())
		if !found || !strings.HasPrefix(body, g.assign) {
			t.Fatalf("the guard is not the first assignment: %s", got)
		}
		if strings.Count(got, quoteIdent(g.hang)+" =") != 1 {
			t.Fatalf("column %s is assigned twice: %s", g.hang, got)
		}
		if !strings.Contains(g.assign, "`kd`.`k`") || strings.Contains(g.assign, "`s`.`kd`") {
			t.Fatalf("the guard does not reference the existing row through the INSERT's own table: %s", g.assign)
		}
		if head != plainHead || !strings.HasSuffix(got, ", `d` = "+upsert.newRowRef("`d`")) {
			t.Fatalf("the guard changed the statement outside its own assignment:\n%s\n%s", got, plain)
		}

		ga := guardFor(t, allKey, upsert)
		gotAll := buildBatchUpsert(allKey, 1, []string{"k"}, upsert, ga)
		if !strings.HasSuffix(gotAll, upsert.clauseOpen()+ga.assign) {
			t.Fatalf("an all-key table's update list is not just the guard: %s", gotAll)
		}
	}
}

// TestCopyKeyCollationMergeRefusal_NeedsTheGuardAndTheMarker pins the
// attribution: only a 1690 carrying the marker, from a statement that
// carried the guard, is a merge.
func TestCopyKeyCollationMergeRefusal_NeedsTheGuardAndTheMarker(t *testing.T) {
	raise := &mysql.MySQLError{Number: 1690, Message: "BIGINT UNSIGNED value is out of range in '(18446744073709551615 + octet_length(concat('KEY-COLLATION-MERGE',new.`k`)))'"}
	armed := copyKeyCollationGuard{plan: &keyCollationPlan{}, schema: "s"}
	err := copyKeyCollationMergeRefusal(raise, armed, "kd")
	var kce *ir.KeyCollationMergeError
	if !errors.As(err, &kce) || !errors.Is(err, ir.ErrKeyCollationMerge) {
		t.Fatalf("the guard's raise was not attributed: %v", err)
	}
	if !strings.Contains(err.Error(), "run the copy again") {
		t.Errorf("the remedy is not the copy's: %v", err)
	}
	if errors.Is(copyKeyCollationMergeRefusal(raise, copyKeyCollationGuard{}, "kd"), ir.ErrKeyCollationMerge) {
		t.Error("a statement without the guard was attributed")
	}
	other := &mysql.MySQLError{Number: 1690, Message: "BIGINT value is out of range"}
	if errors.Is(copyKeyCollationMergeRefusal(other, armed, "kd"), ir.ErrKeyCollationMerge) {
		t.Error("an ordinary overflow was attributed")
	}
}
