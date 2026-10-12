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
	pgUnknown := ir.Varchar{Length: 20, Collation: "C"}
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
		{"postgres named, determinism unknown (a pre-v0.159.0 manifest), primary key", pgUnknown, []string{"k"}, nil, true, true},
		{"postgres named, determinism unknown, unique beside a primary key", pgUnknown, []string{"id"}, [][]string{{"k"}}, true, true},
		{"_bin, primary key", bin, []string{"k"}, nil, true, true},
		{"_bin, no key on the column", bin, []string{"id"}, nil, false, false},
		{"agreeing collations", ci, []string{"k"}, nil, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src, tgt := kcgSource(tc.source), kcgCols(ci)
			compareAs := FlavorVanilla.keyGuardCollations()
			if got := planKeyCollationGuard(src, tgt, targetUniqueKeys{pk: tc.pk, uniques: tc.uniques}, compareAs) != nil; got != tc.applier {
				t.Errorf("applier armed = %v; want %v", got, tc.applier)
			}
			if got := planCopyKeyCollationGuard(src, tgt, targetUniqueKeys{pk: tc.pk, uniques: tc.uniques}, compareAs) != nil; got != tc.copier {
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
		plan := planCopyKeyCollationGuard(tbl, target, targetUniqueKeys{pk: []string{"k"}, uniques: nil}, nil)
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
	if !strings.Contains(err.Error(), "different rows of the source") {
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

// TestCopyKeyCollationMergeRemedy_NamesTheCollationTheKeyNeeds pins review
// 2026-10-12 finding 2 over plans the copy guard actually builds: the copy's
// remedy names, per armed key column, the collation the target key needs —
// a binary collation for a byte-armed Postgres key (whose own collation
// MySQL cannot declare), the NO PAD uca1400 form for a MySQL 8 key on
// MariaDB (whose 0900 name MariaDB 10.11 lacks), the source's own for a
// same-flavor one — and never "as the source declares it". A byte-armed key
// is told to resume, not to recreate the table (sluice recreates it under
// the target default, which equates the values).
func TestCopyKeyCollationMergeRemedy_NamesTheCollationTheKeyNeeds(t *testing.T) {
	varchar := func(cs, coll string) ir.Type { return ir.Varchar{Length: 20, Charset: cs, Collation: coll} }
	target := func(k ir.Type) map[string]*ir.Column {
		return map[string]*ir.Column{"id": {Name: "id", Type: ir.Integer{Width: 32}}, "k": {Name: "k", Type: k}}
	}
	source := func(k ir.Type) *ir.Table {
		return &ir.Table{Name: "kd", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}, {Name: "k", Type: k}}}
	}
	mysqlCI := target(varchar("utf8mb4", "utf8mb4_0900_ai_ci"))
	cases := []struct {
		name      string
		plan      *keyCollationPlan
		want      []string
		forbidden []string
	}{
		{
			"postgres deterministic key (bytes)",
			planCopyKeyCollationGuard(source(ir.Varchar{Length: 20, Collation: "C", Determinism: ir.CollationDeterministic}), mysqlCI, targetUniqueKeys{pk: []string{"k"}, uniques: nil}, mysqlKeyGuardCollations),
			[]string{"`k` a binary collation (for example COLLATE utf8mb4_bin)", "resume the copy", "rather than recreating the table"},
			[]string{"as the source declares", "the source's collation", "COLLATE C", "re-copy the table from scratch"},
		},
		{
			"postgres nondeterministic key (bytes)",
			planCopyKeyCollationGuard(source(ir.Varchar{Length: 20, Collation: "l2", Determinism: ir.CollationNonDeterministic}), mysqlCI, targetUniqueKeys{pk: []string{"k"}, uniques: nil}, mysqlKeyGuardCollations),
			// Review #2 finding 4: the pair may be one the source equates (a stale
			// row), where a re-copy from scratch converges and a resume repeats.
			[]string{"`k` a binary collation", "re-copy the table from scratch", "resuming repeats this refusal", "If the source keeps them apart"},
			[]string{"as the source declares", "the source's collation", "COLLATE l2", "different rows of the source"},
		},
		{
			"postgres default key (bytes)",
			planCopyKeyCollationGuard(source(ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic}), mysqlCI, targetUniqueKeys{pk: []string{"id"}, uniques: [][]string{{"k"}}}, mysqlKeyGuardCollations),
			[]string{"`k` a binary collation", "resume the copy"},
			[]string{"as the source declares", "the source's collation"},
		},
		{
			"mysql 8 to mariadb (NO PAD uca1400)",
			planCopyKeyCollationGuard(source(varchar("utf8mb4", "utf8mb4_0900_ai_ci")), target(varchar("utf8mb4", "utf8mb4_uca1400_ai_ci")), targetUniqueKeys{pk: []string{"k"}, uniques: nil}, mariadbKeyGuardCollations),
			[]string{"`k` CHARACTER SET utf8mb4 COLLATE utf8mb4_uca1400_nopad_ai_ci", "run the copy again"},
			[]string{"utf8mb4_0900_ai_ci", "as the source declares", "resume the copy"},
		},
		{
			"mysql _bin to mysql _ci (plain)",
			planCopyKeyCollationGuard(source(varchar("utf8mb4", "utf8mb4_bin")), target(varchar("utf8mb4", "utf8mb4_general_ci")), targetUniqueKeys{pk: []string{"k"}, uniques: nil}, mysqlKeyGuardCollations),
			[]string{"`k` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", "run the copy again"},
			[]string{"as the source declares", "resume the copy"},
		},
	}
	for _, tc := range cases {
		if tc.plan == nil {
			t.Fatalf("%s: the copy guard armed nothing — the cell exercises no refusal", tc.name)
		}
		got := copyKeyCollationMergeRemedy(tc.plan, "s", "kd")
		for _, w := range tc.want {
			if !strings.Contains(got, w) {
				t.Errorf("%s: remedy lacks %q:\n%s", tc.name, w, got)
			}
		}
		for _, f := range tc.forbidden {
			if strings.Contains(got, f) {
				t.Errorf("%s: remedy says %q:\n%s", tc.name, f, got)
			}
		}
		// The applier's remedy names the same collations for the same plan.
		need, _ := keyCollationNeeds(tc.plan)
		if !strings.Contains(keyCollationMergeRemedy(tc.plan, "s", "kd"), need) {
			t.Errorf("%s: the applier's remedy names other collations than %q", tc.name, need)
		}
	}
}
