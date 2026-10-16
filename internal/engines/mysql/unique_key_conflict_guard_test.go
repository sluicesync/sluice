// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// TestConflictIdentityColumns pins the identity rule: the primary key when
// the insert names every column of it (case-insensitively, in the insert's
// spelling); else the union of every unique key's columns when there are
// at least two; else unarmed (nil) — a single unique key on a table without
// a primary key cannot tell a merge from the newer-image re-send it must
// absorb.
func TestConflictIdentityColumns(t *testing.T) {
	cases := []struct {
		name       string
		pk, cols   []string
		uniqueCols [][]string
		want       []string
	}{
		{"primary key supplied", []string{"id"}, []string{"id", "k", "v"}, [][]string{{"k"}}, []string{"id"}},
		{"composite, case-folded", []string{"A", "b"}, []string{"b", "a", "v"}, [][]string{{"v"}}, []string{"a", "b"}},
		{"no primary key, two unique keys", nil, []string{"id", "k", "v"}, [][]string{{"ID"}, {"k"}}, []string{"id", "k"}},
		{"no primary key, one unique key", nil, []string{"id", "k", "v"}, [][]string{{"k"}}, nil},
		{"surrogate the row does not supply, two keys", []string{"sid"}, []string{"id", "k"}, [][]string{{"id"}, {"k"}}, []string{"id", "k"}},
		{"a key column the insert lacks", nil, []string{"id", "k"}, [][]string{{"id"}, {"g"}}, nil},
	}
	for _, tc := range cases {
		if got := conflictIdentityColumns(tc.pk, tc.cols, tc.uniqueCols); !slices.Equal(got, tc.want) {
			t.Errorf("%s: %v; want %v", tc.name, got, tc.want)
		}
	}
}

// TestRenderUpsertGuard_Shapes pins the composed assignment: unarmed is
// empty; identity-only checks every identity column against the existing row
// and raises in a CONDITION whose arms are the column's own incoming value;
// with the key-collation guard armed too, the identity check is that
// guard's PASSING value, so a collation merge is reported as one; a
// key-collation guard alone renders exactly what it did before.
func TestRenderUpsertGuard_Shapes(t *testing.T) {
	cols := []string{"id", "k", "v"}
	if a, _, ok := renderUpsertGuard("`s`.`kd`", cols, []string{"id"}, upsertRowAlias, nil, nil, nil); a != "" || !ok {
		t.Fatalf("unarmed rendered %q", a)
	}
	coll := planKeyCollationGuard(kcgSource(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_bin"}),
		kcgCols(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"}),
		targetUniqueKeys{pk: []string{"id"}, uniques: [][]string{{"k"}}}, nil)
	if coll == nil {
		t.Fatal("the fixture's key-collation guard is unarmed")
	}
	kcCols := []string{"d", "id", "k"}
	for _, upsert := range []upsertSpelling{upsertRowAlias, upsertValuesFunc} {
		assign, hang, _ := renderUpsertGuard("`s`.`kd`", cols, []string{"id"}, upsert, []string{"id"}, nil, nil)
		newK := upsert.newRowRef("`k`")
		want := "`k` = IF((`s`.`kd`.`id` <=> " + upsert.newRowRef("`id`") + "), " + newK +
			", IF((18446744073709551615 + OCTET_LENGTH(CONCAT('UNIQUE-KEY-CONFLICT-OTHER-ROW', ISNULL(" + newK + ")))) = 0, " +
			newK + ", " + newK + "))"
		if assign != want || hang != "k" {
			t.Errorf("identity only:\n got %s (hang %s)\nwant %s", assign, hang, want)
		}
		// Keyless: every column is the identity, and the hang is the first.
		assign, hang, _ = renderUpsertGuard("`s`.`kd`", cols, nil, upsert, cols, nil, nil)
		for _, c := range cols {
			if !strings.Contains(assign, "`s`.`kd`.`"+c+"` <=> "+upsert.newRowRef("`"+c+"`")) {
				t.Errorf("keyless: %s does not compare %s", assign, c)
			}
		}
		if hang != "id" {
			t.Errorf("keyless hang %q; want id", hang)
		}
		// Key-collation alone: exactly the assignment it rendered before.
		before, beforeHang, _ := coll.render("s", "kd", kcCols, upsert)
		assign, hang, _ = renderUpsertGuard("`s`.`kd`", kcCols, []string{"id"}, upsert, nil, nil, coll)
		if assign != before || hang != beforeHang {
			t.Errorf("key-collation alone changed:\n got %s\nwant %s", assign, before)
		}
		// Composed: the key-collation raise is the OUTER check, and the
		// identity check replaces its passing value.
		assign, hang, _ = renderUpsertGuard("`s`.`kd`", kcCols, []string{"id"}, upsert, []string{"id"}, nil, coll)
		kc := strings.Index(assign, "KEY-COLLATION-MERGE")
		uk := strings.Index(assign, "UNIQUE-KEY-CONFLICT-OTHER-ROW")
		if hang != beforeHang || kc < 0 || uk < 0 || uk > kc || !strings.HasPrefix(assign, "`d` = IF(") {
			t.Errorf("composed: %s (hang %s)", assign, hang)
		}
		if !strings.Contains(assign, ", IF((`s`.`kd`.`id` <=> "+upsert.newRowRef("`id`")+"), ") {
			t.Errorf("composed: the identity check is not the key-collation guard's passing value: %s", assign)
		}
	}
	if _, _, ok := renderUpsertGuard("`s`.`kd`", []string{"d", "id"}, []string{"id"}, upsertRowAlias, []string{"id"}, nil, coll); ok {
		t.Error("a key-collation guard whose key column is not in the insert rendered")
	}
}

// TestBuildMultiRowInsertSQL_ConflictIdentityGuard pins the applier's
// statement: armed (the target has a unique key besides its primary key),
// the guard is the FIRST assignment and replaces its column's plain one;
// unarmed (nil), the statement is byte-identical to the unguarded builder.
func TestBuildMultiRowInsertSQL_ConflictIdentityGuard(t *testing.T) {
	rows := []ir.Row{{"id": int64(1), "k": "a", "v": "one"}, {"id": int64(99), "k": "a", "v": "ninetynine"}}
	pk := []string{"id"}
	plain, _, err := buildMultiRowInsertSQL("s", "kd", rows, pk, nil, upsertRowAlias, nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plain, uniqueKeyConflictMarker) {
		t.Fatalf("an unguarded statement carries the guard: %s", plain)
	}
	g := newApplyUpsertGuard(nil, targetUniqueKeys{pk: pk, uniques: [][]string{{"k"}}, uniqueNames: []string{"kd_k"}})
	if g == nil {
		t.Fatal("a table with a unique key besides its primary key is unarmed")
	}
	got, _, err := buildMultiRowInsertSQL("s", "kd", rows, pk, nil, upsertRowAlias, g)
	if err != nil {
		t.Fatal(err)
	}
	_, clause, ok := strings.Cut(got, upsertRowAlias.clauseOpen())
	if !ok || !strings.HasPrefix(clause, "`k` = IF((`s`.`kd`.`id` <=> new.`id`)") {
		t.Fatalf("the identity guard is not the first assignment: %s", got)
	}
	if strings.Count(clause, "`k` =") != 1 || !strings.Contains(clause, "`v` = new.`v`") {
		t.Fatalf("the hang column is assigned twice or another column lost its assignment: %s", clause)
	}
	if newApplyUpsertGuard(nil, targetUniqueKeys{pk: pk}) != nil {
		t.Fatal("a table whose only unique key is its primary key is armed")
	}
}

// TestUpsertGuardRefusal_Attribution pins which error the guard owns: a 1690
// carrying its marker on a statement that carried the identity guard, and
// nothing else.
func TestUpsertGuardRefusal_Attribution(t *testing.T) {
	armed := newApplyUpsertGuard(nil, targetUniqueKeys{pk: []string{"id"}, uniques: [][]string{{"k"}}, uniqueNames: []string{"kd_k"}})
	raise := &mysql.MySQLError{Number: 1690, Message: "BIGINT UNSIGNED value is out of range in '(18446744073709551615 + octet_length(concat('UNIQUE-KEY-CONFLICT-OTHER-ROW',isnull(`new`.`k`))))'"}
	cases := []struct {
		name string
		err  error
		g    *applyUpsertGuard
		want bool
	}{
		{"1690 with the marker", raise, armed, true},
		{"wrapped", fmt.Errorf("vtgate: %w", raise), armed, true},
		{"1690 without the marker", &mysql.MySQLError{Number: 1690, Message: "BIGINT value is out of range"}, armed, false},
		{"the marker on a 1062", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'UNIQUE-KEY-CONFLICT-OTHER-ROW'"}, armed, false},
		{"an unguarded statement", raise, nil, false},
	}
	for _, tc := range cases {
		err := upsertGuardRefusal(tc.err, tc.g, "s", "kd")
		if got := errors.Is(err, ir.ErrUniqueKeyConflictOtherRow); got != tc.want {
			t.Errorf("%s: attributed = %v; want %v", tc.name, got, tc.want)
		}
		if tc.want {
			if !ir.IsTerminal(err) {
				t.Errorf("%s: the refusal is not terminal", tc.name)
			}
			for _, want := range []string{"kd_k (k)", "DROP INDEX `kd_k`", "--restart-from-scratch", "`s`.`kd`"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s: the refusal does not name %q: %v", tc.name, want, err)
				}
			}
		}
	}
	// A key-collation raise on a statement carrying both guards is still the
	// key-collation guard's.
	both := &applyUpsertGuard{coll: &keyCollationPlan{pk: []string{"id"}}, others: armed.others, keyNames: armed.keyNames}
	kc := &mysql.MySQLError{Number: 1690, Message: "out of range in 'KEY-COLLATION-MERGE'"}
	if err := upsertGuardRefusal(kc, both, "s", "kd"); !errors.Is(err, ir.ErrKeyCollationMerge) || errors.Is(err, ir.ErrUniqueKeyConflictOtherRow) {
		t.Errorf("a key-collation raise was attributed as %v", err)
	}
}

// TestUndeclaredUniqueKeys pins the copy writer's arming: a plain target key
// is declared by a source unique index over the same columns and prefix
// lengths (its name does not count, so a key redefined under the old name
// arms); a functional key is declared only by name; anything else arms.
func TestApplyUniqueKeyConflictRemedy_TwoKeysSaysCheckEach(t *testing.T) {
	one := applyUniqueKeyConflictRemedy([]string{"kd_k"}, "s", "kd")
	if !strings.Contains(one, "DROP INDEX `kd_k`") || strings.Contains(one, "EACH") {
		t.Errorf("one key: %s", one)
	}
	two := applyUniqueKeyConflictRemedy([]string{"kd_k", "kd_zid"}, "s", "kd")
	if !strings.Contains(two, "check EACH against the source") || !strings.Contains(two, "DROP INDEX <key>") ||
		!strings.Contains(two, "for `sluice sync start`, restart with `--restart-from-scratch` and the same `--stream-id`") ||
		!strings.Contains(two, "for `sluice sync from-backup run`, which has no `--restart-from-scratch`, re-run it with `--reset-target-data --yes`") ||
		!strings.Contains(two, "`sluice slot drop sluice_<--slot-name> --source-driver=postgres --source <DSN> --yes`") {
		t.Errorf("two keys: %s", two)
	}
}

func TestUndeclaredUniqueKeys(t *testing.T) {
	src := &ir.Table{
		Name:       "kd",
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{
			{Name: "kd_k", Unique: true, Columns: []ir.IndexColumn{{Column: "k"}}},
			{Name: "src_ab", Unique: true, Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}},
			{Name: "plain_v", Columns: []ir.IndexColumn{{Column: "v"}}},
			{Name: "kd_p", Unique: true, Columns: []ir.IndexColumn{{Column: "p", Length: 5}}},
			{Name: "fn_ok", Unique: true, Columns: []ir.IndexColumn{{Expression: "lower(k)"}}},
		},
	}
	keys := targetUniqueKeys{
		pk:          []string{"id"},
		uniques:     [][]string{{"k"}, {"A", "b"}, {"v"}, {"w"}, {"k"}, {"k"}, {"p"}, {"p"}, {"x"}},
		uniqueSubs:  [][]int{{0}, {0, 0}, {0}, {0}, {0}, {0}, {5}, {9}, {0}},
		uniqueNames: []string{"KD_K", "tgt_ab", "tgt_v", "tgt_w", "fn_k", "fn_ok", "p5", "p9", "kd_k"},
		uniqueFns:   [][]*functionalKeyPart{nil, nil, nil, nil, {{index: "fn_k"}}, {{index: "fn_ok"}}, nil, nil, nil},
	}
	got, names, cols := undeclaredUniqueKeys(src, keys)
	if want := []string{"tgt_v", "tgt_w", "fn_k", "p9", "kd_k"}; !slices.Equal(names, want) {
		t.Fatalf("undeclared %v (%v); want %v", names, got, want)
	}
	if got[0] != "tgt_v (v)" {
		t.Errorf("rendered %q", got[0])
	}
	if cols[2] != nil || len(cols[0]) != 1 || cols[0][0] != (scopePart{col: "v"}) || cols[3][0] != (scopePart{col: "p", sub: 9}) {
		t.Errorf("columns %v; want v first, nil for the functional key, p with its prefix 9", cols)
	}
	if copyGuardScope(cols) != nil {
		t.Error("a functional undeclared key must widen the scope to every conflict")
	}
	if s := copyGuardScope([][]scopePart{{{col: "v"}}, {{col: "w"}}}); len(s) != 2 {
		t.Errorf("plain undeclared keys: scope %v", s)
	}
}

// TestBuildBatchUpsert_ConflictIdentityGuard pins the copy writer's
// statement with only the identity guard armed: the guard is the first
// assignment, keyed on the copy's own key, and the zero guard is unchanged.
func TestBuildBatchUpsert_ConflictIdentityGuard(t *testing.T) {
	table := &ir.Table{Name: "kd", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{}}, {Name: "k", Type: ir.Text{}}, {Name: "v", Type: ir.Text{}}}}
	assign, hang, _ := renderUpsertGuard(quoteIdent("kd"), []string{"id", "k", "v"}, []string{"id"}, upsertRowAlias, []string{"id"}, nil, nil)
	g := copyKeyCollationGuard{assign: assign, hang: hang, schema: "s", undeclared: []string{"kd_k (k)"}, undeclaredNames: []string{"kd_k"}}
	got := buildBatchUpsert(table, 1, []string{"id"}, upsertRowAlias, g)
	_, clause, _ := strings.Cut(got, upsertRowAlias.clauseOpen())
	if !strings.HasPrefix(clause, "`k` = IF((`kd`.`id` <=> new.`id`)") || strings.Count(clause, "`k` =") != 1 {
		t.Fatalf("copy guard misplaced: %s", got)
	}
	raise := &mysql.MySQLError{Number: 1690, Message: "out of range in 'UNIQUE-KEY-CONFLICT-OTHER-ROW'"}
	if err := copyUpsertGuardRefusal(raise, g, "kd"); !errors.Is(err, ir.ErrUniqueKeyConflictOtherRow) || !strings.Contains(err.Error(), "DROP INDEX `kd_k`") {
		t.Errorf("copy refusal: %v", err)
	}
	if err := copyUpsertGuardRefusal(raise, copyKeyCollationGuard{}, "kd"); errors.Is(err, ir.ErrUniqueKeyConflictOtherRow) {
		t.Error("an unguarded copy statement's 1690 was attributed to the guard")
	}
}
