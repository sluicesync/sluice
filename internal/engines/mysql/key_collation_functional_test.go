// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestMySQLCollationName pins the F5 routing test: MySQL and MariaDB
// collation names are recognised with or without a charset beside them;
// Postgres and ICU names are not.
func TestMySQLCollationName(t *testing.T) {
	for name, want := range map[string]bool{
		"utf8mb4_0900_ai_ci": true, "UTF8MB4_BIN": true, "latin1_swedish_ci": true, "utf8_general_ci": true,
		"utf8mb3_general_ci": true, "utf8mb4_uca1400_ai_ci": true, "binary": true,
		"C": false, "POSIX": false, "en_US.utf8": false, "und-x-icu": false, "default": false, "ucs_basic": false,
	} {
		if got := mysqlCollationName(name); got != want {
			t.Errorf("mysqlCollationName(%q) = %v; want %v", name, got, want)
		}
	}
}

// TestParseFunctionalKeyPart is the grammar, both ways: every form the
// guard reads through renders to the expression it read, and every form it
// cannot read is refused rather than half-read. The real-server matrix
// (TestUniqueKeyColumns_FunctionalPartsOnTheRealServer) compares the
// rendering with the server's own values.
func TestParseFunctionalKeyPart(t *testing.T) {
	for _, tc := range []struct{ expr, column, render string }{
		{"lower(`k`)", "k", "LOWER(x)"},
		{"upper(`k`)", "k", "UPPER(x)"},
		{"lcase(`k`)", "k", "LOWER(x)"},
		{"UCASE( `k` )", "k", "UPPER(x)"},
		{"trim(`k`)", "k", "TRIM(x)"},
		{"ltrim(rtrim(`k`))", "k", "LTRIM(RTRIM(x))"},
		{"lower(cast(`email` as char charset utf8mb4))", "email", "LOWER(CAST(x AS CHAR CHARACTER SET utf8mb4))"},
		{"cast(`k` as char(10) charset latin1)", "k", "CAST(x AS CHAR(10) CHARACTER SET latin1)"},
		{"cast(`k` as char charset binary)", "k", "CAST(x AS CHAR CHARACTER SET binary)"},
		{"(lower(`k`) collate utf8mb4_bin)", "k", "(LOWER(x) COLLATE utf8mb4_bin)"},
		{"(`k` collate utf8mb4_bin)", "k", "(x COLLATE utf8mb4_bin)"},
		{"(lower(cast(`k` as char charset utf8mb4)) collate utf8mb4_bin)", "k", "(LOWER(CAST(x AS CHAR CHARACTER SET utf8mb4)) COLLATE utf8mb4_bin)"},
		{"lower(`we``ird`)", "we`ird", "LOWER(x)"},
	} {
		col, steps, ok := parseFunctionalKeyPart(tc.expr)
		if !ok || col != tc.column {
			t.Errorf("%s: column %q ok %v; want %q", tc.expr, col, ok, tc.column)
			continue
		}
		if got := (&functionalKeyPart{steps: steps}).render("x"); got != tc.render {
			t.Errorf("%s: renders %s; want %s", tc.expr, got, tc.render)
		}
	}
	for _, expr := range []string{
		"",
		"`k`", // a bare column is a plain key part, never a functional one
		"md5(`k`)",
		"concat(`a`,`b`)",
		"lower(`a`,`b`)",
		"trim(both _utf8mb4'x' from `k`)",
		"cast(`k` as signed)",
		"cast(`k` as char(0) charset utf8mb4)",
		"cast(json_unquote(json_extract(`j`,_utf8mb4'$.e')) as char(40) charset utf8mb4)",
		"(`j` * 2)",
		"lower(`k`) ",
		"lower(`k`) x",
		"lower(`k`",
		"lower(`naïve`)", // a non-ASCII name the catalog may have mangled (Bug 288)
		"lower(``)",
		"(`k` collate)",
	} {
		if expr == "`k`" {
			// A bare column parses, but the catalog never reports one as an
			// expression; the case pins only that it is read as the column.
			if col, steps, ok := parseFunctionalKeyPart(expr); !ok || col != "k" || len(steps) != 0 {
				t.Errorf("%q: %q %v %v", expr, col, steps, ok)
			}
			continue
		}
		if expr == "lower(`k`) " {
			if _, _, ok := parseFunctionalKeyPart(expr); !ok {
				t.Errorf("%q: trailing space refused", expr)
			}
			continue
		}
		if col, _, ok := parseFunctionalKeyPart(expr); ok {
			t.Errorf("%q: read through as column %q; want opaque", expr, col)
		}
	}
}

// TestFunctionalKeyPart_TargetEq: the target side compares a readable part
// under the index's own collation, and assumes a match (TRUE) for a part it
// cannot read, whose collation is not known, or that is not a string.
func TestFunctionalKeyPart_TargetEq(t *testing.T) {
	steps := []functionalStep{{kind: "fn", fn: "LOWER"}}
	if got := (&functionalKeyPart{readable: true, steps: steps, known: true, collation: "utf8mb4_0900_ai_ci"}).targetEq("e", "i"); got !=
		"(LOWER(e) COLLATE utf8mb4_0900_ai_ci) <=> (LOWER(i) COLLATE utf8mb4_0900_ai_ci)" {
		t.Errorf("known collation: %s", got)
	}
	for name, f := range map[string]*functionalKeyPart{
		"not a string":      {readable: true, steps: steps, known: true},
		"unknown collation": {readable: true, steps: steps},
		"unreadable":        {known: true, collation: "utf8mb4_0900_ai_ci"},
	} {
		if got := f.targetEq("e", "i"); got != "TRUE" {
			t.Errorf("%s: %s", name, got)
		}
	}
}

// TestFunctionalKeyPart_SourceEq: the bytes of every column the part reads,
// or TRUE for a part that is not a collated string.
func TestFunctionalKeyPart_SourceEq(t *testing.T) {
	b := func(c string) string { return "B(" + c + ")" }
	if got := (&functionalKeyPart{columns: []string{"a", "b"}, known: true, collation: "x"}).sourceEq(b); got != "(B(a) AND B(b))" {
		t.Errorf("two columns: %s", got)
	}
	if got := (&functionalKeyPart{columns: []string{"a"}}).sourceEq(b); got != "(B(a))" {
		t.Errorf("unknown: %s", got)
	}
	if got := (&functionalKeyPart{columns: []string{"a"}, known: true}).sourceEq(b); got != "TRUE" {
		t.Errorf("not a string: %s", got)
	}
	// Third review finding 2: a collated part reading no column is never the
	// same source key.
	for name, f := range map[string]*functionalKeyPart{
		"collated, no column": {known: true, collation: "x"},
		"unknown, no column":  {},
	} {
		if got := f.sourceEq(b); got != "FALSE" {
			t.Errorf("%s: %s; want FALSE", name, got)
		}
	}
	if got := (&functionalKeyPart{known: true}).sourceEq(b); got != "TRUE" {
		t.Errorf("not a string, no column: %s", got)
	}
}

// withHidden gives t the source key `fk` over lower(k) whose hidden column
// the MySQL reader read as hidden ("" adds no key).
func withHidden(t *ir.Table, hidden string) *ir.Table {
	if hidden != "" {
		t.Indexes = append(t.Indexes, &ir.Index{Name: "fk", Unique: true, Columns: []ir.IndexColumn{
			{Expression: "lower(k)", ExpressionDialect: "mysql", HiddenCollation: hidden},
		}})
	}
	return t
}

// TestFunctionalPartArms_EachRoute discriminates each route by which a part
// arms its key, one at a time (third review finding 5).
//
// MySQL-family source (FUNCTIONAL-CAST-HIDDEN-COLLATION, the class fix): the
// source key's comparison is its READ hidden collation, compared with the
// target index's — the fifth review's S1 (MODIFY left the hidden column on
// the old collation) and S2 (a literal under an INT column) are both "read
// and different" — AND the columns it reads must agree with the target's
// (sixth review F1: the value is computed under the target column's
// collation), so an armed or unknown-collation column arms.
//
// Postgres/SQLite source: the column routes (armed column, a declared
// collation other than the index's or none, a missing column; a non-string
// column brings none).
func TestFunctionalPartArms_EachRoute(t *testing.T) {
	ci := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}
	pgDefault := ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic}
	// A Postgres-dialect column named like the index's collation, so only
	// the armed-column route can arm it.
	pgSameName := ir.Varchar{Length: 20, Collation: "utf8mb4_0900_ai_ci", Determinism: ir.CollationDeterministic}
	part := func(known bool, coll string) *functionalKeyPart {
		return &functionalKeyPart{index: "fk", seq: 0, named: []string{"K"}, columns: []string{"k"}, known: known, collation: coll}
	}
	mysqlSrc := func(hidden string, cols ...*ir.Column) *ir.Table {
		return &ir.Table{Name: "kd", Columns: cols, Indexes: []*ir.Index{{
			Name: "FK", Unique: true, Columns: []ir.IndexColumn{{Expression: "lower(k)", ExpressionDialect: "mysql", HiddenCollation: hidden}},
		}}}
	}
	src := func(cols ...*ir.Column) *ir.Table { return &ir.Table{Name: "kd", Columns: cols} }
	k := func(t ir.Type) *ir.Column { return &ir.Column{Name: "k", Type: t} }
	for _, tc := range []struct {
		name   string
		f      *functionalKeyPart
		source *ir.Table
		armed  map[string]armedCollation
		want   bool
	}{
		// MySQL-family route.
		{"mysql: read and equal", part(true, "utf8mb4_0900_ai_ci"), mysqlSrc("utf8mb4_0900_ai_ci", k(ci)), nil, false},
		{"mysql: read and different (S1, S2, a session CAST)", part(true, "utf8mb4_0900_ai_ci"), mysqlSrc("utf8mb4_bin", k(ci)), nil, true},
		{"mysql: hidden collation not read", part(true, "utf8mb4_0900_ai_ci"), mysqlSrc("", k(ci)), nil, true},
		{"mysql: the source has no such key (a CDC projection)", part(true, "utf8mb4_0900_ai_ci"), src(k(ci)), nil, true},
		{
			// Sixth review F1: the key compares as the source's, but the
			// value is computed under the target column's collation.
			"mysql: an armed column arms a read-equal key", part(true, "utf8mb4_0900_ai_ci"),
			mysqlSrc("utf8mb4_0900_ai_ci", k(ci)),
			map[string]armedCollation{"k": {charset: "utf8mb4", collation: "utf8mb4_bin"}},
			true,
		},
		{
			"mysql: source column collation unknown (a --type-override)", part(true, "utf8mb4_0900_ai_ci"),
			mysqlSrc("utf8mb4_0900_ai_ci", k(ir.Varchar{Length: 20})), nil, true,
		},
		{
			"mysql: the source lacks the column", part(true, "utf8mb4_0900_ai_ci"),
			mysqlSrc("utf8mb4_0900_ai_ci", &ir.Column{Name: "other", Type: ci}), nil, true,
		},
		{
			"mysql: read equal over an INT column", part(true, "utf8mb4_0900_ai_ci"),
			mysqlSrc("utf8mb4_0900_ai_ci", k(ir.Integer{Width: 32})), nil, false,
		},
		{
			// Sixth review F5: a MySQL collation carried without its charset
			// (the pre-MYDUMPER-COLLATION-NO-CHARSET shape) is still the MySQL
			// route, where the read-different hidden collation arms. Routed by
			// charset presence it read as Postgres, and the column — named
			// like the index — armed nothing.
			"mysql: a charset-less MySQL collation, read different", part(true, "utf8mb4_0900_ai_ci"),
			mysqlSrc("utf8mb4_bin", k(ir.Varchar{Length: 20, Collation: "utf8mb4_0900_ai_ci"})), nil, true,
		},
		{
			"mysql: a charset-less MariaDB collation, read different", part(true, "utf8mb4_uca1400_ai_ci"),
			mysqlSrc("utf8mb4_bin", k(ir.Varchar{Length: 20, Collation: "utf8mb4_uca1400_ai_ci"})), nil, true,
		},
		// Postgres/SQLite route.
		{"postgres: armed column only", part(true, "utf8mb4_0900_ai_ci"), src(k(pgSameName)), map[string]armedCollation{"k": {bytes: true}}, true},
		{"postgres: control (named like the index, unarmed)", part(true, "utf8mb4_0900_ai_ci"), src(k(pgSameName)), nil, false},
		{"postgres: declares none", part(true, "utf8mb4_0900_ai_ci"), src(k(pgDefault)), nil, true},
		{
			"postgres: the source lacks the column", part(true, "utf8mb4_0900_ai_ci"),
			src(&ir.Column{Name: "other", Type: pgDefault}), nil, true,
		},
		{
			"postgres: only an INT column", part(true, "utf8mb4_0900_ai_ci"),
			src(&ir.Column{Name: "k", Type: ir.Integer{Width: 32}}, &ir.Column{Name: "s", Type: pgDefault}), nil, false,
		},
		// Either route.
		{"no source shape (finding 3)", part(true, "utf8mb4_0900_ai_ci"), nil, nil, true},
		{"index collation unknown", part(false, ""), mysqlSrc("utf8mb4_0900_ai_ci", k(ci)), nil, true},
		{"not a string", part(true, ""), nil, nil, false},
	} {
		if got := functionalPartArms(tc.f, tc.source, tc.armed, mysqlKeyGuardCollations); got != tc.want {
			t.Errorf("%s: arms %v; want %v", tc.name, got, tc.want)
		}
	}
}

// TestPlanKeyCollationGuard_FunctionalPartNoSourceShape is finding 3 at the
// planner: with no source shape (a live trigger-CDC stream), a collated
// functional part still arms its key on the applier.
func TestPlanKeyCollationGuard_FunctionalPartNoSourceShape(t *testing.T) {
	f := &functionalKeyPart{
		index: "fk", expr: "lower(`k`)", alone: true, columns: []string{"k"}, named: []string{"k"}, readable: true,
		steps: []functionalStep{{kind: "fn", fn: "LOWER"}}, known: true, collation: "utf8mb4_0900_ai_ci",
	}
	keys := targetUniqueKeys{pk: []string{"id"}, uniques: [][]string{{"k"}}, uniqueSubs: [][]int{{0}}, uniqueFns: [][]*functionalKeyPart{{f}}}
	ci := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}
	if p := planKeyCollationGuard(nil, kcgCols(ci), keys, mysqlKeyGuardCollations); p == nil || !p.armsFunctional() {
		t.Fatalf("no source shape: plan %+v; want the functional key armed", p)
	}
}

// TestPlanKeyCollationGuard_FunctionalParts is the arming matrix: a
// functional part arms its key exactly when its value may be a collated
// string and the source's comparison of it can differ from the index's
// ([functionalPartArms]: an armed column, a source collation other than the
// index's — review F2 — or an unknown index collation), on the applier and
// the copy alike, readable or not (review F4: an unreadable part arms per
// row, never refuses the table).
func TestPlanKeyCollationGuard_FunctionalParts(t *testing.T) {
	pgDefault := ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic}
	srcBin := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_bin"}
	tgtCI := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}
	tgtBin := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_bin"}
	lower := func(known bool, coll string) *functionalKeyPart {
		return &functionalKeyPart{
			index: "fk", expr: "lower(`k`)", alone: true, columns: []string{"k"}, named: []string{"k"}, readable: true,
			steps: []functionalStep{{kind: "fn", fn: "LOWER"}}, known: known, collation: coll,
		}
	}
	md5 := func(known bool, coll string) *functionalKeyPart {
		return &functionalKeyPart{index: "fk", expr: "md5(`k`)", alone: true, columns: []string{"k"}, named: []string{"k"}, known: known, collation: coll}
	}
	keys := func(f *functionalKeyPart) targetUniqueKeys {
		return targetUniqueKeys{pk: []string{"id"}, uniques: [][]string{{"k"}}, uniqueSubs: [][]int{{0}}, uniqueFns: [][]*functionalKeyPart{{f}}}
	}
	const readTgt = "(LOWER(`s`.`kd`.`k`) COLLATE utf8mb4_0900_ai_ci) <=> (LOWER(new.`k`) COLLATE utf8mb4_0900_ai_ci)"
	for _, tc := range []struct {
		name     string
		src, tgt ir.Type
		part     *functionalKeyPart
		wantTgt  string // "" = not armed
		hidden   string // the source key's read hidden collation ("" none read)
	}{
		{"postgres default, collated", pgDefault, tgtCI, lower(true, "utf8mb4_0900_ai_ci"), readTgt, ""},
		{"postgres default, unknown collation", pgDefault, tgtCI, lower(false, ""), "((TRUE) AND NOT", ""},
		{"postgres default, not a string", pgDefault, tgtCI, lower(true, ""), "", ""},
		{"mysql _bin, collated", srcBin, tgtCI, lower(true, "utf8mb4_0900_ai_ci"), readTgt, ""},
		{"F2: column aligned (_bin both sides), key collated", srcBin, tgtBin, lower(true, "utf8mb4_0900_ai_ci"), readTgt, ""},
		{"F4: unreadable collated part", pgDefault, tgtCI, md5(true, "latin1_swedish_ci"), "((TRUE) AND NOT", ""},
		{"F4: unreadable part, collation unknown", pgDefault, tgtCI, md5(false, ""), "((TRUE) AND NOT", ""},
		{"F4: unreadable non-string part", pgDefault, tgtCI, md5(true, ""), "", ""},
		// MySQL → MySQL: the source key's READ hidden collation decides
		// (FUNCTIONAL-CAST-HIDDEN-COLLATION): equal to the target index's,
		// nothing arms (re-review finding 2); different or unread, it arms.
		// The "mysql _bin" and "F2" rows above read none, so they arm.
		{"mysql to mysql, the source's own key (hidden collation read, equal)", tgtCI, tgtCI, lower(true, "utf8mb4_0900_ai_ci"), "", "utf8mb4_0900_ai_ci"},
		{"mysql to mysql, hidden collation read, different (S1/S2)", tgtCI, tgtCI, lower(true, "utf8mb4_0900_ai_ci"), readTgt, "utf8mb4_bin"},
		// Sixth review F1: the key is read equal, but the column diverges.
		{"mysql to mysql, hidden collation read, equal, column diverges", srcBin, tgtCI, lower(true, "utf8mb4_0900_ai_ci"), readTgt, "utf8mb4_0900_ai_ci"},
		{"mysql to mysql, index collation unknown", tgtCI, tgtCI, lower(false, ""), "((TRUE) AND NOT", ""},
		{"mysql to mysql, unreadable part, hidden collation read, equal", tgtCI, tgtCI, md5(true, "utf8mb4_0900_ai_ci"), "", "utf8mb4_0900_ai_ci"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for name, p := range map[string]*keyCollationPlan{
				"applier": planKeyCollationGuard(withHidden(kcgSource(tc.src), tc.hidden), kcgCols(tc.tgt), keys(tc.part), mysqlKeyGuardCollations),
				"copy":    planCopyKeyCollationGuard(withHidden(kcgSource(tc.src), tc.hidden), kcgCols(tc.tgt), keys(tc.part), mysqlKeyGuardCollations),
			} {
				if (p != nil) != (tc.wantTgt != "") {
					t.Fatalf("%s: armed %v; want %v", name, p != nil, tc.wantTgt != "")
				}
				if p == nil {
					continue
				}
				if !p.armsFunctional() {
					t.Errorf("%s: armsFunctional false", name)
				}
				assign, _, ok := p.render("s", "kd", []string{"d", "id", "K"}, upsertRowAlias)
				if !ok {
					t.Fatalf("%s: render refused (the insert spells the column K)", name)
				}
				for _, want := range []string{tc.wantTgt, "CAST(`s`.`kd`.`k` AS BINARY) <=> CAST(new.`k` AS BINARY)"} {
					if !strings.Contains(assign, want) {
						t.Errorf("%s: guard lacks %q:\n%s", name, want, assign)
					}
				}
				needs, recopyAligns := keyCollationNeeds(p)
				if recopyAligns || !strings.Contains(needs, "functional key `fk`") || strings.Contains(needs, "`k` a binary collation") {
					t.Errorf("%s: needs %q recopyAligns %v; want the functional key recreated, never the column", name, needs, recopyAligns)
				}
				if r := keyCollationMergeRemedy(p, "s", "kd"); strings.Contains(r, "MODIFY <column>") || !strings.Contains(r, "DROP INDEX `fk`") {
					t.Errorf("%s: applier remedy %q", name, r)
				}
				if r := copyKeyCollationMergeRemedy(p, "s", "kd"); strings.Contains(r, "MODIFY <column>") || !strings.Contains(r, "DROP INDEX `fk`") {
					t.Errorf("%s: copy remedy %q", name, r)
				}
			}
		})
	}
}

// TestPlanKeyCollationGuard_PlainKeyUnchanged: a plain key renders no
// functional comparison and keeps the column remedy (the control for the
// functional arm), and a table with no armed column and no functional part
// has no plan at all.
func TestPlanKeyCollationGuard_PlainKeyUnchanged(t *testing.T) {
	keys := targetUniqueKeys{pk: []string{"id"}, uniques: [][]string{{"k"}}}
	p := planKeyCollationGuard(kcgSource(ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic}),
		kcgCols(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}), keys, mysqlKeyGuardCollations)
	if p == nil || p.armsFunctional() {
		t.Fatalf("plan %+v", p)
	}
	assign, _, _ := p.render("s", "kd", []string{"d", "id", "k"}, upsertRowAlias)
	if strings.Contains(assign, "LOWER(") || strings.Contains(assign, "TRUE") {
		t.Errorf("a plain key rendered a functional comparison: %s", assign)
	}
	if r := keyCollationMergeRemedy(p, "s", "kd"); !strings.Contains(r, "MODIFY <column>") || !strings.Contains(r, "utf8mb4_0900_bin") {
		t.Errorf("plain remedy %q; want the column altered to a collation that does not pad", r)
	}
	ci := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}
	if p := planKeyCollationGuard(kcgSource(ci), kcgCols(ci), keys, mysqlKeyGuardCollations); p != nil {
		t.Errorf("an unarmed plain-keyed table has a plan: %+v", p)
	}
}

// TestResolveFunctionalColumns: the expression's spelling of a column is
// resolved to the table's (review F1), the key entry with it; a name the
// table does not have is returned for the refusal.
func TestResolveFunctionalColumns(t *testing.T) {
	a := &functionalKeyPart{index: "fa", columns: []string{"EMAIL"}, readable: true}
	b := &functionalKeyPart{index: "fb", columns: []string{"Ab", "j"}}
	c := &functionalKeyPart{index: "fc", columns: []string{"na?ve-mangled"}}
	// Re-review finding 1: a part over a GENERATED column reads its base
	// columns (through a generated column among them), and its target side
	// can no longer render the expression.
	g := &functionalKeyPart{index: "fg", columns: []string{"G"}, readable: true}
	k := targetUniqueKeys{
		uniques:   [][]string{{"EMAIL"}, {"x", "Ab"}, {"na?ve-mangled"}, {"G"}},
		uniqueFns: [][]*functionalKeyPart{{a}, {nil, b}, {c}, {g}},
	}
	bad := k.resolveFunctionalColumns([]tableColumn{
		{name: "id"},
		{name: "Email"},
		{name: "ab"},
		{name: "J"},
		{name: "x"},
		{name: "g", generation: "concat(`h`,`Email`)"},
		{name: "h", generation: "upper(`x`)"},
	})
	if a.columns[0] != "Email" || k.uniques[0][0] != "Email" || !a.readable {
		t.Errorf("a %+v, key %v", a, k.uniques[0])
	}
	if b.columns[0] != "ab" || b.columns[1] != "J" || k.uniques[1][1] != "ab" {
		t.Errorf("b %+v, key %v", b.columns, k.uniques[1])
	}
	if strings.Join(g.columns, "|") != "x|Email" || strings.Join(g.named, "|") != "g" || g.readable || k.uniques[3][0] != "x" {
		t.Errorf("generated: %+v, key %v", g, k.uniques[3])
	}
	if len(bad) != 1 || bad[0] != c {
		t.Fatalf("unresolved %+v; want fc", bad)
	}
	err := keyCollationFunctionalUnsupported("s.kd", bad)
	if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(err.Error(), keyCollationGuardUnsupportedMarker) ||
		!strings.Contains(err.Error(), "`fc`") {
		t.Errorf("refusal %v", err)
	}
	var term interface{ Terminal() bool }
	if !errors.As(err, &term) || !term.Terminal() {
		t.Errorf("the refusal is not terminal: %v", err)
	}
}

// TestExpressionColumns: every backquoted identifier, in order, a doubled
// backquote kept, and a string literal — either delimiter form, a backquote
// inside it — skipped over (re-review finding 6).
func TestExpressionColumns(t *testing.T) {
	// SQL spelling, as expressionColumns receives it.
	for expr, want := range map[string]string{
		"concat(`a`,_utf8mb4'x',`b``c`, md5(`D`))": "a|b`c|D",
		"concat(`k`,_utf8mb4'a`b')":                "k",
		"concat(`k`,'a`b','it''s `x`')":            "k",
		"concat(`k`,_utf8mb4'it\\'s',`j`)":         "k|j",
		"concat(_utf8mb4'x''y',`k`)":               "k",
		"concat(`k`,\"dq'x\",`j`)":                 "k|j",
		"(1 + 2)":                                  "",
	} {
		if got := strings.Join(expressionColumns(expr), "|"); got != want {
			t.Errorf("%s: %q; want %q", expr, got, want)
		}
	}
	// Third review finding 1: the exact catalog text MySQL 8.4 stores for
	// concat(k,'it''s',j) and for "dq'x" — the information_schema layer over
	// the SQL spelling — decoded first, as uniqueKeyColumns does. Scanned raw,
	// the inner \' closed the literal and j was lost.
	for catalog, want := range map[string]string{
		"concat(`k`,_utf8mb4\\'it\\\\\\'s\\',`j`)": "k|j",
		"concat(`k`,_utf8mb4\\'dq\\\\\\'x\\',`j`)": "k|j",
	} {
		if got := strings.Join(expressionColumns(decodeISExpressionEscapes(catalog)), "|"); got != want {
			t.Errorf("catalog %s: %q; want %q", catalog, got, want)
		}
	}
	// A literal that does not scan refuses at resolution.
	if got := expressionColumns("concat(`k`,'open"); len(got) != 2 || got[1] != unreadableLiteral {
		t.Errorf("unterminated: %q", got)
	}
}

// TestDecodeCatalogExpression: MySQL's information_schema layer is undone;
// MariaDB stores none, so its `\'` is the value's own and stays (fourth
// review finding 2).
func TestDecodeCatalogExpression(t *testing.T) {
	const catalog = "concat(`k`,_utf8mb4\\'it\\\\\\'s\\',`j`)"
	if got := decodeCatalogExpression(FlavorVanilla, catalog); got != "concat(`k`,_utf8mb4'it\\'s',`j`)" {
		t.Errorf("mysql: %s", got)
	}
	const mariadb = "concat(`k`,'it\\'s',`j`)"
	if got := decodeCatalogExpression(FlavorMariaDB, mariadb); got != mariadb {
		t.Errorf("mariadb: %s", got)
	}
	if got := strings.Join(expressionColumns(decodeCatalogExpression(FlavorMariaDB, mariadb)), "|"); got != "k|j" {
		t.Errorf("mariadb columns: %q", got)
	}
}

// TestKeyCollationFunctionalUnsupported_RemedyPerCause: each cause names its
// own remedy (fourth review finding 4).
func TestKeyCollationFunctionalUnsupported_RemedyPerCause(t *testing.T) {
	for cause, want := range map[string]string{
		unresolvedColumn:  "columns named in ASCII",
		unresolvedLiteral: "literal written plainly",
		unresolvedNesting: "shallower expression",
	} {
		err := keyCollationFunctionalUnsupported("s.kd", []*functionalKeyPart{{index: "fk", expr: "x", unresolved: cause}})
		if !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), keyCollationGuardUnsupportedMarker) {
			t.Errorf("%q: %v", want, err)
		}
		for other, otherWant := range map[string]string{unresolvedColumn: "ASCII", unresolvedLiteral: "literal", unresolvedNesting: "eight deep"} {
			if other != cause && strings.Contains(err.Error(), otherWant) {
				t.Errorf("%q names another cause's remedy (%q): %v", want, otherWant, err)
			}
		}
	}
}

// TestResolveFunctionalCollations matches a part to its one hidden column —
// by the full key name, or a shortened one by prefix (review F4: a key name
// over 52 characters) — and leaves it unknown when none or two match.
func TestResolveFunctionalCollations(t *testing.T) {
	long := strings.Repeat("functional_key_", 4)
	a := &functionalKeyPart{index: "we!rd", seq: 0}
	b := &functionalKeyPart{index: "two", seq: 1}
	c := &functionalKeyPart{index: long, seq: 0}
	d := &functionalKeyPart{index: long + "2", seq: 0}
	e := &functionalKeyPart{index: "absent", seq: 0}
	k := targetUniqueKeys{uniqueFns: [][]*functionalKeyPart{{a}, {nil, b}, {c}, {e}}}
	hidden := []hiddenKeyColumn{
		{index: "we!rd", part: 0, collation: "utf8mb4_bin"},
		{index: "two", part: 1, collation: "latin1_swedish_ci"},
		{index: long[:52], part: 0, collation: "utf8mb4_0900_ai_ci", truncated: true},
	}
	k.resolveFunctionalCollations(hidden, true)
	if !a.known || a.collation != "utf8mb4_bin" || !b.known || b.collation != "latin1_swedish_ci" {
		t.Errorf("a %+v b %+v", a, b)
	}
	if !c.known || c.collation != "utf8mb4_0900_ai_ci" {
		t.Errorf("shortened name: %+v", c)
	}
	if e.known {
		t.Errorf("no hidden column, yet known: %+v", e)
	}
	// Two long names sharing their first 52 characters: ambiguous.
	k2 := targetUniqueKeys{uniqueFns: [][]*functionalKeyPart{{c}, {d}}}
	k2.resolveFunctionalCollations([]hiddenKeyColumn{
		{index: long[:52], part: 0, collation: "utf8mb4_0900_ai_ci", truncated: true},
		{index: long[:52], part: 0, collation: "utf8mb4_bin", truncated: true},
	}, true)
	if c.known || d.known {
		t.Errorf("ambiguous shortened names resolved: c %+v d %+v", c, d)
	}
	k.resolveFunctionalCollations(nil, false)
	if a.known || b.known {
		t.Error("an unread hidden column left a collation known")
	}
}

// TestFunctionalKeyNeed spells out the statement only for a key whose only
// part this is; for a wider key it names the part and keeps the others. The
// comparison never pads (review F5): utf8mb4_0900_bin for a utf8mb4 value,
// a binary CAST for any other.
func TestFunctionalKeyNeed(t *testing.T) {
	f := &functionalKeyPart{index: "fk", expr: "lower(`k`)", alone: true, known: true, collation: "utf8mb4_0900_ai_ci"}
	if got := functionalKeyNeed(f); !strings.Contains(got, "ADD UNIQUE KEY `fk` (((lower(`k`)) COLLATE utf8mb4_0900_bin))") {
		t.Errorf("alone: %s", got)
	}
	f.alone = false
	if got := functionalKeyNeed(f); strings.Contains(got, "ADD UNIQUE KEY") || !strings.Contains(got, "other parts unchanged") {
		t.Errorf("one part of several: %s", got)
	}
	f.alone, f.collation = true, "latin1_swedish_ci"
	if got := functionalKeyNeed(f); !strings.Contains(got, "((CAST(lower(`k`) AS BINARY)))") {
		t.Errorf("latin1: %s", got)
	}
	// Re-review finding 4: with the charset unknown (vtgate), a utf8mb4
	// collation would error on a latin1 expression; the binary CAST runs on
	// any.
	f.known, f.collation = false, ""
	if got := functionalKeyNeed(f); !strings.Contains(got, "((CAST(lower(`k`) AS BINARY)))") {
		t.Errorf("unknown collation: %s", got)
	}
	if strings.Contains(functionalKeyNeed(f), "utf8mb4_bin") {
		t.Error("names the PAD SPACE utf8mb4_bin")
	}
}

// TestFunctionalPartExpr_PinsTheSourceHiddenCollation: a same-dialect
// functional part carrying the source key's hidden collation is emitted as
// `(expr) COLLATE <it>` (FUNCTIONAL-CAST-HIDDEN-COLLATION, the class fix), so
// the rebuilt key compares as the source's; a part without one, or from
// another dialect, emits as before.
func TestFunctionalPartExpr_PinsTheSourceHiddenCollation(t *testing.T) {
	for _, tc := range []struct {
		c    ir.IndexColumn
		want string
	}{
		{ir.IndexColumn{Expression: "lower(k)", ExpressionDialect: "mysql", HiddenCollation: "utf8mb4_bin"}, "(lower(k)) COLLATE utf8mb4_bin"},
		{
			ir.IndexColumn{Expression: "cast(k as char(10) charset utf8mb4)", ExpressionDialect: "mysql", HiddenCollation: "utf8mb4_general_ci"},
			"(cast(k as char(10) charset utf8mb4)) COLLATE utf8mb4_general_ci",
		},
		{ir.IndexColumn{Expression: "lower(k)", ExpressionDialect: "mysql"}, "lower(k)"},
		{ir.IndexColumn{Expression: "lower(k)", ExpressionDialect: "postgres", HiddenCollation: "utf8mb4_bin"}, "lower(k)"},
	} {
		if got := functionalPartExpr(tc.c, false); got != tc.want {
			t.Errorf("%+v: %q; want %q", tc.c, got, tc.want)
		}
	}
	got := emitIndexColumnList([]ir.IndexColumn{{Expression: "trim(k)", ExpressionDialect: "mysql", HiddenCollation: "utf8mb4_bin"}}, false)
	if got != "(((trim(k)) COLLATE utf8mb4_bin))" {
		t.Errorf("index column list %q", got)
	}
}
