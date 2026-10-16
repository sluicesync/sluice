// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

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

// TestFunctionalPartArms_EachRoute discriminates each route by which a part
// arms its key (third review finding 5: the Postgres cells armed through two
// routes at once, so a mutant of either survived). Every case but its route
// is held off: the index's collation is known and the target column is not
// armed unless the route is the armed one.
func TestFunctionalPartArms_EachRoute(t *testing.T) {
	ci := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}
	bin := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_bin"}
	part := func(known bool, coll string) *functionalKeyPart {
		return &functionalKeyPart{named: []string{"K"}, columns: []string{"k"}, known: known, collation: coll}
	}
	src := func(cols ...*ir.Column) *ir.Table { return &ir.Table{Name: "kd", Columns: cols} }
	for _, tc := range []struct {
		name   string
		f      *functionalKeyPart
		source *ir.Table
		armed  map[string]armedCollation
		want   bool
	}{
		// The control: the source declares k under the index's collation and
		// k is not armed — the source's own key, nothing arms.
		{"control: the source's own key", part(true, "utf8mb4_0900_ai_ci"), src(&ir.Column{Name: "k", Type: ci}), nil, false},
		{
			"armed column only (source collation equals the index's)", part(true, "utf8mb4_0900_ai_ci"),
			src(&ir.Column{Name: "k", Type: ci}),
			map[string]armedCollation{"k": {charset: "utf8mb4", collation: "utf8mb4_0900_ai_ci"}},
			true,
		},
		{
			"collation comparison only (unarmed column, index under another collation)", part(true, "utf8mb4_0900_ai_ci"),
			src(&ir.Column{Name: "k", Type: bin}), nil, true,
		},
		{"the source lacks the column", part(true, "utf8mb4_0900_ai_ci"), src(&ir.Column{Name: "other", Type: ci}), nil, true},
		{"no source shape (finding 3)", part(true, "utf8mb4_0900_ai_ci"), nil, nil, true},
		{"index collation unknown", part(false, ""), src(&ir.Column{Name: "k", Type: ci}), nil, true},
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
	}{
		{"postgres default, collated", pgDefault, tgtCI, lower(true, "utf8mb4_0900_ai_ci"), readTgt},
		{"postgres default, unknown collation", pgDefault, tgtCI, lower(false, ""), "((TRUE) AND NOT"},
		{"postgres default, not a string", pgDefault, tgtCI, lower(true, ""), ""},
		{"mysql _bin, collated", srcBin, tgtCI, lower(true, "utf8mb4_0900_ai_ci"), readTgt},
		{"F2: column aligned (_bin both sides), key collated", srcBin, tgtBin, lower(true, "utf8mb4_0900_ai_ci"), readTgt},
		{"F4: unreadable collated part", pgDefault, tgtCI, md5(true, "latin1_swedish_ci"), "((TRUE) AND NOT"},
		{"F4: unreadable part, collation unknown", pgDefault, tgtCI, md5(false, ""), "((TRUE) AND NOT"},
		{"F4: unreadable non-string part", pgDefault, tgtCI, md5(true, ""), ""},
		// Re-review finding 2: the source compares the column under the very
		// collation the index does (the source's own key), so nothing arms;
		// with the index's collation unknown, it does.
		{"mysql to mysql, the source's own key", tgtCI, tgtCI, lower(true, "utf8mb4_0900_ai_ci"), ""},
		{"mysql to mysql, index collation unknown", tgtCI, tgtCI, lower(false, ""), "((TRUE) AND NOT"},
		{"mysql to mysql, unreadable part under the column's collation", tgtCI, tgtCI, md5(true, "utf8mb4_0900_ai_ci"), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for name, p := range map[string]*keyCollationPlan{
				"applier": planKeyCollationGuard(kcgSource(tc.src), kcgCols(tc.tgt), keys(tc.part), mysqlKeyGuardCollations),
				"copy":    planCopyKeyCollationGuard(kcgSource(tc.src), kcgCols(tc.tgt), keys(tc.part), mysqlKeyGuardCollations),
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
