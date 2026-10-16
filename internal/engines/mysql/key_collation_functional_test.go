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

// TestFunctionalKeyPart_TargetEq: the target side compares under the
// index's own collation, without one for a value that is not a string, and
// assumes a match (TRUE) when the collation is not known.
func TestFunctionalKeyPart_TargetEq(t *testing.T) {
	steps := []functionalStep{{kind: "fn", fn: "LOWER"}}
	if got := (&functionalKeyPart{steps: steps, known: true, collation: "utf8mb4_0900_ai_ci"}).targetEq("e", "i"); got !=
		"(LOWER(e) COLLATE utf8mb4_0900_ai_ci) <=> (LOWER(i) COLLATE utf8mb4_0900_ai_ci)" {
		t.Errorf("known collation: %s", got)
	}
	if got := (&functionalKeyPart{steps: steps, known: true}).targetEq("e", "i"); got != "LOWER(e) <=> LOWER(i)" {
		t.Errorf("not a string: %s", got)
	}
	if got := (&functionalKeyPart{steps: steps}).targetEq("e", "i"); got != "TRUE" {
		t.Errorf("unknown collation: %s", got)
	}
}

// TestPlanKeyCollationGuard_FunctionalParts is the arming matrix: a read-
// through part arms its key exactly when its column is armed and its value
// may be a collated string, on the applier and the copy alike.
func TestPlanKeyCollationGuard_FunctionalParts(t *testing.T) {
	pgDefault := ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic}
	srcBin := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_bin"}
	tgtCI := ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}
	lower := func(known bool, coll string) *functionalKeyPart {
		return &functionalKeyPart{index: "fk", expr: "lower(`k`)", alone: true, column: "k", steps: []functionalStep{{kind: "fn", fn: "LOWER"}}, known: known, collation: coll}
	}
	keys := func(f *functionalKeyPart) targetUniqueKeys {
		return targetUniqueKeys{pk: []string{"id"}, uniques: [][]string{{"k"}}, uniqueSubs: [][]int{{0}}, uniqueFns: [][]*functionalKeyPart{{f}}}
	}
	for _, tc := range []struct {
		name     string
		src, tgt ir.Type
		part     *functionalKeyPart
		armed    bool
	}{
		{"postgres default, collated", pgDefault, tgtCI, lower(true, "utf8mb4_0900_ai_ci"), true},
		{"postgres default, unknown collation", pgDefault, tgtCI, lower(false, ""), true},
		{"postgres default, not a string", pgDefault, tgtCI, lower(true, ""), false},
		{"mysql _bin, collated", srcBin, tgtCI, lower(true, "utf8mb4_0900_ai_ci"), true},
		{"same collation both sides (unarmed column)", tgtCI, tgtCI, lower(true, "utf8mb4_0900_ai_ci"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for name, p := range map[string]*keyCollationPlan{
				"applier": planKeyCollationGuard(kcgSource(tc.src), kcgCols(tc.tgt), keys(tc.part), mysqlKeyGuardCollations),
				"copy":    planCopyKeyCollationGuard(kcgSource(tc.src), kcgCols(tc.tgt), keys(tc.part), mysqlKeyGuardCollations),
			} {
				if (p != nil) != tc.armed {
					t.Fatalf("%s: armed %v; want %v", name, p != nil, tc.armed)
				}
				if p == nil {
					continue
				}
				if !p.armsFunctional() {
					t.Errorf("%s: armsFunctional false", name)
				}
				assign, _, ok := p.render("s", "kd", []string{"d", "id", "k"}, upsertRowAlias)
				if !ok {
					t.Fatalf("%s: render refused", name)
				}
				wantTgt := "(LOWER(`s`.`kd`.`k`) COLLATE utf8mb4_0900_ai_ci) <=> (LOWER(new.`k`) COLLATE utf8mb4_0900_ai_ci)"
				if !tc.part.known {
					wantTgt = "((TRUE) AND NOT"
				}
				for _, want := range []string{wantTgt, "CAST(`s`.`kd`.`k` AS BINARY) <=> CAST(new.`k` AS BINARY)"} {
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
// functional arm).
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
	if r := keyCollationMergeRemedy(p, "s", "kd"); !strings.Contains(r, "MODIFY <column>") {
		t.Errorf("plain remedy %q", r)
	}
}

// TestTargetUniqueKeys_RefusesOpaque: an opaque part refuses unless it is
// known not to be a string; the refusal names the key and the expression.
func TestTargetUniqueKeys_RefusesOpaque(t *testing.T) {
	k := targetUniqueKeys{opaque: []opaqueKeyPart{
		{index: "md", expr: "md5(`k`)", known: true, collation: "latin1_swedish_ci"},
		{index: "nm", expr: "(`j` * 2)", known: true},
		{index: "un", expr: "concat(`a`,`b`)"},
	}}
	got := k.refusesOpaque()
	if len(got) != 2 || got[0].index != "md" || got[1].index != "un" {
		t.Fatalf("refusing %+v; want md and un", got)
	}
	err := keyCollationFunctionalUnsupported("s.kd", got)
	if !errors.Is(err, ir.ErrKeyCollationMerge) {
		t.Errorf("not an ErrKeyCollationMerge: %v", err)
	}
	for _, want := range []string{keyCollationGuardUnsupportedMarker, "s.kd", "`md` (md5(`k`))", "`un` (concat(`a`,`b`))"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal lacks %q: %v", want, err)
		}
	}
	var term interface{ Terminal() bool }
	if !errors.As(err, &term) || !term.Terminal() {
		t.Errorf("the refusal is not terminal: %v", err)
	}
}

// TestResolveFunctionalCollations reads the hidden columns' names from the
// right (an index name may hold '!') and refuses to guess between two.
func TestResolveFunctionalCollations(t *testing.T) {
	a := &functionalKeyPart{index: "we!rd", seq: 0}
	b := &functionalKeyPart{index: "two", seq: 1}
	c := &functionalKeyPart{index: "dup", seq: 0}
	k := targetUniqueKeys{uniqueFns: [][]*functionalKeyPart{{a}, {nil, b}, {c}}, opaque: []opaqueKeyPart{{index: "nm", seq: 0}}}
	k.resolveFunctionalCollations(map[string]string{"we!rd!0": "utf8mb4_bin", "two!1": "latin1_swedish_ci", "dup!0": "\x00", "nm!0": ""}, true)
	if !a.known || a.collation != "utf8mb4_bin" || !b.known || b.collation != "latin1_swedish_ci" || c.known {
		t.Errorf("a %+v b %+v c %+v", a, b, c)
	}
	if !k.opaque[0].known || k.opaque[0].collation != "" {
		t.Errorf("opaque %+v", k.opaque[0])
	}
	k.resolveFunctionalCollations(nil, false)
	if a.known || b.known {
		t.Error("an unread hidden column left a collation known")
	}
}

// TestFunctionalKeyNeed spells out the statement only for a key whose only
// part this is; for a wider key it names the part and keeps the others,
// since recreating the key over one part would drop the rest.
func TestFunctionalKeyNeed(t *testing.T) {
	f := &functionalKeyPart{index: "fk", expr: "lower(`k`)", alone: true, known: true, collation: "latin1_swedish_ci"}
	if got := functionalKeyNeed(f); !strings.Contains(got, "ADD UNIQUE KEY `fk` ((lower(`k`) COLLATE latin1_bin))") {
		t.Errorf("alone: %s", got)
	}
	f.alone = false
	if got := functionalKeyNeed(f); strings.Contains(got, "ADD UNIQUE KEY") || !strings.Contains(got, "other parts unchanged") {
		t.Errorf("one part of several: %s", got)
	}
	f.known, f.alone = false, true
	if got := functionalKeyNeed(f); !strings.Contains(got, "COLLATE utf8mb4_bin") {
		t.Errorf("unknown collation: %s", got)
	}
}
