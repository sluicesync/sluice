// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"strings"
	"testing"
)

// TestUnterminatedLiteralAt pins the Bug 243 detector on the MEASURED
// mangle shapes (the exact recordings the pre-2026-08-11 reader produced,
// from the catalog's fixtures) and on the well-formed portable shapes
// that must keep passing — including the two look-alikes that make the
// lex rules load-bearing: a value ENDING in a backslash, and the
// backslash-doubled recording that is structurally valid but
// semantically wrong on PG (the stated blind spot, asserted as PASSING
// here so nobody believes this check covers it).
func TestUnterminatedLiteralAt(t *testing.T) {
	cases := []struct {
		name      string
		expr      string
		malformed bool
	}{
		{"the Bug 243 CHECK mangle", `name <> 'o\\'brien'`, true},
		{"the generated-column mangle", `concat(c,'\\'s')`, true},
		{"plain literal", `status in ('open','closed')`, false},
		{"doubled apostrophe (portable spelling)", `name <> 'o''brien'`, false},
		{"value ending in a backslash", `c <> 'a\'`, false},
		{"backslash-doubled recording — VALID here, the stated blind spot", `c regexp '^a\\d$'`, false},
		{"no literals at all", `qty >= 0`, false},
		{"bare unterminated open", `c <> 'oops`, true},
		{"empty", ``, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if _, got := UnterminatedLiteralAt(tc.expr); got != tc.malformed {
				t.Fatalf("UnterminatedLiteralAt(%q) malformed=%v; want %v", tc.expr, got, tc.malformed)
			}
		})
	}
}

// TestTableExpressionLexProblems_WalksEveryExpressionPosition holds the
// walk to its roster: a mangled expression in EACH position a recorded
// table can carry one must produce a problem naming that position. A
// position the walk silently skips is exactly how Bug 243 shipped —
// verify looked at everything except the thing that breaks restore.
func TestTableExpressionLexProblems_WalksEveryExpressionPosition(t *testing.T) {
	mangled := `x <> 'o\\'brien'`
	tbl := &Table{
		Name: "t",
		Columns: []*Column{
			{Name: "g", GeneratedExpr: mangled},
			{Name: "d", Default: DefaultExpression{Expr: mangled}},
			{Name: "dom", Type: Domain{Name: "dm", BaseType: Text{}, Checks: []DomainCheck{{Name: "dm_chk", Body: mangled}}}},
		},
		CheckConstraints: []*CheckConstraint{{Name: "chk", Expr: mangled}},
		Indexes: []*Index{{
			Name:      "fidx",
			Columns:   []IndexColumn{{Expression: mangled}},
			Predicate: mangled,
		}},
	}
	problems := TableExpressionLexProblems(tbl)
	if len(problems) != 6 {
		t.Fatalf("got %d problems, want 6 (CHECK, generated, default, domain check, index expression, index predicate):\n%s",
			len(problems), strings.Join(problems, "\n"))
	}
	for _, want := range []string{`CHECK "chk"`, `generated column "g"`, `column "d" DEFAULT`, `DOMAIN "dm"`, `index "fidx" expression`, `index "fidx" predicate`} {
		found := false
		for _, p := range problems {
			if strings.Contains(p, want) {
				found = true
			}
		}
		if !found {
			t.Errorf("no problem names %s:\n%s", want, strings.Join(problems, "\n"))
		}
	}
	if got := SchemaExpressionLexProblems(&Schema{Tables: []*Table{tbl}}); len(got) != 6 {
		t.Errorf("schema-level walk returned %d problems; want 6", len(got))
	}
	if got := SchemaExpressionLexProblems(nil); got != nil {
		t.Errorf("nil schema returned %v; want nil", got)
	}
}
