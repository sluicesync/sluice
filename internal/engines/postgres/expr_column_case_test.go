// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCrossDialectExpressions_ResolveColumnCase pins GitHub #356 at every
// Postgres site that emits a cross-dialect expression body that can name
// a column: the functional-index key part, the partial-index predicate,
// the generated column and the CHECK constraint, for both source dialects
// that resolve column names case-insensitively (MySQL and SQLite).
//
// Before the fix each body reached Postgres with its identifiers bare, and
// Postgres folds a bare identifier to lower case: `lower(status)` over a
// column named `STATUS`, and even `lower(STATUS)` spelled exactly, named
// no column and the CREATE failed with SQLSTATE 42703 — for an index,
// after the whole copy had run.
func TestCrossDialectExpressions_ResolveColumnCase(t *testing.T) {
	tbl := &ir.Table{
		Name: "DCS_DIS_LOGIC_SETTING",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "STATUS", Type: ir.Integer{Width: 32}},
			{Name: "Price", Type: ir.Integer{Width: 32}},
		},
	}
	for _, dialect := range []string{translatableSourceDialect, sqliteSourceDialect} {
		t.Run(dialect, func(t *testing.T) {
			// A body both translators carry through unchanged apart from
			// the identifiers.
			const body = "status + Price > 0"
			const want = `"STATUS" + "Price" > 0`

			gen := translateGeneratedExpr(
				&ir.Column{Name: "g", Type: ir.Integer{Width: 64}, GeneratedExpr: "status + Price", GeneratedExprDialect: dialect},
				tbl, emitOpts{},
			)
			if noParens(gen) != `"STATUS" + "Price"` {
				t.Errorf("generated column body = %q; want %q", gen, `"STATUS" + "Price"`)
			}
			chk := translateCheckExpr(&ir.CheckConstraint{Expr: body, ExprDialect: dialect}, tbl, emitOpts{})
			if noParens(chk) != want {
				t.Errorf("CHECK body = %q; want %q", chk, want)
			}
			pred := translateIndexPredicate(&ir.Index{Predicate: body, PredicateDialect: dialect}, tbl, emitOpts{})
			if noParens(pred) != want {
				t.Errorf("partial-index predicate = %q; want %q", pred, want)
			}
			expr := translateIndexExpr(ir.IndexColumn{Expression: "status + Price", ExpressionDialect: dialect}, tbl, emitOpts{})
			if noParens(expr) != `"STATUS" + "Price"` {
				t.Errorf("index expression = %q; want %q", expr, `"STATUS" + "Price"`)
			}
		})
	}
}

// TestEmitCreateIndex_FunctionalKeyPartResolvesColumnCase is #356's own
// shape end to end through emitCreateIndex: a MySQL functional index whose
// stored expression spells the column in lower case, over an upper-case
// column, plus a plain key part spelled differently from its column.
func TestEmitCreateIndex_FunctionalKeyPartResolvesColumnCase(t *testing.T) {
	tbl := &ir.Table{
		Name: "DCS_DIS_LOGIC_SETTING",
		Columns: []*ir.Column{
			{Name: "PLANT", Type: ir.Varchar{Length: 100}},
			{Name: "STATUS", Type: ir.Varchar{Length: 20}},
		},
	}
	idx := &ir.Index{
		Name: "PLANT_IDX",
		Columns: []ir.IndexColumn{
			{Column: "plant"},
			{Expression: "lower(status)", ExpressionDialect: translatableSourceDialect},
		},
	}
	stmt, err := emitCreateIndex("public", tbl, idx, emitOpts{})
	if err != nil {
		t.Fatalf("emitCreateIndex: %v", err)
	}
	if !strings.Contains(stmt, `("PLANT", (lower("STATUS")))`) {
		t.Errorf("CREATE INDEX = %q; want the key parts resolved to (\"PLANT\", (lower(\"STATUS\")))", stmt)
	}
}

// TestResolveColumnName pins the plain-key-part resolver: exact match
// wins, a single case-insensitive match resolves, and an ambiguous or
// absent name is returned unchanged for Postgres to judge.
func TestResolveColumnName(t *testing.T) {
	tbl := &ir.Table{Columns: []*ir.Column{{Name: "STATUS"}, {Name: "a"}, {Name: "A"}}}
	for _, c := range []struct{ in, want string }{
		{"STATUS", "STATUS"},
		{"status", "STATUS"},
		{"a", "a"},
		{"A", "A"},
		{"missing", "missing"},
	} {
		if got := resolveColumnName(tbl, c.in); got != c.want {
			t.Errorf("resolveColumnName(%q) = %q; want %q", c.in, got, c.want)
		}
	}
	amb := &ir.Table{Columns: []*ir.Column{{Name: "Ab"}, {Name: "AB"}}}
	if got := resolveColumnName(amb, "ab"); got != "ab" {
		t.Errorf("ambiguous fold resolved to %q; want the name unchanged", got)
	}
	if got := resolveColumnName(nil, "x"); got != "x" {
		t.Errorf("nil table changed the name to %q", got)
	}
}

// noParens drops grouping parentheses, which the SQLite translator adds
// and the MySQL one does not; the identifiers are what these pins grade.
func noParens(s string) string {
	return strings.NewReplacer("(", "", ")", "").Replace(s)
}

// TestRequotePGReservedIdents_TrimKeywordsStayGrammar pins how LEADING /
// TRAILING / BOTH inside TRIM(...) are told apart from a column of the same
// name once the source's quoting is gone. Each wrong answer is valid Postgres
// that trims different characters, so both directions are silent:
//
//   - the keyword quoted as a column (value-fidelity review of v0.160.1);
//   - the column left bare as the keyword (Bug 311, introduced by that
//     review's first fix, which kept the word bare anywhere inside TRIM).
//
// The inputs are the shapes MySQL 8.0.46 and MariaDB 11.4 actually store,
// measured 2026-10-15: a keyword always has a remove-string after it, so a
// word right before FROM, `)` or an operator is the column.
func TestRequotePGReservedIdents_TrimKeywordsStayGrammar(t *testing.T) {
	tbl := &ir.Table{Columns: []*ir.Column{{Name: "Leading"}, {Name: "both"}, {Name: "Trailing"}, {Name: "s"}}}
	for _, c := range []struct{ in, want string }{
		// Keyword: first in TRIM's arguments, an operand follows.
		{"trim(both 'x' from s)", "trim(both 'x' from s)"},
		{"trim(TRAILING 'x' from s)", "trim(TRAILING 'x' from s)"},
		{"trim( leading s from s)", "trim( leading s from s)"},
		// MySQL's stored trim(`both` from `s`): the column is the remove-string.
		{"trim(both from s)", `trim("both" from s)`},
		// trim(both `both` from `s`), MySQL and MariaDB: keyword, then column.
		{"trim(both both from s)", `trim(both "both" from s)`},
		// trim(`Leading`) and concat(trim(`trailing`),'!').
		{"trim(Leading)", `trim("Leading")`},
		{"concat(trim(trailing),'!')", `concat(trim("Trailing"),'!')`},
		// A column followed by an operator is an operand, not the keyword.
		{"trim(both || 'x' from s)", `trim("both" || 'x' from s)`},
		// Outside TRIM they are always columns.
		{"Leading + 1", `"Leading" + 1`},
		{"both || s", `"both" || s`},
	} {
		if got := requotePGReservedIdents(c.in, tbl); got != c.want {
			t.Errorf("requotePGReservedIdents(%q) = %q; want %q", c.in, got, c.want)
		}
	}
	// The reserved-word pass alone (no table) follows the same positions.
	for _, c := range []struct{ in, want string }{
		{"trim(leading 'x' from s)", "trim(leading 'x' from s)"},
		{"trim(leading from s)", `trim("leading" from s)`},
	} {
		if got := requotePGReservedIdents(c.in, nil); got != c.want {
			t.Errorf("requotePGReservedIdents(%q, nil) = %q; want %q", c.in, got, c.want)
		}
	}
}
