// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The marker body: translateExprForPG (MySQL→PG) rewrites ifnull(...) →
// COALESCE(...). So a body that emits unchanged proves the translator did NOT
// run; a body containing COALESCE proves it did.
const guardBodyMySQL = "ifnull(a, 0)"

// TestWriterDialectGuard_SQLiteVerbatim pins ADR-0133 §2 on the PG writer: a
// "sqlite"-dialect (or "" / unknown) generated / CHECK / partial-predicate /
// index-expression body emits VERBATIM — never fed through the MySQL→PG
// translator — while a "mysql"-dialect body still translates (regression
// guard). Verbatim is the load-bearing correctness fix: a SQLite expression
// must not be silently mistranslated.
func TestWriterDialectGuard_SQLiteVerbatim(t *testing.T) {
	opts := emitOpts{}

	for _, tc := range []struct {
		name        string
		dialect     string
		wantVerbat  bool // true → expect guardBodyMySQL unchanged; false → expect COALESCE
		mustContain string
	}{
		{"sqlite", "sqlite", true, ""},
		{"empty", "", true, ""},
		{"unknown", "duckdb", true, ""},
		{"mysql_translates", "mysql", false, "COALESCE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gen := translateGeneratedExpr(
				&ir.Column{Type: ir.Integer{Width: 64}, GeneratedExpr: guardBodyMySQL, GeneratedExprDialect: tc.dialect},
				nil, opts,
			)
			chk := translateCheckExpr(
				&ir.CheckConstraint{Expr: guardBodyMySQL, ExprDialect: tc.dialect}, nil, opts,
			)
			pred := translateIndexPredicate(
				&ir.Index{Predicate: guardBodyMySQL, PredicateDialect: tc.dialect}, opts,
			)
			expr := translateIndexExpr(
				ir.IndexColumn{Expression: guardBodyMySQL, ExpressionDialect: tc.dialect}, opts,
			)
			for label, got := range map[string]string{
				"generated": gen, "check": chk, "predicate": pred, "indexExpr": expr,
			} {
				if tc.wantVerbat {
					if got != guardBodyMySQL {
						t.Errorf("%s[%s] = %q; want VERBATIM %q (translator must not run)", label, tc.name, got, guardBodyMySQL)
					}
				} else if !strings.Contains(got, tc.mustContain) {
					t.Errorf("%s[%s] = %q; want it translated to contain %q", label, tc.name, got, tc.mustContain)
				}
			}
		})
	}
}
