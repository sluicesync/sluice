// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The marker body: translateExprForMySQL (PG→MySQL) rewrites the `||` concat
// operator → CONCAT(...). So a body that emits unchanged proves the translator
// did NOT run; a body containing CONCAT proves it did. (a/b are not MySQL
// reserved words, so the verbatim path's reserved-word re-quote leaves them
// alone — the body stays byte-identical.)
const guardBodyPG = "a || b"

// TestWriterDialectGuard_SQLiteVerbatim pins ADR-0133 §2 on the MySQL writer: a
// "sqlite"-dialect (or "" / unknown) generated / CHECK / index-expression body
// emits VERBATIM — never fed through the PG→MySQL translator — while a
// "postgres"-dialect body still translates (regression guard). MySQL has no
// partial-index predicate path, so the predicate site doesn't exist here.
func TestWriterDialectGuard_SQLiteVerbatim(t *testing.T) {
	for _, tc := range []struct {
		name        string
		dialect     string
		wantVerbat  bool
		mustContain string
	}{
		{"sqlite", "sqlite", true, ""},
		{"empty", "", true, ""},
		{"unknown", "duckdb", true, ""},
		{"postgres_translates", "postgres", false, "CONCAT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gen := translateGeneratedExpr(
				&ir.Column{Type: ir.Integer{Width: 64}, GeneratedExpr: guardBodyPG, GeneratedExprDialect: tc.dialect},
			)
			chk := translateCheckExpr(
				&ir.CheckConstraint{Expr: guardBodyPG, ExprDialect: tc.dialect},
			)
			expr := translateIndexExpr(
				ir.IndexColumn{Expression: guardBodyPG, ExpressionDialect: tc.dialect},
			)
			for label, got := range map[string]string{
				"generated": gen, "check": chk, "indexExpr": expr,
			} {
				if tc.wantVerbat {
					if got != guardBodyPG {
						t.Errorf("%s[%s] = %q; want VERBATIM %q (translator must not run)", label, tc.name, got, guardBodyPG)
					}
				} else if !strings.Contains(got, tc.mustContain) {
					t.Errorf("%s[%s] = %q; want it translated to contain %q", label, tc.name, got, tc.mustContain)
				}
			}
		})
	}
}
