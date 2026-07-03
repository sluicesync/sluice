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

// guardBodySQLiteNonPortable is gen_random_uuid(): SQLite has no such function
// so the SQLite→MySQL translator returns ok=false (verbatim), while the
// PG→MySQL translator WOULD rewrite it to (UUID()). A verbatim emit proves
// BOTH translators stayed off it — ADR-0133 §2's "a sqlite body is never fed
// through the PG→MySQL translator".
const guardBodySQLiteNonPortable = "gen_random_uuid()"

// TestWriterDialectGuard_SQLiteVerbatim pins ADR-0133 §2 on the MySQL writer: a
// "sqlite"-dialect body that SQLite can't translate (and every "" / unknown
// body) emits VERBATIM — never fed through the PG→MySQL translator — while a
// "postgres"-dialect body still translates (regression guard). MySQL has no
// partial-index predicate path, so the predicate site doesn't exist here.
// (Portable "sqlite" bodies are translated — pinned in sqlite_expr_route_test.go.)
func TestWriterDialectGuard_SQLiteVerbatim(t *testing.T) {
	for _, tc := range []struct {
		name        string
		dialect     string
		body        string
		wantVerbat  bool
		mustContain string
	}{
		{"sqlite", "sqlite", guardBodySQLiteNonPortable, true, ""},
		{"empty", "", guardBodyPG, true, ""},
		{"unknown", "duckdb", guardBodyPG, true, ""},
		{"postgres_translates", "postgres", guardBodyPG, false, "CONCAT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gen := translateGeneratedExpr(
				&ir.Column{Type: ir.Integer{Width: 64}, GeneratedExpr: tc.body, GeneratedExprDialect: tc.dialect},
			)
			chk := translateCheckExpr(
				&ir.CheckConstraint{Expr: tc.body, ExprDialect: tc.dialect},
			)
			expr := translateIndexExpr(
				ir.IndexColumn{Expression: tc.body, ExpressionDialect: tc.dialect},
			)
			for label, got := range map[string]string{
				"generated": gen, "check": chk, "indexExpr": expr,
			} {
				if tc.wantVerbat {
					if got != tc.body {
						t.Errorf("%s[%s] = %q; want VERBATIM %q (translator must not run)", label, tc.name, got, tc.body)
					}
				} else if !strings.Contains(got, tc.mustContain) {
					t.Errorf("%s[%s] = %q; want it translated to contain %q", label, tc.name, got, tc.mustContain)
				}
			}
		})
	}
}

// TestWriterDialectGuard_DefaultExpr_MySQL extends the guard to the DEFAULT-
// expression dispatch (emitDefault): a "sqlite" / "" / unknown DEFAULT body is
// NOT fed through translateExprForMySQL (the `||` concat stays `||`, not
// CONCAT), a "postgres" body still translates, and the bitLiteralDialect arm is
// intact. emitDefault applies the MySQL function-default paren wrap, so the
// verbatim body lands wrapped — the assertion is on translate-vs-not, not the
// wrap.
func TestWriterDialectGuard_DefaultExpr_MySQL(t *testing.T) {
	typ := ir.Varchar{Length: 50}

	for _, tc := range []struct {
		name       string
		dialect    string
		wantConcat bool
	}{
		{"sqlite", "sqlite", false},
		{"empty", "", false},
		{"unknown", "duckdb", false},
		{"postgres_translates", "postgres", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := emitDefault(ir.DefaultExpression{Expr: guardBodyPG, Dialect: tc.dialect}, typ)
			if !ok {
				t.Fatalf("emitDefault returned ok=false for %s", tc.name)
			}
			if tc.wantConcat {
				if !strings.Contains(got, "CONCAT") {
					t.Errorf("default[%s] = %q; want it translated to contain CONCAT", tc.name, got)
				}
			} else {
				if strings.Contains(got, "CONCAT") {
					t.Errorf("default[%s] = %q; want VERBATIM (|| not translated to CONCAT)", tc.name, got)
				}
				if !strings.Contains(got, "||") {
					t.Errorf("default[%s] = %q; want the || operator carried verbatim", tc.name, got)
				}
			}
		})
	}

	// The bit-literal arm survives (only ever applies to defaults): b'…' bare.
	if got, ok := emitDefault(ir.DefaultExpression{Expr: "b'101'", Dialect: bitLiteralDialect}, ir.Bit{Length: 3}); !ok || got != "b'101'" {
		t.Errorf("bit-literal default = (%q, %v); want (b'101', true) (bit arm must stay intact)", got, ok)
	}

	// The concrete silent-corruption case: SQLite's double-quoted-string
	// misfeature DEFAULT "draft" must NOT become the backtick identifier
	// `draft` (a column reference) — the string must survive verbatim.
	got, ok := emitDefault(ir.DefaultExpression{Expr: `"draft"`, Dialect: "sqlite"}, typ)
	if !ok {
		t.Fatal("emitDefault returned ok=false for the draft case")
	}
	if strings.Contains(got, "`") {
		t.Errorf(`sqlite DEFAULT "draft" = %q; must NOT be rewritten into a backtick identifier`, got)
	}
	if !strings.Contains(got, `"draft"`) {
		t.Errorf(`sqlite DEFAULT "draft" = %q; want the "draft" string carried verbatim`, got)
	}
}
