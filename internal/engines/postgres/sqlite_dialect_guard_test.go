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

// guardCol is the throwaway column context translateDefaultExpr now takes
// (it carries table+column only for the SQLite loud-drop warn message).
var guardCol = &ir.Column{Name: "c"}

// guardDefault drives translateDefaultExpr with the throwaway table/col
// context and returns its (body, emit?) pair.
func guardDefault(d ir.DefaultExpression, opts emitOpts) (string, bool) {
	return translateDefaultExpr(nil, guardCol, d, opts)
}

// TestWriterDialectGuard_DefaultExpr_PG extends the guard to the DEFAULT-
// expression dispatch (translateDefaultExpr): an "" / unknown DEFAULT body
// emits VERBATIM (never through translateExprForPG), a "mysql" body still
// translates, and the bitLiteralDialect special-case arm is intact.
//
// "sqlite" is the exception (D1/SQLite robustness Chunk A): it is NOT
// verbatim — the portable "current instant" spellings translate to PG
// keywords and every other SQLite-only expression is DROPPED with a loud
// warn (asserted in TestWriterDialectGuard_DefaultExpr_SQLite below). It
// is still never fed through the MySQL→PG translator.
func TestWriterDialectGuard_DefaultExpr_PG(t *testing.T) {
	opts := emitOpts{}

	for _, tc := range []struct {
		name        string
		dialect     string
		wantVerbat  bool
		mustContain string
	}{
		{"empty", "", true, ""},
		{"unknown", "duckdb", true, ""},
		{"mysql_translates", "mysql", false, "COALESCE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := guardDefault(ir.DefaultExpression{Expr: guardBodyMySQL, Dialect: tc.dialect}, opts)
			if !ok {
				t.Fatalf("default[%s] ok=false; want it emitted", tc.name)
			}
			if tc.wantVerbat {
				if got != guardBodyMySQL {
					t.Errorf("default[%s] = %q; want VERBATIM %q (translator must not run)", tc.name, got, guardBodyMySQL)
				}
			} else if !strings.Contains(got, tc.mustContain) {
				t.Errorf("default[%s] = %q; want it translated to contain %q", tc.name, got, tc.mustContain)
			}
		})
	}

	// The bit-literal arm survives (it only ever applies to defaults): MySQL
	// b'…' → PG B'…'.
	if got, ok := guardDefault(ir.DefaultExpression{Expr: "b'101'", Dialect: bitLiteralDialect}, opts); !ok || got != "B'101'" {
		t.Errorf("bit-literal default = (%q, %v); want (B'101', true) (bit arm must stay intact)", got, ok)
	}
}

// TestWriterDialectGuard_DefaultExpr_SQLite pins the SQLite DEFAULT dispatch
// (Chunk A): portable spellings translate to PG keywords; non-portable
// SQLite-only bodies — including the double-quoted-string misfeature that
// used to be the silent-corruption case — are DROPPED (ok=false), never fed
// through the MySQL→PG translator and never emitted verbatim.
func TestWriterDialectGuard_DefaultExpr_SQLite(t *testing.T) {
	opts := emitOpts{}

	if got, ok := guardDefault(ir.DefaultExpression{Expr: "datetime('now')", Dialect: "sqlite"}, opts); !ok || got != "CURRENT_TIMESTAMP" {
		t.Errorf("sqlite portable default = (%q, %v); want (CURRENT_TIMESTAMP, true)", got, ok)
	}

	// The former silent-corruption case: DEFAULT "draft" (SQLite's double-
	// quoted-string misfeature) is non-portable → dropped, NOT rewritten and
	// NOT emitted verbatim into PG DDL.
	if got, ok := guardDefault(ir.DefaultExpression{Expr: `"draft"`, Dialect: "sqlite"}, opts); ok || got != "" {
		t.Errorf(`sqlite DEFAULT "draft" = (%q, %v); want ("", false) — dropped`, got, ok)
	}

	// The MySQL-marker body proves the MySQL→PG translator never runs on a
	// SQLite default: it's non-portable here, so it drops (it must NOT become
	// COALESCE).
	if got, ok := guardDefault(ir.DefaultExpression{Expr: guardBodyMySQL, Dialect: "sqlite"}, opts); ok || got != "" {
		t.Errorf("sqlite non-portable default = (%q, %v); want (\"\", false) — dropped, translator must not run", got, ok)
	}
}
