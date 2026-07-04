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

// guardBodySQLiteNonPortable is CONCAT(a, b): SQLite has no CONCAT function
// (it uses ||), so the SQLite→PG translator can't translate it (ok=false →
// verbatim). The MySQL→PG translator WOULD rewrite CONCAT → (a || b). So a
// verbatim emit proves BOTH translators stayed off it — the load-bearing
// ADR-0133 §2 invariant that a "sqlite"-dialect body is never fed through the
// MySQL→PG translator.
const guardBodySQLiteNonPortable = "CONCAT(a, b)"

// TestWriterDialectGuard_SQLiteVerbatim pins ADR-0133 §2 on the PG writer: a
// "sqlite"-dialect body that SQLite can't translate (and every "" / unknown
// body) emits VERBATIM — never fed through the MySQL→PG translator — while a
// "mysql"-dialect body still translates (regression guard). Verbatim is the
// load-bearing correctness fix: a SQLite expression must not be silently
// mistranslated. (Portable "sqlite" bodies are translated by the SQLite→PG
// translator — pinned separately in sqlite_expr_route_test.go.)
func TestWriterDialectGuard_SQLiteVerbatim(t *testing.T) {
	opts := emitOpts{}

	for _, tc := range []struct {
		name        string
		dialect     string
		body        string
		wantVerbat  bool
		mustContain string
	}{
		{"sqlite", "sqlite", guardBodySQLiteNonPortable, true, ""},
		{"empty", "", guardBodyMySQL, true, ""},
		{"unknown", "duckdb", guardBodyMySQL, true, ""},
		{"mysql_translates", "mysql", guardBodyMySQL, false, "COALESCE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gen := translateGeneratedExpr(
				&ir.Column{Type: ir.Integer{Width: 64}, GeneratedExpr: tc.body, GeneratedExprDialect: tc.dialect},
				nil, opts,
			)
			chk := translateCheckExpr(
				&ir.CheckConstraint{Expr: tc.body, ExprDialect: tc.dialect}, nil, opts,
			)
			pred := translateIndexPredicate(
				&ir.Index{Predicate: tc.body, PredicateDialect: tc.dialect}, opts,
			)
			expr := translateIndexExpr(
				ir.IndexColumn{Expression: tc.body, ExpressionDialect: tc.dialect}, opts,
			)
			for label, got := range map[string]string{
				"generated": gen, "check": chk, "predicate": pred, "indexExpr": expr,
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
// (Chunk A + the ADR-0133 translator): portable "current instant" spellings
// translate to PG keywords, the portable general subset translates through
// the SQLite→PG translator, and non-portable SQLite-only bodies — including
// the double-quoted-string misfeature that used to be the silent-corruption
// case — are DROPPED (ok=false), never fed through the MySQL→PG translator
// and never emitted verbatim.
func TestWriterDialectGuard_DefaultExpr_SQLite(t *testing.T) {
	opts := emitOpts{}

	if got, ok := guardDefault(ir.DefaultExpression{Expr: "datetime('now')", Dialect: "sqlite"}, opts); !ok || got != "CURRENT_TIMESTAMP" {
		t.Errorf("sqlite portable default = (%q, %v); want (CURRENT_TIMESTAMP, true)", got, ok)
	}

	// The portable general subset (the parseDefault-misclassification fix's
	// PG half): a concat DEFAULT lands working instead of warn-dropping, and
	// the residual one-level paren nesting PRAGMA leaves on `(('x'))`
	// collapses to the plain literal.
	if got, ok := guardDefault(ir.DefaultExpression{Expr: `'a' || 'b'`, Dialect: "sqlite"}, opts); !ok || got != `('a' || 'b')` {
		t.Errorf("sqlite concat default = (%q, %v); want (('a' || 'b'), true)", got, ok)
	}
	if got, ok := guardDefault(ir.DefaultExpression{Expr: `('x')`, Dialect: "sqlite"}, opts); !ok || got != `'x'` {
		t.Errorf("sqlite paren-residual default = (%q, %v); want ('x', true)", got, ok)
	}

	// Blob literal x'…': the translator refuses it, so it warn-drops — PG
	// reads X'…' as a hex BIT-STRING literal (type bit), not SQLite's blob,
	// so a verbatim emit would silently change the type.
	if got, ok := guardDefault(ir.DefaultExpression{Expr: `x'00ff'`, Dialect: "sqlite"}, opts); ok || got != "" {
		t.Errorf("sqlite blob default = (%q, %v); want (\"\", false) — dropped", got, ok)
	}

	// The former silent-corruption case: DEFAULT "draft" (SQLite's double-
	// quoted-string misfeature) is held back from the translator (its PG
	// policy would carry it as an identifier, which PG rejects in a DEFAULT,
	// aborting the migration) → dropped, NOT rewritten and NOT emitted
	// verbatim into PG DDL.
	if got, ok := guardDefault(ir.DefaultExpression{Expr: `"draft"`, Dialect: "sqlite"}, opts); ok || got != "" {
		t.Errorf(`sqlite DEFAULT "draft" = (%q, %v); want ("", false) — dropped`, got, ok)
	}

	// The marker body proves the MySQL→PG translator never runs on a SQLite
	// default: SQLite has no CONCAT function so the SQLite→PG translator
	// refuses it, and the MySQL→PG translator (which WOULD rewrite it to
	// (a || b)) must not run — so it drops.
	if got, ok := guardDefault(ir.DefaultExpression{Expr: guardBodySQLiteNonPortable, Dialect: "sqlite"}, opts); ok || got != "" {
		t.Errorf("sqlite non-portable default = (%q, %v); want (\"\", false) — dropped, translator must not run", got, ok)
	}
}
