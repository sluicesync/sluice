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
// expression dispatch (emitDefault): an "" / unknown DEFAULT body is NOT fed
// through translateExprForMySQL (the `||` concat stays `||`, not CONCAT), a
// "postgres" body still translates, and the bitLiteralDialect arm is intact.
// A "sqlite" body routes through the SQLite→MySQL translator instead: a
// portable body TRANSLATES (`'a' || 'b'` means concat on SQLite but LOGICAL
// OR to MySQL — verbatim carry silently evaluated to 0); a refused body is
// either a proven-faithful residue carried verbatim or is warn-DROPPED by
// emitColumnDef's value-divergence pre-flight (MySQL parses `||`/`/`/`%`
// bodies with different semantics, so "the parser rejects it loudly" does
// not hold). emitDefault applies the MySQL function-default paren wrap, so
// bodies land wrapped — the assertions are on translate-vs-drop-vs-carry,
// not the wrap.
func TestWriterDialectGuard_DefaultExpr_MySQL(t *testing.T) {
	typ := ir.Varchar{Length: 50}

	for _, tc := range []struct {
		name       string
		dialect    string
		wantConcat bool
	}{
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

	// A portable "sqlite" DEFAULT body translates through the SQLite→MySQL
	// translator — the parseDefault-misclassification fix's MySQL half: the
	// concat must land as CONCAT, never as MySQL's logical-OR reading of ||.
	if got, ok := emitDefault(ir.DefaultExpression{Expr: `'a' || 'b'`, Dialect: "sqlite"}, typ); !ok || got != `(CONCAT('a', 'b'))` {
		t.Errorf("sqlite portable default = (%q, %v); want ((CONCAT('a', 'b')), true)", got, ok)
	}

	// The value-divergence drop boundary (review follow-up): a "sqlite"
	// DEFAULT the translator refuses is NOT carried verbatim in general —
	// MySQL PARSES these bodies with different semantics (`||` reads as
	// logical OR → 0, `7/2` as decimal 3.5 vs SQLite's 3, `7.5 % 2` as 1.5
	// vs SQLite's 1) — so emitColumnDef DROPS the DEFAULT with a loud warn
	// instead of landing a silently wrong value. This also proves the
	// PG→MySQL translator never runs on a sqlite body: it WOULD rewrite
	// the `||` cells into CONCAT, but no DEFAULT clause is emitted at all.
	for _, body := range []string{
		`myfunc(a) || 'x'`,
		`upper('a') || 'b'`,
		`7/2`,
		`7.5 % 2`,
	} {
		col := &ir.Column{
			Name: "d", Type: ir.Varchar{Length: 50}, Nullable: true,
			Default: ir.DefaultExpression{Expr: body, Dialect: "sqlite"},
		}
		def, err := emitColumnDef("t", col)
		if err != nil {
			t.Fatalf("emitColumnDef(sqlite DEFAULT %q) = %v; want nil (warn-drop, never a whole-migration abort)", body, err)
		}
		if strings.Contains(def, "DEFAULT") {
			t.Errorf("sqlite DEFAULT %q emitted %q; want the DEFAULT clause DROPPED (MySQL parses this body with divergent semantics)", body, def)
		}
	}

	// The proven-faithful verbatim residues survive the drop boundary: the
	// bare "draft" misfeature token, its RESERVED-WORD-content sibling
	// "order", and an x'…' blob literal (MySQL's hex-string literal, same
	// bytes). Residues bypass the reserved-word requote entirely — the
	// requote walk doesn't recognise "…" tokens, so it would backtick the
	// word INSIDE the string ("order" landing with literal backticks
	// around the word, a silently different stored value); the no-backtick
	// assertion pins the reserved-content case byte-verbatim, not just the
	// non-reserved representative.
	for body, want := range map[string]string{
		`"draft"`: `"draft"`,
		`"order"`: `"order"`,
		`x'00ff'`: `x'00ff'`,
	} {
		col := &ir.Column{
			Name: "d", Type: ir.Varchar{Length: 50}, Nullable: true,
			Default: ir.DefaultExpression{Expr: body, Dialect: "sqlite"},
		}
		def, err := emitColumnDef("t", col)
		if err != nil {
			t.Fatalf("emitColumnDef(sqlite DEFAULT %q) = %v; want nil", body, err)
		}
		if !strings.Contains(def, "DEFAULT") || !strings.Contains(def, want) {
			t.Errorf("sqlite DEFAULT %q emitted %q; want the verbatim residue %q carried", body, def, want)
		}
		// Scope the no-backtick assertion to the DEFAULT clause — the
		// column-def fragment legitimately backticks the column NAME.
		if i := strings.Index(def, "DEFAULT"); i >= 0 && strings.Contains(def[i:], "`") {
			t.Errorf("sqlite DEFAULT %q emitted %q; a backtick in the DEFAULT clause means the requote mangled the residue's content", body, def)
		}
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
