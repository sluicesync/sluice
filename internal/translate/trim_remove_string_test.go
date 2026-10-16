// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestTrimRemoveStringDiverges pins Bug 312's detector over the shapes each
// source actually stores. The expressions are written the way sluice's readers
// hand them over: MySQL text with backticks and charset introducers stripped
// and literals in standard spelling (normalizeMySQLExpressionText), Postgres
// text as pg_get_expr prints it (quoted identifiers, ::text casts on literals).
//
// The safe cells are the boundary: a remove-string the two families read the
// same way must never be refused, because the refusal blocks a migration that
// would have been exact.
func TestTrimRemoveStringDiverges(t *testing.T) {
	cases := []struct {
		name    string
		dialect string
		expr    string
		want    bool
	}{
		// MySQL source text.
		{"mysql no remove-string", "mysql", "trim(s)", false},
		{"mysql one-char literal", "mysql", "trim(both 'x' from s)", false},
		{"mysql one-char literal, no side", "mysql", "trim('x' from s)", false},
		{"mysql leading one char", "mysql", "trim(leading '0' from code)", false},
		{"mysql empty literal", "mysql", "trim(both '' from s)", false},
		{"mysql doubled quote is one char", "mysql", "trim(both '''' from s)", false},
		// One character, two bytes: over a VARBINARY operand MySQL strips the
		// byte pair and Postgres's bytea trim strips either byte (the Bug 312
		// review, reproduced on real servers). The operand type is not known
		// here, so it is refused.
		{"mysql multibyte one char is two bytes", "mysql", "trim(trailing 'é' from s)", true},
		{"pg multibyte one char is two bytes", "postgres", "TRIM(BOTH 'é'::text FROM s)", true},
		{"mysql two-char literal", "mysql", "trim(both 'xy' from s)", true},
		{"mysql two-char, no side", "mysql", "trim('xy' from s)", true},
		{"mysql trailing two-char", "mysql", "trim(trailing 'ab' from s)", true},
		{"mysql column remove-string", "mysql", "trim(both pad from s)", true},
		// Bug 311's shape: the bare word before FROM is a column in MySQL text.
		{"mysql column named both", "mysql", "trim(both from code)", true},
		{"mysql column named leading after keyword", "mysql", "trim(leading leading from s)", true},
		{"mysql expression remove-string", "mysql", "trim(both concat('x','y') from s)", true},
		{"mysql unary minus", "mysql", "trim(both -1 from code)", true},
		{"mysql nested in concat", "mysql", "concat(trim(both 'xy' from a), b)", true},
		{"mysql nested safe", "mysql", "concat(trim(both 'x' from a), trim(b))", false},
		{"mysql second call diverges", "mysql", "trim(a) = trim(both 'xy' from b)", true},
		{"mysql literal mentioning trim", "mysql", "s <> 'trim(both ''xy'' from s)'", false},
		{"mysql qualified function", "mysql", "app.trim(both 'xy' from s)", false},
		{"mysql FROM inside literal", "mysql", "trim(both 'x' from replace(s, 'from', 'xy'))", false},

		// Postgres source text.
		{"pg keyword-only (trim(x))", "postgres", "TRIM(BOTH FROM s)", false},
		{"pg ltrim(x) printed form", "postgres", "TRIM(LEADING FROM s)", false},
		{"pg one-char cast literal", "postgres", "TRIM(BOTH 'x'::text FROM s)", false},
		{"pg varchar cast", "postgres", "TRIM(LEADING '0'::character varying FROM code)", false},
		{"pg two-char cast literal", "postgres", "TRIM(BOTH 'xy'::text FROM s)", true},
		// pg_get_expr prints the idiom with the control characters raw (under
		// standard_conforming_strings); the E'' form is hand-written text.
		{"pg whitespace-set idiom as printed", "postgres", "TRIM(BOTH ' \t\r\n'::text FROM s)", true},
		{"pg whitespace-set idiom, E-string", "postgres", `TRIM(BOTH E' \t\r\n'::text FROM s)`, true},
		{"mysql no side keyword, two chars", "mysql", "trim('ab' from s)", true},
		{"pg quoted column named both", "postgres", `TRIM(BOTH "both" FROM s)`, true},
		{"pg column remove-string", "postgres", "TRIM(BOTH pad FROM s)", true},
		{"pg collate suffix is not a plain cast", "postgres", `TRIM(BOTH 'x' COLLATE "C" FROM s)`, true},
		{"pg FROM in quoted ident", "postgres", `TRIM(BOTH 'x'::text FROM "from")`, false},
		{"pg btrim is not TRIM", "postgres", "btrim(s, 'xy'::text)", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			call, got := TrimRemoveStringDiverges(c.expr, c.dialect)
			if got != c.want {
				t.Fatalf("TrimRemoveStringDiverges(%q, %q) = %v (call %q), want %v", c.expr, c.dialect, got, call, c.want)
			}
			if got && !strings.HasPrefix(strings.ToUpper(call), "TRIM") {
				t.Errorf("reported call %q does not start at the TRIM", call)
			}
		})
	}
}

// TestRefuseDivergentTrim pins the refusal's handles: the grep-stable marker,
// the site, and a remedy that runs for that site (--expr-override rewrites
// generated columns only, so a CHECK must not be offered it).
func TestRefuseDivergentTrim(t *testing.T) {
	if err := RefuseDivergentTrim("generated column", "t", "c", "trim(both 'x' from s)", "mysql", "Postgres"); err != nil {
		t.Fatalf("one-char remove-string refused: %v", err)
	}
	gen := RefuseDivergentTrim("generated column", "t", "c", "trim(both 'xy' from s)", "mysql", "Postgres")
	if gen == nil {
		t.Fatal("two-char remove-string was not refused")
	}
	for _, want := range []string{TrimRemoveStringMarker, `"c"`, `table "t"`, "--expr-override t.c=", "trim(both 'xy' from s)"} {
		if !strings.Contains(gen.Error(), want) {
			t.Errorf("generated-column refusal lacks %q:\n%s", want, gen)
		}
	}
	chk := RefuseDivergentTrim("CHECK constraint", "", "ck", "TRIM(BOTH 'xy'::text FROM s) <> ''", "postgres", "MySQL")
	if chk == nil {
		t.Fatal("CHECK with a two-char remove-string was not refused")
	}
	if strings.Contains(chk.Error(), "--expr-override") {
		t.Errorf("CHECK refusal offers --expr-override, which cannot rewrite a CHECK:\n%s", chk)
	}
	if !strings.Contains(chk.Error(), TrimRemoveStringMarker) {
		t.Errorf("CHECK refusal lacks the marker:\n%s", chk)
	}
}

// TestRefuseDivergentTrimIndex pins which indexes are asked: a uniqueness-
// enforcing one with a cross-family expression key part. A non-unique index,
// a same-family key part and a plain column key are not.
func TestRefuseDivergentTrimIndex(t *testing.T) {
	key := func(expr, dialect string) []ir.IndexColumn {
		return []ir.IndexColumn{{Expression: expr, ExpressionDialect: dialect}}
	}
	bad := "trim(both 'xy' from s)"
	cases := []struct {
		name string
		idx  *ir.Index
		want bool
	}{
		{"unique cross-family", &ir.Index{Name: "u", Unique: true, Columns: key(bad, "mysql")}, true},
		{"constraint-backed cross-family", &ir.Index{Name: "u", ConstraintBacked: true, Columns: key(bad, "mysql")}, true},
		{"non-unique is a lookup structure", &ir.Index{Name: "i", Columns: key(bad, "mysql")}, false},
		{"same family", &ir.Index{Name: "u", Unique: true, Columns: key(bad, "postgres")}, false},
		{"one-byte literal", &ir.Index{Name: "u", Unique: true, Columns: key("trim(both 'x' from s)", "mysql")}, false},
		{"plain column", &ir.Index{Name: "u", Unique: true, Columns: []ir.IndexColumn{{Column: "s"}}}, false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := RefuseDivergentTrimIndex("t", c.idx, "mysql", "Postgres")
			if (err != nil) != c.want {
				t.Fatalf("refused = %v, want %v (err %v)", err != nil, c.want, err)
			}
			if err != nil && !strings.Contains(err.Error(), "unique index") {
				t.Errorf("refusal does not name the site: %v", err)
			}
		})
	}
}
