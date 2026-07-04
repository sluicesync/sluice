// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRequoteMySQLReservedIdents pins the validation-rig catalog #5
// fix: the read boundary strips backtick identifier quotes for IR
// portability, which de-quotes reserved-word column names (`order`,
// `key`) and breaks the MySQL target parser (Error 1064). The writer
// re-quotes any bare token that is a MySQL reserved word.
func TestRequoteMySQLReservedIdents(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			"catalog #5 STORED shape — order reserved word",
			"if((discarded_at is null),order,NULL)",
			"if((discarded_at is null),`order`,NULL)",
		},
		{
			"catalog #5 VIRTUAL shape — key reserved word",
			"unhex(md5(key))",
			"unhex(md5(`key`))",
		},
		{
			"non-reserved identifiers pass through",
			"qty * price - discount",
			"qty * price - discount",
		},
		{
			"reserved word in call position is left alone (function name)",
			"left(name, 3)",
			"left(name, 3)",
		},
		{
			"already-backticked reserved word untouched",
			"`order` + 1",
			"`order` + 1",
		},
		{
			"reserved word inside a string literal is not requoted",
			"status = 'order placed'",
			"status = 'order placed'",
		},
		{
			"mixed: reserved + non-reserved + literal",
			"concat(`first`, ' ', last) and `key` is not null",
			// `first` already quoted; last non-reserved; key reserved →
			// requoted; the string literal ' ' untouched.
			"concat(`first`, ' ', last) and `key` is not null",
		},
		{
			"case-insensitive reserved match",
			"Order + Key",
			"`Order` + `Key`",
		},
		{
			"numeric literal not mistaken for identifier",
			"col + 100",
			"col + 100",
		},
		// ── Context-aware FROM, reverse PG→MySQL leg (v0.68.1) ───────
		// `FROM` is in mysqlReservedWords but NOT exprGrammarKeywords,
		// so without the contextual rule a grammar `FROM` (e.g. a PG
		// generated column `EXTRACT(YEAR FROM d)` translated to MySQL)
		// would be wrongly backtick-requoted and fail. A de-quoted user
		// column literally named `from` must still be requoted.
		{
			"column `from` requoted (PG→MySQL de-quoted ref)",
			"from < to",
			"`from` < `to`",
		},
		{
			"grammar FROM in EXTRACT stays bare (PG→MySQL)",
			"extract(year from ts) > 2000",
			"extract(year from ts) > 2000",
		},
		{
			"grammar FROM after DISTINCT stays bare (PG→MySQL)",
			"a is not distinct from b",
			"a is not distinct from b",
		},
		{
			"column `from` inside non-FROM call requoted",
			"coalesce(from, 0)",
			"coalesce(`from`, 0)",
		},
		{"empty", "", ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := requoteMySQLReservedIdents(c.in); got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestEmitColumnDef_GeneratedReservedWord is the end-to-end pin for
// catalog #5: a generated column whose body references a reserved-word
// column emits valid MySQL DDL (the reserved word stays backtick-
// quoted) for both STORED and VIRTUAL.
func TestEmitColumnDef_GeneratedReservedWord(t *testing.T) {
	cases := []struct {
		name string
		in   *ir.Column
		want string
	}{
		{
			name: "STORED generated referencing reserved word `order`",
			in: &ir.Column{
				Name:                 "col_1",
				Type:                 ir.Decimal{Precision: 30, Scale: 10},
				Nullable:             true,
				GeneratedExpr:        "if((discarded_at is null),order,NULL)",
				GeneratedExprDialect: "mysql",
				GeneratedStored:      true,
			},
			want: "`col_1` DECIMAL(30,10) GENERATED ALWAYS AS (if((discarded_at is null),`order`,NULL)) STORED",
		},
		{
			name: "VIRTUAL generated referencing reserved word `key`",
			in: &ir.Column{
				Name:                 "key_hash",
				Type:                 ir.Binary{Length: 16},
				GeneratedExpr:        "unhex(md5(key))",
				GeneratedExprDialect: "mysql",
				GeneratedStored:      false,
			},
			want: "`key_hash` BINARY(16) GENERATED ALWAYS AS (unhex(md5(`key`))) VIRTUAL NOT NULL",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := emitColumnDef("t", c.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestRewritePGIdentQuotes pins the writer-side source-quote
// normalization leg (ADR-0045 §4 PG→MySQL): pg_get_expr emits
// reserved-word / mixed-case column refs double-quoted, and the PG
// reader can't strip them, so the MySQL writer converts PG's
// double-quote identifier form to MySQL backticks before the requote
// leg. Without this a PG-source generated/CHECK/index/DEFAULT body
// referencing a reserved-word column emitted the broken `"`order`"`
// shape (Error 1292 on the MySQL target).
func TestRewritePGIdentQuotes(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"reserved word double-quoted → backtick", `("order" * 2)`, "(`order` * 2)"},
		{"mixed-case quoted ident → backtick", `"Mixed Case" + 1`, "`Mixed Case` + 1"},
		{"doubled-quote escape decoded to literal dquote inside backticks", `"a""b"`, "`a\"b`"},
		{"bare identifiers untouched", "qty * price - 2", "qty * price - 2"},
		{"string literal verbatim (contains a dquote)", `note = 'say "hi"'`, `note = 'say "hi"'`},
		{"multiple quoted refs", `"order" + "user"`, "`order` + `user`"},
		{"embedded backtick in quoted ident is doubled", "\"a`b\"", "`a``b`"},
		{"empty", "", ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := rewritePGIdentQuotes(c.in); got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestTranslateGeneratedExpr_PGSourceReservedWord is the end-to-end
// pin for the ADR-0045 §4 PG→MySQL leg through the public writer
// entry point: a PG-dialect generated body that references the
// reserved word `order` (double-quoted by pg_get_expr) must emit a
// well-formed MySQL backtick-quoted reference, not the `"`order`"`
// garbage that broke bulk copy with MySQL Error 1292.
func TestTranslateGeneratedExpr_PGSourceReservedWord(t *testing.T) {
	c := &ir.Column{
		Name:                 "doubled",
		GeneratedExpr:        `("order" * 2)`,
		GeneratedExprDialect: "postgres",
		Type:                 ir.Integer{},
	}
	if got, want := translateGeneratedExpr(c), "(`order` * 2)"; got != want {
		t.Errorf("translateGeneratedExpr = %q; want %q", got, want)
	}
	chk := &ir.CheckConstraint{Expr: `("order" >= 0)`, ExprDialect: "postgres"}
	if got, want := translateCheckExpr(chk), "(`order` >= 0)"; got != want {
		t.Errorf("translateCheckExpr = %q; want %q", got, want)
	}
	// D2: PG functional index body now translate+requote.
	ic := ir.IndexColumn{Expression: `(("order" + 1))`, ExpressionDialect: "postgres"}
	if got, want := translateIndexExpr(ic), "((`order` + 1))"; got != want {
		t.Errorf("translateIndexExpr = %q; want %q", got, want)
	}
}
