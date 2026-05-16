// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestRequotePGReservedIdents pins the validation-rig catalog Bug 63
// fix: a cross-engine (source-dialect != postgres) generated / CHECK /
// index expression body arrives with the source engine's identifier
// quotes stripped, which de-quotes reserved-word column names
// (`order`, `key`). The PG writer re-quotes any bare token that is a
// PostgreSQL reserved word so CREATE TABLE doesn't fail with SQLSTATE
// 42601, while leaving function names, operators, SQL keywords, and
// string literals untouched.
func TestRequotePGReservedIdents(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			"Bug 63 STORED shape — order reserved word",
			"(order + 1)",
			`("order" + 1)`,
		},
		{
			// `key` is NON-reserved in PostgreSQL (unlike MySQL, where
			// it IS reserved). PG accepts a bare `key` column ref, so
			// the helper must NOT quote it — quoting only the words PG
			// actually rejects keeps the emitted DDL minimal and avoids
			// over-quoting non-reserved idents.
			"non-reserved-in-PG `key` passes through (differs from MySQL set)",
			"(key * 2)",
			"(key * 2)",
		},
		{
			"Bug 63 CHECK shape — order reserved word",
			"(order > 0)",
			`("order" > 0)`,
		},
		{
			"non-reserved identifiers pass through",
			"qty * price - discount",
			"qty * price - discount",
		},
		{
			"function name in call position is left alone (coalesce)",
			"coalesce(order, 0)",
			// coalesce is reserved-as-function-name but in call
			// position; the argument `order` IS re-quoted.
			`coalesce("order", 0)`,
		},
		{
			"function name with space before paren left alone",
			"coalesce (a, b)",
			"coalesce (a, b)",
		},
		{
			"already double-quoted reserved word untouched",
			`"order" + 1`,
			`"order" + 1`,
		},
		{
			"reserved word inside a string literal is not requoted",
			"status = 'order placed'",
			"status = 'order placed'",
		},
		{
			"operator/keyword NOT quoted (AND/IS/NULL)",
			"a is not null and b > 0",
			"a is not null and b > 0",
		},
		{
			"CASE/WHEN/THEN/ELSE/END not quoted",
			"case when a > 0 then 1 else 0 end",
			"case when a > 0 then 1 else 0 end",
		},
		{
			"CAST type name not quoted",
			"cast(order as integer)",
			// `order` arg re-quoted; integer (cast type name) left.
			`cast("order" as integer)`,
		},
		{
			"mixed: reserved + non-reserved + literal",
			"order + qty and 'order' = x",
			`"order" + qty and 'order' = x`,
		},
		{
			"case-insensitive reserved match",
			"Order + User",
			`"Order" + "User"`,
		},
		{
			"numeric literal not mistaken for identifier",
			"col + 100",
			"col + 100",
		},
		{
			"table reserved word requoted",
			"table + column",
			`"table" + "column"`,
		},
		// ── Context-aware FROM (v0.68.1 regression fix) ──────────────
		// `FROM` is reserved in PG. It is grammar glue ONLY in
		// `IS [NOT] DISTINCT FROM` and the EXTRACT/SUBSTRING/TRIM/
		// OVERLAY special syntaxes; everywhere else a bare `FROM` is a
		// de-quoted user column named `from` and MUST be re-quoted. A
		// blanket grammar exclusion (the original Bug 8b fix) regressed
		// the column case → SQLSTATE 42601 on MySQL→PG.
		{
			"column `from` in CHECK requoted (regression guard)",
			"from < to",
			`"from" < "to"`,
		},
		{
			"column `from` arithmetic requoted",
			"from + to",
			`"from" + "to"`,
		},
		{
			"grammar FROM after DISTINCT stays bare (Bug 8b <=>)",
			"a IS NOT DISTINCT FROM b",
			"a IS NOT DISTINCT FROM b",
		},
		{
			"grammar FROM in EXTRACT call stays bare",
			"extract(year FROM ts) > 2000",
			"extract(year FROM ts) > 2000",
		},
		{
			"grammar FROM in SUBSTRING call stays bare",
			"substring(name FROM 2) = 'x'",
			"substring(name FROM 2) = 'x'",
		},
		{
			// The FROM (the token under test) stays bare because it is
			// inside a trim(...) call. No reserved modifier is used so
			// the case isolates the FROM-in-TRIM assertion; the
			// orthogonal reserved-modifier requoting (BOTH/LEADING/
			// TRAILING) is exercised elsewhere and is not this pin.
			"grammar FROM in TRIM call (string-literal arg) stays bare",
			"trim(' ' FROM label)",
			"trim(' ' FROM label)",
		},
		{
			"column `from` and grammar FROM mixed in one expr",
			"from > 0 AND a IS NOT DISTINCT FROM b",
			`"from" > 0 AND a IS NOT DISTINCT FROM b`,
		},
		{
			"column `from` referenced inside a non-FROM function call",
			"coalesce(from, 0)",
			`coalesce("from", 0)`,
		},
		{
			"case-insensitive grammar FROM after distinct",
			"x is not distinct from y",
			"x is not distinct from y",
		},
		{"empty", "", ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := requotePGReservedIdents(c.in); got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestTranslateGeneratedExpr_PGReservedWord is the end-to-end pin for
// Bug 63 at the emit-helper level: a cross-engine (mysql-source)
// generated column whose body references a PG-reserved-word column
// emits valid PG DDL (the reserved word becomes double-quoted), while
// a same-dialect (postgres) body is emitted verbatim — the PG reader
// already returns properly-quoted refs from pg_get_expr, so re-quoting
// it would be wrong.
func TestTranslateGeneratedExpr_PGReservedWord(t *testing.T) {
	t.Run("cross-engine mysql source requotes reserved ref", func(t *testing.T) {
		c := &ir.Column{
			Name:                 "next_order",
			Type:                 ir.Integer{Width: 32},
			GeneratedExpr:        "(order + 1)",
			GeneratedExprDialect: "mysql",
			GeneratedStored:      true,
		}
		got := translateGeneratedExpr(c, nil, emitOpts{})
		want := `("order" + 1)`
		if got != want {
			t.Errorf("\n got  %q\n want %q", got, want)
		}
	})

	t.Run("same-engine postgres source emitted verbatim", func(t *testing.T) {
		// PG reader returns pg_get_expr output with reserved refs
		// already quoted. The same-dialect branch must NOT touch it.
		c := &ir.Column{
			Name:                 "next_order",
			Type:                 ir.Integer{Width: 32},
			GeneratedExpr:        `("order" + 1)`,
			GeneratedExprDialect: "postgres",
			GeneratedStored:      true,
		}
		got := translateGeneratedExpr(c, nil, emitOpts{})
		want := `("order" + 1)`
		if got != want {
			t.Errorf("\n got  %q\n want %q", got, want)
		}
	})
}

// TestTranslateCheckExpr_PGReservedWord pins Bug 63 for the CHECK path.
func TestTranslateCheckExpr_PGReservedWord(t *testing.T) {
	t.Run("cross-engine mysql source requotes reserved ref", func(t *testing.T) {
		c := &ir.CheckConstraint{
			Name:        "rw_chk",
			Expr:        "(order > 0)",
			ExprDialect: "mysql",
		}
		got := translateCheckExpr(c, nil, emitOpts{})
		want := `("order" > 0)`
		if got != want {
			t.Errorf("\n got  %q\n want %q", got, want)
		}
	})

	t.Run("same-engine postgres source emitted verbatim", func(t *testing.T) {
		c := &ir.CheckConstraint{
			Name:        "rw_chk",
			Expr:        `("order" > 0)`,
			ExprDialect: "postgres",
		}
		got := translateCheckExpr(c, nil, emitOpts{})
		want := `("order" > 0)`
		if got != want {
			t.Errorf("\n got  %q\n want %q", got, want)
		}
	})
}

// TestTranslateIndexExpr_PGReservedWord pins Bug 63 for the index-
// expression path.
func TestTranslateIndexExpr_PGReservedWord(t *testing.T) {
	t.Run("cross-engine mysql source requotes reserved ref", func(t *testing.T) {
		c := ir.IndexColumn{
			Expression:        "(order + 1)",
			ExpressionDialect: "mysql",
		}
		got := translateIndexExpr(c, emitOpts{})
		want := `("order" + 1)`
		if got != want {
			t.Errorf("\n got  %q\n want %q", got, want)
		}
	})

	t.Run("same-engine postgres source emitted verbatim", func(t *testing.T) {
		c := ir.IndexColumn{
			Expression:        `("order" + 1)`,
			ExpressionDialect: "postgres",
		}
		got := translateIndexExpr(c, emitOpts{})
		want := `("order" + 1)`
		if got != want {
			t.Errorf("\n got  %q\n want %q", got, want)
		}
	})
}
