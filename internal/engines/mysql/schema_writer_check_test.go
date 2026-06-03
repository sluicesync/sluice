// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRefuseUntranslatedCheckExprMySQL pins the v1 cross-dialect
// refuse-loudly check on the MySQL side: a PG-tagged Expr that
// survives translateExprForMySQL with a `->>` / `::` / `~*` token
// is refused before the SQL fires.
func TestRefuseUntranslatedCheckExprMySQL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		chk     *ir.CheckConstraint
		expr    string
		wantErr bool
	}{
		{
			name:    "untranslated-json-extract-arrow",
			chk:     &ir.CheckConstraint{Name: "orders_payload_chk", Expr: "payload->>'k' = 'v'", ExprDialect: "postgres"},
			expr:    "payload->>'k' = 'v'",
			wantErr: true,
		},
		{
			name:    "untranslated-cast-operator",
			chk:     &ir.CheckConstraint{Name: "orders_chk", Expr: "(qty::numeric) > 0", ExprDialect: "postgres"},
			expr:    "(qty::numeric) > 0",
			wantErr: true,
		},
		{
			name:    "untranslated-similar-to",
			chk:     &ir.CheckConstraint{Name: "orders_chk", Expr: "name SIMILAR TO 'A%'", ExprDialect: "postgres"},
			expr:    "name similar to 'A%'",
			wantErr: true,
		},
		{
			// Bug 77: bare POSIX-regex `~` (case-sensitive). v0.85.0
			// listed only `~*`, so this reached MySQL verbatim.
			name:    "untranslated-regex-match",
			chk:     &ir.CheckConstraint{Name: "products_sku_chk", Expr: "sku ~ '^[A-Z]{3}-[0-9]{4}$'", ExprDialect: "postgres"},
			expr:    "sku ~ CAST('^[A-Z]{3}-[0-9]{4}$' AS CHAR)",
			wantErr: true,
		},
		{
			name:    "untranslated-regex-imatch",
			chk:     &ir.CheckConstraint{Name: "products_sku_chk", Expr: "sku ~* 'abc'", ExprDialect: "postgres"},
			expr:    "sku ~* 'abc'",
			wantErr: true,
		},
		{
			name:    "untranslated-regex-not-match",
			chk:     &ir.CheckConstraint{Name: "products_sku_chk", Expr: "sku !~ 'abc'", ExprDialect: "postgres"},
			expr:    "sku !~ 'abc'",
			wantErr: true,
		},
		{
			name:    "untranslated-regex-not-imatch",
			chk:     &ir.CheckConstraint{Name: "products_sku_chk", Expr: "sku !~* 'abc'", ExprDialect: "postgres"},
			expr:    "sku !~* 'abc'",
			wantErr: true,
		},
		{
			name:    "same-dialect-passes",
			chk:     &ir.CheckConstraint{Name: "orders_chk", Expr: "qty >= 0", ExprDialect: "mysql"},
			expr:    "qty >= 0",
			wantErr: false,
		},
		{
			name:    "untagged-passes",
			chk:     &ir.CheckConstraint{Name: "orders_chk", Expr: "qty >= 0"},
			expr:    "qty >= 0",
			wantErr: false,
		},
		{
			name:    "translated-cross-dialect-passes",
			chk:     &ir.CheckConstraint{Name: "orders_chk", Expr: "JSON_EXTRACT(payload, '$.k') = 'v'", ExprDialect: "postgres"},
			expr:    "JSON_EXTRACT(payload, '$.k') = 'v'",
			wantErr: false,
		},
		{
			// Bug 77 v0.85.1 regression pin: the SOURCE carries `::`
			// (PG cast) and `~~` (PG LIKE), but the translator rewrote
			// both into valid MySQL, so the OUTPUT is clean and must NOT
			// be refused. An earlier input-OR-output match false-refused
			// this on the source `::`.
			name: "translated-cast-and-like-passes",
			chk: &ir.CheckConstraint{
				Name:        "accounts_email_check",
				Expr:        "((email)::text ~~ '%@%'::text)",
				ExprDialect: "postgres",
			},
			expr:    "(CAST(email AS CHAR) LIKE CAST('%@%' AS CHAR))",
			wantErr: false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			err := refuseUntranslatedCheckExprMySQL(c.chk, c.expr)
			if c.wantErr && err == nil {
				t.Errorf("expected refuse-loudly error, got nil")
			}
			if !c.wantErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if c.wantErr && err != nil && !strings.Contains(err.Error(), "refuse loudly") {
				t.Errorf("error should contain 'refuse loudly': %v", err)
			}
		})
	}
}

// TestMySQLCheckExprsEquivalent pins the same normalization
// invariants used by the PG-side probe — MySQL's
// CHECK_CONSTRAINTS.CHECK_CLAUSE also adds outer parens.
func TestMySQLCheckExprsEquivalent(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		observed string
		recorded string
		want     bool
	}{
		{"identical", "qty >= 0", "qty >= 0", true},
		{"whitespace-collapse", "  qty  >=   0 ", "qty >= 0", true},
		{"outer-parens", "(qty >= 0)", "qty >= 0", true},
		{"different-expr", "qty > 0", "qty >= 0", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := mysqlCheckExprsEquivalent(c.observed, c.recorded); got != c.want {
				t.Errorf("mysqlCheckExprsEquivalent(%q, %q) = %v, want %v", c.observed, c.recorded, got, c.want)
			}
		})
	}
}
