// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestRefuseUntranslatedCheckExprPG pins the v1 cross-dialect
// refuse-loudly check: a MySQL-tagged Expr that survives the
// translator with a json_extract / IF / date_format token is
// refused before the SQL fires.
func TestRefuseUntranslatedCheckExprPG(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		chk     *ir.CheckConstraint
		expr    string
		wantErr bool
	}{
		{
			name:    "untranslated-json_extract",
			chk:     &ir.CheckConstraint{Name: "orders_payload_chk", Expr: "json_extract(payload, '$.k') = 'v'", ExprDialect: "mysql"},
			expr:    "json_extract(payload, '$.k') = 'v'",
			wantErr: true,
		},
		{
			name:    "untranslated-date_format",
			chk:     &ir.CheckConstraint{Name: "orders_chk", Expr: "date_format(d, '%Y') > '2020'", ExprDialect: "mysql"},
			expr:    "date_format(d, '%Y') > '2020'",
			wantErr: true,
		},
		{
			name:    "same-dialect-passes",
			chk:     &ir.CheckConstraint{Name: "orders_chk", Expr: "qty >= 0", ExprDialect: "postgres"},
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
			chk:     &ir.CheckConstraint{Name: "orders_chk", Expr: "(payload->>'k') = 'v'", ExprDialect: "mysql"},
			expr:    "(payload->>'k') = 'v'",
			wantErr: false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			err := refuseUntranslatedCheckExprPG(c.chk, c.expr)
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

// TestCheckExprsEquivalent pins the whitespace + outer-paren
// normalization the modify-check probe uses to compare an observed
// (pg_get_constraintdef) expression against the recorded IR Expr.
func TestCheckExprsEquivalent(t *testing.T) {
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
		{"double-outer-parens", "((qty >= 0))", "qty >= 0", true},
		{"different-expr", "qty > 0", "qty >= 0", false},
		{"inner-parens-not-stripped", "(qty + 1) >= 0", "qty + 1 >= 0", false}, // (x+1)>=0 != x+1>=0
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := checkExprsEquivalent(c.observed, c.recorded); got != c.want {
				t.Errorf("checkExprsEquivalent(%q, %q) = %v, want %v", c.observed, c.recorded, got, c.want)
			}
		})
	}
}
