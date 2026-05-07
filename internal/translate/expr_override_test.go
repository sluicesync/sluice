// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"errors"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/config"
	"github.com/orware/sluice/internal/ir"
)

// TestApplyExpressionOverrides_Basic exercises the happy path:
// a generated column whose body is replaced verbatim with operator-
// supplied PG text, and the dialect tag cleared so the writer's
// translator sees a same-dialect (verbatim) expression.
func TestApplyExpressionOverrides_Basic(t *testing.T) {
	src := &ir.Schema{
		Tables: []*ir.Table{
			{
				Name: "impact_items",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
					{
						Name:                 "is_cleared",
						Type:                 ir.Integer{Width: 16},
						GeneratedExpr:        "coalesce(cleared_at IS NULL, 0)",
						GeneratedExprDialect: "mysql",
						GeneratedStored:      true,
					},
				},
			},
		},
	}
	overrides := []config.ExpressionMapping{
		{
			Table:      "impact_items",
			Column:     "is_cleared",
			Expression: "coalesce((cleared_at IS NULL)::int, 0)",
		},
	}
	out, err := ApplyExpressionOverrides(src, overrides)
	if err != nil {
		t.Fatalf("ApplyExpressionOverrides: %v", err)
	}
	got := out.Tables[0].Columns[1]
	if got.GeneratedExpr != "coalesce((cleared_at IS NULL)::int, 0)" {
		t.Errorf("GeneratedExpr = %q; want override text", got.GeneratedExpr)
	}
	if got.GeneratedExprDialect != "" {
		t.Errorf("GeneratedExprDialect = %q; want cleared", got.GeneratedExprDialect)
	}
	// The non-generated id column should share its pointer with the
	// source — copy-on-write only the touched column.
	if out.Tables[0].Columns[0] != src.Tables[0].Columns[0] {
		t.Error("non-overridden column should share pointer with source")
	}
}

// TestApplyExpressionOverrides_EmptyIsNoOp confirms the fast path:
// no overrides means the schema pointer is returned unchanged.
func TestApplyExpressionOverrides_EmptyIsNoOp(t *testing.T) {
	src := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "t", Columns: []*ir.Column{{Name: "c", Type: ir.Integer{Width: 32}}}},
		},
	}
	out, err := ApplyExpressionOverrides(src, nil)
	if err != nil {
		t.Fatalf("err = %v; want nil", err)
	}
	if out != src {
		t.Error("empty overrides should return the same pointer")
	}
}

// TestApplyExpressionOverrides_UnknownTable surfaces the operator-
// typo case: a mapping references a table the schema doesn't have.
// Strict-mode error rather than silent passthrough so "why didn't
// my override fire?" doesn't waste hours.
func TestApplyExpressionOverrides_UnknownTable(t *testing.T) {
	src := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	overrides := []config.ExpressionMapping{
		{Table: "user_typo", Column: "x", Expression: "1"},
	}
	_, err := ApplyExpressionOverrides(src, overrides)
	if err == nil {
		t.Fatal("expected unknown-table error; got nil")
	}
	if !strings.Contains(err.Error(), "unknown table") {
		t.Errorf("err = %v; want a 'unknown table' message", err)
	}
}

// TestApplyExpressionOverrides_UnknownColumn covers the typo-on-
// column-name case.
func TestApplyExpressionOverrides_UnknownColumn(t *testing.T) {
	src := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "users", Columns: []*ir.Column{{Name: "id"}}},
		},
	}
	overrides := []config.ExpressionMapping{
		{Table: "users", Column: "username_typo", Expression: "1"},
	}
	_, err := ApplyExpressionOverrides(src, overrides)
	if err == nil {
		t.Fatal("expected unknown-column error; got nil")
	}
	if !strings.Contains(err.Error(), "unknown column") {
		t.Errorf("err = %v; want a 'unknown column' message", err)
	}
}

// TestApplyExpressionOverrides_NotGenerated checks the third strict-
// mode case: the column exists but isn't a generated column. Most
// likely an operator typo (override targeting a regular column when
// they meant a generated one).
func TestApplyExpressionOverrides_NotGenerated(t *testing.T) {
	src := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "users", Columns: []*ir.Column{
				{Name: "email", Type: ir.Varchar{Length: 255}},
			}},
		},
	}
	overrides := []config.ExpressionMapping{
		{Table: "users", Column: "email", Expression: "lower(email)"},
	}
	_, err := ApplyExpressionOverrides(src, overrides)
	if err == nil {
		t.Fatal("expected not-generated error; got nil")
	}
	if !strings.Contains(err.Error(), "not a generated column") {
		t.Errorf("err = %v; want a 'not a generated column' message", err)
	}
}

// TestApplyExpressionOverrides_DuplicateRejected ensures two
// overrides on the same column produce an error rather than picking
// a silent winner.
func TestApplyExpressionOverrides_DuplicateRejected(t *testing.T) {
	overrides := []config.ExpressionMapping{
		{Table: "t", Column: "c", Expression: "first"},
		{Table: "t", Column: "c", Expression: "second"},
	}
	src := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "t", Columns: []*ir.Column{
				{Name: "c", GeneratedExpr: "x"},
			}},
		},
	}
	_, err := ApplyExpressionOverrides(src, overrides)
	if err == nil {
		t.Fatal("expected duplicate-override error; got nil")
	}
	if !strings.Contains(err.Error(), "duplicate override") {
		t.Errorf("err = %v; want a 'duplicate override' message", err)
	}
}

// TestApplyExpressionOverrides_MissingFields covers the three
// required-field error paths so a half-shaped override surfaces as a
// clear validation message rather than silently doing nothing.
func TestApplyExpressionOverrides_MissingFields(t *testing.T) {
	cases := []struct {
		name string
		in   config.ExpressionMapping
		want string
	}{
		{"empty table", config.ExpressionMapping{Column: "c", Expression: "x"}, "table is required"},
		{"empty column", config.ExpressionMapping{Table: "t", Expression: "x"}, "column is required"},
		{"empty expression", config.ExpressionMapping{Table: "t", Column: "c"}, "expression is required"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := ApplyExpressionOverrides(&ir.Schema{}, []config.ExpressionMapping{c.in})
			if err == nil {
				t.Fatal("expected error; got nil")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v; want a %q message", err, c.want)
			}
		})
	}
}

// TestApplyExpressionOverrides_NilSchema defends the entry point.
func TestApplyExpressionOverrides_NilSchema(t *testing.T) {
	_, err := ApplyExpressionOverrides(nil, []config.ExpressionMapping{
		{Table: "t", Column: "c", Expression: "x"},
	})
	if !errors.Is(err, err) || err == nil {
		t.Fatal("expected error on nil schema")
	}
}
