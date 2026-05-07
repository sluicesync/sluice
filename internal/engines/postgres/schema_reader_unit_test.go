// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestStripTypeCast covers the cast-suffix stripper used by
// translateDefault. The suffix may be a simple identifier ("text"), a
// parameterised type ("timestamp(0) without time zone"), or a
// schema-qualified one ("pg_catalog.text") — Postgres emits all of
// these in column_default, depending on the column type and version.
func TestStripTypeCast(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		// No cast — passthrough.
		{"CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"},
		{"42", "42"},
		{"'hello'", "'hello'"},

		// Simple casts.
		{"42::integer", "42"},
		{"'hello'::text", "'hello'"},
		{"'hello'::pg_catalog.text", "'hello'"},

		// Parameterised type — the case the PG→MySQL test runs into
		// when a column is declared TIMESTAMP(0).
		{
			"CURRENT_TIMESTAMP::timestamp(0) without time zone",
			"CURRENT_TIMESTAMP",
		},
		{"'19.95'::numeric(8,2)", "'19.95'"},

		// Mixed: trailing cast strips even when the prefix contains
		// `::` inside a quoted string.
		{"'a::b'::text", "'a::b'"},

		// Suffix with brackets is NOT a recognised type name; leave
		// the value alone rather than guess.
		{"ARRAY[1,2]::integer[]", "ARRAY[1,2]::integer[]"},

		// Suffix with operators / arithmetic is not a type name.
		{"(x + y)::int", "(x + y)"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.in, func(t *testing.T) {
			got := stripTypeCast(c.in)
			if got != c.want {
				t.Errorf("stripTypeCast(%q) = %q; want %q", c.in, got, c.want)
			}
		})
	}
}

// TestTranslateDefault covers the classifier that turns a Postgres
// column_default into an IR DefaultValue. It is the small wart that
// keeps engine-specific normalisation out of the IR.
func TestTranslateDefault(t *testing.T) {
	cases := []struct {
		name string
		in   sql.NullString
		auto bool
		want ir.DefaultValue
	}{
		{"null", sql.NullString{}, false, ir.DefaultNone{}},
		{"empty", sql.NullString{Valid: true, String: ""}, false, ir.DefaultNone{}},
		{
			"identity column ignores default",
			sql.NullString{Valid: true, String: "nextval('users_id_seq'::regclass)"},
			true,
			ir.DefaultNone{},
		},
		{
			"quoted string literal",
			sql.NullString{Valid: true, String: "'hello'::text"},
			false,
			ir.DefaultLiteral{Value: "hello"},
		},
		{
			"numeric literal",
			sql.NullString{Valid: true, String: "0"},
			false,
			ir.DefaultLiteral{Value: "0"},
		},
		{
			"boolean true literal",
			sql.NullString{Valid: true, String: "true"},
			false,
			ir.DefaultLiteral{Value: "true"},
		},
		{
			// PG normalises DEFAULT CURRENT_TIMESTAMP on a TIMESTAMP(0)
			// column to include a cast. After stripping, the IR sees
			// the plain function call and stores it as an expression.
			"current_timestamp with parameterised cast",
			sql.NullString{Valid: true, String: "CURRENT_TIMESTAMP::timestamp(0) without time zone"},
			false,
			ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP"},
		},
		{
			"function expression",
			sql.NullString{Valid: true, String: "now()"},
			false,
			ir.DefaultExpression{Expr: "now()"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := translateDefault(c.in, c.auto)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("translateDefault(%v, %v)\n got = %#v\nwant = %#v", c.in, c.auto, got, c.want)
			}
		})
	}
}
