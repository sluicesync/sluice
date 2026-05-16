// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the MySQL schema reader's expression-normalization
// helpers. These don't need a live MySQL — they cover the read-
// boundary translation that turns MySQL's stored expression form
// (backtick identifiers, charset introducers, C-style escapes) into
// portable text the IR can hand to either engine's writer.
//
// Verbatim-passthrough policy: only the dialect-decoration is
// stripped. Substantive expression text (function calls, operators,
// non-portable constructs) passes through unchanged so it fails
// loudly on the target rather than be silently rewritten.

package mysql

import (
	"database/sql"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestBitLiteralToDecimal pins the validation-rig catalog #4 fix: a
// MySQL `bit(N) DEFAULT b'…'` default is reported verbatim by
// information_schema, and emitting it as a string literal fails on
// every target. The read boundary converts the bit literal to its
// decimal value so the dialect-neutral IR holds something both the
// MySQL (→ TINYINT) and Postgres (→ BOOLEAN) writers accept.
func TestBitLiteralToDecimal(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		want   string
		wantOK bool
	}{
		{"b'0'", "b'0'", "0", true},
		{"b'1'", "b'1'", "1", true},
		{"uppercase B'1'", "B'1'", "1", true},
		{"multi-bit b'1010'", "b'1010'", "10", true},
		{"wide b'11111111'", "b'11111111'", "255", true},
		{"parenthesised (b'1')", "(b'1')", "1", true},
		{"leading/trailing space", "  b'101' ", "5", true},
		{"not a bit literal — plain int", "0", "", false},
		{"not a bit literal — string", "'b0'", "", false},
		{"empty bits", "b''", "", false},
		{"non-binary digit", "b'012'", "", false},
		{"hex literal is not a bit literal", "0x01", "", false},
		{"empty", "", "", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, ok := bitLiteralToDecimal(c.in)
			if ok != c.wantOK {
				t.Fatalf("ok = %v; want %v", ok, c.wantOK)
			}
			if got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestTranslateDefault_BitAndIntroducer pins catalog #4 (bit-literal
// default) and catalog #6 (charset introducer + backslash-escaped
// apostrophes on an expression-form default reaching a Postgres
// target). Both were the IR-expression paths that bypassed the
// read-boundary normalization applied to generated / CHECK exprs.
func TestTranslateDefault_BitAndIntroducer(t *testing.T) {
	t.Run("bit literal → decimal DefaultLiteral", func(t *testing.T) {
		got := translateDefault(sql.NullString{String: "b'0'", Valid: true}, "")
		lit, ok := got.(ir.DefaultLiteral)
		if !ok {
			t.Fatalf("got %T; want ir.DefaultLiteral", got)
		}
		if lit.Value != "0" {
			t.Errorf("Value = %q; want %q", lit.Value, "0")
		}
	})
	t.Run("bit literal even with DEFAULT_GENERATED extra", func(t *testing.T) {
		got := translateDefault(sql.NullString{String: "b'1'", Valid: true}, "DEFAULT_GENERATED")
		lit, ok := got.(ir.DefaultLiteral)
		if !ok {
			t.Fatalf("got %T; want ir.DefaultLiteral", got)
		}
		if lit.Value != "1" {
			t.Errorf("Value = %q; want %q", lit.Value, "1")
		}
	})
	t.Run("charset introducer + escaped apostrophes stripped on expression default", func(t *testing.T) {
		got := translateDefault(sql.NullString{String: `_utf8mb4\'vazio\'`, Valid: true}, "DEFAULT_GENERATED")
		exp, ok := got.(ir.DefaultExpression)
		if !ok {
			t.Fatalf("got %T; want ir.DefaultExpression", got)
		}
		if exp.Expr != `'vazio'` {
			t.Errorf("Expr = %q; want %q", exp.Expr, `'vazio'`)
		}
		if exp.Dialect != "mysql" {
			t.Errorf("Dialect = %q; want %q", exp.Dialect, "mysql")
		}
	})
	t.Run("plain literal default unaffected", func(t *testing.T) {
		got := translateDefault(sql.NullString{String: "hello", Valid: true}, "")
		lit, ok := got.(ir.DefaultLiteral)
		if !ok {
			t.Fatalf("got %T; want ir.DefaultLiteral", got)
		}
		if lit.Value != "hello" {
			t.Errorf("Value = %q; want %q", lit.Value, "hello")
		}
	})
	t.Run("NULL default → DefaultNone", func(t *testing.T) {
		got := translateDefault(sql.NullString{Valid: false}, "")
		if _, ok := got.(ir.DefaultNone); !ok {
			t.Fatalf("got %T; want ir.DefaultNone", got)
		}
	})
}

func TestStripMySQLIdentifierQuotes(t *testing.T) {
	cases := []struct{ in, want string }{
		{"qty * price", "qty * price"},
		{"`qty` * `price`", "qty * price"},
		{"(`qty` >= 0)", "(qty >= 0)"},
		{"", ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.in, func(t *testing.T) {
			if got := stripMySQLIdentifierQuotes(c.in); got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestStripMySQLCharsetIntroducers covers the _<charset>'...' prefix
// MySQL's parser inserts on every string literal in stored
// expression text. The strip is structural — it walks the string
// and only removes _<word> when followed by an apostrophe — so a
// genuine identifier or function name that happens to start with an
// underscore (rare after backtick stripping) is preserved.
func TestStripMySQLCharsetIntroducers(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			"single literal with utf8mb4 introducer",
			"status = _utf8mb4'open'",
			"status = 'open'",
		},
		{
			"latin1 introducers in IN list",
			"status in (_latin1'open',_latin1'closed',_latin1'cancelled')",
			"status in ('open','closed','cancelled')",
		},
		{
			"no introducer present",
			"qty >= 0",
			"qty >= 0",
		},
		{
			"introducer not followed by apostrophe is preserved",
			"_some_identifier + 1",
			"_some_identifier + 1",
		},
		{
			"identifier-internal underscore is preserved",
			"first_name = _utf8mb4'foo'",
			"first_name = 'foo'",
		},
		{
			"empty string",
			"",
			"",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := stripMySQLCharsetIntroducers(c.in); got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestConvertMySQLEscapedApostrophes covers the \' → ' rewrite that
// turns MySQL's stored-form delimiter escape (\'foo\') into the
// portable form ('foo') Postgres requires under
// standard_conforming_strings=on.
func TestConvertMySQLEscapedApostrophes(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			// Realistic stored form: every literal delimiter is \'.
			"escaped delimiters around literal",
			`x = \'open\'`,
			`x = 'open'`,
		},
		{
			"IN list with three escaped literals",
			`status in (\'open\',\'closed\',\'cancelled\')`,
			`status in ('open','closed','cancelled')`,
		},
		{
			"backslash without trailing apostrophe is preserved",
			`x = a\b`,
			`x = a\b`,
		},
		{
			"no backslash present",
			"x = 'abc'",
			"x = 'abc'",
		},
		{
			"no string literal present",
			`qty >= 0`,
			`qty >= 0`,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := convertMySQLEscapedApostrophes(c.in); got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestNormalizeMySQLExpressionText is the integration of the three
// normalizations above: the input is the kind of text MySQL stores
// in information_schema.check_constraints / generation_expression,
// and the output is portable SQL the writer for either engine can
// emit verbatim.
func TestNormalizeMySQLExpressionText(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			// The actual storage form for `CHECK (qty >= 0)`.
			"qty comparison",
			"(`qty` >= 0)",
			"(qty >= 0)",
		},
		{
			// The actual storage form for
			// `CHECK (status IN ('open','closed','cancelled'))`.
			// The raw string carries literal backslash-apostrophe
			// pairs, matching the bytes MySQL stores.
			"IN list with charset introducers",
			`(` + "`status`" + ` in (_latin1\'open\',_latin1\'closed\',_latin1\'cancelled\'))`,
			`(status in ('open','closed','cancelled'))`,
		},
		{
			// CONCAT with a space literal — common in generated
			// columns.
			"CONCAT with space literal",
			"concat(`first_name`,_latin1\\' \\',`last_name`)",
			"concat(first_name,' ',last_name)",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := normalizeMySQLExpressionText(c.in); got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}
