// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"database/sql"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestParseDefault_Classification pins parseDefault over the PRAGMA-REPORTED
// forms, probed on modernc (the production driver): PRAGMA table_info strips
// the OUTER parens off a parenthesised expression default, so `DEFAULT
// ('a' || 'b')` is REPORTED as `'a' || 'b'` — quote-endpointed text that the
// pre-fix endpoint check swallowed as the silently WRONG DefaultLiteral
// `a' || 'b`. The matrix covers every reported-form family × shape variant:
// well-formed literals (plain / doubled-quote interior / trailing-doubled /
// empty / backslash), quote-endpointed EXPRESSIONS (the bug class),
// non-quote expressions, keywords, blob and hex literals, the bare
// double-quoted misfeature, numerics, and the NULL spellings.
func TestParseDefault_Classification(t *testing.T) {
	lit := func(v string) ir.DefaultValue { return ir.DefaultLiteral{Value: v} }
	expr := func(e string) ir.DefaultValue { return ir.DefaultExpression{Expr: e, Dialect: "sqlite"} }
	none := ir.DefaultValue(ir.DefaultNone{})

	cases := []struct {
		reported string // exactly as PRAGMA table_info reports it
		want     ir.DefaultValue
	}{
		// Well-formed single-quoted literals → DefaultLiteral, unescaped.
		{`'abc'`, lit("abc")},
		{`'it''s'`, lit("it's")},
		{`'x'''`, lit("x'")},
		{`''`, lit("")},
		{`''''`, lit("'")},
		{`'a\b'`, lit(`a\b`)}, // SEC-1: backslash is an ordinary char in SQLite
		{`  'padded'  `, lit("padded")},

		// Quote-endpointed EXPRESSIONS (PRAGMA stripped the outer parens) —
		// the misclassification bug class. Must NEVER become literals.
		{`'a' || 'b'`, expr(`'a' || 'b'`)},
		{`'a' || 'b' || 'c'`, expr(`'a' || 'b' || 'c'`)},
		{`'it''s ' || 'q'`, expr(`'it''s ' || 'q'`)},
		{`coalesce(NULL, 'x')`, expr(`coalesce(NULL, 'x')`)},

		// Malformed / unterminated quote shapes → expression (loud on the
		// target), never a guessed literal.
		{`'a`, expr(`'a`)},
		{`'a''`, expr(`'a''`)},
		{`'''`, expr(`'''`)},
		{`'`, expr(`'`)},

		// Non-quote expressions, incl. the residual one-level paren nesting
		// PRAGMA leaves on `(('x'))`.
		{`abs(-1)`, expr(`abs(-1)`)},
		{`('x')`, expr(`('x')`)},
		{`1+2`, expr(`1+2`)},
		{`-(-1)`, expr(`-(-1)`)},

		// Keyword defaults → expression (the writers translate/wrap them).
		{`CURRENT_TIMESTAMP`, expr(`CURRENT_TIMESTAMP`)},
		{`current_timestamp`, expr(`current_timestamp`)},
		{`CURRENT_DATE`, expr(`CURRENT_DATE`)},
		{`CURRENT_TIME`, expr(`CURRENT_TIME`)},
		{`TRUE`, expr(`TRUE`)},
		{`FALSE`, expr(`FALSE`)},

		// Blob / hex-integer literals: end with (or contain) quotes but do
		// not START with one — expression, both prefix cases.
		{`x'00ff'`, expr(`x'00ff'`)},
		{`X'00FF'`, expr(`X'00FF'`)},
		{`0x1A`, expr(`0x1A`)},

		// The bare double-quoted-string misfeature (valid in DEFAULT
		// position; the parenthesised form SQLite rejects as non-constant).
		{`"misfeature"`, expr(`"misfeature"`)},

		// Numeric literals → DefaultLiteral verbatim.
		{`42`, lit("42")},
		{`-7`, lit("-7")},
		{`+3`, lit("+3")},
		{`1.5`, lit("1.5")},
		{`1e10`, lit("1e10")},

		// NULL spellings / blank → DefaultNone.
		{`NULL`, none},
		{`null`, none},
		{``, none},
		{`   `, none},
	}
	for _, tc := range cases {
		got := parseDefault(sql.NullString{String: tc.reported, Valid: true})
		if got != tc.want {
			t.Errorf("parseDefault(%q) = %#v; want %#v", tc.reported, got, tc.want)
		}
	}

	if got := parseDefault(sql.NullString{}); got != none {
		t.Errorf("parseDefault(invalid) = %#v; want DefaultNone", got)
	}
}
