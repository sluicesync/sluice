// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "testing"

// TestTranslateExprForMySQL covers the v1 set of PG → MySQL cross-
// engine rewrites. Each row in the table maps to one entry in the
// translation policy listed in the file-level doc on
// expr_translate.go.
func TestTranslateExprForMySQL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		// ---- (expr)::type → CAST(expr AS <mysql-type>) ----
		{
			name: "cast on parenthesised expression",
			in:   "(qty)::numeric",
			want: "CAST(qty AS DECIMAL)",
		},
		{
			name: "cast with precision modifier",
			in:   "(qty)::numeric(10,2)",
			want: "CAST(qty AS DECIMAL(10,2))",
		},
		{
			name: "cast on bare identifier",
			in:   "qty::int",
			want: "CAST(qty AS SIGNED)",
		},
		{
			name: "cast text → CHAR",
			in:   "name::text",
			want: "CAST(name AS CHAR)",
		},
		{
			name: "cast varchar with modifier",
			in:   "name::varchar(50)",
			want: "CAST(name AS CHAR(50))",
		},
		{
			name: "cast bigint → SIGNED",
			in:   "id::bigint",
			want: "CAST(id AS SIGNED)",
		},
		{
			name: "cast on function-call operand",
			in:   "LOWER(email)::text",
			want: "CAST(LOWER(email) AS CHAR)",
		},
		{
			name: "unknown cast type passes through verbatim",
			in:   "x::weirdtype",
			want: "x::weirdtype",
		},
		{
			// Multi-word type names like `character varying` or
			// `double precision` need the lookahead in rewritePGCasts
			// to recognize the full name; without it, only the first
			// word would be consumed and the second would remain in
			// the output stream.
			name: "cast to character varying",
			in:   "x::character varying",
			want: "CAST(x AS CHAR)",
		},
		{
			name: "cast to double precision",
			in:   "x::double precision",
			want: "CAST(x AS DECIMAL)",
		},

		// ---- || → CONCAT(...) ----
		{
			name: "two-operand string concat",
			in:   "a || b",
			want: "CONCAT(a, b)",
		},
		{
			name: "multi-operand concat chain",
			in:   "a || ' <' || b || '>'",
			want: "CONCAT(a, ' <', b, '>')",
		},
		{
			name: "concat with cast operands collapses after cast pass",
			in:   "(qty)::numeric || ' x ' || unit_price::numeric",
			want: "CONCAT(CAST(qty AS DECIMAL), ' x ', CAST(unit_price AS DECIMAL))",
		},

		// ---- ~~ / ~~* → LIKE / LOWER(...) LIKE LOWER(...) ----
		{
			name: "tilde-tilde becomes LIKE",
			in:   "email ~~ '%@%'",
			want: "email LIKE '%@%'",
		},
		{
			name: "tilde-tilde-star becomes case-insensitive LIKE",
			in:   "email ~~* '%@%'",
			want: "LOWER(email) LIKE LOWER('%@%')",
		},

		// ---- = ANY(ARRAY[...]) → IN (...) ----
		{
			name: "= ANY(ARRAY[..]) on text values",
			in:   "status = ANY(ARRAY['open','closed'])",
			want: "status IN ('open', 'closed')",
		},
		{
			name: "= ANY with cast on the whole array",
			in:   "status = ANY(ARRAY['open'::text,'closed'::text]::text[])",
			want: "status IN ('open', 'closed')",
		},
		// PG often emits `character varying` as the cast type for
		// VARCHAR columns. The translator needs to recognize the
		// multi-word form so the per-arg cast is stripped.
		{
			name: "= ANY with character varying casts",
			in:   "(status)::text = ANY (ARRAY['open'::\"character varying\", 'closed'::\"character varying\"]::\"character varying\"[])",
			want: "CAST(status AS CHAR) IN ('open', 'closed')",
		},
		// The exact string PG 16's pg_get_expr emits for a
		// VARCHAR-column IN-list CHECK. The ARRAY is wrapped in an
		// extra `(...)` and the per-arg casts use the unquoted
		// multi-word `character varying` type.
		{
			name: "pg_get_expr IN form with parenthesised ARRAY and unquoted multi-word casts",
			in:   "((status)::text = ANY ((ARRAY['open'::character varying, 'closed'::character varying, 'cancelled'::character varying])::text[]))",
			want: "(CAST(status AS CHAR) IN ('open', 'closed', 'cancelled'))",
		},
		// pg_get_expr's canonical form for `LIKE '%@%'` is
		// `email ~~ '%@%'::text`. Both passes need to fire: the cast
		// rewriter handles the trailing `::text` (cast on a string
		// literal), then the ~~ rewriter rewrites the operator. The
		// result is mechanically MySQL-grammatical even if a touch
		// uglier than the source.
		{
			name: "pg_get_expr LIKE form with literal cast",
			in:   "email ~~ '%@%'::text",
			want: "email LIKE CAST('%@%' AS CHAR)",
		},
		// pg_get_expr's canonical form for IN-list checks wraps each
		// literal in `::text` plus a final `::text[]` on the array.
		// The = ANY rewrite strips per-arg casts so the resulting IN
		// list contains only the bare literals.
		{
			name: "pg_get_expr IN form with per-arg and array casts",
			in:   "(status)::text = ANY (ARRAY['open'::text, 'closed'::text, 'cancelled'::text])",
			want: "CAST(status AS CHAR) IN ('open', 'closed', 'cancelled')",
		},
		// pg_get_expr's canonical form for `a || '/' || b` wraps
		// every operand in `::text` casts because PG treats VARCHAR
		// arguments to || as text-coerced. The outer parens come
		// from pg_get_expr's pretty-print.
		{
			name: "pg_get_expr || form with per-operand text casts",
			in:   "((a)::text || '/'::text) || (b)::text",
			want: "CONCAT(CAST(a AS CHAR), CAST('/' AS CHAR), CAST(b AS CHAR))",
		},

		// ---- Passthrough cases ----
		{
			name: "string-literal containing :: stays untouched",
			in:   "'x::y'",
			want: "'x::y'",
		},
		{
			name: "string-literal containing ~~ stays untouched",
			in:   "'a ~~ b'",
			want: "'a ~~ b'",
		},
		{
			name: "string-literal containing CONCAT-like text stays untouched",
			in:   "'a || b'",
			want: "'a || b'",
		},
		{
			name: "empty input returns empty",
			in:   "",
			want: "",
		},
		{
			name: "no PG-isms passes verbatim",
			in:   "qty * unit_price",
			want: "qty * unit_price",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := translateExprForMySQL(c.in)
			if got != c.want {
				t.Errorf("translateExprForMySQL(%q) =\n  got  %q\n  want %q",
					c.in, got, c.want)
			}
		})
	}
}
