package postgres

import "testing"

// TestTranslateExprForPG covers the v1 set of MySQL → PG cross-engine
// rewrites: every translation listed in the file-level doc on
// expr_translate.go gets a row, plus passthrough cases that confirm
// unrecognized constructs and string-literal contents are left alone.
func TestTranslateExprForPG(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		// ---- CONCAT(a, b, ...) → (a || b || ...) ----
		{
			name: "concat two args",
			in:   "CONCAT(a, b)",
			want: "(a || b)",
		},
		{
			name: "concat three args with literal separator",
			in:   "CONCAT(a, '/', b)",
			want: "(a || '/' || b)",
		},
		{
			name: "lowercase concat",
			in:   "concat(a, b, c)",
			want: "(a || b || c)",
		},
		{
			name: "concat with space before paren",
			in:   "CONCAT (a, b)",
			want: "(a || b)",
		},
		{
			name: "single-arg concat collapses to parens",
			in:   "CONCAT(a)",
			want: "(a)",
		},

		// ---- IFNULL(a, b) → COALESCE(a, b) ----
		{
			name: "ifnull two args",
			in:   "IFNULL(a, b)",
			want: "COALESCE(a, b)",
		},
		{
			name: "ifnull preserves arg whitespace pattern",
			in:   "IFNULL(name, 'unknown')",
			want: "COALESCE(name, 'unknown')",
		},

		// ---- IF(cond, a, b) → CASE WHEN cond THEN a ELSE b END ----
		{
			name: "if three args",
			in:   "IF(qty > 0, 'yes', 'no')",
			want: "CASE WHEN qty > 0 THEN 'yes' ELSE 'no' END",
		},

		// ---- JSON extract idioms ----
		{
			name: "json_unquote(json_extract) → ->>",
			in:   "JSON_UNQUOTE(JSON_EXTRACT(attrs, '$.color'))",
			want: "(attrs->>'color')",
		},
		{
			name: "lowercase variant of JSON helpers",
			in:   "json_unquote(json_extract(attrs, '$.color'))",
			want: "(attrs->>'color')",
		},
		{
			name: "bare json_extract → ->",
			in:   "JSON_EXTRACT(attrs, '$.color')",
			want: "(attrs->'color')",
		},

		// ---- Passthrough / fallback cases ----
		{
			name: "concat inside a string literal stays untouched",
			in:   "'CONCAT(a, b)'",
			want: "'CONCAT(a, b)'",
		},
		{
			name: "non-portable function passes through verbatim",
			in:   "WEIRD_FN(a, b)",
			want: "WEIRD_FN(a, b)",
		},
		{
			name: "empty input returns empty",
			in:   "",
			want: "",
		},
		{
			name: "json_extract with multi-segment path falls back to verbatim",
			in:   "JSON_EXTRACT(attrs, '$.a.b')",
			want: "JSON_EXTRACT(attrs, '$.a.b')",
		},
		{
			name: "ifnull does not match an unrelated identifier prefix",
			in:   "IFNULLY(a, b)",
			want: "IFNULLY(a, b)",
		},
		// Composition: concat with a column whose name is qualified.
		{
			name: "concat over a qualified column reference",
			in:   "CONCAT(t.first_name, ' ', t.last_name)",
			want: "(t.first_name || ' ' || t.last_name)",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := translateExprForPG(c.in)
			if got != c.want {
				t.Errorf("translateExprForPG(%q) =\n  got  %q\n  want %q",
					c.in, got, c.want)
			}
		})
	}
}
