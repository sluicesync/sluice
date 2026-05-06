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
			got := translateExprForPG(c.in, ExprContext{})
			if got != c.want {
				t.Errorf("translateExprForPG(%q) =\n  got  %q\n  want %q",
					c.in, got, c.want)
			}
		})
	}
}

// TestTranslateExprForPG_BoolIdioms covers the v0.8.0 bool-idiom
// rewrites. These fire only when the caller supplies a non-empty
// BoolColumns set — without that context there's no way to tell `0
// = is_active` (a bool comparison sluice should rewrite) from `0 =
// qty` (an integer comparison sluice must leave alone).
func TestTranslateExprForPG_BoolIdioms(t *testing.T) {
	ctx := ExprContext{BoolColumns: map[string]bool{
		"is_active": true,
		"deleted":   true,
	}}
	cases := []struct {
		name string
		in   string
		want string
	}{
		// ---- Comparison-with-int-literal patterns (CHECK and
		// generated-column body shapes the bug report named).
		{
			name: "int-lit on left, bool ident on right",
			in:   "0 <> is_active",
			want: "false <> is_active",
		},
		{
			name: "bool ident on left, int-lit on right",
			in:   "is_active = 1",
			want: "is_active = true",
		},
		{
			name: "inequality with !=",
			in:   "is_active != 0",
			want: "is_active != false",
		},
		{
			name: "inequality with <>",
			in:   "deleted <> 1",
			want: "deleted <> true",
		},
		{
			name: "compound expression rewrites both occurrences",
			in:   "0 = is_active AND deleted = 1",
			want: "false = is_active AND deleted = true",
		},
		{
			name: "non-bool column is untouched",
			in:   "0 = qty",
			want: "0 = qty",
		},
		{
			name: "non-zero-or-one literal is untouched",
			in:   "is_active = 2",
			want: "is_active = 2",
		},
		{
			name: "comparison inside a string literal stays untouched",
			in:   "'is_active = 1'",
			want: "'is_active = 1'",
		},
		{
			name: "bool ident on both sides falls through verbatim",
			in:   "is_active = deleted",
			want: "is_active = deleted",
		},

		// ---- COALESCE patterns. IFNULL gets renamed first so
		// IFNULL(is_active, 0) flows in here as COALESCE(is_active, 0).
		{
			name: "coalesce(bool, 0)",
			in:   "COALESCE(is_active, 0)",
			want: "COALESCE(is_active, false)",
		},
		{
			name: "coalesce(bool, 1)",
			in:   "COALESCE(is_active, 1)",
			want: "COALESCE(is_active, true)",
		},
		{
			name: "coalesce(0, bool)",
			in:   "COALESCE(0, deleted)",
			want: "COALESCE(false, deleted)",
		},
		{
			name: "ifnull renames to coalesce and rewrites the int",
			in:   "IFNULL(is_active, 0)",
			want: "COALESCE(is_active, false)",
		},
		{
			name: "coalesce of two non-bool args is untouched",
			in:   "COALESCE(qty, 0)",
			want: "COALESCE(qty, 0)",
		},
		{
			name: "coalesce three-arg form falls through verbatim",
			in:   "COALESCE(is_active, deleted, 0)",
			want: "COALESCE(is_active, deleted, 0)",
		},
		{
			name: "lowercase coalesce",
			in:   "coalesce(is_active, 0)",
			want: "COALESCE(is_active, false)",
		},

		// ---- Empty BoolColumns disables the rewrite entirely
		// (covers the empty-context guard in rewriteBoolIdioms).
		// The rest of the cases are run with the bool-context above;
		// this one carries a separate context so it lives inline.
		{
			name: "passthrough when ident not in BoolColumns",
			in:   "0 = unknown_col",
			want: "0 = unknown_col",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := translateExprForPG(c.in, ctx)
			if got != c.want {
				t.Errorf("translateExprForPG(%q) =\n  got  %q\n  want %q",
					c.in, got, c.want)
			}
		})
	}

	// Empty-context case: same input as a positive case above, but
	// with no BoolColumns → no rewrite.
	t.Run("empty ExprContext disables bool rewrite", func(t *testing.T) {
		got := translateExprForPG("0 = is_active", ExprContext{})
		if got != "0 = is_active" {
			t.Errorf("empty-ctx translateExprForPG = %q; want passthrough", got)
		}
	})
}
