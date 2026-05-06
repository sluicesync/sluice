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

		// ---- Bug 17 follow-up (v0.9.0): coalesce with a bool-returning
		// sub-expression instead of a bare bool ident. Covers the
		// generated-column / CHECK shapes where the bool side is a
		// comparison or IS NULL test rather than a direct column
		// reference.
		{
			name: "coalesce with parenthesised comparison rewrites to false",
			in:   "COALESCE((qty = 0), 0)",
			want: "COALESCE((qty = 0), false)",
		},
		{
			name: "coalesce with bare comparison (no parens) rewrites",
			in:   "COALESCE(qty = 0, 0)",
			want: "COALESCE(qty = 0, false)",
		},
		{
			name: "coalesce with IS NULL rewrites to false",
			in:   "COALESCE(notes IS NULL, 0)",
			want: "COALESCE(notes IS NULL, false)",
		},
		{
			name: "coalesce with IS NOT NULL rewrites to true",
			in:   "COALESCE(notes IS NOT NULL, 1)",
			want: "COALESCE(notes IS NOT NULL, true)",
		},
		{
			name: "coalesce with inequality rewrites",
			in:   "COALESCE((a <> b), 1)",
			want: "COALESCE((a <> b), true)",
		},
		{
			name: "coalesce with arithmetic expression is NOT rewritten",
			in:   "COALESCE(qty + 1, 0)",
			want: "COALESCE(qty + 1, 0)",
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

		// ---- v0.9.1 / Bug 16 residual: CAST CHAR(N) [CHARSET ...]
		// → CAST AS VARCHAR(N). Runs regardless of bool context.
		{
			name: "cast char with charset rewrites to varchar",
			in:   "cast(value as char(10) charset utf8mb4)",
			want: "CAST(value AS VARCHAR(10))",
		},
		{
			name: "cast char with charset and collate rewrites",
			in:   "cast(name as char(64) charset utf8mb4 collate utf8mb4_bin)",
			want: "CAST(name AS VARCHAR(64))",
		},
		{
			name: "cast char without charset rewrites",
			in:   "cast(qty as char(20))",
			want: "CAST(qty AS VARCHAR(20))",
		},
		{
			name: "bare cast char (no length) becomes TEXT",
			in:   "cast(x as char)",
			want: "CAST(x AS TEXT)",
		},
		{
			name: "cast to non-CHAR type passes through",
			in:   "CAST(price AS DECIMAL(10,2))",
			want: "CAST(price AS DECIMAL(10,2))",
		},
		{
			name: "CAST inside CONCAT triggers both rewrites",
			in:   "CONCAT('id-', cast(id as char(10) charset utf8mb4))",
			want: "('id-' || CAST(id AS VARCHAR(10)))",
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

// TestTranslateExprForPG_BoolToIntCoalesce covers v0.9.1 / Bug 17
// residual: when the outer column is integer-typed (e.g. a generated
// column whose tinyint(1) source got widened to smallint via
// --type-override), `coalesce(<bool>, <int_lit>)` casts the bool side
// to int instead of converting the int literal to bool. The flip is
// gated on ExprContext.OuterColumnIsInteger.
func TestTranslateExprForPG_BoolToIntCoalesce(t *testing.T) {
	intCtx := ExprContext{
		OuterColumnIsInteger: true,
		BoolColumns:          map[string]bool{"is_active": true},
	}
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "coalesce(bool ident, int) wraps bool with ::int",
			in:   "COALESCE(is_active, 0)",
			want: "COALESCE((is_active)::int, 0)",
		},
		{
			name: "coalesce(int, bool ident) wraps bool with ::int",
			in:   "COALESCE(0, is_active)",
			want: "COALESCE(0, (is_active)::int)",
		},
		{
			name: "coalesce with parenthesised bool sub-expression",
			in:   "COALESCE((qty = 0), 0)",
			want: "COALESCE((qty = 0)::int, 0)",
		},
		{
			name: "coalesce with bare comparison",
			in:   "COALESCE(qty = 0, 0)",
			want: "COALESCE((qty = 0)::int, 0)",
		},
		{
			name: "coalesce with IS NULL",
			in:   "COALESCE(notes IS NULL, 0)",
			want: "COALESCE((notes IS NULL)::int, 0)",
		},
		{
			name: "coalesce of two non-bool args is untouched",
			in:   "COALESCE(qty, 0)",
			want: "COALESCE(qty, 0)",
		},
		{
			name: "ifnull renames and the bool side gets cast",
			in:   "IFNULL(is_active, 0)",
			want: "COALESCE((is_active)::int, 0)",
		},

		// ---- v0.9.2: hasTopLevelCompareOp expanded to cover
		// `<`, `>`, `<=`, `>=`, LIKE, BETWEEN, IN, IS [NOT] NULL.
		// Each of these returns bool in MySQL/PG; previously the
		// detector only handled `=`, `!=`, `<>` and missed real-
		// world cases that surfaced in v0.9.1 testing.
		{
			name: "coalesce with > comparison",
			in:   "COALESCE(qty > 0, 0)",
			want: "COALESCE((qty > 0)::int, 0)",
		},
		{
			name: "coalesce with <= comparison",
			in:   "COALESCE(price <= max_price, 0)",
			want: "COALESCE((price <= max_price)::int, 0)",
		},
		{
			name: "coalesce with >= comparison",
			in:   "COALESCE(score >= threshold, 0)",
			want: "COALESCE((score >= threshold)::int, 0)",
		},
		{
			name: "coalesce with bare <",
			in:   "COALESCE(qty < 10, 0)",
			want: "COALESCE((qty < 10)::int, 0)",
		},
		{
			name: "coalesce with LIKE",
			in:   "COALESCE(email LIKE '%@example.com', 0)",
			want: "COALESCE((email LIKE '%@example.com')::int, 0)",
		},
		{
			name: "coalesce with BETWEEN",
			in:   "COALESCE(qty BETWEEN 1 AND 10, 0)",
			want: "COALESCE((qty BETWEEN 1 AND 10)::int, 0)",
		},
		{
			name: "coalesce with IN list",
			in:   "COALESCE(status IN ('open','pending'), 0)",
			want: "COALESCE((status IN ('open','pending'))::int, 0)",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := translateExprForPG(c.in, intCtx)
			if got != c.want {
				t.Errorf("translateExprForPG(%q) =\n  got  %q\n  want %q",
					c.in, got, c.want)
			}
		})
	}

	// Cross-check: with OuterColumnIsInteger=false (default) and a
	// bool context, the SAME input rewrites the int literal to bool
	// instead — confirms the flag flips the rewrite direction
	// without affecting the bool-context path.
	t.Run("flag off restores bool-literal direction", func(t *testing.T) {
		boolCtx := ExprContext{
			BoolColumns: map[string]bool{"is_active": true},
		}
		got := translateExprForPG("COALESCE(is_active, 0)", boolCtx)
		if got != "COALESCE(is_active, false)" {
			t.Errorf("bool-context COALESCE rewrite = %q; want bool-literal direction", got)
		}
	})
}
