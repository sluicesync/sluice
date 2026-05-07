// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

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
			// v0.10.1: aggressive cast — even an already-int
			// expression gets the (no-op) `::int` wrapper.
			// Harmless syntactically; the cost is one extra `::int`
			// token in the emitted DDL. (Pre-v0.10.1 behaviour was
			// "leave alone if not bool"; the detector kept missing
			// real-world bool shapes so we switched to "trust the
			// column-type signal.")
			name: "coalesce with already-int side gets a no-op cast",
			in:   "COALESCE(qty, 0)",
			want: "COALESCE((qty)::int, 0)",
		},
		{
			name: "ifnull renames and the bool side gets cast",
			in:   "IFNULL(is_active, 0)",
			want: "COALESCE((is_active)::int, 0)",
		},

		// ---- v0.10.1: aggressive cast — drop the bool-detector
		// gate. When the outer column is integer-typed and a COALESCE
		// has 0/1 on one side, cast the OTHER side regardless of
		// detected shape. Catches the long tail of bool-returning
		// expressions that v0.9.x detectors missed (function calls,
		// AND/OR chains, NOT prefixes, etc.).
		{
			name: "function-call bool side gets cast",
			in:   "COALESCE(json_valid(payload), 0)",
			want: "COALESCE((json_valid(payload))::int, 0)",
		},
		{
			name: "AND-chain bool side gets cast",
			in:   "COALESCE(a > 0 AND b < 10, 0)",
			want: "COALESCE((a > 0 AND b < 10)::int, 0)",
		},
		{
			name: "NOT-prefix bool side gets cast",
			in:   "COALESCE(NOT is_disabled, 1)",
			want: "COALESCE((NOT is_disabled)::int, 1)",
		},
		{
			name: "EXISTS subquery bool side gets cast",
			in:   "COALESCE(EXISTS(SELECT 1 FROM peers WHERE peer_id = id), 0)",
			want: "COALESCE((EXISTS(SELECT 1 FROM peers WHERE peer_id = id))::int, 0)",
		},
		{
			name: "already-integer expression gets a no-op cast",
			in:   "COALESCE(qty + 5, 0)",
			want: "COALESCE((qty + 5)::int, 0)",
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

// TestTranslateExprForPG_V11Catalog covers the v0.11.0 batch from the
// translator coverage catalog (docs/dev/translator-coverage.md).
// Five rule families: NOW(), UNIX_TIMESTAMP/FROM_UNIXTIME,
// CHAR_LENGTH/CHARACTER_LENGTH, LCASE/UCASE, SUBSTR/MID. All are
// context-free (no ExprContext needed) and follow the established
// rewriteFunctionCalls pattern.
func TestTranslateExprForPG_V11Catalog(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		// ---- NOW() family → bare keyword ----
		{
			name: "NOW() to CURRENT_TIMESTAMP keyword",
			in:   "NOW()",
			want: "CURRENT_TIMESTAMP",
		},
		{
			name: "lowercase now() also rewrites",
			in:   "now()",
			want: "CURRENT_TIMESTAMP",
		},
		{
			name: "CURRENT_TIMESTAMP() drops the parens",
			in:   "CURRENT_TIMESTAMP()",
			want: "CURRENT_TIMESTAMP",
		},
		{
			name: "LOCALTIMESTAMP() to LOCALTIMESTAMP keyword",
			in:   "LOCALTIMESTAMP()",
			want: "LOCALTIMESTAMP",
		},
		{
			name: "LOCALTIME() to LOCALTIMESTAMP keyword",
			in:   "LOCALTIME()",
			want: "LOCALTIMESTAMP",
		},
		{
			name: "NOW with precision arg falls through verbatim",
			in:   "NOW(6)",
			want: "NOW(6)",
		},
		{
			name: "NOW() inside a larger expression",
			in:   "(created_at < NOW())",
			want: "(created_at < CURRENT_TIMESTAMP)",
		},

		// ---- UNIX_TIMESTAMP ----
		{
			name: "UNIX_TIMESTAMP with column arg",
			in:   "UNIX_TIMESTAMP(created_at)",
			want: "EXTRACT(EPOCH FROM created_at)::bigint",
		},
		{
			name: "bare UNIX_TIMESTAMP() expands to CURRENT_TIMESTAMP form",
			in:   "UNIX_TIMESTAMP()",
			want: "EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::bigint",
		},
		{
			name: "lowercase unix_timestamp",
			in:   "unix_timestamp(ts)",
			want: "EXTRACT(EPOCH FROM ts)::bigint",
		},
		{
			name: "two-arg UNIX_TIMESTAMP falls through verbatim",
			in:   "UNIX_TIMESTAMP(ts, 0)",
			want: "UNIX_TIMESTAMP(ts, 0)",
		},

		// ---- FROM_UNIXTIME ----
		{
			name: "FROM_UNIXTIME single-arg renames to TO_TIMESTAMP",
			in:   "FROM_UNIXTIME(epoch_col)",
			want: "TO_TIMESTAMP(epoch_col)",
		},
		{
			name: "FROM_UNIXTIME with format arg falls through verbatim",
			in:   "FROM_UNIXTIME(epoch, '%Y-%m-%d')",
			want: "FROM_UNIXTIME(epoch, '%Y-%m-%d')",
		},
		{
			name: "FROM_UNIXTIME composes with UNIX_TIMESTAMP",
			in:   "FROM_UNIXTIME(UNIX_TIMESTAMP(now()))",
			want: "TO_TIMESTAMP(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::bigint)",
		},

		// ---- CHAR_LENGTH / CHARACTER_LENGTH ----
		{
			name: "CHAR_LENGTH renames to LENGTH",
			in:   "CHAR_LENGTH(name)",
			want: "LENGTH(name)",
		},
		{
			name: "CHARACTER_LENGTH renames to LENGTH",
			in:   "CHARACTER_LENGTH(name)",
			want: "LENGTH(name)",
		},
		{
			name: "CHAR_LENGTH inside a CHECK comparison",
			in:   "CHAR_LENGTH(slug) >= 3",
			want: "LENGTH(slug) >= 3",
		},
		{
			name: "MySQL LENGTH (byte length) is left alone — different semantics",
			in:   "LENGTH(payload)",
			want: "LENGTH(payload)",
		},

		// ---- LCASE / UCASE ----
		{
			name: "LCASE renames to LOWER",
			in:   "LCASE(email)",
			want: "LOWER(email)",
		},
		{
			name: "UCASE renames to UPPER",
			in:   "UCASE(code)",
			want: "UPPER(code)",
		},
		{
			name: "lowercase lcase",
			in:   "lcase(email)",
			want: "LOWER(email)",
		},
		{
			name: "case-folding in a CHECK constraint shape",
			in:   "LCASE(name) = name",
			want: "LOWER(name) = name",
		},

		// ---- SUBSTR / MID ----
		{
			name: "SUBSTR three-arg renames to SUBSTRING",
			in:   "SUBSTR(name, 1, 5)",
			want: "SUBSTRING(name, 1, 5)",
		},
		{
			name: "SUBSTR two-arg also rewrites",
			in:   "SUBSTR(name, 2)",
			want: "SUBSTRING(name, 2)",
		},
		{
			name: "MID three-arg renames to SUBSTRING",
			in:   "MID(name, 1, 5)",
			want: "SUBSTRING(name, 1, 5)",
		},
		{
			name: "SUBSTR inside CONCAT composes with the CONCAT rewrite",
			in:   "CONCAT(SUBSTR(first_name, 1, 1), '. ', last_name)",
			want: "(SUBSTRING(first_name, 1, 1) || '. ' || last_name)",
		},
		{
			name: "SUBSTR with single arg falls through (PG SUBSTRING needs 2+)",
			in:   "SUBSTR(name)",
			want: "SUBSTR(name)",
		},

		// ---- Passthrough / negative cases ----
		{
			name: "string literal containing a rule name is untouched",
			in:   "'CHAR_LENGTH(x) returns chars'",
			want: "'CHAR_LENGTH(x) returns chars'",
		},
		{
			name: "identifier prefixed by a rule name is untouched",
			in:   "LCASEFOLD(x)",
			want: "LCASEFOLD(x)",
		},
		{
			name: "unrelated function falls through",
			in:   "WEIRD_FN(a, b)",
			want: "WEIRD_FN(a, b)",
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

// TestTranslateExprForPG_V11p1Catalog covers the v0.11.1 batch:
// RAND, UUID, ISNULL, REGEXP_REPLACE, INSTR/LOCATE. All context-free.
func TestTranslateExprForPG_V11p1Catalog(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		// ---- RAND() → RANDOM() ----
		{
			name: "RAND() argless to RANDOM()",
			in:   "RAND()",
			want: "RANDOM()",
		},
		{
			name: "lowercase rand() also rewrites",
			in:   "rand()",
			want: "RANDOM()",
		},
		{
			name: "RAND with seed arg falls through verbatim",
			in:   "RAND(42)",
			want: "RAND(42)",
		},

		// ---- UUID() → gen_random_uuid() ----
		{
			name: "UUID() to gen_random_uuid()",
			in:   "UUID()",
			want: "gen_random_uuid()",
		},
		{
			name: "lowercase uuid() also rewrites",
			in:   "uuid()",
			want: "gen_random_uuid()",
		},

		// ---- ISNULL(x) → (x IS NULL) ----
		{
			name: "ISNULL of bare ident",
			in:   "ISNULL(deleted_at)",
			want: "(deleted_at IS NULL)",
		},
		{
			name: "ISNULL of qualified column",
			in:   "ISNULL(t.deleted_at)",
			want: "(t.deleted_at IS NULL)",
		},
		{
			name: "lowercase isnull",
			in:   "isnull(name)",
			want: "(name IS NULL)",
		},
		{
			name: "ISNULL with extra args falls through",
			in:   "ISNULL(a, b)",
			want: "ISNULL(a, b)",
		},

		// ---- REGEXP_REPLACE 3-arg → 4-arg with 'g' flag ----
		{
			name: "REGEXP_REPLACE 3-arg adds 'g' flag",
			in:   "REGEXP_REPLACE(name, '[0-9]+', '#')",
			want: "REGEXP_REPLACE(name, '[0-9]+', '#', 'g')",
		},
		{
			name: "REGEXP_REPLACE 4-arg falls through (different MySQL semantic)",
			in:   "REGEXP_REPLACE(name, '[0-9]+', '#', 5)",
			want: "REGEXP_REPLACE(name, '[0-9]+', '#', 5)",
		},

		// ---- INSTR(s, sub) → STRPOS(s, sub) (same arg order) ----
		{
			name: "INSTR renames to STRPOS",
			in:   "INSTR(name, 'foo')",
			want: "STRPOS(name, 'foo')",
		},
		{
			name: "INSTR with 3 args falls through",
			in:   "INSTR(s, 'sub', 5)",
			want: "INSTR(s, 'sub', 5)",
		},

		// ---- LOCATE(sub, s) → STRPOS(s, sub) (arg-swap!) ----
		{
			name: "LOCATE swaps args to STRPOS form",
			in:   "LOCATE('foo', name)",
			want: "STRPOS(name, 'foo')",
		},
		{
			name: "LOCATE 3-arg form (with start position) falls through",
			in:   "LOCATE('foo', name, 5)",
			want: "LOCATE('foo', name, 5)",
		},
		{
			name: "LOCATE used in a CHECK comparison",
			in:   "LOCATE('@', email) > 0",
			want: "STRPOS(email, '@') > 0",
		},

		// ---- Composition cases ----
		{
			name: "ISNULL inside COALESCE composes via existing bool path (no ctx → no bool rewrite)",
			in:   "COALESCE(ISNULL(x), 0)",
			want: "COALESCE((x IS NULL), 0)",
		},
		{
			name: "INSTR inside CHAR_LENGTH composes both rules",
			in:   "CHAR_LENGTH(name) - INSTR(name, '_')",
			want: "LENGTH(name) - STRPOS(name, '_')",
		},

		// ---- Passthrough / negative cases ----
		{
			name: "string literal containing rule names is untouched",
			in:   "'RAND() and UUID() are random'",
			want: "'RAND() and UUID() are random'",
		},
		{
			name: "identifier prefixed by RAND is untouched",
			in:   "RANDOMIZED(x)",
			want: "RANDOMIZED(x)",
		},
		{
			name: "identifier prefixed by UUID is untouched",
			in:   "UUIDV5(ns, name)",
			want: "UUIDV5(ns, name)",
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

	// Cross-check: with int-context, COALESCE(ISNULL(x), 0) should
	// pick up the v0.10.1 aggressive cast around the IS NULL bool.
	t.Run("ISNULL inside COALESCE under int-context gets ::int cast", func(t *testing.T) {
		intCtx := ExprContext{OuterColumnIsInteger: true}
		got := translateExprForPG("COALESCE(ISNULL(x), 0)", intCtx)
		want := "COALESCE((x IS NULL)::int, 0)"
		if got != want {
			t.Errorf("translateExprForPG(int-ctx) =\n  got  %q\n  want %q", got, want)
		}
	})
}

// TestTranslateExprForPG_DateAddSub covers the DATE_ADD / DATE_SUB
// rewrites. The second arg is MySQL's `INTERVAL <n> <unit>` literal
// grammar, not a normal expression — needs the parseMySQLInterval
// helper.
func TestTranslateExprForPG_DateAddSub(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		// ---- DATE_ADD basic shapes ----
		{
			name: "DATE_ADD with DAY",
			in:   "DATE_ADD(created_at, INTERVAL 7 DAY)",
			want: "(created_at + INTERVAL '7 day')",
		},
		{
			name: "DATE_ADD with MONTH",
			in:   "DATE_ADD(created_at, INTERVAL 1 MONTH)",
			want: "(created_at + INTERVAL '1 month')",
		},
		{
			name: "DATE_ADD with YEAR",
			in:   "DATE_ADD(d, INTERVAL 5 YEAR)",
			want: "(d + INTERVAL '5 year')",
		},
		{
			name: "DATE_ADD with HOUR",
			in:   "DATE_ADD(d, INTERVAL 12 HOUR)",
			want: "(d + INTERVAL '12 hour')",
		},
		{
			name: "DATE_ADD with WEEK",
			in:   "DATE_ADD(d, INTERVAL 2 WEEK)",
			want: "(d + INTERVAL '2 week')",
		},
		{
			name: "lowercase date_add and unit",
			in:   "date_add(d, interval 7 day)",
			want: "(d + INTERVAL '7 day')",
		},
		{
			name: "DATE_ADD with CURRENT_TIMESTAMP composes with NOW family rewrite",
			in:   "DATE_ADD(NOW(), INTERVAL 1 DAY)",
			want: "(CURRENT_TIMESTAMP + INTERVAL '1 day')",
		},

		// ---- DATE_SUB basic shapes ----
		{
			name: "DATE_SUB with DAY",
			in:   "DATE_SUB(d, INTERVAL 1 DAY)",
			want: "(d - INTERVAL '1 day')",
		},
		{
			name: "DATE_SUB with MONTH",
			in:   "DATE_SUB(created_at, INTERVAL 6 MONTH)",
			want: "(created_at - INTERVAL '6 month')",
		},

		// ---- Fall-through cases ----
		{
			name: "DATE_ADD with QUARTER unit falls through (no PG INTERVAL 'n quarter')",
			in:   "DATE_ADD(d, INTERVAL 1 QUARTER)",
			want: "DATE_ADD(d, INTERVAL 1 QUARTER)",
		},
		{
			name: "DATE_ADD with compound unit (HOUR_MINUTE) falls through",
			in:   "DATE_ADD(d, INTERVAL '5 30' HOUR_MINUTE)",
			want: "DATE_ADD(d, INTERVAL '5 30' HOUR_MINUTE)",
		},
		{
			name: "DATE_ADD with non-literal count falls through",
			in:   "DATE_ADD(d, INTERVAL n_days DAY)",
			want: "DATE_ADD(d, INTERVAL n_days DAY)",
		},
		{
			name: "DATE_ADD missing INTERVAL keyword falls through",
			in:   "DATE_ADD(d, 7)",
			want: "DATE_ADD(d, 7)",
		},
		{
			name: "DATE_ADD with unrecognized unit falls through",
			in:   "DATE_ADD(d, INTERVAL 1 FORTNIGHT)",
			want: "DATE_ADD(d, INTERVAL 1 FORTNIGHT)",
		},
		{
			name: "DATE_ADD with one arg falls through",
			in:   "DATE_ADD(d)",
			want: "DATE_ADD(d)",
		},

		// ---- Composition / nesting ----
		{
			name: "DATE_ADD nested inside CHAR_LENGTH-shaped expr",
			in:   "DATE_ADD(d, INTERVAL 1 DAY) > NOW()",
			want: "(d + INTERVAL '1 day') > CURRENT_TIMESTAMP",
		},

		// ---- Passthrough / negative ----
		{
			name: "DATE_ADD inside string literal is untouched",
			in:   "'DATE_ADD(d, INTERVAL 1 DAY)'",
			want: "'DATE_ADD(d, INTERVAL 1 DAY)'",
		},
		{
			name: "identifier prefixed by DATE_ADD is untouched",
			in:   "DATE_ADDED(x)",
			want: "DATE_ADDED(x)",
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

// TestTranslateExprForPG_DateFormat covers DATE_FORMAT → TO_CHAR
// rewrites with the format-string mapping table. The strict-mode
// fall-through-on-unknown-token policy is the load-bearing safety
// rule — silently emitting wrong output is much worse than the
// operator getting a clear "function does not exist" error.
func TestTranslateExprForPG_DateFormat(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		// ---- Common date/time format strings ----
		{
			name: "ISO date %Y-%m-%d",
			in:   "DATE_FORMAT(created_at, '%Y-%m-%d')",
			want: "TO_CHAR(created_at, 'YYYY-MM-DD')",
		},
		{
			name: "ISO datetime %Y-%m-%d %H:%i:%s",
			in:   "DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s')",
			want: "TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS')",
		},
		{
			name: "Time only %H:%i:%s",
			in:   "DATE_FORMAT(t, '%H:%i:%s')",
			want: "TO_CHAR(t, 'HH24:MI:SS')",
		},
		{
			name: "12-hour with AM/PM %h:%i %p",
			in:   "DATE_FORMAT(t, '%h:%i %p')",
			want: "TO_CHAR(t, 'HH12:MI AM')",
		},
		{
			name: "Month name %M %d, %Y",
			in:   "DATE_FORMAT(d, '%M %d, %Y')",
			want: "TO_CHAR(d, 'Month DD, YYYY')",
		},
		{
			name: "Day name %W, %M %d",
			in:   "DATE_FORMAT(d, '%W, %M %d')",
			want: "TO_CHAR(d, 'Day, Month DD')",
		},
		{
			name: "Compound %T",
			in:   "DATE_FORMAT(t, '%T')",
			want: "TO_CHAR(t, 'HH24:MI:SS')",
		},
		{
			name: "lowercase date_format",
			in:   "date_format(d, '%Y-%m-%d')",
			want: "TO_CHAR(d, 'YYYY-MM-DD')",
		},

		// ---- Literal-text wrapping ----
		{
			name: "year suffix _year wraps in double quotes",
			in:   "DATE_FORMAT(d, '%Y_year')",
			want: "TO_CHAR(d, 'YYYY_\"year\"')",
		},
		{
			name: "literal Z timezone marker wraps",
			in:   "DATE_FORMAT(d, '%Y-%m-%dT%H:%i:%sZ')",
			want: "TO_CHAR(d, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
		},
		{
			name: "%% literal percent",
			in:   "DATE_FORMAT(d, '%Y%%')",
			want: "TO_CHAR(d, 'YYYY%')",
		},

		// ---- Fall-through cases ----
		{
			name: "unknown %D ordinal token falls through",
			in:   "DATE_FORMAT(d, '%D %M %Y')",
			want: "DATE_FORMAT(d, '%D %M %Y')",
		},
		{
			name: "week-numbering %u token falls through",
			in:   "DATE_FORMAT(d, '%Y week %u')",
			want: "DATE_FORMAT(d, '%Y week %u')",
		},
		{
			name: "dangling %% at end falls through",
			in:   "DATE_FORMAT(d, '%Y-%m-%')",
			want: "DATE_FORMAT(d, '%Y-%m-%')",
		},
		{
			name: "non-literal format arg (column ref) falls through",
			in:   "DATE_FORMAT(d, fmt_col)",
			want: "DATE_FORMAT(d, fmt_col)",
		},
		{
			name: "1-arg DATE_FORMAT falls through",
			in:   "DATE_FORMAT(d)",
			want: "DATE_FORMAT(d)",
		},
		{
			name: "format with embedded single quote falls through",
			in:   "DATE_FORMAT(d, 'It''s %Y')",
			want: "DATE_FORMAT(d, 'It''s %Y')",
		},

		// ---- Composition ----
		{
			name: "DATE_FORMAT inside CONCAT composes",
			in:   "CONCAT('day:', DATE_FORMAT(d, '%Y-%m-%d'))",
			want: "('day:' || TO_CHAR(d, 'YYYY-MM-DD'))",
		},
		{
			name: "DATE_FORMAT of NOW() composes with NOW family rewrite",
			in:   "DATE_FORMAT(NOW(), '%Y-%m-%d')",
			want: "TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD')",
		},

		// ---- Passthrough / negative ----
		{
			name: "DATE_FORMAT inside string literal is untouched",
			in:   "'DATE_FORMAT(d, ...)'",
			want: "'DATE_FORMAT(d, ...)'",
		},
		{
			name: "identifier prefixed by DATE_FORMAT is untouched",
			in:   "DATE_FORMATTED(x)",
			want: "DATE_FORMATTED(x)",
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

// TestTranslateExprForPG_IntervalOperatorForm covers the v0.11.3 fix
// for Bug 30: MySQL's information_schema canonicalizes
// `DATE_ADD(d, INTERVAL N DAY)` to `(d + interval N day)`. The
// function-call rewrite never sees the function call because it's
// gone; the operator-form rewrite handles the resulting INTERVAL
// literal directly.
func TestTranslateExprForPG_IntervalOperatorForm(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		// ---- The Bug 30 trigger shape ----
		{
			name: "MySQL canonicalized DATE_ADD operator form",
			in:   "(event_at + interval 7 day)",
			want: "(event_at + INTERVAL '7 day')",
		},
		{
			name: "DATE_SUB operator form",
			in:   "(event_at - interval 1 month)",
			want: "(event_at - INTERVAL '1 month')",
		},
		{
			name: "uppercase INTERVAL keyword",
			in:   "(d + INTERVAL 5 YEAR)",
			want: "(d + INTERVAL '5 year')",
		},
		{
			name: "all supported singular units rewrite",
			in:   "interval 1 microsecond, interval 1 second, interval 1 minute, interval 1 hour, interval 1 day, interval 1 week, interval 1 month, interval 1 year",
			want: "INTERVAL '1 microsecond', INTERVAL '1 second', INTERVAL '1 minute', INTERVAL '1 hour', INTERVAL '1 day', INTERVAL '1 week', INTERVAL '1 month', INTERVAL '1 year'",
		},
		{
			name: "multi-digit magnitude",
			in:   "(d + interval 365 day)",
			want: "(d + INTERVAL '365 day')",
		},

		// ---- Composes with existing rules ----
		{
			name: "DATE_ADD function form rewrites first; result has no operator-form to translate",
			in:   "DATE_ADD(d, INTERVAL 7 DAY)",
			want: "(d + INTERVAL '7 day')",
		},
		{
			name: "operator form composes with NOW family",
			in:   "(NOW() + interval 1 day)",
			want: "(CURRENT_TIMESTAMP + INTERVAL '1 day')",
		},

		// ---- Fall-through cases ----
		{
			name: "compound HOUR_MINUTE unit falls through verbatim",
			in:   "(d + interval '5 30' HOUR_MINUTE)",
			want: "(d + interval '5 30' HOUR_MINUTE)",
		},
		{
			name: "QUARTER unit falls through (no PG equivalent)",
			in:   "(d + interval 1 quarter)",
			want: "(d + interval 1 quarter)",
		},
		{
			name: "non-literal magnitude falls through",
			in:   "(d + interval n_days day)",
			want: "(d + interval n_days day)",
		},
		{
			name: "unrecognised unit falls through",
			in:   "(d + interval 1 fortnight)",
			want: "(d + interval 1 fortnight)",
		},
		{
			name: "INTERVAL inside string literal is untouched",
			in:   "'try interval 7 day in here'",
			want: "'try interval 7 day in here'",
		},
		{
			name: "identifier prefixed by INTERVAL is untouched",
			in:   "INTERVALS(x)",
			want: "INTERVALS(x)",
		},
		{
			name: "identifier suffixed by INTERVAL is untouched",
			in:   "MY_INTERVAL",
			want: "MY_INTERVAL",
		},
		{
			name: "INTERVAL not followed by magnitude+unit falls through",
			in:   "INTERVAL DAY",
			want: "INTERVAL DAY",
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
