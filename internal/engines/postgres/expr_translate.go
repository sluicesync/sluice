// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-dialect expression translation for the Postgres writer.
//
// Translation in sluice is a layered policy:
//
//  1. Identifier-quote / charset-introducer normalization runs at the
//     read boundary in the source engine (see normalizeMySQLExpressionText
//     in the mysql package). This strips dialect decorations on
//     identifiers and string literals so the IR carries a clean
//     expression body.
//
//  2. A small set of high-frequency operator/function rewrites runs
//     here, at the writer boundary, when an IR expression's dialect
//     tag (Column.GeneratedExprDialect / CheckConstraint.ExprDialect)
//     differs from this writer's dialect. The translation table is
//     intentionally tiny — see the v1 scope below — and only handles
//     idioms that broke real cross-engine migrations during testing.
//
//  3. Anything not matched by either pass falls through verbatim. The
//     "loud failure on the target beats silent corruption" tenet still
//     holds: an unrecognized non-portable construct surfaces as a
//     CREATE TABLE rejection on the target, not a guess at translation.
//
// v1 scope (MySQL → Postgres):
//
//   - CONCAT(a, b, ...) → (a || b || ...)
//     PG's concat() is STABLE not IMMUTABLE so it can't sit in a
//     generated column; the operator form is IMMUTABLE and matches
//     MySQL's NULL semantics (any NULL → NULL on both sides).
//
//   - JSON_UNQUOTE(JSON_EXTRACT(j, '$.path')) → (j->>'path')
//   - JSON_EXTRACT(j, '$.path')                → (j->'path')
//     The "extract a JSON value" idiom. MySQL chains UNQUOTE+EXTRACT
//     to get text out; PG has the dedicated ->/->> operators.
//
//   - IFNULL(a, b) → COALESCE(a, b)        (direct rename)
//   - IF(cond, a, b) → CASE WHEN cond THEN a ELSE b END
//
// v0.8.0 added bool-idiom rewrites for CHECK / generated columns
// referencing tinyint(1)→BOOLEAN-mapped columns. These run only when
// the caller supplies an [ExprContext] naming the bool-mapped columns;
// see ExprContext and rewriteBoolIdioms.
//
// v0.11.0 added the first batch from the translator coverage catalog
// (docs/dev/translator-coverage.md): the highest-priority MySQL→PG
// rewrites for constructs that show up in real DDL bodies but were
// previously falling through to verbatim-passthrough and surfacing as
// CREATE TABLE rejections on the target.
//
//   - NOW() / CURRENT_TIMESTAMP() / LOCALTIMESTAMP() / LOCALTIME()
//     → CURRENT_TIMESTAMP / LOCALTIMESTAMP (bare keyword, no parens)
//   - UNIX_TIMESTAMP(x) → EXTRACT(EPOCH FROM x)::bigint
//   - UNIX_TIMESTAMP() → EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::bigint
//   - FROM_UNIXTIME(x) → TO_TIMESTAMP(x)        (single-arg form only)
//   - CHAR_LENGTH(x) / CHARACTER_LENGTH(x) → LENGTH(x)
//   - LCASE(x) → LOWER(x)
//   - UCASE(x) → UPPER(x)
//   - SUBSTR(x, …) / MID(x, …) → SUBSTRING(x, …)
//
// v0.11.1 added the second batch — smaller-but-frequent constructs
// from the catalog's high-priority and medium-priority tiers:
//
//   - RAND() → RANDOM()                  (argless only; seed form passes through)
//   - UUID() → gen_random_uuid()         (PG 13+ baseline assumed)
//   - ISNULL(x) → (x IS NULL)            (function form; bool result)
//   - REGEXP_REPLACE(x, p, r) → REGEXP_REPLACE(x, p, r, 'g')
//                                        (PG defaults to first-match, MySQL to all-match)
//   - INSTR(s, sub) → STRPOS(s, sub)
//   - LOCATE(sub, s) → STRPOS(s, sub)    (arg-swap; 3-arg form passes through)
//   - DATE_ADD(d, INTERVAL n unit) → (d + INTERVAL 'n unit')
//   - DATE_SUB(d, INTERVAL n unit) → (d - INTERVAL 'n unit')
//                                        (singular units only; compound units pass through)
//   - DATE_FORMAT(x, '<fmt>') → TO_CHAR(x, '<pg_fmt>')
//                                        (format-string token mapping; loud failure on unknown tokens)
//
// See ADR-0016 for the design rationale.

package postgres

import (
	"strings"
	"unicode"
)

// ExprContext carries the table-level information rewrite passes need
// in order to detect dialect idioms that depend on column-type
// context — specifically the bool-idiom rewrite added in v0.8.0,
// which can't tell `0 = is_active` from `0 = qty` without knowing
// which identifiers refer to bool-mapped columns. Existing rewrites
// (CONCAT, IFNULL, IF, JSON_*) are context-free and ignore it.
//
// Pass an empty value (zero-value ExprContext) when no context-aware
// rewrites should fire — the IR's existing tests build expressions
// without a table around them, and the rewrites that need context
// are gated on a non-empty BoolColumns map.
type ExprContext struct {
	// BoolColumns is the set of unquoted column names in the table
	// being emitted whose IR type is ir.Boolean. The bool-idiom
	// rewrite uses this to decide whether `<int_lit> [op] <ident>`
	// patterns should be rewritten.
	BoolColumns map[string]bool
	// OuterColumnIsInteger is true when the outer column being
	// emitted has an integer IR type (e.g. a generated column whose
	// `tinyint(1)` source got mapped to `smallint` via
	// --type-override). Flips the COALESCE rewrite direction
	// (Bug 17 residual): instead of converting an int literal to a
	// bool literal, the bool-returning side is cast to integer with
	// `::int` so PG's strict typing accepts the resulting
	// `coalesce(int, int)` pair. False (the default) preserves the
	// v0.8.0 / v0.9.0 bool-context behaviour.
	OuterColumnIsInteger bool
}

// dialectName is the canonical name this engine uses for the
// ExprDialect / GeneratedExprDialect tags on IR expressions. Held as
// a constant so readers and the translator stay in sync if the
// canonical name ever changes.
const dialectName = "postgres"

// translateExprForPG translates a MySQL-dialect expression into PG-
// dialect form for the v1 set of cross-engine constructs. Unrecognized
// constructs pass through verbatim — translation is additive on top of
// the existing verbatim-passthrough policy.
//
// The input is the IR's expression text, which has already been
// normalized at the read boundary (backticks stripped, charset
// introducers removed). What remains is the substantive expression
// body in MySQL dialect.
func translateExprForPG(expr string, ctx ExprContext) string {
	if expr == "" {
		return expr
	}
	// Order matters: the nested JSON_UNQUOTE(JSON_EXTRACT(...)) shape
	// must be rewritten before the bare JSON_EXTRACT(...) pattern so
	// the bare-form replacer doesn't shred the inner call.
	expr = rewriteJSONUnquoteExtract(expr)
	expr = rewriteJSONExtract(expr)
	expr = rewriteIFNULL(expr)
	expr = rewriteIF(expr)
	expr = rewriteCONCAT(expr)
	// CAST(x AS CHAR(N) [CHARSET y] [COLLATE z]) → CAST(x AS VARCHAR(N))
	// (Bug 16 residual). Drops MySQL's CHARSET / COLLATE clauses that
	// PG rejects, and switches to VARCHAR which more closely matches
	// MySQL's CAST CHAR semantics than PG's blank-padded CHAR.
	expr = rewriteCASTCharCharset(expr)
	// v0.11.0 catalog batch — see file-level doc and
	// docs/dev/translator-coverage.md for the rule sources.
	// Order: UNIX_TIMESTAMP runs before the NOW() rewrite so the
	// argless `UNIX_TIMESTAMP()` form (which contains no inner call to
	// rewrite as a NOW-equivalent) is handled by its own rule. The
	// other rules are commutative — none of them produces output that
	// looks like another rule's input.
	expr = rewriteUNIXTIMESTAMP(expr)
	expr = rewriteFROMUNIXTIME(expr)
	expr = rewriteCHARLENGTH(expr)
	expr = rewriteLCASE(expr)
	expr = rewriteUCASE(expr)
	expr = rewriteSUBSTR(expr)
	expr = rewriteMID(expr)
	expr = rewriteNOWFamily(expr)
	// v0.11.1 catalog batch — small additive rules. ISNULL runs
	// before the bool-idiom pass so `COALESCE(ISNULL(x), 0)` flows
	// through as `COALESCE((x IS NULL), 0)` and the outer COALESCE
	// rewrite picks up the bool-returning sub-expression. The other
	// rules are commutative.
	expr = rewriteINSTR(expr)
	expr = rewriteLOCATE(expr)
	expr = rewriteISNULL(expr)
	expr = rewriteRAND(expr)
	expr = rewriteREGEXPREPLACE(expr)
	expr = rewriteUUID(expr)
	expr = rewriteDATEADD(expr)
	expr = rewriteDATESUB(expr)
	expr = rewriteDATEFORMAT(expr)
	// v0.11.3 — operator-form INTERVAL rewrite. MySQL canonicalizes
	// `DATE_ADD(d, INTERVAL N UNIT)` to the operator form
	// `(d + interval N unit)` when a generated-column body is read
	// back via information_schema.generation_expression. The
	// DATE_ADD rule above never fires on that text because the
	// function call is gone. This rewrite operates on the operator
	// form directly: any `INTERVAL <int> <unit>` token sequence at
	// top level becomes `INTERVAL '<int> <unit>'` with the magnitude
	// quoted (PG's required syntax). See Bug 30.
	expr = rewriteIntervalLiteral(expr)
	// Bool-idiom rewrites run last: they need the canonical names
	// (IFNULL → COALESCE has already happened) and they're gated on
	// the caller-supplied BoolColumns set — empty means no rewrites.
	expr = rewriteBoolIdioms(expr, ctx)
	return expr
}

// rewriteCONCAT rewrites every top-level CONCAT(a, b, ...) call into
// a parenthesised || chain. CONCAT calls nested inside other CONCATs
// or other calls are also rewritten (the walker recurses into argument
// text). Single-argument CONCAT(a) collapses to (a).
func rewriteCONCAT(expr string) string {
	return rewriteFunctionCalls(expr, "CONCAT", func(args []string) string {
		if len(args) == 0 {
			return "''"
		}
		if len(args) == 1 {
			return "(" + strings.TrimSpace(args[0]) + ")"
		}
		trimmed := make([]string, len(args))
		for i, a := range args {
			trimmed[i] = strings.TrimSpace(a)
		}
		return "(" + strings.Join(trimmed, " || ") + ")"
	})
}

// rewriteIFNULL renames IFNULL(a, b) to COALESCE(a, b). PG's
// COALESCE accepts an arbitrary number of args, but MySQL's IFNULL
// is strictly two-arg; we preserve the arity.
func rewriteIFNULL(expr string) string {
	return rewriteFunctionCalls(expr, "IFNULL", func(args []string) string {
		trimmed := make([]string, len(args))
		for i, a := range args {
			trimmed[i] = strings.TrimSpace(a)
		}
		return "COALESCE(" + strings.Join(trimmed, ", ") + ")"
	})
}

// rewriteIF rewrites IF(cond, a, b) into a CASE expression. Only the
// three-arg shape is valid; anything else returns the original call
// untouched (the verbatim-passthrough fallback).
func rewriteIF(expr string) string {
	return rewriteFunctionCalls(expr, "IF", func(args []string) string {
		if len(args) != 3 {
			return ""
		}
		return "CASE WHEN " + strings.TrimSpace(args[0]) +
			" THEN " + strings.TrimSpace(args[1]) +
			" ELSE " + strings.TrimSpace(args[2]) +
			" END"
	})
}

// rewriteCASTCharCharset rewrites MySQL's
// `CAST(<expr> AS CHAR(N) [CHARSET y] [COLLATE z])` form to PG's
// `CAST(<expr> AS VARCHAR(N))`. Three aspects of the MySQL form are
// non-portable to PG:
//
//  1. **CHARSET / COLLATE clauses** are MySQL-specific decorations
//     PG's CAST grammar doesn't accept.
//  2. **CHAR(N) semantics differ.** MySQL's `CAST(x AS CHAR(N))`
//     truncates/coerces to a string of up to N characters with no
//     padding. PG's `CHAR(N)` is fixed-length and blank-padded; the
//     resulting value would carry trailing spaces, which then leak
//     into comparison and indexing behaviour. PG's `VARCHAR(N)`
//     matches MySQL's CAST semantics far better.
//  3. **Bare `CAST(x AS CHAR)`** (no length) is also handled — it
//     becomes `CAST(x AS TEXT)` since neither side has a length.
//
// Anything outside this shape (e.g. `CAST(x AS DECIMAL(10,2))`,
// `CAST(x AS DATE)`) passes through verbatim — the verbatim-passthrough
// policy plus PG's strict typing surface any non-portable cast as a
// loud error at apply time.
//
// v0.9.1 / Bug 16 residual.
func rewriteCASTCharCharset(expr string) string {
	return rewriteFunctionCalls(expr, "CAST", func(args []string) string {
		if len(args) != 1 {
			return ""
		}
		body := strings.TrimSpace(args[0])
		// Find ` AS ` at the top level (not inside parens or strings).
		asIdx := indexOfTopLevelAS(body)
		if asIdx < 0 {
			return ""
		}
		valExpr := strings.TrimSpace(body[:asIdx])
		typeSpec := strings.TrimSpace(body[asIdx+len(" AS "):])
		// Lowercase a leading word for matching; preserve the original
		// for any bits we choose to pass through verbatim.
		lower := strings.ToLower(typeSpec)
		// Strip any CHARSET or COLLATE clause from the tail; either
		// can appear, in either order, and consumes the rest of the
		// type spec for matching purposes.
		if cut := indexOfCharsetOrCollate(lower); cut >= 0 {
			typeSpec = strings.TrimSpace(typeSpec[:cut])
			lower = strings.ToLower(typeSpec)
		}
		switch {
		case strings.HasPrefix(lower, "char(") && strings.HasSuffix(lower, ")"):
			// CHAR(N) → VARCHAR(N); preserve the length token verbatim
			// so quoted/decorated forms (e.g. `char(10)`) round-trip.
			length := typeSpec[len("char(") : len(typeSpec)-1]
			return "CAST(" + valExpr + " AS VARCHAR(" + length + "))"
		case lower == "char":
			return "CAST(" + valExpr + " AS TEXT)"
		}
		return ""
	})
}

// indexOfTopLevelAS returns the byte offset of ` AS ` in s, ignoring
// occurrences inside parens or single-quoted string literals. Returns
// -1 when not found. The match is case-insensitive on the AS keyword
// and requires whitespace on both sides so it doesn't match
// identifiers like `MAS_TER` or `aSCAS`.
func indexOfTopLevelAS(s string) int {
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			i = scanStringLiteral(s, i) - 1
		case '(':
			depth++
		case ')':
			depth--
		case ' ', '\t', '\n':
			if depth != 0 {
				continue
			}
			// Look for `AS ` (case-insensitive) followed by whitespace.
			if i+3 < len(s) && (s[i+1] == 'A' || s[i+1] == 'a') && (s[i+2] == 'S' || s[i+2] == 's') && (s[i+3] == ' ' || s[i+3] == '\t' || s[i+3] == '\n') {
				return i
			}
		}
	}
	return -1
}

// indexOfCharsetOrCollate returns the byte offset of a top-level
// ` charset ` or ` collate ` clause in lower (assumed already
// lowercased), or -1 when neither is present. The clauses are
// MySQL-specific and the rewrite drops them whole.
func indexOfCharsetOrCollate(lower string) int {
	cut := -1
	if i := strings.Index(lower, " charset "); i >= 0 {
		cut = i
	}
	if i := strings.Index(lower, " collate "); i >= 0 && (cut < 0 || i < cut) {
		cut = i
	}
	return cut
}

// rewriteJSONUnquoteExtract collapses
// JSON_UNQUOTE(JSON_EXTRACT(j, '$.path')) into (j->>'path'). The
// MySQL idiom for "give me the text value at this JSON path" is
// equivalent to PG's ->> operator.
//
// Only the simple "$.path" form is matched — paths with array
// indexes or wildcards still pass through verbatim and rely on the
// loud-failure fallback.
func rewriteJSONUnquoteExtract(expr string) string {
	return rewriteFunctionCalls(expr, "JSON_UNQUOTE", func(args []string) string {
		if len(args) != 1 {
			return ""
		}
		inner := strings.TrimSpace(args[0])
		// The argument must itself be a JSON_EXTRACT(j, '$.path')
		// call. We re-parse via the same call-locator the outer
		// rewrite uses so we get the same string-aware semantics.
		j, path, ok := matchJSONExtractCall(inner)
		if !ok {
			return ""
		}
		return "(" + j + "->>'" + path + "')"
	})
}

// rewriteJSONExtract rewrites bare JSON_EXTRACT(j, '$.path') (without
// the outer JSON_UNQUOTE) into (j->'path'). The -> operator returns
// JSON, matching JSON_EXTRACT's return type.
func rewriteJSONExtract(expr string) string {
	return rewriteFunctionCalls(expr, "JSON_EXTRACT", func(args []string) string {
		if len(args) != 2 {
			return ""
		}
		j := strings.TrimSpace(args[0])
		path, ok := unquotedSimpleJSONPath(strings.TrimSpace(args[1]))
		if !ok {
			return ""
		}
		return "(" + j + "->'" + path + "')"
	})
}

// matchJSONExtractCall recognizes a JSON_EXTRACT(j, '$.path') call as
// the entire content of s (modulo surrounding whitespace) and returns
// (j, path, true). Anything else returns ok=false and the caller
// falls back to verbatim passthrough.
func matchJSONExtractCall(s string) (j, path string, ok bool) {
	s = strings.TrimSpace(s)
	const prefix = "JSON_EXTRACT"
	if !strings.HasPrefix(strings.ToUpper(s), prefix) {
		return "", "", false
	}
	rest := strings.TrimSpace(s[len(prefix):])
	if !strings.HasPrefix(rest, "(") || !strings.HasSuffix(rest, ")") {
		return "", "", false
	}
	inside := rest[1 : len(rest)-1]
	args := splitTopLevelArgs(inside)
	if len(args) != 2 {
		return "", "", false
	}
	jj := strings.TrimSpace(args[0])
	pp, ok := unquotedSimpleJSONPath(strings.TrimSpace(args[1]))
	if !ok {
		return "", "", false
	}
	return jj, pp, true
}

// unquotedSimpleJSONPath parses a literal of the form '$.path' and
// returns "path". Anything else (multi-segment paths, array indexes,
// non-literal arguments) returns ok=false; the caller falls back to
// verbatim passthrough.
func unquotedSimpleJSONPath(s string) (string, bool) {
	if len(s) < 2 || s[0] != '\'' || s[len(s)-1] != '\'' {
		return "", false
	}
	inner := s[1 : len(s)-1]
	if !strings.HasPrefix(inner, "$.") {
		return "", false
	}
	path := inner[2:]
	if path == "" {
		return "", false
	}
	// Reject paths with structural punctuation; only simple
	// alphanumeric/underscore keys are within v1 scope.
	for _, r := range path {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_':
		default:
			return "", false
		}
	}
	return path, true
}

// rewriteBoolIdioms rewrites two MySQL-side idioms that surface when
// a tinyint(1) column gets mapped to PG BOOLEAN: integer-literal
// comparisons and integer-literal coalesce defaults. PG's strict
// typing rejects `0 = bool_col` and `coalesce(bool_col, 0)`; MySQL
// accepts both via implicit coercion. The rewrite is gated on the
// identifier appearing in ctx.BoolColumns — without that context we
// don't know which integer literals are bool-coerced.
//
// Recognized patterns (op ∈ {=, !=, <>}, lit ∈ {0, 1}):
//
//   - <lit> <op> <bool_ident>
//   - <bool_ident> <op> <lit>
//   - COALESCE(<bool_ident>, <lit>)
//   - COALESCE(<lit>, <bool_ident>)
//
// `0` becomes `false` and `1` becomes `true`. Anything outside this
// set falls through verbatim — the loud-failure tenet still applies
// to the constructs the rewrite doesn't recognize.
//
// IFNULL(...) is already renamed to COALESCE(...) by an earlier pass
// in [translateExprForPG]; this rewrite only needs to look at
// COALESCE.
func rewriteBoolIdioms(expr string, ctx ExprContext) string {
	// The two coalesce paths are mutually exclusive and depend on the
	// outer column's expected type:
	//   - OuterColumnIsInteger: cast the bool-returning side to int
	//     so PG accepts coalesce(int, int). v0.9.1 / Bug 17 residual.
	//   - Otherwise (or empty context): the existing v0.8.0 / v0.9.0
	//     path converts the int literal to a bool, assuming the outer
	//     context wants bool (CHECK constraint, BOOLEAN-typed column).
	if ctx.OuterColumnIsInteger {
		expr = rewriteBoolToIntCoalesce(expr, ctx.BoolColumns)
		// The comparison rewrite is bool-context only — int-context
		// `<int_lit> = <bool_ident>` is rare and the implicit-coercion
		// path works correctly for it (PG casts the int to bool).
		// Skip it here.
		return expr
	}
	if len(ctx.BoolColumns) == 0 {
		return expr
	}
	expr = rewriteBoolCoalesce(expr, ctx.BoolColumns)
	expr = rewriteBoolComparison(expr, ctx.BoolColumns)
	return expr
}

// rewriteBoolToIntCoalesce wraps the non-literal side of a
// `COALESCE(<expr>, <int_lit>)` (or symmetric) call with `::int` so
// PG sees `coalesce(int, int)` instead of the type mismatch its
// strict typing rejects. Used for generated columns whose IR type
// is integer (e.g. a MySQL `tinyint(1)` source widened to `smallint`
// via --type-override; the body still references bool-returning sub-
// expressions but the column is integer-typed). MySQL accepts the
// bool/int mix via implicit coercion; PG rejects with an operator-
// resolution error.
//
// **Aggressive cast.** v0.9.1 / v0.9.2 gated the cast on a hand-
// coded `isBoolReturning` detector that recognised bare bool idents,
// comparisons, IS NULL/NOT NULL, keyword forms, and parenthesised
// wrappers. Real-world v0.10.0 testing kept surfacing expression
// shapes the detector missed (function calls returning bool, AND/OR
// chains, NOT prefixes, etc.). v0.10.1 drops the detector and casts
// the non-literal side unconditionally when the outer column is
// integer-typed:
//
//   - bool-returning expression → cast helps (true→1, false→0).
//   - already-integer expression → cast is a syntactic no-op.
//   - non-numeric expression → cast fails at apply time, but the
//     column was integer-typed so the body would have failed
//     anyway. Loud-failure tenet preserved.
//
// The boolCols parameter is retained for symmetry with the rest of
// the rewriteBoolIdioms surface but isn't consulted on this path —
// the column-type signal is enough.
func rewriteBoolToIntCoalesce(expr string, _ map[string]bool) string {
	return rewriteFunctionCalls(expr, "COALESCE", func(args []string) string {
		if len(args) != 2 {
			return ""
		}
		left := strings.TrimSpace(args[0])
		right := strings.TrimSpace(args[1])
		switch {
		case isBoolIntLiteral(right) && !isBoolIntLiteral(left):
			return "COALESCE(" + boolToIntCast(left) + ", " + right + ")"
		case isBoolIntLiteral(left) && !isBoolIntLiteral(right):
			return "COALESCE(" + left + ", " + boolToIntCast(right) + ")"
		}
		return ""
	})
}

// boolToIntCast returns the bool expression wrapped with `::int` in a
// way that survives whatever quoting the source had. Bare idents and
// parenthesised expressions both render as `<expr>::int`; the cast is
// associative on the left of a coalesce arg.
func boolToIntCast(expr string) string {
	if expr == "" {
		return expr
	}
	// Already parenthesised? Just append the cast — `(a = b)::int`
	// is unambiguous.
	if expr[0] == '(' && expr[len(expr)-1] == ')' {
		return expr + "::int"
	}
	// Bare ident or unparenthesised expression: wrap in parens
	// before the cast to keep the precedence right against the
	// surrounding comma.
	return "(" + expr + ")::int"
}

// rewriteBoolCoalesce rewrites two-arg `COALESCE` calls where one
// side is a bool-returning argument and the other is an int literal
// (`0` or `1`). The bool side is recognised in two flavours:
//
//   - **Bare ident** matching the table's bool-mapped column set.
//     Covers the common `COALESCE(is_active, 0)` shape from MySQL
//     `tinyint(1)` columns mapped to PG BOOLEAN.
//   - **Comparison / NULL-test sub-expression** (`a = b`, `a <> b`,
//     `a != b`, `a IS NULL`, `a IS NOT NULL`). Covers Bug 17's
//     follow-up case where the bool side is a generated-column body
//     that returns bool but isn't a direct column reference. PG's
//     strict typing rejects `COALESCE(<bool_expr>, 0)` even though
//     MySQL accepts it via implicit coercion.
//
// Multi-arg COALESCE and shapes that don't match either pattern
// fall through verbatim (loud-failure tenet).
func rewriteBoolCoalesce(expr string, boolCols map[string]bool) string {
	return rewriteFunctionCalls(expr, "COALESCE", func(args []string) string {
		if len(args) != 2 {
			return ""
		}
		left := strings.TrimSpace(args[0])
		right := strings.TrimSpace(args[1])
		leftBool := isBoolReturning(left, boolCols)
		rightBool := isBoolReturning(right, boolCols)
		switch {
		case leftBool && isBoolIntLiteral(right):
			return "COALESCE(" + left + ", " + intLitToBool(right) + ")"
		case rightBool && isBoolIntLiteral(left):
			return "COALESCE(" + intLitToBool(left) + ", " + right + ")"
		}
		return ""
	})
}

// isBoolReturning reports whether s is a top-level expression that
// returns boolean. Recognises bare bool-mapped column names and the
// comparison / NULL-test shapes that operators put inside generated-
// column bodies and CHECK constraints. The check is conservative: a
// false negative just means the rewrite doesn't fire and the loud-
// failure tenet kicks in; a false positive would silently rewrite a
// non-bool expression's int literal, which we explicitly avoid.
func isBoolReturning(s string, boolCols map[string]bool) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	// Case 1: bare identifier matching a bool-mapped column.
	if boolCols[s] {
		return true
	}
	// Strip a single layer of outer parens — operators commonly write
	// `coalesce((a = b), 0)` and the parens shouldn't defeat the
	// check.
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		inner := strings.TrimSpace(s[1 : len(s)-1])
		// Only strip if the parens are matched (not just wrapping
		// part of a larger expression). scanParenGroup confirms.
		if end, ok := scanParenGroup(s, 0); ok && end == len(s)-1 {
			s = inner
		}
	}
	// Case 2: contains a top-level comparison operator. Walks the
	// string respecting nested parens and string literals.
	if hasTopLevelCompareOp(s) {
		return true
	}
	// Case 3: top-level IS NULL / IS NOT NULL. Match
	// case-insensitively at the tail.
	upper := strings.ToUpper(s)
	if strings.HasSuffix(upper, " IS NULL") || strings.HasSuffix(upper, " IS NOT NULL") {
		return true
	}
	return false
}

// hasTopLevelCompareOp reports whether s contains a comparison
// operator at depth zero (not inside parens or string literals).
// Recognised operators: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`. Also
// recognises the keyword forms `LIKE`, `BETWEEN`, `IN`, and the
// `IS [NOT] NULL` / `IS [NOT] DISTINCT FROM` variants — all of
// which return bool. v0.9.2 expanded the operator set after Bug 17
// real-world testing surfaced cases that the v0.9.1 detector
// (which only handled `=` / `!=` / `<>`) missed.
//
// The check stays conservative on the false-positive side: operators
// inside parens (sub-expressions) don't count, and the keyword forms
// are only matched as standalone tokens, not as identifier
// substrings. False negatives just mean the bool-to-int rewrite
// doesn't fire for that particular shape, and the loud-failure tenet
// kicks in.
func hasTopLevelCompareOp(s string) bool {
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			i = scanStringLiteral(s, i) - 1 // -1 because the for loop increments
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case '=':
			if depth == 0 {
				return true
			}
		case '!':
			if depth == 0 && i+1 < len(s) && s[i+1] == '=' {
				return true
			}
		case '<':
			if depth == 0 && i+1 < len(s) {
				// `<>`, `<=`, or bare `<`.
				if s[i+1] == '>' || s[i+1] == '=' {
					return true
				}
				return true
			}
		case '>':
			if depth == 0 {
				return true
			}
		case ' ', '\t', '\n':
			if depth != 0 {
				continue
			}
			// Look for keyword-form bool operators starting at i+1.
			// Required surrounding whitespace prevents matching
			// identifier substrings like "BETWEENNESS".
			if matchKeywordOp(s, i+1) {
				return true
			}
		}
	}
	return false
}

// matchKeywordOp reports whether s[i:] starts with one of the bool-
// returning keyword operators (`LIKE`, `BETWEEN`, `IN`, `IS`),
// followed by whitespace or `(`. Case-insensitive. Used by
// [hasTopLevelCompareOp] to catch keyword forms that aren't
// punctuation operators.
func matchKeywordOp(s string, i int) bool {
	keywords := []string{"LIKE", "BETWEEN", "IN", "IS"}
	for _, kw := range keywords {
		if i+len(kw) > len(s) {
			continue
		}
		if !strings.EqualFold(s[i:i+len(kw)], kw) {
			continue
		}
		// Must be followed by whitespace or `(` (for IN-list).
		// Catches `IN (a, b)`, `LIKE 'pat%'`, `BETWEEN x AND y`,
		// `IS NULL`, `IS NOT NULL`, `IS DISTINCT FROM`.
		next := i + len(kw)
		if next >= len(s) {
			return false
		}
		switch s[next] {
		case ' ', '\t', '\n', '(':
			return true
		}
	}
	return false
}

// rewriteBoolComparison walks expr and rewrites `<int_lit> <op>
// <bool_ident>` and `<bool_ident> <op> <int_lit>` patterns where
// op ∈ {=, !=, <>} and int_lit ∈ {0, 1}. Comparisons embedded in
// string literals are skipped via the shared scanStringLiteral
// helper.
//
// The walker matches tokens in order: identifier or int literal,
// then a comparison operator, then the opposite token. If both
// sides match the bool/lit pair, the int literal is replaced with
// the corresponding bool literal. Anything else (different operator,
// non-literal RHS, ident not in BoolColumns) is emitted verbatim.
func rewriteBoolComparison(expr string, boolCols map[string]bool) string {
	var sb strings.Builder
	for i := 0; i < len(expr); {
		// String literal: copy verbatim, no rewrites inside.
		if expr[i] == '\'' {
			end := scanStringLiteral(expr, i)
			sb.WriteString(expr[i:end])
			i = end
			continue
		}
		// Try to match a token starting here.
		tok, tokLen, ok := scanToken(expr, i)
		if !ok {
			sb.WriteByte(expr[i])
			i++
			continue
		}
		// Found a token. Check whether it's part of a bool comparison.
		// We need to peek at the next non-space char(s) for a
		// comparison operator, then peek at the token on the other side.
		afterTok := i + tokLen
		opStart := afterTok
		for opStart < len(expr) && unicode.IsSpace(rune(expr[opStart])) {
			opStart++
		}
		op, opLen, opOK := scanCompareOp(expr, opStart)
		if !opOK {
			// No comparison after this token; emit it verbatim and
			// advance past it. (The token's bytes are non-string-
			// literal so single-byte advancement would've also worked,
			// but skipping the whole token is cheaper and avoids
			// re-matching it.)
			sb.WriteString(expr[i:afterTok])
			i = afterTok
			continue
		}
		rhsStart := opStart + opLen
		for rhsStart < len(expr) && unicode.IsSpace(rune(expr[rhsStart])) {
			rhsStart++
		}
		rhs, rhsLen, rhsOK := scanToken(expr, rhsStart)
		if !rhsOK {
			sb.WriteString(expr[i:afterTok])
			i = afterTok
			continue
		}
		// Decide whether (tok, op, rhs) forms a bool comparison.
		var rewritten string
		switch {
		case boolCols[tok] && isBoolIntLiteral(rhs):
			rewritten = tok + " " + op + " " + intLitToBool(rhs)
		case boolCols[rhs] && isBoolIntLiteral(tok):
			rewritten = intLitToBool(tok) + " " + op + " " + rhs
		}
		if rewritten == "" {
			sb.WriteString(expr[i:afterTok])
			i = afterTok
			continue
		}
		sb.WriteString(rewritten)
		i = rhsStart + rhsLen
	}
	return sb.String()
}

// scanToken returns the identifier or numeric-literal token at expr[i].
// Returns the token text, its length in bytes, and ok=true on a
// successful match. Identifiers must start with a letter or underscore
// and continue with identifier bytes; numeric tokens are runs of
// digits. Anything else (operators, parens, whitespace) returns
// ok=false.
func scanToken(expr string, i int) (tok string, n int, ok bool) {
	if i >= len(expr) {
		return "", 0, false
	}
	c := expr[i]
	switch {
	case (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_':
		// Identifier.
		j := i + 1
		for j < len(expr) && isIdentifierByte(expr[j]) {
			j++
		}
		return expr[i:j], j - i, true
	case c >= '0' && c <= '9':
		// Numeric literal (integer-only — bool comparisons that use
		// floats fall outside the rewrite scope and pass through).
		j := i + 1
		for j < len(expr) && expr[j] >= '0' && expr[j] <= '9' {
			j++
		}
		return expr[i:j], j - i, true
	}
	return "", 0, false
}

// scanCompareOp returns the comparison operator at expr[i] for the
// three forms the bool-idiom rewrite cares about: `=`, `!=`, `<>`.
// Other comparison operators (`<`, `>`, `<=`, `>=`) aren't part of
// the v0.8.0 rewrite set — the bug report only lists equality and
// inequality forms; expanding the set is a future-PR decision.
func scanCompareOp(expr string, i int) (op string, n int, ok bool) {
	if i >= len(expr) {
		return "", 0, false
	}
	if i+1 < len(expr) {
		two := expr[i : i+2]
		switch two {
		case "!=", "<>":
			return two, 2, true
		}
	}
	if expr[i] == '=' {
		// Single '=', not '=>' or part of a different token. PG and
		// MySQL both use bare '=' for equality (no SQL-spec '==' here).
		return "=", 1, true
	}
	return "", 0, false
}

// isBoolIntLiteral reports whether s is the literal string "0" or "1".
func isBoolIntLiteral(s string) bool {
	return s == "0" || s == "1"
}

// intLitToBool maps "0" → "false" and "1" → "true". Caller must
// ensure s is one of those via [isBoolIntLiteral].
func intLitToBool(s string) string {
	if s == "0" {
		return "false"
	}
	return "true"
}

// rewriteNOWFamily rewrites MySQL's parenthesised current-time
// functions (`NOW()`, `CURRENT_TIMESTAMP()`, `LOCALTIMESTAMP()`,
// `LOCALTIME()`) to PG's bare-keyword form. PG accepts
// `CURRENT_TIMESTAMP` / `LOCALTIMESTAMP` as keywords (no parens) and
// rejects `NOW()` outright; the bare-keyword form also matches what
// PG emits when reading back its own DEFAULTs, so the rewrite
// normalises round-trips. All four forms must be argless — the
// 1-arg precision form (`NOW(6)`) is rare in DDL and PG's keyword
// form doesn't accept precision; falls through verbatim and the
// loud-failure tenet kicks in if it surfaces.
func rewriteNOWFamily(expr string) string {
	expr = rewriteFunctionCalls(expr, "NOW", func(args []string) string {
		if len(args) != 0 {
			return ""
		}
		return "CURRENT_TIMESTAMP"
	})
	expr = rewriteFunctionCalls(expr, "CURRENT_TIMESTAMP", func(args []string) string {
		if len(args) != 0 {
			return ""
		}
		return "CURRENT_TIMESTAMP"
	})
	expr = rewriteFunctionCalls(expr, "LOCALTIMESTAMP", func(args []string) string {
		if len(args) != 0 {
			return ""
		}
		return "LOCALTIMESTAMP"
	})
	expr = rewriteFunctionCalls(expr, "LOCALTIME", func(args []string) string {
		if len(args) != 0 {
			return ""
		}
		return "LOCALTIMESTAMP"
	})
	return expr
}

// rewriteUNIXTIMESTAMP rewrites MySQL's `UNIX_TIMESTAMP(x)` to PG's
// `EXTRACT(EPOCH FROM x)::bigint`, and the bare argless
// `UNIX_TIMESTAMP()` to `EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::bigint`.
// MySQL returns an integer (or fractional decimal); PG's
// `EXTRACT(EPOCH FROM …)` returns `double precision`, so the explicit
// `::bigint` cast preserves MySQL's storable-as-integer semantics for
// generated columns. Two-arg / fractional-precision forms are out of
// scope and pass through verbatim.
//
// **Immutability caveat (catalog #2).** PG treats
// `extract(epoch from timestamp)` as `STABLE` not `IMMUTABLE`, which
// blocks STORED generated columns. The rewrite still helps for CHECK
// constraints, DEFAULTs, and VIRTUAL bodies; STORED generated bodies
// fall back to the loud-failure tenet — operator escape via
// `--expr-override`.
func rewriteUNIXTIMESTAMP(expr string) string {
	return rewriteFunctionCalls(expr, "UNIX_TIMESTAMP", func(args []string) string {
		switch len(args) {
		case 0:
			return "EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::bigint"
		case 1:
			return "EXTRACT(EPOCH FROM " + strings.TrimSpace(args[0]) + ")::bigint"
		}
		return ""
	})
}

// rewriteFROMUNIXTIME renames the single-arg `FROM_UNIXTIME(x)` to
// PG's `TO_TIMESTAMP(x)`. The two-arg form `FROM_UNIXTIME(epoch, fmt)`
// returns a formatted string in MySQL and has no clean PG equivalent;
// it falls through verbatim under the loud-failure tenet.
func rewriteFROMUNIXTIME(expr string) string {
	return rewriteFunctionCalls(expr, "FROM_UNIXTIME", func(args []string) string {
		if len(args) != 1 {
			return ""
		}
		return "TO_TIMESTAMP(" + strings.TrimSpace(args[0]) + ")"
	})
}

// rewriteCHARLENGTH renames MySQL's `CHAR_LENGTH(x)` and the
// equivalent `CHARACTER_LENGTH(x)` to PG's `LENGTH(x)`. Both engines
// have a `LENGTH` function but the semantics differ on MySQL —
// MySQL's `LENGTH` returns BYTE length while `CHAR_LENGTH` returns
// CHARACTER length. PG's `LENGTH(text)` returns characters, matching
// MySQL's `CHAR_LENGTH`. The reverse direction (`MySQL LENGTH` →
// `PG OCTET_LENGTH`) is a separate rule with different semantics and
// is not part of this batch — it requires column-type context to
// fire safely.
func rewriteCHARLENGTH(expr string) string {
	rename := func(args []string) string {
		if len(args) != 1 {
			return ""
		}
		return "LENGTH(" + strings.TrimSpace(args[0]) + ")"
	}
	expr = rewriteFunctionCalls(expr, "CHAR_LENGTH", rename)
	expr = rewriteFunctionCalls(expr, "CHARACTER_LENGTH", rename)
	return expr
}

// rewriteLCASE renames MySQL's `LCASE(x)` synonym to PG's `LOWER(x)`.
// Both engines accept `LOWER`; only MySQL accepts `LCASE`.
func rewriteLCASE(expr string) string {
	return rewriteFunctionCalls(expr, "LCASE", func(args []string) string {
		if len(args) != 1 {
			return ""
		}
		return "LOWER(" + strings.TrimSpace(args[0]) + ")"
	})
}

// rewriteUCASE renames MySQL's `UCASE(x)` synonym to PG's `UPPER(x)`.
// Both engines accept `UPPER`; only MySQL accepts `UCASE`.
func rewriteUCASE(expr string) string {
	return rewriteFunctionCalls(expr, "UCASE", func(args []string) string {
		if len(args) != 1 {
			return ""
		}
		return "UPPER(" + strings.TrimSpace(args[0]) + ")"
	})
}

// rewriteSUBSTR renames MySQL's `SUBSTR(x, …)` to PG's
// `SUBSTRING(x, …)`. PG accepts the comma-form `SUBSTRING(x, start,
// length)` so a direct rename is sufficient; the SQL-standard
// `SUBSTRING(x FROM start FOR length)` form is also valid in PG but
// the comma form round-trips MySQL's grammar without re-tokenising.
// Both 2-arg `SUBSTR(x, start)` and 3-arg `SUBSTR(x, start, length)`
// are accepted by PG's `SUBSTRING`.
func rewriteSUBSTR(expr string) string {
	return rewriteFunctionCalls(expr, "SUBSTR", func(args []string) string {
		if len(args) < 2 || len(args) > 3 {
			return ""
		}
		trimmed := make([]string, len(args))
		for i, a := range args {
			trimmed[i] = strings.TrimSpace(a)
		}
		return "SUBSTRING(" + strings.Join(trimmed, ", ") + ")"
	})
}

// rewriteMID renames MySQL's `MID(x, …)` synonym (an alias for
// `SUBSTR`) to PG's `SUBSTRING(x, …)`. Same arity rules as
// [rewriteSUBSTR].
func rewriteMID(expr string) string {
	return rewriteFunctionCalls(expr, "MID", func(args []string) string {
		if len(args) < 2 || len(args) > 3 {
			return ""
		}
		trimmed := make([]string, len(args))
		for i, a := range args {
			trimmed[i] = strings.TrimSpace(a)
		}
		return "SUBSTRING(" + strings.Join(trimmed, ", ") + ")"
	})
}

// rewriteRAND renames MySQL's `RAND()` to PG's `RANDOM()`. Both are
// VOLATILE so they can't appear in STORED generated columns on either
// side, but they're common in DEFAULT expressions for token / random-
// initial-value patterns. The 1-arg seed form (`RAND(seed)`) has no
// single-call PG equivalent — PG's `setseed()` is a separate stateful
// call — and falls through verbatim.
func rewriteRAND(expr string) string {
	return rewriteFunctionCalls(expr, "RAND", func(args []string) string {
		if len(args) != 0 {
			return ""
		}
		return "RANDOM()"
	})
}

// rewriteUUID renames MySQL's `UUID()` to PG's built-in
// `gen_random_uuid()`. Both are VOLATILE — DEFAULT-only, never
// generated. PG 13+ has `gen_random_uuid()` in core; pre-13 needs
// the `uuid-ossp` extension and a different name (`uuid_generate_v4()`).
// sluice's PG baseline is modern enough that the rewrite assumes 13+;
// if older PG support becomes a concern, gate on the same version
// check sluice already runs for capability declaration.
//
// **Note on column-level vs. expression-level UUIDs.** sluice's MySQL
// schema reader may already canonicalize a `CHAR(36)` column with
// `DEFAULT (UUID())` into the IR's `UUID` type, in which case PG's
// writer emits a `uuid` column with `DEFAULT gen_random_uuid()` via
// the type-mapping path and this expression rewrite never sees the
// `UUID()` call. The rule here covers cases where the type
// canonicalization didn't fire (text columns with UUID-shaped
// defaults, CHECK constraints referencing `UUID()`, etc.).
func rewriteUUID(expr string) string {
	return rewriteFunctionCalls(expr, "UUID", func(args []string) string {
		if len(args) != 0 {
			return ""
		}
		return "gen_random_uuid()"
	})
}

// rewriteISNULL rewrites MySQL's `ISNULL(x)` function form to PG's
// `(x IS NULL)` operator form. MySQL's function returns an integer
// (1 or 0); PG's IS NULL operator returns boolean. For CHECK
// constraints this is fine (PG promotes the bool implicitly); for
// generated columns the outer-context determines whether the bool
// result needs an integer cast — the existing v0.10.1 aggressive
// `::int` cast on `COALESCE(<bool>, <int_lit>)` picks up
// `COALESCE(ISNULL(x), 0)` automatically once this rewrite has fired.
//
// Standalone `ISNULL(x)` as the entire body of an integer-typed
// generated column would need a `(x IS NULL)::int` cast that this
// rule doesn't emit on its own; if that surfaces in real-world
// testing, add a column-context-aware wrapper here. The
// `--expr-override` (v0.10.0) escape hatch covers the case today.
//
// PG also has a non-standard `x ISNULL` operator form (alias for
// `IS NULL`) but that's the operator, not a function call; this
// rewrite only sees the function-call form.
func rewriteISNULL(expr string) string {
	return rewriteFunctionCalls(expr, "ISNULL", func(args []string) string {
		if len(args) != 1 {
			return ""
		}
		return "(" + strings.TrimSpace(args[0]) + " IS NULL)"
	})
}

// rewriteREGEXPREPLACE adds the `'g'` global-replace flag to the
// 3-arg form of `REGEXP_REPLACE` so PG's "replace first match by
// default" matches MySQL's "replace all matches by default" semantics.
// Without this flag, a generated column or CHECK using
// `REGEXP_REPLACE(name, '[0-9]+', '#')` would replace only the first
// digit-run on PG and silently produce different output from MySQL.
//
// The 4-arg MySQL form `REGEXP_REPLACE(x, pat, repl, pos)` takes a
// position argument with different semantics from PG's 4-arg
// `REGEXP_REPLACE(x, pat, repl, flags)` — falls through verbatim.
//
// **Regex-dialect caveat.** MySQL uses ICU regex; PG uses POSIX. A
// meaningful subset of patterns work the same, but lookaheads /
// lookbehinds / named captures don't translate. The rewrite handles
// the global-flag arity difference; regex semantic divergence is the
// operator's responsibility (loud-failure tenet at apply time, or
// `--expr-override` for known-divergent patterns).
func rewriteREGEXPREPLACE(expr string) string {
	return rewriteFunctionCalls(expr, "REGEXP_REPLACE", func(args []string) string {
		if len(args) != 3 {
			return ""
		}
		trimmed := make([]string, len(args))
		for i, a := range args {
			trimmed[i] = strings.TrimSpace(a)
		}
		return "REGEXP_REPLACE(" + strings.Join(trimmed, ", ") + ", 'g')"
	})
}

// rewriteINSTR renames MySQL's `INSTR(s, sub)` (haystack-then-needle)
// to PG's `STRPOS(s, sub)`. Same argument order, direct rename. Both
// return the 1-based byte position of the first occurrence, or 0 if
// not found. 0/1-arg or 3+-arg forms fall through verbatim.
func rewriteINSTR(expr string) string {
	return rewriteFunctionCalls(expr, "INSTR", func(args []string) string {
		if len(args) != 2 {
			return ""
		}
		return "STRPOS(" + strings.TrimSpace(args[0]) + ", " + strings.TrimSpace(args[1]) + ")"
	})
}

// rewriteLOCATE rewrites MySQL's `LOCATE(sub, s)` (needle-then-
// haystack — argument order is FLIPPED from `INSTR`) to PG's
// `STRPOS(s, sub)` (haystack-then-needle). The arg-swap is
// load-bearing: getting the order wrong would silently search the
// haystack inside the needle.
//
// The 3-arg form `LOCATE(sub, s, start)` (search starting at a given
// 1-based position) has no clean single-call PG equivalent — would
// need a `SUBSTRING(s FROM start) + STRPOS(...)` composition with a
// position offset to map back to the original string. Falls through
// verbatim under the loud-failure tenet; operator can use
// `--expr-override` for the rare case.
func rewriteLOCATE(expr string) string {
	return rewriteFunctionCalls(expr, "LOCATE", func(args []string) string {
		if len(args) != 2 {
			return ""
		}
		return "STRPOS(" + strings.TrimSpace(args[1]) + ", " + strings.TrimSpace(args[0]) + ")"
	})
}

// rewriteDATEADD rewrites MySQL's `DATE_ADD(d, INTERVAL n unit)` to
// PG's operator form `(d + INTERVAL 'n unit')`. The second arg is
// MySQL's interval literal grammar (`INTERVAL <expr> <unit>`), not a
// normal expression — needs the [parseMySQLInterval] helper to
// extract the count and unit, then re-emit as PG's quoted-interval
// string form.
//
// Recognised units (singular MySQL keywords): MICROSECOND, SECOND,
// MINUTE, HOUR, DAY, WEEK, MONTH, YEAR. PG accepts each of these as
// the unit name in `INTERVAL 'n unit'`. **QUARTER falls through** —
// PG doesn't accept `INTERVAL 'n quarter'`; an operator wanting that
// can use `--expr-override` with `(d + INTERVAL '3 month' * n)` or
// similar.
//
// **MySQL compound units** (`HOUR_MINUTE`, `DAY_HOUR`, etc.) take a
// string-shaped count like `'5 1'`; the parse rejects them and the
// call falls through verbatim. The operator's `--expr-override`
// covers the rare cases.
//
// **Non-literal counts** (`DATE_ADD(d, INTERVAL n_days DAY)` where
// n_days is a column) currently fall through too — PG's quoted-
// interval form needs a literal-typed expression. Could be extended
// to emit `(d + n_days * INTERVAL '1 day')` if real-world testing
// surfaces the need.
func rewriteDATEADD(expr string) string {
	return rewriteFunctionCalls(expr, "DATE_ADD", func(args []string) string {
		if len(args) != 2 {
			return ""
		}
		count, unit, ok := parseMySQLInterval(args[1])
		if !ok {
			return ""
		}
		return "(" + strings.TrimSpace(args[0]) + " + INTERVAL '" + count + " " + unit + "')"
	})
}

// rewriteDATESUB is the subtraction sibling of [rewriteDATEADD].
// Same parsing rules; emits the `-` operator instead of `+`.
func rewriteDATESUB(expr string) string {
	return rewriteFunctionCalls(expr, "DATE_SUB", func(args []string) string {
		if len(args) != 2 {
			return ""
		}
		count, unit, ok := parseMySQLInterval(args[1])
		if !ok {
			return ""
		}
		return "(" + strings.TrimSpace(args[0]) + " - INTERVAL '" + count + " " + unit + "')"
	})
}

// parseMySQLInterval parses MySQL's interval literal grammar
// (`INTERVAL <int_lit> <unit_keyword>`) from the second argument of
// DATE_ADD / DATE_SUB. Returns the count (digit string), unit
// (lowercase singular PG-acceptable keyword), and ok=true on a
// successful parse.
//
// Rejects (returns ok=false): missing INTERVAL keyword, non-integer
// count, unrecognized unit, compound units, anything trailing.
// Caller falls through verbatim.
func parseMySQLInterval(arg string) (count, unit string, ok bool) {
	s := strings.TrimSpace(arg)
	const kw = "INTERVAL"
	if len(s) < len(kw) || !strings.EqualFold(s[:len(kw)], kw) {
		return "", "", false
	}
	// Must be followed by whitespace (not "INTERVALX").
	if len(s) <= len(kw) || !isSpace(s[len(kw)]) {
		return "", "", false
	}
	rest := strings.TrimSpace(s[len(kw):])
	// Count: a run of digits.
	i := 0
	for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
		i++
	}
	if i == 0 {
		return "", "", false
	}
	count = rest[:i]
	// Whitespace separator.
	if i >= len(rest) || !isSpace(rest[i]) {
		return "", "", false
	}
	rest = strings.TrimSpace(rest[i:])
	// Unit: a single keyword token, no underscores (compound units
	// like HOUR_MINUTE rejected).
	j := 0
	for j < len(rest) {
		c := rest[j]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') {
			j++
			continue
		}
		break
	}
	if j == 0 || j != len(rest) {
		// Either no unit, or trailing junk after the unit (which
		// includes underscore-separated compound units).
		return "", "", false
	}
	unitLower := strings.ToLower(rest[:j])
	switch unitLower {
	case "microsecond", "second", "minute", "hour", "day", "week", "month", "year":
		return count, unitLower, true
	}
	return "", "", false
}

// isSpace reports whether b is an ASCII whitespace byte. Limited to
// the bytes that show up in IR-normalised expression text.
func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

// rewriteDATEFORMAT rewrites MySQL's `DATE_FORMAT(x, '<fmt>')` to
// PG's `TO_CHAR(x, '<pg_fmt>')`. The format-string translation is
// the load-bearing tricky bit — MySQL uses C-style `%X` tokens
// (`%Y`/`%m`/`%d`/`%H`/`%i`/`%s` etc.) while PG uses spelled-out
// uppercase tokens (`YYYY`/`MM`/`DD`/`HH24`/`MI`/`SS`).
//
// **Strict mode.** Any `%X` token not in the supported set causes the
// entire DATE_FORMAT call to fall through verbatim under the loud-
// failure tenet. Silent partial translation would produce a format
// string PG would render incorrectly without raising an error — much
// worse than a clean apply-time rejection.
//
// **Literal text in the format string** is wrapped in double quotes
// in the PG output (PG's TO_CHAR convention for literal characters
// inside a format pattern). Punctuation and digits pass through
// unquoted; runs of letters get a single `"..."` wrapper.
//
// **Single-quote in the format body** isn't supported — falls
// through verbatim. Date format strings rarely contain quotes; the
// rare case can use `--expr-override`.
//
// **PG immutability caveat.** PG's `TO_CHAR` is `STABLE`, not
// `IMMUTABLE` (it depends on `lc_time` and other session GUCs), so
// a STORED generated column using the rewritten output will fail
// with "generation expression is not immutable". The rewrite makes
// the cross-engine syntax valid; the immutability constraint is the
// operator's call (use VIRTUAL on PG ≥18, an immutable wrapper
// function, or `--expr-override` to drop the column).
func rewriteDATEFORMAT(expr string) string {
	return rewriteFunctionCalls(expr, "DATE_FORMAT", func(args []string) string {
		if len(args) != 2 {
			return ""
		}
		x := strings.TrimSpace(args[0])
		fmtArg := strings.TrimSpace(args[1])
		// Format must be a single-quoted string literal.
		if len(fmtArg) < 2 || fmtArg[0] != '\'' || fmtArg[len(fmtArg)-1] != '\'' {
			return ""
		}
		fmtBody := fmtArg[1 : len(fmtArg)-1]
		// Reject embedded single quotes — would need escaping logic
		// the rewriter doesn't carry.
		if strings.Contains(fmtBody, "'") {
			return ""
		}
		pgFmt, ok := translateMySQLDateFormat(fmtBody)
		if !ok {
			return ""
		}
		return "TO_CHAR(" + x + ", '" + pgFmt + "')"
	})
}

// translateMySQLDateFormat maps a MySQL `DATE_FORMAT` format-string
// body to its PG `TO_CHAR` equivalent. Returns ok=false if the
// format contains a `%X` token outside [mysqlDateFormatTokens] or a
// dangling `%` at the end.
//
// Letter runs in literal positions are wrapped in double quotes so
// PG treats them as literal text rather than format patterns.
// Digits and punctuation pass through unquoted.
func translateMySQLDateFormat(format string) (pg string, ok bool) {
	var sb strings.Builder
	i := 0
	for i < len(format) {
		c := format[i]
		if c == '%' {
			if i+1 >= len(format) {
				return "", false
			}
			tok, found := mysqlDateFormatTokens[format[i+1]]
			if !found {
				return "", false
			}
			sb.WriteString(tok)
			i += 2
			continue
		}
		if isAlpha(c) {
			j := i + 1
			for j < len(format) && isAlpha(format[j]) {
				j++
			}
			sb.WriteByte('"')
			sb.WriteString(format[i:j])
			sb.WriteByte('"')
			i = j
			continue
		}
		sb.WriteByte(c)
		i++
	}
	return sb.String(), true
}

// mysqlDateFormatTokens maps the MySQL `%X` format byte to its PG
// `TO_CHAR` equivalent. Coverage targets the tokens that show up in
// real-world DATE_FORMAT calls in DDL bodies (defaults, generated
// columns, CHECK constraints):
//
//   - Year: `%Y` → YYYY (4-digit), `%y` → YY (2-digit).
//   - Month: `%m` → MM (2-digit), `%c` → FMMM (no leading zero),
//     `%M` → Month (full padded name), `%b` → Mon (3-char name).
//   - Day-of-month: `%d` → DD (2-digit), `%e` → FMDD (no padding),
//     `%j` → DDD (day-of-year).
//   - Day name: `%W` → Day (full padded), `%a` → Dy (3-char).
//   - Hour: `%H`/`%k` → HH24/FMHH24 (24-hour),
//     `%h`/`%I`/`%l` → HH12/FMHH12 (12-hour).
//   - Minute/second: `%i` → MI, `%s`/`%S` → SS.
//   - AM/PM: `%p` → AM (PG handles both AM/PM via the same token).
//   - Compound: `%T` → HH24:MI:SS, `%r` → HH12:MI:SS AM.
//   - Literal `%`: `%%` → `%` (PG TO_CHAR has no special meaning for `%`).
//
// Tokens that don't fit cleanly (`%U`, `%u`, `%V`, `%v`, `%w`, `%X`,
// `%x` for various week-numbering modes; `%D` for ordinal day suffix;
// `%f` for microseconds with different formatting) are deliberately
// omitted — the entire DATE_FORMAT call falls through verbatim if
// any of them appears, preserving the loud-failure tenet rather than
// producing silently-different output.
var mysqlDateFormatTokens = map[byte]string{
	'Y': "YYYY",
	'y': "YY",
	'm': "MM",
	'c': "FMMM",
	'M': "Month",
	'b': "Mon",
	'd': "DD",
	'e': "FMDD",
	'j': "DDD",
	'W': "Day",
	'a': "Dy",
	'H': "HH24",
	'k': "FMHH24",
	'h': "HH12",
	'I': "HH12",
	'l': "FMHH12",
	'i': "MI",
	's': "SS",
	'S': "SS",
	'p': "AM",
	'T': "HH24:MI:SS",
	'r': "HH12:MI:SS AM",
	'%': "%",
}

// isAlpha reports whether b is an ASCII letter. Used by
// [translateMySQLDateFormat] to identify literal letter runs that
// need double-quoting in PG TO_CHAR format strings.
func isAlpha(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// rewriteIntervalLiteral rewrites MySQL's operator-form interval
// literal — `INTERVAL <int> <unit>` (unquoted magnitude/unit) — to
// PG's required quoted-magnitude form `INTERVAL '<int> <unit>'`.
//
// **Why this rule exists.** MySQL's `information_schema` canonicalizes
// `DATE_ADD(d, INTERVAL 7 DAY)` to the operator form `(d + interval
// 7 day)` when a generated-column body is read back. The function-call
// rewrite (rewriteDATEADD / rewriteDATESUB) never fires on that text
// because the function call is gone. This rule operates on the
// operator form directly so the generated-column body translates
// correctly. Bug 30, surfaced by v0.11.2 real-world testing.
//
// Recognised units (singular, lowercased on output): microsecond,
// second, minute, hour, day, week, month, year. Same set as
// [parseMySQLInterval] for symmetry. Compound units (HOUR_MINUTE,
// DAY_HOUR, etc.), QUARTER (no PG equivalent), and non-literal
// magnitudes pass through verbatim under the loud-failure tenet.
//
// Non-greedy: the rewrite walks the expression once, skipping inside
// string literals via [scanStringLiteral]. Multiple `INTERVAL ...`
// occurrences in one expression all rewrite independently.
func rewriteIntervalLiteral(expr string) string {
	const kw = "INTERVAL"
	var sb strings.Builder
	i := 0
	for i < len(expr) {
		// Skip and copy through string literals verbatim.
		if expr[i] == '\'' {
			end := scanStringLiteral(expr, i)
			sb.WriteString(expr[i:end])
			i = end
			continue
		}
		// Try to match INTERVAL at position i (case-insensitive,
		// requires word boundary on the left).
		if i+len(kw) > len(expr) || !strings.EqualFold(expr[i:i+len(kw)], kw) {
			sb.WriteByte(expr[i])
			i++
			continue
		}
		if i > 0 && isIdentifierByte(expr[i-1]) {
			sb.WriteByte(expr[i])
			i++
			continue
		}
		// Word boundary on the right (non-identifier byte must follow).
		afterKw := i + len(kw)
		if afterKw >= len(expr) || isIdentifierByte(expr[afterKw]) {
			sb.WriteByte(expr[i])
			i++
			continue
		}
		// Skip whitespace between INTERVAL and the magnitude.
		j := afterKw
		for j < len(expr) && isSpace(expr[j]) {
			j++
		}
		// Magnitude: a run of digits.
		magStart := j
		for j < len(expr) && expr[j] >= '0' && expr[j] <= '9' {
			j++
		}
		if j == magStart {
			// No magnitude — pass through verbatim.
			sb.WriteByte(expr[i])
			i++
			continue
		}
		magnitude := expr[magStart:j]
		// Whitespace separator before the unit.
		if j >= len(expr) || !isSpace(expr[j]) {
			sb.WriteByte(expr[i])
			i++
			continue
		}
		for j < len(expr) && isSpace(expr[j]) {
			j++
		}
		// Unit: a run of letters.
		unitStart := j
		for j < len(expr) && isAlpha(expr[j]) {
			j++
		}
		if j == unitStart {
			sb.WriteByte(expr[i])
			i++
			continue
		}
		unit := strings.ToLower(expr[unitStart:j])
		switch unit {
		case "microsecond", "second", "minute", "hour", "day", "week", "month", "year":
			// Recognised — emit the quoted form.
			sb.WriteString("INTERVAL '")
			sb.WriteString(magnitude)
			sb.WriteByte(' ')
			sb.WriteString(unit)
			sb.WriteString("'")
			i = j
			continue
		}
		// Unrecognised unit (compound forms, QUARTER, etc.) — pass
		// through verbatim, advancing past the INTERVAL keyword only
		// so the rest of the expression keeps walking.
		sb.WriteByte(expr[i])
		i++
	}
	return sb.String()
}
