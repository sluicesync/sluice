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

// rewriteBoolToIntCoalesce wraps the bool-returning side of a
// `COALESCE(<bool>, <int_lit>)` (or symmetric) call with `::int` so
// PG sees `coalesce(int, int)` instead of `coalesce(bool, int)`.
// Used for generated columns whose IR type is integer (e.g. a
// MySQL `tinyint(1)` source column the operator widened to
// `smallint` via --type-override; the body still references bool-
// returning sub-expressions but the column is integer-typed). MySQL
// accepts the bool/int mix via implicit coercion; PG's strict typing
// rejects with an operator-resolution error. v0.9.1 / Bug 17 residual.
//
// Bool-returning sub-expressions are recognised the same way
// [isBoolReturning] handles them: bare bool-mapped column names,
// comparisons (`=`, `!=`, `<>`), `IS [NOT] NULL`, and parenthesised
// wrappers. Anything else falls through verbatim.
func rewriteBoolToIntCoalesce(expr string, boolCols map[string]bool) string {
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
			return "COALESCE(" + boolToIntCast(left) + ", " + right + ")"
		case rightBool && isBoolIntLiteral(left):
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

// hasTopLevelCompareOp reports whether s contains `=`, `!=`, or `<>`
// at depth zero (not inside parens or string literals). The check is
// conservative — operators like `<` and `>` are legal comparisons but
// less commonly bool-returning in the COALESCE-with-int-literal idiom,
// and skipping them avoids false positives on arithmetic expressions.
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
			if depth == 0 && i+1 < len(s) && s[i+1] == '>' {
				return true
			}
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
