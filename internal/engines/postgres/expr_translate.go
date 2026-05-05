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
// See ADR-0016 for the design rationale.

package postgres

import (
	"strings"
)

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
func translateExprForPG(expr string) string {
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
