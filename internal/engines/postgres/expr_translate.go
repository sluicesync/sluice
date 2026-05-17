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
	"fmt"
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
	// EnabledPGExtensions is the set of operator-opted-into PG
	// extensions (ADR-0032 framework). The set is sourced from
	// [emitOpts.EnabledExtensions] at the schema writer boundary and
	// gates extension-dependent translator rewrites — specifically
	// v0.38.0's SHA1/SHA2 → digest() rule which depends on pgcrypto.
	// Without the matching flag, those rules fall through verbatim
	// so PG's parse-time error is the operator's signal that they
	// need to enable the extension (loud-failure tenet).
	//
	// Rules that don't depend on an extension (the vast majority)
	// ignore this field entirely; nil / empty is the default.
	EnabledPGExtensions map[string]bool
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
	expr = rewriteJSONVALID(expr)
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
	// Bug 20: must run AFTER LCASE/UCASE so the MySQL synonyms have
	// already become LOWER/UPPER and a single rule covers all four
	// spellings.
	expr = rewriteLowerUpperLiteralCollation(expr)
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
	// v0.35.0 catalog batch — six additive rules from
	// docs/dev/translator-coverage.md. Each is mechanical (no
	// version-gated emit, no extension dependency, no cross-engine
	// semantic surprise). Order-independent vs prior rules — none of
	// these emits text that another rule would match.
	expr = rewriteHEX(expr)
	expr = rewriteFIELD(expr)
	expr = rewriteDAYNAME(expr)
	expr = rewriteMONTHNAME(expr)
	expr = rewriteWEEKOFYEAR(expr)
	expr = rewriteQUARTER(expr)
	expr = rewriteDATEDIFF(expr)
	// v0.37.0 — three additional catalog rules from
	// docs/dev/translator-coverage.md. Each was previously deferred;
	// closer review made the case for shipping each one cleaner than
	// the catalog's original "skip" verdict suggested.
	expr = rewriteTIMESTAMPDIFF(expr)
	expr = rewriteJSONOBJECT(expr)
	expr = rewriteJSONARRAY(expr)
	expr = rewriteLASTDAY(expr)
	// v0.38.0 — hash family. MD5 is core PG (mechanical case-fold).
	// SHA1 and SHA2 need pgcrypto's digest(), gated on the operator
	// having opted in via --enable-pg-extension pgcrypto. Without the
	// flag, SHA1/SHA2 fall through verbatim and PG's parse-time error
	// surfaces the missing extension; with the flag, sluice's preflight
	// has already confirmed pgcrypto is installed on the target.
	expr = rewriteMD5(expr)
	expr = rewriteSHA1(expr, ctx)
	expr = rewriteSHA2(expr, ctx)
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
	// v0.68.1 / Bug 8b — MySQL's NULL-safe equality operator `<=>`
	// becomes PG's `IS NOT DISTINCT FROM`. Operator-form rewrite
	// (string-literal-safe walk); runs after the function rewrites so
	// it doesn't disturb function argument parsing.
	expr = rewriteNullSafeEquals(expr)
	// Bool-idiom rewrites run last: they need the canonical names
	// (IFNULL → COALESCE has already happened) and they're gated on
	// the caller-supplied BoolColumns set — empty means no rewrites.
	expr = rewriteBoolIdioms(expr, ctx)
	return expr
}

// rewriteJSONVALID rewrites MySQL's `JSON_VALID(x)` predicate to PG's
// `(x IS JSON)` (PG 16+). MySQL uses `JSON_VALID()` chiefly inside
// CHECK constraints to enforce that a text/longtext column holds
// well-formed JSON; PG has no `json_valid` function (Bug 8a — PG
// surfaces `function json_valid(text) does not exist` mid-pipeline)
// but the `IS JSON` predicate is the direct equivalent. Single-arg
// form only; the rare 2-arg `JSON_VALID(doc, path)` (not standard
// MySQL — there is no such overload) and any other arity fall
// through verbatim under the loud-failure tenet.
//
// PG-version note: `IS JSON` is PG 16+. Targets on PG 15 or earlier
// will reject it at apply time — that is the loud-failure tenet
// working as intended (a clear PG parse error naming the construct),
// strictly better than the pre-v0.68.1 behaviour where the
// untranslated `json_valid(...)` aborted migrate after partial table
// creation with no preview warning.
func rewriteJSONVALID(expr string) string {
	return rewriteFunctionCalls(expr, "JSON_VALID", func(args []string) string {
		if len(args) != 1 {
			return ""
		}
		return "(" + strings.TrimSpace(args[0]) + " IS JSON)"
	})
}

// rewriteNullSafeEquals rewrites MySQL's NULL-safe equality operator
// `a <=> b` to PG's `a IS NOT DISTINCT FROM b` (Bug 8b). Both have
// identical semantics: equal when both sides are non-NULL and equal,
// equal when both are NULL, unequal otherwise. PG has no `<=>`
// operator (it surfaces `operator does not exist` mid-pipeline), so
// the verbatim-passthrough policy turned this common MySQL idiom into
// a partial-migration abort.
//
// The walk is string-literal-aware (reusing the shared exprident
// scan primitive) so a `<=>` appearing inside a quoted string literal
// is left untouched. The operator binds the same as `=` in both
// dialects, and `IS NOT DISTINCT FROM` is itself a comparison
// operator at the same precedence, so a token-level substitution
// preserves the parse — no re-parenthesisation needed. The `<=>`
// token cannot be confused with `<=` followed by `>` because MySQL
// tokenises `<=>` greedily; we match the full three-byte operator
// only.
func rewriteNullSafeEquals(expr string) string {
	const op = "<=>"
	var sb strings.Builder
	for i := 0; i < len(expr); {
		if expr[i] == '\'' {
			end := scanStringLiteral(expr, i)
			sb.WriteString(expr[i:end])
			i = end
			continue
		}
		if i+len(op) <= len(expr) && expr[i:i+len(op)] == op {
			// Consume any whitespace already adjacent to the operator
			// in the source so the rewritten form has exactly one
			// space on each side (no `a  IS NOT DISTINCT FROM  b`
			// double-spacing when the source was `a <=> b`). Trailing
			// space already in sb is trimmed; leading source space is
			// skipped.
			cur := sb.String()
			sb.Reset()
			sb.WriteString(strings.TrimRight(cur, " \t"))
			sb.WriteString(" IS NOT DISTINCT FROM ")
			i += len(op)
			for i < len(expr) && (expr[i] == ' ' || expr[i] == '\t') {
				i++
			}
			continue
		}
		sb.WriteByte(expr[i])
		i++
	}
	return sb.String()
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

// rewriteNOWFamily rewrites MySQL's parenthesised current-date/time
// functions to PG's keyword form:
//
//   - NOW() / CURRENT_TIMESTAMP() / LOCALTIMESTAMP() / LOCALTIME()
//     → CURRENT_TIMESTAMP / LOCALTIMESTAMP (bare keyword)
//   - the 1-arg fractional-precision form NOW(N) /
//     CURRENT_TIMESTAMP(N) / LOCALTIMESTAMP(N) / LOCALTIME(N)
//     → CURRENT_TIMESTAMP(N) / LOCALTIMESTAMP(N)  (Bug 8c — PG
//     accepts the precision-keyword form; `DEFAULT (now(3))` is
//     extremely common in MySQL 8 DDL)
//   - CURDATE() / CURRENT_DATE() → CURRENT_DATE  (Bug 8c)
//   - CURTIME() / CURRENT_TIME() → CURRENT_TIME; CURTIME(N) →
//     CURRENT_TIME(N)
//
// PG accepts the keyword forms (with optional precision) and rejects
// the MySQL `NOW()` / `curdate()` spellings outright; the keyword
// form also matches what PG emits when reading back its own DEFAULTs,
// so the rewrite normalises round-trips. The precision arg must be a
// bare integer literal — an expression-valued precision falls through
// verbatim under the loud-failure tenet.
func rewriteNOWFamily(expr string) string {
	// NOW() → CURRENT_TIMESTAMP; NOW(N) → CURRENT_TIMESTAMP(N).
	// PG accepts the fractional-seconds-precision form
	// `CURRENT_TIMESTAMP(N)` as a function-of-precision keyword, so
	// the MySQL `now(3)` / `NOW(6)` shape (common in
	// `DEFAULT (now(3))` columns) is mechanically translatable —
	// Bug 8c. Only a single integer-literal precision arg is
	// translated; anything else (multi-arg, expression arg) falls
	// through verbatim under the loud-failure tenet.
	expr = rewriteFunctionCalls(expr, "NOW", func(args []string) string {
		switch len(args) {
		case 0:
			return "CURRENT_TIMESTAMP"
		case 1:
			if p := strings.TrimSpace(args[0]); isIntLiteral(p) {
				return "CURRENT_TIMESTAMP(" + p + ")"
			}
		}
		return ""
	})
	expr = rewriteFunctionCalls(expr, "CURRENT_TIMESTAMP", func(args []string) string {
		switch len(args) {
		case 0:
			return "CURRENT_TIMESTAMP"
		case 1:
			if p := strings.TrimSpace(args[0]); isIntLiteral(p) {
				return "CURRENT_TIMESTAMP(" + p + ")"
			}
		}
		return ""
	})
	expr = rewriteFunctionCalls(expr, "LOCALTIMESTAMP", func(args []string) string {
		switch len(args) {
		case 0:
			return "LOCALTIMESTAMP"
		case 1:
			if p := strings.TrimSpace(args[0]); isIntLiteral(p) {
				return "LOCALTIMESTAMP(" + p + ")"
			}
		}
		return ""
	})
	expr = rewriteFunctionCalls(expr, "LOCALTIME", func(args []string) string {
		switch len(args) {
		case 0:
			return "LOCALTIMESTAMP"
		case 1:
			if p := strings.TrimSpace(args[0]); isIntLiteral(p) {
				return "LOCALTIMESTAMP(" + p + ")"
			}
		}
		return ""
	})
	// CURDATE() / CURRENT_DATE() → CURRENT_DATE (Bug 8c). MySQL's
	// `curdate()` and the parenthesised `current_date()` synonym
	// both map to PG's bare `CURRENT_DATE` keyword (PG rejects the
	// parenthesised forms). Argless only.
	expr = rewriteFunctionCalls(expr, "CURDATE", func(args []string) string {
		if len(args) != 0 {
			return ""
		}
		return "CURRENT_DATE"
	})
	expr = rewriteFunctionCalls(expr, "CURRENT_DATE", func(args []string) string {
		if len(args) != 0 {
			return ""
		}
		return "CURRENT_DATE"
	})
	// CURTIME() → CURRENT_TIME; CURTIME(N) → CURRENT_TIME(N). PG's
	// CURRENT_TIME accepts the fractional-precision form, matching
	// the NOW(N) treatment above. CURRENT_TIME() parenthesised
	// synonym maps the same way.
	curTime := func(args []string) string {
		switch len(args) {
		case 0:
			return "CURRENT_TIME"
		case 1:
			if p := strings.TrimSpace(args[0]); isIntLiteral(p) {
				return "CURRENT_TIME(" + p + ")"
			}
		}
		return ""
	}
	expr = rewriteFunctionCalls(expr, "CURTIME", curTime)
	expr = rewriteFunctionCalls(expr, "CURRENT_TIME", curTime)
	return expr
}

// isIntLiteral reports whether s is a non-empty run of ASCII digits
// (an unsigned integer literal). Used by the fractional-precision
// time rewrites to gate `NOW(N)` → `CURRENT_TIMESTAMP(N)` on N being
// a literal — an expression-valued precision arg is not portable and
// falls through verbatim.
func isIntLiteral(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
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

// rewriteLowerUpperLiteralCollation wraps a sole string-literal
// argument to LOWER()/UPPER() in an explicit `::text` cast so
// PostgreSQL can resolve a collation. MySQL accepts `LOWER('ABC')`
// in a STORED generated column (and CHECK); PostgreSQL rejects it
// with SQLSTATE 42P22 ("could not determine which collation to use
// for lower() function") because an unadorned string literal has the
// `unknown` type and carries no collation, and a STORED generated
// column's expression must have a determinable collation at DDL time
// (catalog Bug 20). Casting the literal to `text` gives it the
// database default collation; `lower('ABC'::text)` is byte-identical
// in result to MySQL's `LOWER('ABC')`, so the rewrite is faithful
// rather than a loud refusal.
//
// Scope is deliberately tight (false-rewrite safety, mirroring the
// other rules' conservatism): only a SINGLE argument that is exactly
// one string literal is wrapped. A column reference (`lower(name)`)
// already carries the column's collation; a compound argument
// (`lower('a' || b)`) or an already-cast one (`lower('a'::text)`) is
// out of scope and passes through verbatim — if such a form is still
// non-collatable, PG's loud parse error remains the operator's signal,
// consistent with the verbatim-passthrough policy.
func rewriteLowerUpperLiteralCollation(expr string) string {
	castLiteralArg := func(fn string) func([]string) string {
		return func(args []string) string {
			if len(args) != 1 {
				return ""
			}
			a := strings.TrimSpace(args[0])
			if !isSingleStringLiteral(a) {
				return ""
			}
			return fn + "(" + a + "::text)"
		}
	}
	expr = rewriteFunctionCalls(expr, "LOWER", castLiteralArg("LOWER"))
	expr = rewriteFunctionCalls(expr, "UPPER", castLiteralArg("UPPER"))
	return expr
}

// isSingleStringLiteral reports whether s is exactly one single-quoted
// SQL string literal with nothing before or after it. A compound
// expression (`'a' || x`) or an already-cast literal (`'a'::text`)
// returns false — both are correctly left unwrapped by
// [rewriteLowerUpperLiteralCollation].
func isSingleStringLiteral(s string) bool {
	if len(s) < 2 || s[0] != '\'' {
		return false
	}
	return scanStringLiteral(s, 0) == len(s)
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

// ============================================================
// v0.35.0 catalog batch — additive rules from the v1 catalog
// (`docs/dev/translator-coverage.md`). Each rule below is mechanical
// per its catalog entry; the per-rule comments cite the catalog
// section number for cross-reference.
//
// Deliberately NOT shipped this batch (per the catalog's own
// guidance):
//   - #10 MD5/SHA1/SHA2 — requires pgcrypto's digest(); crosses
//     extension boundary, violates contain-Postgres-complexity tenet.
//   - #11 GREATEST/LEAST — same function name in both engines but
//     NULL semantics differ; auto-rewrite would mask the divergence.
//   - #13 REGEXP_LIKE — MySQL ICU vs PG POSIX regex flavours diverge
//     beyond clean rewrite; operator's `--expr-override` is the right
//     escape hatch.
//   - #16 TIMESTAMPDIFF — unit-cross-product makes the rule table
//     unwieldy; case-by-case `--expr-override`.
//   - #20 JSON_OBJECT/JSON_ARRAY — version-gated (PG 16+ vs
//     JSON_BUILD_*); deferred until version-aware emit lands.
//   - #21 FIND_IN_SET — full position semantic needs a LATERAL
//     subquery, invalid in CHECK/GENERATED contexts.
//   - #23 CONVERT_TZ — AT TIME ZONE has subtle timestamp-vs-
//     timestamptz semantics; auto-rewrite would surprise operators.
//   - #24 LAST_DAY — verbose 5-token expansion; `--expr-override` is
//     cleaner than baking it into the catalog.
//   - #29 INET_ATON/INET_NTOA — no portable PG equivalent without
//     a custom function.
// ============================================================

// rewriteHEX rewrites MySQL's `HEX(int_or_bigint)` to PG's
// `to_hex(...)`. Both return the integer's hexadecimal representation
// as a string. Catalog rule #19 (narrow form: integer-typed argument
// only — `HEX(string)` returning hex of bytes would need
// `encode(x::bytea, 'hex')` which is the wrong rewrite if the column
// is integer-typed). Falls through verbatim on 0-arg / 2+-arg forms.
//
// Single rewrite arm; the column-type context isn't reachable here so
// we conservatively assume integer-typed input. Operators whose HEX
// argument is a varbinary / blob column should use `--expr-override`.
func rewriteHEX(expr string) string {
	return rewriteFunctionCalls(expr, "HEX", func(args []string) string {
		if len(args) != 1 {
			return ""
		}
		return "to_hex(" + strings.TrimSpace(args[0]) + ")"
	})
}

// rewriteFIELD rewrites MySQL's `FIELD(x, a, b, c, ...)` (returns the
// 1-based position of x in the list, or 0 if not present) to PG's
// `array_position(ARRAY[a, b, c, ...], x)` (returns the 1-based
// position, or NULL if not present). Catalog rule #22.
//
// Semantic gotcha: PG returns NULL when the value isn't in the
// array; MySQL returns 0. For most DDL-body uses (ORDER BY proxies,
// custom enum ranks) NULL ordering follows the same logical
// direction as 0, but operators with a strict 0-vs-NULL distinction
// in a CHECK constraint should use `--expr-override`. Documented as
// a known sharp edge in the catalog.
//
// Requires at least one needle + one haystack value (≥ 2 args). Falls
// through verbatim otherwise.
func rewriteFIELD(expr string) string {
	return rewriteFunctionCalls(expr, "FIELD", func(args []string) string {
		if len(args) < 2 {
			return ""
		}
		trimmed := make([]string, len(args))
		for i, a := range args {
			trimmed[i] = strings.TrimSpace(a)
		}
		needle := trimmed[0]
		haystack := strings.Join(trimmed[1:], ", ")
		return "array_position(ARRAY[" + haystack + "], " + needle + ")"
	})
}

// rewriteDAYNAME rewrites MySQL's `DAYNAME(d)` (returns the weekday
// name as a string — "Monday", "Tuesday", ...) to PG's
// `TO_CHAR(d, 'FMDay')`. The `FM` prefix suppresses PG's default
// right-padding to 9 chars; without it `TO_CHAR(d, 'Day')` produces
// e.g. "Monday   " with trailing spaces that would silently diverge
// from MySQL's output in a string-compare CHECK constraint.
// Catalog rule #25.
//
// Same STABLE-not-IMMUTABLE caveat as DATE_FORMAT — PG marks TO_CHAR
// as STABLE, which means it can't appear in IMMUTABLE generated
// columns. PG raises a clear error at apply time; loud-failure tenet
// preserved.
func rewriteDAYNAME(expr string) string {
	return rewriteFunctionCalls(expr, "DAYNAME", func(args []string) string {
		if len(args) != 1 {
			return ""
		}
		return "TO_CHAR(" + strings.TrimSpace(args[0]) + ", 'FMDay')"
	})
}

// rewriteMONTHNAME rewrites MySQL's `MONTHNAME(d)` to PG's
// `TO_CHAR(d, 'FMMonth')`. Same shape as [rewriteDAYNAME]; see that
// function's comment for the FM-prefix and IMMUTABLE caveats.
// Catalog rule #25.
func rewriteMONTHNAME(expr string) string {
	return rewriteFunctionCalls(expr, "MONTHNAME", func(args []string) string {
		if len(args) != 1 {
			return ""
		}
		return "TO_CHAR(" + strings.TrimSpace(args[0]) + ", 'FMMonth')"
	})
}

// rewriteWEEKOFYEAR rewrites MySQL's `WEEKOFYEAR(d)` (equivalent to
// `WEEK(d, 3)` — ISO 8601 week numbering) to PG's
// `EXTRACT(WEEK FROM d)::int`. Both return the ISO-8601 week number.
// PG's EXTRACT returns double precision; we cast to int to match
// MySQL's integer return type for CHECK / GENERATED context fidelity.
// Catalog rule #26 (the WEEKOFYEAR subset).
//
// `WEEK(d, mode)` with mode != 1 / 3 (ISO) uses different
// Sunday/Monday-start semantics that PG doesn't model; sluice does
// NOT auto-rewrite the moded WEEK form to avoid silent divergence.
// The bare `WEEK(d)` form is mode-dependent on MySQL's
// default_week_format session variable; also not auto-rewritten.
// Operator's `--expr-override` covers the moded cases.
func rewriteWEEKOFYEAR(expr string) string {
	return rewriteFunctionCalls(expr, "WEEKOFYEAR", func(args []string) string {
		if len(args) != 1 {
			return ""
		}
		return "EXTRACT(WEEK FROM " + strings.TrimSpace(args[0]) + ")::int"
	})
}

// rewriteQUARTER rewrites MySQL's `QUARTER(d)` (returns 1-4) to PG's
// `EXTRACT(QUARTER FROM d)::int`. Both return the quarter number as
// an integer in the 1..4 range. PG's EXTRACT returns double
// precision; we cast to int for type fidelity in CHECK / GENERATED
// contexts. Catalog rule #27 (the QUARTER subset; YEARWEEK is
// deferred — it composes EXTRACT with arithmetic and inherits #26's
// week-numbering caveats).
func rewriteQUARTER(expr string) string {
	return rewriteFunctionCalls(expr, "QUARTER", func(args []string) string {
		if len(args) != 1 {
			return ""
		}
		return "EXTRACT(QUARTER FROM " + strings.TrimSpace(args[0]) + ")::int"
	})
}

// rewriteDATEDIFF rewrites MySQL's `DATEDIFF(a, b)` (returns days as
// integer; a − b in calendar-day terms) to PG's `(a::date - b::date)`
// operator form. Both return the signed integer difference in days.
// Catalog rule #28.
//
// PG's date subtraction is an SQL operator, not a function call, so
// the rewrite produces a parenthesised binary expression instead of a
// fn-call shape. The `::date` casts are belt-and-braces: if the
// arguments are already date-typed they're a no-op; if they're
// timestamps the cast truncates to day precision, matching MySQL's
// behaviour (MySQL ignores the time portion).
func rewriteDATEDIFF(expr string) string {
	return rewriteFunctionCalls(expr, "DATEDIFF", func(args []string) string {
		if len(args) != 2 {
			return ""
		}
		return "(" + strings.TrimSpace(args[0]) + "::date - " + strings.TrimSpace(args[1]) + "::date)"
	})
}

// ============================================================
// v0.37.0 catalog batch — three additional rules from
// docs/dev/translator-coverage.md that were previously deferred. The
// catalog's per-rule deferral reasoning for these three didn't fully
// hold up under closer review:
//
//   - #16 TIMESTAMPDIFF — the "unit-cross-product makes the rule
//     table unwieldy" objection turns out to be 8-9 mechanical arms
//     in one switch. Manageable; ships.
//   - #20 JSON_OBJECT / JSON_ARRAY — the "needs version-aware emit"
//     objection vanishes by always emitting JSON_BUILD_OBJECT /
//     JSON_BUILD_ARRAY (works on every PG version sluice supports;
//     pre-16 needs them, ≥16 still accepts them). No detection
//     needed.
//   - #24 LAST_DAY — the "verbose 5-token expansion" objection is
//     real but trivial; the rewrite is mechanical and operator-
//     visible.
//
// The 6 rules still deferred (#10 MD5/SHA family, #11 GREATEST/LEAST,
// #13 REGEXP_LIKE, #21 FIND_IN_SET, #23 CONVERT_TZ, #29 INET_*) each
// have a load-bearing catalog reason that stands — extension
// boundary, NULL-semantics divergence, ICU-vs-POSIX regex flavour,
// invalid-in-CHECK/GENERATED LATERAL, TZ-semantics fuzziness, or
// no-portable-equivalent. All six have actionable --expr-override
// workarounds.
// ============================================================

// rewriteTIMESTAMPDIFF rewrites MySQL's `TIMESTAMPDIFF(unit, a, b)`
// (returns the difference between a and b in the given unit, as a
// truncated-toward-zero integer) to PG-equivalent expressions.
// Catalog rule #16.
//
// Per-unit emit shapes:
//
//   - MICROSECOND → `(EXTRACT(EPOCH FROM (b - a)) * 1000000)::bigint`
//   - SECOND      → `EXTRACT(EPOCH FROM (b - a))::bigint`
//   - MINUTE      → `(EXTRACT(EPOCH FROM (b - a)) / 60)::bigint`
//   - HOUR        → `(EXTRACT(EPOCH FROM (b - a)) / 3600)::bigint`
//   - DAY         → `(b::date - a::date)` (date subtraction returns int)
//   - WEEK        → `((b::date - a::date) / 7)`
//   - MONTH       → `(EXTRACT(YEAR FROM AGE(b, a)) * 12 + EXTRACT(MONTH FROM AGE(b, a)))::int`
//   - QUARTER     → same as MONTH with `/ 3`
//   - YEAR        → `EXTRACT(YEAR FROM AGE(b, a))::int`
//
// MySQL's TIMESTAMPDIFF returns COMPLETE calendar units (truncated
// toward zero), not duration-based units. The MONTH/QUARTER/YEAR
// arms use `AGE(b, a)` which returns a calendar-aware interval
// matching MySQL's semantic. DAY and below are duration-based, where
// EPOCH or date-subtraction is the right primitive.
//
// Falls through verbatim on unrecognised unit names, non-3-arg
// shapes, or empty args. Operators with unusual unit specifiers can
// always use `--expr-override`.
func rewriteTIMESTAMPDIFF(expr string) string {
	return rewriteFunctionCalls(expr, "TIMESTAMPDIFF", func(args []string) string {
		if len(args) != 3 {
			return ""
		}
		unit := strings.ToUpper(strings.TrimSpace(args[0]))
		a := strings.TrimSpace(args[1])
		b := strings.TrimSpace(args[2])
		if unit == "" || a == "" || b == "" {
			return ""
		}
		switch unit {
		case "MICROSECOND":
			return fmt.Sprintf("(EXTRACT(EPOCH FROM (%s - %s)) * 1000000)::bigint", b, a)
		case "SECOND":
			return fmt.Sprintf("EXTRACT(EPOCH FROM (%s - %s))::bigint", b, a)
		case "MINUTE":
			return fmt.Sprintf("(EXTRACT(EPOCH FROM (%s - %s)) / 60)::bigint", b, a)
		case "HOUR":
			return fmt.Sprintf("(EXTRACT(EPOCH FROM (%s - %s)) / 3600)::bigint", b, a)
		case "DAY":
			return fmt.Sprintf("(%s::date - %s::date)", b, a)
		case "WEEK":
			return fmt.Sprintf("((%s::date - %s::date) / 7)", b, a)
		case "MONTH":
			return fmt.Sprintf("(EXTRACT(YEAR FROM AGE(%s, %s)) * 12 + EXTRACT(MONTH FROM AGE(%s, %s)))::int", b, a, b, a)
		case "QUARTER":
			return fmt.Sprintf("((EXTRACT(YEAR FROM AGE(%s, %s)) * 12 + EXTRACT(MONTH FROM AGE(%s, %s))) / 3)::int", b, a, b, a)
		case "YEAR":
			return fmt.Sprintf("EXTRACT(YEAR FROM AGE(%s, %s))::int", b, a)
		}
		return ""
	})
}

// rewriteJSONOBJECT rewrites MySQL's `JSON_OBJECT(k1, v1, k2, v2, ...)`
// (positional key/value pairs) to PG's `JSON_BUILD_OBJECT(k1, v1, k2,
// v2, ...)`. Both are PG-portable across every version sluice
// supports — pre-16 they're the only choice, ≥16 still accepts them.
//
// PG 16 added an SQL-standard `JSON_OBJECT(k1: v1, k2: v2)` syntax
// with `:` separators; sluice deliberately doesn't emit that because
// (a) we'd need server-version detection to know whether it's safe
// and (b) JSON_BUILD_OBJECT produces equivalent JSON output, so
// there's no operator-visible value in the choice.
//
// Catalog rule #20. Falls through on empty or zero-arg shapes.
//
// MySQL accepts JSON_OBJECT with an odd-count arg list (last key has
// no value); PG's JSON_BUILD_OBJECT rejects that. The rewrite passes
// the count through unchanged — if MySQL accepted odd args, PG will
// catch the malformed call at apply time (loud-failure tenet).
func rewriteJSONOBJECT(expr string) string {
	return rewriteFunctionCalls(expr, "JSON_OBJECT", func(args []string) string {
		if len(args) == 0 {
			return ""
		}
		trimmed := make([]string, len(args))
		for i, a := range args {
			trimmed[i] = strings.TrimSpace(a)
		}
		return "JSON_BUILD_OBJECT(" + strings.Join(trimmed, ", ") + ")"
	})
}

// rewriteJSONARRAY rewrites MySQL's `JSON_ARRAY(a, b, c)` to PG's
// `JSON_BUILD_ARRAY(a, b, c)`. Same version-agnostic shipping
// strategy as [rewriteJSONOBJECT] — JSON_BUILD_ARRAY works on every
// PG version sluice supports, including PG 16+. Catalog rule #20.
//
// Empty `JSON_ARRAY()` is a valid MySQL form returning `[]`; PG's
// `JSON_BUILD_ARRAY()` also returns `[]`. Pass through.
func rewriteJSONARRAY(expr string) string {
	return rewriteFunctionCalls(expr, "JSON_ARRAY", func(args []string) string {
		trimmed := make([]string, len(args))
		for i, a := range args {
			trimmed[i] = strings.TrimSpace(a)
		}
		return "JSON_BUILD_ARRAY(" + strings.Join(trimmed, ", ") + ")"
	})
}

// rewriteLASTDAY rewrites MySQL's `LAST_DAY(d)` (returns the last
// day of the month containing d, as a date) to the PG-equivalent
// date-truncation expression:
//
//	(DATE_TRUNC('month', d) + INTERVAL '1 month' - INTERVAL '1 day')::date
//
// Catalog rule #24. The rewrite is verbose (5 tokens vs MySQL's
// single function call) but mechanical and produces identical output
// on every date / timestamp input. Operators wanting a tighter
// expression can use `--expr-override`.
//
// Falls through on non-1-arg shapes (MySQL only accepts 1 arg).
func rewriteLASTDAY(expr string) string {
	return rewriteFunctionCalls(expr, "LAST_DAY", func(args []string) string {
		if len(args) != 1 {
			return ""
		}
		d := strings.TrimSpace(args[0])
		if d == "" {
			return ""
		}
		return fmt.Sprintf("(DATE_TRUNC('month', %s) + INTERVAL '1 month' - INTERVAL '1 day')::date", d)
	})
}

// ============================================================
// v0.38.0 catalog batch — MD5 / SHA1 / SHA2 hash family
// (catalog rule #10). Re-assessment of the v0.35.0 deferral:
// MD5 doesn't actually need an extension (PG core has `md5(text)`);
// SHA1 / SHA2 need pgcrypto's `digest()` but pgcrypto is a contrib
// extension that ships with PG itself, available on every major
// hosted PG service (PlanetScale, RDS, Cloud SQL, Azure DB, Supabase).
// Gating the SHA rules on `--enable-pg-extension pgcrypto` mirrors
// hstore's pattern — the operator opts in, sluice's preflight
// confirms the extension is installed on the target before any
// translation happens, and the rewrite emits the right
// pgcrypto-backed expression.
// ============================================================

// rewriteMD5 rewrites MySQL's `MD5(x)` to PG's core `md5(x)`. Both
// return the 32-character lowercase hex digest of the input. PG's
// `md5(text)` is in core (not pgcrypto); no extension dependency.
// Catalog rule #10 — the MD5-only subset.
//
// Mechanical case-fold rewrite; falls through verbatim on
// non-1-arg shapes.
func rewriteMD5(expr string) string {
	return rewriteFunctionCalls(expr, "MD5", func(args []string) string {
		if len(args) != 1 {
			return ""
		}
		return "md5(" + strings.TrimSpace(args[0]) + ")"
	})
}

// rewriteSHA1 rewrites MySQL's `SHA1(x)` to PG's
// `encode(digest(x, 'sha1'), 'hex')`. Catalog rule #10 — the SHA1
// subset. Gated on the operator having passed
// `--enable-pg-extension pgcrypto`; without the flag the call falls
// through verbatim so PG's parse-time "function sha1 does not exist"
// error surfaces as the operator's signal to enable the extension.
//
// Both MySQL `SHA1` and the digest+encode combination return a
// 40-character lowercase hex string for the same input. Identical
// output bytes; safe for CHECK / GENERATED contexts.
func rewriteSHA1(expr string, ctx ExprContext) string {
	if !ctx.EnabledPGExtensions["pgcrypto"] {
		return expr
	}
	return rewriteFunctionCalls(expr, "SHA1", func(args []string) string {
		if len(args) != 1 {
			return ""
		}
		return "encode(digest(" + strings.TrimSpace(args[0]) + ", 'sha1'), 'hex')"
	})
}

// rewriteSHA2 rewrites MySQL's `SHA2(x, bits)` to PG's
// `encode(digest(x, '<algo>'), 'hex')` where `<algo>` is selected
// from `bits`: 0 / 256 → sha256, 224 → sha224, 384 → sha384,
// 512 → sha512. Catalog rule #10 — the SHA2 subset. Gated on
// pgcrypto (same as SHA1).
//
// MySQL's `SHA2(x, 0)` is documented as the SHA-256 default; the
// rewrite preserves that semantic. Unrecognised bit widths fall
// through verbatim (PG's parse-time error surfaces the unsupported
// bit-width choice).
func rewriteSHA2(expr string, ctx ExprContext) string {
	if !ctx.EnabledPGExtensions["pgcrypto"] {
		return expr
	}
	return rewriteFunctionCalls(expr, "SHA2", func(args []string) string {
		if len(args) != 2 {
			return ""
		}
		x := strings.TrimSpace(args[0])
		bits := strings.TrimSpace(args[1])
		var algo string
		switch bits {
		case "0", "256":
			algo = "sha256"
		case "224":
			algo = "sha224"
		case "384":
			algo = "sha384"
		case "512":
			algo = "sha512"
		default:
			return ""
		}
		return "encode(digest(" + x + ", '" + algo + "'), 'hex')"
	})
}
