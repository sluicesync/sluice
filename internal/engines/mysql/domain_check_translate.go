// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// Cross-engine PG → MySQL DOMAIN-CHECK translator (v0.97.0 Option A,
// the v0.96.2 follow-up stretch).
//
// Background. v0.96.2's `maybeWarnDomainCheckDrop` made the silent-
// CHECK-drop class loud-observable: a structured WARN per writer
// lifetime listed every PG DOMAIN whose CHECK didn't carry to the
// MySQL target. v0.97.0 closes the remaining gap from "operator-
// actionable observability" to "in-database enforcement on MySQL
// targets" — by translating two well-defined PG DOMAIN CHECK shapes
// into MySQL 8.0.16+ table-level CHECK constraints inline at CREATE
// TABLE time.
//
// Why ONLY two shapes. The PG side accepts arbitrary SQL in a DOMAIN
// CHECK (`LENGTH(VALUE) > 5`, `VALUE IN ('a','b')`, custom functions,
// etc.); the MySQL CHECK grammar accepts a similar surface but the
// semantics diverge on regex syntax (PG POSIX regex vs MySQL ICU
// regex — different lookahead / backreference / Unicode property
// escape support), on type coercion (PG explicit casts vs MySQL
// implicit), and on operator precedence in corner cases. A loose
// translator that "best-efforts" arbitrary expressions would silently
// re-introduce the original Bug 113 silent-loss class: a CHECK present
// on dst with different enforcement semantics than the source is a
// MORE dangerous shape than no CHECK, because the operator sees a
// CHECK in `SHOW CREATE TABLE` and assumes parity.
//
// The two shapes this translator handles are the documented exemplars
// from the v0.95.x Bug 113 / v0.96.2 cycle work — operator-facing
// DOMAIN patterns that translate exactly (no semantic drift):
//
//	Regex: PG `CHECK (VALUE ~ 'pattern')` → MySQL
//	       `CHECK (REGEXP_LIKE(<col>, 'pattern'))`.
//	       PG's `~` and MySQL's `REGEXP_LIKE` both compile the
//	       pattern as a regular expression; the basic shapes
//	       (`^`, `$`, `.`, `[]`, `+`, `*`, `?`, character classes)
//	       behave identically. Anchored patterns (the email regex
//	       exemplar) round-trip exactly.
//
//	Range: PG `CHECK (VALUE >= X AND VALUE <= Y)` → MySQL
//	       `CHECK (<col> >= X AND <col> <= Y)`. Numeric / temporal
//	       range comparisons are universally portable.
//
// Anything else falls through with `ok=false` and the caller emits
// the v0.96.2 WARN unchanged — silent-loss class stays closed, just
// at the observability tier (WARN) rather than the enforcement tier
// (CHECK). Operators with more elaborate DOMAINs continue to add the
// MySQL CHECK manually per the WARN's hint.

import (
	"fmt"
	"regexp"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// regexCheckBodyPattern matches a PG DOMAIN regex CHECK body. The
// reader has already stripped the outer `CHECK (` and trailing `)`.
// The body PG produces from pg_get_constraintdef has the shape
// `(VALUE ~ '...'::text)` — outer parens (stripped by the caller via
// stripOuterParens) and an explicit `::text` cast (optional, depends
// on PG version and DOMAIN base type). Capture group 1 is the regex
// pattern; the cast suffix is consumed but discarded.
var regexCheckBodyPattern = regexp.MustCompile(`(?s)^VALUE\s*~\s*'((?:[^']|'')*)'\s*(?:::\w+)?\s*$`)

// rangeCheckBodyPattern matches a PG DOMAIN range CHECK body. PG
// produces `((VALUE >= (0)::numeric) AND (VALUE <= (100)::numeric))`
// from pg_get_constraintdef; the reader strips the outer CHECK wrapper.
// After one stripOuterParens pass the input is
// `(VALUE >= (0)::numeric) AND (VALUE <= (100)::numeric)`. Each
// half-expression is captured for further parsing; the AND joiner is
// case-insensitive (PG normalises it but defence-in-depth costs
// nothing). Permits arbitrary whitespace.
var rangeCheckBodyPattern = regexp.MustCompile(`(?is)^(?P<lo>.+?)\s+AND\s+(?P<hi>.+?)$`)

// rangeHalfPattern matches one half of a range CHECK after the AND
// split. Captures the comparison operator and the bare numeric
// literal. Permits leading/trailing parens and an optional `::type`
// cast on the value (PG normalises numeric literals with explicit
// casts; the cast is consumed and discarded — MySQL infers the type
// from the column's declared shape).
var rangeHalfPattern = regexp.MustCompile(`(?s)^\(?\s*VALUE\s*(?P<op>>=|<=|>|<|=)\s*\(?\s*(?P<val>-?\d+(?:\.\d+)?)\s*\)?\s*(?:::\w+)?\s*\)?\s*$`)

// translateDomainCheckToMySQL converts a PG DOMAIN's CHECK body (as
// produced by pg_get_constraintdef and stripped of the outer
// `CHECK (...)` wrapper by the PG schema reader) into a MySQL
// table-level CHECK constraint clause naming `col`. Returns ok=false
// for any expression shape this translator doesn't recognize — the
// caller then falls through to the v0.96.2 silent-downgrade WARN
// behavior.
//
// The returned clause is the bare CHECK clause body suitable for
// inline emission inside `CREATE TABLE (..., CHECK (...))`. The
// caller is responsible for the surrounding column-list comma and
// any CONSTRAINT-name prefix (this translator does NOT name the
// constraint; MySQL auto-generates a name per `tablename_chk_N`).
//
// Conservative by design: see file-header docstring for the rationale
// behind handling only regex and range shapes. Broadening this
// translator without a comprehensive semantic-equivalence audit
// would silently re-introduce the original Bug 113 silent-loss class.
func translateDomainCheckToMySQL(col string, check ir.DomainCheck) (clause string, ok bool) {
	return stdEmitter.translateDomainCheckToMySQL(col, check)
}

// translateDomainCheckToMySQL is the emitter method: the regex arm renders the
// pattern as a SQL string literal ([mysqlEmitter.quoteSQLString]), so the
// backslash-escaping follows this writer's resolved sql_mode (task 2.5).
func (m mysqlEmitter) translateDomainCheckToMySQL(col string, check ir.DomainCheck) (clause string, ok bool) {
	body := strings.TrimSpace(check.Body)
	if body == "" {
		return "", false
	}
	// stripOuterParens (defined in expr_walk.go) strips at most one
	// layer per call. PG's pg_get_constraintdef wraps the CHECK body
	// in 1-3 layers of parens depending on the DOMAIN's grammar; loop
	// until idempotent so the regex matchers see the bare expression.
	for {
		stripped := strings.TrimSpace(stripOuterParens(body))
		if stripped == body {
			break
		}
		body = stripped
	}
	if body == "" {
		return "", false
	}

	if c, ok := m.translateRegexCheckBody(col, body); ok {
		return c, true
	}
	if c, ok := translateRangeCheckBody(col, body); ok {
		return c, true
	}
	return "", false
}

// translateRegexCheckBody handles `VALUE ~ 'pattern'[::text]`. The
// pattern is emitted verbatim inside MySQL's REGEXP_LIKE — PG and
// MySQL share the basic regex surface (anchors, character classes,
// repetition operators) that operator-defined DOMAINs typically use.
// Advanced features (lookahead, backreferences, Unicode property
// escapes, possessive quantifiers) MAY differ between engines; for
// v0.97.0 we accept that operators using those features need to
// hand-translate. The fallback path (ok=false) is the v0.96.2 WARN.
//
// v0.97.1 doubled the pattern's backslashes at this call site so `'\.'`
// survives MySQL's string-literal parser as `\.` (undoubled, the regex
// engine saw `.` — any character — a strict-fidelity gap: the email
// regex `^[^@]+@[^@]+\.[^@]+$` accepted `aliceXexample.com`). Since the
// SEC-1 gap-2 fix, [quoteSQLString] itself doubles backslashes
// (sql_mode-aware), so the pattern passes through RAW here — doubling
// at both layers would corrupt the regex (`\.` → `\\.` = escaped
// backslash + any char). The mode-awareness also fixes v0.97.1's
// residual: under NO_BACKSLASH_ESCAPES the old unconditional doubling
// was itself wrong.
func (m mysqlEmitter) translateRegexCheckBody(col, body string) (string, bool) {
	sm := regexCheckBodyPattern.FindStringSubmatch(body)
	if sm == nil {
		return "", false
	}
	return fmt.Sprintf("CHECK (REGEXP_LIKE(%s, %s))", quoteIdent(col), m.quoteSQLString(sm[1])), true
}

// translateRangeCheckBody handles `VALUE >= X AND VALUE <= Y` and
// the inverse-bound variants. Both halves must reference VALUE with
// a comparison operator and a numeric literal (optionally cast); the
// literal text is passed through verbatim. MySQL coerces the literal
// to the column's declared type at parse time — same as PG.
func translateRangeCheckBody(col, body string) (string, bool) {
	m := rangeCheckBodyPattern.FindStringSubmatch(body)
	if m == nil {
		return "", false
	}
	lo := strings.TrimSpace(m[1])
	hi := strings.TrimSpace(m[2])
	loOp, loVal, ok := parseRangeHalf(lo)
	if !ok {
		return "", false
	}
	hiOp, hiVal, ok := parseRangeHalf(hi)
	if !ok {
		return "", false
	}
	c := quoteIdent(col)
	return fmt.Sprintf("CHECK (%s %s %s AND %s %s %s)", c, loOp, loVal, c, hiOp, hiVal), true
}

// parseRangeHalf pulls the operator and value out of one half of a
// range expression. Returns ok=false on any deviation from the
// `VALUE OP literal[::type]` shape.
func parseRangeHalf(half string) (op, val string, ok bool) {
	m := rangeHalfPattern.FindStringSubmatch(half)
	if m == nil {
		return "", "", false
	}
	return m[1], m[2], true
}

// mysqlVersionPattern parses the leading semver-like prefix of MySQL's
// VERSION() string. The string can look like `8.0.46`, `8.0.46-debug`,
// `5.7.42-log`, `10.11.4-MariaDB`. We extract major / minor / patch
// from the leading three-digit-group prefix and ignore the suffix.
var mysqlVersionPattern = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)`)

// mysqlVersionSupportsInlineCheck returns true when the parsed VERSION()
// string indicates MySQL 8.0.16 or newer — the version that started
// enforcing CHECK constraints (older versions parsed-and-ignored them,
// which would silently re-introduce the Bug 113 silent-loss class if
// we emitted CHECK without the version gate). MariaDB is excluded
// regardless of version: its regex dialect, casting rules, and CHECK
// semantics diverge from MySQL's in ways that operator-defined
// DOMAINs can trip on; absent a separate compatibility audit, the
// safe default is to fall through to the v0.96.2 WARN. The MariaDB
// branch suffix (`-MariaDB`) is the signal.
func mysqlVersionSupportsInlineCheck(version string) bool {
	if strings.Contains(version, "MariaDB") {
		return false
	}
	m := mysqlVersionPattern.FindStringSubmatch(version)
	if m == nil {
		return false
	}
	var major, minor, patch int
	_, _ = fmt.Sscanf(m[1], "%d", &major)
	_, _ = fmt.Sscanf(m[2], "%d", &minor)
	_, _ = fmt.Sscanf(m[3], "%d", &patch)
	if major > 8 {
		return true
	}
	if major == 8 && minor > 0 {
		return true
	}
	if major == 8 && minor == 0 && patch >= 16 {
		return true
	}
	return false
}
