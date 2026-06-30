// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import "strings"

// sqliteSourceDialect is the IR DefaultExpression / schema-feature dialect
// tag the SQLite engine stamps on every non-literal column DEFAULT (see
// internal/engines/sqlite/schema_reader.go::parseDefault). The PG writer
// recognises it on the DEFAULT path to translate the small portable set
// below and to loud-drop everything else — it is NEVER fed through the
// MySQL→PG expression translator (that path is gated on
// translatableSourceDialect == "mysql").
const sqliteSourceDialect = "sqlite"

// translateSQLiteDefaultExpr maps the small, well-known set of PORTABLE
// SQLite column-DEFAULT expressions to their Postgres equivalents,
// returning ok=false for anything outside that set.
//
// The set is deliberately tiny — only the SQLite "current instant"
// spellings that have an exact PG keyword counterpart:
//
//	datetime('now') / CURRENT_TIMESTAMP → CURRENT_TIMESTAMP
//	date('now')     / CURRENT_DATE      → CURRENT_DATE
//	time('now')     / CURRENT_TIME      → CURRENT_TIME
//
// Matching is case-insensitive and whitespace-tolerant, accepts the form
// with OR without surrounding parens (SQLite stores `(datetime('now'))`),
// and accepts SQLite's double-quoted `"now"` misfeature as well as the
// single-quoted form.
//
// Everything else — julianday(…), strftime(…), unixepoch(…), arbitrary
// expressions, the bare double-quoted-string misfeature like `"draft"`,
// etc. — returns ok=false. The caller drops the DEFAULT with a loud warn
// (see translateDefaultExpr) rather than guessing an exotic translation or
// aborting the whole CREATE TABLE. We do NOT try to be clever here: a
// missing entry is a safe loud-drop, a wrong entry is silent schema drift,
// so the map stays small and provably-portable.
func translateSQLiteDefaultExpr(expr string) (string, bool) {
	switch normalizeSQLiteDefaultExpr(expr) {
	case "datetime('now')", "current_timestamp":
		return "CURRENT_TIMESTAMP", true
	case "date('now')", "current_date":
		return "CURRENT_DATE", true
	case "time('now')", "current_time":
		return "CURRENT_TIME", true
	}
	return "", false
}

// normalizeSQLiteDefaultExpr canonicalises a SQLite DEFAULT expression for
// token-stable matching: it strips any fully-enclosing parens, removes all
// interior whitespace, lowercases, and rewrites SQLite's double-quoted
// `"now"` to the single-quoted `'now'` so both spellings collapse to one
// form. The result is only ever compared against the fixed portable set in
// translateSQLiteDefaultExpr — it is never emitted — so aggressive
// normalisation is safe (it can only turn a portable spelling into a
// recognised token, never invent one from a non-portable expression).
func normalizeSQLiteDefaultExpr(expr string) string {
	s := strings.TrimSpace(expr)
	// Strip fully-enclosing parens, repeatedly: SQLite stores a
	// parenthesised functional default as `(datetime('now'))`.
	for len(s) >= 2 && s[0] == '(' && s[len(s)-1] == ')' && parensWrapWhole(s) {
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			b.WriteRune(r)
		}
	}
	s = strings.ToLower(b.String())
	return strings.ReplaceAll(s, `"now"`, `'now'`)
}

// parensWrapWhole reports whether the leading '(' of s (which the caller
// has already confirmed starts with '(' and ends with ')') matches the
// trailing ')'. It returns false for shapes like `(a)+(b)` where the first
// paren closes before the end — guarding the repeated-strip loop from
// mangling an expression whose outer parens don't actually wrap the whole.
func parensWrapWhole(s string) bool {
	depth := 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(s)-1 {
				return false
			}
		}
	}
	return depth == 0
}
