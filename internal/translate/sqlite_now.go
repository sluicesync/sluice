// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import "strings"

// SQLiteNowShape names what a SQLite "current instant" column DEFAULT
// stores (GC-39 item 3). SQLite evaluates 'now' in UTC, always — there is
// no session zone — and stores the result as TEXT in the shape below
// (or, for [SQLiteNowEpoch], an integer). A target writer turns the shape
// into its own spelling for the COLUMN'S TYPE: the same source expression
// needs a different target expression on a naive timestamp, a date, a
// time and a text column, and a target's bare CURRENT_TIMESTAMP family
// evaluates in the target SESSION's zone, which is how a migrated
// `DEFAULT CURRENT_TIMESTAMP` came to stamp Tokyo wall-clock digits into
// a naive column that had held UTC (measured: nine hours ahead on an
// Asia/Tokyo Postgres database and on a `+09:00` MySQL session).
type SQLiteNowShape int

const (
	// SQLiteNowNone is not a recognised current-instant spelling.
	SQLiteNowNone SQLiteNowShape = iota
	// SQLiteNowDateTime is `YYYY-MM-DD HH:MM:SS` — datetime('now'),
	// CURRENT_TIMESTAMP, strftime('%Y-%m-%d %H:%M:%S','now').
	SQLiteNowDateTime
	// SQLiteNowISOZ is `YYYY-MM-DDTHH:MM:SSZ` —
	// strftime('%Y-%m-%dT%H:%M:%SZ','now').
	SQLiteNowISOZ
	// SQLiteNowDate is `YYYY-MM-DD` — date('now'), CURRENT_DATE,
	// strftime('%Y-%m-%d','now').
	SQLiteNowDate
	// SQLiteNowTime is `HH:MM:SS` — time('now'), CURRENT_TIME,
	// strftime('%H:%M:%S','now').
	SQLiteNowTime
	// SQLiteNowEpoch is integer seconds since the Unix epoch —
	// strftime('%s','now'). Zone-free.
	SQLiteNowEpoch
)

// sqliteNowStrftimeFormats are the whole strftime formats with a known
// shape. Matched EXACTLY: strftime specifiers are case-sensitive (%M is
// minutes, %m months; %S is the seconds field, %s the epoch), so the
// format literal is never case-folded. The previous Postgres-side matcher
// lowercased the whole expression and so read strftime('%S','now') — the
// two-digit seconds field — as the epoch.
var sqliteNowStrftimeFormats = map[string]SQLiteNowShape{
	"%Y-%m-%d %H:%M:%S":  SQLiteNowDateTime,
	"%Y-%m-%dT%H:%M:%SZ": SQLiteNowISOZ,
	"%Y-%m-%d":           SQLiteNowDate,
	"%H:%M:%S":           SQLiteNowTime,
	"%s":                 SQLiteNowEpoch,
}

// ClassifySQLiteNowDefault reports the shape of a SQLite column DEFAULT
// that is exactly one current-instant spelling: a CURRENT_TIMESTAMP /
// CURRENT_DATE / CURRENT_TIME keyword, datetime/date/time('now'), or
// strftime(<format>,'now') for a format in [sqliteNowStrftimeFormats].
// Function and keyword names are case-insensitive, whitespace outside
// string literals is ignored, enclosing parentheses are stripped (SQLite
// stores `(datetime('now'))`), and 'now' may be single- or double-quoted
// (SQLite's double-quoted-string misfeature) in any case, as SQLite
// itself accepts. A modifier argument (`datetime('now','+1 day')`,
// `'localtime'`), any other base, or a composite expression is
// [SQLiteNowNone].
func ClassifySQLiteNowDefault(expr string) SQLiteNowShape {
	s := strings.TrimSpace(expr)
	for len(s) >= 2 && s[0] == '(' && s[len(s)-1] == ')' && sqliteParensWrapWhole(s) {
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	open := strings.IndexByte(s, '(')
	if open < 0 {
		switch strings.ToLower(s) {
		case "current_timestamp":
			return SQLiteNowDateTime
		case "current_date":
			return SQLiteNowDate
		case "current_time":
			return SQLiteNowTime
		}
		return SQLiteNowNone
	}
	if s[len(s)-1] != ')' {
		return SQLiteNowNone
	}
	name := strings.ToLower(strings.TrimSpace(s[:open]))
	args, ok := sqliteStringArgs(s[open+1 : len(s)-1])
	if !ok {
		return SQLiteNowNone
	}
	switch {
	case len(args) == 1 && strings.EqualFold(args[0], "now"):
		switch name {
		case "datetime":
			return SQLiteNowDateTime
		case "date":
			return SQLiteNowDate
		case "time":
			return SQLiteNowTime
		}
	case len(args) == 2 && name == "strftime" && strings.EqualFold(args[1], "now"):
		return sqliteNowStrftimeFormats[args[0]]
	}
	return SQLiteNowNone
}

// sqliteStringArgs splits a comma-separated argument list whose every
// argument is a single string literal ('…' or "…", the quote doubled to
// escape it), returning the literals' contents. Anything else — a
// number, an identifier, a nested call, an empty list — is ok=false.
func sqliteStringArgs(list string) ([]string, bool) {
	var args []string
	i := 0
	for {
		for i < len(list) && isSQLiteSpace(list[i]) {
			i++
		}
		if i >= len(list) || (list[i] != '\'' && list[i] != '"') {
			return nil, false
		}
		q := list[i]
		i++
		var b strings.Builder
		for {
			if i >= len(list) {
				return nil, false
			}
			if list[i] == q {
				if i+1 < len(list) && list[i+1] == q {
					b.WriteByte(q)
					i += 2
					continue
				}
				i++
				break
			}
			b.WriteByte(list[i])
			i++
		}
		args = append(args, b.String())
		for i < len(list) && isSQLiteSpace(list[i]) {
			i++
		}
		if i == len(list) {
			return args, true
		}
		if list[i] != ',' {
			return nil, false
		}
		i++
	}
}

func isSQLiteSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

// sqliteParensWrapWhole reports whether s's leading '(' closes at its
// final byte, so `(a)+(b)` is not stripped. Parentheses inside string
// literals do not count.
func sqliteParensWrapWhole(s string) bool {
	depth := 0
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth == 0 && i != len(s)-1 {
				return false
			}
		}
	}
	return depth == 0
}
