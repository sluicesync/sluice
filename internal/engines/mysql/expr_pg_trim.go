// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"

	"sluicesync.dev/sluice/internal/translate/exprident"
)

// pgTrimKeywordOnly maps the TRIM keyword Postgres prints with no
// remove-string to the MySQL function that trims the same side.
var pgTrimKeywordOnly = map[string]string{
	"BOTH":     "TRIM(",
	"LEADING":  "LTRIM(",
	"TRAILING": "RTRIM(",
}

// rewritePGTrimKeywordOnly rewrites Postgres's `TRIM(BOTH FROM x)`,
// `TRIM(LEADING FROM x)` and `TRIM(TRAILING FROM x)` to MySQL's `TRIM(x)`,
// `LTRIM(x)` and `RTRIM(x)`. pg_get_expr prints trim(x) as
// `TRIM(BOTH FROM x)`, a form MySQL accepts in a statement but cannot
// store: measured on MySQL 8.0.46 (2026-10-15), a generated column, CHECK
// or functional index using it fails CREATE TABLE with ERROR 1064, because
// the server's own re-print of the expression does not parse. So every
// Postgres → MySQL table with trim(x) in one of those positions was
// refused. All three sides trim spaces only on both engines, so the
// rewrite is exact. A TRIM with a remove-string (`TRIM(BOTH 'x' FROM s)`,
// or a quoted column, which pg_get_expr always quotes when it is named
// both/leading/trailing) is left alone. Text inside string literals and
// quoted identifiers is never matched.
func rewritePGTrimKeywordOnly(expr string) string {
	var sb strings.Builder
	for i := 0; i < len(expr); {
		c := expr[i]
		switch {
		case c == '\'':
			end := exprident.ScanStringLiteral(expr, i)
			sb.WriteString(expr[i:end])
			i = end
		case c == '`' || c == '"':
			j := i + 1
			for j < len(expr) {
				if expr[j] == c {
					if j+1 < len(expr) && expr[j+1] == c {
						j += 2
						continue
					}
					j++
					break
				}
				j++
			}
			sb.WriteString(expr[i:j])
			i = j
		case exprident.IsIdentStartByte(c):
			j := i
			for j < len(expr) && exprident.IsIdentifierByte(expr[j]) {
				j++
			}
			if strings.EqualFold(expr[i:j], "TRIM") {
				if repl, next, ok := pgTrimKeywordOnlyHead(expr, j); ok {
					sb.WriteString(repl)
					i = next
					continue
				}
			}
			sb.WriteString(expr[i:j])
			i = j
		default:
			sb.WriteByte(c)
			i++
		}
	}
	return sb.String()
}

// pgTrimKeywordOnlyHead matches `( KEYWORD FROM ` after a TRIM token ending
// at expr[end], returning the replacement head and the index after it.
func pgTrimKeywordOnlyHead(expr string, end int) (repl string, next int, ok bool) {
	k := skipExprSpace(expr, end)
	if k >= len(expr) || expr[k] != '(' {
		return "", 0, false
	}
	kw, k := exprWordAt(expr, skipExprSpace(expr, k+1))
	repl, ok = pgTrimKeywordOnly[strings.ToUpper(kw)]
	if !ok {
		return "", 0, false
	}
	from, k := exprWordAt(expr, skipExprSpace(expr, k))
	if !strings.EqualFold(from, "FROM") {
		return "", 0, false
	}
	return repl, skipExprSpace(expr, k), true
}

// skipExprSpace returns the index of the first non-whitespace byte at or
// after i.
func skipExprSpace(expr string, i int) int {
	for i < len(expr) && (expr[i] == ' ' || expr[i] == '\t' || expr[i] == '\n' || expr[i] == '\r') {
		i++
	}
	return i
}

// exprWordAt returns the bare identifier-like word starting at i ("" when
// none starts there) and the index after it.
func exprWordAt(expr string, i int) (word string, next int) {
	if i >= len(expr) || !exprident.IsIdentStartByte(expr[i]) {
		return "", i
	}
	j := i
	for j < len(expr) && exprident.IsIdentifierByte(expr[j]) {
		j++
	}
	return expr[i:j], j
}
