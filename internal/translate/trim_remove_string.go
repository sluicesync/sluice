// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"fmt"
	"regexp"
	"strings"

	"sluicesync.dev/sluice/internal/ir"

	"sluicesync.dev/sluice/internal/translate/exprident"
)

// TRIM's remove-string means different things on MySQL and Postgres (Bug 312).
//
// MySQL strips the remove-string as a repeated SUBSTRING; Postgres strips any
// CHARACTER in it, treating it as a set. Asked directly, without sluice:
//
//	MySQL 8.0:   SELECT TRIM(BOTH 'xy' FROM 'yxAyx')   → 'yxAyx'
//	Postgres 16: SELECT TRIM(BOTH 'xy' FROM 'yxAyx')   → 'A'
//
// Both spell the call identically and both parse it, so an expression that
// crosses between the two families verbatim computes a different value at
// exit 0. The two readings agree exactly when the remove-string is zero or one
// unit long: a one-unit substring and a one-unit set are the same thing. The
// unit is a character on a character operand and a BYTE on a binary one
// (VARBINARY/BLOB on MySQL, bytea on Postgres), and nothing here knows the
// operand's type, so the safe set is a single string literal of at most one
// BYTE. A one-character multibyte literal is two or more bytes, and over a
// binary operand MySQL strips the byte sequence while Postgres strips any one
// of its bytes (found by the Bug 312 review, reproduced on real servers). A
// column (its value is unknown until a row arrives), an expression, or a
// longer literal is not safe either.
//
// The likeliest real trigger is the Postgres whitespace-set idiom,
// `trim(both E' \t\r\n' from s)`, which on MySQL strips only exact repeats of
// that four-character run at either end, and usually nothing.

// TrimRemoveStringDiverges returns the first TRIM call in expr whose
// remove-string MySQL and Postgres would read differently, and true; or "" and
// false when every TRIM in expr is safe to carry across the two families.
//
// dialect is the dialect expr is written in ("mysql" or "postgres"); it
// decides one thing, how a bare BOTH/LEADING/TRAILING right before FROM is
// read. Postgres quotes a column with one of those names, so there the bare
// word is the keyword with no remove-string (pg_get_expr prints trim(s) as
// TRIM(BOTH FROM s)). MySQL never stores the keyword without a remove-string,
// and sluice's MySQL reader strips identifier backticks, so there the bare
// word is a column (the Bug 311 positional rule, [exprident.ContextRule]
// LeadsOperand).
//
// It is deliberately conservative: an escape-string (E-prefixed) remove-string, or a
// literal followed by anything other than a plain cast, is reported as
// divergent rather than decoded. The cost of that is a refusal on a
// one-character E'\\' remove-string; the cost of the other mistake is a
// silently wrong value.
func TrimRemoveStringDiverges(expr, dialect string) (call string, diverges bool) {
	for i := 0; i < len(expr); {
		c := expr[i]
		switch {
		case c == '\'':
			i = exprident.ScanStringLiteral(expr, i)
		case c == '"' || c == '`':
			i = skipQuotedIdent(expr, i)
		case exprident.IsIdentStartByte(c):
			j := i
			for j < len(expr) && exprident.IsIdentifierByte(expr[j]) {
				j++
			}
			// A qualified name (schema.trim) is not the builtin.
			qualified := i > 0 && expr[i-1] == '.'
			if !qualified && strings.EqualFold(expr[i:j], "TRIM") {
				k := skipSpace(expr, j)
				if k < len(expr) && expr[k] == '(' {
					if end, ok := exprident.ScanParenGroup(expr, k); ok &&
						!trimRemoveStringSafe(expr[k+1:end], dialect) {
						return expr[i : end+1], true
					}
				}
			}
			// Continue INSIDE the call, so a TRIM nested in an argument is
			// examined too.
			i = j
		default:
			i++
		}
	}
	return "", false
}

// TrimRemoveStringMarker is the grep-stable marker every Bug 312 refusal
// carries.
const TrimRemoveStringMarker = "TRIM-REMOVE-STRING-SEMANTICS"

// RefuseDivergentTrim returns a loud refusal when expr, written in
// sourceDialect and bound for targetEngine, carries a TRIM whose remove-string
// the two families read differently ([TrimRemoveStringDiverges]); nil
// otherwise. kind is "generated column", "CHECK constraint", "view" or "unique
// index"; table and name locate it (table may be empty).
//
// Callers pass only the dialect their translator accepts from the OTHER
// family (a Postgres target passes "mysql", a MySQL target "postgres"); a
// same-family body is never divergent and must not be asked.
func RefuseDivergentTrim(kind, table, name, expr, sourceDialect, targetEngine string) error {
	call, diverges := TrimRemoveStringDiverges(expr, sourceDialect)
	if !diverges {
		return nil
	}
	remedy := "drop it on the source and re-create an equivalent one on the target after the run, or skip the table with `--exclude-table`"
	switch kind {
	case "generated column":
		remedy = "supply a target-valid body with `--expr-override " + table + "." + name + "=<expression>`, or skip the table with `--exclude-table`"
	case "view":
		remedy = "drop the view on the source before the run and re-create an equivalent one on the target after it"
	}
	where := ""
	if table != "" {
		where = fmt.Sprintf(" on table %q", table)
	}
	return fmt.Errorf(
		"%s: %s %q%s uses %s, and %s reads TRIM's remove-string differently from %s: "+
			"MySQL strips it as a repeated substring, Postgres strips any character in it as a set "+
			"(TRIM(BOTH 'xy' FROM 'yxAyx') is 'yxAyx' on MySQL and 'A' on Postgres). Carried across "+
			"verbatim it would compute different values at exit 0. The two agree only for a remove-string "+
			"that is a literal of at most one byte. Remedy: %s",
		TrimRemoveStringMarker, kind, name, where, call, targetEngine, sourceDialect, remedy,
	)
}

// RefuseDivergentTrimIndex applies [RefuseDivergentTrim] to every
// sourceDialect expression key part of idx when idx enforces uniqueness
// (Unique or ConstraintBacked).
//
// Only a UNIQUE index is asked, because only uniqueness changes what rows the
// target accepts: a non-unique expression index is a lookup structure, and a
// query on the target computes the same target-side expression the index
// stores. A unique one does not diverge harmlessly in either direction. Postgres
// → MySQL, rows the source's unique key refuses (`'xA'` and `'yA'` under
// `trim(both 'xy' from s)`) are both accepted on the target, so the key is
// silently weaker after cutover, which is the CHECK harm. MySQL → Postgres the
// target collides on rows the source keyed apart, which is loud, and refusing
// it up front only moves that failure ahead of the copy.
func RefuseDivergentTrimIndex(table string, idx *ir.Index, sourceDialect, targetEngine string) error {
	if idx == nil || (!idx.Unique && !idx.ConstraintBacked) {
		return nil
	}
	for _, c := range idx.Columns {
		if c.Expression == "" || c.ExpressionDialect != sourceDialect {
			continue
		}
		if err := RefuseDivergentTrim("unique index", table, idx.Name, c.Expression, sourceDialect, targetEngine); err != nil {
			return err
		}
	}
	return nil
}

// trimRemoveStringSafe reports whether the argument list of one TRIM call
// (the text between its parentheses) has no remove-string, or a remove-string
// both families read identically.
func trimRemoveStringSafe(args, dialect string) bool {
	from, ok := topLevelFrom(args)
	if !ok {
		// TRIM(s): no remove-string, spaces only on both families.
		return true
	}
	start := skipSpace(args, 0)
	if word, next := wordAt(args, start); isTrimSideKeyword(word) {
		nextTok := skipSpace(args, next)
		switch {
		case nextTok != from:
			// TRIM(BOTH <remove-string> FROM s): the word is grammar.
			start = nextTok
		case strings.EqualFold(dialect, "postgres"):
			// TRIM(BOTH FROM s) in Postgres text: the keyword, no
			// remove-string.
			return true
		}
		// Otherwise, MySQL text: the bare word before FROM is a column and
		// is itself the remove-string.
	}
	return singleCharLiteral(strings.TrimSpace(args[start:from]))
}

// plainCastSuffix matches a chain of casts to a VARIABLE-length text type
// ('x'::text, 'x'::character varying, 'x'::varchar(1)), the casts pg_get_expr
// appends to a text literal, and nothing else. An allowlist on purpose: a cast
// can change the remove-string before TRIM sees it. The fixed-length types are
// the measured case (Bug 312's second review, real servers): ' '::bpchar or
// ' '::character(1) drops its trailing space when Postgres converts it back to
// text, so Postgres trims nothing while MySQL trims spaces.
var plainCastSuffix = regexp.MustCompile(`(?i)^(\s*::\s*(text|varchar|character\s+varying)(\(\s*\d+\s*\))?)*\s*$`)

// singleCharLiteral reports whether s is exactly one standard single-quoted
// string literal of at most one byte, optionally followed by plain
// casts. An empty s (a remove-string that is not there) is safe too.
func singleCharLiteral(s string) bool {
	if s == "" {
		return true
	}
	if s[0] != '\'' {
		return false
	}
	end := exprident.ScanStringLiteral(s, 0)
	if end > len(s) || end < 2 || s[end-1] != '\'' {
		return false
	}
	if !plainCastSuffix.MatchString(s[end:]) {
		return false
	}
	// One BYTE, not one character: see the comment at the top of this file.
	body := strings.ReplaceAll(s[1:end-1], "''", "'")
	return len(body) <= 1
}

// topLevelFrom returns the index of the first FROM keyword in args at paren
// depth zero, outside string literals and quoted identifiers.
func topLevelFrom(args string) (int, bool) {
	depth := 0
	for i := 0; i < len(args); {
		c := args[i]
		switch {
		case c == '\'':
			i = exprident.ScanStringLiteral(args, i)
		case c == '"' || c == '`':
			i = skipQuotedIdent(args, i)
		case c == '(':
			depth++
			i++
		case c == ')':
			depth--
			i++
		case exprident.IsIdentStartByte(c):
			word, next := wordAt(args, i)
			if depth == 0 && strings.EqualFold(word, "FROM") {
				return i, true
			}
			i = next
		default:
			i++
		}
	}
	return 0, false
}

// isTrimSideKeyword reports whether word is one of TRIM's side keywords.
func isTrimSideKeyword(word string) bool {
	switch strings.ToUpper(word) {
	case "BOTH", "LEADING", "TRAILING":
		return true
	}
	return false
}

// wordAt returns the bare identifier-like word starting at s[i] ("" when none
// starts there) and the index after it.
func wordAt(s string, i int) (word string, next int) {
	if i >= len(s) || !exprident.IsIdentStartByte(s[i]) {
		return "", i
	}
	j := i
	for j < len(s) && exprident.IsIdentifierByte(s[j]) {
		j++
	}
	return s[i:j], j
}

// skipSpace returns the index of the first non-whitespace byte at or after i.
func skipSpace(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

// skipQuotedIdent returns the index just past the quoted identifier opening at
// s[i] (a doubled quote character is an escaped one), or len(s) when it is
// unterminated.
func skipQuotedIdent(s string, i int) int {
	q := s[i]
	for j := i + 1; j < len(s); j++ {
		if s[j] != q {
			continue
		}
		if j+1 < len(s) && s[j+1] == q {
			j++
			continue
		}
		return j + 1
	}
	return len(s)
}
