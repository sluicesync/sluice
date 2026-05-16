// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import "strings"

// requotePGReservedIdents re-applies double-quote quoting to bare
// identifiers in a cross-engine expression body that are PostgreSQL
// reserved words used as column references.
//
// Background (validation-rig catalog Bug 63): when the source dialect
// is not "postgres", a generated-column / CHECK / index expression
// arrives in the IR spelled in the source engine's dialect with that
// engine's identifier quotes stripped at the read boundary (the MySQL
// reader strips backticks so the IR text is portable). The PG writer's
// cross-dialect translator (translateExprForPG) rewrites function and
// operator spellings but does NOT re-quote bare identifiers — so a
// MySQL source column named `order` or `key` lands in the PG generated-
// column body as the bare token `order` / `key`. `order` is a PG
// reserved word, so CREATE TABLE fails with
// `ERROR: syntax error at or near "order"` (SQLSTATE 42601).
//
// This is the PG-writer analogue of the MySQL writer's
// requoteMySQLReservedIdents (validation-rig catalog #5). The fix is
// target-side, where PG's reserved-word set is known (target knowledge
// belongs in the writer, not the IR): walk the expression string-
// literal-aware and wrap any bare token that is a PG reserved word in
// double quotes — *unless* the token is one of the expression-grammar
// keywords (operators, logical keywords, NULL/boolean literals, CAST
// type names, CASE/control keywords) that can legitimately appear
// unquoted in a generated/CHECK/index expression body. A token
// immediately followed by `(` is treated as a function/type call and
// left alone (several reserved words double as built-in function or
// type names).
//
// Same-engine PG→PG never reaches this helper — the PG reader returns
// pg_get_expr output with reserved-word column refs already correctly
// double-quoted, and the writer's same-dialect path emits that text
// verbatim. The helper is invoked only on the cross-dialect branch.
//
// Deliberately a small, mechanical pass: it only re-quotes the
// reserved-word subset that is realistically a column identifier
// (never an operator or literal in expression position). Everything
// else is verbatim passthrough, consistent with the project's
// translation policy.
func requotePGReservedIdents(expr string) string {
	if expr == "" {
		return expr
	}
	var sb strings.Builder
	sb.Grow(len(expr) + 8)
	for i := 0; i < len(expr); {
		c := expr[i]
		switch {
		case c == '\'':
			// String literal — copy verbatim.
			end := scanStringLiteral(expr, i)
			sb.WriteString(expr[i:end])
			i = end
		case c == '"':
			// Already double-quoted identifier — copy verbatim
			// (including the closing quote, honouring doubled-quote
			// escapes).
			j := i + 1
			for j < len(expr) {
				if expr[j] == '"' {
					if j+1 < len(expr) && expr[j+1] == '"' {
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
		case isPGIdentStartByte(c):
			j := i
			for j < len(expr) && isIdentifierByte(expr[j]) {
				j++
			}
			tok := expr[i:j]
			// Look past any whitespace to see if this is a call /
			// type-name position (`coalesce (...)`, `numeric (...)`).
			k := j
			for k < len(expr) && (expr[k] == ' ' || expr[k] == '\t') {
				k++
			}
			callPos := k < len(expr) && expr[k] == '('
			if !callPos && shouldRequotePGIdent(tok) {
				sb.WriteByte('"')
				sb.WriteString(tok)
				sb.WriteByte('"')
			} else {
				sb.WriteString(tok)
			}
			i = j
		default:
			sb.WriteByte(c)
			i++
		}
	}
	return sb.String()
}

// shouldRequotePGIdent reports whether tok (case-insensitive) is a
// PostgreSQL reserved word that must be double-quoted when it appears
// as a column reference inside an expression — i.e. it's reserved AND
// it is not one of the expression-grammar keywords that legitimately
// appear unquoted in a generated/CHECK/index expression body.
func shouldRequotePGIdent(tok string) bool {
	u := strings.ToUpper(tok)
	if _, excluded := pgExprGrammarKeywords[u]; excluded {
		return false
	}
	_, reserved := pgReservedWords[u]
	return reserved
}

// isPGIdentStartByte reports whether b can begin an unquoted SQL
// identifier (letter or underscore — not a digit, so numeric literals
// aren't mistaken for identifiers).
func isPGIdentStartByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z':
		return true
	case b >= 'A' && b <= 'Z':
		return true
	case b == '_':
		return true
	}
	return false
}

// pgExprGrammarKeywords is the subset of PG reserved words that can
// appear unquoted in an expression body in a grammatical role:
// logical / comparison operators, the NULL and boolean literals, CAST
// target type names, CASE/control keywords, and the temporal keywords
// usable bare in expressions. Re-quoting any of these would turn a
// valid expression into a syntax error (`x "IS" "NULL"` is not valid
// SQL). Everything in pgReservedWords that is NOT here is treated as a
// potential column identifier and re-quoted when it appears outside
// call position.
var pgExprGrammarKeywords = map[string]struct{}{
	// Logical / set / comparison operators.
	"AND": {}, "OR": {}, "NOT": {}, "IS": {}, "IN": {},
	"LIKE": {}, "ILIKE": {}, "SIMILAR": {}, "BETWEEN": {},
	"DISTINCT": {}, "SYMMETRIC": {}, "ANY": {}, "ALL": {},
	"SOME": {}, "OVERLAPS": {}, "ISNULL": {}, "NOTNULL": {},
	"ESCAPE": {}, "COLLATE": {},
	// Literals.
	"NULL": {}, "TRUE": {}, "FALSE": {}, "UNKNOWN": {},
	// CASE / control flow.
	"CASE": {}, "WHEN": {}, "THEN": {}, "ELSE": {}, "END": {},
	// CAST / type-name glue + CAST target type names.
	"AS": {}, "CAST": {}, "CHAR": {}, "CHARACTER": {},
	"VARCHAR": {}, "VARYING": {}, "NUMERIC": {}, "DECIMAL": {},
	"DEC": {}, "INTEGER": {}, "INT": {}, "SMALLINT": {},
	"BIGINT": {}, "REAL": {}, "FLOAT": {}, "DOUBLE": {},
	"PRECISION": {}, "BOOLEAN": {}, "BIT": {}, "NATIONAL": {},
	"NCHAR": {}, "TIME": {}, "TIMESTAMP": {}, "INTERVAL": {},
	"WITH": {}, "WITHOUT": {}, "ZONE": {}, "DATE": {},
	"XMLPARSE": {}, "XMLSERIALIZE": {},
	// Temporal / session keywords usable bare in expressions.
	"CURRENT_DATE": {}, "CURRENT_TIME": {}, "CURRENT_TIMESTAMP": {},
	"CURRENT_USER": {}, "CURRENT_ROLE": {}, "CURRENT_CATALOG": {},
	"CURRENT_SCHEMA": {}, "LOCALTIME": {}, "LOCALTIMESTAMP": {},
	"SESSION_USER": {}, "SYSTEM_USER": {}, "ARRAY": {}, "ROW": {},
}

// pgReservedWords is the PostgreSQL reserved-keyword set: the keywords
// PG's grammar classifies as "reserved" or "reserved (can be
// function/type name)" — i.e. words that CANNOT be used as a bare
// column or table identifier without double-quoting. Drawn from PG 16's
// keyword table (https://www.postgresql.org/docs/16/sql-keywords-
// appendix.html), filtered to the entries an expression body could
// realistically carry as a column reference. Stored upper-cased;
// lookups upper-case the probe.
//
// Note: words that also appear in pgExprGrammarKeywords (NULL, AND,
// CASE, CAST type names, …) are excluded from re-quoting by
// shouldRequotePGIdent even though they are listed here / are reserved
// — keeping both maps complete keeps the classification readable.
var pgReservedWords = map[string]struct{}{
	"ALL": {}, "ANALYSE": {}, "ANALYZE": {}, "AND": {}, "ANY": {},
	"ARRAY": {}, "AS": {}, "ASC": {}, "ASYMMETRIC": {}, "AUTHORIZATION": {},
	"BINARY": {}, "BOTH": {}, "CASE": {}, "CAST": {}, "CHECK": {},
	"COLLATE": {}, "COLLATION": {}, "COLUMN": {}, "CONCURRENTLY": {},
	"CONSTRAINT": {}, "CREATE": {}, "CROSS": {}, "CURRENT_CATALOG": {},
	"CURRENT_DATE": {}, "CURRENT_ROLE": {}, "CURRENT_SCHEMA": {},
	"CURRENT_TIME": {}, "CURRENT_TIMESTAMP": {}, "CURRENT_USER": {},
	"DEFAULT": {}, "DEFERRABLE": {}, "DESC": {}, "DISTINCT": {},
	"DO": {}, "ELSE": {}, "END": {}, "EXCEPT": {}, "FALSE": {},
	"FETCH": {}, "FOR": {}, "FOREIGN": {}, "FREEZE": {}, "FROM": {},
	"FULL": {}, "GRANT": {}, "GROUP": {}, "HAVING": {}, "ILIKE": {},
	"IN": {}, "INITIALLY": {}, "INNER": {}, "INTERSECT": {}, "INTO": {},
	"IS": {}, "ISNULL": {}, "JOIN": {}, "LATERAL": {}, "LEADING": {},
	"LEFT": {}, "LIKE": {}, "LIMIT": {}, "LOCALTIME": {},
	"LOCALTIMESTAMP": {}, "NATURAL": {}, "NOT": {}, "NOTNULL": {},
	"NULL": {}, "OFFSET": {}, "ON": {}, "ONLY": {}, "OR": {},
	"ORDER": {}, "OUTER": {}, "OVERLAPS": {}, "PLACING": {},
	"PRIMARY": {}, "REFERENCES": {}, "RETURNING": {}, "RIGHT": {},
	"SELECT": {}, "SESSION_USER": {}, "SIMILAR": {}, "SOME": {},
	"SYMMETRIC": {}, "SYSTEM_USER": {}, "TABLE": {}, "TABLESAMPLE": {},
	"THEN": {}, "TO": {}, "TRAILING": {}, "TRUE": {}, "UNION": {},
	"UNIQUE": {}, "USER": {}, "USING": {}, "VARIADIC": {}, "VERBOSE": {},
	"WHEN": {}, "WHERE": {}, "WINDOW": {}, "WITH": {},
	// "Reserved (can be function or type name)" — still illegal as a
	// bare column identifier in expression position, so re-quote.
	"BETWEEN": {}, "BIGINT": {}, "BIT": {}, "BOOLEAN": {}, "CHAR": {},
	"CHARACTER": {}, "COALESCE": {}, "DEC": {}, "DECIMAL": {},
	"EXISTS": {}, "EXTRACT": {}, "FLOAT": {}, "GREATEST": {},
	"GROUPING": {}, "INOUT": {}, "INT": {}, "INTEGER": {},
	"INTERVAL": {}, "LEAST": {}, "NATIONAL": {}, "NCHAR": {},
	"NONE": {}, "NORMALIZE": {}, "NULLIF": {}, "NUMERIC": {},
	"OUT": {}, "OVERLAY": {}, "POSITION": {}, "PRECISION": {},
	"REAL": {}, "ROW": {}, "SETOF": {}, "SMALLINT": {},
	"SUBSTRING": {}, "TIME": {}, "TIMESTAMP": {}, "TREAT": {},
	"TRIM": {}, "VALUES": {}, "VARCHAR": {}, "XMLATTRIBUTES": {},
	"XMLCONCAT": {}, "XMLELEMENT": {}, "XMLEXISTS": {}, "XMLFOREST": {},
	"XMLNAMESPACES": {}, "XMLPARSE": {}, "XMLPI": {}, "XMLROOT": {},
	"XMLSERIALIZE": {}, "XMLTABLE": {},
}
