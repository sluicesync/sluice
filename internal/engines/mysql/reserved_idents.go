// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "strings"

// requoteMySQLReservedIdents re-applies backtick quoting to bare
// identifiers in an expression body that are MySQL reserved words used
// as column references.
//
// Background (validation-rig catalog #5): the MySQL schema reader
// strips backtick identifier quotes from generated-column / CHECK /
// index expressions at the read boundary so the IR holds text portable
// to either engine's writer (Postgres rejects backticks). That strip
// is lossy for the narrow case of an identifier that is *also* a MySQL
// reserved word — e.g. a column named `order` or `key`. Once the
// backticks are gone, a same-engine MySQL target emits
// `if((discarded_at is null),order,NULL)` and MySQL's parser rejects
// the bare reserved word `order` with Error 1064.
//
// The fix is target-side, where MySQL's reserved-word set is known
// (target knowledge belongs in the writer, not the IR): walk the
// expression string-literal-aware and wrap any bare token that is a
// MySQL reserved word in backticks — *unless* the token is one of the
// expression-grammar keywords (operators, logical keywords, NULL/
// boolean literals, CAST type names, interval units). Those keywords
// can appear unquoted in a generated/CHECK expression body in their
// grammatical role and must not be quoted (quoting `IS`, `AND`, or the
// `NULL` literal would itself produce a syntax error). A token
// immediately followed by `(` is treated as a function call and left
// alone too — several reserved words (`LEFT`, `MOD`, `IF`) are also
// built-in function names.
//
// This is deliberately a small, mechanical pass: it only re-quotes
// what the reader would have had to strip, and only the reserved-word
// subset that is realistically a column identifier (never an operator
// or literal in expression position). Everything else is verbatim
// passthrough, consistent with the project's translation policy.
func requoteMySQLReservedIdents(expr string) string {
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
		case c == '`':
			// Already-quoted identifier — copy verbatim (including the
			// closing backtick, honouring doubled-backtick escapes).
			j := i + 1
			for j < len(expr) {
				if expr[j] == '`' {
					if j+1 < len(expr) && expr[j+1] == '`' {
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
		case isIdentStartByte(c):
			j := i
			for j < len(expr) && isIdentifierByte(expr[j]) {
				j++
			}
			tok := expr[i:j]
			callPos := j < len(expr) && expr[j] == '('
			if !callPos && shouldRequoteAsIdent(tok) {
				sb.WriteByte('`')
				sb.WriteString(tok)
				sb.WriteByte('`')
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

// shouldRequoteAsIdent reports whether tok (case-insensitive) is a
// MySQL reserved word that must be backtick-quoted when it appears as
// a column reference inside an expression — i.e. it's reserved AND it
// is not one of the expression-grammar keywords that legitimately
// appear unquoted in a generated/CHECK expression body.
func shouldRequoteAsIdent(tok string) bool {
	u := strings.ToUpper(tok)
	if _, excluded := exprGrammarKeywords[u]; excluded {
		return false
	}
	_, reserved := mysqlReservedWords[u]
	return reserved
}

// isIdentStartByte reports whether b can begin an unquoted SQL
// identifier (letter or underscore — not a digit, so numeric literals
// aren't mistaken for identifiers).
func isIdentStartByte(b byte) bool {
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

// exprGrammarKeywords is the subset of MySQL reserved words that can
// appear unquoted in an expression body in a grammatical role:
// logical / comparison operators, the NULL and boolean literals, CAST
// target type names, INTERVAL units, and CASE/control keywords.
// Re-quoting any of these would turn a valid expression into a syntax
// error (`x `IS` `NULL“ is not valid SQL). Everything in
// mysqlReservedWords that is NOT here is treated as a potential column
// identifier and re-quoted when it appears outside call position.
var exprGrammarKeywords = map[string]struct{}{
	// Logical / set / comparison operators.
	"AND": {}, "OR": {}, "XOR": {}, "NOT": {}, "IS": {}, "IN": {},
	"LIKE": {}, "REGEXP": {}, "RLIKE": {}, "BETWEEN": {}, "DIV": {},
	"MOD": {}, "DISTINCT": {}, "BINARY": {}, "COLLATE": {}, "ESCAPE": {},
	"SOUNDS": {},
	// Literals.
	"NULL": {}, "TRUE": {}, "FALSE": {},
	// CASE / control flow.
	"CASE": {}, "WHEN": {}, "THEN": {}, "ELSE": {}, "END": {},
	// CAST / CONVERT target type names + AS / USING glue.
	"AS": {}, "CAST": {}, "CONVERT": {}, "USING": {}, "CHAR": {},
	"CHARACTER": {}, "DEC": {}, "DECIMAL": {}, "NUMERIC": {},
	"SIGNED": {}, "UNSIGNED": {}, "INTEGER": {}, "INT": {},
	"FLOAT": {}, "DOUBLE": {}, "REAL": {}, "PRECISION": {},
	// Temporal keywords usable bare in expressions.
	"INTERVAL": {}, "CURRENT_DATE": {}, "CURRENT_TIME": {},
	"CURRENT_TIMESTAMP": {}, "LOCALTIME": {}, "LOCALTIMESTAMP": {},
	"UTC_DATE": {}, "UTC_TIME": {}, "UTC_TIMESTAMP": {},
	"CURRENT_USER": {}, "DEFAULT": {}, "VALUES": {},
}

// mysqlReservedWords is the MySQL 8.0 reserved-keyword set (the "(R)"
// entries from https://dev.mysql.com/doc/refman/8.0/en/keywords.html).
// Stored upper-cased; lookups upper-case the probe.
var mysqlReservedWords = map[string]struct{}{
	"ACCESSIBLE": {}, "ADD": {}, "ALL": {}, "ALTER": {}, "ANALYZE": {},
	"AND": {}, "AS": {}, "ASC": {}, "ASENSITIVE": {}, "BEFORE": {},
	"BETWEEN": {}, "BIGINT": {}, "BINARY": {}, "BLOB": {}, "BOTH": {},
	"BY": {}, "CALL": {}, "CASCADE": {}, "CASE": {}, "CHANGE": {},
	"CHAR": {}, "CHARACTER": {}, "CHECK": {}, "COLLATE": {}, "COLUMN": {},
	"CONDITION": {}, "CONSTRAINT": {}, "CONTINUE": {}, "CONVERT": {},
	"CREATE": {}, "CROSS": {}, "CUBE": {}, "CUME_DIST": {},
	"CURRENT_DATE": {}, "CURRENT_TIME": {}, "CURRENT_TIMESTAMP": {},
	"CURRENT_USER": {}, "CURSOR": {}, "DATABASE": {}, "DATABASES": {},
	"DAY_HOUR": {}, "DAY_MICROSECOND": {}, "DAY_MINUTE": {},
	"DAY_SECOND": {}, "DEC": {}, "DECIMAL": {}, "DECLARE": {},
	"DEFAULT": {}, "DELAYED": {}, "DELETE": {}, "DENSE_RANK": {},
	"DESC": {}, "DESCRIBE": {}, "DETERMINISTIC": {}, "DISTINCT": {},
	"DISTINCTROW": {}, "DIV": {}, "DOUBLE": {}, "DROP": {}, "DUAL": {},
	"EACH": {}, "ELSE": {}, "ELSEIF": {}, "EMPTY": {}, "ENCLOSED": {},
	"ESCAPED": {}, "EXCEPT": {}, "EXISTS": {}, "EXIT": {}, "EXPLAIN": {},
	"FALSE": {}, "FETCH": {}, "FIRST_VALUE": {}, "FLOAT": {},
	"FLOAT4": {}, "FLOAT8": {}, "FOR": {}, "FORCE": {}, "FOREIGN": {},
	"FROM": {}, "FULLTEXT": {}, "FUNCTION": {}, "GENERATED": {},
	"GET": {}, "GRANT": {}, "GROUP": {}, "GROUPING": {}, "GROUPS": {},
	"HAVING": {}, "HIGH_PRIORITY": {}, "HOUR_MICROSECOND": {},
	"HOUR_MINUTE": {}, "HOUR_SECOND": {}, "IF": {}, "IGNORE": {},
	"IN": {}, "INDEX": {}, "INFILE": {}, "INNER": {}, "INOUT": {},
	"INSENSITIVE": {}, "INSERT": {}, "INT": {}, "INT1": {}, "INT2": {},
	"INT3": {}, "INT4": {}, "INT8": {}, "INTEGER": {}, "INTERSECT": {},
	"INTERVAL": {}, "INTO": {}, "IO_AFTER_GTIDS": {},
	"IO_BEFORE_GTIDS": {}, "IS": {}, "ITERATE": {}, "JOIN": {},
	"JSON_TABLE": {}, "KEY": {}, "KEYS": {}, "KILL": {}, "LAG": {},
	"LAST_VALUE": {}, "LATERAL": {}, "LEAD": {}, "LEADING": {},
	"LEAVE": {}, "LEFT": {}, "LIKE": {}, "LIMIT": {}, "LINEAR": {},
	"LINES": {}, "LOAD": {}, "LOCALTIME": {}, "LOCALTIMESTAMP": {},
	"LOCK": {}, "LONG": {}, "LONGBLOB": {}, "LONGTEXT": {}, "LOOP": {},
	"LOW_PRIORITY": {}, "MASTER_BIND": {},
	"MASTER_SSL_VERIFY_SERVER_CERT": {}, "MATCH": {}, "MAXVALUE": {},
	"MEDIUMBLOB": {}, "MEDIUMINT": {}, "MEDIUMTEXT": {}, "MIDDLEINT": {},
	"MINUTE_MICROSECOND": {}, "MINUTE_SECOND": {}, "MOD": {},
	"MODIFIES": {}, "NATURAL": {}, "NOT": {},
	"NO_WRITE_TO_BINLOG": {}, "NTH_VALUE": {}, "NTILE": {}, "NULL": {},
	"NUMERIC": {}, "OF": {}, "ON": {}, "OPTIMIZE": {}, "OPTIMIZER_COSTS": {},
	"OPTION": {}, "OPTIONALLY": {}, "OR": {}, "ORDER": {}, "OUT": {},
	"OUTER": {}, "OUTFILE": {}, "OVER": {}, "PARTITION": {},
	"PERCENT_RANK": {}, "PRECISION": {}, "PRIMARY": {}, "PROCEDURE": {},
	"PURGE": {}, "RANGE": {}, "RANK": {}, "READ": {}, "READS": {},
	"READ_WRITE": {}, "REAL": {}, "RECURSIVE": {}, "REFERENCES": {},
	"REGEXP": {}, "RELEASE": {}, "RENAME": {}, "REPEAT": {},
	"REPLACE": {}, "REQUIRE": {}, "RESIGNAL": {}, "RESTRICT": {},
	"RETURN": {}, "REVOKE": {}, "RIGHT": {}, "RLIKE": {}, "ROW": {},
	"ROWS": {}, "ROW_NUMBER": {}, "SCHEMA": {}, "SCHEMAS": {},
	"SECOND_MICROSECOND": {}, "SELECT": {}, "SENSITIVE": {},
	"SEPARATOR": {}, "SET": {}, "SHOW": {}, "SIGNAL": {}, "SMALLINT": {},
	"SPATIAL": {}, "SPECIFIC": {}, "SQL": {}, "SQLEXCEPTION": {},
	"SQLSTATE": {}, "SQLWARNING": {}, "SQL_BIG_RESULT": {},
	"SQL_CALC_FOUND_ROWS": {}, "SQL_SMALL_RESULT": {}, "SSL": {},
	"STARTING": {}, "STORED": {}, "STRAIGHT_JOIN": {}, "SYSTEM": {},
	"TABLE": {}, "TERMINATED": {}, "THEN": {}, "TINYBLOB": {},
	"TINYINT": {}, "TINYTEXT": {}, "TO": {}, "TRAILING": {},
	"TRIGGER": {}, "TRUE": {}, "UNDO": {}, "UNION": {}, "UNIQUE": {},
	"UNLOCK": {}, "UNSIGNED": {}, "UPDATE": {}, "USAGE": {}, "USE": {},
	"USING": {}, "UTC_DATE": {}, "UTC_TIME": {}, "UTC_TIMESTAMP": {},
	"VALUES": {}, "VARBINARY": {}, "VARCHAR": {}, "VARCHARACTER": {},
	"VARYING": {}, "VIRTUAL": {}, "WHEN": {}, "WHERE": {}, "WHILE": {},
	"WINDOW": {}, "WITH": {}, "WRITE": {}, "XOR": {}, "YEAR_MONTH": {},
	"ZEROFILL": {},
}
