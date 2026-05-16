// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// PostgreSQL dialect definitions for identifier re-quoting. The
// mechanism that consumes these sets lives in the shared
// internal/translate/exprident package (ADR-0045); the sets stay here
// because they are PG dialect definitions, correctly engine-owned.
// requotePGReservedIdents (the thin wrapper) is in
// exprident_shared.go.

package postgres

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
