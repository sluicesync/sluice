// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// PostgreSQL dialect definitions for identifier re-quoting. The
// mechanism that consumes these sets lives in the shared
// internal/translate/exprident package (ADR-0045); the sets stay here
// because they are PG dialect definitions, correctly engine-owned.
// requotePGReservedIdents (the thin wrapper) is in
// exprident_shared.go.

package postgres

import "github.com/orware/sluice/internal/translate/exprident"

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
	// NOTE: `FROM` is deliberately NOT here. It is grammar glue only in
	// specific positions (`IS [NOT] DISTINCT FROM`,
	// `EXTRACT/SUBSTRING/TRIM/OVERLAY(… FROM …)`) and is an ordinary
	// column identifier everywhere else. A blanket exclusion would
	// suppress re-quoting a de-quoted user column literally named
	// `from` (MySQL→PG `CHECK (`from` < `to`)` → SQLSTATE 42601). The
	// position-aware discrimination lives in pgExprContextualKeywords
	// (see exprident.ContextRule).
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

// pgExprContextualKeywords holds reserved words that are grammar glue
// ONLY in a specific syntactic position and are an ordinary column
// identifier everywhere else (see [exprident.ContextRule]).
//
// `FROM` is the sole entry. PostgreSQL's grammar permits a bare,
// unquoted `FROM` in an expression body in exactly two shapes:
//
//   - `IS [NOT] DISTINCT FROM` — `FROM` immediately follows `DISTINCT`.
//     This is the form the Bug 8b `<=>` rewrite emits
//     (`a IS NOT DISTINCT FROM b`); its `FROM` must stay bare.
//   - the special function syntaxes `EXTRACT(field FROM src)`,
//     `SUBSTRING(s FROM n [FOR len])`, `TRIM([…] FROM s)`,
//     `OVERLAY(s PLACING r FROM n [FOR len])` — `FROM` sits inside the
//     call's paren group. (sluice's own translator emits the EXTRACT
//     form; the others can arrive verbatim from an ANSI/MySQL source
//     expression — MySQL shares this `EXTRACT(… FROM …)` syntax.)
//
// In every other expression position a bare `FROM` can only be a
// de-quoted user column literally named `from`, which MUST be
// re-quoted or CREATE TABLE fails with SQLSTATE 42601. The blanket
// pgExprGrammarKeywords entry that used to live here suppressed that
// column case (a MySQL→PG regression for `CHECK (`from` < `to`)` and
// any generated-column / expression-index / DEFAULT referencing a
// `from` column); the position-aware rule fixes it without regressing
// the grammar-`FROM` shapes.
var pgExprContextualKeywords = map[string]exprident.ContextRule{
	"FROM": {
		AfterToken: map[string]struct{}{"DISTINCT": {}},
		InFunction: map[string]struct{}{
			"EXTRACT": {}, "SUBSTRING": {}, "TRIM": {}, "OVERLAY": {},
		},
	},
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
