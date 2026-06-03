// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

// Generated-column-references-generated-column preflight (catalog
// Bug 9). MySQL permits a generated column's expression to reference
// another generated column in the same table; PostgreSQL forbids it
// and rejects the CREATE TABLE with SQLSTATE 42P17 ("cannot use
// generated column ... in column generation expression"). Without this
// scanner the failure surfaces raw, mid-create-tables, AFTER other
// tables already migrated — a loud-failure-*quality* gap (no silent
// corruption, but not the clean up-front refusal the loud-failure
// tenet requires). This refuses at BOTH `schema preview` and `migrate`
// preflight, before any DDL is applied, naming the sites and the
// `--expr-override` / `--exclude-table` remedies.
//
// It is engine-neutral IR analysis (no engine import) like the
// sibling gaps.go / pgvalidity.go scanners, and scoped to cross-engine
// MySQL → Postgres exactly like ScanMySQLToPGGaps: a PostgreSQL source
// cannot contain the construct (PG forbids it), and MySQL → MySQL is
// fine, so other pairs short-circuit to nil.

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate/exprident"
)

// GeneratedColRefGeneratedCol is one detected site where a generated
// column's generation expression references another generated column
// in the same table.
type GeneratedColRefGeneratedCol struct {
	// Table is the source-side table name.
	Table string
	// Column is the referencing generated column.
	Column string
	// ReferencedColumn is the generated column it references.
	ReferencedColumn string
	// Expression is the referencing column's raw generation expr.
	Expression string
}

// ScanGeneratedColRefGeneratedCol returns every generated column whose
// generation expression references another generated column in the
// same table. Cross-engine MySQL → Postgres only; returns nil for any
// other engine pair or a nil schema (mirrors ScanMySQLToPGGaps's
// scoping exactly). Results are sorted by (table, column,
// referenced-column) for a stable refusal message.
func ScanGeneratedColRefGeneratedCol(
	schema *ir.Schema,
	sourceEngine, targetEngine string,
) []GeneratedColRefGeneratedCol {
	if schema == nil {
		return nil
	}
	if !strings.EqualFold(sourceEngine, "mysql") || !strings.EqualFold(targetEngine, "postgres") {
		return nil
	}

	var out []GeneratedColRefGeneratedCol
	for _, tbl := range schema.Tables {
		if tbl == nil {
			continue
		}
		// Generated-column names for this table, lower-cased →
		// canonical. SQL identifier matching here is case-insensitive
		// (MySQL column-name matching is, and PG lower-cases unquoted
		// identifiers).
		gen := make(map[string]string)
		for _, col := range tbl.Columns {
			if col != nil && col.GeneratedExpr != "" {
				gen[strings.ToLower(col.Name)] = col.Name
			}
		}
		if len(gen) < 2 {
			// Need at least two generated columns for one to reference
			// another.
			continue
		}
		for _, col := range tbl.Columns {
			if col == nil || col.GeneratedExpr == "" {
				continue
			}
			if !strings.EqualFold(col.GeneratedExprDialect, "mysql") {
				continue
			}
			self := strings.ToLower(col.Name)
			seen := make(map[string]bool)
			for _, ref := range bareIdentifierRefs(col.GeneratedExpr) {
				lr := strings.ToLower(ref)
				if lr == self || seen[lr] {
					continue
				}
				canonical, ok := gen[lr]
				if !ok {
					continue
				}
				seen[lr] = true
				out = append(out, GeneratedColRefGeneratedCol{
					Table:            tbl.Name,
					Column:           col.Name,
					ReferencedColumn: canonical,
					Expression:       col.GeneratedExpr,
				})
			}
		}
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Table != out[j].Table {
			return out[i].Table < out[j].Table
		}
		if out[i].Column != out[j].Column {
			return out[i].Column < out[j].Column
		}
		return out[i].ReferencedColumn < out[j].ReferencedColumn
	})
	return out
}

// bareIdentifierRefs returns the bare identifier tokens in expr that
// are column references — i.e. NOT a function-call name (a token
// immediately followed, modulo whitespace, by `(`) and NOT inside a
// single-quoted string literal. This is the same string-literal-aware
// walk discipline as pgvalidity.go's scanFunctionCallIdents, inverted:
// here we want the column-reference idents, not the call idents. A
// qualifier in a `qualifier.col` reference is harmlessly returned too
// — it simply won't match any generated-column name, while the `col`
// tail still matches (a qualified reference to a generated column is
// equally forbidden by PG, so capturing the tail is correct).
func bareIdentifierRefs(expr string) []string {
	var out []string
	for i := 0; i < len(expr); {
		c := expr[i]
		if c == '\'' {
			i = exprident.ScanStringLiteral(expr, i)
			continue
		}
		if !exprident.IsIdentStartByte(c) {
			i++
			continue
		}
		start := i
		j := i + 1
		for j < len(expr) && exprident.IsIdentifierByte(expr[j]) {
			j++
		}
		word := expr[start:j]
		k := j
		for k < len(expr) && (expr[k] == ' ' || expr[k] == '\t' || expr[k] == '\n' || expr[k] == '\r') {
			k++
		}
		if k >= len(expr) || expr[k] != '(' {
			out = append(out, word)
		}
		i = j
	}
	return out
}

// RefuseOnGeneratedColRefGeneratedCol returns a non-nil,
// operator-actionable error when scanning schema for the given
// MySQL→PG pair surfaces one or more generated columns that reference
// another generated column in the same table; nil otherwise. The
// error names every offending site and the `--expr-override` /
// `--exclude-table` remedies so the operator can act before any data
// or DDL moves. contextID is the caller's phase label
// ("schema preview" / "migrate") so the diagnostic reads correctly at
// either surface (the v0.68.1-consistency requirement).
func RefuseOnGeneratedColRefGeneratedCol(
	schema *ir.Schema,
	sourceEngine, targetEngine, contextID string,
) error {
	bad := ScanGeneratedColRefGeneratedCol(schema, sourceEngine, targetEngine)
	if len(bad) == 0 {
		return nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d generated column(s) reference another generated column in "+
		"the same table — MySQL permits this but PostgreSQL forbids it "+
		"(SQLSTATE 42P17) and rejects the CREATE TABLE", contextID, len(bad))
	if contextID == "migrate" {
		b.WriteString(" after partially creating the target")
	}
	b.WriteString(". sluice refuses before any DDL is applied (loud-failure tenet) " +
		"rather than letting PostgreSQL's error surface mid-pipeline:")
	for _, u := range bad {
		fmt.Fprintf(&b, "\n  - table %q column %q (GENERATED) references generated "+
			"column %q. Inline %q's own generation expression into %q via "+
			"`--expr-override %s.%s=<inlined-expression>` (or `--exclude-table "+
			"%s` to skip the table). Source expression: %s",
			u.Table, u.Column, u.ReferencedColumn, u.ReferencedColumn, u.Column,
			u.Table, u.Column, u.Table, u.Expression)
	}
	return errors.New(b.String())
}
