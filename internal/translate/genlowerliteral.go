// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

// LOWER/UPPER-over-a-string-literal in a GENERATED column preflight
// (catalog Bug 20 residual). MySQL accepts `LOWER('ABC')` as a
// generated-column body (the generated value is just the constant).
// On PostgreSQL every migrated generated column is STORED, and a
// STORED generated column's expression must have a *determinable
// collation*. A bare string literal has the `unknown`/`text` type
// with no pinned collation, so PG rejects the CREATE TABLE with
// SQLSTATE 42P22 ("could not determine which collation to use for
// lower() function") mid-create-tables, leaving a partial target.
//
// The translator's `::text` rewrite (rewriteLowerUpperLiteralCollation
// in the postgres engine) rescues the CHECK / DEFAULT positions, where
// `lower('ABC'::text)` is accepted — but it does NOT rescue a STORED
// generated column, where PG still demands an explicit collation that
// sluice cannot synthesise faithfully (a `COLLATE` choice changes
// Unicode case-folding semantics vs MySQL). Per the loud-failure
// tenet, that residual is a clean up-front refusal naming the site and
// the `--expr-override` remedy (the value is constant — the operator
// just supplies the already-lowered literal), not a raw mid-pipeline
// 42P22 + partial target.
//
// Engine-neutral, cross-engine MySQL → Postgres only (mirrors
// ScanMySQLToPGGaps's scoping); scoped to GENERATED columns only (the
// CHECK/DEFAULT positions are handled by the translator rewrite, so
// flagging them here would be a false positive).

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/translate/exprident"
)

// LowerUpperLiteralGenerated is one detected GENERATED column whose
// body applies LOWER()/UPPER() to a bare string literal.
type LowerUpperLiteralGenerated struct {
	Table      string
	Column     string
	Function   string // "LOWER" or "UPPER"
	Expression string
}

// ScanLowerUpperLiteralInGenerated returns every GENERATED column
// whose generation expression contains a LOWER()/UPPER() call whose
// sole argument is exactly one string literal. Cross-engine
// MySQL → Postgres only; nil for any other pair or a nil schema
// (mirrors ScanMySQLToPGGaps). Sorted by (table, column) for a stable
// refusal message.
func ScanLowerUpperLiteralInGenerated(
	schema *ir.Schema,
	sourceEngine, targetEngine string,
) []LowerUpperLiteralGenerated {
	if schema == nil {
		return nil
	}
	if !strings.EqualFold(sourceEngine, "mysql") || !strings.EqualFold(targetEngine, "postgres") {
		return nil
	}
	var out []LowerUpperLiteralGenerated
	for _, tbl := range schema.Tables {
		if tbl == nil {
			continue
		}
		for _, col := range tbl.Columns {
			if col == nil || col.GeneratedExpr == "" {
				continue
			}
			if !strings.EqualFold(col.GeneratedExprDialect, "mysql") {
				continue
			}
			if fn := lowerUpperLiteralCall(col.GeneratedExpr); fn != "" {
				out = append(out, LowerUpperLiteralGenerated{
					Table:      tbl.Name,
					Column:     col.Name,
					Function:   fn,
					Expression: col.GeneratedExpr,
				})
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Table != out[j].Table {
			return out[i].Table < out[j].Table
		}
		return out[i].Column < out[j].Column
	})
	return out
}

// lowerUpperLiteralCall returns "LOWER"/"UPPER" if expr contains such a
// call whose sole argument is exactly one string literal, else "". The
// walk is string-literal-aware (a function name inside a quoted string
// is data, not a call) and paren-balanced via the shared exprident
// primitives — the same discipline as the other translate scanners.
func lowerUpperLiteralCall(expr string) string {
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
		word := strings.ToUpper(expr[start:j])
		k := j
		for k < len(expr) && (expr[k] == ' ' || expr[k] == '\t' || expr[k] == '\n' || expr[k] == '\r') {
			k++
		}
		if (word == "LOWER" || word == "UPPER") && k < len(expr) && expr[k] == '(' {
			if end, ok := exprident.ScanParenGroup(expr, k); ok {
				args := exprident.SplitTopLevelArgs(expr[k+1 : end])
				if len(args) == 1 && argIsOneStringLiteral(strings.TrimSpace(args[0])) {
					return word
				}
			}
		}
		i = j
	}
	return ""
}

// argIsOneStringLiteral reports whether s is exactly one single-quoted
// SQL string literal with nothing before or after — so a column ref, a
// compound expression, or an already-cast literal returns false.
func argIsOneStringLiteral(s string) bool {
	if len(s) < 2 || s[0] != '\'' {
		return false
	}
	return exprident.ScanStringLiteral(s, 0) == len(s)
}

// RefuseOnLowerUpperLiteralInGenerated returns a non-nil,
// operator-actionable error when scanning schema for the given
// MySQL→PG pair surfaces one or more GENERATED columns applying
// LOWER()/UPPER() to a bare string literal; nil otherwise. contextID
// is the caller's phase label ("schema preview" / "migrate") so the
// diagnostic reads correctly at either surface.
func RefuseOnLowerUpperLiteralInGenerated(
	schema *ir.Schema,
	sourceEngine, targetEngine, contextID string,
) error {
	bad := ScanLowerUpperLiteralInGenerated(schema, sourceEngine, targetEngine)
	if len(bad) == 0 {
		return nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d generated column(s) apply LOWER()/UPPER() to a bare "+
		"string literal — MySQL accepts this but on PostgreSQL every generated "+
		"column is STORED and a STORED generated column's expression must have a "+
		"determinable collation, which a string-literal argument lacks "+
		"(SQLSTATE 42P22); PostgreSQL rejects the CREATE TABLE", contextID, len(bad))
	if contextID == "migrate" {
		b.WriteString(" after partially creating the target")
	}
	b.WriteString(". sluice refuses before any DDL is applied (loud-failure tenet); " +
		"the ::text translator rewrite rescues CHECK/DEFAULT positions but cannot " +
		"a STORED generated column:")
	for _, u := range bad {
		fmt.Fprintf(&b, "\n  - table %q column %q (GENERATED): %s() over a string "+
			"literal yields a constant — supply a PostgreSQL-valid form via "+
			"`--expr-override %s.%s=<already-lowered-literal>` (or `--exclude-table "+
			"%s`). Source expression: %s",
			u.Table, u.Column, u.Function, u.Table, u.Column, u.Table, u.Expression)
	}
	return errors.New(b.String())
}
