// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
)

// ApplyExpressionOverrides rewrites generated-column bodies in s
// according to the per-column rules in overrides. The escape hatch
// for cross-dialect expression translation cases ADR-0016's hand-
// coded rewrite table doesn't recognise: the operator supplies
// target-dialect text, sluice emits it verbatim, and the translator
// stays out of the way for that specific column.
//
// Mechanism: when an override matches a column, sluice replaces
// `Column.GeneratedExpr` with the override's text and clears
// `Column.GeneratedExprDialect`. The cleared dialect tag is the
// signal to the writer-side translator that no rewrite is needed —
// `internal/engines/postgres/ddl_emit.go::translateGeneratedExpr`
// short-circuits and emits verbatim when the tag is empty or matches
// the writer's own dialect.
//
// Returns:
//   - The same shape as ApplyMappings: a copy of s that shares
//     pointers with unaffected tables, with affected tables
//     copy-on-written so the caller can still rely on s being
//     unchanged.
//   - Errors when an override references an unknown table or column,
//     when two overrides target the same (table, column), or when
//     the named column doesn't actually have a generated expression
//     to override (almost always an operator typo — silent
//     passthrough would mask "why didn't my override fire?").
//
// When overrides is empty, ApplyExpressionOverrides returns s
// unchanged with a nil error.
func ApplyExpressionOverrides(s *ir.Schema, overrides []config.ExpressionMapping) (*ir.Schema, error) {
	if s == nil {
		return nil, errors.New("translate: schema is nil")
	}
	if len(overrides) == 0 {
		return s, nil
	}

	byTable, err := groupExpressionOverrides(overrides)
	if err != nil {
		return nil, err
	}

	if err := validateExpressionOverridesAgainstSchema(s, byTable); err != nil {
		return nil, err
	}

	out := &ir.Schema{
		Tables: make([]*ir.Table, len(s.Tables)),
		// Schema-level objects pass through untouched: these passes
		// rewrite table/column shapes only, and dropping Views /
		// Sequences here would silently strip them from every run that
		// engages the pass (the item-51 lesson).
		Views:     s.Views,
		Sequences: s.Sequences,
	}
	for i, tbl := range s.Tables {
		colMap, hit := byTable[tbl.Name]
		if !hit {
			out.Tables[i] = tbl
			continue
		}
		out.Tables[i] = rewriteGeneratedExprs(tbl, colMap)
	}
	return out, nil
}

// groupExpressionOverrides returns a `table -> column -> expression`
// map. Two overrides on the same column is an operator bug — surface
// it loudly rather than picking a silent winner.
func groupExpressionOverrides(overrides []config.ExpressionMapping) (map[string]map[string]string, error) {
	out := map[string]map[string]string{}
	for i, m := range overrides {
		if m.Table == "" {
			return nil, fmt.Errorf("translate: expression_mappings[%d]: table is required", i)
		}
		if m.Column == "" {
			return nil, fmt.Errorf("translate: expression_mappings[%d] (%s): column is required", i, m.Table)
		}
		if m.Expression == "" {
			return nil, fmt.Errorf("translate: expression_mappings[%d] (%s.%s): expression is required", i, m.Table, m.Column)
		}
		cols, ok := out[m.Table]
		if !ok {
			cols = map[string]string{}
			out[m.Table] = cols
		}
		if _, dup := cols[m.Column]; dup {
			return nil, fmt.Errorf("translate: expression_mappings[%d]: duplicate override for %s.%s", i, m.Table, m.Column)
		}
		cols[m.Column] = m.Expression
	}
	return out, nil
}

// validateExpressionOverridesAgainstSchema returns an error for any
// override that names a table the schema doesn't contain, a column
// the table doesn't have, or a column that isn't a generated column.
// The third check exists so a typo (e.g. operator overrides
// `users.email` instead of `users.email_lower`) doesn't silently
// succeed and leave the operator wondering why the migrate still
// fails.
func validateExpressionOverridesAgainstSchema(s *ir.Schema, byTable map[string]map[string]string) error {
	tables := map[string]*ir.Table{}
	for _, t := range s.Tables {
		tables[t.Name] = t
	}
	for tableName, cols := range byTable {
		tbl, ok := tables[tableName]
		if !ok {
			return fmt.Errorf("translate: expression_mappings reference unknown table %q", tableName)
		}
		colMap := map[string]*ir.Column{}
		for _, c := range tbl.Columns {
			colMap[c.Name] = c
		}
		for colName := range cols {
			col, ok := colMap[colName]
			if !ok {
				return fmt.Errorf("translate: expression_mappings reference unknown column %s.%s", tableName, colName)
			}
			if !col.IsGenerated() {
				return fmt.Errorf("translate: expression_mappings target %s.%s but the column is not a generated column", tableName, colName)
			}
		}
	}
	return nil
}

// rewriteGeneratedExprs produces a copy of tbl with each column
// named in colMap given a verbatim-emit override: GeneratedExpr
// replaced with the operator's text, GeneratedExprDialect cleared
// so the writer's translator skips the column.
func rewriteGeneratedExprs(tbl *ir.Table, colMap map[string]string) *ir.Table {
	out := *tbl
	out.Columns = make([]*ir.Column, len(tbl.Columns))
	for i, c := range tbl.Columns {
		newExpr, hit := colMap[c.Name]
		if !hit {
			out.Columns[i] = c
			continue
		}
		newCol := *c
		newCol.GeneratedExpr = newExpr
		newCol.GeneratedExprDialect = ""
		slog.Info(
			"translate: applying expression override",
			slog.String("table", tbl.Name),
			slog.String("column", c.Name),
		)
		out.Columns[i] = &newCol
	}
	return &out
}
