// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// InferredOverride is a single validated rich-type promotion (ADR-0144): a
// resolved target IR type for one (table, column), produced by the migrate
// orchestrator's `--infer-types` inference AFTER exhaustively validating the
// source data. Unlike a [config.Mapping] it carries the already-RESOLVED
// [ir.Type] — the timestamptz-vs-timestamp choice is data-dependent and cannot
// be expressed as a target_type alias string — so it bypasses alias resolution
// and rides the SAME copy-on-write rewrite (recording [ir.Column.SourceColumnType])
// the operator's `--type-override` path uses, for byte-identical downstream
// behaviour.
type InferredOverride struct {
	Table  string
	Column string
	Type   ir.Type
}

// ApplyInferredOverrides rewrites column types in s according to overrides,
// mirroring [ApplyMappings] but taking resolved [ir.Type] values directly. The
// returned schema is a copy that shares pointers with s for unaffected tables;
// a table with at least one overridden column is duplicated so callers can
// still rely on s being unchanged.
//
// Like ApplyMappings it is strict: an override naming a table/column not in s,
// or two overrides for the same column, is an error (the inference never emits
// these — the strictness catches a future caller bug rather than masking it).
// An empty overrides slice returns s unchanged (the no-op fast path).
func ApplyInferredOverrides(s *ir.Schema, overrides []InferredOverride) (*ir.Schema, error) {
	if s == nil {
		return nil, errors.New("translate: schema is nil")
	}
	if len(overrides) == 0 {
		return s, nil
	}

	byTable := map[string]map[string]ir.Type{}
	for i, o := range overrides {
		if o.Table == "" {
			return nil, fmt.Errorf("translate: inferred override[%d]: table is required", i)
		}
		if o.Column == "" {
			return nil, fmt.Errorf("translate: inferred override[%d] (%s): column is required", i, o.Table)
		}
		if o.Type == nil {
			return nil, fmt.Errorf("translate: inferred override[%d] (%s.%s): type is required", i, o.Table, o.Column)
		}
		cols, ok := byTable[o.Table]
		if !ok {
			cols = map[string]ir.Type{}
			byTable[o.Table] = cols
		}
		if _, dup := cols[o.Column]; dup {
			return nil, fmt.Errorf("translate: inferred override[%d]: duplicate override for %s.%s", i, o.Table, o.Column)
		}
		cols[o.Column] = o.Type
	}

	if err := validateInferredOverridesAgainstSchema(s, byTable); err != nil {
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
		out.Tables[i] = rewriteTableTypes(tbl, colMap)
	}
	return out, nil
}

// validateInferredOverridesAgainstSchema returns an error for any override
// naming a table/column the schema doesn't contain — the same strict contract
// as [validateMappingsAgainstSchema].
func validateInferredOverridesAgainstSchema(s *ir.Schema, byTable map[string]map[string]ir.Type) error {
	tables := map[string]*ir.Table{}
	for _, t := range s.Tables {
		tables[t.Name] = t
	}
	for tableName, cols := range byTable {
		tbl, ok := tables[tableName]
		if !ok {
			return fmt.Errorf("translate: inferred override references unknown table %q", tableName)
		}
		colSet := map[string]struct{}{}
		for _, c := range tbl.Columns {
			colSet[c.Name] = struct{}{}
		}
		for colName := range cols {
			if _, ok := colSet[colName]; !ok {
				return fmt.Errorf("translate: inferred override references unknown column %s.%s", tableName, colName)
			}
		}
	}
	return nil
}

// rewriteTableTypes copies tbl with the columns named in colMap re-typed,
// recording the pre-override type in [ir.Column.SourceColumnType] — identical
// field-setting to [rewriteTable] (the Bug-47 load-bearing case), so an
// inferred promotion is indistinguishable downstream from an operator override.
func rewriteTableTypes(tbl *ir.Table, colMap map[string]ir.Type) *ir.Table {
	out := *tbl
	out.Columns = make([]*ir.Column, len(tbl.Columns))
	for i, c := range tbl.Columns {
		newType, mapped := colMap[c.Name]
		if !mapped {
			out.Columns[i] = c
			continue
		}
		newCol := *c
		newCol.SourceColumnType = c.Type
		newCol.Type = newType
		out.Columns[i] = &newCol
	}
	return &out
}
