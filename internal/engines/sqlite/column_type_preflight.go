// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// Compile-time proof the ENGINE (not a test stub) answers the pre-copy
// column-type question, so the orchestrator's [ir.ColumnTypeEmitPreflighter]
// gate engages. The registry holds an Engine VALUE, so the assertion is on the
// value type — same shape as the index, view and table siblings next door.
var _ ir.ColumnTypeEmitPreflighter = Engine{}

// PreflightColumnTypes reports whether every column type in s can be rendered
// on a SQLite target, WITHOUT a connection and before any data moves.
//
// SQLite is the engine this question matters most for, and the reason is that
// nothing else asks it on its behalf. It has the SMALLEST type surface of any
// target — geometry, inet/cidr/macaddr, bit, interval, array, domain and
// verbatim/uncatalogued extension types all have no faithful SQLite storage and
// are refused rather than coerced to a silently-wrong TEXT column — and it is
// covered by NO direction of [migcore.CheckCrossEngineSupportable], whose only
// arm is PG-source → MySQL-target. So before this, every one of those refusals
// arrived from `emitColumnType` at CREATE TABLE, with the tables ahead of the
// offending one already created.
//
// It delegates to [emitColumnType] — the same function [emitColumnDef] calls on
// the create path, and its only caller — so the early answer and the late one
// are one function and cannot drift.
//
// # What it walks, and the one thing it skips
//
// Every column of every table. A nil column, or a column with a nil Type, is
// SKIPPED: a nil type is a corrupt IR rather than an unrepresentable one, this
// gate has no better answer for it than the emitter does, and calling
// `emitColumnType(nil)` here would introduce a NEW panic (the refusal arm
// formats `t.String()`) on a path that today reaches the emitter.
func (Engine) PreflightColumnTypes(s *ir.Schema) error {
	if s == nil {
		return errors.New("sqlite: PreflightColumnTypes: schema is nil")
	}
	for _, table := range s.Tables {
		if table == nil {
			continue
		}
		for _, col := range table.Columns {
			if col == nil || col.Type == nil {
				continue
			}
			if _, err := emitColumnType(col.Type); err != nil {
				return fmt.Errorf("sqlite: table %q column %q: %w", table.Name, col.Name, err)
			}
		}
	}
	return nil
}
