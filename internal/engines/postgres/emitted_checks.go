// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import "sluicesync.dev/sluice/internal/ir"

// PredictEmittedChecks satisfies [ir.EmittedCheckPredictor]: it names
// the CHECK constraints THIS writer synthesizes for table beyond the
// ones the source declares in [ir.Table.CheckConstraints].
//
// Two, and they are exactly the two [emitTableDef] appends:
//
//   - the SET-membership CHECK, for every `ir.Set` column — PostgreSQL
//     has no SET type, so the column lands as `TEXT[]` and the member
//     list survives only in this constraint;
//   - the generated-enum CHECK, for every GENERATED `ir.Enum` column
//     (Bug 25) — the column lands as `TEXT` because a generated body
//     cannot call the non-IMMUTABLE `enum_in()`, and the value list
//     survives only in this constraint.
//
// Both bodies come from the SAME functions the emitter uses
// ([setCheckConstraint] / [generatedEnumCheckConstraint]), so the
// prediction cannot drift from the DDL. Neither is version- or
// server-gated: PostgreSQL has enforced table CHECK constraints since
// long before any version sluice supports, so — unlike MySQL's inline
// CHECKs — there is no live-server condition to consult and the
// prediction holds for every target this writer can be opened against.
//
// Order is the emitter's: all SET checks in column order, then all
// generated-enum checks in column order. Nothing depends on it (the
// diff matches by expression, not by position), but a stable order
// keeps a rendered suggestion list reproducible.
func (w *SchemaWriter) PredictEmittedChecks(table *ir.Table) []*ir.CheckConstraint {
	if table == nil {
		return nil
	}
	var out []*ir.CheckConstraint
	for _, col := range table.Columns {
		set, ok := col.Type.(ir.Set)
		if !ok {
			continue
		}
		name, body := setCheckConstraint(table.Name, col.Name, set.Values)
		out = append(out, &ir.CheckConstraint{
			Name:          name,
			Expr:          body,
			ExprDialect:   "postgres",
			SluiceEmitted: true,
		})
	}
	for _, col := range table.Columns {
		enum, ok := col.Type.(ir.Enum)
		if !ok || !col.IsGenerated() {
			continue
		}
		name, body := generatedEnumCheckConstraint(table.Name, col.Name, enum.Values)
		out = append(out, &ir.CheckConstraint{
			Name:          name,
			Expr:          body,
			ExprDialect:   "postgres",
			SluiceEmitted: true,
		})
	}
	return out
}
