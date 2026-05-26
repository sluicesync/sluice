// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "github.com/orware/sluice/internal/ir"

// NormalizeForCDCComparison strips Table fields the MySQL CDC
// reader's tableSchema-derived projection cannot carry from the
// binlog wire format. Implements [ir.CDCSchemaSnapshotNormalizer].
//
// Historical note: MySQL was originally exempt from this surface
// because its TableMapEvent decoder re-reads information_schema
// for column-level metadata on schema-change boundaries — so
// columns matched the SchemaReader's view. Constraints, however,
// were NOT re-read at the boundary; the cold-start SchemaReader
// populates `Table.CheckConstraints` from
// information_schema.CHECK_CONSTRAINTS, but the CDC IR projection
// at [maybeSnapshotSchemaB1] builds a fresh `*ir.Table` carrying
// only Schema / Name / Columns / PrimaryKey — no CheckConstraints.
//
// Without normalization, [pipeline.diffChecks] (ADR-0065 task #22
// classifier extension) fires a false `ShapeKindDropCheck` on every
// CDC boundary for any table that carries a CHECK constraint at
// cold-start. The asymmetry is identical in shape to the PG
// Bug 86 column-attribute fix; the normalizer makes the loss
// explicit at the comparison surface.
//
// Trade-off (ADR-0065): live-coordination cannot detect CHECK
// constraint shapes via CDC. Operators issuing constraint-only
// DDL while live-coordination is engaged see the cold-start
// SchemaReader land the change at the next snapshot boundary;
// the CDC stream's row apply path observes whatever the new
// constraint admits / refuses at INSERT/UPDATE time (loud-failure
// by default — MySQL 8.0+ enforces CHECK by default).
//
// Returns a new *ir.Table (deep-enough copy that mutating the
// returned struct cannot mutate the input). Idempotent on repeated
// calls.
func (Engine) NormalizeForCDCComparison(t *ir.Table) *ir.Table {
	if t == nil {
		return nil
	}
	out := *t
	// ADR-0065: strip the constraint slices the CDC projection
	// cannot carry — see file-level docstring.
	out.CheckConstraints = nil
	return &out
}
