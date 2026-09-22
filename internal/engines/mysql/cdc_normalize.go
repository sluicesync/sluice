// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "sluicesync.dev/sluice/internal/ir"

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
// Secondary indexes (GC-1, audit backlog 2026-09-22): NEITHER flavor's
// CDC projection carries them. The binlog path re-reads
// information_schema on a DDL boundary, but its `tableSchema` is
// columns + PK column names by design (indexes are not consulted on a
// row event, so the reader does not pay to load them on every cache
// miss), and [projectTableIR] publishes exactly {Schema, Name, Columns,
// PrimaryKey}. An earlier revision of this file asserted the opposite —
// "a real MySQL-binlog index delta still classifies" — and preserved
// Indexes on the seed for the binlog flavors, so the seed's every
// secondary index diffed against the first post-cold-start boundary as
// a phantom ShapeKindDropIndex. Alone that is absorbed by the
// routeForwardBoundary seed-guard; alongside the real ADD COLUMN that
// caused the boundary it is a two-class combo, and ClassifyShape's
// multi-shape refusal fires BEFORE the seed-guard — halting every
// binlog-source sync at its first source DDL on any table with a named
// secondary index. Stripping Indexes for every flavor makes the seed
// match what the projection can carry, exactly as the PG normalizer
// strips what pgoutput omits. Consequence, unchanged from before (the
// projection never carried them): index-only DDL produces no boundary
// on a binlog source and is not forwarded — roadmap item 24 / ADR-0103,
// whose Phase 1(a) (load information_schema.statistics at the boundary
// and track a separate forwardIndexSig) is the path to forwarding it,
// and the point at which this strip must be lifted for the binlog
// flavors. Pinned as an argument, not two facts, by
// TestNormalizeForCDCComparison_Binlog_SeedAgreesWithBoundaryProjection.
//
// F7c (ADR-0091): the VStream (PlanetScale / Vitess) flavor's CDC
// projection is LOWER fidelity still. The binlog projection carries the
// PRIMARY key (Bug 89); the VStream path projects from the FieldEvent's
// per-column metadata ONLY ([projectVStreamFields] builds an ir.Table
// with Columns but NO PrimaryKey — the FIELD wire never carries it). So
// a VStream-source cold-start seed (a full SchemaReader read, which DOES
// carry the PRIMARY key) diffed against the first CDC SchemaSnapshot
// surfaces a PHANTOM index-drop of the PRIMARY key — classified as a
// multi-shape combo alongside a real ADD COLUMN and refused, so the ADD
// never forwards (the soak's 42703/1054 second facet). Stripping
// PrimaryKey from the seed for the VStream flavor makes the seed match
// the projection's fidelity. CREATE/DROP INDEX therefore cannot be
// forwarded on a VStream source either (the wire never signals them) —
// a documented limitation, symmetric with PG-source indexes.
//
// Returns a new *ir.Table (deep-enough copy that mutating the
// returned struct cannot mutate the input). Idempotent on repeated
// calls.
func (e Engine) NormalizeForCDCComparison(t *ir.Table) *ir.Table {
	if t == nil {
		return nil
	}
	out := *t
	// ADR-0065: strip the constraint slices the CDC projection
	// cannot carry — see file-level docstring. Applies to both flavors
	// (neither binlog nor VStream re-reads CHECK constraints on a
	// boundary).
	out.CheckConstraints = nil
	// GC-1: no MySQL CDC projection carries secondary indexes — the
	// binlog projectTableIR is columns + PK, the VStream FIELD
	// projection is columns only — so strip them from the seed for every
	// flavor. See the file-level docstring for the phantom-combo this
	// closes and for ADR-0103 as the path to lifting it.
	out.Indexes = nil
	// F7c: the VStream FIELD projection carries no PRIMARY key; strip it
	// from the seed so a VStream-source seed→firstCDC diff doesn't surface
	// a phantom index-drop of the PK. The binlog flavor DOES carry
	// PrimaryKey (projectTableIR projects it from tableSchema.PrimaryKey),
	// so it is preserved there. VStream also cannot reliably carry
	// per-column CHARACTER SET / COLLATE (vtgate's FieldEvent.column_type
	// omits the charset suffix for the common case), so zero those type
	// sub-fields too — otherwise the SchemaReader's populated
	// Charset/Collation diffs against the empty CDC projection as a
	// phantom AlterColumnType (the soak's third facet). The CDC side
	// already lands empty, so normalizing the seed to match closes the
	// asymmetry; a charset-only ALTER consequently cannot be forwarded on
	// a VStream source — a documented limitation in line with the wire's
	// fidelity (ADR-0091 §1d). The binlog loader (loadTableSchema) reads
	// character_set_name / collation_name, so the binlog flavors keep
	// them and a real charset ALTER still classifies there.
	if e.Flavor.usesVStream() {
		out.PrimaryKey = nil
		out.Columns = normalizeColumnsForVStreamCDC(t.Columns)
	}
	return &out
}

// normalizeColumnsForVStreamCDC returns a copy of cols with each
// column's CDC-unprojectable type sub-fields zeroed for the VStream
// flavor. Today that is the CHARACTER SET / COLLATE on the string
// families (Char / Varchar / Text), which the VStream FieldEvent's
// column_type does not reliably carry. Mirrors the PG normalizer's
// normalizeTypeForCDCComparison. Deep-enough copy: each *ir.Column is
// reallocated so mutating the result cannot mutate the input.
func normalizeColumnsForVStreamCDC(cols []*ir.Column) []*ir.Column {
	out := make([]*ir.Column, 0, len(cols))
	for _, c := range cols {
		if c == nil {
			continue
		}
		nc := *c
		nc.Type = stripVStreamUnprojectableTypeFields(c.Type)
		out = append(out, &nc)
	}
	return out
}

// stripVStreamUnprojectableTypeFields zeroes the charset/collation
// fields the VStream FIELD projection cannot reliably carry, so a
// seed↔VStream-CDC type comparison doesn't fire a phantom
// AlterColumnType. Returns the type unchanged for families that carry no
// such field. Pinned across the whole string family (Char / Varchar /
// Text), not one representative — the projection path is identical for
// all three.
func stripVStreamUnprojectableTypeFields(t ir.Type) ir.Type {
	switch v := t.(type) {
	case ir.Char:
		v.Charset = ""
		v.Collation = ""
		return v
	case ir.Varchar:
		v.Charset = ""
		v.Collation = ""
		return v
	case ir.Text:
		v.Charset = ""
		v.Collation = ""
		return v
	default:
		return t
	}
}
