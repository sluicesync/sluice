// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
)

// binlogFlavors returns every flavor that rides the binlog CDC reader
// ([openBinlogCDCReader]) and therefore publishes DDL boundaries through
// [projectTableIR]. Derived from the same predicate the engine dispatches
// on ([Flavor.usesVStream]) so a new binlog flavor cannot escape these
// pins by omission — the anti-vacuity floor is the length check.
func binlogFlavors(t *testing.T) []Flavor {
	t.Helper()
	var out []Flavor
	for f := range flavorCapabilities {
		if !f.usesVStream() {
			out = append(out, f)
		}
	}
	if len(out) < 2 {
		t.Fatalf("binlog flavor roster = %v; want at least vanilla + mariadb (the roster walk is broken, not the code)", out)
	}
	return out
}

// projectBinlogBoundary is what the binlog reader publishes at a DDL
// boundary for the seed's table: the SchemaReader's columns plus the PK
// column names, through the production projection. Nothing else — no
// secondary indexes, no CHECK constraints. extra columns model a source
// ADD COLUMN that landed between the cold-start read and the boundary.
func projectBinlogBoundary(seed *ir.Table, extra ...*ir.Column) *ir.Table {
	cols := append([]*ir.Column(nil), seed.Columns...)
	cols = append(cols, extra...)
	var pk []string
	if seed.PrimaryKey != nil {
		for _, c := range seed.PrimaryKey.Columns {
			pk = append(pk, c.Column)
		}
	}
	return projectTableIR(&tableSchema{Schema: seed.Schema, Name: seed.Name, Columns: cols, PrimaryKey: pk})
}

// TestNormalizeForCDCComparison_Binlog_SeedAgreesWithBoundaryProjection
// is the invariant the normalizer exists to hold, pinned as an
// ARGUMENT rather than as two separate facts: a cold-start seed (full
// SchemaReader fidelity) normalized for the binlog flavor must classify
// as ShapeKindNone against the binlog boundary projection of the SAME
// table, both sides through the lens exactly as the pipeline applies it
// (pipeline.normalizeSnapshotForComparison). Before GC-1 the seed kept
// Indexes while projectTableIR never carried them, so this diffed as a
// phantom ShapeKindDropIndex of every secondary index on every binlog
// source — "the normalizer preserves Indexes" and "the projection omits
// Indexes" were each pinned, and nothing bound the two.
func TestNormalizeForCDCComparison_Binlog_SeedAgreesWithBoundaryProjection(t *testing.T) {
	for _, f := range binlogFlavors(t) {
		e := Engine{Flavor: f}
		pre := e.NormalizeForCDCComparison(normTestTable())
		post := e.NormalizeForCDCComparison(projectBinlogBoundary(normTestTable()))
		shape, err := pipeline.ClassifyShape(pre, post)
		if err != nil {
			t.Fatalf("%s: seed vs boundary projection of the SAME table refused: %v", f, err)
		}
		if shape.Kind != pipeline.ShapeKindNone {
			t.Errorf("%s: seed vs boundary projection of the SAME table = %s (dropped-idx=%d); want None — the seed carries something the binlog projection cannot",
				f, shape.Kind, len(shape.DroppedIndexes))
		}
	}
}

// TestNormalizeForCDCComparison_Binlog_AddColumnOnIndexedTableIsAddColumn
// is the GC-1 probe (audit backlog 2026-09-22): seed {id PK, …, idx_email}
// vs the first post-cold-start boundary {id PK, …, +b}. Before the fix
// the phantom index-drop joined the real ADD COLUMN as a two-class
// combo, and ClassifyShape's multi-shape refusal fired BEFORE the
// routeForwardBoundary seed-guard that exists to absorb seed-vs-CDC
// phantoms — halting every binlog-source sync at its first source DDL
// on any table carrying a named secondary index. The end-to-end pin is
// TestStreamer_AddColumnForward_MySQL_IndexedTable_ForwardsALTER.
func TestNormalizeForCDCComparison_Binlog_AddColumnOnIndexedTableIsAddColumn(t *testing.T) {
	for _, f := range binlogFlavors(t) {
		e := Engine{Flavor: f}
		seed := normTestTable()
		if len(seed.Indexes) == 0 {
			t.Fatal("fixture carries no secondary index; the probe is vacuous")
		}
		pre := e.NormalizeForCDCComparison(seed)
		post := e.NormalizeForCDCComparison(projectBinlogBoundary(
			normTestTable(),
			&ir.Column{Name: "b", Type: ir.Integer{Width: 32}, Nullable: true},
		))
		shape, err := pipeline.ClassifyShape(pre, post)
		if err != nil {
			t.Fatalf("%s: ADD COLUMN on an indexed table refused (GC-1 phantom index-drop combo): %v", f, err)
		}
		if shape.Kind != pipeline.ShapeKindAddColumn {
			t.Fatalf("%s: shape = %s; want AddColumn", f, shape.Kind)
		}
		if len(shape.AddedColumns) != 1 || shape.AddedColumns[0].Name != "b" {
			t.Errorf("%s: AddedColumns = %+v; want exactly [b]", f, shape.AddedColumns)
		}
	}
}
