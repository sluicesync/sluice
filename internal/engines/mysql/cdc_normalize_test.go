// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// normTestTable builds a SchemaReader-fidelity table: full columns +
// PRIMARY key + a secondary index + a CHECK constraint — the shape the
// cold-start seed carries before NormalizeForCDCComparison runs.
func normTestTable() *ir.Table {
	return &ir.Table{
		Schema: "app",
		Name:   "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			// SchemaReader populates Charset/Collation; the VStream FIELD
			// projection does not — the F7c phantom-AlterColumnType source.
			{Name: "email", Type: ir.Varchar{Length: 255, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}},
			{Name: "label", Type: ir.Char{Length: 8, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}},
			{Name: "bio", Type: ir.Text{Size: ir.TextMedium, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}},
		},
		PrimaryKey: &ir.Index{
			Name:    "PRIMARY",
			Unique:  true,
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
		Indexes: []*ir.Index{
			{Name: "idx_email", Columns: []ir.IndexColumn{{Column: "email"}}},
		},
		CheckConstraints: []*ir.CheckConstraint{
			{Name: "ck_id_pos", Expr: "id > 0"},
		},
	}
}

// TestNormalizeForCDCComparison_Binlog_PreservesPrimaryKeyStripsIndexes
// pins what the binlog flavors (vanilla + MariaDB — the roster is
// derived from usesVStream in binlogFlavors) keep and strip, field by
// field, to match the fidelity of projectTableIR's boundary projection:
// PrimaryKey kept (the projection carries it — Bug 89), charset /
// collation kept (loadTableSchema reads them), Indexes stripped (GC-1:
// the projection is columns + PK, it never carried secondary indexes),
// CheckConstraints stripped (ADR-0065). This test pins the normalizer's
// output alone; the pin that BINDS it to the projection is
// TestNormalizeForCDCComparison_Binlog_SeedAgreesWithBoundaryProjection —
// an earlier revision of this test asserted "binlog carries secondary
// indexes" and proved nothing about the projection.
func TestNormalizeForCDCComparison_Binlog_PreservesPrimaryKeyStripsIndexes(t *testing.T) {
	for _, f := range binlogFlavors(t) {
		out := Engine{Flavor: f}.NormalizeForCDCComparison(normTestTable())
		if out.PrimaryKey == nil {
			t.Errorf("%s: PrimaryKey was stripped; projectTableIR DOES carry it", f)
		}
		if out.Indexes != nil {
			t.Errorf("%s: Indexes NOT stripped (len %d); projectTableIR carries no secondary indexes, so the seed must not either (GC-1 phantom index-drop)", f, len(out.Indexes))
		}
		if out.CheckConstraints != nil {
			t.Errorf("%s: CheckConstraints not stripped (ADR-0065: boundary projection omits CHECKs)", f)
		}
		// loadTableSchema reads character_set_name / collation_name, so
		// charset/collation ARE carried by the binlog projection — they
		// must be preserved (a real charset ALTER still classifies).
		if v, ok := out.Columns[1].Type.(ir.Varchar); !ok || v.Charset != "utf8mb4" {
			t.Errorf("%s: email charset = %v; want utf8mb4 preserved", f, out.Columns[1].Type)
		}
	}
}

// TestNormalizeForCDCComparison_VStream_StripsPKAndIndexes is the F7c pin:
// the VStream (PlanetScale / Vitess) flavor's FIELD projection
// (projectVStreamFields) carries neither the PRIMARY key nor secondary
// indexes, so the seed must be normalized to match — otherwise a
// VStream-source seed→firstCDC diff surfaces a PHANTOM index-drop of the
// PRIMARY key, classified as a multi-shape combo alongside a real ADD
// COLUMN and refused (the soak's 42703/1054 second facet).
func TestNormalizeForCDCComparison_VStream_StripsPKAndIndexes(t *testing.T) {
	for _, f := range []Flavor{FlavorPlanetScale, FlavorVitess} {
		out := Engine{Flavor: f}.NormalizeForCDCComparison(normTestTable())
		if out.PrimaryKey != nil {
			t.Errorf("%s: PrimaryKey NOT stripped; the VStream FIELD projection drops it, so the seed must too (F7c phantom index-drop)", f)
		}
		if out.Indexes != nil {
			t.Errorf("%s: Indexes NOT stripped; the VStream FIELD projection carries no secondary indexes", f)
		}
		if out.CheckConstraints != nil {
			t.Errorf("%s: CheckConstraints not stripped", f)
		}
		// The column SET must be preserved verbatim — the names ARE carried
		// by the FIELD projection, so dropping a column would hide a real
		// ADD/DROP. Only the CDC-unprojectable charset/collation sub-fields
		// are zeroed.
		if len(out.Columns) != 4 {
			t.Fatalf("%s: Columns len = %d; want 4 (columns must be preserved)", f, len(out.Columns))
		}
		// Charset/Collation zeroed on every string-family column (pin the
		// class: Varchar + Char + Text, not one representative).
		for _, c := range out.Columns {
			switch v := c.Type.(type) {
			case ir.Varchar:
				if v.Charset != "" || v.Collation != "" {
					t.Errorf("%s: %s Varchar charset/collation not zeroed: %+v", f, c.Name, v)
				}
			case ir.Char:
				if v.Charset != "" || v.Collation != "" {
					t.Errorf("%s: %s Char charset/collation not zeroed: %+v", f, c.Name, v)
				}
			case ir.Text:
				if v.Charset != "" || v.Collation != "" {
					t.Errorf("%s: %s Text charset/collation not zeroed: %+v", f, c.Name, v)
				}
			}
		}
	}
}

// TestNormalizeForCDCComparison_DoesNotMutateInput pins the deep-enough
// copy contract: normalizing must not zero the caller's PrimaryKey.
func TestNormalizeForCDCComparison_DoesNotMutateInput(t *testing.T) {
	in := normTestTable()
	_ = Engine{Flavor: FlavorPlanetScale}.NormalizeForCDCComparison(in)
	if in.PrimaryKey == nil {
		t.Error("input table's PrimaryKey was mutated by NormalizeForCDCComparison")
	}
}
