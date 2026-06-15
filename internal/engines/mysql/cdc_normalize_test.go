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

// TestNormalizeForCDCComparison_Vanilla_PreservesPrimaryKey pins that the
// binlog (vanilla) flavor keeps PrimaryKey + Indexes (its CDC projection
// re-reads information_schema and carries them) and strips only
// CheckConstraints (ADR-0065 — the boundary projection omits CHECKs).
func TestNormalizeForCDCComparison_Vanilla_PreservesPrimaryKey(t *testing.T) {
	out := Engine{Flavor: FlavorVanilla}.NormalizeForCDCComparison(normTestTable())
	if out.PrimaryKey == nil {
		t.Error("vanilla: PrimaryKey was stripped; binlog CDC re-reads information_schema and DOES carry it")
	}
	if len(out.Indexes) != 1 {
		t.Errorf("vanilla: Indexes len = %d; want 1 (binlog carries secondary indexes)", len(out.Indexes))
	}
	if out.CheckConstraints != nil {
		t.Error("vanilla: CheckConstraints not stripped (ADR-0065: boundary projection omits CHECKs)")
	}
	// Binlog re-reads information_schema, so charset/collation ARE carried
	// — they must be preserved (a real charset ALTER still classifies).
	if v, ok := out.Columns[1].Type.(ir.Varchar); !ok || v.Charset != "utf8mb4" {
		t.Errorf("vanilla: email charset = %v; want utf8mb4 preserved", out.Columns[1].Type)
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
