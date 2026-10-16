// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// TestIndicesEqual_HiddenCollation: a functional key part's hidden-column
// collation (FUNCTIONAL-CAST-HIDDEN-COLLATION) is compared only when both
// shapes carry it. A parent manifest an older binary wrote has none, and
// reading every functional key as redefined on the first incremental after
// an upgrade would stamp a schema delta the source never ran; both carrying
// it and differing is a real rebuild.
func TestIndicesEqual_HiddenCollation(t *testing.T) {
	idx := func(hidden string) *ir.Index {
		return &ir.Index{Name: "fk", Unique: true, Columns: []ir.IndexColumn{
			{Expression: "lower(k)", ExpressionDialect: "mysql", HiddenCollation: hidden},
		}}
	}
	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{"", "utf8mb4_bin", true},
		{"utf8mb4_bin", "", true},
		{"utf8mb4_bin", "utf8mb4_bin", true},
		{"utf8mb4_bin", "utf8mb4_0900_ai_ci", false},
	} {
		if got := indicesEqual(idx(tc.a), idx(tc.b)); got != tc.want {
			t.Errorf("%q vs %q: equal %v; want %v", tc.a, tc.b, got, tc.want)
		}
	}
	// The other attributes still decide.
	other := idx("utf8mb4_bin")
	other.Columns[0].Expression = "upper(k)"
	if indicesEqual(idx(""), other) {
		t.Error("a changed expression read as equal")
	}
}

// TestDeltaChangesKeyCollation_HiddenCollation: a UNIQUE functional key
// redefined under a different hidden-column collation is a change to the
// key's comparison (sixth review F4), so the delta holds as a column
// collation change does; one side not recording it, an equal one, and a
// non-unique index do not.
func TestDeltaChangesKeyCollation_HiddenCollation(t *testing.T) {
	table := func(unique bool, hidden string) *ir.Table {
		return &ir.Table{
			Name:       "kd",
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}, {Name: "k", Type: ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}}},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
			Indexes: []*ir.Index{{Name: "fk", Unique: unique, Columns: []ir.IndexColumn{
				{Expression: "lower(k)", ExpressionDialect: "mysql", HiddenCollation: hidden},
			}}},
		}
	}
	for _, tc := range []struct {
		name          string
		before, after *ir.Table
		want          bool
	}{
		{"unique key, hidden collation changed", table(true, "utf8mb4_0900_ai_ci"), table(true, "utf8mb4_bin"), true},
		{"unique key, equal", table(true, "utf8mb4_bin"), table(true, "utf8mb4_bin"), false},
		{"unique key, before not recorded (an older parent)", table(true, ""), table(true, "utf8mb4_bin"), false},
		{"non-unique index, changed", table(false, "utf8mb4_0900_ai_ci"), table(false, "utf8mb4_bin"), false},
	} {
		d := []*irbackup.SchemaDeltaEntry{{Kind: irbackup.SchemaDeltaAlterTable, Table: "kd", Before: tc.before, After: tc.after}}
		if got := DeltaChangesKeyCollation(d); got != tc.want {
			t.Errorf("%s: DeltaChangesKeyCollation = %v; want %v", tc.name, got, tc.want)
		}
	}
}
