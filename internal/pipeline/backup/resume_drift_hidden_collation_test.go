// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRefuseAnchoredResumeOnSchemaDrift_HiddenCollationAcrossTheUpgrade is
// the seventh review's finding 3: an attempt interrupted on a binary that did
// not read a functional key's hidden-column collation (v0.160.x), resumed on
// one that does, must not refuse "the source schema changed" — the
// fingerprint folds the field, but kept row chunks do not depend on any
// index collation. The prior schema is the shape the older binary recorded
// (no field). A real change elsewhere in the schema still refuses.
func TestRefuseAnchoredResumeOnSchemaDrift_HiddenCollationAcrossTheUpgrade(t *testing.T) {
	schema := func(hidden string, width int8) *ir.Schema {
		return &ir.Schema{Tables: []*ir.Table{{
			Name: "kd",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: width}, Default: ir.DefaultNone{}},
				{Name: "k", Type: ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}, Default: ir.DefaultNone{}},
			},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
			Indexes: []*ir.Index{{Name: "fk", Unique: true, Columns: []ir.IndexColumn{
				{Expression: "cast(`k` as char(10) charset utf8mb4)", ExpressionDialect: "mysql", HiddenCollation: hidden},
			}}},
		}}}
	}
	if err := refuseAnchoredResumeOnSchemaDrift(schema("utf8mb4_bin", 32), schema("", 32)); err != nil {
		t.Errorf("a prior attempt without the field, resumed by a binary reading it, refused: %v", err)
	}
	if err := refuseAnchoredResumeOnSchemaDrift(schema("utf8mb4_bin", 64), schema("", 32)); err == nil {
		t.Error("a real column change beside the hidden collation was accepted")
	}
	// Eighth review: both attempts recorded the field and they differ — the
	// key was recreated under another collation between them. A real change.
	if err := refuseAnchoredResumeOnSchemaDrift(schema("utf8mb4_0900_ai_ci", 32), schema("utf8mb4_bin", 32)); err == nil {
		t.Error("a hidden collation both attempts recorded, and that changed, was accepted")
	}
	if err := refuseAnchoredResumeOnSchemaDrift(schema("utf8mb4_bin", 32), schema("utf8mb4_bin", 32)); err != nil {
		t.Errorf("an equal recorded hidden collation refused: %v", err)
	}
	if err := refuseAnchoredResumeOnSchemaDrift(schema("", 32), schema("utf8mb4_bin", 32)); err != nil {
		t.Errorf("a current read that lacks the field (vtgate) refused: %v", err)
	}
}
