// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestClassifyShape_EmptyAndNilListsAreNoChange pins the
// TYPE-WIRE-EMPTY-SLICE-SIGNATURE sibling sweep in the shape classifier: a
// prior read back from schema history spells a domain's empty CHECK list nil
// where the live boundary spells it empty. ClassifyShape (reached from the
// unforwarded-stream check's prior comparison) and its rename detection must
// see no change; a real CHECK change must still classify as one.
func TestClassifyShape_EmptyAndNilListsAreNoChange(t *testing.T) {
	table := func(name string, checks []ir.DomainCheck) *ir.Table {
		return &ir.Table{Schema: "public", Name: "t", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: name, Type: ir.Domain{Name: "d", BaseType: ir.Varchar{Length: 30}, Checks: checks}},
		}}
	}
	history, live := table("v", nil), table("v", []ir.DomainCheck{})
	if got := alteredColumns(history, live); len(got) != 0 {
		t.Errorf("alteredColumns(history, live) = %d altered column(s); an empty and a nil CHECK list are the same type", len(got))
	}
	shape, err := ClassifyShape(history, live)
	if err != nil || shape.Kind != ShapeKindNone {
		t.Errorf("ClassifyShape(history, live) = %v, %v; want no shape change", shape.Kind, err)
	}
	// A rename whose type differs only in the empty-vs-nil spelling is still a
	// rename, not a drop + add.
	if _, _, ok := diffRenameColumn([]*ir.Column{live.Columns[1]}, []*ir.Column{table("w", nil).Columns[1]}); !ok {
		t.Error("diffRenameColumn: a column renamed with an empty-vs-nil CHECK list was not recognised as a rename")
	}
	// The control: a real CHECK change is still an ALTER COLUMN TYPE.
	changed := table("v", []ir.DomainCheck{{Name: "c", Body: "VALUE <> ''"}})
	if got := alteredColumns(history, changed); len(got) != 1 {
		t.Errorf("alteredColumns(history, changed) = %d; a real CHECK change must be one altered column", len(got))
	}
}
