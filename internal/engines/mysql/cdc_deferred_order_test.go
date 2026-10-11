// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func deferredShape(synthesized bool, names ...string) *tableSchema {
	tbl := &tableSchema{Schema: "app", Name: "kd", synthesized: synthesized}
	for _, n := range names {
		tbl.Columns = append(tbl.Columns, &ir.Column{Name: n})
	}
	return tbl
}

// TestHoldDecodedOrder_DeferredShapeMatrix pins BUG301-FULL-DEFERRED-SWAP's
// reader-side door: a shape that will not be a boundary (synthesized) may
// add or lose columns but may not reorder the ones it shares with the
// previous event; a shape that IS a boundary passes and carries the change
// to the pipeline's doors.
func TestHoldDecodedOrder_DeferredShapeMatrix(t *testing.T) {
	type step struct {
		shape  *tableSchema
		refuse bool
	}
	cases := []struct {
		name     string
		baseline []string // nil: no baseline
		steps    []step
	}{
		{"swap after baseline", []string{"id", "b", "c"}, []step{{deferredShape(true, "id", "c", "b"), true}}},
		{"rotation", []string{"id", "b", "c", "d"}, []step{{deferredShape(true, "id", "c", "d", "b"), true}}},
		{"pre-drop row keeps the order", []string{"id", "b", "c"}, []step{{deferredShape(true, "id", "b", "c"), false}}},
		{"added column, order held", []string{"id", "b", "c"}, []step{{deferredShape(true, "id", "b", "c", "d"), false}}},
		{"one shared name cannot flip", []string{"id", "b", "c"}, []step{{deferredShape(true, "x", "c"), false}}},
		{"swap that is a boundary passes", []string{"id", "b", "c"}, []step{{deferredShape(false, "id", "c", "b"), false}}},
		{"no baseline, nothing to compare", nil, []step{{deferredShape(true, "id", "c", "b"), false}}},
		{
			"previous decoded event is the basis", nil,
			[]step{{deferredShape(true, "id", "b", "c"), false}, {deferredShape(true, "id", "c", "b"), true}},
		},
		{
			"a boundary's order becomes the basis",
			[]string{"id", "b", "c"},
			[]step{{deferredShape(false, "id", "c", "b"), false}, {deferredShape(true, "id", "c", "b"), false}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &CDCReader{schema: "app"}
			if tc.baseline != nil {
				// No Schema: the record takes the reader's bound database.
				r.SetDecodedOrderBaseline([]ir.ColumnOrderRecord{{Table: "kd", SourceColumns: tc.baseline}})
			}
			for i, s := range tc.steps {
				err := r.holdDecodedOrder("app.kd", s.shape)
				switch {
				case s.refuse && !errors.Is(err, ir.ErrColumnOrderChanged):
					t.Fatalf("step %d: err = %v, want %v", i, err, ir.ErrColumnOrderChanged)
				case s.refuse && !strings.Contains(err.Error(), "--reset-target-data"):
					t.Fatalf("step %d: refusal names no re-copy remedy: %v", i, err)
				case !s.refuse && err != nil:
					t.Fatalf("step %d: err = %v, want none", i, err)
				}
			}
		})
	}
}
