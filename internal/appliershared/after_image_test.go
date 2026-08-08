// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// afterImageColTypes is a three-column target, one of which is GENERATED —
// the column whose absence from an after-image is never an omission,
// because neither engine accepts a value for it.
func afterImageColTypes() map[string]*ir.Column {
	return map[string]*ir.Column{
		"id":   {Name: "id", Type: ir.Integer{Width: 64}},
		"tag":  {Name: "tag", Type: ir.Text{}},
		"body": {Name: "body", Type: ir.Text{}},
		"gen":  {Name: "gen", Type: ir.Text{}, GeneratedExpr: "upper(tag)"},
	}
}

func TestMissingNonGeneratedColumns(t *testing.T) {
	cases := []struct {
		name     string
		row      ir.Row
		colTypes map[string]*ir.Column
		want     []string
	}{
		{
			name:     "complete after-image (generated column absent is not an omission)",
			row:      ir.Row{"id": 1, "tag": "a", "body": "b"},
			colTypes: afterImageColTypes(),
			want:     nil,
		},
		{
			name:     "unchanged-TOAST shape: one large column omitted",
			row:      ir.Row{"id": 1, "tag": "a"},
			colTypes: afterImageColTypes(),
			want:     []string{"body"},
		},
		{
			name:     "several omitted, reported sorted",
			row:      ir.Row{"id": 1},
			colTypes: afterImageColTypes(),
			want:     []string{"body", "tag"},
		},
		{
			// A present-but-NULL column is NOT missing. The distinction is
			// the whole point: NULL is the source saying "this is now
			// NULL"; absent is the source saying "leave it alone".
			name:     "present-with-nil is not missing",
			row:      ir.Row{"id": 1, "tag": nil, "body": nil},
			colTypes: afterImageColTypes(),
			want:     nil,
		},
		{
			// Tolerant in the same direction as NonGeneratedRowKeys: an
			// unknown target shape (cache cold) is not evidence of an
			// omission, so the upsert keeps its pre-fix behaviour there
			// rather than degrading every table on a cold cache.
			name:     "cold cache reports nothing missing",
			row:      ir.Row{"id": 1},
			colTypes: nil,
			want:     nil,
		},
		{
			// The target has a column the source never mentions. It reads
			// identically to an omission and is treated as one — stated
			// where the decision is made (dispatchUpdate): the applier
			// cannot tell "target-only column" from "TOAST-omitted", and
			// the conservative reading is the one that never fabricates.
			name: "target-only column counts as missing",
			row:  ir.Row{"id": 1, "tag": "a", "body": "b"},
			colTypes: map[string]*ir.Column{
				"id":    {Name: "id", Type: ir.Integer{Width: 64}},
				"tag":   {Name: "tag", Type: ir.Text{}},
				"body":  {Name: "body", Type: ir.Text{}},
				"extra": {Name: "extra", Type: ir.Text{}},
			},
			want: []string{"extra"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MissingNonGeneratedColumns(tc.row, tc.colTypes)
			if !slices.Equal(got, tc.want) {
				t.Errorf("got %v; want %v", got, tc.want)
			}
		})
	}
}

func TestRefuseNoRowPredicate(t *testing.T) {
	err := RefuseNoRowPredicate("mysql", "update", "shop", "orders", ir.Row{"gen": "x"})
	if !errors.Is(err, ErrNoRowPredicate) {
		t.Fatalf("error %v does not wrap ErrNoRowPredicate", err)
	}
	for _, want := range []string{"mysql", "update", "shop.orders", "gen"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not name %q: %v", want, err)
		}
	}
	// An empty schema renders the bare table name, matching
	// ir.Change.QualifiedName so a refusal reads like every other
	// applier message.
	err = RefuseNoRowPredicate("postgres", "delete", "", "orders", nil)
	if strings.Contains(err.Error(), ".orders") {
		t.Errorf("empty schema should not render a leading dot: %v", err)
	}
}
