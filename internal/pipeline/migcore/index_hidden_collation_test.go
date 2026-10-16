// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
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
