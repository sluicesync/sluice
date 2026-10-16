// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestIndexReadsColumn: a plain entry, and an expression entry naming the
// column outside a literal, in either engine's spelling and any case.
func TestIndexReadsColumn(t *testing.T) {
	expr := func(e string) *ir.Index { return &ir.Index{Columns: []ir.IndexColumn{{Expression: e}}} }
	for _, tc := range []struct {
		idx  *ir.Index
		name string
		want bool
	}{
		{&ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}}, "k", true},
		{&ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}}, "kk", false},
		{expr("lower(k)"), "k", true},
		{expr("lower((email)::text)"), "email", true},                       // Postgres
		{expr("lower(cast(Email as char charset utf8mb4))"), "email", true}, // MySQL, case-folded
		{expr("lower(emailx)"), "email", false},
		{expr("lower('email')"), "email", false}, // a literal
		{expr("concat(a, 'x', b)"), "b", true},
		{expr("lower(my col)"), "my col", true}, // a name that is not one token
		{expr("lower(naïve)"), "naïve", true},
		{nil, "k", false},
	} {
		if got := IndexReadsColumn(tc.idx, tc.name); got != tc.want {
			t.Errorf("%+v / %q: %v; want %v", tc.idx, tc.name, got, tc.want)
		}
	}
}

// TestKeyCollationChange_FunctionalKey: a collation change of a column a
// functional UNIQUE key reads is a key collation change (the window doors
// and the manifest stamp), and one a NON-unique functional index reads is
// not.
func TestKeyCollationChange_FunctionalKey(t *testing.T) {
	shape := func(coll string, unique bool) *ir.Table {
		return &ir.Table{
			Name: "t",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "email", Type: ir.Varchar{Length: 40, Charset: "utf8mb4", Collation: coll}},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
			Indexes:    []*ir.Index{{Name: "fx", Unique: unique, Columns: []ir.IndexColumn{{Expression: "lower(email)"}}}},
		}
	}
	if !KeyCollationChange(shape("utf8mb4_bin", true), shape("utf8mb4_0900_ai_ci", true)) {
		t.Error("a functional unique key's column changed collation unseen")
	}
	if KeyCollationChange(shape("utf8mb4_bin", false), shape("utf8mb4_0900_ai_ci", false)) {
		t.Error("a non-unique functional index made a key collation change")
	}
	unestablished := shape("", true)
	unestablished.Columns[1].Type = ir.Varchar{Length: 40, Collation: "C"}
	if !KeyCollationUnestablished(nil, unestablished) {
		t.Error("a functional unique key's column with an unestablished collation unseen")
	}
}
