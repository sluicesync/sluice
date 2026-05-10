// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"reflect"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

func TestBuildBatchInsert(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
	}

	cases := []struct {
		rows int
		want string
	}{
		{1, `INSERT INTO "public"."users" ("id", "email") VALUES ($1, $2)`},
		{3, `INSERT INTO "public"."users" ("id", "email") VALUES ($1, $2), ($3, $4), ($5, $6)`},
	}
	for _, c := range cases {
		got := buildBatchInsert("public", table, c.rows)
		if got != c.want {
			t.Errorf("buildBatchInsert(%d):\n got  %q\n want %q", c.rows, got, c.want)
		}
	}
}

func TestBuildBatchInsertSchemaQualified(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
		},
	}
	got := buildBatchInsert("app", table, 1)
	want := `INSERT INTO "app"."users" ("id") VALUES ($1)`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

func TestPrepareValuePassthrough(t *testing.T) {
	// Most types pass through unchanged.
	cases := []struct {
		name string
		in   any
		t    ir.Type
	}{
		{"int64", int64(42), ir.Integer{Width: 32}},
		{"string", "hello", ir.Varchar{Length: 32}},
		{"bool", true, ir.Boolean{}},
		{"float64", 3.14, ir.Float{Precision: ir.FloatDouble}},
		{"bytes", []byte{0xde, 0xad}, ir.Blob{Size: ir.BlobLong}},
		{"nil", nil, ir.Integer{Width: 32}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := prepareValue(c.in, c.t)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, c.in) {
				t.Errorf("prepareValue(%#v) = %#v; want %#v", c.in, got, c.in)
			}
		})
	}
}

func TestPrepareValueArrayConversion(t *testing.T) {
	cases := []struct {
		name string
		in   []any
		elem ir.Type
		want any
	}{
		{
			"int array → []int64",
			[]any{int64(1), int64(2), int64(3)},
			ir.Integer{Width: 32},
			[]int64{1, 2, 3},
		},
		{
			"text array → []string",
			[]any{"a", "b"},
			ir.Text{Size: ir.TextLong},
			[]string{"a", "b"},
		},
		{
			"bool array → []bool",
			[]any{true, false, true},
			ir.Boolean{},
			[]bool{true, false, true},
		},
		{
			"uuid array → []string",
			[]any{"00000000-0000-0000-0000-000000000001"},
			ir.UUID{},
			[]string{"00000000-0000-0000-0000-000000000001"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := prepareValue(c.in, ir.Array{Element: c.elem})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("\n got  %#v (%T)\n want %#v (%T)", got, got, c.want, c.want)
			}
		})
	}
}

func TestPrepareValueArrayWrongElementType(t *testing.T) {
	// An int64 element where the column is text[] → error.
	_, err := prepareValue([]any{int64(1)}, ir.Array{Element: ir.Text{Size: ir.TextLong}})
	if err == nil {
		t.Error("expected error for type mismatch in array element; got nil")
	}
}

// TestTableHasPGVectorColumn pins the predicate that drives whether
// writeViaCopy registers the per-conn pgvector codec. Bug 47: the
// codec must register exactly when (and only when) the table carries
// a vector column — otherwise the COPY path either misencodes the
// vector or pays a stray pg_type lookup on every unrelated table.
func TestTableHasPGVectorColumn(t *testing.T) {
	cases := []struct {
		name string
		cols []*ir.Column
		want bool
	}{
		{
			name: "no extension columns",
			cols: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "name", Type: ir.Varchar{Length: 64}},
			},
			want: false,
		},
		{
			name: "vector column present",
			cols: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "embedding", Type: ir.ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{3}}},
			},
			want: true,
		},
		{
			name: "different extension only",
			cols: []*ir.Column{
				{Name: "fingerprint", Type: ir.ExtensionType{Extension: "pg_trgm", Name: "trgm"}},
			},
			want: false,
		},
		{
			name: "empty columns",
			cols: nil,
			want: false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := tableHasPGVectorColumn(&ir.Table{Name: "t", Columns: c.cols})
			if got != c.want {
				t.Errorf("tableHasPGVectorColumn = %v, want %v", got, c.want)
			}
		})
	}
}
