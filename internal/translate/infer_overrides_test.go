// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func inferSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "is_active", Type: ir.Integer{Width: 64}},
			{Name: "meta", Type: ir.Text{Size: ir.TextLong}},
		},
	}}}
}

// TestApplyInferredOverrides_Rewrites pins the happy path: the named columns are
// re-typed, SourceColumnType records the pre-override type (the Bug-47 contract,
// identical to ApplyMappings), and unaffected columns/tables share pointers.
func TestApplyInferredOverrides_Rewrites(t *testing.T) {
	s := inferSchema()
	out, err := ApplyInferredOverrides(s, []InferredOverride{
		{Table: "users", Column: "is_active", Type: ir.Boolean{}},
		{Table: "users", Column: "meta", Type: ir.JSON{Binary: true}},
	})
	if err != nil {
		t.Fatalf("ApplyInferredOverrides: %v", err)
	}

	cols := out.Tables[0].Columns
	if _, ok := cols[1].Type.(ir.Boolean); !ok {
		t.Fatalf("is_active type = %T, want ir.Boolean", cols[1].Type)
	}
	if cols[1].SourceColumnType != (ir.Integer{Width: 64}) {
		t.Fatalf("is_active SourceColumnType = %v, want Integer(64)", cols[1].SourceColumnType)
	}
	if j, ok := cols[2].Type.(ir.JSON); !ok || !j.Binary {
		t.Fatalf("meta type = %T, want ir.JSON{Binary:true}", cols[2].Type)
	}

	// Unaffected column shares its pointer with the source.
	if out.Tables[0].Columns[0] != s.Tables[0].Columns[0] {
		t.Fatalf("unaffected column was copied")
	}
	// Source schema is unchanged (copy-on-write).
	if _, ok := s.Tables[0].Columns[1].Type.(ir.Integer); !ok {
		t.Fatalf("source schema mutated")
	}
}

// TestApplyInferredOverrides_Empty pins the no-op fast path: an empty override
// slice returns the SAME schema pointer (identity — the off path).
func TestApplyInferredOverrides_Empty(t *testing.T) {
	s := inferSchema()
	out, err := ApplyInferredOverrides(s, nil)
	if err != nil {
		t.Fatalf("ApplyInferredOverrides: %v", err)
	}
	if out != s {
		t.Fatalf("empty overrides did not return the identity schema")
	}
}

// TestApplyInferredOverrides_Errors pins the strict validation (unknown
// table/column, duplicate, nil type, nil schema).
func TestApplyInferredOverrides_Errors(t *testing.T) {
	s := inferSchema()
	cases := []struct {
		name string
		ov   []InferredOverride
	}{
		{"unknown table", []InferredOverride{{Table: "nope", Column: "x", Type: ir.Boolean{}}}},
		{"unknown column", []InferredOverride{{Table: "users", Column: "nope", Type: ir.Boolean{}}}},
		{"duplicate", []InferredOverride{
			{Table: "users", Column: "is_active", Type: ir.Boolean{}},
			{Table: "users", Column: "is_active", Type: ir.Integer{Width: 16}},
		}},
		{"nil type", []InferredOverride{{Table: "users", Column: "is_active"}}},
		{"missing column field", []InferredOverride{{Table: "users", Type: ir.Boolean{}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ApplyInferredOverrides(s, tc.ov); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
	if _, err := ApplyInferredOverrides(nil, []InferredOverride{{Table: "t", Column: "c", Type: ir.Boolean{}}}); err == nil {
		t.Fatalf("expected error for nil schema")
	}
}
