// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
)

// fakeInferReader is an ir.SchemaReader that also implements
// ir.InferredTypeValidator, returning canned per-column results and recording
// which columns were actually validated (so explicit-wins / non-candidate
// skipping can be asserted).
type fakeInferReader struct {
	results map[string]inferResult
	called  []string
}

type inferResult struct {
	conforms  bool
	resolved  ir.Type
	validated int64
	err       error
}

func (f *fakeInferReader) ReadSchema(context.Context) (*ir.Schema, error) { return nil, nil }

func (f *fakeInferReader) ValidateInferredType(
	_ context.Context, _, column string, target ir.Type,
) (conforms bool, resolved ir.Type, validated int64, err error) {
	f.called = append(f.called, column)
	r, ok := f.results[column]
	if !ok {
		return false, target, 0, nil
	}
	return r.conforms, r.resolved, r.validated, r.err
}

func inferTestSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "is_active", Type: ir.Integer{Width: 64}},       // boolean candidate
			{Name: "created_at", Type: ir.Text{Size: ir.TextLong}}, // temporal candidate
			{Name: "metadata", Type: ir.Text{Size: ir.TextLong}},   // jsonb candidate
			{Name: "user_id", Type: ir.Text{Size: ir.TextLong}},    // uuid candidate
			{Name: "name", Type: ir.Text{Size: ir.TextLong}},       // no hint → not a candidate
			{Name: "count", Type: ir.Integer{Width: 64}},           // no hint → not a candidate
		},
	}}}
}

func colType(s *ir.Schema, name string) ir.Type {
	for _, c := range s.Tables[0].Columns {
		if c.Name == name {
			return c.Type
		}
	}
	return nil
}

// TestApplyInferredTypes_PromotesAndKeeps pins the orchestration: a conforming
// candidate is promoted to its resolved type; a non-conforming one is kept; a
// non-candidate is never validated.
func TestApplyInferredTypes_PromotesAndKeeps(t *testing.T) {
	tsTZ := ir.Timestamp{Precision: 6, WithTimeZone: true}
	sr := &fakeInferReader{results: map[string]inferResult{
		"is_active":  {conforms: true, resolved: ir.Boolean{}, validated: 5},
		"created_at": {conforms: true, resolved: tsTZ, validated: 5},
		"metadata":   {conforms: true, resolved: ir.JSON{Binary: true}, validated: 5},
		"user_id":    {conforms: false, validated: 5}, // the cus_abc123 case
	}}
	m := &Migrator{}
	out, err := m.applyInferredTypes(context.Background(), sr, inferTestSchema())
	if err != nil {
		t.Fatalf("applyInferredTypes: %v", err)
	}

	if _, ok := colType(out, "is_active").(ir.Boolean); !ok {
		t.Errorf("is_active = %T, want Boolean", colType(out, "is_active"))
	}
	if colType(out, "created_at") != tsTZ {
		t.Errorf("created_at = %v, want %v", colType(out, "created_at"), tsTZ)
	}
	if j, ok := colType(out, "metadata").(ir.JSON); !ok || !j.Binary {
		t.Errorf("metadata = %T, want JSON{Binary:true}", colType(out, "metadata"))
	}
	// Non-conforming candidate kept at its safe text type.
	if _, ok := colType(out, "user_id").(ir.Text); !ok {
		t.Errorf("user_id = %T, want Text (kept)", colType(out, "user_id"))
	}
	// Non-candidates were never validated.
	for _, c := range sr.called {
		if c == "name" || c == "count" || c == "id" {
			t.Errorf("non-candidate %q was validated", c)
		}
	}
	if len(sr.called) != 4 {
		t.Errorf("validated %d columns, want 4 candidates", len(sr.called))
	}
}

// TestApplyInferredTypes_ExplicitWins pins that an explicit --type-override on a
// candidate column is never inferred over: it is not even validated.
func TestApplyInferredTypes_ExplicitWins(t *testing.T) {
	sr := &fakeInferReader{results: map[string]inferResult{
		"is_active": {conforms: true, resolved: ir.Boolean{}, validated: 5},
	}}
	m := &Migrator{Mappings: []config.Mapping{
		{Table: "users", Column: "is_active", TargetType: "smallint"},
	}}
	if _, err := m.applyInferredTypes(context.Background(), sr, inferTestSchema()); err != nil {
		t.Fatalf("applyInferredTypes: %v", err)
	}
	for _, c := range sr.called {
		if c == "is_active" {
			t.Fatalf("explicitly-overridden column was validated (explicit must win)")
		}
	}
}

// TestApplyInferredTypes_NonValidatorSource pins the loud refusal when the
// source reader does not implement the validator (inference is SQLite/D1-only).
func TestApplyInferredTypes_NonValidatorSource(t *testing.T) {
	m := &Migrator{}
	_, err := m.applyInferredTypes(context.Background(), nonValidatorReader{}, inferTestSchema())
	if err == nil {
		t.Fatalf("expected loud refusal for a non-validator source")
	}
}

// TestApplyInferredTypes_ValidatorError pins error propagation (a validation
// query failure aborts rather than silently skipping the column).
func TestApplyInferredTypes_ValidatorError(t *testing.T) {
	sr := &fakeInferReader{results: map[string]inferResult{
		"is_active": {err: errors.New("boom")},
	}}
	m := &Migrator{}
	if _, err := m.applyInferredTypes(context.Background(), sr, inferTestSchema()); err == nil {
		t.Fatalf("expected validator error to propagate")
	}
}

// nonValidatorReader is an ir.SchemaReader with NO validator surface.
type nonValidatorReader struct{}

func (nonValidatorReader) ReadSchema(context.Context) (*ir.Schema, error) { return nil, nil }

// TestInferTypeCandidate pins the name-hint × source-family candidate selection
// for every family, including the already-rich and wrong-family non-candidates.
func TestInferTypeCandidate(t *testing.T) {
	txt := ir.Text{Size: ir.TextLong}
	intT := ir.Integer{Width: 64}
	cases := []struct {
		name     string
		typ      ir.Type
		wantOK   bool
		wantKind string // type name of the candidate target
	}{
		{"is_active", intT, true, "ir.Boolean"},
		{"has_photo", intT, true, "ir.Boolean"},
		{"deleted_flag", intT, true, "ir.Boolean"},
		{"created_at", txt, true, "ir.Timestamp"},
		{"login_time", txt, true, "ir.Timestamp"},
		{"created", txt, true, "ir.Timestamp"},
		{"updated", txt, true, "ir.Timestamp"},
		{"config_json", txt, true, "ir.JSON"},
		{"metadata", txt, true, "ir.JSON"},
		{"payload", txt, true, "ir.JSON"},
		{"settings", txt, true, "ir.JSON"},
		{"attributes", txt, true, "ir.JSON"},
		{"user_id", txt, true, "ir.UUID"},
		{"row_uuid", txt, true, "ir.UUID"},
		{"uuid", txt, true, "ir.UUID"},
		{"guid", txt, true, "ir.UUID"},
		// Non-candidates:
		{"name", txt, false, ""},        // no hint
		{"count", intT, false, ""},      // no boolean hint
		{"is_active", txt, false, ""},   // boolean hint but wrong family (TEXT)
		{"created_at", intT, false, ""}, // temporal hint but wrong family (INTEGER)
		// Already-rich columns fall through (their family is Boolean/Timestamp).
		{"is_active", ir.Boolean{}, false, ""},
		{"created_at", ir.Timestamp{}, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/"+kindName(tc.typ), func(t *testing.T) {
			target, hint, ok := inferTypeCandidate(&ir.Column{Name: tc.name, Type: tc.typ})
			if ok != tc.wantOK {
				t.Fatalf("ok=%v want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if got := kindName(target); got != tc.wantKind {
				t.Fatalf("target kind = %s want %s", got, tc.wantKind)
			}
			if hint == "" {
				t.Fatalf("expected a non-empty hint label")
			}
		})
	}
}

func kindName(t ir.Type) string {
	switch t.(type) {
	case ir.Boolean:
		return "ir.Boolean"
	case ir.Timestamp:
		return "ir.Timestamp"
	case ir.JSON:
		return "ir.JSON"
	case ir.UUID:
		return "ir.UUID"
	case ir.Text:
		return "ir.Text"
	case ir.Integer:
		return "ir.Integer"
	default:
		return "other"
	}
}
