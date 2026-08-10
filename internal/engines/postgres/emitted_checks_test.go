// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestPredictEmittedChecks_PostgresMatrix pins BOTH constraints this
// writer synthesizes, and the controls that must produce nothing.
//
// Both, not one: the SET-membership CHECK and the Bug-25 generated-enum
// CHECK are separate emitter loops with separate name and body shapes,
// and item 158 is the precedent for a fix reaching one axis of a pair
// and silently missing the other.
func TestPredictEmittedChecks_PostgresMatrix(t *testing.T) {
	w := &SchemaWriter{}
	enum := ir.Enum{TypeName: "mood", Values: []string{"happy", "sad"}}
	table := &ir.Table{
		Name: "prefs",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "flags", Type: ir.Set{Values: []string{"email", "sms"}}},
			// Controls. A PLAIN enum column keeps the native PG enum type
			// and needs no CHECK (the type constrains the values), and a
			// generated non-enum column is nothing to do with either loop.
			{Name: "mood", Type: enum},
			{Name: "n", Type: ir.Integer{Width: 32}, GeneratedExpr: "id + 1", GeneratedStored: true},
			{Name: "g_mood", Type: enum, GeneratedExpr: "mood", GeneratedStored: true},
		},
	}

	got := w.PredictEmittedChecks(table)
	want := map[string]string{
		"prefs_flags_set":       `"flags" <@ ARRAY['email','sms']::TEXT[]`,
		"prefs_g_mood_enum_chk": `"g_mood" IN ('happy','sad')`,
	}
	if len(got) != len(want) {
		t.Fatalf("predicted %d check(s), want %d: %+v", len(got), len(want), got)
	}
	for _, c := range got {
		if !c.SluiceEmitted {
			t.Errorf("predicted check %q is not marked SluiceEmitted", c.Name)
		}
		wantExpr, ok := want[c.Name]
		if !ok {
			t.Errorf("unexpected predicted check %q => %q", c.Name, c.Expr)
			continue
		}
		if c.Expr != wantExpr {
			t.Errorf("predicted %q => %q; want %q", c.Name, c.Expr, wantExpr)
		}
		delete(want, c.Name)
	}
	for name := range want {
		t.Errorf("predicted check %q missing", name)
	}
}

// TestPredictedChecksAreTheEmittedClausesWithoutTheWrapper binds each
// prediction to the CREATE TABLE the writer would actually issue —
// the independent expected value here being the emitter's own output,
// not a hand-written `want` string that the same hand also put in the
// prediction.
func TestPredictedChecksAreTheEmittedClausesWithoutTheWrapper(t *testing.T) {
	table := &ir.Table{
		Name: "prefs",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "flags", Type: ir.Set{Values: []string{"email", "sms"}}},
			{Name: "g_mood", Type: ir.Enum{TypeName: "mood", Values: []string{"happy", "sad"}}, GeneratedExpr: "mood", GeneratedStored: true},
		},
		PrimaryKey: &ir.Index{Name: "prefs_pkey", Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	ddl, err := emitTableDef("public", table, emitOpts{})
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	for _, c := range (&SchemaWriter{}).PredictEmittedChecks(table) {
		clause := `CONSTRAINT "` + c.Name + `" CHECK (` + c.Expr + `)`
		if !strings.Contains(ddl, clause) {
			t.Errorf("predicted constraint is not in the DDL the writer emits.\n predicted: %s\n DDL:\n%s", clause, ddl)
		}
	}
}

// TestPostgresGeneratedEnumEmitDispatchIsDeclaredType is the premise pin
// behind translate's `target_emit_shape.go:postgresTargetEmitShape:c.Type`
// domain-gate exemption.
//
// The compare-lane rule predicts TEXT for a generated enum column, and
// reads the DECLARED type to decide — against internal/translate's usual
// unwrap-the-domain rule — for exactly one reason: this emitter does the
// same. If the emitter ever starts unwrapping (whether as a Bug-233 fix
// or by accident), the prediction becomes wrong in the direction that
// reports drift on a target `migrate` created, and the exemption's stated
// reason becomes false. This is what fails then.
//
// Both directions, so it cannot pass vacuously: a BARE generated enum
// must emit TEXT, and a DOMAIN-wrapped one must not.
func TestPostgresGeneratedEnumEmitDispatchIsDeclaredType(t *testing.T) {
	enum := ir.Enum{TypeName: "mood", Values: []string{"happy", "sad"}}
	table := &ir.Table{Name: "t"}

	bare := &ir.Column{Name: "g", Type: enum, GeneratedExpr: "mood", GeneratedStored: true}
	def, err := emitColumnDef(table, bare, emitOpts{})
	if err != nil {
		t.Fatalf("emitColumnDef(bare generated enum): %v", err)
	}
	if !strings.Contains(def, " TEXT") {
		t.Fatalf("a generated enum column emits %q; Bug 25 says it must land as TEXT, and the compare-lane "+
			"prediction says so too", def)
	}

	wrapped := &ir.Column{
		Name:            "d",
		Type:            ir.Domain{Name: "d_mood", BaseType: enum},
		GeneratedExpr:   "mood",
		GeneratedStored: true,
	}
	def, err = emitColumnDef(table, wrapped, emitOpts{})
	if err != nil {
		t.Fatalf("emitColumnDef(generated domain-over-enum): %v", err)
	}
	if strings.Contains(def, " TEXT") {
		t.Fatalf("a generated DOMAIN-over-enum column now emits %q (TEXT). The emitter has started reading "+
			"through the wrapper, so translate's postgresTargetEmitShape must unwrap too — its domain-gate "+
			"exemption cites this dispatch as the reason it reads the DECLARED type, and that reason is now "+
			"false. Until both move, `schema diff` reports phantom drift on this column class", def)
	}
}
