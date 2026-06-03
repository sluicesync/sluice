// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRetargetForEngine_PGtoMySQL pins the v0.7.0 auto-emit defaults
// for the cross-engine-diff path. PG-native types that don't have a
// MySQL counterpart get rewritten to the IR shape the MySQL DDL
// emitter would land them on. Without this rewrite, `sluice schema
// diff` against a MySQL target would flag every translated column as
// drift even when the target storage is exactly what sluice would
// produce.
func TestRetargetForEngine_PGtoMySQL(t *testing.T) {
	src := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.UUID{}},
				{Name: "ip_address", Type: ir.Inet{}},
				{Name: "subnet", Type: ir.Cidr{}},
				{Name: "mac", Type: ir.Macaddr{}},
				{Name: "tags", Type: ir.Array{Element: ir.Text{Size: ir.TextLong}}},
				{Name: "email", Type: ir.Varchar{Length: 255}},
				{Name: "active", Type: ir.Boolean{}},
			},
		},
	}}

	got := RetargetForEngine(src, "postgres", "mysql")

	wantTypes := map[string]ir.Type{
		"id":         ir.Char{Length: 36},
		"ip_address": ir.Varchar{Length: 45},
		"subnet":     ir.Varchar{Length: 45},
		"mac":        ir.Varchar{Length: 30},
		"tags":       ir.JSON{Binary: true},
		"email":      ir.Varchar{Length: 255},
		"active":     ir.Boolean{},
	}
	for _, col := range got.Tables[0].Columns {
		want, ok := wantTypes[col.Name]
		if !ok {
			t.Errorf("unexpected column %q", col.Name)
			continue
		}
		if col.Type != want {
			t.Errorf("column %q type = %v; want %v", col.Name, col.Type, want)
		}
	}
}

// TestRetargetForEngine_PGtoMySQL_Hstore pins the ADR-0032 default
// cross-engine translator: PG `hstore` rewrites to MySQL JSON.
// Mirrors the mysql/ddl_emit.go::emitColumnType case so the diff
// path sees the same shape the migrate path lands on.
func TestRetargetForEngine_PGtoMySQL_Hstore(t *testing.T) {
	src := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "attrs",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "tags", Type: ir.ExtensionType{Extension: "hstore", Name: "hstore"}},
			},
		},
	}}
	got := RetargetForEngine(src, "postgres", "mysql")
	tags := got.Tables[0].Columns[1]
	want := ir.JSON{Binary: true}
	if tags.Type != want {
		t.Errorf("hstore retargeted type = %v; want %v", tags.Type, want)
	}
}

// TestRetargetForEngine_PGtoMySQL_CiText pins the citext → MySQL
// VARCHAR with `_ci` collation translator.
func TestRetargetForEngine_PGtoMySQL_CiText(t *testing.T) {
	src := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "email", Type: ir.ExtensionType{Extension: "citext", Name: "citext"}},
			},
		},
	}}
	got := RetargetForEngine(src, "postgres", "mysql")
	email := got.Tables[0].Columns[1]
	want := ir.Varchar{Length: 255, Collation: "utf8mb4_0900_ai_ci"}
	if email.Type != want {
		t.Errorf("citext retargeted type = %v; want %v", email.Type, want)
	}
}

// TestRetargetForEngine_PGtoMySQL_OtherExtensionPassthrough confirms
// the rewrite is narrow: extension types without a default cross-
// engine translator (vector, pg_trgm, postgis) pass through unchanged
// so the cross-engine refusal in checkCrossEngineSupportable fires.
func TestRetargetForEngine_PGtoMySQL_OtherExtensionPassthrough(t *testing.T) {
	src := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "items",
			Columns: []*ir.Column{
				{Name: "embedding", Type: ir.ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{384}}},
			},
		},
	}}
	got := RetargetForEngine(src, "postgres", "mysql")
	emb := got.Tables[0].Columns[0]
	ext, ok := emb.Type.(ir.ExtensionType)
	if !ok {
		t.Fatalf("vector retargeted to %T; want ir.ExtensionType (unchanged)", emb.Type)
	}
	if ext.Extension != "vector" {
		t.Errorf("vector extension name = %q; want unchanged", ext.Extension)
	}
}

// TestRetargetForEngine_PGtoPlanetScale uses the same retarget rules
// as PG→MySQL since PlanetScale's MySQL flavor has the same native-
// type set.
func TestRetargetForEngine_PGtoPlanetScale(t *testing.T) {
	src := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "t",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.UUID{}},
			},
		},
	}}
	got := RetargetForEngine(src, "postgres", "planetscale")
	if got.Tables[0].Columns[0].Type != (ir.Char{Length: 36}) {
		t.Errorf("uuid column not retargeted on planetscale flavor: %v", got.Tables[0].Columns[0].Type)
	}
}

// TestRetargetForEngine_SameEngineIdentity verifies that same-engine
// pairs (and unknown engine pairs) return the schema unchanged. The
// retarget pass exists for cross-engine drift comparison; same-engine
// diffs are already comparable in their source-native IR.
func TestRetargetForEngine_SameEngineIdentity(t *testing.T) {
	src := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "t",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.UUID{}},
			},
		},
	}}
	got := RetargetForEngine(src, "postgres", "postgres")
	if got != src {
		t.Errorf("same-engine retarget returned a copy; expected identity")
	}

	got = RetargetForEngine(src, "mysql", "postgres")
	if got.Tables[0].Columns[0].Type != (ir.UUID{}) {
		t.Errorf("MySQL→PG retarget rewrote a type; v0.8.0 scope is PG→MySQL only")
	}
}

// TestRetargetForEngine_NilSafe defends against accidentally calling
// the function with a nil schema (e.g. a caller that didn't check the
// SchemaReader's return).
func TestRetargetForEngine_NilSafe(t *testing.T) {
	if got := RetargetForEngine(nil, "postgres", "mysql"); got != nil {
		t.Errorf("nil schema retarget = %v; want nil", got)
	}
}

// TestRetargetForEngine_PreservesUnaffectedTables verifies the pass
// only allocates copies for tables containing rewritten columns —
// tables with only portable types share their backing pointer with
// the input schema. Important for memory behaviour on large schemas
// where most tables don't touch the rewritten types.
func TestRetargetForEngine_PreservesUnaffectedTables(t *testing.T) {
	plain := &ir.Table{
		Name: "plain",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "name", Type: ir.Varchar{Length: 255}},
		},
	}
	withUUID := &ir.Table{
		Name: "with_uuid",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.UUID{}},
		},
	}
	src := &ir.Schema{Tables: []*ir.Table{plain, withUUID}}
	got := RetargetForEngine(src, "postgres", "mysql")

	if got.Tables[0] != plain {
		t.Errorf("plain table was copied; expected identity (no rewritten columns)")
	}
	if got.Tables[1] == withUUID {
		t.Errorf("with_uuid table was not copied; expected a copy")
	}
}
