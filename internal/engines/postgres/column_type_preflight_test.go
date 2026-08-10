// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/typematrix"
)

func oneColumnTypeSchema(t ir.Type) (*ir.Schema, *ir.Table) {
	tbl := &ir.Table{
		Name:    "t",
		Columns: []*ir.Column{{Name: "c", Type: t, Nullable: true}},
	}
	return &ir.Schema{Tables: []*ir.Table{tbl}}, tbl
}

// THE PREFLIGHT AND THE EMITTER MUST AGREE, SHAPE BY SHAPE, IN BOTH DIRECTIONS.
//
// Both sides run under [preflightEmitOpts] — the most permissive options the
// run could have — so the only variable is the walk: does the preflight reach
// every column the table emitter renders, and does it refuse nothing more.
//
// Postgres is the engine where the over-refusal direction has real teeth,
// because two of its refusals are configuration-dependent and one of its
// emitColumnType arms is an internal "wrong entry point" guard rather than a
// refusal at all. A preflight that called emitColumnType naively would refuse
// every ENUM column, every GEOMETRY column and every extension column on every
// PG target. The matrix carries all three.
func TestPreflightColumnTypesAgreesWithTheTableEmitter(t *testing.T) {
	var refused, accepted int
	for _, c := range typematrix.Cases() {
		schema, tbl := oneColumnTypeSchema(c.Type)
		preflightErr := (Engine{}).PreflightColumnTypes(schema)
		_, emitErr := emitTableDef("", tbl, preflightEmitOpts(schema))

		switch {
		case preflightErr != nil && emitErr == nil:
			t.Errorf("%s: the PREFLIGHT refused a type the emitter renders happily — an over-refusal, "+
				"which breaks migrations that work today.\npreflight: %v", c.Name, preflightErr)
		case preflightErr == nil && emitErr != nil:
			t.Errorf("%s: the EMITTER refuses this type and the preflight does not — the refusal is "+
				"back to firing at CREATE TABLE, after the plan and with earlier tables already "+
				"created.\nemitter: %v", c.Name, emitErr)
		case preflightErr != nil:
			refused++
		default:
			accepted++
		}
	}
	if refused == 0 {
		t.Error("no shape in the matrix was refused; PG has no legal zero-length char/varchar, no " +
			"third macaddr width and no rendering for a nil array element, and must refuse those")
	}
	if accepted == 0 {
		t.Error("no shape in the matrix was accepted; the preflight is refusing everything")
	}
}

// THE ENUM CARVE-OUT, PINNED IN BOTH DIRECTIONS.
//
// A top-level Enum column must NOT be refused (emitColumnDef resolves it to a
// generated type name and never asks emitColumnType), and an Enum reached
// through an ARRAY must be — the emitter has no branch for that one and refuses
// it, so a preflight that skipped it would be the coverage gap rather than the
// over-refusal. The two arms are one line apart in the implementation and are
// exactly the pair a careless simplification would collapse.
func TestPreflightColumnTypes_EnumCarveOutIsTopLevelOnly(t *testing.T) {
	top, _ := oneColumnTypeSchema(ir.Enum{Values: []string{"a", "b"}})
	if err := (Engine{}).PreflightColumnTypes(top); err != nil {
		t.Errorf("a top-level ENUM column must not be refused — emitColumnDef renders it as a "+
			"per-column generated type and emitColumnType is never asked: %v", err)
	}
	nested, _ := oneColumnTypeSchema(ir.Array{Element: ir.Enum{Values: []string{"a"}}})
	if err := (Engine{}).PreflightColumnTypes(nested); err == nil {
		t.Error("an ENUM inside an ARRAY has no emitColumnDef branch and IS refused by the emitter; " +
			"the preflight must refuse it too or the refusal is back to firing at CREATE TABLE")
	}
}

// THE DECLARED GAP, PINNED SO IT CANNOT BE MISTAKEN FOR COVERAGE.
//
// [ir.ColumnTypeEmitPreflighter] says in words that configuration-dependent
// refusals are deliberately not asked by this connection-free surface. This is
// the check behind those words: the preflight accepts a GEOMETRY column and an
// un-enabled extension column, and the EMITTER still refuses both once the real
// (non-permissive) options are known. Without the second half this would look
// like a silent hole rather than a relocation.
func TestPreflightColumnTypes_LeavesTheConfigurationDependentRefusalsToTheEmitter(t *testing.T) {
	geo, geoTbl := oneColumnTypeSchema(ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326})
	if err := (Engine{}).PreflightColumnTypes(geo); err != nil {
		t.Errorf("PostGIS presence is probed when the WRITER opens; refusing geometry here would "+
			"break every PG target that does have the extension: %v", err)
	}
	if _, err := emitTableDef("", geoTbl, emitOpts{}); err == nil {
		t.Error("with HasPostGIS false the emitter must still refuse geometry — that refusal is where " +
			"the preflight's gap is supposed to be caught, in the create-tables phase")
	}

	ext, extTbl := oneColumnTypeSchema(ir.ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{384}})
	if err := (Engine{}).PreflightColumnTypes(ext); err != nil {
		t.Errorf("--enable-pg-extension is a flag this surface does not carry; refusing a catalogued "+
			"extension column here would break every run that passes the flag: %v", err)
	}
	if _, err := emitTableDef("", extTbl, emitOpts{}); err == nil {
		t.Error("with no enabled extensions the emitter must still refuse the column and name the flag")
	}
}

// The refusal must name the TABLE and the COLUMN, not just the type.
func TestPreflightColumnTypes_NamesTheTableAndColumn(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "legacy_users",
		Columns: []*ir.Column{{Name: "marker", Type: ir.Varchar{Length: 0}}},
	}}}
	err := (Engine{}).PreflightColumnTypes(schema)
	if err == nil {
		t.Fatal("PG refuses VARCHAR(0) at CREATE TABLE (SQLSTATE 22023) and the preflight must too")
	}
	for _, want := range []string{"legacy_users", "marker", "VARCHAR(0)", "--type-override"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q; got: %v", want, err)
		}
	}
}

// The control: an ordinary MySQL-sourced schema must not be refused.
func TestPreflightColumnTypes_AcceptsAnOrdinarySchema(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true}},
			{Name: "email", Type: ir.Varchar{Length: 255, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}},
			{Name: "notes", Type: ir.Text{Size: ir.TextLong}},
			{Name: "flags", Type: ir.Set{Values: []string{"a", "b"}}},
			{Name: "kind", Type: ir.Enum{Values: []string{"x", "y"}}},
			{Name: "payload", Type: ir.JSON{Binary: true}},
			{Name: "placed_at", Type: ir.DateTime{Precision: 6}},
		},
	}}}
	if err := (Engine{}).PreflightColumnTypes(schema); err != nil {
		t.Fatalf("every column here is a routine MySQL shape with a PG landing: %v", err)
	}
}

// preflightEmitOpts must derive its permissive extension set from the SCHEMA,
// including through an array element — the one nesting emitColumnType follows.
func TestPreflightEmitOpts_DerivesTheExtensionSetFromTheSchema(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "a", Type: ir.ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{3}}},
			{Name: "b", Type: ir.Array{Element: ir.ExtensionType{Extension: "citext", Name: "citext"}}},
			{Name: "c", Type: ir.Integer{Width: 32}},
		},
	}}}
	opts := preflightEmitOpts(schema)
	if !opts.HasPostGIS {
		t.Error("HasPostGIS must be true — the real value needs a connection this surface does not have")
	}
	for _, want := range []string{"vector", "citext"} {
		if !opts.EnabledExtensions[want] {
			t.Errorf("extension %q named by the schema is not in the permissive set %v", want, opts.EnabledExtensions)
		}
	}
	if len(opts.EnabledExtensions) != 2 {
		t.Errorf("the permissive set must be derived from the schema, not from the catalog; got %v",
			opts.EnabledExtensions)
	}
}

// Nil-shaped input must not turn a preflight into a panic on a path that
// previously reached the emitter.
func TestPreflightColumnTypes_SkipsNilShapes(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{
		nil,
		{Name: "t", Columns: []*ir.Column{nil, {Name: "c"}}},
	}}
	if err := (Engine{}).PreflightColumnTypes(schema); err != nil {
		t.Fatalf("nil tables/columns/types must be skipped, not refused: %v", err)
	}
	if err := (Engine{}).PreflightColumnTypes(nil); err == nil {
		t.Error("a nil SCHEMA is a caller bug and must be refused loudly")
	}
}
