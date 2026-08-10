// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/typematrix"
)

// oneColumnSchema wraps a single type as the minimal schema both the preflight
// and the table emitter accept, so the ONLY variable between the two verdicts
// is the column's type.
func oneColumnSchema(t ir.Type) (*ir.Schema, *ir.Table) {
	tbl := &ir.Table{
		Name:    "t",
		Columns: []*ir.Column{{Name: "c", Type: t, Nullable: true}},
	}
	return &ir.Schema{Tables: []*ir.Table{tbl}}, tbl
}

// THE PREFLIGHT AND THE EMITTER MUST AGREE, SHAPE BY SHAPE, IN BOTH DIRECTIONS.
//
// The preflight calls [emitColumnType] and so does [emitColumnDef], so by
// construction they cannot disagree about a TYPE — but "by construction" is
// exactly the phrase CLAUDE.md says to distrust, and the construction only
// holds while the preflight's walk keeps matching the emitter's. This drives
// both, over the whole family × shape matrix, and fails in BOTH directions:
//
//   - preflight refuses / emitter accepts  → an OVER-REFUSAL, the failure mode
//     that breaks migrations which work today. This is the direction that
//     matters most and the reason the matrix carries the shapes SQLite happily
//     collapses (decimal → TEXT, uuid → TEXT, json → TEXT) alongside the ones
//     it refuses.
//   - preflight accepts / emitter refuses  → a coverage GAP: the refusal is
//     back to arriving from CREATE TABLE with the tables ahead of it created.
func TestPreflightColumnTypesAgreesWithTheTableEmitter(t *testing.T) {
	var refused, accepted int
	for _, c := range typematrix.Cases() {
		schema, tbl := oneColumnSchema(c.Type)
		preflightErr := Engine{}.PreflightColumnTypes(schema)
		_, emitErr := emitTableDef(tbl)

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

	// Anti-vacuity floor in both directions. A preflight that returned nil
	// unconditionally, or a matrix that carried only representable shapes,
	// would pass every assertion above.
	if refused == 0 {
		t.Error("no shape in the matrix was refused; SQLite has the smallest type surface of any " +
			"target (no geometry, inet, bit, interval, array, domain) and must refuse several")
	}
	if accepted == 0 {
		t.Error("no shape in the matrix was accepted; the preflight is refusing everything")
	}
}

// The refusal must name the TABLE and the COLUMN, not just the type. The
// emitter's own error names the IR type because that is all it knows; the
// operator needs to be told where to look, and this surface is the only place
// that has both.
func TestPreflightColumnTypes_NamesTheTableAndColumn(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "shipments",
		Columns: []*ir.Column{{Name: "route", Type: ir.Geometry{Subtype: ir.GeometryPoint}}},
	}}}
	err := Engine{}.PreflightColumnTypes(schema)
	if err == nil {
		t.Fatal("SQLite has no faithful storage for a geometry column and must refuse it")
	}
	for _, want := range []string{"shipments", "route", "Geometry"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q; got: %v", want, err)
		}
	}
}

// A schema whose columns SQLite can all hold must produce no refusal — the
// control for the gate above, at the schema level rather than per shape.
func TestPreflightColumnTypes_AcceptsAnOrdinarySchema(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "total", Type: ir.Decimal{Precision: 10, Scale: 2}},
			{Name: "ref", Type: ir.UUID{}},
			{Name: "payload", Type: ir.JSON{Binary: true}},
			{Name: "placed_at", Type: ir.Timestamp{WithTimeZone: true}},
		},
	}}}
	if err := (Engine{}).PreflightColumnTypes(schema); err != nil {
		t.Fatalf("an ordinary schema must migrate to SQLite; every one of these columns has a "+
			"documented SQLite landing (ADR-0134): %v", err)
	}
}

// Nil-shaped input must not turn a preflight into a panic on a path that
// previously reached the emitter. A nil Type is a corrupt IR, not an
// unrepresentable one, and this surface deliberately has no opinion about it.
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
