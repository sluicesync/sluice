// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRecoverMariaDBJSONColumns pins item 73 Phase 2 item 1: a longtext
// column whose ONLY CHECK is exactly json_valid(<that column>) is
// remapped to ir.JSON{Binary:false} AND that auto-CHECK is stripped from
// the IR — while a longtext without it, and a longtext carrying a
// non-auto json_valid-containing CHECK, are BOTH left untouched.
func TestRecoverMariaDBJSONColumns(t *testing.T) {
	longtext := func() ir.Type { return ir.Text{Size: ir.TextLong, Charset: "utf8mb4"} }

	cases := []struct {
		name       string
		col        *ir.Column
		checks     []*ir.CheckConstraint
		wantType   ir.Type
		wantChecks []*ir.CheckConstraint
	}{
		{
			name:       "longtext + auto json_valid → JSON, check stripped",
			col:        &ir.Column{Name: "js", Type: longtext()},
			checks:     []*ir.CheckConstraint{{Name: "js", Expr: "json_valid(js)", ExprDialect: dialectName}},
			wantType:   ir.JSON{Binary: false},
			wantChecks: nil,
		},
		{
			name:       "case-insensitive function name still detected",
			col:        &ir.Column{Name: "js", Type: longtext()},
			checks:     []*ir.CheckConstraint{{Name: "js", Expr: "JSON_VALID(js)"}},
			wantType:   ir.JSON{Binary: false},
			wantChecks: nil,
		},
		{
			name:       "longtext WITHOUT json_valid stays Text, check kept",
			col:        &ir.Column{Name: "blurb", Type: longtext()},
			checks:     []*ir.CheckConstraint{{Name: "blurb_len", Expr: "length(blurb) > 0"}},
			wantType:   longtext(),
			wantChecks: []*ir.CheckConstraint{{Name: "blurb_len", Expr: "length(blurb) > 0"}},
		},
		{
			name: "longtext with a COMPLEX json_valid CHECK is NOT mis-detected",
			col:  &ir.Column{Name: "doc", Type: longtext()},
			// A real user CHECK that references json_valid but is more than a
			// bare json_valid(col) — must stay Text and keep the CHECK.
			checks:     []*ir.CheckConstraint{{Name: "doc_ck", Expr: "json_valid(doc) and length(doc) > 2"}},
			wantType:   longtext(),
			wantChecks: []*ir.CheckConstraint{{Name: "doc_ck", Expr: "json_valid(doc) and length(doc) > 2"}},
		},
		{
			name: "json_valid referencing a DIFFERENT column is not the auto-marker",
			col:  &ir.Column{Name: "a", Type: longtext()},
			// The CHECK targets `b`, not `a`; `a` stays Text and the CHECK is kept.
			checks:     []*ir.CheckConstraint{{Name: "ck", Expr: "json_valid(b)"}},
			wantType:   longtext(),
			wantChecks: []*ir.CheckConstraint{{Name: "ck", Expr: "json_valid(b)"}},
		},
		{
			name:       "json_valid on a non-longtext column is not remapped",
			col:        &ir.Column{Name: "vc", Type: ir.Varchar{Length: 64}},
			checks:     []*ir.CheckConstraint{{Name: "vc", Expr: "json_valid(vc)"}},
			wantType:   ir.Varchar{Length: 64},
			wantChecks: []*ir.CheckConstraint{{Name: "vc", Expr: "json_valid(vc)"}},
		},
		{
			name:       "residual backtick pair tolerated",
			col:        &ir.Column{Name: "js", Type: longtext()},
			checks:     []*ir.CheckConstraint{{Name: "js", Expr: "json_valid(`js`)"}},
			wantType:   ir.JSON{Binary: false},
			wantChecks: nil,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			tables := map[string]*ir.Table{
				"t": {Name: "t", Columns: []*ir.Column{c.col}, CheckConstraints: c.checks},
			}
			recoverMariaDBJSONColumns(tables)
			if !reflect.DeepEqual(c.col.Type, c.wantType) {
				t.Errorf("column type = %#v; want %#v", c.col.Type, c.wantType)
			}
			if !reflect.DeepEqual(tables["t"].CheckConstraints, c.wantChecks) {
				t.Errorf("checks = %#v; want %#v", tables["t"].CheckConstraints, c.wantChecks)
			}
		})
	}
}

// TestRecoverMariaDBJSONColumns_MixedChecksPreserveOrder pins that when a
// table has BOTH the auto json_valid CHECK and a genuine user CHECK, only
// the auto one is stripped and the user CHECK survives in order.
func TestRecoverMariaDBJSONColumns_MixedChecksPreserveOrder(t *testing.T) {
	js := &ir.Column{Name: "js", Type: ir.Text{Size: ir.TextLong}}
	qty := &ir.Column{Name: "qty", Type: ir.Integer{Width: 32}}
	tables := map[string]*ir.Table{
		"t": {
			Name:    "t",
			Columns: []*ir.Column{js, qty},
			CheckConstraints: []*ir.CheckConstraint{
				{Name: "qty_ck", Expr: "qty >= 0"},
				{Name: "js", Expr: "json_valid(js)"},
			},
		},
	}
	recoverMariaDBJSONColumns(tables)
	if _, ok := js.Type.(ir.JSON); !ok {
		t.Errorf("js type = %#v; want ir.JSON", js.Type)
	}
	got := tables["t"].CheckConstraints
	want := []*ir.CheckConstraint{{Name: "qty_ck", Expr: "qty >= 0"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("checks = %#v; want %#v (only the auto json_valid check stripped)", got, want)
	}
}

// TestEmitColumnDef_MariaDBGeometrySRID pins the item-73 Phase-2 item-4
// EMIT half (F3): a mariadb-flavor emitter renders the geometry SRID as
// the `REF_SYSTEM_ID=<n>` TYPE attribute (BEFORE NOT NULL — MariaDB
// rejects it after, and rejects MySQL 8's `SRID <n>` outright), while a
// vanilla emitter keeps the MySQL-8 `SRID <n>` form after NOT NULL.
// Pins the geometry FAMILY (POINT / LINESTRING / POLYGON) × {SRID 0,
// non-zero} so the class — not one representative — is covered.
func TestEmitColumnDef_MariaDBGeometrySRID(t *testing.T) {
	mariadb := newMySQLEmitterForFlavor(nil, FlavorMariaDB)
	vanilla := newMySQLEmitterForFlavor(nil, FlavorVanilla)

	cases := []struct {
		name        string
		col         *ir.Column
		wantMariaDB string
		wantVanilla string
	}{
		{
			name:        "point srid 4326 not null",
			col:         &ir.Column{Name: "loc", Type: ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326}},
			wantMariaDB: "`loc` POINT REF_SYSTEM_ID=4326 NOT NULL",
			wantVanilla: "`loc` POINT NOT NULL SRID 4326",
		},
		{
			name:        "linestring srid 3857 nullable",
			col:         &ir.Column{Name: "path", Type: ir.Geometry{Subtype: ir.GeometryLineString, SRID: 3857}, Nullable: true},
			wantMariaDB: "`path` LINESTRING REF_SYSTEM_ID=3857",
			wantVanilla: "`path` LINESTRING SRID 3857",
		},
		{
			name:        "polygon srid 0 — no reference attribute either flavor",
			col:         &ir.Column{Name: "boundary", Type: ir.Geometry{Subtype: ir.GeometryPolygon, SRID: 0}},
			wantMariaDB: "`boundary` POLYGON NOT NULL",
			wantVanilla: "`boundary` POLYGON NOT NULL",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			gotM, err := mariadb.emitColumnDef("t", c.col)
			if err != nil {
				t.Fatalf("mariadb emit: %v", err)
			}
			if gotM != c.wantMariaDB {
				t.Errorf("mariadb:\n got  %q\n want %q", gotM, c.wantMariaDB)
			}
			gotV, err := vanilla.emitColumnDef("t", c.col)
			if err != nil {
				t.Fatalf("vanilla emit: %v", err)
			}
			if gotV != c.wantVanilla {
				t.Errorf("vanilla (parity anchor):\n got  %q\n want %q", gotV, c.wantVanilla)
			}
		})
	}
}

// TestApplyMariaDBGeometrySRID pins the mariadb geometry SRID backfill
// (item 73 Phase 2 item 4): SRIDs read from
// information_schema.GEOMETRY_COLUMNS are applied to the matching
// geometry columns (preserving subtype), a column with no catalog entry
// keeps SRID 0, and non-geometry columns are untouched. Pins the
// geometry FAMILY (POINT / LINESTRING / POLYGON) × {SRID 0, SRID 4326}.
func TestApplyMariaDBGeometrySRID(t *testing.T) {
	pt0 := &ir.Column{Name: "p0", Type: ir.Geometry{Subtype: ir.GeometryPoint}}
	pt4326 := &ir.Column{Name: "p4326", Type: ir.Geometry{Subtype: ir.GeometryPoint}}
	ls4326 := &ir.Column{Name: "ls", Type: ir.Geometry{Subtype: ir.GeometryLineString}}
	poly4326 := &ir.Column{Name: "poly", Type: ir.Geometry{Subtype: ir.GeometryPolygon}}
	notGeom := &ir.Column{Name: "id", Type: ir.Integer{Width: 32}}

	tables := map[string]*ir.Table{
		"geo": {Name: "geo", Columns: []*ir.Column{notGeom, pt0, pt4326, ls4326, poly4326}},
	}
	srids := map[geometryColumnKey]int{
		// p0 deliberately absent → keeps SRID 0.
		{table: "geo", column: "p4326"}: 4326,
		{table: "geo", column: "ls"}:    4326,
		{table: "geo", column: "poly"}:  4326,
	}
	applyMariaDBGeometrySRID(tables, srids)

	want := map[string]ir.Geometry{
		"p0":    {Subtype: ir.GeometryPoint, SRID: 0},
		"p4326": {Subtype: ir.GeometryPoint, SRID: 4326},
		"ls":    {Subtype: ir.GeometryLineString, SRID: 4326},
		"poly":  {Subtype: ir.GeometryPolygon, SRID: 4326},
	}
	for _, col := range tables["geo"].Columns {
		w, tracked := want[col.Name]
		if !tracked {
			continue
		}
		got, ok := col.Type.(ir.Geometry)
		if !ok {
			t.Errorf("column %q type = %#v; want ir.Geometry", col.Name, col.Type)
			continue
		}
		if got != w {
			t.Errorf("column %q = %#v; want %#v", col.Name, got, w)
		}
	}
	if _, ok := notGeom.Type.(ir.Integer); !ok {
		t.Errorf("non-geometry column mutated: %#v", notGeom.Type)
	}
}
