// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"reflect"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// int64Val is a small helper so test cases can write &int64Val(255)
// where an *int64 is needed for columnMeta numeric fields.
func int64Val(v int64) *int64 { return &v }

func TestTranslateType(t *testing.T) {
	cases := []struct {
		name string
		in   columnMeta
		want ir.Type
	}{
		// ---- Booleans ----
		{"boolean", columnMeta{DataType: "boolean"}, ir.Boolean{}},

		// ---- Integers ----
		{"smallint", columnMeta{DataType: "smallint"}, ir.Integer{Width: 16}},
		{"integer", columnMeta{DataType: "integer"}, ir.Integer{Width: 32}},
		{"bigint", columnMeta{DataType: "bigint"}, ir.Integer{Width: 64}},
		{"serial → integer auto", columnMeta{DataType: "integer", IsAutoIncrement: true}, ir.Integer{Width: 32, AutoIncrement: true}},
		{"bigserial → bigint auto", columnMeta{DataType: "bigint", IsAutoIncrement: true}, ir.Integer{Width: 64, AutoIncrement: true}},

		// ---- Decimal / float ----
		{
			"numeric(18,4)",
			columnMeta{DataType: "numeric", NumPrec: int64Val(18), NumScale: int64Val(4)},
			ir.Decimal{Precision: 18, Scale: 4},
		},
		{"real", columnMeta{DataType: "real"}, ir.Float{Precision: ir.FloatSingle}},
		{"double precision", columnMeta{DataType: "double precision"}, ir.Float{Precision: ir.FloatDouble}},

		// ---- Character ----
		{"char(10)", columnMeta{DataType: "character", CharMaxLen: int64Val(10)}, ir.Char{Length: 10}},
		{"varchar(255)", columnMeta{DataType: "character varying", CharMaxLen: int64Val(255)}, ir.Varchar{Length: 255}},
		{"text", columnMeta{DataType: "text"}, ir.Text{Size: ir.TextLong}},

		// ---- Binary ----
		{"bytea", columnMeta{DataType: "bytea"}, ir.Blob{Size: ir.BlobLong}},

		// ---- Temporal ----
		{"date", columnMeta{DataType: "date"}, ir.Date{}},
		{"time", columnMeta{DataType: "time without time zone", DTPrec: int64Val(6)}, ir.Time{Precision: 6}},
		{"timestamp", columnMeta{DataType: "timestamp without time zone", DTPrec: int64Val(3)}, ir.DateTime{Precision: 3}},
		{"timestamptz", columnMeta{DataType: "timestamp with time zone", DTPrec: int64Val(6)}, ir.Timestamp{Precision: 6, WithTimeZone: true}},

		// ---- Structured ----
		{"json", columnMeta{DataType: "json"}, ir.JSON{Binary: false}},
		{"jsonb", columnMeta{DataType: "jsonb"}, ir.JSON{Binary: true}},

		// ---- Identity / network ----
		{"uuid", columnMeta{DataType: "uuid"}, ir.UUID{}},
		{"inet", columnMeta{DataType: "inet"}, ir.Inet{}},
		{"cidr", columnMeta{DataType: "cidr"}, ir.Cidr{}},
		{"macaddr", columnMeta{DataType: "macaddr"}, ir.Macaddr{}},
		{"macaddr8", columnMeta{DataType: "macaddr8"}, ir.Macaddr{}},

		// ---- Enum ----
		{
			"enum with values",
			columnMeta{
				DataType:   "USER-DEFINED",
				UDTName:    "user_role",
				EnumValues: []string{"admin", "user", "guest"},
			},
			ir.Enum{Values: []string{"admin", "user", "guest"}},
		},

		// ---- Array ----
		{
			"int array",
			columnMeta{
				DataType:     "ARRAY",
				UDTName:      "_int4",
				ArrayElement: &columnMeta{DataType: "integer"},
			},
			ir.Array{Element: ir.Integer{Width: 32}},
		},
		{
			"text array",
			columnMeta{
				DataType:     "ARRAY",
				UDTName:      "_text",
				ArrayElement: &columnMeta{DataType: "text"},
			},
			ir.Array{Element: ir.Text{Size: ir.TextLong}},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := translateType(c.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("translateType(%+v)\n got = %#v\nwant = %#v", c.in, got, c.want)
			}
		})
	}
}

// TestTranslateTypeGeometry covers the PostGIS geometry path
// through translateType. With GeometryInfo present, the
// translator returns ir.Geometry with the precise Subtype and
// SRID; without it (PostGIS not installed, view didn't know the
// column), it falls back to GeometryUnspecified+SRID=0.
func TestTranslateTypeGeometry(t *testing.T) {
	cases := []struct {
		name string
		info *geometryColumnInfo
		want ir.Geometry
	}{
		{
			"point with srid", &geometryColumnInfo{Subtype: "POINT", SRID: 4326},
			ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326},
		},
		{
			"polygon zero srid", &geometryColumnInfo{Subtype: "POLYGON", SRID: 0},
			ir.Geometry{Subtype: ir.GeometryPolygon, SRID: 0},
		},
		{
			"linestring", &geometryColumnInfo{Subtype: "LINESTRING", SRID: 3857},
			ir.Geometry{Subtype: ir.GeometryLineString, SRID: 3857},
		},
		{
			"multipolygon", &geometryColumnInfo{Subtype: "MULTIPOLYGON", SRID: 4326},
			ir.Geometry{Subtype: ir.GeometryMultiPolygon, SRID: 4326},
		},
		{
			"generic geometry wildcard", &geometryColumnInfo{Subtype: "GEOMETRY", SRID: 0},
			ir.Geometry{Subtype: ir.GeometryUnspecified, SRID: 0},
		},
		{
			"unknown subtype falls back", &geometryColumnInfo{Subtype: "TIN", SRID: 4326},
			ir.Geometry{Subtype: ir.GeometryUnspecified, SRID: 4326},
		},
		{
			"no info (postgis not installed)", nil,
			ir.Geometry{Subtype: ir.GeometryUnspecified},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := translateType(columnMeta{
				DataType:     "USER-DEFINED",
				UDTName:      "geometry",
				GeometryInfo: c.info,
			})
			if err != nil {
				t.Fatalf("translateType: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("\n got:  %#v\nwant: %#v", got, c.want)
			}
		})
	}
}

// TestParseGeometrySubtype maps every PostGIS-canonical subtype
// string to the IR enum. Unknown strings (and the empty string)
// return GeometryUnspecified — the wildcard.
func TestParseGeometrySubtype(t *testing.T) {
	cases := []struct {
		in   string
		want ir.GeometrySubtype
	}{
		{"POINT", ir.GeometryPoint},
		{"LINESTRING", ir.GeometryLineString},
		{"POLYGON", ir.GeometryPolygon},
		{"MULTIPOINT", ir.GeometryMultiPoint},
		{"MULTILINESTRING", ir.GeometryMultiLineString},
		{"MULTIPOLYGON", ir.GeometryMultiPolygon},
		{"GEOMETRYCOLLECTION", ir.GeometryCollection},
		{"GEOMETRY", ir.GeometryUnspecified},
		{"", ir.GeometryUnspecified},
		{"TIN", ir.GeometryUnspecified}, // unknown → wildcard
	}
	for _, c := range cases {
		c := c
		t.Run(c.in, func(t *testing.T) {
			if got := parseGeometrySubtype(c.in); got != c.want {
				t.Errorf("got %v; want %v", got, c.want)
			}
		})
	}
}

func TestTranslateTypeErrors(t *testing.T) {
	cases := []struct {
		name string
		in   columnMeta
	}{
		{"unsupported data_type", columnMeta{DataType: "tsvector"}},
		{"array without element", columnMeta{DataType: "ARRAY", UDTName: "_int4"}},
		{"user-defined without enum values", columnMeta{DataType: "USER-DEFINED", UDTName: "some_composite"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if _, err := translateType(c.in); err == nil {
				t.Errorf("expected error for %s; got nil", c.name)
			}
		})
	}
}
