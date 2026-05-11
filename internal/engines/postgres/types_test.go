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

// TestTranslateTypeGeography covers the PostGIS geography path
// (parallel to TestTranslateTypeGeometry). The IsGeography flag on
// the info struct propagates through to [ir.Geometry.IsGeography] so
// the PG writer emits `geography(...)` rather than `geometry(...)`.
// Without info (PostGIS schema-reader saw `geography` udt_name but
// the lookup returned nothing), the translator falls back to
// GeometryUnspecified+IsGeography=true based on the udt_name alone.
func TestTranslateTypeGeography(t *testing.T) {
	cases := []struct {
		name string
		info *geometryColumnInfo
		want ir.Geometry
	}{
		{
			"point with default srid 4326",
			&geometryColumnInfo{Subtype: "POINT", SRID: 4326, IsGeography: true},
			ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326, IsGeography: true},
		},
		{
			"polygon",
			&geometryColumnInfo{Subtype: "POLYGON", SRID: 4326, IsGeography: true},
			ir.Geometry{Subtype: ir.GeometryPolygon, SRID: 4326, IsGeography: true},
		},
		{
			"unspecified geography",
			&geometryColumnInfo{Subtype: "GEOMETRY", SRID: 4326, IsGeography: true},
			ir.Geometry{Subtype: ir.GeometryUnspecified, SRID: 4326, IsGeography: true},
		},
		{
			"no info (udt_name geography but lookup empty) keeps IsGeography",
			nil,
			ir.Geometry{Subtype: ir.GeometryUnspecified, IsGeography: true},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := translateType(columnMeta{
				DataType:     "USER-DEFINED",
				UDTName:      "geography",
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
// string to the IR enum + (HasZ, HasM) pair. Unknown strings (and
// the empty string) return GeometryUnspecified — the wildcard.
//
// Three input shapes covered:
//
//   - ALL-CAPS (geometry_columns.type — the pre-Bug-51 path).
//   - Mixed-case (geography_columns.type — Bug 51 fix exercises
//     ToUpper before dispatch).
//   - Z / M / ZM dimensional suffixes (Bug 52 — POINTZ, LINESTRINGM,
//     MULTIPOLYGONZM, etc.). The suffix strips before subtype
//     dispatch and surfaces as HasZ / HasM on the return tuple.
func TestParseGeometrySubtype(t *testing.T) {
	cases := []struct {
		in       string
		want     ir.GeometrySubtype
		wantHasZ bool
		wantHasM bool
	}{
		{"POINT", ir.GeometryPoint, false, false},
		{"LINESTRING", ir.GeometryLineString, false, false},
		{"POLYGON", ir.GeometryPolygon, false, false},
		{"MULTIPOINT", ir.GeometryMultiPoint, false, false},
		{"MULTILINESTRING", ir.GeometryMultiLineString, false, false},
		{"MULTIPOLYGON", ir.GeometryMultiPolygon, false, false},
		{"GEOMETRYCOLLECTION", ir.GeometryCollection, false, false},
		{"GEOMETRY", ir.GeometryUnspecified, false, false},
		{"", ir.GeometryUnspecified, false, false},
		{"TIN", ir.GeometryUnspecified, false, false}, // unknown → wildcard

		// Bug 51 — geography_columns.type uses mixed case. Pre-fix the
		// switch fell through to GeometryUnspecified silently.
		{"Point", ir.GeometryPoint, false, false},
		{"Polygon", ir.GeometryPolygon, false, false},
		{"LineString", ir.GeometryLineString, false, false},
		{"MultiPolygon", ir.GeometryMultiPolygon, false, false},
		{"point", ir.GeometryPoint, false, false}, // defensive: all-lower also works

		// Bug 52 — Z / M / ZM dimensional variants.
		{"POINTZ", ir.GeometryPoint, true, false},
		{"POINTM", ir.GeometryPoint, false, true},
		{"POINTZM", ir.GeometryPoint, true, true},
		{"LINESTRINGZ", ir.GeometryLineString, true, false},
		{"POLYGONM", ir.GeometryPolygon, false, true},
		{"MULTIPOINTZ", ir.GeometryMultiPoint, true, false},
		{"MULTIPOLYGONZM", ir.GeometryMultiPolygon, true, true},
		{"GEOMETRYCOLLECTIONZ", ir.GeometryCollection, true, false},

		// Mixed-case + dimensional suffix (geography_columns shape).
		{"PointZ", ir.GeometryPoint, true, false},
		{"PolygonZM", ir.GeometryPolygon, true, true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.in, func(t *testing.T) {
			gotSubtype, gotHasZ, gotHasM := parseGeometrySubtype(c.in)
			if gotSubtype != c.want {
				t.Errorf("subtype = %v; want %v", gotSubtype, c.want)
			}
			if gotHasZ != c.wantHasZ {
				t.Errorf("hasZ = %v; want %v", gotHasZ, c.wantHasZ)
			}
			if gotHasM != c.wantHasM {
				t.Errorf("hasM = %v; want %v", gotHasM, c.wantHasM)
			}
		})
	}
}

// TestDimensionFlagsFromCoordDim pins the PostGIS two-channel
// dimension-encoding mapping (Bug 53). The function combines the
// `type` column's optional Z / M / ZM suffix with the
// `coord_dimension` column to recover the orthogonal Z / M flags
// PostGIS stores in different places for different cases.
func TestDimensionFlagsFromCoordDim(t *testing.T) {
	cases := []struct {
		name     string
		typeName string
		coordDim int
		wantZ    bool
		wantM    bool
	}{
		// 2D — coord_dimension=2 means no flags regardless of type.
		{"POINT 2D", "POINT", 2, false, false},
		{"POLYGON 2D", "POLYGON", 2, false, false},

		// 3D — coord_dimension=3 disambiguates by type suffix.
		// "POINT" + cd=3 → XYZ (Z only). "POINTM" + cd=3 → XYM (M only).
		{"POINT cd=3 → POINTZ", "POINT", 3, true, false},
		{"POLYGON cd=3 → POLYGONZ", "POLYGON", 3, true, false},
		{"POINTM cd=3 → M-only", "POINTM", 3, false, true},
		{"LINESTRINGM cd=3 → M-only", "LINESTRINGM", 3, false, true},

		// 4D — coord_dimension=4 unambiguously XYZM.
		{"POINT cd=4 → POINTZM", "POINT", 4, true, true},
		{"POLYGONZM cd=4 → POINTZM", "POLYGONZM", 4, true, true},

		// Mixed-case (geography_columns shape) — the helper
		// upper-cases internally so dispatch survives PostGIS's
		// inconsistent casing across views.
		{"Point cd=3 → Z (mixed-case)", "Point", 3, true, false},
		{"LineStringM cd=3 → M (mixed-case)", "LineStringM", 3, false, true},

		// Unknown coord_dimension → no flags (degrade gracefully).
		{"unknown cd=1", "POINT", 1, false, false},
		{"unknown cd=5", "POINT", 5, false, false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			gotZ, gotM := dimensionFlagsFromCoordDim(c.typeName, c.coordDim)
			if gotZ != c.wantZ {
				t.Errorf("hasZ = %v; want %v", gotZ, c.wantZ)
			}
			if gotM != c.wantM {
				t.Errorf("hasM = %v; want %v", gotM, c.wantM)
			}
		})
	}
}

// TestTranslateType_GeometryCoordDimensionMerge pins the OR-merge in
// translateType: the schema reader's coord_dimension-derived flags
// on `c.GeometryInfo.HasZ` / `.HasM` combine with the type-string
// parsing inside parseGeometrySubtype. Either source alone may be
// load-bearing; the merged result is what the IR carries.
func TestTranslateType_GeometryCoordDimensionMerge(t *testing.T) {
	cases := []struct {
		name string
		info *geometryColumnInfo
		want ir.Geometry
	}{
		{
			// Canonical POINTZ: type='POINT', reader sets HasZ from cd=3.
			"POINTZ via coord_dimension",
			&geometryColumnInfo{Subtype: "POINT", SRID: 4326, HasZ: true},
			ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326, HasZ: true},
		},
		{
			// LINESTRINGM: type='LINESTRINGM', reader sets HasM from cd=3.
			// Both channels carry the M signal; OR-merge preserves it.
			"LINESTRINGM via both channels",
			&geometryColumnInfo{Subtype: "LINESTRINGM", SRID: 0, HasM: true},
			ir.Geometry{Subtype: ir.GeometryLineString, SRID: 0, HasM: true},
		},
		{
			// POLYGONZM via coord_dimension=4: type='POLYGON', reader
			// sets both flags. parseGeometrySubtype contributes nothing.
			"POLYGONZM via coord_dimension only",
			&geometryColumnInfo{Subtype: "POLYGON", SRID: 4326, HasZ: true, HasM: true},
			ir.Geometry{Subtype: ir.GeometryPolygon, SRID: 4326, HasZ: true, HasM: true},
		},
		{
			// Defensive: pre-Bug-53 IR with type='POINTZ' and reader
			// flags empty. parseGeometrySubtype still recovers the Z
			// from the type-string suffix.
			"POINTZ via type-string fallback (pre-Bug-53)",
			&geometryColumnInfo{Subtype: "POINTZ", SRID: 4326},
			ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326, HasZ: true},
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
