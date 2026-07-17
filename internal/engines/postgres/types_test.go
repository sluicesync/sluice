// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// int64Val is a small helper so test cases can write &int64Val(255)
// where an *int64 is needed for columnMeta numeric fields.
func int64Val(v int64) *int64 { return &v }

// TestTranslateType_UnrecognizedUDT_HonestRefusal pins that a genuinely-
// unsupported user-defined type (not an enum, not a catalogued/owned
// extension type, not geometry, not verbatim-eligible — e.g. a composite
// or domain type) refuses with an honest message that names the type and
// says it is unsupported, rather than the old misleading "is not a
// recognised enum" wording (which implied the type should have been an
// enum). The message points at the reliable --exclude-table escape.
func TestTranslateType_UnrecognizedUDT_HonestRefusal(t *testing.T) {
	_, err := translateType(columnMeta{
		DataType: "USER-DEFINED",
		UDTName:  "some_composite_type",
		// no EnumValues, no ExtensionName, not VerbatimEligible, not geometry
	})
	if err == nil {
		t.Fatal("expected a refusal for an unsupported user-defined type")
	}
	msg := err.Error()
	// The old misleading message ENDED with "is not a recognised enum"
	// (implying the type should have been one). The new message may mention
	// "recognised enum" only inside the list of what the type is NOT, so
	// guard against the bare old tail, not the phrase.
	if strings.HasSuffix(msg, "is not a recognised enum") {
		t.Errorf("error still uses the misleading bare-enum wording: %q", msg)
	}
	for _, want := range []string{"some_composite_type", "not supported", "--exclude-table"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message %q missing %q", msg, want)
		}
	}
}

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
		{
			// Bug 69: bare `numeric` — information_schema reports BOTH
			// numeric_precision and numeric_scale as NULL. Must map to
			// the unconstrained IR shape, NOT Decimal{0,0}.
			"numeric (unconstrained)",
			columnMeta{DataType: "numeric", NumPrec: nil, NumScale: nil},
			ir.Decimal{Unconstrained: true},
		},
		{
			// Bug 69: `numeric[]` element is also bare numeric — the
			// array recursion must carry the unconstrained shape.
			"numeric[] (unconstrained element)",
			columnMeta{
				DataType:     "ARRAY",
				UDTName:      "_numeric",
				ArrayElement: &columnMeta{DataType: "numeric", NumPrec: nil, NumScale: nil},
			},
			ir.Array{Element: ir.Decimal{Unconstrained: true}},
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
		// TRIAGE #3 read matrix, pinned per family × shape (the Bug 74
		// discipline): every family member × {bare (atttypmod -1 →
		// PrecisionUnspecified, even though information_schema reports
		// the default 6), explicit 0, explicit mid, explicit 6}.
		// pg_attribute.atttypmod is the declaredness ground truth;
		// datetime_precision alone cannot distinguish bare from an
		// explicit (6).
		{"date", columnMeta{DataType: "date"}, ir.Date{}},
		{"time bare", columnMeta{DataType: "time without time zone", DTPrec: int64Val(6), AttTypmod: -1}, ir.Time{PrecisionUnspecified: true}},
		{"time(0)", columnMeta{DataType: "time without time zone", DTPrec: int64Val(0), AttTypmod: 0}, ir.Time{Precision: 0}},
		{"time(4)", columnMeta{DataType: "time without time zone", DTPrec: int64Val(4), AttTypmod: 4}, ir.Time{Precision: 4}},
		{"time(6)", columnMeta{DataType: "time without time zone", DTPrec: int64Val(6), AttTypmod: 6}, ir.Time{Precision: 6}},
		// Bug 71: timetz must map distinctly from plain time, not
		// collapse to ir.Time{} (OID 1083) and hard-fail the COPY writer.
		{"timetz bare", columnMeta{DataType: "time with time zone", DTPrec: int64Val(6), AttTypmod: -1}, ir.Time{WithTimeZone: true, PrecisionUnspecified: true}},
		{"timetz(0)", columnMeta{DataType: "time with time zone", DTPrec: int64Val(0), AttTypmod: 0}, ir.Time{Precision: 0, WithTimeZone: true}},
		{"timetz(2)", columnMeta{DataType: "time with time zone", DTPrec: int64Val(2), AttTypmod: 2}, ir.Time{Precision: 2, WithTimeZone: true}},
		{"timetz(6)", columnMeta{DataType: "time with time zone", DTPrec: int64Val(6), AttTypmod: 6}, ir.Time{Precision: 6, WithTimeZone: true}},
		{"timestamp bare", columnMeta{DataType: "timestamp without time zone", DTPrec: int64Val(6), AttTypmod: -1}, ir.DateTime{PrecisionUnspecified: true}},
		{"timestamp(0)", columnMeta{DataType: "timestamp without time zone", DTPrec: int64Val(0), AttTypmod: 0}, ir.DateTime{Precision: 0}},
		{"timestamp(3)", columnMeta{DataType: "timestamp without time zone", DTPrec: int64Val(3), AttTypmod: 3}, ir.DateTime{Precision: 3}},
		{"timestamp(6)", columnMeta{DataType: "timestamp without time zone", DTPrec: int64Val(6), AttTypmod: 6}, ir.DateTime{Precision: 6}},
		{"timestamptz bare", columnMeta{DataType: "timestamp with time zone", DTPrec: int64Val(6), AttTypmod: -1}, ir.Timestamp{WithTimeZone: true, PrecisionUnspecified: true}},
		{"timestamptz(0)", columnMeta{DataType: "timestamp with time zone", DTPrec: int64Val(0), AttTypmod: 0}, ir.Timestamp{Precision: 0, WithTimeZone: true}},
		{"timestamptz(3)", columnMeta{DataType: "timestamp with time zone", DTPrec: int64Val(3), AttTypmod: 3}, ir.Timestamp{Precision: 3, WithTimeZone: true}},
		{"timestamptz(6)", columnMeta{DataType: "timestamp with time zone", DTPrec: int64Val(6), AttTypmod: 6}, ir.Timestamp{Precision: 6, WithTimeZone: true}},
		// Array elements: the synthesized element meta carries the array
		// column's atttypmod (a temporal typmod IS the precision) and no
		// datetime_precision — bare arrays stay unspecified, declared
		// precision carries.
		{
			"timestamptz[] bare element",
			columnMeta{
				DataType: "ARRAY", UDTName: "_timestamptz",
				AttTypmod:    -1,
				ArrayElement: &columnMeta{DataType: "timestamp with time zone", UDTName: "timestamptz", AttTypmod: -1},
			},
			ir.Array{Element: ir.Timestamp{WithTimeZone: true, PrecisionUnspecified: true}},
		},
		{
			"timestamptz(3)[] element",
			columnMeta{
				DataType: "ARRAY", UDTName: "_timestamptz",
				AttTypmod:    3,
				ArrayElement: &columnMeta{DataType: "timestamp with time zone", UDTName: "timestamptz", AttTypmod: 3},
			},
			ir.Array{Element: ir.Timestamp{Precision: 3, WithTimeZone: true}},
		},

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

		// ---- Parameterized array elements (Bug 195) ----
		// information_schema reports NULL for EVERY modifier of an ARRAY
		// column, so the element's length/precision comes only from the
		// array column's atttypmod. Pre-fix, only the temporal family got
		// the typmod fallback (above); varchar(n)[]/char(n)[] false-
		// refused as VARCHAR(0)/CHAR(0) and numeric(p,s)[] silently
		// dropped to bare numeric[]. Typmod values are ground-truthed on
		// PG 17: varchar(20)[] → 24, char(5)[] → 9, numeric(10,2)[] →
		// ((10<<16)|2)+4 = 655366; -1 is the bare form.
		{
			"varchar(20)[] element",
			columnMeta{
				DataType: "ARRAY", UDTName: "_varchar",
				AttTypmod:    24,
				ArrayElement: &columnMeta{DataType: "character varying", UDTName: "varchar", AttTypmod: 24},
			},
			ir.Array{Element: ir.Varchar{Length: 20}},
		},
		{
			// Bare varchar[] (typmod -1) is UNBOUNDED — it must land on
			// the unbounded Text collapse (oidToType's convention), NOT
			// VARCHAR(0)'s false refusal.
			"varchar[] bare element → unbounded",
			columnMeta{
				DataType: "ARRAY", UDTName: "_varchar",
				AttTypmod:    -1,
				ArrayElement: &columnMeta{DataType: "character varying", UDTName: "varchar", AttTypmod: -1},
			},
			ir.Array{Element: ir.Text{Size: ir.TextLong}},
		},
		{
			"char(5)[] element",
			columnMeta{
				DataType: "ARRAY", UDTName: "_bpchar",
				AttTypmod:    9,
				ArrayElement: &columnMeta{DataType: "character", UDTName: "bpchar", AttTypmod: 9},
			},
			ir.Array{Element: ir.Char{Length: 5}},
		},
		{
			// Bare char[] element: bpchar with typmod -1 is unbounded in
			// PG (accepts any length), so Char{1} would silently truncate
			// and Char{0} false-refuses — unbounded Text is the faithful
			// carry.
			"char[] bare element → unbounded",
			columnMeta{
				DataType: "ARRAY", UDTName: "_bpchar",
				AttTypmod:    -1,
				ArrayElement: &columnMeta{DataType: "character", UDTName: "bpchar", AttTypmod: -1},
			},
			ir.Array{Element: ir.Text{Size: ir.TextLong}},
		},
		{
			// The silent-loss half of Bug 195: numeric(10,2)[] carried
			// no (p,s) and landed as bare numeric[] on the target.
			"numeric(10,2)[] element",
			columnMeta{
				DataType: "ARRAY", UDTName: "_numeric",
				AttTypmod:    655366,
				ArrayElement: &columnMeta{DataType: "numeric", UDTName: "numeric", AttTypmod: 655366},
			},
			ir.Array{Element: ir.Decimal{Precision: 10, Scale: 2}},
		},
		{
			// Bare numeric[] (typmod -1) stays unconstrained (Bug 69's
			// shape) — the typmod fallback must not manufacture (0,0).
			"numeric[] bare element stays unconstrained",
			columnMeta{
				DataType: "ARRAY", UDTName: "_numeric",
				AttTypmod:    -1,
				ArrayElement: &columnMeta{DataType: "numeric", UDTName: "numeric", AttTypmod: -1},
			},
			ir.Array{Element: ir.Decimal{Unconstrained: true}},
		},

		// ---- Scalar unbounded character forms (the same Bug 195 class
		// at the scalar layer: bare `varchar` / `bpchar` report
		// character_maximum_length NULL and atttypmod -1, and pre-fix
		// false-refused as VARCHAR(0)/CHAR(0) even PG→PG) ----
		{
			"scalar bare varchar → unbounded",
			columnMeta{DataType: "character varying", UDTName: "varchar", AttTypmod: -1},
			ir.Text{Size: ir.TextLong},
		},
		{
			"scalar bare bpchar → unbounded",
			columnMeta{DataType: "character", UDTName: "bpchar", AttTypmod: -1},
			ir.Text{Size: ir.TextLong},
		},
		{
			// Scalar declared lengths keep information_schema as ground
			// truth (regression guard: the typmod fallback must not
			// shadow CharMaxLen).
			"scalar varchar(20) with typmod",
			columnMeta{DataType: "character varying", UDTName: "varchar", CharMaxLen: int64Val(20), AttTypmod: 24},
			ir.Varchar{Length: 20},
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

// TestArrayElement_BitFamilyRefusedLoudly pins the Bug 195 class
// BOUNDARY: bit(n)[] / varbit(n)[] are NOT a supported array element
// family (builtinArrayElement carries no _bit/_varbit), so they refuse
// loudly by name at schema read — they can never reach the typmod
// decode with the char-family +4 layout, which would mis-read bit
// typmods (bit stores the raw length, NO offset — ground-truthed:
// bit(3)[] carries atttypmod=3). Widening bit arrays into a supported
// family is a separate evidence exercise (value-codec + pin matrix),
// not a typmod thread.
func TestArrayElement_BitFamilyRefusedLoudly(t *testing.T) {
	for _, udt := range []string{"_bit", "_varbit"} {
		if got, ok := arrayElementDataType(udt); ok {
			t.Errorf("arrayElementDataType(%q) = (%q, true); want unsupported (loud refusal upstream)", udt, got)
		}
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

// Bug 17: tsvector / tsquery are PG CORE types. Cross-engine they
// stay a loud refusal (no MySQL equivalent), but a same-engine PG → PG
// run (VerbatimEligible) must carry them verbatim instead of refusing.
func TestTranslateType_TsvectorVerbatimSameEngine(t *testing.T) {
	for _, dt := range []string{"tsvector", "tsquery"} {
		got, err := translateType(columnMeta{
			DataType:         dt,
			UDTName:          dt,
			FormatType:       dt,
			VerbatimEligible: true,
		})
		if err != nil {
			t.Fatalf("%s verbatim: unexpected error: %v", dt, err)
		}
		want := ir.VerbatimType{Definition: dt}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %#v want %#v", dt, got, want)
		}
	}
	// Cross-engine (not eligible) still refuses — no silent loss.
	if _, err := translateType(columnMeta{DataType: "tsvector", UDTName: "tsvector"}); err == nil {
		t.Error("tsvector cross-engine: expected loud refusal, got nil")
	}
}

// Bug 19c: a PG enum's source type name rides onto ir.Enum.TypeName.
func TestTranslateType_EnumCarriesTypeName(t *testing.T) {
	got, err := translateType(columnMeta{
		DataType:     "USER-DEFINED",
		UDTName:      "post_status",
		EnumValues:   []string{"draft", "published"},
		EnumTypeName: "post_status",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := ir.Enum{Values: []string{"draft", "published"}, TypeName: "post_status"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v want %#v", got, want)
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
