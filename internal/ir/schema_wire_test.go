// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"encoding/json"
	"testing"
)

// Type round-trip exhaustively covers every concrete IR type so a
// future addition to the IR catches at the test boundary if the
// MarshalType/UnmarshalType branches drift apart.
func TestMarshalType_RoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   Type
	}{
		{"Boolean", Boolean{}},
		{"Integer 64 signed autoinc", Integer{Width: 64, AutoIncrement: true}},
		{"Integer 32 unsigned", Integer{Width: 32, Unsigned: true}},
		{"Decimal", Decimal{Precision: 19, Scale: 4}},
		{"Float single", Float{Precision: FloatSingle}},
		{"Float double", Float{Precision: FloatDouble}},
		{"Char", Char{Length: 36, Charset: "utf8mb4", Collation: "utf8mb4_general_ci"}},
		{"Varchar", Varchar{Length: 255}},
		{"Text long", Text{Size: TextLong}},
		{"Binary", Binary{Length: 16}},
		{"Varbinary", Varbinary{Length: 64}},
		{"Blob medium", Blob{Size: BlobMedium}},
		{"Date", Date{}},
		{"Interval", Interval{}},
		{"Time precision 6", Time{Precision: 6}},
		{"DateTime precision 3", DateTime{Precision: 3}},
		{"Timestamp tz", Timestamp{Precision: 6, WithTimeZone: true}},
		{"JSON binary", JSON{Binary: true}},
		{"JSON text", JSON{Binary: false}},
		{"Enum", Enum{Values: []string{"a", "b", "c"}}},
		{"Set", Set{Values: []string{"r", "w", "x"}}},
		{"UUID", UUID{}},
		{"Inet", Inet{}},
		{"Cidr", Cidr{}},
		{"Macaddr", Macaddr{}},
		{"Geometry point SRID", Geometry{Subtype: GeometryPoint, SRID: 4326}},
		{"Geography point SRID 4326", Geometry{Subtype: GeometryPoint, SRID: 4326, IsGeography: true}},
		{"Geography polygon", Geometry{Subtype: GeometryPolygon, SRID: 4326, IsGeography: true}},
		{"Geometry POINTZ", Geometry{Subtype: GeometryPoint, SRID: 4326, HasZ: true}},
		{"Geometry POINTZM", Geometry{Subtype: GeometryPoint, SRID: 4326, HasZ: true, HasM: true}},
		{"Geography POLYGONZM", Geometry{Subtype: GeometryPolygon, SRID: 4326, IsGeography: true, HasZ: true, HasM: true}},
		{"Array of Integer", Array{Element: Integer{Width: 32}}},
		{"Array of UUID", Array{Element: UUID{}}},
		{"Array of nil element", Array{Element: nil}},
		// ADR-0047 verbatim (uncatalogued) PG extension type.
		{"VerbatimType ltree", VerbatimType{Definition: "ltree"}},
		{"VerbatimType cube", VerbatimType{Definition: "cube"}},
		{"VerbatimType schema-qualified", VerbatimType{Definition: "public.mytype"}},
		{"VerbatimType with modifier spelling", VerbatimType{Definition: "geometry(Point,4326)"}},
		// ADR-0049 Chunk B/C prerequisite: Bit (catalog Bug 62/77) +
		// ADR-0032 catalogued ExtensionType. Pin the class — fixed vs
		// varying bit; ext with and without modifiers.
		{"Bit fixed", Bit{Length: 8}},
		{"Bit varying", Bit{Length: 16, Varying: true}},
		{"ExtensionType no mods", ExtensionType{Extension: "uuid-ossp", Name: "uuid"}},
		{"ExtensionType vector with mods", ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{1536}}},
		{"ExtensionType postgis multi-mod", ExtensionType{Extension: "postgis", Name: "geometry", Modifiers: []int{4326, 2}}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			b, err := MarshalType(c.in)
			if err != nil {
				t.Fatalf("MarshalType(%v): %v", c.in, err)
			}
			out, err := UnmarshalType(b)
			if err != nil {
				t.Fatalf("UnmarshalType(%s): %v", b, err)
			}
			if got, want := out.String(), c.in.String(); got != want {
				t.Errorf("round-trip String() = %q; want %q (json=%s)", got, want, b)
			}
		})
	}
}

func TestUnmarshalType_NullAndUnknownKind(t *testing.T) {
	got, err := UnmarshalType([]byte("null"))
	if err != nil {
		t.Fatalf("null: %v", err)
	}
	if got != nil {
		t.Errorf("null type = %v; want nil", got)
	}

	got, err = UnmarshalType([]byte(`{"kind":"WatNotReal"}`))
	if err == nil {
		t.Fatalf("expected error on unknown kind; got %v", got)
	}
}

func TestMarshalDefault_RoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   DefaultValue
	}{
		{"None", DefaultNone{}},
		{"Literal", DefaultLiteral{Value: "0"}},
		{"Expression", DefaultExpression{Expr: "CURRENT_TIMESTAMP", Dialect: "postgres"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			b, err := MarshalDefault(c.in)
			if err != nil {
				t.Fatalf("MarshalDefault: %v", err)
			}
			out, err := UnmarshalDefault(b)
			if err != nil {
				t.Fatalf("UnmarshalDefault: %v", err)
			}
			// Compare via stringification: each variant has a distinct
			// shape. This doubles as an interface-implementation check.
			if got, want := defaultDescribe(out), defaultDescribe(c.in); got != want {
				t.Errorf("round-trip = %s; want %s (json=%s)", got, want, b)
			}
		})
	}
}

func defaultDescribe(d DefaultValue) string {
	switch v := d.(type) {
	case DefaultNone:
		return "None"
	case DefaultLiteral:
		return "Literal:" + v.Value
	case DefaultExpression:
		return "Expr:" + v.Expr + "/" + v.Dialect
	}
	return "?"
}

// Schema round-trip via Column's custom MarshalJSON: the serialised
// JSON must decode back to a Column whose Type / Default match. This
// is the load-bearing path the manifest writer + restore reader rely
// on; a regression here means cross-engine restore can't survive a
// round-trip through the manifest.
func TestColumnJSON_RoundTrip(t *testing.T) {
	original := &Column{
		Name:     "id",
		Type:     Integer{Width: 64, AutoIncrement: true},
		Nullable: false,
		Default:  DefaultExpression{Expr: "nextval('seq')", Dialect: "postgres"},
		Comment:  "primary key",
	}
	b, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got Column
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Name != original.Name {
		t.Errorf("Name: got %q want %q", got.Name, original.Name)
	}
	if got.Type == nil || got.Type.String() != original.Type.String() {
		t.Errorf("Type: got %v want %v", got.Type, original.Type)
	}
	if got.Default == nil {
		t.Fatal("Default is nil")
	}
	if d, ok := got.Default.(DefaultExpression); !ok {
		t.Errorf("Default not DefaultExpression: got %T", got.Default)
	} else if d.Expr != "nextval('seq')" {
		t.Errorf("Default.Expr: got %q", d.Expr)
	}
	if got.Comment != original.Comment {
		t.Errorf("Comment: got %q want %q", got.Comment, original.Comment)
	}
}

// A Column with no default decodes to DefaultNone — both for absent-
// field (manifest emitted with omitempty) and for the explicit None
// envelope.
func TestColumnJSON_NoDefault(t *testing.T) {
	col := &Column{Name: "name", Type: Varchar{Length: 100}}
	b, err := json.Marshal(col)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got Column
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if _, ok := got.Default.(DefaultNone); !ok {
		t.Errorf("Default = %T; want DefaultNone", got.Default)
	}
}

// A nested Array(Element=Array(Element=Varchar)) ensures recursive
// type encoding works — multi-dimensional PG arrays are real.
func TestMarshalType_NestedArray(t *testing.T) {
	in := Array{Element: Array{Element: Varchar{Length: 10}}}
	b, err := MarshalType(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	out, err := UnmarshalType(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out.String() != in.String() {
		t.Errorf("got %v want %v", out, in)
	}
}
