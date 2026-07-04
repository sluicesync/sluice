// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"encoding/json"
	"reflect"
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
		// TRIAGE #3: the temporal precision-unspecified state (bare PG
		// form) round-trips per family — the flag is append-only wire,
		// mirroring DecimalUnconstrained.
		{"Time unspecified", Time{PrecisionUnspecified: true}},
		{"TimeTZ unspecified", Time{WithTimeZone: true, PrecisionUnspecified: true}},
		{"DateTime unspecified", DateTime{PrecisionUnspecified: true}},
		{"Timestamp tz unspecified", Timestamp{WithTimeZone: true, PrecisionUnspecified: true}},
		{"Timestamp explicit 0", Timestamp{Precision: 0, WithTimeZone: true}},
		{"Array of unspecified timestamptz", Array{Element: Timestamp{WithTimeZone: true, PrecisionUnspecified: true}}},
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

// TestMarshalTable_IndexConstraintBackedRoundTrip pins that
// ir.Index.ConstraintBacked survives the backup / schema-history wire.
// Today Table marshals Index via the default struct JSON, so the flag
// rides for free — this test is the guard against a future custom
// Index marshal silently reopening the TRIAGE-#4 demotion through the
// backup door: a restore that loses the flag re-creates the source
// UNIQUE constraint as a plain unique index, breaking ON CONFLICT ON
// CONSTRAINT on the restored target.
func TestMarshalTable_IndexConstraintBackedRoundTrip(t *testing.T) {
	in := &Table{
		Name:    "accounts",
		Columns: []*Column{{Name: "email", Type: Text{Size: TextLong}}},
		Indexes: []*Index{
			{Name: "accounts_email_unique", Unique: true, ConstraintBacked: true, Columns: []IndexColumn{{Column: "email"}}},
			{Name: "accounts_email_uidx", Unique: true, Columns: []IndexColumn{{Column: "email"}}},
		},
	}
	b, err := MarshalTable(in)
	if err != nil {
		t.Fatalf("MarshalTable: %v", err)
	}
	out, err := UnmarshalTable(b)
	if err != nil {
		t.Fatalf("UnmarshalTable: %v", err)
	}
	if len(out.Indexes) != 2 {
		t.Fatalf("round-trip indexes = %d; want 2 (json=%s)", len(out.Indexes), b)
	}
	if !out.Indexes[0].ConstraintBacked {
		t.Errorf("ConstraintBacked lost through the wire on %q — a backup restore would demote the UNIQUE constraint to a plain index (json=%s)", out.Indexes[0].Name, b)
	}
	if out.Indexes[1].ConstraintBacked {
		t.Errorf("plain unique index %q gained ConstraintBacked through the wire (json=%s)", out.Indexes[1].Name, b)
	}
	// Full-table DeepEqual is deliberately NOT asserted: the Column
	// codec normalizes an absent Default to DefaultNone{} on decode
	// (documented UnmarshalJSON behaviour). The Indexes slice — the
	// surface this pin guards — must round-trip exactly.
	if !reflect.DeepEqual(in.Indexes, out.Indexes) {
		t.Errorf("index round-trip mismatch:\n in: %#v %#v\nout: %#v %#v",
			in.Indexes[0], in.Indexes[1], out.Indexes[0], out.Indexes[1])
	}
}

// TestUnmarshalType_OldTemporalWireDecodesExplicit pins the TRIAGE #3
// cross-version contract: a manifest written by an OLDER binary
// carries the materialized Precision=6 for a bare temporal column and
// NO temporal_precision_unspecified key. Decoding it must yield an
// explicit (6) — restoring an old backup keeps emitting
// `timestamp(6) with time zone`, byte-identical to the old binary's
// restore behaviour (additive wire semantics, no format bump).
func TestUnmarshalType_OldTemporalWireDecodesExplicit(t *testing.T) {
	cases := []struct {
		name string
		wire string
		want Type
	}{
		{"old Timestamp tz 6", `{"kind":"Timestamp","precision":6,"with_time_zone":true}`, Timestamp{Precision: 6, WithTimeZone: true}},
		{"old DateTime 6", `{"kind":"DateTime","precision":6}`, DateTime{Precision: 6}},
		{"old Time 6", `{"kind":"Time","precision":6}`, Time{Precision: 6}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := UnmarshalType([]byte(c.wire))
			if err != nil {
				t.Fatalf("UnmarshalType(%s): %v", c.wire, err)
			}
			if got != c.want {
				t.Errorf("UnmarshalType(%s) = %#v; want %#v", c.wire, got, c.want)
			}
		})
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

// TestSchemaJSON_SequencesRoundTrip pins the item-51 backup-envelope
// shape for [Schema.Sequences]: a schema carrying a standalone
// sequence must survive the same encoding/json round-trip the CDC
// schema-history store and the backup manifests use, with every field
// (options, ownership, captured position) intact. Sequence is all
// concrete fields so Schema's natural marshal covers it — this pin
// exists so a future custom Schema codec can't silently drop the
// slice (the Bug 116 class).
func TestSchemaJSON_SequencesRoundTrip(t *testing.T) {
	in := &Schema{
		Tables: []*Table{{
			Name: "orders",
			Columns: []*Column{{
				Name: "order_number",
				Type: Integer{Width: 64},
				Default: DefaultExpression{
					Expr:    "nextval('order_number_seq'::regclass)",
					Dialect: "postgres",
				},
			}},
		}},
		Sequences: []*Sequence{{
			Schema: "public", Name: "order_number_seq",
			DataType: "bigint", Start: 1000, Increment: 5,
			MinValue: 1, MaxValue: 9223372036854775807, Cache: 1, Cycle: true,
			OwnedByTable: "orders", OwnedByColumn: "order_number",
			LastValue: 1005, LastValueIsCalled: true, LastValueValid: true,
		}},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got Schema
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got.Sequences, in.Sequences) {
		t.Errorf("Sequences did not round-trip:\n got  %+v\n want %+v", got.Sequences, in.Sequences)
	}
}
