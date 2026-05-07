// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"encoding/json"
	"testing"
	"time"
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
		{"Array of Integer", Array{Element: Integer{Width: 32}}},
		{"Array of UUID", Array{Element: UUID{}}},
		{"Array of nil element", Array{Element: nil}},
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

// Manifest round-trip via standard json.Marshal. Validates that the
// public-contract type is JSON-stable end-to-end. Includes a Schema
// with a column to exercise the Column custom marshal pathway.
func TestManifestJSON_RoundTrip(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 30, 0, 0, time.UTC)
	original := &Manifest{
		FormatVersion: BackupFormatVersion,
		SluiceVersion: "0.14.1",
		CreatedAt:     now,
		SourceEngine:  "postgres",
		Schema: &Schema{
			Tables: []*Table{
				{
					Name: "users",
					Columns: []*Column{
						{Name: "id", Type: Integer{Width: 64, AutoIncrement: true}},
						{Name: "email", Type: Varchar{Length: 255}, Nullable: true},
					},
				},
			},
		},
		Tables: []*TableManifest{
			{
				Name:     "users",
				RowCount: 12345,
				Chunks: []*ChunkInfo{
					{File: "chunks/users/users-0.jsonl.gz", RowCount: 10000, SHA256: "abc123"},
					{File: "chunks/users/users-1.jsonl.gz", RowCount: 2345, SHA256: "def456"},
				},
			},
		},
	}
	b, err := json.MarshalIndent(original, "", "  ")
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got Manifest
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v\nJSON:\n%s", err, b)
	}
	if got.FormatVersion != original.FormatVersion {
		t.Errorf("FormatVersion: got %d want %d", got.FormatVersion, original.FormatVersion)
	}
	if got.SourceEngine != "postgres" {
		t.Errorf("SourceEngine: got %q", got.SourceEngine)
	}
	if !got.CreatedAt.Equal(now) {
		t.Errorf("CreatedAt: got %v want %v", got.CreatedAt, now)
	}
	if len(got.Schema.Tables) != 1 || got.Schema.Tables[0].Name != "users" {
		t.Fatalf("Schema.Tables: got %v", got.Schema.Tables)
	}
	if len(got.Schema.Tables[0].Columns) != 2 {
		t.Fatalf("Columns count: %d", len(got.Schema.Tables[0].Columns))
	}
	gotInt := got.Schema.Tables[0].Columns[0]
	if gotInt.Type == nil || gotInt.Type.String() != "Int64 AutoIncrement" {
		t.Errorf("id Type: got %v want Int64 AutoIncrement", gotInt.Type)
	}
	if len(got.Tables) != 1 {
		t.Fatalf("Tables: got %d", len(got.Tables))
	}
	if got.Tables[0].RowCount != 12345 {
		t.Errorf("RowCount: got %d", got.Tables[0].RowCount)
	}
	if len(got.Tables[0].Chunks) != 2 {
		t.Fatalf("Chunks: got %d", len(got.Tables[0].Chunks))
	}
	if got.Tables[0].Chunks[0].SHA256 != "abc123" {
		t.Errorf("SHA256[0]: got %q", got.Tables[0].Chunks[0].SHA256)
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
