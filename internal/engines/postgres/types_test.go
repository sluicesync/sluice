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
