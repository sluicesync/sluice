// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"database/sql"
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
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
		// ----- Booleans / tinyint -----
		{
			name: "tinyint(1) is Boolean",
			in:   columnMeta{DataType: "tinyint", ColumnType: "tinyint(1)"},
			want: ir.Boolean{},
		},
		{
			name: "tinyint(1) unsigned is Integer (the (1) is just a display width)",
			in:   columnMeta{DataType: "tinyint", ColumnType: "tinyint(1) unsigned"},
			want: ir.Integer{Width: 8, Unsigned: true},
		},
		{
			name: "tinyint(3) is Integer",
			in:   columnMeta{DataType: "tinyint", ColumnType: "tinyint(3)"},
			want: ir.Integer{Width: 8},
		},
		{
			name: "tinyint(1) auto_increment is Integer (auto_increment can't be on bool)",
			in:   columnMeta{DataType: "tinyint", ColumnType: "tinyint(1)", Extra: "auto_increment"},
			want: ir.Integer{Width: 8, AutoIncrement: true},
		},

		// ----- Integer family -----
		{
			name: "smallint",
			in:   columnMeta{DataType: "smallint", ColumnType: "smallint"},
			want: ir.Integer{Width: 16},
		},
		{
			name: "mediumint unsigned",
			in:   columnMeta{DataType: "mediumint", ColumnType: "mediumint unsigned"},
			want: ir.Integer{Width: 24, Unsigned: true},
		},
		{
			name: "int auto_increment",
			in:   columnMeta{DataType: "int", ColumnType: "int(11)", Extra: "auto_increment"},
			want: ir.Integer{Width: 32, AutoIncrement: true},
		},
		{
			name: "bigint unsigned auto_increment",
			in:   columnMeta{DataType: "bigint", ColumnType: "bigint(20) unsigned", Extra: "auto_increment"},
			want: ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true},
		},
		{
			name: "year",
			in:   columnMeta{DataType: "year", ColumnType: "year(4)"},
			want: ir.Integer{Width: 16},
		},

		// ----- Decimal / float -----
		{
			name: "decimal(18,4)",
			in:   columnMeta{DataType: "decimal", ColumnType: "decimal(18,4)", NumPrec: int64Val(18), NumScale: int64Val(4)},
			want: ir.Decimal{Precision: 18, Scale: 4},
		},
		{
			name: "float",
			in:   columnMeta{DataType: "float", ColumnType: "float"},
			want: ir.Float{Precision: ir.FloatSingle},
		},
		{
			name: "double",
			in:   columnMeta{DataType: "double", ColumnType: "double"},
			want: ir.Float{Precision: ir.FloatDouble},
		},

		// ----- Bit -----
		{
			name: "bit(1) is Boolean",
			in:   columnMeta{DataType: "bit", ColumnType: "bit(1)"},
			want: ir.Boolean{},
		},
		{
			// catalog Bug 62: BIT(N>1) is a fixed-width bit string,
			// not Varbinary (the pre-v0.65.1 mis-mapping).
			name: "bit(8) is Bit(8)",
			in:   columnMeta{DataType: "bit", ColumnType: "bit(8)"},
			want: ir.Bit{Length: 8},
		},
		{
			name: "bit(16) is Bit(16)",
			in:   columnMeta{DataType: "bit", ColumnType: "bit(16)"},
			want: ir.Bit{Length: 16},
		},
		{
			name: "bit(9) is Bit(9) (no byte rounding)",
			in:   columnMeta{DataType: "bit", ColumnType: "bit(9)"},
			want: ir.Bit{Length: 9},
		},

		// ----- Strings -----
		{
			name: "char(10) utf8mb4",
			in: columnMeta{
				DataType: "char", ColumnType: "char(10)",
				CharMaxLen: int64Val(10), Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci",
			},
			want: ir.Char{Length: 10, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"},
		},
		{
			name: "varchar(255)",
			in: columnMeta{
				DataType: "varchar", ColumnType: "varchar(255)",
				CharMaxLen: int64Val(255), Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci",
			},
			want: ir.Varchar{Length: 255, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"},
		},
		{
			name: "tinytext",
			in:   columnMeta{DataType: "tinytext", ColumnType: "tinytext", Charset: "utf8mb4"},
			want: ir.Text{Size: ir.TextTiny, Charset: "utf8mb4"},
		},
		{
			name: "longtext",
			in:   columnMeta{DataType: "longtext", ColumnType: "longtext", Charset: "utf8mb4"},
			want: ir.Text{Size: ir.TextLong, Charset: "utf8mb4"},
		},

		// ----- Binary -----
		{
			name: "binary(16)",
			in:   columnMeta{DataType: "binary", ColumnType: "binary(16)", CharMaxLen: int64Val(16)},
			want: ir.Binary{Length: 16},
		},
		{
			name: "varbinary(64)",
			in:   columnMeta{DataType: "varbinary", ColumnType: "varbinary(64)", CharMaxLen: int64Val(64)},
			want: ir.Varbinary{Length: 64},
		},
		{
			name: "longblob",
			in:   columnMeta{DataType: "longblob", ColumnType: "longblob"},
			want: ir.Blob{Size: ir.BlobLong},
		},

		// ----- Temporal -----
		{
			name: "date",
			in:   columnMeta{DataType: "date", ColumnType: "date"},
			want: ir.Date{},
		},
		{
			name: "time(6)",
			in:   columnMeta{DataType: "time", ColumnType: "time(6)", DTPrec: int64Val(6)},
			want: ir.Time{Precision: 6},
		},
		{
			name: "datetime(0)",
			in:   columnMeta{DataType: "datetime", ColumnType: "datetime", DTPrec: int64Val(0)},
			want: ir.DateTime{Precision: 0},
		},
		{
			name: "timestamp is zoned",
			in:   columnMeta{DataType: "timestamp", ColumnType: "timestamp(3)", DTPrec: int64Val(3)},
			want: ir.Timestamp{Precision: 3, WithTimeZone: true},
		},

		// ----- ENUM / SET -----
		{
			name: "enum",
			in:   columnMeta{DataType: "enum", ColumnType: "enum('a','b','c')"},
			want: ir.Enum{Values: []string{"a", "b", "c"}},
		},
		{
			name: "set",
			in:   columnMeta{DataType: "set", ColumnType: "set('x','y')"},
			want: ir.Set{Values: []string{"x", "y"}},
		},

		// ----- JSON -----
		{
			name: "json is binary",
			in:   columnMeta{DataType: "json", ColumnType: "json"},
			want: ir.JSON{Binary: true},
		},

		// ----- Geometry -----
		{
			name: "point",
			in:   columnMeta{DataType: "point", ColumnType: "point"},
			want: ir.Geometry{Subtype: ir.GeometryPoint},
		},
		{
			name: "geomcollection alias",
			in:   columnMeta{DataType: "geomcollection", ColumnType: "geomcollection"},
			want: ir.Geometry{Subtype: ir.GeometryCollection},
		},
		{
			// Bug 26 (v0.10.3): SRID threads through from
			// information_schema.columns.srs_id into ir.Geometry.SRID
			// so the cross-engine emit lands `geometry(POINT, 4326)`
			// on PG instead of dropping to `geometry(POINT, 0)`.
			name: "point with explicit SRID 4326",
			in:   columnMeta{DataType: "point", ColumnType: "point", SrsID: 4326},
			want: ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326},
		},
		{
			name: "polygon with SRID",
			in:   columnMeta{DataType: "polygon", ColumnType: "polygon", SrsID: 3857},
			want: ir.Geometry{Subtype: ir.GeometryPolygon, SRID: 3857},
		},
		{
			// SrsID=0 (no spatial reference declared) is the
			// default behaviour and matches the pre-Bug-26 state.
			name: "geometry with no SRID stays at 0",
			in:   columnMeta{DataType: "geometry", ColumnType: "geometry", SrsID: 0},
			want: ir.Geometry{Subtype: ir.GeometryUnspecified},
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

func TestTranslateTypeUnsupported(t *testing.T) {
	_, err := translateType(columnMeta{DataType: "unobtainium", ColumnType: "unobtainium"})
	if err == nil {
		t.Fatal("expected error for unsupported type, got nil")
	}
}

func TestParseEnumOrSet(t *testing.T) {
	cases := []struct {
		name string
		in   string
		kind string
		want []string
	}{
		{"single", "enum('a')", "enum", []string{"a"}},
		{"multi", "enum('a','b','c')", "enum", []string{"a", "b", "c"}},
		{"set", "set('x','y','z')", "set", []string{"x", "y", "z"}},
		{"escaped quote", `enum('it''s','ok')`, "enum", []string{"it's", "ok"}},
		{"empty value", "enum('','b')", "enum", []string{"", "b"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := parseEnumOrSet(c.in, c.kind)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("got %#v; want %#v", got, c.want)
			}
		})
	}
}

func TestParseEnumOrSetMalformed(t *testing.T) {
	cases := []string{
		"enum",          // no parens
		"enum(",         // open paren only
		"enum('a'",      // missing close paren
		"enum('a)",      // unterminated value
		"enum(a,b)",     // unquoted
		"enum('a' 'b')", // missing comma
	}
	for _, c := range cases {
		c := c
		t.Run(c, func(t *testing.T) {
			if _, err := parseEnumOrSet(c, "enum"); err == nil {
				t.Errorf("expected error for %q, got nil", c)
			}
		})
	}
}

func TestDisplayWidth(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"tinyint", 0},
		{"tinyint(1)", 1},
		{"int(11) unsigned", 11},
		{"bit(8)", 8},
		{"varchar(255)", 255},
	}
	for _, c := range cases {
		if got := displayWidth(c.in); got != c.want {
			t.Errorf("displayWidth(%q) = %d; want %d", c.in, got, c.want)
		}
	}
}

func TestTranslateDefault(t *testing.T) {
	cases := []struct {
		name  string
		def   sql.NullString
		extra string
		typ   ir.Type
		want  ir.DefaultValue
	}{
		{"no default", sql.NullString{Valid: false}, "", ir.Integer{Width: 32}, ir.DefaultNone{}},
		{"literal zero", sql.NullString{String: "0", Valid: true}, "", ir.Integer{Width: 32}, ir.DefaultLiteral{Value: "0"}},
		{
			"expression CURRENT_TIMESTAMP",
			sql.NullString{String: "CURRENT_TIMESTAMP", Valid: true},
			"DEFAULT_GENERATED",
			ir.DateTime{},
			ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP", Dialect: "mysql"},
		},
		{
			"expression mixed-case extra token",
			sql.NullString{String: "now()", Valid: true},
			"default_generated on update current_timestamp",
			ir.DateTime{},
			ir.DefaultExpression{Expr: "now()", Dialect: "mysql"},
		},
		{
			// catalog Bug 62: BIT(N>1) default preserved as a tagged
			// bit literal, not decimal-collapsed.
			"bit(8) literal preserved",
			sql.NullString{String: "b'10100101'", Valid: true},
			"",
			ir.Bit{Length: 8},
			ir.DefaultExpression{Expr: "b'10100101'", Dialect: bitLiteralDialect},
		},
		{
			// catalog #4 (unchanged): BIT(1) → Boolean decimal collapse.
			"bit(1) literal decimal-collapsed",
			sql.NullString{String: "b'1'", Valid: true},
			"",
			ir.Boolean{},
			ir.DefaultLiteral{Value: "1"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := translateDefault(c.def, c.extra, c.typ)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("got %#v; want %#v", got, c.want)
			}
		})
	}
}

func TestSortedKeys(t *testing.T) {
	in := map[string]int{"banana": 1, "apple": 2, "cherry": 3}
	got := sortedKeys(in)
	want := []string{"apple", "banana", "cherry"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v; want %v", got, want)
	}
}
