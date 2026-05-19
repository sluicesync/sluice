// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"reflect"
	"testing"

	"vitess.io/vitess/go/vt/proto/query"

	"github.com/orware/sluice/internal/ir"
)

// TestProjectVStreamFields_EveryFieldFamily is the Bug-74-class pin
// for the ADR-0049 Chunk B2 projector. It exercises EVERY Vitess
// proto-Type family AND every ColumnType parameter shape the
// projector must reconstruct — native int (signed / unsigned /
// tinyint(1)-bool / mediumint / year), float (single / double),
// decimal(p,s), every string-leaf (char / varchar / text sizes /
// binary / varbinary / blob sizes) with and without CHARACTER SET,
// every temporal (date / time / datetime(n) / timestamp(n)), enum,
// set, bit fixed-width, json, and a geometry subtype — not one
// representative. A projector right for int/text but silently wrong
// for decimal/enum/bit/temporal is the exact silent-loss class
// ADR-0049 + the Bug-74 lesson forbid; this table is re-derived by
// the lead against the canonical translateType mapping.
//
// The expected ir.Type is ground-truthed against translateType (the
// single canonical MySQL→IR authority the binlog/schema readers use),
// because the projector deliberately routes through it — divergence
// here would mean the columnMeta extraction is wrong.
func TestProjectVStreamFields_EveryFieldFamily(t *testing.T) {
	cases := []struct {
		name       string
		protoType  query.Type
		columnType string
		flags      uint32
		want       ir.Type
	}{
		// ---- native integer ----
		{"tinyint(1) bool", query.Type_INT8, "tinyint(1)", 0, ir.Boolean{}},
		{"tinyint signed", query.Type_INT8, "tinyint", 0, ir.Integer{Width: 8}},
		{"tinyint(4)", query.Type_INT8, "tinyint(4)", 0, ir.Integer{Width: 8}},
		{"tinyint unsigned", query.Type_UINT8, "tinyint unsigned", 0, ir.Integer{Width: 8, Unsigned: true}},
		{"smallint", query.Type_INT16, "smallint", 0, ir.Integer{Width: 16}},
		{"smallint unsigned", query.Type_UINT16, "smallint unsigned", 0, ir.Integer{Width: 16, Unsigned: true}},
		{"mediumint", query.Type_INT24, "mediumint", 0, ir.Integer{Width: 24}},
		{"int", query.Type_INT32, "int", 0, ir.Integer{Width: 32}},
		{"int unsigned", query.Type_UINT32, "int unsigned", 0, ir.Integer{Width: 32, Unsigned: true}},
		{
			"int auto_increment", query.Type_INT32, "int", mysqlFlagAutoIncrement,
			ir.Integer{Width: 32, AutoIncrement: true},
		},
		{"bigint", query.Type_INT64, "bigint", 0, ir.Integer{Width: 64}},
		{"bigint unsigned", query.Type_UINT64, "bigint unsigned", 0, ir.Integer{Width: 64, Unsigned: true}},
		{"year", query.Type_YEAR, "year", 0, ir.Integer{Width: 16}},

		// ---- float / decimal ----
		{"float", query.Type_FLOAT32, "float", 0, ir.Float{Precision: ir.FloatSingle}},
		{"double", query.Type_FLOAT64, "double", 0, ir.Float{Precision: ir.FloatDouble}},
		{"decimal(10,2)", query.Type_DECIMAL, "decimal(10,2)", 0, ir.Decimal{Precision: 10, Scale: 2}},
		{"decimal(38,0)", query.Type_DECIMAL, "decimal(38,0)", 0, ir.Decimal{Precision: 38, Scale: 0}},
		{"numeric(7)", query.Type_DECIMAL, "decimal(7,0)", 0, ir.Decimal{Precision: 7, Scale: 0}},

		// ---- string-leaf (with and without CHARACTER SET) ----
		{"char(36)", query.Type_CHAR, "char(36)", 0, ir.Char{Length: 36}},
		{
			"char charset", query.Type_CHAR, "char(10) character set utf8mb4 collate utf8mb4_bin", 0,
			ir.Char{Length: 10, Charset: "utf8mb4", Collation: "utf8mb4_bin"},
		},
		{"varchar(255)", query.Type_VARCHAR, "varchar(255)", 0, ir.Varchar{Length: 255}},
		{
			"varchar charset", query.Type_VARCHAR, "varchar(64) character set latin1", 0,
			ir.Varchar{Length: 64, Charset: "latin1"},
		},
		{"tinytext", query.Type_TEXT, "tinytext", 0, ir.Text{Size: ir.TextTiny}},
		{"text", query.Type_TEXT, "text", 0, ir.Text{Size: ir.TextRegular}},
		{"mediumtext", query.Type_TEXT, "mediumtext", 0, ir.Text{Size: ir.TextMedium}},
		{"longtext", query.Type_TEXT, "longtext", 0, ir.Text{Size: ir.TextLong}},
		{"binary(16)", query.Type_BINARY, "binary(16)", 0, ir.Binary{Length: 16}},
		{"varbinary(64)", query.Type_VARBINARY, "varbinary(64)", 0, ir.Varbinary{Length: 64}},
		{"tinyblob", query.Type_BLOB, "tinyblob", 0, ir.Blob{Size: ir.BlobTiny}},
		{"blob", query.Type_BLOB, "blob", 0, ir.Blob{Size: ir.BlobRegular}},
		{"mediumblob", query.Type_BLOB, "mediumblob", 0, ir.Blob{Size: ir.BlobMedium}},
		{"longblob", query.Type_BLOB, "longblob", 0, ir.Blob{Size: ir.BlobLong}},

		// ---- temporal (with and without fractional precision) ----
		{"date", query.Type_DATE, "date", 0, ir.Date{}},
		{"time", query.Type_TIME, "time", 0, ir.Time{Precision: 0}},
		{"time(6)", query.Type_TIME, "time(6)", 0, ir.Time{Precision: 6}},
		{"datetime", query.Type_DATETIME, "datetime", 0, ir.DateTime{Precision: 0}},
		{"datetime(3)", query.Type_DATETIME, "datetime(3)", 0, ir.DateTime{Precision: 3}},
		{
			"timestamp", query.Type_TIMESTAMP, "timestamp", 0,
			ir.Timestamp{Precision: 0, WithTimeZone: true},
		},
		{
			"timestamp(6)", query.Type_TIMESTAMP, "timestamp(6)", 0,
			ir.Timestamp{Precision: 6, WithTimeZone: true},
		},

		// ---- categorical / structured / bit / spatial ----
		{
			"enum", query.Type_ENUM, "enum('red','green','blue')", 0,
			ir.Enum{Values: []string{"red", "green", "blue"}},
		},
		{
			"set", query.Type_SET, "set('r','w','x')", 0,
			ir.Set{Values: []string{"r", "w", "x"}},
		},
		{"bit(1) bool", query.Type_BIT, "bit(1)", 0, ir.Boolean{}},
		{"bit(8)", query.Type_BIT, "bit(8)", 0, ir.Bit{Length: 8}},
		{"bit(64)", query.Type_BIT, "bit(64)", 0, ir.Bit{Length: 64}},
		{"json", query.Type_JSON, "json", 0, ir.JSON{Binary: true}},
		{
			"geometry point", query.Type_GEOMETRY, "point", 0,
			ir.Geometry{Subtype: ir.GeometryPoint, SRID: 0},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &query.Field{
				Name:       "c",
				Type:       tc.protoType,
				ColumnType: tc.columnType,
				Flags:      tc.flags,
			}
			tbl, err := projectVStreamFields("ks", "tbl", []*query.Field{f})
			if err != nil {
				t.Fatalf("projectVStreamFields(%q): %v", tc.columnType, err)
			}
			if len(tbl.Columns) != 1 {
				t.Fatalf("want 1 column, got %d", len(tbl.Columns))
			}
			got := tbl.Columns[0].Type
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ColumnType %q → %#v, want %#v", tc.columnType, got, tc.want)
			}
		})
	}
}

// TestProjectVStreamFields_MatchesTranslateType cross-checks the
// projector against the canonical translateType for the parametric
// families, asserting the columnMeta extraction feeds translateType
// the same inputs an information_schema row would. This is the
// "single mapping authority" guard: if the projector ever diverges
// from translateType, this fails even when the standalone table above
// still passes (defence against an extraction bug that happens to
// agree with a hand-written expected value).
func TestProjectVStreamFields_MatchesTranslateType(t *testing.T) {
	samples := []struct {
		columnType string
		meta       columnMeta
	}{
		{"varchar(255)", columnMeta{DataType: "varchar", ColumnType: "varchar(255)", CharMaxLen: int64p(255)}},
		{
			"decimal(12,4)",
			columnMeta{DataType: "decimal", ColumnType: "decimal(12,4)", NumPrec: int64p(12), NumScale: int64p(4)},
		},
		{"datetime(6)", columnMeta{DataType: "datetime", ColumnType: "datetime(6)", DTPrec: int64p(6)}},
		{"bit(8)", columnMeta{DataType: "bit", ColumnType: "bit(8)"}},
		{
			"enum('a','b')",
			columnMeta{DataType: "enum", ColumnType: "enum('a','b')"},
		},
	}
	for _, s := range samples {
		want, wantErr := translateType(s.meta)
		f := &query.Field{Name: "c", ColumnType: s.columnType}
		tbl, err := projectVStreamFields("ks", "t", []*query.Field{f})
		if wantErr != nil {
			if err == nil {
				t.Errorf("%q: projector ok but translateType errored: %v", s.columnType, wantErr)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%q: projector err: %v", s.columnType, err)
		}
		if !reflect.DeepEqual(tbl.Columns[0].Type, want) {
			t.Errorf("%q: projector %#v != translateType %#v", s.columnType, tbl.Columns[0].Type, want)
		}
	}
}

// TestProjectVStreamFields_Errors pins the loud-floor: empty field
// list and an unmappable ColumnType both error rather than producing
// a silent fallback type in a persisted schema-history version.
func TestProjectVStreamFields_Errors(t *testing.T) {
	if _, err := projectVStreamFields("ks", "t", nil); err == nil {
		t.Error("empty field list: want error, got nil")
	}
	f := &query.Field{Name: "c", ColumnType: "some_future_type(9)"}
	if _, err := projectVStreamFields("ks", "t", []*query.Field{f}); err == nil {
		t.Error("unmappable column_type: want loud error, got nil")
	} else if errors.Is(err, errFieldMetadataUnavailable) {
		t.Error("present-but-unmappable column_type wrongly classified as metadata-unavailable " +
			"(must stay a hard loud error, not degrade to cold-start)")
	}

	// Absent ColumnType is the DISTINCT metadata-unavailable sentinel
	// (degrade-to-cold-start, NOT a hard unknown-type error). This is
	// the discriminator the Chunk-B2 boundary path branches on.
	noMeta := &query.Field{Name: "c", Type: query.Type_INT64, ColumnType: ""}
	_, err := projectVStreamFields("ks", "t", []*query.Field{noMeta})
	if !errors.Is(err, errFieldMetadataUnavailable) {
		t.Errorf("absent column_type: want errFieldMetadataUnavailable, got %v", err)
	}
}

// TestProjectVStreamFields_Nullability pins that the NOT_NULL proto
// flag drives Column.Nullable (informational, but a faithful
// projection).
func TestProjectVStreamFields_Nullability(t *testing.T) {
	notNull := &query.Field{Name: "a", Type: query.Type_INT32, ColumnType: "int", Flags: mysqlFlagNotNull}
	nullable := &query.Field{Name: "b", Type: query.Type_INT32, ColumnType: "int", Flags: 0}
	tbl, err := projectVStreamFields("ks", "t", []*query.Field{notNull, nullable})
	if err != nil {
		t.Fatalf("projectVStreamFields: %v", err)
	}
	if tbl.Columns[0].Nullable {
		t.Error("NOT_NULL column projected Nullable=true")
	}
	if !tbl.Columns[1].Nullable {
		t.Error("nullable column projected Nullable=false")
	}
}
