// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/orware/sluice/internal/ir"
)

// TestBitBytesMySQLToPG pins the catalog Bug 62 byte re-alignment:
// MySQL stores BIT(n) right-justified in ceil(n/8) big-endian bytes;
// PG's bit(n) wire format is left-justified. Byte-aligned widths
// (BIT(8)/BIT(16)) are a straight copy (the repro shapes); non-byte-
// aligned widths (BIT(9)) must shift so logical bit 0 is byte 0's MSB.
func TestBitBytesMySQLToPG(t *testing.T) {
	cases := []struct {
		name string
		src  []byte
		n    int
		want []byte
	}{
		// BIT(8) b'10100101' = 0xA5 — byte-aligned, identity.
		{"bit(8) 0xA5", []byte{0xA5}, 8, []byte{0xA5}},
		// BIT(16) b'1111000011110000' = 0xF0F0 — byte-aligned.
		{"bit(16) 0xF0F0", []byte{0xF0, 0xF0}, 16, []byte{0xF0, 0xF0}},
		// BIT(1) value 1 → MSB of the single byte.
		{"bit(1) one", []byte{0x01}, 1, []byte{0x80}},
		// BIT(9) value 0b1_0101_0101 (0x155). MySQL right-justified:
		// [0x01, 0x55]. PG left-justified 9 bits: bit0..bit8 →
		// byte0 = 1010_1010 (0xAA), byte1 MSB = bit8 (1) → 0x80.
		{"bit(9) 0x155", []byte{0x01, 0x55}, 9, []byte{0xAA, 0x80}},
		// BIT(4) value 0b1011 = 0x0B. PG: 1011_0000 = 0xB0.
		{"bit(4) 0x0B", []byte{0x0B}, 4, []byte{0xB0}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := bitBytesMySQLToPG(c.src, c.n)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("bitBytesMySQLToPG(%#v, %d) = %#v; want %#v", c.src, c.n, got, c.want)
			}
		})
	}
}

// TestPrepareValueBit pins that an ir.Bit column's []byte value is
// wrapped in a pgtype.Bits with the correct Len so pgx's BitsCodec
// encodes it under COPY binary / batch INSERT (catalog Bug 62).
func TestPrepareValueBit(t *testing.T) {
	got, err := prepareValue([]byte{0xA5}, ir.Bit{Length: 8})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bits, ok := got.(pgtype.Bits)
	if !ok {
		t.Fatalf("got %T; want pgtype.Bits", got)
	}
	if bits.Len != 8 || !bits.Valid || !reflect.DeepEqual(bits.Bytes, []byte{0xA5}) {
		t.Errorf("got %+v; want {Bytes:[0xA5] Len:8 Valid:true}", bits)
	}
	// Same-engine PG bit value can surface as a '0'/'1' string; pass
	// through for the codec to parse.
	gotStr, err := prepareValue("10100101", ir.Bit{Length: 8})
	if err != nil {
		t.Fatalf("unexpected error (string path): %v", err)
	}
	if gotStr != "10100101" {
		t.Errorf("string path: got %#v; want %q", gotStr, "10100101")
	}
}

func TestBuildBatchInsert(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
	}

	cases := []struct {
		rows int
		want string
	}{
		{1, `INSERT INTO "public"."users" ("id", "email") VALUES ($1, $2)`},
		{3, `INSERT INTO "public"."users" ("id", "email") VALUES ($1, $2), ($3, $4), ($5, $6)`},
	}
	for _, c := range cases {
		got := buildBatchInsert("public", table, c.rows)
		if got != c.want {
			t.Errorf("buildBatchInsert(%d):\n got  %q\n want %q", c.rows, got, c.want)
		}
	}
}

func TestBuildBatchInsertSchemaQualified(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
		},
	}
	got := buildBatchInsert("app", table, 1)
	want := `INSERT INTO "app"."users" ("id") VALUES ($1)`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

func TestPrepareValuePassthrough(t *testing.T) {
	// Most types pass through unchanged.
	cases := []struct {
		name string
		in   any
		t    ir.Type
	}{
		{"int64", int64(42), ir.Integer{Width: 32}},
		{"string", "hello", ir.Varchar{Length: 32}},
		{"bool", true, ir.Boolean{}},
		{"float64", 3.14, ir.Float{Precision: ir.FloatDouble}},
		{"bytes", []byte{0xde, 0xad}, ir.Blob{Size: ir.BlobLong}},
		{"nil", nil, ir.Integer{Width: 32}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := prepareValue(c.in, c.t)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, c.in) {
				t.Errorf("prepareValue(%#v) = %#v; want %#v", c.in, got, c.in)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

func dim(n int) pgtype.ArrayDimension {
	return pgtype.ArrayDimension{Length: int32(n), LowerBound: 1}
}

// TestPrepareValueArrayConversion pins the pgtype.Array[*T] shape
// convertArray hands pgx (catalog Bug 70). Pointer leaf elements let a
// nil slot survive as a SQL NULL element; the explicit Dims +
// row-major-flattened Elements is the only shape that round-trips
// multi-dimensional arrays through pgx (a nested Go slice gets
// flattened by pgx's plain-slice wrapper before the multi-dim wrapper
// runs). pgx encodes []*T and []T identically, so the 1-D cross-engine
// wire result is unchanged.
func TestPrepareValueArrayConversion(t *testing.T) {
	cases := []struct {
		name string
		in   []any
		elem ir.Type
		want any
	}{
		{
			"int array",
			[]any{int64(1), int64(2), int64(3)},
			ir.Integer{Width: 32},
			pgtype.Array[*int64]{Elements: []*int64{ptr(int64(1)), ptr(int64(2)), ptr(int64(3))}, Dims: []pgtype.ArrayDimension{dim(3)}, Valid: true},
		},
		{
			"text array",
			[]any{"a", "b"},
			ir.Text{Size: ir.TextLong},
			pgtype.Array[*string]{Elements: []*string{ptr("a"), ptr("b")}, Dims: []pgtype.ArrayDimension{dim(2)}, Valid: true},
		},
		{
			"bool array",
			[]any{true, false, true},
			ir.Boolean{},
			pgtype.Array[*bool]{Elements: []*bool{ptr(true), ptr(false), ptr(true)}, Dims: []pgtype.ArrayDimension{dim(3)}, Valid: true},
		},
		{
			"uuid array",
			[]any{"00000000-0000-0000-0000-000000000001"},
			ir.UUID{},
			pgtype.Array[*string]{Elements: []*string{ptr("00000000-0000-0000-0000-000000000001")}, Dims: []pgtype.ArrayDimension{dim(1)}, Valid: true},
		},
		// Bug 70: a NULL element inside a typed array. The nil slot
		// must survive as a typed nil pointer (SQL NULL), not abort
		// with "expected int64, got <nil>".
		{
			"int array with NULL element",
			[]any{int64(1), nil, int64(3)},
			ir.Integer{Width: 32},
			pgtype.Array[*int64]{Elements: []*int64{ptr(int64(1)), nil, ptr(int64(3))}, Dims: []pgtype.ArrayDimension{dim(3)}, Valid: true},
		},
		{
			"text array with NULL element",
			[]any{"x", nil, "z"},
			ir.Text{Size: ir.TextLong},
			pgtype.Array[*string]{Elements: []*string{ptr("x"), nil, ptr("z")}, Dims: []pgtype.ArrayDimension{dim(3)}, Valid: true},
		},
		{
			"numeric[] (string) with NULL element",
			[]any{"1.5", nil, "3.5"},
			ir.Decimal{},
			pgtype.Array[*string]{Elements: []*string{ptr("1.5"), nil, ptr("3.5")}, Dims: []pgtype.ArrayDimension{dim(3)}, Valid: true},
		},
		// Bug 70: a multi-dimensional array. Elements is row-major
		// flattened and Dims carries the 2-D shape so pgx emits
		// {{1,2},{3,4}} not the flattened {1,2,3,4}.
		{
			"int[][] multi-dim",
			[]any{[]any{int64(1), int64(2)}, []any{int64(3), int64(4)}},
			ir.Integer{Width: 32},
			pgtype.Array[*int64]{
				Elements: []*int64{ptr(int64(1)), ptr(int64(2)), ptr(int64(3)), ptr(int64(4))},
				Dims:     []pgtype.ArrayDimension{dim(2), dim(2)},
				Valid:    true,
			},
		},
		{
			"int[][] multi-dim with NULL element",
			[]any{[]any{int64(1), nil}, []any{nil, int64(4)}},
			ir.Integer{Width: 32},
			pgtype.Array[*int64]{
				Elements: []*int64{ptr(int64(1)), nil, nil, ptr(int64(4))},
				Dims:     []pgtype.ArrayDimension{dim(2), dim(2)},
				Valid:    true,
			},
		},
		{
			"empty array",
			[]any{},
			ir.Integer{Width: 32},
			pgtype.Array[*int64]{Elements: []*int64{}, Dims: []pgtype.ArrayDimension{dim(0)}, Valid: true},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := prepareValue(c.in, ir.Array{Element: c.elem})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("\n got  %#v (%T)\n want %#v (%T)", got, got, c.want, c.want)
			}
		})
	}
}

func TestPrepareValueArrayWrongElementType(t *testing.T) {
	// An int64 element where the column is text[] → error.
	_, err := prepareValue([]any{int64(1)}, ir.Array{Element: ir.Text{Size: ir.TextLong}})
	if err == nil {
		t.Error("expected error for type mismatch in array element; got nil")
	}
}

// TestTableHasPGVectorColumn pins the predicate that drives whether
// writeViaCopy registers the per-conn pgvector codec. Bug 47: the
// codec must register exactly when (and only when) the table carries
// a vector column — otherwise the COPY path either misencodes the
// vector or pays a stray pg_type lookup on every unrelated table.
func TestTableHasPGVectorColumn(t *testing.T) {
	cases := []struct {
		name string
		cols []*ir.Column
		want bool
	}{
		{
			name: "no extension columns",
			cols: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "name", Type: ir.Varchar{Length: 64}},
			},
			want: false,
		},
		{
			name: "vector column present",
			cols: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "embedding", Type: ir.ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{3}}},
			},
			want: true,
		},
		{
			name: "different extension only",
			cols: []*ir.Column{
				{Name: "fingerprint", Type: ir.ExtensionType{Extension: "pg_trgm", Name: "trgm"}},
			},
			want: false,
		},
		{
			name: "empty columns",
			cols: nil,
			want: false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := tableHasPGVectorColumn(&ir.Table{Name: "t", Columns: c.cols})
			if got != c.want {
				t.Errorf("tableHasPGVectorColumn = %v, want %v", got, c.want)
			}
		})
	}
}
