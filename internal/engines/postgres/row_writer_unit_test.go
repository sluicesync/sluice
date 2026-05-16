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

func TestPrepareValueArrayConversion(t *testing.T) {
	cases := []struct {
		name string
		in   []any
		elem ir.Type
		want any
	}{
		{
			"int array → []int64",
			[]any{int64(1), int64(2), int64(3)},
			ir.Integer{Width: 32},
			[]int64{1, 2, 3},
		},
		{
			"text array → []string",
			[]any{"a", "b"},
			ir.Text{Size: ir.TextLong},
			[]string{"a", "b"},
		},
		{
			"bool array → []bool",
			[]any{true, false, true},
			ir.Boolean{},
			[]bool{true, false, true},
		},
		{
			"uuid array → []string",
			[]any{"00000000-0000-0000-0000-000000000001"},
			ir.UUID{},
			[]string{"00000000-0000-0000-0000-000000000001"},
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
