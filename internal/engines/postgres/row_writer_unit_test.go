// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"sluicesync.dev/sluice/internal/ir"
)

// TestPrepareValueBit pins the catalog Bug 75 fix: an ir.Bit value is
// the IR-canonical '0'/'1' bit-string (engine-neutral), and the PG
// writer must turn it into a pgtype.Bits with PG-left-aligned bytes
// and the correct Len so pgx's BitsCodec encodes it faithfully under
// COPY binary / batch INSERT. The prior []byte path silently
// truncated PG-source values to one byte.
func TestPrepareValueBit(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		length   int
		wantByte []byte
		wantLen  int32
	}{
		// BIT(8) 10100101 → left-aligned single byte 0xA5.
		{"bit(8)", "10100101", 8, []byte{0xA5}, 8},
		// BIT(16) — two bytes, left-aligned.
		{"bit(16)", "1111000011110000", 16, []byte{0xF0, 0xF0}, 16},
		// BIT(1) — MSB of the single byte.
		{"bit(1)", "1", 1, []byte{0x80}, 1},
		// BIT(9) — 9 bits left-aligned: 1010_1010 1|.......
		{"bit(9)", "101010101", 9, []byte{0xAA, 0x80}, 9},
		// BIT(4) 1011 → 1011_0000.
		{"bit(4)", "1011", 4, []byte{0xB0}, 4},
		// varbit (Length 0): the value's own length is authoritative.
		{"varbit", "1100", 0, []byte{0xC0}, 4},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := prepareValue(c.in, ir.Bit{Length: c.length})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			bits, ok := got.(pgtype.Bits)
			if !ok {
				t.Fatalf("got %T; want pgtype.Bits", got)
			}
			if bits.Len != c.wantLen || !bits.Valid || !reflect.DeepEqual(bits.Bytes, c.wantByte) {
				t.Errorf("got %+v; want {Bytes:%#v Len:%d Valid:true}", bits, c.wantByte, c.wantLen)
			}
		})
	}
	// A non-string ir.Bit value is an upstream decode bug → loud error,
	// never a silent wrong value.
	if _, err := prepareValue([]byte{0xA5}, ir.Bit{Length: 8}); err == nil {
		t.Error("expected error for non-string Bit value; got nil")
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

// TestPrepareValueNULByteRefused pins the Vector C loud refusal: a string
// carrying an embedded NUL (0x00) bound for a PG text type (text/varchar/
// char) is refused with an actionable message, instead of letting pgx fail
// opaquely mid-COPY (PG can't store NUL in text). A NUL-free string passes
// through; the same bytes bound for bytea are fine (bytea holds NUL).
func TestPrepareValueNULByteRefused(t *testing.T) {
	withNUL := "ab\x00cd"
	for _, tt := range []struct {
		name string
		typ  ir.Type
	}{
		{"text", ir.Text{Size: ir.TextLong}},
		{"varchar", ir.Varchar{Length: 32}},
		{"char", ir.Char{Length: 8}},
	} {
		t.Run(tt.name+" with NUL refuses", func(t *testing.T) {
			_, err := prepareValue(withNUL, tt.typ)
			if err == nil {
				t.Fatalf("prepareValue(%q, %T) err = nil; want a NUL-byte refusal", withNUL, tt.typ)
			}
			if !strings.Contains(err.Error(), "NUL byte") || !strings.Contains(err.Error(), "bytea") {
				t.Errorf("err = %q; want it to name the NUL byte + the bytea remedy", err)
			}
		})
	}
	// NUL-free text passes through unchanged.
	if got, err := prepareValue("abcd", ir.Text{Size: ir.TextLong}); err != nil || got != "abcd" {
		t.Errorf("NUL-free text: got (%#v, %v); want (\"abcd\", nil)", got, err)
	}
	// The same NUL-bearing bytes are fine for bytea (Blob) — NUL is legal there.
	if _, err := prepareValue([]byte(withNUL), ir.Blob{Size: ir.BlobLong}); err != nil {
		t.Errorf("bytea with NUL: unexpected error %v", err)
	}
	// A DOMAIN over a text base recurses into the guard.
	if _, err := prepareValue(withNUL, ir.Domain{Name: "d", BaseType: ir.Text{Size: ir.TextLong}}); err == nil {
		t.Error("DOMAIN over text with NUL: err = nil; want refusal via base-type recursion")
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
			pgtype.Array[*pgtype.Text]{Elements: []*pgtype.Text{ptr(pgtype.Text{String: "a", Valid: true}), ptr(pgtype.Text{String: "b", Valid: true})}, Dims: []pgtype.ArrayDimension{dim(2)}, Valid: true},
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
			pgtype.Array[*pgtype.Text]{Elements: []*pgtype.Text{ptr(pgtype.Text{String: "00000000-0000-0000-0000-000000000001", Valid: true})}, Dims: []pgtype.ArrayDimension{dim(1)}, Valid: true},
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
			pgtype.Array[*pgtype.Text]{Elements: []*pgtype.Text{ptr(pgtype.Text{String: "x", Valid: true}), nil, ptr(pgtype.Text{String: "z", Valid: true})}, Dims: []pgtype.ArrayDimension{dim(3)}, Valid: true},
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

// The per-family leaf-type matrix (TestConvertArrayPerFamilyLeafAndDims)
// pins, for every element family, the concrete leaf Go type
// convertArray must select (catalog Bug 73/74). The leaf type is
// load-bearing: pgx's ArrayCodec plans the element encode against the
// *target column element OID*, and a leaf the OID's codec can't plan
// makes pgx silently flatten ≥2-D arrays (Bug 74) — so it is asserted
// structurally (leaf type + Dims + NULL positions), not just via a
// single representative type. The matrix covers every family at 1-D,
// multi-dim (2-D), and with a NULL element — the class-closing shape
// the v0.69.3 review gap missed.

func dimsOf(v any) []pgtype.ArrayDimension {
	return reflect.ValueOf(v).FieldByName("Dims").Interface().([]pgtype.ArrayDimension)
}

func nullMask(v any) []bool {
	els := reflect.ValueOf(v).FieldByName("Elements")
	out := make([]bool, els.Len())
	for i := 0; i < els.Len(); i++ {
		out[i] = els.Index(i).IsNil()
	}
	return out
}

func leafType(v any) reflect.Type {
	// pgtype.Array[*T] -> *T -> T
	return reflect.TypeOf(v).Field(0).Type.Elem().Elem()
}

func TestConvertArrayPerFamilyLeafAndDims(t *testing.T) {
	// IR Go value forms as decodeValue produces them per family:
	//   int64 / float64 / bool / string / time.Time.
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := []struct {
		name string
		elem ir.Type
		one  any // a single representative non-nil element value
		leaf reflect.Type
	}{
		{"integer", ir.Integer{Width: 32}, int64(7), reflect.TypeOf(int64(0))},
		{"float", ir.Float{}, float64(1.5), reflect.TypeOf(float64(0))},
		{"boolean", ir.Boolean{}, true, reflect.TypeOf(false)},
		{"text", ir.Text{Size: ir.TextLong}, "a", reflect.TypeOf(pgtype.Text{})},
		{"varchar", ir.Varchar{Length: 10}, "a", reflect.TypeOf(pgtype.Text{})},
		{"char", ir.Char{Length: 3}, "abc", reflect.TypeOf(pgtype.Text{})},
		{"uuid", ir.UUID{}, "00000000-0000-0000-0000-000000000001", reflect.TypeOf(pgtype.Text{})},
		{"inet", ir.Inet{}, "10.0.0.1", reflect.TypeOf(pgtype.Text{})},
		{"cidr", ir.Cidr{}, "10.0.0.0/24", reflect.TypeOf(pgtype.Text{})},
		{"macaddr", ir.Macaddr{}, "08:00:2b:01:02:03", reflect.TypeOf(pgtype.Text{})},
		{"decimal", ir.Decimal{}, "1.5", reflect.TypeOf(pgtype.Numeric{})},
		{"date", ir.Date{}, ts, reflect.TypeOf(pgtype.Date{})},
		{"datetime", ir.DateTime{}, ts, reflect.TypeOf(pgtype.Timestamp{})},
		{"timestamp", ir.Timestamp{}, ts, reflect.TypeOf(pgtype.Timestamp{})},
		{"timestamptz", ir.Timestamp{WithTimeZone: true}, ts, reflect.TypeOf(pgtype.Timestamptz{})},
		{"time", ir.Time{}, "01:02:03", reflect.TypeOf(pgtype.Time{})},
		{"time-frac", ir.Time{}, "23:59:59.123456", reflect.TypeOf(pgtype.Time{})},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			// 1-D with a NULL element: [v, nil, v]
			got1, err := convertArray([]any{c.one, nil, c.one}, c.elem)
			if err != nil {
				t.Fatalf("1-D: unexpected error: %v", err)
			}
			if lt := leafType(got1); lt != c.leaf {
				t.Fatalf("1-D leaf type = %v, want %v", lt, c.leaf)
			}
			if d := dimsOf(got1); len(d) != 1 || d[0].Length != 3 {
				t.Fatalf("1-D dims = %+v, want [{3,1}]", d)
			}
			if m := nullMask(got1); !reflect.DeepEqual(m, []bool{false, true, false}) {
				t.Fatalf("1-D null mask = %v, want [false true false]", m)
			}

			// multi-dim 2x2 with a NULL element.
			got2, err := convertArray(
				[]any{[]any{c.one, nil}, []any{c.one, c.one}}, c.elem,
			)
			if err != nil {
				t.Fatalf("2-D: unexpected error: %v", err)
			}
			if lt := leafType(got2); lt != c.leaf {
				t.Fatalf("2-D leaf type = %v, want %v", lt, c.leaf)
			}
			d := dimsOf(got2)
			if len(d) != 2 || d[0].Length != 2 || d[1].Length != 2 {
				t.Fatalf("2-D dims = %+v, want [{2,1},{2,1}]", d)
			}
			if m := nullMask(got2); !reflect.DeepEqual(m, []bool{false, true, false, false}) {
				t.Fatalf("2-D null mask = %v, want [false true false false]", m)
			}
		})
	}
}

// TestConvertArrayTimetzLoudRefuse pins the loud-failure decision for
// timetz arrays (catalog Bug 73/74 boundary): no faithful binary array
// leaf exists, so convertArray must refuse loudly rather than silently
// flatten/corrupt. A refused migration beats a silently corrupted one.
func TestConvertArrayTimetzLoudRefuse(t *testing.T) {
	_, err := convertArray([]any{"01:02:03+05"}, ir.Time{WithTimeZone: true})
	if err == nil {
		t.Fatal("expected loud refusal for timetz array; got nil")
	}
	if !strings.Contains(err.Error(), "timetz") {
		t.Errorf("error %q should name timetz", err)
	}
}

// TestTimeOfDayMicros pins the IR time-of-day string → microseconds
// conversion (the pgtype.Time leaf unit). Sub-second precision is
// right-padded to microseconds; PG `time` resolution is microsecond.
func TestTimeOfDayMicros(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"00:00:00", 0},
		{"01:02:03", 1*3_600_000_000 + 2*60_000_000 + 3*1_000_000},
		{"23:59:59.123456", 23*3_600_000_000 + 59*60_000_000 + 59*1_000_000 + 123456},
		{"12:00:00.5", 12*3_600_000_000 + 500000},
		{"12:00:00.123456789", 12*3_600_000_000 + 123456}, // truncated to µs
	}
	for _, c := range cases {
		got, err := timeOfDayMicros(c.in)
		if err != nil {
			t.Fatalf("%q: unexpected error: %v", c.in, err)
		}
		if got != c.want {
			t.Errorf("%q -> %d, want %d", c.in, got, c.want)
		}
	}
	if _, err := timeOfDayMicros("not-a-time"); err == nil {
		t.Error("expected error for malformed time-of-day")
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
