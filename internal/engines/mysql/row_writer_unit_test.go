// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"reflect"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

func TestBuildRowPlaceholder(t *testing.T) {
	cases := []struct {
		cols int
		want string
	}{
		{0, "()"},
		{1, "(?)"},
		{2, "(?, ?)"},
		{3, "(?, ?, ?)"},
		{5, "(?, ?, ?, ?, ?)"},
	}
	for _, c := range cases {
		if got := buildRowPlaceholder(c.cols); got != c.want {
			t.Errorf("buildRowPlaceholder(%d) = %q; want %q", c.cols, got, c.want)
		}
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
		{1, "INSERT INTO `users` (`id`, `email`) VALUES (?, ?)"},
		{3, "INSERT INTO `users` (`id`, `email`) VALUES (?, ?), (?, ?), (?, ?)"},
	}
	for _, c := range cases {
		got := buildBatchInsert(table, c.rows)
		if got != c.want {
			t.Errorf("buildBatchInsert(%d):\n got  %q\n want %q", c.rows, got, c.want)
		}
	}
}

func TestBuildBatchInsertEscapesIdentifiers(t *testing.T) {
	table := &ir.Table{
		Name: "weird`table",
		Columns: []*ir.Column{
			{Name: "weird`col", Type: ir.Integer{Width: 32}},
		},
	}
	got := buildBatchInsert(table, 1)
	want := "INSERT INTO `weird``table` (`weird``col`) VALUES (?)"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

func TestFlattenArgs(t *testing.T) {
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "a", Type: ir.Integer{Width: 32}},
			{Name: "b", Type: ir.Varchar{Length: 32}},
		},
	}
	batch := []ir.Row{
		{"a": int64(1), "b": "first"},
		{"a": int64(2), "b": "second"},
	}
	got := flattenArgs(batch, table)
	want := []any{int64(1), "first", int64(2), "second"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("flattenArgs:\n got  %#v\n want %#v", got, want)
	}
}

func TestFlattenArgsMissingValueIsNil(t *testing.T) {
	// A row that omits a column: the missing key looks up to nil
	// (the zero value of any), which is what the driver expects for
	// SQL NULL.
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "a", Type: ir.Integer{Width: 32}},
			{Name: "b", Type: ir.Varchar{Length: 32}},
		},
	}
	batch := []ir.Row{
		{"a": int64(1)}, // b is omitted
	}
	got := flattenArgs(batch, table)
	want := []any{int64(1), nil}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("flattenArgs (missing value):\n got  %#v\n want %#v", got, want)
	}
}

func TestPrepareValue(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		in   any
		t    ir.Type
		want any
		// src is optional: when non-nil, the test wraps it in
		// Column.SourceColumnType to simulate a translate.ApplyMappings
		// override having fired on the column. Tests that don't set
		// `src` exercise the no-override default path (the common case).
		src ir.Type
	}{
		// Pass-through types — driver handles them natively.
		{name: "nil", in: nil, t: ir.Integer{Width: 32}, want: nil},
		{name: "bool true", in: true, t: ir.Boolean{}, want: true},
		{name: "int64", in: int64(42), t: ir.Integer{Width: 32}, want: int64(42)},
		{name: "uint64", in: uint64(1 << 63), t: ir.Integer{Width: 64, Unsigned: true}, want: uint64(1 << 63)},
		{name: "float64", in: 3.14, t: ir.Float{Precision: ir.FloatDouble}, want: 3.14},
		{name: "string", in: "hello", t: ir.Varchar{Length: 32}, want: "hello"},
		{name: "bytes", in: []byte{0xde, 0xad}, t: ir.Blob{Size: ir.BlobRegular}, want: []byte{0xde, 0xad}},
		{name: "time", in: now, t: ir.Timestamp{Precision: 0, WithTimeZone: true}, want: now},
		{name: "decimal as string", in: "19.95", t: ir.Decimal{Precision: 10, Scale: 2}, want: "19.95"},

		// Special case: Set's canonical []string becomes a comma-joined string.
		{name: "set with members", in: []string{"a", "b", "c"}, t: ir.Set{Values: []string{"a", "b", "c", "d"}}, want: "a,b,c"},
		{name: "set empty", in: []string{}, t: ir.Set{Values: []string{"a"}}, want: ""},

		// JSON []byte → string. Without this conversion the driver
		// labels the parameter with the _binary charset prefix on
		// the wire and Vitess rejects with "Cannot create a JSON
		// value from a string with CHARACTER SET 'binary'". Real-
		// world failure surfaced during PlanetScale-target
		// integration testing.
		{name: "json bytes → string", in: []byte(`{"k":"v"}`), t: ir.JSON{Binary: true}, want: `{"k":"v"}`},
		{name: "json textual → string", in: []byte(`["a",1]`), t: ir.JSON{Binary: false}, want: `["a",1]`},
		{name: "json string passthrough", in: `{"k":"v"}`, t: ir.JSON{Binary: true}, want: `{"k":"v"}`},

		// A non-Set column receiving []string passes through unchanged
		// — the driver would error, which is what we want when the
		// caller has a type confusion bug.
		{name: "unexpected []string", in: []string{"x"}, t: ir.Varchar{Length: 32}, want: []string{"x"}},

		// Bug 14: PG array values landing on a MySQL JSON column
		// (operator-supplied --type-override=col=jsonb) get
		// re-encoded as a JSON array string the MySQL JSON parser
		// accepts. Without this branch the driver hands the
		// driver-incompatible Go slice or the PG-array literal
		// straight through, and MySQL rejects with "Invalid JSON
		// text".
		{
			name: "json target with []any → JSON array",
			in:   []any{int64(1), int64(2), int64(3)},
			t:    ir.JSON{Binary: true},
			want: `[1,2,3]`,
		},
		{
			name: "json target with []any of strings",
			in:   []any{"a", "b", "c"},
			t:    ir.JSON{Binary: true},
			want: `["a","b","c"]`,
		},
		{
			name: "json target with PG-array-literal string",
			in:   "{a,b,c}",
			t:    ir.JSON{Binary: true},
			want: `["a","b","c"]`,
		},
		{
			name: "json target with quoted PG-array elements",
			in:   `{"alpha","beta","with,comma"}`,
			t:    ir.JSON{Binary: true},
			want: `["alpha","beta","with,comma"]`,
		},
		{
			name: "json target with PG-array NULL element",
			in:   "{a,NULL,c}",
			t:    ir.JSON{Binary: true},
			want: `["a",null,"c"]`,
		},
		{
			name: "json target with empty PG-array literal",
			in:   "{}",
			t:    ir.JSON{Binary: true},
			want: `[]`,
		},
		{
			name: "json target with []any containing nil",
			in:   []any{int64(1), nil, int64(3)},
			t:    ir.JSON{Binary: true},
			want: `[1,null,3]`,
		},

		// Bug 47: MySQL JSON source value `{}` arrives as []byte and
		// must round-trip as the JSON object `{}`. The column has no
		// `SourceColumnType` (no override fired) so the disambiguator
		// in convertArrayLikeToJSON's []byte branch returns the bytes
		// through the JSON-pass branch as the string "{}". This was
		// silently corrupting to `[]` pre-fix.
		{
			name: "Bug 47: empty JSON object bytes preserved (no override)",
			in:   []byte(`{}`),
			t:    ir.JSON{Binary: true},
			want: `{}`,
		},
		// Bug 47 explicit-JSON-source variant: even when an override
		// fires, if the pre-override type was already JSON, the empty
		// object bytes should still preserve as `{}` (an override from
		// JSON to JSON is a no-op shape change but the SourceColumnType
		// is set).
		{
			name: "Bug 47: empty JSON object bytes preserved (JSON→JSON override)",
			in:   []byte(`{}`),
			t:    ir.JSON{Binary: true},
			src:  ir.JSON{Binary: false},
			want: `{}`,
		},
		// Bug 14 closure: empty PG `text[]` value with the `jsonb`
		// override fires must land as the JSON array `[]`. The Column
		// carries `SourceColumnType = ir.Array{...}` because
		// translate.ApplyMappings recorded the pre-override type.
		{
			name: "Bug 14: empty PG array bytes with array→JSON override → []",
			in:   []byte(`{}`),
			t:    ir.JSON{Binary: true},
			src:  ir.Array{Element: ir.Text{Size: ir.TextLong}},
			want: `[]`,
		},
		// Bug 14 closure (non-empty): a populated PG array literal
		// from an array-typed source still parses through to a JSON
		// array. This shape was already working pre-Bug-47; the test
		// pins the existing behavior across the signature change.
		{
			name: "Bug 14: non-empty PG-array bytes with array→JSON override → JSON array",
			in:   []byte(`{a,b,c}`),
			t:    ir.JSON{Binary: true},
			src:  ir.Array{Element: ir.Text{Size: ir.TextLong}},
			want: `["a","b","c"]`,
		},
		// Without an array source override, populated `{a,b,c}` bytes
		// still parse as a PG array (MySQL JSON sources never produce
		// non-quoted-key shapes; the parse-succeeds-when-not-JSON
		// heuristic is high signal). Pins existing behavior so a future
		// edit doesn't regress non-empty heuristic.
		{
			name: "non-empty `{a,b,c}` bytes parse as PG array even without override",
			in:   []byte(`{a,b,c}`),
			t:    ir.JSON{Binary: true},
			want: `["a","b","c"]`,
		},
		// Populated JSON object bytes pass through untouched — the
		// PG-array parse fails on the colon/quoted-key shape and the
		// next branch emits the bytes as a string.
		{
			name: "populated JSON object as bytes passes through",
			in:   []byte(`{"k":"v"}`),
			t:    ir.JSON{Binary: true},
			want: `{"k":"v"}`,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			col := &ir.Column{Name: "c", Type: c.t, SourceColumnType: c.src}
			got := prepareValue(c.in, col)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("prepareValue(%#v, %T) = %#v; want %#v", c.in, c.t, got, c.want)
			}
		})
	}
}

// TestPrepareValue_PGArrayLiteralOnNonJSONPasses through unchanged —
// the array→JSON transformation is gated on the target being JSON.
// A `--type-override=col=text` (the documented Bug 14 workaround) on
// a PG-array column should land the literal as a string, not be
// re-shaped.
func TestPrepareValue_PGArrayLiteralOnNonJSONPassesThrough(t *testing.T) {
	col := &ir.Column{Name: "c", Type: ir.Text{Size: ir.TextLong}}
	got := prepareValue("{a,b,c}", col)
	if got != "{a,b,c}" {
		t.Errorf("prepareValue text-target: got %#v; want \"{a,b,c}\"", got)
	}
}

// TestPrepareValue_AnySliceOnNonJSONPassesThrough — the conversion
// is intentionally narrow. A []any landing on a non-JSON column is
// a caller bug, but it shouldn't silently morph; the driver's own
// error surface should report the type mismatch.
func TestPrepareValue_AnySliceOnNonJSONPassesThrough(t *testing.T) {
	in := []any{int64(1), int64(2)}
	col := &ir.Column{Name: "c", Type: ir.Varchar{Length: 32}}
	got := prepareValue(in, col)
	if !reflect.DeepEqual(got, in) {
		t.Errorf("prepareValue varchar-target: got %#v; want %#v", got, in)
	}
}
