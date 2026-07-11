// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
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
	got := mustFlattenArgs(t, batch, table)
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
	got := mustFlattenArgs(t, batch, table)
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
			got := mustPrepareValue(t, c.in, col)
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
	got := mustPrepareValue(t, "{a,b,c}", col)
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
	got := mustPrepareValue(t, in, col)
	if !reflect.DeepEqual(got, in) {
		t.Errorf("prepareValue varchar-target: got %#v; want %#v", got, in)
	}
}

// TestPrepareValue_PGArrayColumnToJSON pins Bug 18: a PG array source
// column (text[]/int[]/…) lands on a MySQL column whose IR type is
// [ir.Array] (NOT rewritten to [ir.JSON] — only ddl_emit's
// emitColumnType renders ir.Array as MySQL `JSON`). The PG RowReader
// yields the array as a Go []any (string/int64/nil elements, possibly
// nested). prepareValue must serialize it to the JSON text form the
// MySQL JSON column accepts via LOAD DATA — pre-fix the []any fell
// through untouched and crashed tsvEncode with
// "unsupported value type []interface {}".
func TestPrepareValue_PGArrayColumnToJSON(t *testing.T) {
	cases := []struct {
		name string
		in   any
		t    ir.Type
		want any
	}{
		{
			name: "text[] {x,y} → JSON string array",
			in:   []any{"x", "y"},
			t:    ir.Array{Element: ir.Text{Size: ir.TextLong}},
			want: `["x","y"]`,
		},
		{
			name: "int[] {1,2} → JSON int array",
			in:   []any{int64(1), int64(2)},
			t:    ir.Array{Element: ir.Integer{Width: 32}},
			want: `[1,2]`,
		},
		{
			name: "empty array {} → []",
			in:   []any{},
			t:    ir.Array{Element: ir.Integer{Width: 32}},
			want: `[]`,
		},
		{
			name: "NULL whole-array column → nil (NULL, not [])",
			in:   nil,
			t:    ir.Array{Element: ir.Text{Size: ir.TextLong}},
			want: nil,
		},
		{
			name: "NULL element inside array → JSON null",
			in:   []any{"a", nil, "c"},
			t:    ir.Array{Element: ir.Text{Size: ir.TextLong}},
			want: `["a",null,"c"]`,
		},
		{
			name: "non-ASCII / needs-escaping text element JSON-escaped",
			in:   []any{`a"b`, "c\td", "é"},
			t:    ir.Array{Element: ir.Text{Size: ir.TextLong}},
			want: `["a\"b","c\td","é"]`,
		},
		{
			name: "nested int[][] → nested JSON array",
			in:   []any{[]any{int64(1), int64(2)}, []any{int64(3)}},
			t:    ir.Array{Element: ir.Array{Element: ir.Integer{Width: 32}}},
			want: `[[1,2],[3]]`,
		},
		{
			// Bug 68 exact repro values (PG int[][] {{1,2},{3,4}}
			// decoded by the postgres reader to nested []any) → the
			// faithful nested MySQL JSON the integration test asserts.
			name: "Bug 68: int[][] {{1,2},{3,4}} → [[1,2],[3,4]]",
			in:   []any{[]any{int64(1), int64(2)}, []any{int64(3), int64(4)}},
			t:    ir.Array{Element: ir.Integer{Width: 32}},
			want: `[[1,2],[3,4]]`,
		},
		{
			name: "Bug 68: int[][] with NULL element → [[1,null],[null,4]]",
			in:   []any{[]any{int64(1), nil}, []any{nil, int64(4)}},
			t:    ir.Array{Element: ir.Integer{Width: 32}},
			want: `[[1,null],[null,4]]`,
		},
		{
			name: "PG array text literal on ir.Array column → JSON array",
			in:   "{a,b,c}",
			t:    ir.Array{Element: ir.Text{Size: ir.TextLong}},
			want: `["a","b","c"]`,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			col := &ir.Column{Name: "c", Type: c.t}
			got := mustPrepareValue(t, c.in, col)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("prepareValue(%#v, %s) = %#v; want %#v", c.in, c.t, got, c.want)
			}
		})
	}
}

// TestPrepareValue_PGTriggerCDCValueFamilies is the task #72 / Bug-74
// value-family pin at the value-prepare layer. The postgres-trigger CDC
// reader (internal/engines/pgtrigger/cdc_reader.go decodeJSONBRow) decodes
// the JSONB capture log into a DIFFERENT value shape than the proven
// pgoutput path's value_decode.go:
//
//	integers              -> int64        (same as pgoutput)
//	non-integer numerics  -> json.Number  (pgoutput: string)
//	bytea                 -> `\x`-hex TEXT string  (pgoutput: raw []byte)
//	timestamp             -> ISO string   (pgoutput: time.Time)
//	timestamptz           -> ISO+offset string (pgoutput: time.Time)
//	jsonb                 -> map[string]any / []any (pgoutput: []byte)
//	bool                  -> bool         (same)
//
// Those shapes flow straight into prepareValue when the trigger source
// targets MySQL cross-engine. This test pins that EVERY family lands as a
// driver-bindable, byte-correct value — the cross-engine differential
// integration test asserts the same end-to-end, but this keeps the unit
// pin close to the code so a regression names the family.
//
// Per CLAUDE.md "pin the class, not the representative": this exercises
// every family + the danger shapes (bytea silent corruption, jsonb map
// LOUD failure, timestamptz offset rejection), not one representative.
func TestPrepareValue_PGTriggerCDCValueFamilies(t *testing.T) {
	cases := []struct {
		name string
		in   any
		t    ir.Type
		want any
	}{
		// int4 / int8: trigger reader yields int64 — pass through.
		{name: "int4 int64", in: int64(147), t: ir.Integer{Width: 32}, want: int64(147)},
		{name: "int8 int64", in: int64(21), t: ir.Integer{Width: 64}, want: int64(21)},

		// numeric(30,12): trigger reader yields json.Number. json.Number
		// is a string kind; the driver binds it as the exact numeric text,
		// which MySQL DECIMAL parses without precision loss. We pin that
		// prepareValue passes it through unchanged (it is already correct).
		{
			name: "numeric json.Number passes through (binds as numeric string)",
			in:   json.Number("99999.999999999999"),
			t:    ir.Decimal{Precision: 30, Scale: 12},
			want: json.Number("99999.999999999999"),
		},

		// text / varchar: trigger reader yields a Go string — pass through.
		{name: "text string", in: "multi-col-update-中", t: ir.Text{Size: ir.TextLong}, want: "multi-col-update-中"},
		{name: "varchar string", in: "CODE-0021", t: ir.Varchar{Length: 32}, want: "CODE-0021"},

		// boolean: trigger reader yields Go bool — pass through.
		{name: "bool true", in: true, t: ir.Boolean{}, want: true},
		{name: "bool false", in: false, t: ir.Boolean{}, want: false},

		// timestamp (PG `timestamp` -> MySQL DATETIME): ISO string, no
		// offset — passes through; stripTimeZoneOffset is a no-op.
		{
			name: "timestamp ISO string (no offset)",
			in:   "2026-02-02 02:02:02.020202",
			t:    ir.DateTime{Precision: 6},
			want: "2026-02-02 02:02:02.020202",
		},

		// timestamptz (PG `timestamptz` -> MySQL TIMESTAMP): a postgres-trigger
		// timestamptz arrives as an offset-bearing ISO string. PROM-M1: the
		// offset encodes the INSTANT, so prepareValue now converts it to a UTC
		// time.Time (matching the bulk-copy path) rather than stripping to the
		// source-session wall clock. Because the result is a time.Time — not a
		// string — the instant-conversion cases live in
		// TestPrepareValue_TimestamptzInstantConversion (which compares by
		// instant, not reflect.DeepEqual on a time.Time's internal fields);
		// plain-timestamp stripping and time.Time passthrough are pinned in the
		// sibling tests in row_writer_timestamptz_test.go.

		// bytea (PG `bytea` -> MySQL LONGBLOB): the trigger reader emits
		// the `\x`-hex TEXT form. Binding it as a string stores the literal
		// ASCII of the hex text (SILENT corruption, Bug-92 class). The fix
		// hex-decodes to raw bytes. THE most dangerous family.
		{
			name: "bytea \\x-hex string → raw bytes (Blob)",
			in:   `\xdeadbeef`,
			t:    ir.Blob{Size: ir.BlobLong},
			want: []byte{0xde, 0xad, 0xbe, 0xef},
		},
		{
			name: "bytea \\x-hex string → raw bytes (Varbinary)",
			in:   `\x010203`,
			t:    ir.Varbinary{Length: 16},
			want: []byte{0x01, 0x02, 0x03},
		},
		{
			name: "bytea raw []byte passes through (same-engine / pgoutput shape)",
			in:   []byte{0xca, 0xfe},
			t:    ir.Blob{Size: ir.BlobLong},
			want: []byte{0xca, 0xfe},
		},

		// jsonb (PG `jsonb` -> MySQL JSON): the trigger reader yields a
		// nested map[string]any (object) or []any (top-level array). A bare
		// map reaches the driver as reflect.Map → "unsupported type map"
		// (LOUD failure). The fix marshals it. json.Number leaves preserve
		// exact numeric precision inside the document.
		{
			name: "jsonb map[string]any → JSON object string",
			in:   map[string]any{"k": int64(7), "ok": true},
			t:    ir.JSON{Binary: true},
			want: `{"k":7,"ok":true}`,
		},
		{
			name: "jsonb map with json.Number leaf preserves precision",
			in:   map[string]any{"ratio": json.Number("7.777777777")},
			t:    ir.JSON{Binary: true},
			want: `{"ratio":7.777777777}`,
		},
		{
			name: "jsonb top-level array []any → JSON array string",
			in:   []any{"u", "p", "d"},
			t:    ir.JSON{Binary: true},
			want: `["u","p","d"]`,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			col := &ir.Column{Name: "c", Type: c.t}
			got := mustPrepareValue(t, c.in, col)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("prepareValue(%#v, %T) = %#v (%T); want %#v (%T)",
					c.in, c.t, got, got, c.want, c.want)
			}
		})
	}
}

// TestDecodeHexByteaText pins the helper directly: only `\x`-prefixed,
// even-length, valid-hex strings decode; everything else reports false so
// the caller passes the value through unchanged (the same-engine / non-PG
// shape is untouched).
func TestDecodeHexByteaText(t *testing.T) {
	cases := []struct {
		in     string
		want   []byte
		wantOK bool
	}{
		{`\xdeadbeef`, []byte{0xde, 0xad, 0xbe, 0xef}, true},
		{`\x`, []byte{}, true}, // empty bytea
		{`\x00`, []byte{0x00}, true},
		{`deadbeef`, nil, false},    // no prefix
		{`\xabc`, nil, false},       // odd length
		{`\xZZ`, nil, false},        // not hex
		{`plain text`, nil, false},  // ordinary string
		{`\xdead beef`, nil, false}, // space → invalid hex
	}
	for _, c := range cases {
		got, ok := decodeHexByteaText(c.in)
		if ok != c.wantOK {
			t.Errorf("decodeHexByteaText(%q) ok = %v; want %v", c.in, ok, c.wantOK)
			continue
		}
		if ok && !reflect.DeepEqual(got, c.want) {
			t.Errorf("decodeHexByteaText(%q) = %#v; want %#v", c.in, got, c.want)
		}
	}
}

// TestPrepareValue_HstoreToJSON pins the ADR-0032 cross-engine value
// translator: a PG hstore source column with `col.Type =
// ir.ExtensionType{Extension: "hstore"}` gets its PG-canonical text
// form reparsed into a JSON object string the MySQL JSON column
// accepts. The IR Type is the unchanged source-engine ExtensionType
// — the writer rewrites at emit time (mysql/ddl_emit.go) and again
// at value-write time (here).
func TestPrepareValue_HstoreToJSON(t *testing.T) {
	hstoreCol := &ir.Column{
		Name: "tags",
		Type: ir.ExtensionType{Extension: "hstore", Name: "hstore"},
	}
	cases := []struct {
		name string
		in   any
		want any
	}{
		{
			name: "simple two-pair hstore",
			in:   `"a"=>"1", "b"=>"2"`,
			want: `{"a":"1","b":"2"}`,
		},
		{
			name: "single-pair hstore",
			in:   `"key"=>"value"`,
			want: `{"key":"value"}`,
		},
		{
			name: "empty hstore",
			in:   "",
			want: "{}",
		},
		{
			name: "hstore from bytes",
			in:   []byte(`"x"=>"y"`),
			want: `{"x":"y"}`,
		},
		{
			name: "hstore with NULL value",
			in:   `"a"=>"1", "b"=>NULL`,
			want: `{"a":"1","b":null}`,
		},
		{
			name: "hstore with escaped quote in value",
			in:   `"k"=>"he said \"hi\""`,
			want: `{"k":"he said \"hi\""}`,
		},
		{
			name: "nil passes through as nil",
			in:   nil,
			want: nil,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := mustPrepareValue(t, c.in, hstoreCol)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("prepareValue(%#v, hstore) = %#v; want %#v", c.in, got, c.want)
			}
		})
	}
}

// TestPrepareValue_CiTextIdentity pins the citext path — the value
// passes through as a Go string (no encoding conversion). citext is
// `text` with a custom collation; the case-insensitive comparison is
// a server-side property of the column collation, not a wire-format
// concern.
func TestPrepareValue_CiTextIdentity(t *testing.T) {
	citextCol := &ir.Column{
		Name: "email",
		Type: ir.ExtensionType{Extension: "citext", Name: "citext"},
	}
	cases := []struct {
		name string
		in   any
		want any
	}{
		{"plain string", "Alice@Example.com", "Alice@Example.com"},
		{"bytes to string", []byte("Bob@Example.com"), "Bob@Example.com"},
		{"empty string", "", ""},
		{"nil passes through", nil, nil},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := mustPrepareValue(t, c.in, citextCol)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("prepareValue(%#v, citext) = %#v; want %#v", c.in, got, c.want)
			}
		})
	}
}

// TestParseHstoreText exercises the PG-hstore text-form parser
// against the canonical shapes PG produces. Each input mirrors the
// `\d hstore_output` cases from PG's regression tests, minus the
// brace-wrapped envelope (the parser handles bare hstore form;
// arrays of hstore are a separate concern).
func TestParseHstoreText(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want map[string]any
	}{
		{
			name: "empty",
			in:   "",
			want: map[string]any{},
		},
		{
			name: "single pair",
			in:   `"a"=>"1"`,
			want: map[string]any{"a": "1"},
		},
		{
			name: "two pairs comma-separated",
			in:   `"a"=>"1", "b"=>"2"`,
			want: map[string]any{"a": "1", "b": "2"},
		},
		{
			name: "value with embedded spaces",
			in:   `"name"=>"John Doe"`,
			want: map[string]any{"name": "John Doe"},
		},
		{
			name: "NULL value",
			in:   `"a"=>NULL`,
			want: map[string]any{"a": nil},
		},
		{
			name: "lowercase null value",
			in:   `"a"=>null`,
			want: map[string]any{"a": nil},
		},
		{
			name: "escaped quote in value",
			in:   `"k"=>"a\"b"`,
			want: map[string]any{"k": `a"b`},
		},
		{
			name: "escaped backslash in value",
			in:   `"k"=>"a\\b"`,
			want: map[string]any{"k": `a\b`},
		},
		{
			name: "mixed NULL and value",
			in:   `"x"=>"1", "y"=>NULL, "z"=>"3"`,
			want: map[string]any{"x": "1", "y": nil, "z": "3"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := parseHstoreText(c.in)
			if err != nil {
				t.Fatalf("parseHstoreText(%q): %v", c.in, err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("parseHstoreText(%q) = %#v; want %#v", c.in, got, c.want)
			}
		})
	}
}

// TestParseHstoreText_Errors pins the loud-failure shape for
// malformed input. The MySQL writer's prepareValue catches parse
// errors and passes the bytes through unchanged — the driver then
// raises its own "Invalid JSON text" error citing the row's value.
func TestParseHstoreText_Errors(t *testing.T) {
	cases := []string{
		`unquoted_key=>"v"`,    // bare-word key
		`"key"->"v"`,           // wrong arrow
		`"key"=>missing_quote`, // unquoted value not NULL
		`"k"=>"unterminated`,   // unterminated quoted string
		`"a"=>"1" "b"=>"2"`,    // missing comma separator
	}
	for _, in := range cases {
		in := in
		t.Run(in, func(t *testing.T) {
			if _, err := parseHstoreText(in); err == nil {
				t.Errorf("parseHstoreText(%q) = nil; want error", in)
			}
		})
	}
}

// mustPrepareValue unwraps prepareValue's error return for the shaping
// tests above — none of their corpora trip the
// SLUICE-E-VALUE-UNREPRESENTABLE float guard (that refusal has its own
// pins in TestPrepareValue_RefusesNaNAndInf).
func mustPrepareValue(t *testing.T, v any, col *ir.Column) any {
	t.Helper()
	got, err := prepareValue(v, col)
	if err != nil {
		t.Fatalf("prepareValue(%#v): unexpected refusal: %v", v, err)
	}
	return got
}

// mustFlattenArgs is the flattenArgs sibling of mustPrepareValue.
func mustFlattenArgs(t *testing.T, batch []ir.Row, table *ir.Table) []any {
	t.Helper()
	got, err := flattenArgs(batch, table)
	if err != nil {
		t.Fatalf("flattenArgs: unexpected refusal: %v", err)
	}
	return got
}

// TestPrepareValue_RefusesNaNAndInf pins the SLUICE-E-VALUE-UNREPRESENTABLE
// guard (ADR-0153): float64 NaN / ±Inf are refused by prepareValue BEFORE
// the driver sees them — on both statement protocols the server's own
// failure shape is misleading (interpolation renders a bare `NaN` literal
// that draws Error 1054, the code the Bug-F8 schema-drift self-healing
// deliberately RETRIES — an unguarded NaN would spin the retry window
// instead of failing loudly). The refusal names the column and the code.
func TestPrepareValue_RefusesNaNAndInf(t *testing.T) {
	col := &ir.Column{Name: "dbl", Type: ir.Float{Precision: ir.FloatDouble}}
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := prepareValue(v, col); err == nil {
			t.Errorf("prepareValue(%v) = nil error; want SLUICE-E-VALUE-UNREPRESENTABLE refusal", v)
		} else if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeValueUnrepresentable {
			t.Errorf("prepareValue(%v) error %q; want a CodedError with SLUICE-E-VALUE-UNREPRESENTABLE (DEVEX-D2)", v, err)
		} else if !strings.Contains(err.Error(), `"dbl"`) {
			t.Errorf("prepareValue(%v) error %q; want the refusal to name column dbl", v, err)
		}
		// The guard fires even with a nil column descriptor (defensive
		// applier path) — the value is unrepresentable regardless.
		if _, err := prepareValue(v, nil); err == nil {
			t.Errorf("prepareValue(%v, nil col) = nil error; want refusal", v)
		}
		// And through flattenArgs, naming the batch row.
		table := &ir.Table{Name: "t", Columns: []*ir.Column{col}}
		if _, err := flattenArgs([]ir.Row{{"dbl": 1.5}, {"dbl": v}}, table); err == nil {
			t.Errorf("flattenArgs with %v = nil error; want refusal", v)
		} else if !strings.Contains(err.Error(), "row 2") {
			t.Errorf("flattenArgs refusal %q; want it to name row 2 of the batch", err)
		}
	}
	// Ordinary extremes pass: the guard is NaN/Inf-only.
	for _, v := range []float64{math.MaxFloat64, -math.MaxFloat64, math.SmallestNonzeroFloat64, math.Copysign(0, -1)} {
		if _, err := prepareValue(v, col); err != nil {
			t.Errorf("prepareValue(%v) refused a representable float: %v", v, err)
		}
	}
}

// TestPrepareValue_NegZeroTypeFreeOnNilColumn pins the nil-descriptor leg
// of the ADR-0153 negative-zero wart (round-2 review G3): the applier's
// cache-miss/unknown-column branches call prepareValue with no column
// type, where the ir.Float-gated encoding can't fire — the type-free
// branch must still encode −0.0 as the string '-0' so an interpolated
// statement can't mangle the sign to +0 while binary preserves it.
func TestPrepareValue_NegZeroTypeFreeOnNilColumn(t *testing.T) {
	neg := math.Copysign(0, -1)
	got, err := prepareValue(neg, nil)
	if err != nil {
		t.Fatalf("prepareValue(-0, nil): %v", err)
	}
	if s, ok := got.(string); !ok || s != "-0" {
		t.Errorf("prepareValue(-0, nil) = %#v (%T); want the type-free \"-0\" encoding", got, got)
	}
	// Through the applier's defensive branches too.
	if got, err = prepareApplierValue(neg, nil, "dbl"); err != nil || got != "-0" {
		t.Errorf("prepareApplierValue(-0, nil colTypes) = %#v, %v; want \"-0\"", got, err)
	}
	if got, err = prepareApplierValue(neg, map[string]*ir.Column{}, "dbl"); err != nil || got != "-0" {
		t.Errorf("prepareApplierValue(-0, unknown column) = %#v, %v; want \"-0\"", got, err)
	}
	// Positive zero and ordinary floats pass through untouched.
	for _, v := range []float64{0, 1.5, -1.5} {
		got, err := prepareValue(v, nil)
		if err != nil || got != v {
			t.Errorf("prepareValue(%v, nil) = %#v, %v; want passthrough", v, got, err)
		}
	}
}
