// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"net"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestDecodeValue(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 34, 56, 0, time.UTC)
	prefix := netip.MustParsePrefix("192.168.1.0/24")
	mac, _ := net.ParseMAC("08:00:2b:01:02:03")

	cases := []struct {
		name string
		raw  any
		t    ir.Type
		want any
	}{
		// ---- NULL ----
		{"null int", nil, ir.Integer{Width: 32}, nil},
		{"null array", nil, ir.Array{Element: ir.Integer{Width: 32}}, nil},

		// ---- Boolean ----
		{"bool true", true, ir.Boolean{}, true},
		{"bool false", false, ir.Boolean{}, false},

		// ---- Integer (widening) ----
		{"int16 → int64", int16(7), ir.Integer{Width: 16}, int64(7)},
		{"int32 → int64", int32(42), ir.Integer{Width: 32}, int64(42)},
		{"int64 passthrough", int64(99), ir.Integer{Width: 64}, int64(99)},

		// ---- Decimal ----
		{"decimal as string", "3.14159", ir.Decimal{Precision: 6, Scale: 5}, "3.14159"},
		{"decimal from bytes", []byte("19.95"), ir.Decimal{Precision: 8, Scale: 2}, "19.95"},

		// ---- Float ----
		{"float64 passthrough", 2.71828, ir.Float{Precision: ir.FloatDouble}, 2.71828},
		{"float32 widened", float32(1.5), ir.Float{Precision: ir.FloatSingle}, float64(1.5)},

		// ---- Strings ----
		{"varchar string", "hello", ir.Varchar{Length: 32}, "hello"},
		{"text string", "longer text", ir.Text{Size: ir.TextLong}, "longer text"},

		// ---- Bytes ----
		{"bytea bytes", []byte{0xde, 0xad}, ir.Blob{Size: ir.BlobLong}, []byte{0xde, 0xad}},

		// ---- Temporal ----
		{"timestamp passthrough", now, ir.Timestamp{Precision: 0, WithTimeZone: true}, now},
		{"date passthrough", now, ir.Date{}, now},
		{
			"time as string",
			time.Date(0, 1, 1, 8, 30, 0, 0, time.UTC),
			ir.Time{Precision: 0},
			"08:30:00",
		},
		// pgoutput CDC tuple values arrive as []byte in Postgres
		// canonical text form. The decoder is shared with the
		// row-reader path that gives us time.Time, so both shapes
		// must round-trip. (TIMESTAMPTZ parsing is exercised by the
		// integration test — the location pointer comparison here
		// is too brittle for a unit test.)
		{
			"timestamp from text bytes",
			[]byte("2026-05-01 12:34:56"),
			ir.DateTime{Precision: 0},
			time.Date(2026, 5, 1, 12, 34, 56, 0, time.UTC),
		},
		{
			"date from text bytes",
			[]byte("2026-05-01"),
			ir.Date{},
			time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		},

		// ---- JSON ----
		{"json bytes", []byte(`{"k":"v"}`), ir.JSON{Binary: true}, []byte(`{"k":"v"}`)},

		// ---- Enum ----
		{"enum string", "admin", ir.Enum{Values: []string{"admin", "user"}}, "admin"},

		// ---- UUID ----
		{
			"uuid [16]byte → string",
			[16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef},
			ir.UUID{},
			"01234567-89ab-cdef-0123-456789abcdef",
		},
		{"uuid string passthrough", "11111111-2222-3333-4444-555555555555", ir.UUID{}, "11111111-2222-3333-4444-555555555555"},
		// Bug 41 — pgoutput CDC tuple values arrive as []byte in
		// canonical text form (36 bytes); the decoder must accept
		// them and return the IR-canonical lowercased string.
		{
			"uuid 36-byte text []byte (pgoutput CDC)",
			[]byte("11111111-2222-3333-4444-555555555555"),
			ir.UUID{},
			"11111111-2222-3333-4444-555555555555",
		},
		{
			"uuid uppercase canonical text lowercased",
			"AABBCCDD-EEFF-0011-2233-445566778899",
			ir.UUID{},
			"aabbccdd-eeff-0011-2233-445566778899",
		},
		{
			"uuid mixed-case []byte lowercased",
			[]byte("AaBbCcDd-EeFf-0011-2233-445566778899"),
			ir.UUID{},
			"aabbccdd-eeff-0011-2233-445566778899",
		},

		// ---- Network types ----
		{"inet from netip.Prefix", prefix, ir.Inet{}, "192.168.1.0/24"},
		{"cidr from netip.Prefix", prefix, ir.Cidr{}, "192.168.1.0/24"},
		{"macaddr from net.HardwareAddr", mac, ir.Macaddr{}, "08:00:2b:01:02:03"},

		// ---- Arrays ----
		{
			"int32 array",
			[]int32{1, 2, 3},
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{int64(1), int64(2), int64(3)},
		},
		{
			"text array",
			[]string{"a", "b", "c"},
			ir.Array{Element: ir.Text{Size: ir.TextLong}},
			[]any{"a", "b", "c"},
		},
		{
			"any-typed array (pgx fast-path)",
			[]any{int64(7), int64(8)},
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{int64(7), int64(8)},
		},

		// ---- Array text form (pgx stdlib *any scan path) ----
		{
			"int array from text",
			"{10,20,30}",
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{int64(10), int64(20), int64(30)},
		},
		{
			"empty array from text",
			"{}",
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{},
		},
		{
			"int array with NULL",
			"{1,NULL,3}",
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{int64(1), nil, int64(3)},
		},
		{
			"text array from text",
			`{"alpha","beta","gamma"}`,
			ir.Array{Element: ir.Text{Size: ir.TextLong}},
			[]any{"alpha", "beta", "gamma"},
		},
		{
			"text array with embedded comma",
			`{"a, b","c"}`,
			ir.Array{Element: ir.Text{Size: ir.TextLong}},
			[]any{"a, b", "c"},
		},
		{
			"text array with escaped quote",
			`{"he said \"hi\"","plain"}`,
			ir.Array{Element: ir.Text{Size: ir.TextLong}},
			[]any{`he said "hi"`, "plain"},
		},
		{
			"bool array from text",
			"{t,f,t}",
			ir.Array{Element: ir.Boolean{}},
			[]any{true, false, true},
		},

		// ---- Multi-dimensional arrays (Bug 68) ----
		{
			"int[][] from text (rectangular 2x2)",
			"{{1,2},{3,4}}",
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{[]any{int64(1), int64(2)}, []any{int64(3), int64(4)}},
		},
		{
			"int[][] from text (single inner row)",
			"{{9,8}}",
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{[]any{int64(9), int64(8)}},
		},
		{
			"text[][] from text with quoting edges",
			`{{"a, b","c"},{"d\"e","NULL"}}`,
			ir.Array{Element: ir.Text{Size: ir.TextLong}},
			[]any{[]any{"a, b", "c"}, []any{`d"e`, "NULL"}},
		},
		{
			"int[][] with NULL element",
			"{{1,NULL},{NULL,4}}",
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{[]any{int64(1), nil}, []any{nil, int64(4)}},
		},
		{
			"int[][][] from text (3-D)",
			"{{{1,2}},{{3,4}}}",
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{[]any{[]any{int64(1), int64(2)}}, []any{[]any{int64(3), int64(4)}}},
		},

		// ---- Array []byte text form (Bug 144 — the pgoutput CDC path) ----
		// pgoutput delivers the array as its TEXT encoding in a []byte (the
		// cold-start path above yields []any or string). decodeArray case 3b
		// must route []byte through the SAME decodePGArrayText parser; without
		// it the reflect path walked the text's bytes and decoded each uint8 as
		// an element. These twins pin the []byte path on the edge cases (comma,
		// escaped quote, brace, backslash, 2-D, NULL-element, empty) so it
		// cannot drift from the string path.
		{
			"[]byte int array",
			[]byte("{10,20,30}"),
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{int64(10), int64(20), int64(30)},
		},
		{
			"[]byte text array with comma/quote/brace/backslash",
			[]byte(`{"a, b","he said \"hi\"","brace}{","back\\slash"}`),
			ir.Array{Element: ir.Text{Size: ir.TextLong}},
			[]any{"a, b", `he said "hi"`, "brace}{", `back\slash`},
		},
		{
			"[]byte int[][] (2-D not flattened)",
			[]byte("{{1,2},{3,4}}"),
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{[]any{int64(1), int64(2)}, []any{int64(3), int64(4)}},
		},
		{
			"[]byte int array with NULL element",
			[]byte("{1,NULL,3}"),
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{int64(1), nil, int64(3)},
		},
		{
			"[]byte empty array",
			[]byte("{}"),
			ir.Array{Element: ir.Integer{Width: 32}},
			[]any{},
		},

		// ---- Scalar string fallbacks ----
		{"int from numeric string", "42", ir.Integer{Width: 32}, int64(42)},
		{"float from numeric string", "3.14", ir.Float{Precision: ir.FloatDouble}, 3.14},
		{"bool from t", "t", ir.Boolean{}, true},
		{"bool from f", "f", ir.Boolean{}, false},

		// ---- Geometry (PostGIS) ----
		// pgx stdlib's `default:` branch hands an unknown-OID column
		// to us as a string in PostGIS text-format (EWKB-as-hex). The
		// decoder must hex-decode and strip the EWKB framing back to
		// raw WKB to match the IR contract.
		//
		// EWKB POINT(0 0) SRID=4326 little-endian:
		//   byte_order  = 01
		//   type|flag   = 01 00 00 20  (POINT | 0x20000000)
		//   srid        = E6 10 00 00  (4326 LE)
		//   x, y        = 16 bytes of zero (two LE float64 zeros)
		// -> raw WKB POINT(0 0) LE:
		//   byte_order  = 01
		//   type        = 01 00 00 00
		//   x, y        = 16 bytes of zero
		{
			"geometry hex string (pgx stdlib default)",
			"0101000020E6100000" + "0000000000000000" + "0000000000000000",
			ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326},
			[]byte{
				0x01,
				0x01, 0x00, 0x00, 0x00,
				0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0,
			},
		},
		{
			"geometry hex string with bytea \\x prefix",
			`\x0101000020E6100000` + "0000000000000000" + "0000000000000000",
			ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326},
			[]byte{
				0x01,
				0x01, 0x00, 0x00, 0x00,
				0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0,
			},
		},
		{
			"geometry EWKB bytes",
			[]byte{
				0x01,
				0x01, 0x00, 0x00, 0x20,
				0xE6, 0x10, 0x00, 0x00,
				0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0,
			},
			ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326},
			[]byte{
				0x01,
				0x01, 0x00, 0x00, 0x00,
				0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0,
			},
		},
		{
			"geometry raw WKB bytes pass through (no SRID flag)",
			[]byte{
				0x01,
				0x01, 0x00, 0x00, 0x00,
				0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0,
			},
			ir.Geometry{Subtype: ir.GeometryPoint, SRID: 0},
			[]byte{
				0x01,
				0x01, 0x00, 0x00, 0x00,
				0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0,
			},
		},

		// ---- ADR-0032 PG extension passthrough (pgvector) ----
		// pgvector returns vectors as `[1,2,3]`-style strings under
		// pgx stdlib mode; decoder passes them through verbatim.
		{
			"pgvector string passthrough",
			"[0.1,0.2,0.3]",
			ir.ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{3}},
			"[0.1,0.2,0.3]",
		},
		{
			"pgvector null",
			nil,
			ir.ExtensionType{Extension: "vector", Name: "vector"},
			nil,
		},

		// ---- ADR-0032 hstore + citext passthrough (Tier 1) ----
		// hstore values arrive as PG-canonical text under pgx stdlib
		// mode; the decoder passes them through verbatim for both
		// same-engine PG→PG (where the value is reapplied as-is) and
		// cross-engine PG→MySQL (where the writer's prepareValue
		// reparses to JSON).
		{
			"hstore string passthrough",
			`"a"=>"1", "b"=>"2"`,
			ir.ExtensionType{Extension: "hstore", Name: "hstore"},
			`"a"=>"1", "b"=>"2"`,
		},
		{
			"hstore empty string",
			"",
			ir.ExtensionType{Extension: "hstore", Name: "hstore"},
			"",
		},
		// citext values are plain strings.
		{
			"citext string passthrough",
			"Hello",
			ir.ExtensionType{Extension: "citext", Name: "citext"},
			"Hello",
		},
		{
			"citext bytes round-tripped to bytes",
			[]byte("MixedCase"),
			ir.ExtensionType{Extension: "citext", Name: "citext"},
			[]byte("MixedCase"),
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := decodeValue(c.raw, c.t)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("decodeValue(%#v, %T)\n got = %#v\nwant = %#v", c.raw, c.t, got, c.want)
			}
		})
	}
}

func TestDecodeValueErrors(t *testing.T) {
	cases := []struct {
		name string
		raw  any
		t    ir.Type
	}{
		{"bool from int", int64(1), ir.Boolean{}},
		{"int from non-numeric string", "not a number", ir.Integer{Width: 32}},
		{"bool from gibberish string", "maybe", ir.Boolean{}},
		{"timestamp from gibberish string", "not a date", ir.Timestamp{}},
		{"uuid wrong length bytes", []byte{1, 2, 3}, ir.UUID{}},
		// Bug 41 negative cases — malformed text-format must error
		// loudly rather than slipping past validation.
		{"uuid 36-byte text with missing hyphen", []byte("11111111+2222-3333-4444-555555555555"), ir.UUID{}},
		{"uuid 36-byte text with non-hex byte", []byte("zzzzzzzz-2222-3333-4444-555555555555"), ir.UUID{}},
		{"uuid string wrong length", "1234", ir.UUID{}},
		{"uuid string missing hyphen", "11111111+2222-3333-4444-555555555555", ir.UUID{}},
		{"uuid byte slice unrecognised length", []byte("123456789012345"), ir.UUID{}},
		{"array from string without braces", "not an array literal", ir.Array{Element: ir.Integer{}}},
		{"array nil element type", []int32{1}, ir.Array{}},
		// Geometry — malformed inputs surface loudly rather than
		// reaching the writer with garbage.
		{"geometry non-hex string", "not-hex", ir.Geometry{}},
		{"geometry empty bytes", []byte{}, ir.Geometry{}},
		{"geometry EWKB declaring SRID but no body", []byte{0x01, 0x01, 0x00, 0x00, 0x20}, ir.Geometry{}},
		{"geometry unknown byte-order", []byte{0x42, 0x00, 0x00, 0x00, 0x00}, ir.Geometry{}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if _, err := decodeValue(c.raw, c.t); err == nil {
				t.Errorf("expected error for %s; got nil", c.name)
			}
		})
	}
}

func TestDecodeBytesIsCopy(t *testing.T) {
	src := []byte{0xaa, 0xbb, 0xcc}
	got, err := decodeValue(src, ir.Blob{Size: ir.BlobLong})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := got.([]byte)
	if &out[0] == &src[0] {
		t.Fatal("decodeValue returned the driver's slice; expected a copy")
	}
	src[0] = 0x00
	if out[0] != 0xaa {
		t.Errorf("mutating source mutated decoded value: got %#v", out)
	}
}

// TestDecodeBytea pins the two shapes the bytea value-decoder must
// disambiguate (Bug 92 follow-up — surfaced by the FULL family-matrix
// pin exercising a bytea column end-to-end):
//
//   - CDC (pgoutput tuple text format): the value arrives as the
//     server's `bytea_output = hex` text — `\x`-prefixed even-length
//     hex, delivered as the ASCII bytes of that text. It MUST be
//     hex-decoded to the raw bytes, not copied verbatim (verbatim was
//     the pre-fix silent bytea corruption: `\xcafebabe` stored as the
//     10 literal ASCII bytes instead of 4).
//   - row-reader (database/sql via pgx): pgx hands raw decoded bytes
//     that do NOT carry the `\x` text prefix; those are copied verbatim.
func TestDecodeBytea(t *testing.T) {
	cases := []struct {
		name string
		raw  any
		want []byte
	}{
		{
			name: "CDC hex text (bytes) decodes to raw bytes",
			raw:  []byte(`\xcafebabe`),
			want: []byte{0xca, 0xfe, 0xba, 0xbe},
		},
		{
			name: "CDC hex text (string) decodes to raw bytes",
			raw:  `\xdeadbeef`,
			want: []byte{0xde, 0xad, 0xbe, 0xef},
		},
		{
			name: "CDC empty bytea (\\x prefix only) decodes to empty",
			raw:  []byte(`\x`),
			want: []byte{},
		},
		{
			name: "row-reader raw bytes copied verbatim",
			raw:  []byte{0xde, 0xad},
			want: []byte{0xde, 0xad},
		},
		{
			name: "row-reader raw bytes that happen to start with 0x5c not hex",
			// 0x5c 0x78 is the ASCII for `\x`, but the remainder ("zz")
			// is not valid hex, so it falls through to a verbatim copy.
			raw:  []byte{0x5c, 0x78, 'z', 'z'},
			want: []byte{0x5c, 0x78, 'z', 'z'},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := decodeValue(c.raw, ir.Blob{Size: ir.BlobLong})
			if err != nil {
				t.Fatalf("decodeValue: %v", err)
			}
			b, ok := got.([]byte)
			if !ok {
				t.Fatalf("decodeValue returned %T; want []byte", got)
			}
			if !bytes.Equal(b, c.want) {
				t.Errorf("got %#v; want %#v", b, c.want)
			}
		})
	}
}

// TestCanonicalizeUUIDText covers the Bug 41 fix surface: pgoutput
// CDC tuples deliver UUID values as 36-byte canonical text, and the
// decoder must validate + lowercase before handing the string to the
// IR. The shape-validation negative cases are already covered through
// decodeValue in TestDecodeValueErrors; this test pins the helper's
// own contract.
func TestCanonicalizeUUIDText(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
		err  bool
	}{
		{"canonical lowercase", "11111111-2222-3333-4444-555555555555", "11111111-2222-3333-4444-555555555555", false},
		{"canonical uppercase lowercased", "AABBCCDD-EEFF-0011-2233-445566778899", "aabbccdd-eeff-0011-2233-445566778899", false},
		{"mixed case lowercased", "AaBbCcDd-1111-2222-3333-4a4B4c4D4E4f", "aabbccdd-1111-2222-3333-4a4b4c4d4e4f", false},
		{"too short", "11111111-2222-3333-4444-55555555555", "", true},
		{"too long", "11111111-2222-3333-4444-5555555555556", "", true},
		{"missing hyphen at 8", "11111111+2222-3333-4444-555555555555", "", true},
		{"missing hyphen at 13", "11111111-2222+3333-4444-555555555555", "", true},
		{"missing hyphen at 18", "11111111-2222-3333+4444-555555555555", "", true},
		{"missing hyphen at 23", "11111111-2222-3333-4444+555555555555", "", true},
		{"non-hex byte", "zzzzzzzz-2222-3333-4444-555555555555", "", true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := canonicalizeUUIDText(c.in)
			if c.err {
				if err == nil {
					t.Errorf("canonicalizeUUIDText(%q): expected error, got %q", c.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("canonicalizeUUIDText(%q): unexpected error: %v", c.in, err)
			}
			if got != c.want {
				t.Errorf("canonicalizeUUIDText(%q):\n got  %q\n want %q", c.in, got, c.want)
			}
		})
	}
}

func TestFormatUUIDBytes(t *testing.T) {
	cases := []struct {
		in   []byte
		want string
		err  bool
	}{
		{
			[]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
			"00112233-4455-6677-8899-aabbccddeeff",
			false,
		},
		{[]byte{1, 2, 3}, "", true},
	}
	for _, c := range cases {
		got, err := formatUUIDBytes(c.in)
		if c.err {
			if err == nil {
				t.Errorf("formatUUIDBytes(%v): expected error", c.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("formatUUIDBytes: unexpected error: %v", err)
		}
		if got != c.want {
			t.Errorf("formatUUIDBytes:\n got  %q\n want %q", got, c.want)
		}
	}
}
