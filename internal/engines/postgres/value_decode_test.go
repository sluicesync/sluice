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
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestDecodeValue is the family table AND the provenance-inertness gate
// (item 135). Every case runs through BOTH entry points and must give
// the same answer: provenance exists for exactly one family — `bytea`,
// where PG's text rendering and a raw binary value are mutually
// ambiguous by content — and every other decoder accepts the driver's
// Go type and PG's text spelling unambiguously. bytea therefore has no
// case in this table; its lanes are pinned in TestDecodeBytea. A family
// that newly becomes provenance-sensitive fails here rather than
// diverging silently between the CDC door and the copy door.
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
		// bytea is the ONE provenance-sensitive family and is
		// deliberately absent from this table: []byte{0xde,0xad} is a
		// verbatim 2-byte value in the binary lane and is not a legal
		// text rendering at all. See TestDecodeBytea.

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
			for _, lane := range decodeLanes() {
				got, err := lane.decode(c.raw, c.t)
				if err != nil {
					t.Fatalf("%s: unexpected error: %v", lane.name, err)
				}
				if !reflect.DeepEqual(got, c.want) {
					t.Errorf("%s(%#v, %T)\n got = %#v\nwant = %#v", lane.name, c.raw, c.t, got, c.want)
				}
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
			for _, lane := range decodeLanes() {
				if _, err := lane.decode(c.raw, c.t); err == nil {
					t.Errorf("expected error for %s via %s; got nil", c.name, lane.name)
				}
			}
		})
	}
}

// decodeLanes returns the two provenance entry points (item 135). The
// helper exists so a table test cannot exercise one lane and imply the
// other — the shape that let the bytea sniffing survive.
func decodeLanes() []struct {
	name   string
	decode func(any, ir.Type) (any, error)
} {
	return []struct {
		name   string
		decode func(any, ir.Type) (any, error)
	}{
		{"decodeValueFromText", decodeValueFromText},
		{"decodeValueFromBinary", decodeValueFromBinary},
	}
}

func TestDecodeBytesIsCopy(t *testing.T) {
	src := []byte{0xaa, 0xbb, 0xcc}
	got, err := decodeValueFromBinary(src, ir.Blob{Size: ir.BlobLong})
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

// TestDecodeBytea is the item-135 provenance matrix, and it replaces a
// pin that asserted ONE answer for BOTH readings of the same bytes.
//
// bytea is the one family where PG's text rendering and a genuine
// binary value are mutually ambiguous by content: `\xdead` is both the
// hex spelling of two bytes and six perfectly ordinary bytes. The old
// decoder decided by CONTENT, so the SCAN path silently shrank the
// binary value (6 → 2 bytes; the bare 2-byte `\x` → ZERO bytes) — and
// the old test blessed that zero-length result as correct, which it is
// for the CDC tuple path and destructive for the scan path.
//
// Each row therefore carries an answer PER LANE. wantTextErr marks the
// renderings the text lane must REFUSE rather than guess at (verbatim
// there is the Bug-92 corruption: the ASCII of the rendering stored as
// the value). Ground truth for the collision cells is PG's own
// `octet_length()`, pinned end-to-end in the integration matrix.
func TestDecodeBytea(t *testing.T) {
	cases := []struct {
		name string
		raw  any

		// wantFromText is the answer when the value is a PG text
		// rendering (pgoutput tuple column, array-literal leaf).
		wantFromText []byte
		wantTextErr  bool

		// wantFromBinary is the answer when the driver already decoded
		// it (database/sql scan, pgx-decoded slice element).
		wantFromBinary []byte
	}{
		// ---- The collision cells: legal hex spellings AND legal bytes.
		{
			name:           `\xdead — the audit's cell (6 bytes of binary, 2 bytes of hex)`,
			raw:            []byte(`\xdead`),
			wantFromText:   []byte{0xde, 0xad},
			wantFromBinary: []byte(`\xdead`),
		},
		{
			name:           `bare \x — the worst cell: 2 bytes of binary, ZERO bytes of hex`,
			raw:            []byte(`\x`),
			wantFromText:   []byte{},
			wantFromBinary: []byte{0x5c, 0x78},
		},
		{
			name:           `\xdeadbeef — 10 bytes of binary, 4 bytes of hex`,
			raw:            []byte(`\xdeadbeef`),
			wantFromText:   []byte{0xde, 0xad, 0xbe, 0xef},
			wantFromBinary: []byte(`\xdeadbeef`),
		},
		{
			name: `\xDEAD uppercase — PG never emits it, encoding/hex accepts it`,
			raw:  []byte(`\xDEAD`),
			// Kept decoding rather than refusing: a rendering that is
			// unambiguously hex is not the failure this refusal exists
			// for, and refusing it would be a new failure mode.
			wantFromText:   []byte{0xde, 0xad},
			wantFromBinary: []byte(`\xDEAD`),
		},
		{
			name:           "hex text as a string, not bytes",
			raw:            `\xcafebabe`,
			wantFromText:   []byte{0xca, 0xfe, 0xba, 0xbe},
			wantFromBinary: []byte(`\xcafebabe`),
		},

		// ---- The mutation controls: the neighbours that made content
		// sniffing look safe. Both are verbatim in the binary lane and
		// REFUSED in the text lane (they are not renderings PG's
		// `bytea_output = hex` can produce).
		{
			name:           `invalid hex \xzz`,
			raw:            []byte{0x5c, 0x78, 'z', 'z'},
			wantTextErr:    true,
			wantFromBinary: []byte{0x5c, 0x78, 'z', 'z'},
		},
		{
			name:           `odd-length hex \xabc`,
			raw:            []byte(`\xabc`),
			wantTextErr:    true,
			wantFromBinary: []byte(`\xabc`),
		},
		{
			name:           "escape-format rendering (bytea_output = escape)",
			raw:            []byte(`\001\002abc`),
			wantTextErr:    true,
			wantFromBinary: []byte(`\001\002abc`),
		},

		// ---- The non-colliding shapes.
		{
			name:           "genuine binary",
			raw:            []byte{0x00, 0xff, 0x10, 0x80},
			wantTextErr:    true,
			wantFromBinary: []byte{0x00, 0xff, 0x10, 0x80},
		},
		{
			name:           "empty",
			raw:            []byte{},
			wantTextErr:    true,
			wantFromBinary: []byte{},
		},
		{
			name:           "embedded NUL",
			raw:            []byte{0x61, 0x00, 0x62},
			wantTextErr:    true,
			wantFromBinary: []byte{0x61, 0x00, 0x62},
		},
		{
			name:           "hex text carrying an embedded NUL byte value",
			raw:            []byte(`\x610062`),
			wantFromText:   []byte{0x61, 0x00, 0x62},
			wantFromBinary: []byte(`\x610062`),
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			gotText, errText := decodeValueFromText(c.raw, ir.Blob{Size: ir.BlobLong})
			switch {
			case c.wantTextErr && errText == nil:
				t.Errorf("decodeValueFromText(%#v) = %#v; want a loud refusal", c.raw, gotText)
			case !c.wantTextErr && errText != nil:
				t.Errorf("decodeValueFromText(%#v): %v", c.raw, errText)
			case !c.wantTextErr:
				b, ok := gotText.([]byte)
				if !ok {
					t.Fatalf("decodeValueFromText returned %T; want []byte", gotText)
				}
				if !bytes.Equal(b, c.wantFromText) {
					t.Errorf("decodeValueFromText(%#v) = %#v; want %#v", c.raw, b, c.wantFromText)
				}
			}

			gotBin, errBin := decodeValueFromBinary(c.raw, ir.Blob{Size: ir.BlobLong})
			if errBin != nil {
				t.Fatalf("decodeValueFromBinary(%#v): %v", c.raw, errBin)
			}
			b, ok := gotBin.([]byte)
			if !ok {
				t.Fatalf("decodeValueFromBinary returned %T; want []byte", gotBin)
			}
			if !bytes.Equal(b, c.wantFromBinary) {
				t.Errorf("decodeValueFromBinary(%#v) = %#v; want %#v", c.raw, b, c.wantFromBinary)
			}
			// The binary lane must never inspect the content: whatever
			// it was handed comes back byte-identical.
			if raw, isBytes := c.raw.([]byte); isBytes && !bytes.Equal(b, raw) {
				t.Errorf("binary lane transformed its input: %#v → %#v", raw, b)
			}
		})
	}
}

// TestDecodeByteaTextRefusalCarriesItsCode pins the operator-facing half
// of the text lane's refusal: a rendering it cannot read fails with
// SLUICE-E-VALUE-BYTEA-TEXT-UNRECOGNIZED, not a bare error, so the
// operator gets the `bytea_output` remedy rather than a decode string.
func TestDecodeByteaTextRefusalCarriesItsCode(t *testing.T) {
	_, err := decodeValueFromText([]byte(`\001\002`), ir.Blob{Size: ir.BlobLong})
	if err == nil {
		t.Fatal("decodeValueFromText accepted an escape-format rendering; want a refusal")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeValueByteaTextUnrecognized {
		t.Fatalf("refusal carries %v; want %s", err, sluicecode.CodeValueByteaTextUnrecognized)
	}
}

// TestDecodeByteaUndeclaredProvenanceRefuses pins the zero value. There
// is no safe default lane for bytea — one silently shrinks binary, the
// other silently stores ASCII — so an undeclared provenance refuses
// rather than picking one.
func TestDecodeByteaUndeclaredProvenanceRefuses(t *testing.T) {
	if _, err := decodeValue([]byte(`\xdead`), ir.Blob{Size: ir.BlobLong}, provUnset); err == nil {
		t.Fatal("decodeValue with provUnset decoded a bytea; want a refusal")
	}
	// Every other family is provenance-inert, so provUnset must NOT
	// break them — the refusal is scoped to the ambiguous family.
	if _, err := decodeValue(int32(7), ir.Integer{Width: 32}, provUnset); err != nil {
		t.Fatalf("provUnset broke a provenance-inert family: %v", err)
	}
}

// TestDecodeByteaArrayLeafProvenance is the half the audit's proposed
// fix ("hex-decode only in decodeTuple") would have broken. A `bytea[]`
// scanned into *any comes back as a STRING in PG array-text form with
// every element already hex-rendered — so the array leaves are TEXT
// whichever door the array itself arrived through, and the pgx-decoded
// slice shapes are BINARY whichever door they arrived through. Both
// entry points are exercised on every shape for exactly that reason:
// decodeArray must NOT forward its caller's provenance.
func TestDecodeByteaArrayLeafProvenance(t *testing.T) {
	byteaArray := ir.Array{Element: ir.Blob{Size: ir.BlobLong}}

	textShapes := []struct {
		name string
		raw  any
		want []any
	}{
		{
			// PG renders a bytea element with its backslash escaped
			// inside the quoted token: {"\\xdead","\\x"}.
			name: "1-D",
			raw:  `{"\\xdead","\\x","\\xdeadbeef"}`,
			want: []any{[]byte{0xde, 0xad}, []byte{}, []byte{0xde, 0xad, 0xbe, 0xef}},
		},
		{
			name: "2-D not flattened",
			raw:  `{{"\\xdead","\\xbeef"},{"\\x","\\x00"}}`,
			want: []any{
				[]any{[]byte{0xde, 0xad}, []byte{0xbe, 0xef}},
				[]any{[]byte{}, []byte{0x00}},
			},
		},
		{
			name: "NULL element",
			raw:  `{"\\xdead",NULL,"\\x"}`,
			want: []any{[]byte{0xde, 0xad}, nil, []byte{}},
		},
		{
			// The pgoutput CDC door delivers the same literal as bytes.
			name: "1-D delivered as []byte (pgoutput)",
			raw:  []byte(`{"\\xcafe",NULL}`),
			want: []any{[]byte{0xca, 0xfe}, nil},
		},
	}
	for _, s := range textShapes {
		s := s
		t.Run("arrayText/"+s.name, func(t *testing.T) {
			for _, lane := range decodeLanes() {
				got, err := lane.decode(s.raw, byteaArray)
				if err != nil {
					t.Fatalf("%s: %v", lane.name, err)
				}
				if !reflect.DeepEqual(got, s.want) {
					t.Errorf("%s\n got = %#v\nwant = %#v", lane.name, got, s.want)
				}
			}
		})
	}

	// The already-decoded sub-paths: elements pgx produced are raw
	// bytes and must NOT be hex-decoded, whichever door we came in.
	binaryShapes := []struct {
		name string
		raw  any
		want []any
	}{
		{
			name: "[]any of raw bytes spelling the hex form",
			raw:  []any{[]byte(`\xdead`), []byte(`\x`), nil},
			want: []any{[]byte(`\xdead`), []byte{0x5c, 0x78}, nil},
		},
		{
			name: "[][]byte via reflect",
			raw:  [][]byte{[]byte(`\xdead`), {0x00, 0xff}},
			want: []any{[]byte(`\xdead`), []byte{0x00, 0xff}},
		},
	}
	for _, s := range binaryShapes {
		s := s
		t.Run("driverSlice/"+s.name, func(t *testing.T) {
			for _, lane := range decodeLanes() {
				got, err := lane.decode(s.raw, byteaArray)
				if err != nil {
					t.Fatalf("%s: %v", lane.name, err)
				}
				if !reflect.DeepEqual(got, s.want) {
					t.Errorf("%s\n got = %#v\nwant = %#v", lane.name, got, s.want)
				}
			}
		})
	}
}

// TestDecodeByteaThroughDomain pins that provenance survives the
// ir.Domain recursion — a DOMAIN over bytea is decoded by its base
// type, and dropping the lane there would reintroduce the sniff.
func TestDecodeByteaThroughDomain(t *testing.T) {
	dom := ir.Domain{Name: "hashval", BaseType: ir.Blob{Size: ir.BlobLong}}

	got, err := decodeValueFromBinary([]byte(`\xdead`), dom)
	if err != nil {
		t.Fatalf("decodeValueFromBinary(domain): %v", err)
	}
	if b := got.([]byte); !bytes.Equal(b, []byte(`\xdead`)) {
		t.Errorf("domain binary lane = %#v; want the 6 verbatim bytes", b)
	}

	got, err = decodeValueFromText([]byte(`\xdead`), dom)
	if err != nil {
		t.Fatalf("decodeValueFromText(domain): %v", err)
	}
	if b := got.([]byte); !bytes.Equal(b, []byte{0xde, 0xad}) {
		t.Errorf("domain text lane = %#v; want the 2 decoded bytes", b)
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
