// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/orware/sluice/internal/ir"
)

// TestPGHstoreCodec_EmptyHstore pins the empty-map edge case: an empty
// hstore text ("") encodes to a 4-byte payload — int32 BE 0 pair count
// — and decodes back to "".
func TestPGHstoreCodec_EmptyHstore(t *testing.T) {
	plan := pgHstoreBinaryCodec{}.PlanEncode(nil, 0, pgtype.BinaryFormatCode, "")
	if plan == nil {
		t.Fatal("PlanEncode returned nil for binary+empty-string")
	}
	out, err := plan.Encode("", nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(out) != 4 {
		t.Fatalf("empty hstore encoded len = %d; want 4", len(out))
	}
	if got := binary.BigEndian.Uint32(out[0:4]); got != 0 {
		t.Errorf("empty hstore pair count = %d; want 0", got)
	}
	back, err := decodeHstoreBinary(out)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if back != "" {
		t.Errorf("decode = %q; want \"\"", back)
	}
}

// TestPGHstoreCodec_SinglePair pins the smallest non-empty case:
// `"k"=>"v"` is one pair, total payload = 4 (count) + 4 (keylen) + 1
// (k) + 4 (vallen) + 1 (v) = 14 bytes.
func TestPGHstoreCodec_SinglePair(t *testing.T) {
	const text = `"k"=>"v"`
	plan := pgHstoreBinaryCodec{}.PlanEncode(nil, 0, pgtype.BinaryFormatCode, text)
	if plan == nil {
		t.Fatal("PlanEncode returned nil")
	}
	out, err := plan.Encode(text, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	wantLen := 4 + 4 + 1 + 4 + 1
	if len(out) != wantLen {
		t.Fatalf("single-pair encoded len = %d; want %d", len(out), wantLen)
	}
	if got := binary.BigEndian.Uint32(out[0:4]); got != 1 {
		t.Errorf("pair count = %d; want 1", got)
	}
	if got := binary.BigEndian.Uint32(out[4:8]); got != 1 {
		t.Errorf("keylen = %d; want 1", got)
	}
	if out[8] != 'k' {
		t.Errorf("key byte = %q; want 'k'", out[8])
	}
	if got := binary.BigEndian.Uint32(out[9:13]); got != 1 {
		t.Errorf("vallen = %d; want 1", got)
	}
	if out[13] != 'v' {
		t.Errorf("value byte = %q; want 'v'", out[13])
	}
	back, err := decodeHstoreBinary(out)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if back != `"k"=>"v"` {
		t.Errorf("decode = %q; want %q", back, `"k"=>"v"`)
	}
}

// TestPGHstoreCodec_NullValue pins the NULL-value case: vallen = -1
// signals SQL null on the wire; no value bytes follow.
func TestPGHstoreCodec_NullValue(t *testing.T) {
	const text = `"k"=>NULL`
	plan := pgHstoreBinaryCodec{}.PlanEncode(nil, 0, pgtype.BinaryFormatCode, text)
	if plan == nil {
		t.Fatal("PlanEncode returned nil")
	}
	out, err := plan.Encode(text, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	wantLen := 4 + 4 + 1 + 4 // pair count, keylen, 'k', vallen=-1 (no value bytes)
	if len(out) != wantLen {
		t.Fatalf("NULL-value encoded len = %d; want %d", len(out), wantLen)
	}
	// vallen field should be 0xFFFFFFFF (-1 as int32 BE).
	gotVL := int32(binary.BigEndian.Uint32(out[9:13]))
	if gotVL != -1 {
		t.Errorf("NULL vallen = %d; want -1", gotVL)
	}
	back, err := decodeHstoreBinary(out)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if back != `"k"=>NULL` {
		t.Errorf("decode = %q; want %q", back, `"k"=>NULL`)
	}
}

// TestPGHstoreCodec_MultiPairMixedNull pins the multi-pair-with-NULL
// case the full Tier 1 integration test exercises (`'"a"=>"1", "b"=>"2"'`
// shape plus a NULL mixed in).
func TestPGHstoreCodec_MultiPairMixedNull(t *testing.T) {
	const text = `"a"=>"1", "b"=>"2", "c"=>NULL`
	plan := pgHstoreBinaryCodec{}.PlanEncode(nil, 0, pgtype.BinaryFormatCode, text)
	if plan == nil {
		t.Fatal("PlanEncode returned nil")
	}
	out, err := plan.Encode(text, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if got := binary.BigEndian.Uint32(out[0:4]); got != 3 {
		t.Fatalf("pair count = %d; want 3", got)
	}
	back, err := decodeHstoreBinary(out)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Pair order is preserved on round-trip; the decoder emits
	// canonical "k"=>"v" with `, ` separators.
	if back != `"a"=>"1", "b"=>"2", "c"=>NULL` {
		t.Errorf("decode = %q; want exact round-trip", back)
	}
}

// TestPGHstoreCodec_RoundTripProperty walks a small set of canonical
// hstore inputs through encode-then-decode and confirms the output
// matches the input. The decoder normalises to ", " separators and
// always quotes keys/values, so each input here is already in that
// canonical form.
func TestPGHstoreCodec_RoundTripProperty(t *testing.T) {
	cases := []string{
		``,
		`"k"=>"v"`,
		`"k"=>NULL`,
		`"a"=>"1", "b"=>"2"`,
		`"a"=>"1", "b"=>NULL, "c"=>"3"`,
		// Backslash-escaped quotes / backslashes in keys and values.
		`"key\"with\"quotes"=>"val\\with\\backslash"`,
		// Multi-byte UTF-8 content.
		`"éclair"=>"ê"`,
		// Empty key and empty value are legal in PG hstore.
		`""=>""`,
		`""=>NULL`,
	}
	codec := pgHstoreBinaryCodec{}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			plan := codec.PlanEncode(nil, 0, pgtype.BinaryFormatCode, in)
			if plan == nil {
				t.Fatal("PlanEncode returned nil")
			}
			encoded, err := plan.Encode(in, nil)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			back, err := decodeHstoreBinary(encoded)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if back != in {
				t.Errorf("round-trip mismatch:\n  in   = %q\n  back = %q", in, back)
			}
		})
	}
}

// TestPGHstoreCodec_BytesEncode pins the []byte input shape — the
// IR sometimes carries hstore values as []byte rather than string
// (e.g. when the source reader produces driver.Value).
func TestPGHstoreCodec_BytesEncode(t *testing.T) {
	in := []byte(`"k"=>"v"`)
	plan := pgHstoreBinaryCodec{}.PlanEncode(nil, 0, pgtype.BinaryFormatCode, in)
	if plan == nil {
		t.Fatal("PlanEncode returned nil for binary+[]byte")
	}
	out, err := plan.Encode(in, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if got := binary.BigEndian.Uint32(out[0:4]); got != 1 {
		t.Errorf("pair count = %d; want 1", got)
	}
}

// TestPGHstoreCodec_TextEncode pins the text-format encode path. COPY
// uses binary, but PG can still ask the codec for text in some
// scenarios (e.g. extended-query bind in non-COPY paths) and the
// codec should be a no-op passthrough rather than a refusal.
func TestPGHstoreCodec_TextEncode(t *testing.T) {
	const in = `"k"=>"v"`
	plan := pgHstoreBinaryCodec{}.PlanEncode(nil, 0, pgtype.TextFormatCode, in)
	if plan == nil {
		t.Fatal("PlanEncode returned nil for text+string")
	}
	out, err := plan.Encode(in, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if string(out) != in {
		t.Errorf("text encode = %q; want %q", out, in)
	}
}

// TestPGHstoreCodec_ParseTextErrors confirms malformed inputs surface
// loudly rather than silently encoding garbage. Each case is shaped
// to trip one of the parser's expect-clauses.
func TestPGHstoreCodec_ParseTextErrors(t *testing.T) {
	cases := []string{
		`k=>v`,          // unquoted key
		`"k"`,           // no `=>`
		`"k"=>`,         // no value
		`"k"=v"`,        // wrong arrow
		`"k"=>"v`,       // unterminated quoted value
		`"k"=>"v" "k2"`, // missing comma between pairs
		`"k"=>foo`,      // unquoted, non-NULL value
		`"k"=>"v",x`,    // trailing-comma + non-quoted follow-on
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			_, err := parseHstoreTextPG(c)
			if err == nil {
				t.Errorf("parseHstoreTextPG(%q): want error, got nil", c)
			}
		})
	}
}

// TestPGHstoreCodec_FormatSupported and PreferredFormat lock in the
// codec's contract with pgx — binary is preferred (CopyFrom path),
// both formats supported.
func TestPGHstoreCodec_FormatSupported(t *testing.T) {
	c := pgHstoreBinaryCodec{}
	if !c.FormatSupported(pgtype.BinaryFormatCode) {
		t.Error("binary format not supported")
	}
	if !c.FormatSupported(pgtype.TextFormatCode) {
		t.Error("text format not supported")
	}
	if c.PreferredFormat() != pgtype.BinaryFormatCode {
		t.Errorf("preferred format = %d; want %d", c.PreferredFormat(), pgtype.BinaryFormatCode)
	}
}

// TestDecodeHstoreBinary_Truncated guards against malformed wire
// payloads (mid-stream truncation, etc.) — surface a clear error
// rather than slice-out-of-bounds-panic.
func TestDecodeHstoreBinary_Truncated(t *testing.T) {
	cases := [][]byte{
		nil,
		{0x00},                         // < 4 bytes header
		{0x00, 0x00, 0x00, 0x01},       // count=1 but no pair follows
		{0x00, 0x00, 0x00, 0x01, 0x00}, // count=1, partial keylen
		// count=1, keylen=4, only 1 byte of key
		{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 'a'},
		// count=1, keylen=0, no vallen
		{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00},
		// count=1, keylen=0, vallen=5, only 2 value bytes
		{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 'a', 'b'},
		// negative pair count
		{0xFF, 0xFF, 0xFF, 0xFF},
		// trailing bytes after a valid 0-pair payload
		{0x00, 0x00, 0x00, 0x00, 0x99},
	}
	for i, c := range cases {
		_, err := decodeHstoreBinary(c)
		if err == nil {
			t.Errorf("case %d (%d bytes): want error, got nil", i, len(c))
		}
	}
}

// TestPGHstoreCodec_EncodeRefusesUnknownShape pins the
// translator-bug-surfaces-loudly path: encoding against an unexpected
// value type (e.g. int, map) returns a nil plan so the caller's
// dispatch raises a clear error rather than producing garbage.
func TestPGHstoreCodec_EncodeRefusesUnknownShape(t *testing.T) {
	cases := []any{
		42,
		3.14,
		map[string]string{"k": "v"},
		nil,
	}
	for _, v := range cases {
		plan := pgHstoreBinaryCodec{}.PlanEncode(nil, 0, pgtype.BinaryFormatCode, v)
		if plan != nil {
			t.Errorf("PlanEncode(%T) returned non-nil plan; want nil for unsupported type", v)
		}
	}
}

// TestLookupHstoreOID_NotInstalled pins the not-installed signal —
// the helper returns errHstoreTypeNotFound (not a wrapped generic db
// error) so callers can distinguish "no extension here" from "lookup
// failed". The error sentinel itself is the contract; this just
// guards that the message text doesn't drift.
func TestLookupHstoreOID_NotInstalled(t *testing.T) {
	if !errors.Is(errHstoreTypeNotFound, errHstoreTypeNotFound) {
		t.Fatal("errHstoreTypeNotFound: identity check failed")
	}
	const want = "extension not installed"
	if !strings.Contains(errHstoreTypeNotFound.Error(), want) {
		t.Errorf("err message = %q; want substring %q",
			errHstoreTypeNotFound.Error(), want)
	}
}

// TestTableHasHstoreColumn pins the gate the writer uses to decide
// whether to register the codec on a COPY conn. Only ExtensionType
// columns with Extension == "hstore" trigger; other extension types
// (vector, citext) are excluded. Mirrors TestTableHasPGVectorColumn's
// shape.
func TestTableHasHstoreColumn(t *testing.T) {
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
			name: "hstore column present",
			cols: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "tags", Type: ir.ExtensionType{Extension: "hstore", Name: "hstore"}},
			},
			want: true,
		},
		{
			name: "different extension only",
			cols: []*ir.Column{
				{Name: "embedding", Type: ir.ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{3}}},
			},
			want: false,
		},
		{
			name: "mixed vector + hstore",
			cols: []*ir.Column{
				{Name: "embedding", Type: ir.ExtensionType{Extension: "vector", Name: "vector"}},
				{Name: "tags", Type: ir.ExtensionType{Extension: "hstore", Name: "hstore"}},
			},
			want: true,
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
			got := tableHasHstoreColumn(&ir.Table{Name: "t", Columns: c.cols})
			if got != c.want {
				t.Errorf("tableHasHstoreColumn = %v; want %v", got, c.want)
			}
		})
	}
}
