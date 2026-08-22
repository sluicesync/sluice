// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The MySQL half of item 135's provenance rule (audit 2026-08-05 B-2).
//
// `\xdead` is both PG's `bytea_output = hex` rendering of two bytes and a
// perfectly ordinary six-byte string, so a writer that decides by looking
// at the CONTENT silently shrinks one of the two readings. The Postgres
// DECODER stopped sniffing in item 135; this pins the last content sniff
// on the MySQL WRITER, which is where a PG-sourced value becomes bytes in
// a MySQL BLOB.
//
// The discriminator here is the COLUMN, not the value:
// [ir.Column.SourceColumnType] says whether the binary type is the
// source's own or one a `--type-override` imposed. So the matrix is
// family × shape × COLUMN PROVENANCE, and the provenance axis is the one
// that did not exist before — a table that exercised only natively-binary
// columns would pass on both sides of the bug.

package mysql

import (
	"bytes"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// byteaShape is one cell of the value axis. want* are the answers for a
// STRING value; a []byte value is verbatim on every lane and is asserted
// separately.
type byteaShape struct {
	name string
	// in is the string value the reader handed the writer.
	in string
	// wantNative is the bytes a NATIVELY-binary column must store, or
	// nil when the rendering must be refused by name.
	wantNative  []byte
	wantRefusal bool
}

// byteaShapes is the shape axis the task contract names: spells
// `\x`+valid hex, bare `\x`, `\x`+odd-length hex, `\x`+non-hex, empty,
// genuinely-hex-encoded — plus an ordinary non-`\x` string, which is the
// shape a `text` column overridden to binary most often carries.
var byteaShapes = []byteaShape{
	{
		name:       `collision: \xdead (2 bytes of hex, 6 bytes of text)`,
		in:         `\xdead`,
		wantNative: []byte{0xde, 0xad},
	},
	{
		// The worst cell: as a rendering it is EMPTY, as text it is two
		// bytes. A value that becomes empty rather than wrong is the one
		// a row count and a NOT NULL constraint both wave through.
		name:       `collision: bare \x (ZERO bytes of hex, 2 bytes of text)`,
		in:         `\x`,
		wantNative: []byte{},
	},
	{
		name:       `collision: \xdeadbeef`,
		in:         `\xdeadbeef`,
		wantNative: []byte{0xde, 0xad, 0xbe, 0xef},
	},
	// Uppercase / mixed-case hex (audit B-2d): PG accepts uppercase bytea
	// input even though its own emitters render lowercase, so this lane
	// can be handed `\xDEAD` by an upstream producer. Natively-binary it
	// decodes to the same bytes as the lowercase spelling; overridden it
	// is a DIFFERENT verbatim value, which the shared overridden-lane
	// assertion covers per shape.
	{
		name:       `collision: uppercase \xDEAD`,
		in:         `\xDEAD`,
		wantNative: []byte{0xde, 0xad},
	},
	{
		name:       `collision: mixed-case \xDeAd`,
		in:         `\xDeAd`,
		wantNative: []byte{0xde, 0xad},
	},
	{
		name:       "genuinely hex-encoded, no collision",
		in:         `\x00ff1080`,
		wantNative: []byte{0x00, 0xff, 0x10, 0x80},
	},
	{
		name:       "hex text carrying an embedded NUL byte value",
		in:         `\x610062`,
		wantNative: []byte{0x61, 0x00, 0x62},
	},
	// A `\x` prefix that does not parse is a rendering ATTEMPT that
	// cannot be read; storing its ASCII would be the silent corruption
	// in smaller print, so it is refused by name.
	{
		name:        `\x + odd-length hex`,
		in:          `\xabc`,
		wantRefusal: true,
	},
	{
		name:        `\x + non-hex`,
		in:          `\xzz`,
		wantRefusal: true,
	},
	// No `\x` prefix ⇒ never a rendering ⇒ the bytes are the only
	// reading, so these pass through even on a natively-binary column.
	// See the WART note in prepareValue for why the scalar branch
	// diverges from byteaArrayLeaf here (the applier lane).
	{
		name:       "empty",
		in:         "",
		wantNative: []byte{},
	},
	{
		name:       "escape-format rendering (bytea_output = escape)",
		in:         `\001\002abc`,
		wantNative: []byte(`\001\002abc`),
	},
	{
		name:       "ordinary text with no rendering shape",
		in:         "hello",
		wantNative: []byte("hello"),
	},
}

// byteaTargetFamilies is the target-type axis. Every MySQL binary family
// takes the same branch, and ir.Domain is included because
// [prepareValue] unwraps it to its base type — the shape that let the
// Postgres decoder's Domain recursion drop provenance in the first draft
// of item 135.
var byteaTargetFamilies = []struct {
	name string
	typ  ir.Type
}{
	{"Blob", ir.Blob{Size: ir.BlobLong}},
	{"Varbinary", ir.Varbinary{Length: 64}},
	{"Binary", ir.Binary{Length: 16}},
	{"Domain over Blob", ir.Domain{Name: "hashval", BaseType: ir.Blob{Size: ir.BlobLong}}},
}

// byteaColumnProvenances is the axis this test exists for. A nil
// SourceColumnType means no override fired (the column is natively
// binary); a non-nil one names what the override REPLACED.
var byteaColumnProvenances = []struct {
	name string
	// src is ir.Column.SourceColumnType.
	src ir.Type
	// native says whether a string on this column is PG's bytea
	// rendering (decode/refuse) or the source's own bytes (verbatim).
	native bool
}{
	{"no override (natively binary)", nil, true},
	{"overridden from bytea (no-op)", ir.Blob{Size: ir.BlobLong}, true},
	{"overridden from varbinary", ir.Varbinary{Length: 32}, true},
	{"overridden from DOMAIN over bytea", ir.Domain{Name: "d", BaseType: ir.Blob{Size: ir.BlobLong}}, true},
	{"--type-override text=bytea", ir.Text{Size: ir.TextLong}, false},
	{"--type-override varchar=bytea", ir.Varchar{Length: 255}, false},
	{"--type-override json=bytea", ir.JSON{Binary: true}, false},
}

// TestPrepareValue_ByteaProvenanceMatrix is the family × shape × column
// provenance matrix.
//
// Scope, stated rather than implied: it grades [prepareValue], which is
// the SINGLE funnel every MySQL write core binds through — the batched /
// multi-row INSERT cores, the `LOAD DATA` writer's tsvEncode path, and
// the ChangeApplier (via prepareApplierValue). There is no second place
// to sniff. The real-server half, including which cores actually reach
// this funnel, is TestByteaProvenance_MySQLWriteCores.
func TestPrepareValue_ByteaProvenanceMatrix(t *testing.T) {
	for _, fam := range byteaTargetFamilies {
		fam := fam
		for _, prov := range byteaColumnProvenances {
			prov := prov
			for _, shape := range byteaShapes {
				shape := shape
				t.Run(fam.name+"/"+prov.name+"/"+shape.name, func(t *testing.T) {
					col := &ir.Column{
						Name:             "b",
						Type:             fam.typ,
						SourceColumnType: prov.src,
					}
					got, err := prepareValue(shape.in, col)

					if !prov.native {
						// An overridden column carries the SOURCE's bytes.
						// No decode, and no refusal either — the value is
						// not a rendering, so there is nothing to fail on.
						if err != nil {
							t.Fatalf("overridden column refused a source value: %v", err)
						}
						if s, ok := got.(string); !ok || s != shape.in {
							t.Errorf("got %#v; want the verbatim source string %q", got, shape.in)
						}
						return
					}

					if shape.wantRefusal {
						if err == nil {
							t.Fatalf("got %#v; want a loud refusal for an unreadable rendering", got)
						}
						ce, ok := sluicecode.FromError(err)
						if !ok || ce.Code != sluicecode.CodeValueByteaTextUnrecognized {
							t.Errorf("refusal carries %v; want %s",
								err, sluicecode.CodeValueByteaTextUnrecognized)
						}
						return
					}
					if err != nil {
						t.Fatalf("natively-binary column refused a readable value: %v", err)
					}
					// Compare the BYTES the value would store, whichever
					// Go shape carries them: a decoded rendering comes
					// back as []byte, a pass-through as the string. The
					// server sees only the bytes, so that is the honest
					// comparison — and it is the number (a LENGTH) that
					// the pre-fix bug got wrong.
					b, ok := preparedBytes(got)
					if !ok {
						t.Fatalf("got %T; want []byte or string", got)
					}
					if !bytes.Equal(b, shape.wantNative) {
						t.Errorf("got %#v; want %#v", b, shape.wantNative)
					}
				})
			}
		}
	}
}

// preparedBytes renders a prepared value as the bytes MySQL will store.
func preparedBytes(v any) ([]byte, bool) {
	switch x := v.(type) {
	case []byte:
		return x, true
	case string:
		return []byte(x), true
	}
	return nil, false
}

// TestPrepareValue_ByteaBytesAreNeverInspected pins the other half: a
// []byte value is the IR-canonical binary leaf and comes back
// byte-identical on EVERY column provenance. If this ever stops holding,
// the writer has started sniffing the lane that is not ambiguous at all.
func TestPrepareValue_ByteaBytesAreNeverInspected(t *testing.T) {
	raws := [][]byte{
		[]byte(`\xdead`),
		[]byte(`\x`),
		{},
		{0x00, 0xff, 0x10, 0x80},
		{0x61, 0x00, 0x62},
	}
	for _, fam := range byteaTargetFamilies {
		fam := fam
		for _, prov := range byteaColumnProvenances {
			prov := prov
			for _, raw := range raws {
				raw := raw
				t.Run(fam.name+"/"+prov.name+"/"+string(raw), func(t *testing.T) {
					col := &ir.Column{Name: "b", Type: fam.typ, SourceColumnType: prov.src}
					got, err := prepareValue(raw, col)
					if err != nil {
						t.Fatalf("prepareValue(%#v): %v", raw, err)
					}
					b, ok := got.([]byte)
					if !ok {
						t.Fatalf("got %T; want []byte", got)
					}
					if !bytes.Equal(b, raw) {
						t.Errorf("binary lane transformed its input: %#v -> %#v", raw, b)
					}
				})
			}
		}
	}
}

// TestPrepareValue_ByteaNilColumnStillDecodes pins the applier's
// nil-descriptor branch. It has no column to consult, so it keeps the
// pre-existing reading (decode) — stated here rather than left to be
// rediscovered, because a nil col is the one input where this guard
// cannot be provenance-decided.
func TestPrepareValue_ByteaNilColumnStillDecodes(t *testing.T) {
	got, err := prepareValue(`\xdead`, nil)
	if err != nil {
		t.Fatalf("prepareValue(nil col): %v", err)
	}
	// With no column there is no type dispatch at all: prepareValue's
	// nil-col branch is a pass-through, so the value stays a string.
	if s, ok := got.(string); !ok || s != `\xdead` {
		t.Errorf("nil-col prepareValue = %#v; want the verbatim string", got)
	}
}

// TestColumnIsNativelyBinary is the guard's own truth table, kept
// separate so a change to [prepareValue]'s dispatch cannot make this
// vacuous.
func TestColumnIsNativelyBinary(t *testing.T) {
	cases := []struct {
		name string
		col  *ir.Column
		want bool
	}{
		{"nil column", nil, true},
		{"no override", &ir.Column{Type: ir.Blob{}}, true},
		{"override from blob", &ir.Column{Type: ir.Blob{}, SourceColumnType: ir.Blob{}}, true},
		{"override from binary", &ir.Column{Type: ir.Blob{}, SourceColumnType: ir.Binary{Length: 4}}, true},
		{"override from varbinary", &ir.Column{Type: ir.Blob{}, SourceColumnType: ir.Varbinary{Length: 4}}, true},
		{
			"override from DOMAIN over blob",
			&ir.Column{Type: ir.Blob{}, SourceColumnType: ir.Domain{Name: "d", BaseType: ir.Blob{}}},
			true,
		},
		{"override from text", &ir.Column{Type: ir.Blob{}, SourceColumnType: ir.Text{}}, false},
		{"override from varchar", &ir.Column{Type: ir.Blob{}, SourceColumnType: ir.Varchar{Length: 10}}, false},
		{"override from json", &ir.Column{Type: ir.Blob{}, SourceColumnType: ir.JSON{}}, false},
		{"override from uuid", &ir.Column{Type: ir.Binary{Length: 16}, SourceColumnType: ir.UUID{}}, false},
		{
			"override from DOMAIN over text",
			&ir.Column{Type: ir.Blob{}, SourceColumnType: ir.Domain{Name: "d", BaseType: ir.Text{}}},
			false,
		},
		{"override from DOMAIN with nil base", &ir.Column{Type: ir.Blob{}, SourceColumnType: ir.Domain{Name: "d"}}, false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := columnIsNativelyBinary(c.col); got != c.want {
				t.Errorf("columnIsNativelyBinary = %v; want %v", got, c.want)
			}
		})
	}
}
