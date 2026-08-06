// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"sluicesync.dev/sluice/internal/ir"
)

// Roadmap item 141 — the bytea array LEAF's input shapes.
//
// The shape matrix next door (row_writer_array_shape_test.go) covers this
// family's dimensions and NULL handling alongside every other family.
// What it does not cover is the leaf's two accepted INPUT spellings and
// the one wart, so those live here:
//
//   - []byte, the IR-canonical leaf from both decode provenance lanes.
//   - the pgtrigger payload's `\x`-hex string, whose rendering is DECIDED
//     by the trigger's `SET bytea_output = hex` rather than sniffed
//     (item 135's lesson).
//   - a nil []byte for a PRESENT element must become an EMPTY bytea, not
//     a NULL — pgx maps a nil []byte to SQL NULL, so the normalisation is
//     load-bearing.

func byteaArrayElements(t *testing.T, v any) []*[]byte {
	t.Helper()
	arr, ok := v.(pgtype.Array[*[]byte])
	if !ok {
		t.Fatalf("convertArray returned %T; want pgtype.Array[*[]byte] (a different leaf type means pgx "+
			"plans a different element encode — the Bug-74 hazard)", v)
	}
	return arr.Elements
}

func TestByteaArrayLeaf_CanonicalBytes(t *testing.T) {
	got, err := convertArray([]any{[]byte{0x00, 0xff}, nil, []byte("AB")}, ir.Blob{})
	if err != nil {
		t.Fatalf("convertArray: %v", err)
	}
	elems := byteaArrayElements(t, got)
	if len(elems) != 3 {
		t.Fatalf("got %d elements, want 3", len(elems))
	}
	if !bytes.Equal(*elems[0], []byte{0x00, 0xff}) {
		t.Errorf("element 0 = %v; want 00 ff", *elems[0])
	}
	if elems[1] != nil {
		t.Errorf("element 1 = %v; want a nil pointer (SQL NULL)", *elems[1])
	}
	if !bytes.Equal(*elems[2], []byte("AB")) {
		t.Errorf("element 2 = %q; want AB", *elems[2])
	}
}

// TestByteaArrayLeaf_EmptyElementIsNotNull is the WART pin. A present
// zero-length bytea and a NULL element are different values; pgx encodes
// a nil []byte as NULL, so without the normalisation the first would
// silently become the second.
func TestByteaArrayLeaf_EmptyElementIsNotNull(t *testing.T) {
	got, err := convertArray([]any{[]byte(nil), []byte{}}, ir.Varbinary{Length: 4})
	if err != nil {
		t.Fatalf("convertArray: %v", err)
	}
	for i, e := range byteaArrayElements(t, got) {
		if e == nil {
			t.Fatalf("element %d is a NULL pointer; both inputs are PRESENT zero-length values", i)
		}
		if *e == nil {
			t.Errorf("element %d holds a nil []byte, which pgx encodes as SQL NULL — a present empty bytea "+
				"must survive as an empty bytea", i)
		}
		if len(*e) != 0 {
			t.Errorf("element %d = %v; want zero length", i, *e)
		}
	}
}

// TestByteaArrayLeaf_TriggerPayloadHexString: the pgtrigger change-payload
// shape. to_jsonb renders bytea through bytea_output, which the trigger
// capture pins to hex.
func TestByteaArrayLeaf_TriggerPayloadHexString(t *testing.T) {
	got, err := convertArray([]any{`\x00ff`, `\x`}, ir.Binary{Length: 2})
	if err != nil {
		t.Fatalf("convertArray: %v", err)
	}
	elems := byteaArrayElements(t, got)
	if !bytes.Equal(*elems[0], []byte{0x00, 0xff}) {
		t.Errorf("element 0 = %v; want 00 ff", *elems[0])
	}
	if len(*elems[1]) != 0 {
		t.Errorf("element 1 = %v; want the empty rendering to decode to zero bytes", *elems[1])
	}
}

// TestByteaArrayLeaf_RefusesUnrecognisedRenderings is item 135's discipline
// on the write side: the lane's provenance FIXES the rendering, so anything
// else is refused loudly rather than guessed at. The `escape` rendering is
// deliberately among the refusals — accepting both spellings is exactly the
// ambiguity item 135 removed from the read side.
func TestByteaArrayLeaf_RefusesUnrecognisedRenderings(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"escape rendering", `\000\377`, "hex"},
		{"bare text", "AB", "hex"},
		{"odd hex digits", `\xabc`, "parse bytea array element"},
		{"non-hex after prefix", `\xzz`, "parse bytea array element"},
		{"wrong Go type", 42, "expected []byte"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := convertArray([]any{tc.value}, ir.Blob{})
			if err == nil {
				t.Fatalf("convertArray accepted %v; an unrecognised rendering must refuse loudly", tc.value)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("refusal %q does not mention %q", err, tc.want)
			}
		})
	}
}
