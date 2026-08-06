// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"sluicesync.dev/sluice/internal/ir"
)

// Roadmap item 145 — the json/jsonb array LEAF's input shapes.
//
// The shape matrix next door (row_writer_array_shape_test.go) runs this
// family through the dimensions/NULL/ragged cases alongside every other
// one, and TestMigrate_PGToPG_JSONArrays ground-truths the encode on a
// real target. What neither covers is the leaf's accepted INPUT set,
// which for this family is the load-bearing decision:
//
//   - []byte (and json.RawMessage), the IR-canonical leaf decodeBytes
//     produces on both provenance lanes.
//   - a PRESENT empty element is REFUSED, not normalised — the inverse of
//     [byteaArrayLeaf]'s wart, for a stated reason.
//   - every shape the pgtrigger change payload delivers is refused, because
//     to_jsonb embeds a json element AS JSON and two of the resulting
//     shapes are unrecoverable (JSON null vs SQL NULL; an array-valued
//     element vs a nested dimension). This is the one family where
//     accepting the trigger door would trade a loud refusal for silent
//     loss, so the refusals below are the fix, not a limitation of it.

func jsonArrayElements(t *testing.T, v any) []*[]byte {
	t.Helper()
	arr, ok := v.(pgtype.Array[*[]byte])
	if !ok {
		t.Fatalf("convertArray returned %T; want pgtype.Array[*[]byte] — a leaf pgx's JSONCodec does not "+
			"recognise is not refused, it is json.Marshal'ed, so the document would land as a base64 string", v)
	}
	return arr.Elements
}

func TestJSONArrayLeaf_CanonicalBytes(t *testing.T) {
	for _, elem := range []ir.Type{ir.JSON{}, ir.JSON{Binary: true}} {
		got, err := convertArray([]any{
			[]byte(`{"a":1}`),
			nil,
			json.RawMessage(`[1,2,3]`),
			[]byte(`null`),
		}, elem)
		if err != nil {
			t.Fatalf("convertArray(%#v): %v", elem, err)
		}
		elems := jsonArrayElements(t, got)
		if len(elems) != 4 {
			t.Fatalf("got %d elements, want 4", len(elems))
		}
		if !bytes.Equal(*elems[0], []byte(`{"a":1}`)) {
			t.Errorf("element 0 = %q; want {\"a\":1}", *elems[0])
		}
		if elems[1] != nil {
			t.Errorf("element 1 = %q; want a nil pointer (SQL NULL)", *elems[1])
		}
		if !bytes.Equal(*elems[2], []byte(`[1,2,3]`)) {
			t.Errorf("element 2 = %q; want [1,2,3] (json.RawMessage is []byte under a JSON name)", *elems[2])
		}
		// The JSON `null` DOCUMENT is a present element and must not collapse
		// into the SQL NULL slot above — the distinction the trigger door
		// cannot make, made here because the SQL door renders it as bytes.
		if elems[3] == nil {
			t.Fatalf("element 3 is a NULL pointer; the JSON document `null` is a PRESENT element")
		}
		if !bytes.Equal(*elems[3], []byte(`null`)) {
			t.Errorf("element 3 = %q; want the literal document null", *elems[3])
		}
	}
}

// TestJSONArrayLeaf_EmptyElementIsRefusedNotNulled is the WART pin, and it
// is deliberately the OPPOSITE resolution from the bytea arm's. pgx encodes
// a nil []byte for json as SQL NULL; an empty byte slice is not a valid
// JSON document, so there is no value to preserve and nothing to normalise
// TO — the only answers are "silently become NULL" and "refuse".
func TestJSONArrayLeaf_EmptyElementIsRefusedNotNulled(t *testing.T) {
	for _, in := range []any{[]byte(nil), []byte{}, json.RawMessage{}} {
		_, err := convertArray([]any{[]byte(`1`), in}, ir.JSON{Binary: true})
		if err == nil {
			t.Fatalf("convertArray accepted a present empty element (%#v); pgx would encode it as SQL NULL, "+
				"which is silent loss of the present/absent distinction", in)
		}
		if !strings.Contains(err.Error(), "not a valid JSON document") {
			t.Errorf("refusal %q does not explain the empty element", err)
		}
	}
}

// TestJSONArrayLeaf_RefusesTriggerPayloadShapes pins the decision that makes
// this arm safe. Each input below is what `to_jsonb(NEW)` + the pgtrigger
// UseNumber decode delivers for a json[]/jsonb[] column, and each must
// refuse — accepting them would land a re-encoded value (and, for the two
// shapes the leaf can never even see, silently the wrong one).
func TestJSONArrayLeaf_RefusesTriggerPayloadShapes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"decoded object", map[string]any{"a": int64(1)}},
		{"decoded string", "hello"},
		{"decoded integer", int64(7)},
		{"decoded number", json.Number("1.5")},
		{"decoded bool", true},
		{"decoded float", 1.5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := convertArray([]any{tc.value}, ir.JSON{})
			if err == nil {
				t.Fatalf("convertArray accepted %#v; the trigger-CDC payload shape must refuse loudly", tc.value)
			}
			if !strings.Contains(err.Error(), "expected []byte") {
				t.Errorf("refusal %q does not name the expected leaf shape", err)
			}
		})
	}
}
