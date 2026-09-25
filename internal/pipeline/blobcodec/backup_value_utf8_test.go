// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package blobcodec

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The invalid-UTF-8 family: each is a Go string encoding/json would
// rewrite (in part) to U+FFFD. Valid-by-accident bytes are not a family
// here — a codec cannot tell them from real text, which is why that
// feeder is fixed in the reader.
var invalidUTF8Family = map[string]string{
	"lone continuation 0x80": "a\x80b",
	"latin1 e-acute 0xE9":    "caf\xe9",
	"truncated multibyte":    "x\xe4\xb8",
	"overlong slash":         "\xc0\xaf",
	"surrogate half":         "\xed\xa0\x80",
	"lone 0xFF":              "\xff",
}

// The shapes a value reaches the codec in (encodeValue's text-bearing
// cases): scalar, list element, []string element, map value, map key.
func invalidUTF8Shapes(bad string) map[string]any {
	return map[string]any{
		"scalar":        bad,
		"in list":       []any{"ok", bad},
		"in []string":   []string{"ok", bad},
		"in map value":  map[string]any{"k": map[string]any{"deep": bad}},
		"as map key":    map[string]any{bad: "v"},
		"in list(list)": []any{[]any{int64(1), bad}},
	}
}

// Valid controls that must round-trip exactly — including a LITERAL
// U+FFFD, so the check cannot be "reject anything containing �".
var validUTF8Controls = map[string]any{
	"ascii":             "plain",
	"2-byte":            "café",
	"3-byte":            "中文",
	"4-byte":            "😀x",
	"literal U+FFFD":    "a�b",
	"U+2028":            "line sep",
	"nested":            map[string]any{"é": []any{"中", "😀"}},
	"[]string":          []string{"é", "�"},
	"bytes not text":    []byte("\xff\xfe\x00\xe9"), // binary is carried base64, never refused
	"empty":             "",
	"list with nil":     []any{nil, "é"},
	"deep valid string": []any{[]any{[]any{"ß"}}},
}

// TestChunkWriter_RefusesNonUTF8String grades the full-backup write core:
// every family × shape refuses loudly, names the column, carries the
// marker and is terminal. The independent expected value is the input
// itself: had it been written, it could not have come back.
func TestChunkWriter_RefusesNonUTF8String(t *testing.T) {
	cols := []*ir.Column{{Name: "id"}, {Name: "note"}}
	for fam, bad := range invalidUTF8Family {
		for shape, v := range invalidUTF8Shapes(bad) {
			t.Run(fam+"/"+shape, func(t *testing.T) {
				w, err := NewChunkWriter(&bytes.Buffer{}, []string{"id", "note"}, nil, CodecNone, nil)
				if err != nil {
					t.Fatal(err)
				}
				err = w.WriteRow(ir.Row{"id": int64(1), "note": v}, cols)
				assertNonUTF8Refusal(t, err, `"note`)
				if w.RowCount() != 0 {
					t.Errorf("a refused row was counted as written (%d)", w.RowCount())
				}
			})
		}
	}
}

// TestChangeChunkWriter_RefusesNonUTF8String grades the change-chunk write
// core for every image a change carries — Insert row, Update before- and
// after-image, Delete before-image (a key column included) — and that the
// refusal names the table.
func TestChangeChunkWriter_RefusesNonUTF8String(t *testing.T) {
	pos := ir.Position{Engine: "postgres", Token: "0/10"}
	for fam, bad := range invalidUTF8Family {
		for shape, v := range invalidUTF8Shapes(bad) {
			changes := map[string]ir.Change{
				"insert row": ir.Insert{Position: pos, Schema: "public", Table: "t", Row: ir.Row{"id": int64(1), "note": v}},
				"update after": ir.Update{
					Position: pos, Schema: "public", Table: "t",
					Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(1), "note": v},
				},
				"update before-image key": ir.Update{
					Position: pos, Schema: "public", Table: "t",
					Before: ir.Row{"note": v}, After: ir.Row{"note": "ok"},
				},
				"delete before-image key": ir.Delete{Position: pos, Schema: "public", Table: "t", Before: ir.Row{"note": v}},
			}
			for kind, c := range changes {
				t.Run(fam+"/"+shape+"/"+kind, func(t *testing.T) {
					w, err := NewChangeChunkWriter(&bytes.Buffer{}, nil, CodecNone, nil)
					if err != nil {
						t.Fatal(err)
					}
					err = w.WriteChange(c)
					assertNonUTF8Refusal(t, err, `"note`)
					if err != nil && !strings.Contains(err.Error(), `"public.t"`) {
						t.Errorf("refusal does not name the table: %v", err)
					}
					if w.ChangeCount() != 0 {
						t.Errorf("a refused change was counted as written (%d)", w.ChangeCount())
					}
				})
			}
		}
	}
}

// TestBackupCodec_ValidUTF8RoundTrips is the control: every valid shape,
// including a literal U+FFFD, and a []byte holding invalid bytes, is
// accepted by both write cores and read back equal to the input.
func TestBackupCodec_ValidUTF8RoundTrips(t *testing.T) {
	cols := []*ir.Column{{Name: "id"}, {Name: "v"}}
	pos := ir.Position{Engine: "postgres", Token: "0/10"}
	for name, v := range validUTF8Controls {
		t.Run(name+"/data chunk", func(t *testing.T) {
			var buf bytes.Buffer
			w, err := NewChunkWriter(&buf, []string{"id", "v"}, nil, CodecNone, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.WriteRow(ir.Row{"id": int64(1), "v": v}, cols); err != nil {
				t.Fatalf("valid value refused: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			r, err := NewChunkReader(nopReadCloserFromBytes(buf.Bytes()), w.Hash(), nil, CodecNone, nil)
			if err != nil {
				t.Fatal(err)
			}
			row, err := r.ReadRow()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(normalizeForCompare(row["v"]), normalizeForCompare(v)) {
				t.Errorf("round-trip = %#v; want %#v", row["v"], v)
			}
		})
		t.Run(name+"/change chunk", func(t *testing.T) {
			var buf bytes.Buffer
			w, err := NewChangeChunkWriter(&buf, nil, CodecNone, nil)
			if err != nil {
				t.Fatal(err)
			}
			in := ir.Update{Position: pos, Schema: "public", Table: "t", Before: ir.Row{"v": v}, After: ir.Row{"v": v}}
			if err := w.WriteChange(in); err != nil {
				t.Fatalf("valid value refused: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			r, err := NewChangeChunkReader(nopReadCloserFromBytes(buf.Bytes()), w.Hash(), nil, CodecNone, nil)
			if err != nil {
				t.Fatal(err)
			}
			c, err := r.ReadChange()
			if err != nil {
				t.Fatal(err)
			}
			up, ok := c.(ir.Update)
			if !ok {
				t.Fatalf("read %T; want ir.Update", c)
			}
			for img, row := range map[string]ir.Row{"before": up.Before, "after": up.After} {
				if !reflect.DeepEqual(normalizeForCompare(row["v"]), normalizeForCompare(v)) {
					t.Errorf("%s round-trip = %#v; want %#v", img, row["v"], v)
				}
			}
			if _, err := r.ReadChange(); !errors.Is(err, io.EOF) {
				t.Errorf("trailing read: %v; want EOF", err)
			}
		})
	}
}

func assertNonUTF8Refusal(t *testing.T, err error, column string) {
	t.Helper()
	if err == nil {
		t.Fatal("a non-UTF-8 string was accepted; the codec would have written it as U+FFFD")
	}
	msg := err.Error()
	if !strings.Contains(msg, nonUTF8ValueMarker) {
		t.Errorf("refusal lacks the %s marker: %v", nonUTF8ValueMarker, err)
	}
	if !strings.Contains(msg, column) {
		t.Errorf("refusal does not name column %s: %v", column, err)
	}
	if !ir.IsTerminal(err) {
		t.Errorf("refusal is not terminal (a backup stream retry would spin on it): %v", err)
	}
}

// normalizeForCompare maps the codec's documented decode shapes onto the
// input's: a []string reads back as []any of strings.
func normalizeForCompare(v any) any {
	switch x := v.(type) {
	case []string:
		out := make([]any, len(x))
		for i, s := range x {
			out[i] = s
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = normalizeForCompare(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = normalizeForCompare(e)
		}
		return out
	}
	return v
}
