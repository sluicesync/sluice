// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package blobcodec

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// numbersRow is the family matrix a postgres-trigger change payload hands
// the chunk codec: json.Number for a non-integer numeric scalar (long
// mantissa, trailing zeros, negative, an integer past int64), for numeric[]
// elements at 1-D and 2-D with a NULL element, and for every jsonb number
// leaf (integer leaves included — objects are not normalized) — beside the
// kinds the reader turns into typed values.
func numbersRow() ir.Row {
	return ir.Row{
		"id":    int64(7),
		"nu":    json.Number("123456789012345678.123456789012"),
		"long":  json.Number("0.1234567890123456789012345678901234567890"),
		"tz":    json.Number("1.500000"),
		"neg":   json.Number("-98765432109876543210.5"),
		"bigi":  json.Number("12345678901234567890"),
		"arr1":  []any{json.Number("1.10"), json.Number("22222222222222222222.2"), int64(3)},
		"arr2":  []any{[]any{json.Number("1.5"), json.Number("2.25")}, []any{json.Number("-3.125"), nil}},
		"doc":   map[string]any{"big": json.Number("12345678901234567890"), "dec": json.Number("0.100"), "nested": map[string]any{"i": json.Number("9223372036854775808")}, "s": "1.5"},
		"txt":   "1.5",
		"u64":   uint64(math.MaxUint64),
		"bytes": []byte{0x00, 0xff},
		"when":  time.Date(2026, 9, 25, 1, 2, 3, 4, time.UTC),
		"flag":  true,
		"nil":   nil,
	}
}

// readOneInsert writes row as one Insert and reads it back, optionally with
// PreserveNumbers.
func readOneInsert(t *testing.T, row ir.Row, preserve bool) ir.Row {
	t.Helper()
	buf := &bytes.Buffer{}
	w, err := NewChangeChunkWriter(buf, nil, CodecGzip, nil)
	if err != nil {
		t.Fatalf("NewChangeChunkWriter: %v", err)
	}
	if err := w.WriteChange(ir.Insert{Position: ir.Position{Engine: PreservedNumberEngine, Token: "1"}, Schema: "public", Table: "t", Row: row}); err != nil {
		t.Fatalf("WriteChange: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	r, err := NewChangeChunkReader(nopReadCloser{bytes.NewReader(buf.Bytes())}, w.Hash(), nil, CodecGzip, nil)
	if err != nil {
		t.Fatalf("NewChangeChunkReader: %v", err)
	}
	if preserve {
		r.PreserveNumbers()
	}
	c, err := r.ReadChange()
	if err != nil {
		t.Fatalf("ReadChange: %v", err)
	}
	if _, err := r.ReadChange(); !errors.Is(err, io.EOF) {
		t.Fatalf("second ReadChange = %v; want EOF", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("reader Close: %v", err)
	}
	ins, ok := c.(ir.Insert)
	if !ok {
		t.Fatalf("change = %T; want ir.Insert", c)
	}
	return ins.Row
}

// TestChangeChunk_PreserveNumbers_JSONNumberRoundTripsExactly pins the
// fix: with PreserveNumbers the reader hands back every json.Number the
// writer was given, byte-for-byte, at every depth, and leaves every other
// kind exactly as before. The independent expected value is the input row.
func TestChangeChunk_PreserveNumbers_JSONNumberRoundTripsExactly(t *testing.T) {
	in := numbersRow()
	got := readOneInsert(t, in, true)
	if !reflect.DeepEqual(got, in) {
		for k, v := range in {
			if !reflect.DeepEqual(got[k], v) {
				t.Errorf("column %q: got %#v; want %#v", k, got[k], v)
			}
		}
	}
}

// TestChangeChunk_DefaultDecode_RoundsJSONNumber is the negative control
// and the record of the defect: the SAME chunk bytes, read without
// PreserveNumbers, round a json.Number through float64. It is also what
// every other engine's change chunks keep: the default is unchanged.
func TestChangeChunk_DefaultDecode_RoundsJSONNumber(t *testing.T) {
	got := readOneInsert(t, numbersRow(), false)
	if f, ok := got["nu"].(float64); !ok || f != 123456789012345678.123456789012 {
		t.Fatalf("default decode of nu = %#v; want the float64 the pre-fix restore applied", got["nu"])
	}
	if doc, ok := got["doc"].(map[string]any); !ok || doc["big"] != float64(12345678901234567890) {
		t.Fatalf("default decode of doc.big = %#v; want a float64", got["doc"])
	}
}

// TestChangeChunk_PreserveNumbers_ReencodeIsStable pins smart compaction's
// shape: a change decoded with PreserveNumbers and written again produces
// the same line bytes, so a compacted chain keeps the exact digits.
func TestChangeChunk_PreserveNumbers_ReencodeIsStable(t *testing.T) {
	in := numbersRow()
	first := readOneInsert(t, in, true)
	second := readOneInsert(t, first, true)
	if !reflect.DeepEqual(second, in) {
		t.Fatalf("re-encoded row drifted:\n got %#v\nwant %#v", second, in)
	}
	a, err := json.Marshal(encodeRowValuesForTest(t, in))
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(encodeRowValuesForTest(t, first))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("wire bytes differ after a decode/re-encode:\n%s\n%s", a, b)
	}
}

func encodeRowValuesForTest(t *testing.T, r ir.Row) map[string]json.RawMessage {
	t.Helper()
	m, err := encodeRowValues(r, "row")
	if err != nil {
		t.Fatalf("encodeRowValues: %v", err)
	}
	return m
}

// TestChangeChunk_PreserveNumbers_FloatsComeBackAsExactText records the
// one type change the rule makes, on the one engine that opts in: a
// float64 (an ADD COLUMN fill captured through the row reader) decodes as
// the json.Number of its shortest rendering, which parses back to the
// same float64 — the value is unchanged.
func TestChangeChunk_PreserveNumbers_FloatsComeBackAsExactText(t *testing.T) {
	for _, f := range []float64{1.5, 0.1, -2.5e-300, 1.7976931348623157e308, 123456789.123456789} {
		got := readOneInsert(t, ir.Row{"f": f}, true)
		n, ok := got["f"].(json.Number)
		if !ok {
			t.Fatalf("f=%v decoded as %T; want json.Number", f, got["f"])
		}
		back, err := n.Float64()
		if err != nil || back != f {
			t.Fatalf("f=%v came back as %q (%v, %v)", f, n, back, err)
		}
	}
}

// TestNumbersArePreserved_EngineSet pins the opt-in's reach: the
// postgres-trigger engine name and nothing else.
func TestNumbersArePreserved_EngineSet(t *testing.T) {
	if !NumbersArePreserved("postgres-trigger") {
		t.Error("postgres-trigger change chunks must be read with PreserveNumbers")
	}
	for _, e := range []string{"", "postgres", "mysql", "mariadb", "planetscale", "vitess", "sqlite-trigger", "d1-trigger", "sqlite"} {
		if NumbersArePreserved(e) {
			t.Errorf("%q change chunks must keep the default decode", e)
		}
	}
}
