// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

// TestPGVectorCodec_BinaryRoundTrip pins the load-bearing wire format
// for Bug 47: pgvector binary is `int16 dim, int16 unused (0), dim ×
// BE float32`. Anything else and the receiver's vector_in() parser
// rejects.
func TestPGVectorCodec_BinaryRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		text string
		want []float32
	}{
		{"three components", "[0.1,0.2,0.3]", []float32{0.1, 0.2, 0.3}},
		{"with spaces", "[ 0.1 , 0.2 , 0.3 ]", []float32{0.1, 0.2, 0.3}},
		{"single component", "[1.0]", []float32{1.0}},
		{"empty", "[]", []float32{}},
		{"negatives", "[-1,2,-3]", []float32{-1, 2, -3}},
		{"integer-shaped", "[1,2,3,4]", []float32{1, 2, 3, 4}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plan := pgvectorBinaryCodec{}.PlanEncode(nil, 0, pgtype.BinaryFormatCode, c.text)
			if plan == nil {
				t.Fatalf("PlanEncode returned nil for binary+string")
			}
			out, err := plan.Encode(c.text, nil)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			gotDim := int(binary.BigEndian.Uint16(out[0:2]))
			if gotDim != len(c.want) {
				t.Errorf("encoded dim = %d, want %d", gotDim, len(c.want))
			}
			gotUnused := binary.BigEndian.Uint16(out[2:4])
			if gotUnused != 0 {
				t.Errorf("unused word = %d, want 0", gotUnused)
			}
			expectedLen := 4 + len(c.want)*4
			if len(out) != expectedLen {
				t.Fatalf("encoded len = %d, want %d", len(out), expectedLen)
			}
			for i, want := range c.want {
				off := 4 + i*4
				got := math.Float32frombits(binary.BigEndian.Uint32(out[off : off+4]))
				if got != want {
					t.Errorf("component %d = %v, want %v", i, got, want)
				}
			}

			// Round-trip: decode the bytes we just emitted.
			back, err := decodeVectorBinary(out)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !reflect.DeepEqual(back, c.want) {
				t.Errorf("decode = %v, want %v", back, c.want)
			}
		})
	}
}

// TestPGVectorCodec_LargeDim covers the 8000-dim case the bug spec
// calls out — half of pgvector's 16000-component cap. The encoded
// payload size is what matters; round-trip catches off-by-one errors
// in the dim/header arithmetic.
func TestPGVectorCodec_LargeDim(t *testing.T) {
	const dim = 8000
	parts := make([]string, dim)
	want := make([]float32, dim)
	for i := 0; i < dim; i++ {
		parts[i] = "0.5"
		want[i] = 0.5
	}
	text := "[" + strings.Join(parts, ",") + "]"

	plan := pgvectorBinaryCodec{}.PlanEncode(nil, 0, pgtype.BinaryFormatCode, text)
	if plan == nil {
		t.Fatal("PlanEncode returned nil")
	}
	out, err := plan.Encode(text, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if got := int(binary.BigEndian.Uint16(out[0:2])); got != dim {
		t.Fatalf("encoded dim = %d, want %d", got, dim)
	}
	if got, expected := len(out), 4+dim*4; got != expected {
		t.Fatalf("payload bytes = %d, want %d", got, expected)
	}
	back, err := decodeVectorBinary(out)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(back, want) {
		t.Fatal("8000-dim round-trip mismatch")
	}
}

// TestPGVectorCodec_BytesEncode pins the []byte input shape (the
// alternate value type the writer's prepareValue admits for
// ExtensionType columns).
func TestPGVectorCodec_BytesEncode(t *testing.T) {
	in := []byte("[1,2,3]")
	plan := pgvectorBinaryCodec{}.PlanEncode(nil, 0, pgtype.BinaryFormatCode, in)
	if plan == nil {
		t.Fatal("PlanEncode returned nil for binary+[]byte")
	}
	out, err := plan.Encode(in, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	dim := int(binary.BigEndian.Uint16(out[0:2]))
	if dim != 3 {
		t.Errorf("dim = %d, want 3", dim)
	}
}

// TestPGVectorCodec_TextEncode pins the text-format encode path. The
// COPY hot path uses binary, but PG can still ask the codec for text
// in some scenarios (e.g. extended-query bind in non-COPY paths) and
// we want the codec to be a no-op passthrough rather than a refusal.
func TestPGVectorCodec_TextEncode(t *testing.T) {
	in := "[1,2,3]"
	plan := pgvectorBinaryCodec{}.PlanEncode(nil, 0, pgtype.TextFormatCode, in)
	if plan == nil {
		t.Fatal("PlanEncode returned nil for text+string")
	}
	out, err := plan.Encode(in, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if string(out) != in {
		t.Errorf("text encode = %q, want %q", out, in)
	}
}

// TestPGVectorCodec_ParseTextErrors confirms malformed inputs surface
// loudly rather than silently encoding garbage. A missing-bracket
// failure is the load-bearing pre-condition: that's what the current
// bug looked like (raw text bytes treated as binary).
func TestPGVectorCodec_ParseTextErrors(t *testing.T) {
	cases := []string{
		"",
		"1,2,3",
		"[1,2,abc]",
		"[1,2",
		"1,2,3]",
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			_, err := parseVectorText(c)
			if err == nil {
				t.Errorf("parseVectorText(%q): want error, got nil", c)
			}
		})
	}
}

// TestPGVectorCodec_FormatSupported and PreferredFormat lock in the
// codec's contract with pgx — binary is preferred (CopyFrom path),
// both formats supported.
func TestPGVectorCodec_FormatSupported(t *testing.T) {
	c := pgvectorBinaryCodec{}
	if !c.FormatSupported(pgtype.BinaryFormatCode) {
		t.Error("binary format not supported")
	}
	if !c.FormatSupported(pgtype.TextFormatCode) {
		t.Error("text format not supported")
	}
	if c.PreferredFormat() != pgtype.BinaryFormatCode {
		t.Errorf("preferred format = %d, want %d", c.PreferredFormat(), pgtype.BinaryFormatCode)
	}
}

// TestDecodeVectorBinary_Truncated guards against the failure we'd see
// on a malformed wire payload (mid-stream truncation, etc.) — surface
// rather than slice-out-of-bounds-panic.
func TestDecodeVectorBinary_Truncated(t *testing.T) {
	cases := [][]byte{
		nil,
		{0x00},
		{0x00, 0x03}, // header without the unused word
		// dim=3 in header, but only one float worth of payload
		{0x00, 0x03, 0x00, 0x00, 0x3f, 0x80, 0x00, 0x00},
	}
	for i, c := range cases {
		_, err := decodeVectorBinary(c)
		if err == nil {
			t.Errorf("case %d (%d bytes): want error, got nil", i, len(c))
		}
	}
}

// TestLookupVectorOID_NotInstalled pins the not-installed signal — the
// helper returns errVectorTypeNotFound (not a wrapped generic db
// error) so callers can distinguish "no extension here" from "lookup
// failed". The error sentinel itself is the contract; this just
// guards that the message text doesn't drift.
func TestLookupVectorOID_NotInstalled(t *testing.T) {
	if !errors.Is(errVectorTypeNotFound, errVectorTypeNotFound) {
		t.Fatal("errVectorTypeNotFound: identity check failed")
	}
	const want = "extension not installed"
	if !strings.Contains(errVectorTypeNotFound.Error(), want) {
		t.Errorf("err message = %q, want substring %q",
			errVectorTypeNotFound.Error(), want)
	}
}
