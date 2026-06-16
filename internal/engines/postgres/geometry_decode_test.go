// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Unit tests for decodePGGeometry's input-shape handling (Bug 147). The
// load-bearing case: a geometry value arriving as pgoutput TEXT-format
// []byte (hex-EWKB ASCII — the CDC read path) must decode IDENTICALLY to
// the cold-start string-hex path and to raw binary EWKB — never be
// mistaken for raw EWKB (the Bug-144 []byte-is-text trap). The real
// PostGIS round-trip is pinned by the integration matrices.

import (
	"bytes"
	"encoding/hex"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestDecodePGGeometry_InputShapesAgree pins that the three shapes a
// geometry value can arrive in — cold-start string (hex), CDC pgoutput
// []byte (hex ASCII), and raw binary EWKB — all decode to the SAME WKB.
func TestDecodePGGeometry_InputShapesAgree(t *testing.T) {
	rawEWKB := ewkbPointLE() // 25 bytes, leading byte-order 0x01
	hexStr := hex.EncodeToString(rawEWKB)

	fromString, err := decodePGGeometry(hexStr)
	if err != nil {
		t.Fatalf("decode string-hex: %v", err)
	}
	fromHexBytes, err := decodePGGeometry([]byte(hexStr)) // the CDC pgoutput shape
	if err != nil {
		t.Fatalf("decode []byte-hex (CDC path): %v", err)
	}
	fromRawBytes, err := decodePGGeometry(rawEWKB)
	if err != nil {
		t.Fatalf("decode raw EWKB []byte: %v", err)
	}

	sb, _ := fromString.([]byte)
	hb, _ := fromHexBytes.([]byte)
	rb, _ := fromRawBytes.([]byte)
	if sb == nil || hb == nil || rb == nil {
		t.Fatalf("decode did not return []byte: string=%T hexbytes=%T raw=%T", fromString, fromHexBytes, fromRawBytes)
	}
	if !bytes.Equal(hb, sb) {
		t.Errorf("CDC []byte-hex decoded differently from string-hex (Bug-144 trap):\n []byte=%x\n string=%x", hb, sb)
	}
	if !bytes.Equal(rb, sb) {
		t.Errorf("raw-EWKB []byte decoded differently from string-hex:\n raw=%x\n string=%x", rb, sb)
	}
	if len(sb) == 0 {
		t.Error("decoded WKB is empty")
	}
}

// TestDecodePGGeometry_HexPrefix: a "\x"-prefixed hex spelling (bytea-style
// escape) is accepted on both the string and []byte paths.
func TestDecodePGGeometry_HexPrefix(t *testing.T) {
	rawEWKB := ewkbPointLE()
	plain := hex.EncodeToString(rawEWKB)
	prefixed := `\x` + plain

	a, err := decodePGGeometry(prefixed)
	if err != nil {
		t.Fatalf("decode \\x string: %v", err)
	}
	b, err := decodePGGeometry([]byte(prefixed))
	if err != nil {
		t.Fatalf("decode \\x []byte: %v", err)
	}
	want, _ := decodePGGeometry(plain)
	if !bytes.Equal(a.([]byte), want.([]byte)) || !bytes.Equal(b.([]byte), want.([]byte)) {
		t.Error("\\x-prefixed hex did not decode equal to the bare hex form")
	}
}

// TestDecodePGGeometry_Errors: unsupported types and undecodable values
// fail loudly (no silent fallback).
func TestDecodePGGeometry_Errors(t *testing.T) {
	if _, err := decodePGGeometry(42); err == nil {
		t.Error("expected error decoding a non-string/[]byte geometry value")
	}
	// Odd-length, non-hex bytes are neither valid hex nor valid EWKB; the
	// odd length means isHexASCII=false → treated as raw EWKB → ewkbToWKB
	// must reject it loudly rather than silently producing garbage.
	if _, err := decodePGGeometry([]byte{0x01, 0x99, 0x7a}); err == nil {
		t.Error("expected error decoding malformed geometry bytes")
	}
}

// TestDecodePGGeometry_ViaDecodeValue: routing through decodeValue for an
// ir.Geometry column (the CDC decodeTuple entry point) reaches
// decodePGGeometry, and nil is preserved.
func TestDecodePGGeometry_ViaDecodeValue(t *testing.T) {
	hexBytes := []byte(hex.EncodeToString(ewkbPointLE()))
	got, err := decodeValue(hexBytes, ir.Geometry{})
	if err != nil {
		t.Fatalf("decodeValue(ir.Geometry): %v", err)
	}
	if b, ok := got.([]byte); !ok || len(b) == 0 {
		t.Errorf("decodeValue(ir.Geometry) = %T(%v); want non-empty []byte WKB", got, got)
	}
	if v, err := decodeValue(nil, ir.Geometry{}); err != nil || v != nil {
		t.Errorf("decodeValue(nil, Geometry) = (%v,%v); want (nil,nil)", v, err)
	}
}

func TestIsHexASCII(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want bool
	}{
		{"even lowercase hex", []byte("0101000020e6100000"), true},
		{"even uppercase hex", []byte("0101000020E6100000"), true},
		{"empty", []byte(""), false},
		{"odd length", []byte("abc"), false},
		{"non-hex char", []byte("01zz"), false},
		{"raw EWKB leading 0x01", ewkbPointLE(), false}, // 0x01 byte is not an ASCII hex digit
		{"raw WKB leading 0x00 (BE)", wkbPointBE(), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isHexASCII(c.in); got != c.want {
				t.Errorf("isHexASCII(%x) = %v; want %v", c.in, got, c.want)
			}
		})
	}
}
