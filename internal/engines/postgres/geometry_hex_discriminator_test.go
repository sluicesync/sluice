// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestGeometryHexDiscriminatorIsTotal is the written half of the
// `structurally-total` verdict the engine-package hex roster
// (internal/engines/hex_provenance_roster_test.go) records for
// [decodeGeometryHexOrRaw].
//
// bytea has no such argument — `\xdead` really is both readings, which is
// why item 135 had to carry provenance. Geometry is the family where the
// two readings PROVABLY cannot collide, and a claim like that is a
// hypothesis until something fails when it stops being true. What makes
// it total is the conjunction of two facts, and the test asserts the
// CONJUNCTION rather than each half:
//
//  1. [ewkbToWKB] accepts a leading byte-order flag of ONLY 0x00 or 0x01
//     and refuses everything else by name;
//  2. neither of those bytes is an ASCII hex digit, so no legal raw EWKB
//     can satisfy [isHexASCII].
//
// Pinning them apart would leave the argument that connects them unpinned
// (the 2026-08-01 lesson): if PostGIS ever grew a third byte-order flag
// that happened to be an ASCII hex digit, two separate tests would both
// stay green while the discriminator silently became ambiguous.
//
// Scope note: [decodeGeometryHexOrRaw] strips a leading `\x` BEFORE the
// discriminator runs, so the argument is about values that are legal
// geometry — which is the whole population PostGIS can emit. A raw
// byte sequence beginning 0x5C 0x78 is not legal EWKB (0x5C is not a
// byte-order flag), so it is refused rather than mis-read.
func TestGeometryHexDiscriminatorIsTotal(t *testing.T) {
	// Every byte value, partitioned by whether ewkbToWKB accepts it as a
	// leading byte-order flag. The oracle is ewkbToWKB itself running on
	// a minimal well-formed 5-byte WKB header, not a copy of its switch.
	for b := 0; b < 256; b++ {
		lead := byte(b)
		hdr := make([]byte, 5)
		hdr[0] = lead
		// A geomType with the SRID flag clear, so a valid header returns
		// the bytes unchanged rather than tripping the length check.
		binary.LittleEndian.PutUint32(hdr[1:5], 1)

		_, err := ewkbToWKB(hdr)
		accepted := err == nil

		hexDigit := isHexASCII([]byte{lead, lead})

		if accepted && hexDigit {
			t.Errorf(
				"byte-order flag 0x%02x is BOTH a legal EWKB leading byte and an ASCII hex digit — "+
					"the geometry discriminator is no longer total and decodeGeometryHexOrRaw needs "+
					"provenance the way bytea does (item 135)", lead,
			)
		}
		if accepted && lead != 0x00 && lead != 0x01 {
			t.Errorf(
				"ewkbToWKB accepted byte-order flag 0x%02x; the roster's structural argument "+
					"names 0x00/0x01 only", lead,
			)
		}
	}

	// The behavioural half: the two readings land on the right answer for
	// a value that exists in both spellings. Hex-EWKB text and the raw
	// bytes it spells must decode identically.
	raw := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef}
	fromRaw, err := decodeValueFromBinary(raw, ir.Geometry{})
	if err != nil {
		t.Fatalf("raw EWKB: %v", err)
	}
	fromHex, err := decodeValueFromBinary(hex.EncodeToString(raw), ir.Geometry{})
	if err != nil {
		t.Fatalf("hex-EWKB text: %v", err)
	}
	if !bytes.Equal(fromRaw.([]byte), fromHex.([]byte)) {
		t.Errorf("raw and hex spellings of the same geometry decoded differently: %#v vs %#v",
			fromRaw, fromHex)
	}
}
