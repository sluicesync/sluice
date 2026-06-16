// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Unit tests for the PostGIS geometry applier codec (#20). These exercise
// the encode/decode DISPATCH (format × value-type) without a database; the
// real PostGIS round-trip — every geometry subtype × dimension × SRID — is
// pinned by the integration matrix in
// change_applier_pipelined_postgis_integration_test.go (the Bug-74
// family-coverage requirement for a family-dispatched codec).

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestGeometryCodec_FormatSupportAndPreferred(t *testing.T) {
	c := pgGeometryBinaryCodec{}
	if !c.FormatSupported(pgtype.BinaryFormatCode) || !c.FormatSupported(pgtype.TextFormatCode) {
		t.Fatal("geometry codec must support both binary and text formats")
	}
	if c.FormatSupported(42) {
		t.Error("geometry codec must not claim support for an unknown format")
	}
	if c.PreferredFormat() != pgtype.BinaryFormatCode {
		t.Error("geometry codec preferred format must be binary (EWKB verbatim)")
	}
}

// TestGeometryCodec_EncodeBinaryBytes: the load-bearing path — EWKB []byte
// (what prepareValue produces) is appended verbatim under binary format,
// which is exactly what PostGIS geometry_recv reads.
func TestGeometryCodec_EncodeBinaryBytes(t *testing.T) {
	c := pgGeometryBinaryCodec{}
	ewkb := ewkbPointLE()
	plan := c.PlanEncode(nil, 0, pgtype.BinaryFormatCode, ewkb)
	if plan == nil {
		t.Fatal("no binary encode plan for []byte")
	}
	got, err := plan.Encode(ewkb, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !bytes.Equal(got, ewkb) {
		t.Errorf("binary encode altered the EWKB bytes:\n got %x\nwant %x", got, ewkb)
	}
}

// TestGeometryCodec_EncodeTextBytes: under text format the EWKB is
// hex-encoded — the hex-EWKB spelling geometry_in accepts.
func TestGeometryCodec_EncodeTextBytes(t *testing.T) {
	c := pgGeometryBinaryCodec{}
	ewkb := ewkbPointLE()
	plan := c.PlanEncode(nil, 0, pgtype.TextFormatCode, ewkb)
	if plan == nil {
		t.Fatal("no text encode plan for []byte")
	}
	got, err := plan.Encode(ewkb, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if want := hex.EncodeToString(ewkb); string(got) != want {
		t.Errorf("text encode = %q; want hex-EWKB %q", got, want)
	}
}

// TestGeometryCodec_EncodeStringShapes: a hex-EWKB string value is robustly
// handled on both formats (binary → decode to bytes; text → passthrough).
func TestGeometryCodec_EncodeStringShapes(t *testing.T) {
	c := pgGeometryBinaryCodec{}
	ewkb := ewkbPointLE()
	hexStr := hex.EncodeToString(ewkb)

	binPlan := c.PlanEncode(nil, 0, pgtype.BinaryFormatCode, hexStr)
	if binPlan == nil {
		t.Fatal("no binary encode plan for string")
	}
	gotBin, err := binPlan.Encode(hexStr, nil)
	if err != nil {
		t.Fatalf("binary encode string: %v", err)
	}
	if !bytes.Equal(gotBin, ewkb) {
		t.Errorf("binary encode of hex string = %x; want %x", gotBin, ewkb)
	}

	txtPlan := c.PlanEncode(nil, 0, pgtype.TextFormatCode, hexStr)
	gotTxt, err := txtPlan.Encode(hexStr, nil)
	if err != nil {
		t.Fatalf("text encode string: %v", err)
	}
	if string(gotTxt) != hexStr {
		t.Errorf("text encode of hex string = %q; want passthrough %q", gotTxt, hexStr)
	}
}

// TestGeometryCodec_EmptyIsLoud: an empty geometry value is a translator
// bug; the codec must error loudly, never emit malformed/empty EWKB.
func TestGeometryCodec_EmptyIsLoud(t *testing.T) {
	c := pgGeometryBinaryCodec{}
	for _, tc := range []struct {
		name   string
		format int16
		value  any
	}{
		{"binary bytes", pgtype.BinaryFormatCode, []byte{}},
		{"text bytes", pgtype.TextFormatCode, []byte{}},
		{"binary string", pgtype.BinaryFormatCode, ""},
		{"text string", pgtype.TextFormatCode, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := c.PlanEncode(nil, 0, tc.format, tc.value)
			if plan == nil {
				t.Fatalf("no encode plan for %s", tc.name)
			}
			if _, err := plan.Encode(tc.value, nil); err == nil {
				t.Errorf("expected a loud error encoding empty geometry (%s), got nil", tc.name)
			}
		})
	}
}

// TestGeometryCodec_UnsupportedValueNoPlan: a non-[]byte/string value
// returns no plan (pgx then surfaces a clear "cannot encode" rather than
// the codec silently mangling it).
func TestGeometryCodec_UnsupportedValueNoPlan(t *testing.T) {
	c := pgGeometryBinaryCodec{}
	if p := c.PlanEncode(nil, 0, pgtype.BinaryFormatCode, 42); p != nil {
		t.Error("expected nil plan for an int value")
	}
	if p := c.PlanEncode(nil, 0, pgtype.TextFormatCode, 3.14); p != nil {
		t.Error("expected nil plan for a float value")
	}
}

// TestGeometryCodec_DecodeRoundTrip: binary decode returns the EWKB bytes
// (a copy), text decode hex-decodes — symmetric with the encode plans.
func TestGeometryCodec_DecodeRoundTrip(t *testing.T) {
	c := pgGeometryBinaryCodec{}
	ewkb := ewkbPointLE()

	gotBin, err := c.DecodeValue(nil, 0, pgtype.BinaryFormatCode, ewkb)
	if err != nil {
		t.Fatalf("decode binary: %v", err)
	}
	if b, ok := gotBin.([]byte); !ok || !bytes.Equal(b, ewkb) {
		t.Errorf("binary decode = %v; want EWKB %x", gotBin, ewkb)
	}

	gotTxt, err := c.DecodeValue(nil, 0, pgtype.TextFormatCode, []byte(hex.EncodeToString(ewkb)))
	if err != nil {
		t.Fatalf("decode text: %v", err)
	}
	if b, ok := gotTxt.([]byte); !ok || !bytes.Equal(b, ewkb) {
		t.Errorf("text decode = %v; want EWKB %x", gotTxt, ewkb)
	}

	if v, err := c.DecodeValue(nil, 0, pgtype.BinaryFormatCode, nil); err != nil || v != nil {
		t.Errorf("nil src decode = (%v,%v); want (nil,nil)", v, err)
	}
}

// TestGeometryCodec_DecodeDatabaseSQLValue: SQL scanners get a portable
// string (hex for binary, the raw text for text).
func TestGeometryCodec_DecodeDatabaseSQLValue(t *testing.T) {
	c := pgGeometryBinaryCodec{}
	ewkb := ewkbPointLE()
	got, err := c.DecodeDatabaseSQLValue(nil, 0, pgtype.BinaryFormatCode, ewkb)
	if err != nil {
		t.Fatalf("DecodeDatabaseSQLValue binary: %v", err)
	}
	if s, ok := got.(string); !ok || s != hex.EncodeToString(ewkb) {
		t.Errorf("sql value = %v; want hex string %q", got, hex.EncodeToString(ewkb))
	}
}
