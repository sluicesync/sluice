// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"encoding/hex"
	"testing"
)

// decodeMariaDBNative is a family-dispatched codec (uuid vs inet4 vs
// inet6), so per the Bug-74 discipline this pins EVERY family × shape, not
// one representative. The (hex, canonical-text) pairs below are GROUND
// TRUTH: the hex is the exact bytes go-mysql's RowsEvent delivered for the
// value on live mariadb:11.4 AND 10.11 (identical), and the text is that
// server's own SELECT rendering. The two load-bearing findings the ADR
// records are pinned here:
//
//   - NO byte reordering (uuid is canonical big-endian, not a UUID_TO_BIN
//     swap): 01234567-89ab-cdef-8123-... arrives as 0123456789abcdef8123...
//   - trailing-zero stripping: an all-zero value arrives EMPTY and a
//     trailing-zero value arrives short, so the decode right-pads to the
//     fixed width. The "short" rows below (received fewer bytes than the
//     width) are the discriminating cases that prove the pad.

func mustHex(t *testing.T, s string) string {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return string(b)
}

func TestDecodeMariaDBNativeUUID(t *testing.T) {
	// {receivedHex (trailing-zero-stripped as the binlog delivers), want}
	cases := []struct {
		name string
		hex  string
		want string
	}{
		{"canonical", "0123456789abcdef8123456789abcdef", "01234567-89ab-cdef-8123-456789abcdef"},
		{"all-zeros (arrives empty)", "", "00000000-0000-0000-0000-000000000000"},
		{"all-Fs", "ffffffffffffffffffffffffffffffff", "ffffffff-ffff-ffff-ffff-ffffffffffff"},
		{"trailing-zero (arrives 9 bytes)", "0123456789abcdef81", "01234567-89ab-cdef-8100-000000000000"},
		{"trailing-zero-Fs (arrives 14 bytes)", "ffffffffffffffffffffffffffff", "ffffffff-ffff-ffff-ffff-ffffffff0000"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := decodeMariaDBNative(mustHex(t, c.hex), mariadbNativeUUID)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got != c.want {
				t.Errorf("uuid %s: got %q; want %q", c.hex, got, c.want)
			}
		})
	}
}

func TestDecodeMariaDBNativeInet6(t *testing.T) {
	cases := []struct {
		name string
		hex  string
		want string
	}{
		{"full 8-group", "00010002000300040005000600070008", "1:2:3:4:5:6:7:8"},
		{"compressed", "20010db8000000000000000000000001", "2001:db8::1"},
		{"loopback", "00000000000000000000000000000001", "::1"},
		{"all-zeros (arrives empty)", "", "::"},
		{"ipv4-mapped", "00000000000000000000ffff01020304", "::ffff:1.2.3.4"},
		{"ipv4-mapped max", "00000000000000000000ffffffffffff", "::ffff:255.255.255.255"},
		{"ipv4-mapped zero", "00000000000000000000ffff00000000", "::ffff:0.0.0.0"},
		{"ipv4-compatible (dotted)", "00000000000000000000000001020304", "::1.2.3.4"},
		{"ipv4-compat 1.0.0.0", "00000000000000000000000001000000", "::1.0.0.0"},
		{"ipv4-compat 0.1.0.0", "00000000000000000000000000010000", "::0.1.0.0"},
		{"not-dotted small (::2)", "00000000000000000000000000000002", "::2"},
		{"not-dotted (::100)", "00000000000000000000000000000100", "::100"},
		{"not-dotted (::ffff)", "0000000000000000000000000000ffff", "::ffff"},
		{"trailing-zero (arrives 4 bytes)", "20010db8", "2001:db8::"},
		{"leading group", "00010000000000000000000000000000", "1::"},
		{"trailing group", "ffff0000000000000000000000000000", "ffff::"},
		{"two zero runs (leftmost wins)", "20010db8000000000001000000000001", "2001:db8::1:0:0:1"},
		{"nat64 prefix (not dotted)", "0064ff9b000000000000000001020304", "64:ff9b::102:304"},
		{"mixed hextets", "000a000b000c00000000000000010002", "a:b:c::1:2"},
		{"fe80 link-local", "fe800000000000000000000000000001", "fe80::1"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := decodeMariaDBNative(mustHex(t, c.hex), mariadbNativeInet6)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got != c.want {
				t.Errorf("inet6 %s: got %q; want %q", c.hex, got, c.want)
			}
		})
	}
}

func TestDecodeMariaDBNativeInet4(t *testing.T) {
	cases := []struct {
		name string
		hex  string
		want string
	}{
		{"dotted quad", "c0a8010a", "192.168.1.10"},
		{"all-zeros (arrives empty)", "", "0.0.0.0"},
		{"broadcast", "ffffffff", "255.255.255.255"},
		{"trailing-zero (arrives 1 byte)", "0a", "10.0.0.0"},
		{"trailing-zero 255 (arrives 1 byte)", "ff", "255.0.0.0"},
		{"trailing-zero 2 bytes", "0a01", "10.1.0.0"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := decodeMariaDBNative(mustHex(t, c.hex), mariadbNativeInet4)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got != c.want {
				t.Errorf("inet4 %s: got %q; want %q", c.hex, got, c.want)
			}
		})
	}
}

// TestDecodeMariaDBNative_NULL: a nil value (SQL NULL) passes through as nil
// for every family.
func TestDecodeMariaDBNative_NULL(t *testing.T) {
	for _, k := range []mariadbNativeKind{mariadbNativeUUID, mariadbNativeInet4, mariadbNativeInet6} {
		got, err := decodeMariaDBNative(nil, k)
		if err != nil {
			t.Fatalf("%s NULL: %v", k, err)
		}
		if got != nil {
			t.Errorf("%s NULL: got %#v; want nil", k, got)
		}
	}
}

// TestDecodeMariaDBNative_OverWidthRefused: a value with MORE significant
// bytes than the fixed storage width is a corruption signal and refuses
// loudly rather than truncating (loud-failure tenet). 17-byte uuid,
// 5-byte inet4.
func TestDecodeMariaDBNative_OverWidthRefused(t *testing.T) {
	if _, err := decodeMariaDBNative(mustHex(t, "0123456789abcdef8123456789abcdefaa"), mariadbNativeUUID); err == nil {
		t.Error("17-byte uuid: want refusal, got nil error")
	}
	if _, err := decodeMariaDBNative(mustHex(t, "c0a8010aff"), mariadbNativeInet4); err == nil {
		t.Error("5-byte inet4: want refusal, got nil error")
	}
	if _, err := decodeMariaDBNative(mustHex(t, "00000000000000000000ffff01020304aa"), mariadbNativeInet6); err == nil {
		t.Error("17-byte inet6: want refusal, got nil error")
	}
}

// TestDecodeMariaDBNative_ByteSliceInput: go-mysql delivers these as a Go
// string, but the decoder also accepts []byte defensively — same result.
func TestDecodeMariaDBNative_ByteSliceInput(t *testing.T) {
	b, _ := hex.DecodeString("0123456789abcdef8123456789abcdef")
	got, err := decodeMariaDBNative(b, mariadbNativeUUID)
	if err != nil {
		t.Fatalf("decode []byte: %v", err)
	}
	if got != "01234567-89ab-cdef-8123-456789abcdef" {
		t.Errorf("[]byte uuid: got %q", got)
	}
}

// TestMariaDBNativeKindOf pins the data_type → kind map (the loadTableSchema
// discriminator).
func TestMariaDBNativeKindOf(t *testing.T) {
	cases := map[string]mariadbNativeKind{
		"uuid":    mariadbNativeUUID,
		"inet4":   mariadbNativeInet4,
		"inet6":   mariadbNativeInet6,
		"varchar": mariadbNativeNone,
		"char":    mariadbNativeNone,
		"":        mariadbNativeNone,
	}
	for dt, want := range cases {
		if got := mariadbNativeKindOf(dt); got != want {
			t.Errorf("mariadbNativeKindOf(%q) = %v; want %v", dt, got, want)
		}
	}
}

// TestMariaDBNativeTypeRegistryCoupling is the A3 (audit 2026-07-17) pin:
// [mariadbNativeDataTypes] is the SINGLE source of truth coupling the
// schema-read ir.Type mapping (translateType) to the CDC binlog decoder
// ([mariadbNativeKindOf] → [decodeMariaDBNative]). It fails if the two
// ever drift — the exact silent-mis-decode-of-raw-binlog-bytes forward-
// fragility A3 flags: a native binary-storage type mapped by translateType
// without a decoder would stringify raw binlog bytes into a wrong value a
// CHAR/VARCHAR target silently accepts (the Bug-74 class). For every
// registry entry it asserts (1) translateType maps that data_type to the
// registry's ir.Type, (2) mariadbNativeKindOf returns the registry kind,
// (3) the kind is a real (non-None) native kind, and (4) decodeMariaDBNative
// actually has a decoder for that kind (never the "no native decoder"
// fall-through).
func TestMariaDBNativeTypeRegistryCoupling(t *testing.T) {
	if len(mariadbNativeDataTypes) == 0 {
		t.Fatal("mariadbNativeDataTypes is empty — the coupling registry vanished")
	}
	for dt, nt := range mariadbNativeDataTypes {
		// (3) every registered native type has a real decoder kind.
		if nt.kind == mariadbNativeNone {
			t.Errorf("registry[%q].kind = mariadbNativeNone — a registered native type MUST carry a decoder kind", dt)
		}
		// (2) mariadbNativeKindOf is driven by the registry.
		if got := mariadbNativeKindOf(dt); got != nt.kind {
			t.Errorf("mariadbNativeKindOf(%q) = %v; registry kind = %v — the schema-read discriminator drifted from the registry", dt, got, nt.kind)
		}
		// (1) translateType maps the data_type to the registry's ir.Type.
		got, err := translateType(columnMeta{DataType: dt})
		if err != nil {
			t.Errorf("translateType(%q) errored (%v) — a registry native type must be schema-mappable", dt, err)
			continue
		}
		if got.String() != nt.irType.String() {
			t.Errorf("translateType(%q) = %s; registry irType = %s — schema mapping drifted from the registry", dt, got.String(), nt.irType.String())
		}
		// (4) decodeMariaDBNative has a real decoder for the kind — a
		// single non-nil input byte proves it never hits the "no native
		// decoder for kind" fall-through (over-width is refused elsewhere).
		if _, err := decodeMariaDBNative([]byte{0x01}, nt.kind); err != nil {
			t.Errorf("decodeMariaDBNative(kind for %q) errored on a 1-byte value (%v) — the decoder is missing for a registered kind", dt, err)
		}
	}

	// A non-native data_type must NOT be in the registry (else it would
	// route real text bytes through the raw-storage decoder).
	if _, ok := mariadbNativeDataTypes["varchar"]; ok {
		t.Error("mariadbNativeDataTypes contains \"varchar\" — only native fixed-width storage types belong")
	}
}
