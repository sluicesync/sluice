// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Binlog CDC could not decode BIT(N>1) at all (audit 2026-08-05 B-3).
//
// go-mysql's replication/row_event.go documents its own wire contract as
// `MYSQL_TYPE_BIT: int64` — the binlog decoder hands BIT up as an INTEGER,
// while the row reader delivers the ceil(N/8) big-endian byte slice.
// decodeBit accepted only []byte and string, so the first change touching a
// BIT(N>1) column failed with "cannot decode int64 as Bit" and killed the
// sync — deterministic and loud, but only after the cold copy had already
// succeeded.
//
// BIT(1) escaped because the type mapper renders it as ir.Boolean and it never
// reaches decodeBit. The one width everybody tests was the one width immune.
//
// This is the unswept third sibling of the Bug 145/148 ENUM/SET int64 arms.

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestDecodeBit_BinlogInt64Wire covers the family × shape matrix that matters
// here: the two WIRE FORMS (row-reader []byte vs binlog int64) × a spread of
// declared widths including both byte boundaries and the 64-bit maximum.
//
// The expected value is independent of decodeBit: each case states the bit
// string directly, and the byte-form arm is cross-checked against the int64
// arm so the two wire shapes must agree.
func TestDecodeBit_BinlogInt64Wire(t *testing.T) {
	cases := []struct {
		name  string
		width int
		val   uint64
		want  string
	}{
		{"width2 zero", 2, 0, "00"},
		{"width2 one", 2, 1, "01"},
		{"width2 all", 2, 3, "11"},
		{"width3 mid", 3, 5, "101"},
		{"width8 boundary", 8, 0xA5, "10100101"},
		{"width9 crosses byte", 9, 0x155, "101010101"},
		{"width16", 16, 0xBEEF, "1011111011101111"},
		// 0x1BEEF = 1 1011 1110 1110 1111 (nibble-by-nibble: 1, B, E, E, F).
		{"width17 crosses two bytes", 17, 0x1BEEF, "11011111011101111"},
		{"width63", 63, 1<<62 | 1, "1" + zeros(61) + "1"},
		{"width64 max", 64, ^uint64(0), ones(64)},
		{"width64 msb only", 64, 1 << 63, "1" + zeros(63)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The BINLOG wire form — the arm that did not exist.
			got, err := decodeBit(int64(tc.val), tc.width)
			if err != nil {
				t.Fatalf("decodeBit(int64) failed — this is the defect: %v", err)
			}
			if got != tc.want {
				t.Errorf("decodeBit(int64(%#x), %d) = %q; want %q", tc.val, tc.width, got, tc.want)
			}

			// uint64 too — some driver configurations surface it unsigned.
			gotU, err := decodeBit(tc.val, tc.width)
			if err != nil {
				t.Fatalf("decodeBit(uint64): %v", err)
			}
			if gotU != tc.want {
				t.Errorf("decodeBit(uint64(%#x), %d) = %q; want %q", tc.val, tc.width, gotU, tc.want)
			}

			// The ROW-READER wire form must agree with the binlog one. Two
			// wire shapes, one value: if they ever disagree, a table copied by
			// the cold copy and then updated over CDC would carry two
			// different renderings of the same bits.
			be := bitUint64Bytes(tc.val)
			nbytes := (tc.width + 7) / 8
			gotBytes, err := decodeBit(be[len(be)-nbytes:], tc.width)
			if err != nil {
				t.Fatalf("decodeBit([]byte): %v", err)
			}
			if gotBytes != got {
				t.Errorf("row-reader form decoded to %q but binlog form to %q — the two wire "+
					"shapes of one value must agree", gotBytes, got)
			}
		})
	}
}

// The value must survive the round trip back to the form the writer binds,
// using the pre-existing inverse rather than a hand-rolled one.
func TestDecodeBit_BinlogInt64RoundTrips(t *testing.T) {
	for _, v := range []uint64{0, 1, 0xA5, 0xBEEF, 1 << 62, ^uint64(0)} {
		s, err := decodeBit(int64(v), 64)
		if err != nil {
			t.Fatalf("decodeBit(%#x): %v", v, err)
		}
		back, err := ir.BitStringToUint64(s.(string))
		if err != nil {
			t.Fatalf("BitStringToUint64(%q): %v", s, err)
		}
		if back != v {
			t.Errorf("round trip of %#x produced %#x (via %q)", v, back, s)
		}
	}
}

// An unsupported wire type must still refuse loudly rather than silently
// producing a wrong bit string — the arms added here must not become a
// catch-all.
func TestDecodeBit_UnknownWireTypeStillRefuses(t *testing.T) {
	if _, err := decodeBit(3.5, 8); err == nil {
		t.Error("decodeBit accepted a float64; an unrecognised wire form must refuse loudly")
	}
}

func zeros(n int) string { return repeat('0', n) }
func ones(n int) string  { return repeat('1', n) }

func repeat(c byte, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = c
	}
	return string(b)
}
