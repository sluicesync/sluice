// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"math"
	"testing"
)

// TestBitStringToUint64 pins the bit-string→integer conversion that
// the MySQL writer binds to a BIT(N) column (catalog Bug 77). The
// class: empty, small, byte-boundary, 16-bit high-bit-set, 64-bit
// all-ones, leading zeros, and the malformed / over-wide error paths.
func TestBitStringToUint64(t *testing.T) {
	ok := []struct {
		in   string
		want uint64
	}{
		{"", 0},
		{"0", 0},
		{"1", 1},
		{"1100", 12},
		{"01010010011", 659},                 // 11-bit "passing" Bug 77 value
		{"10110100110010", 11570},            // 14-bit value that raised 22003
		{"0010110100110110", 11574},          // 16-bit, leading zeros
		{"1111111111111111", math.MaxUint16}, // BIT(16) all-ones
		{"0000000000001100", 12},             // wide, leading zeros preserved-as-value
	}
	for _, c := range ok {
		got, err := BitStringToUint64(c.in)
		if err != nil {
			t.Errorf("BitStringToUint64(%q) unexpected err: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("BitStringToUint64(%q) = %d; want %d", c.in, got, c.want)
		}
	}

	// 64-bit all-ones is the max-width boundary (MySQL caps BIT at 64,
	// so does ir.Bit).
	allOnes64 := "1111111111111111111111111111111111111111111111111111111111111111"
	if got, err := BitStringToUint64(allOnes64); err != nil || got != math.MaxUint64 {
		t.Errorf("BitStringToUint64(64×'1') = %d, err=%v; want %d, nil", got, err, uint64(math.MaxUint64))
	}

	// Error paths: a non-'0'/'1' byte and a >64-bit string must fail
	// loudly, never silently truncate.
	bad := []string{
		"10201",         // '2' is not a bit
		"1100x",         // trailing junk
		"1" + allOnes64, // 65 significant bits — overflows uint64
	}
	for _, s := range bad {
		if got, err := BitStringToUint64(s); err == nil {
			t.Errorf("BitStringToUint64(%q) = %d, nil; want error", s, got)
		}
	}
}

// TestBitStringRoundTripUint pins that the canonical string survives
// string→uint round-trips at the widths the MySQL writer cares about,
// and that BitBytesToString (the MySQL→IR read direction, still used
// by the MySQL value decoder) is its faithful inverse.
func TestBitStringRoundTripUint(t *testing.T) {
	cases := []struct {
		s string
		n int
	}{
		{"1100", 4},
		{"10110100110010", 14},
		{"1111111111111111", 16},
		{"0000000000001100", 16},
	}
	for _, c := range cases {
		v, err := BitStringToUint64(c.s)
		if err != nil {
			t.Fatalf("BitStringToUint64(%q): %v", c.s, err)
		}
		// Render the integer back to the declared-width canonical
		// string the way the MySQL read path would (ceil(n/8) BE
		// bytes, right-justified) and confirm it equals the input
		// zero-extended to n bits.
		nbytes := (c.n + 7) / 8
		buf := make([]byte, nbytes)
		for i := 0; i < nbytes; i++ {
			buf[nbytes-1-i] = byte(v >> (8 * uint(i)))
		}
		got := BitBytesToString(buf, c.n)
		want := leftPadZeros(c.s, c.n)
		if got != want {
			t.Errorf("round-trip %q@%d: got %q; want %q", c.s, c.n, got, want)
		}
	}
}

func leftPadZeros(s string, n int) string {
	for len(s) < n {
		s = "0" + s
	}
	return s
}
