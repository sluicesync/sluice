// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "fmt"

// IR-canonical form for [Bit] (PostgreSQL `bit`/`bit varying`, MySQL
// `BIT(N)`) values: a string of exactly the column's bit-length made
// up of ASCII '0' and '1', most-significant bit first (the same form
// PostgreSQL's `bit` text I/O and the literal `B'1010'` use).
//
// Why a bit-string string rather than raw bytes (catalog Bug 75):
// the two engines disagree on the byte layout — MySQL hands BIT(N)
// back as ceil(N/8) big-endian bytes with the value right-justified;
// PostgreSQL's wire/text form is left-justified and pgx stdlib mode
// surfaces it as the '0'/'1' text already. Carrying raw bytes through
// the IR made the contract ambiguous, and the PG reader's previous
// `[]byte(textForm)` decode silently corrupted every value (it stored
// the ASCII bytes of the digits, then the writer kept only the last
// one). A canonical bit-string is exact for any width, engine-neutral,
// and round-trips losslessly in all four directions. Readers convert
// their engine's wire shape into this form; writers convert it back.

// BitBytesToString renders ceil(n/8) big-endian, right-justified bytes
// (MySQL's BIT(n) storage layout) as the canonical n-character '0'/'1'
// string, most-significant bit first. n is the declared bit width.
//
// Only the low n bits of the big-endian value are significant; any
// unused high bits in the first byte are ignored. n==0 yields the
// empty string.
func BitBytesToString(src []byte, n int) string {
	if n <= 0 {
		return ""
	}
	out := make([]byte, n)
	// Bit i (0 = most-significant of the n-bit value) lives, in a
	// right-justified big-endian buffer of nbytes, at bit position
	// (n-1-i) counted from the LSB of the whole value.
	for i := 0; i < n; i++ {
		bitFromLSB := n - 1 - i
		byteIdx := len(src) - 1 - bitFromLSB/8
		if byteIdx < 0 {
			out[i] = '0'
			continue
		}
		if src[byteIdx]&(1<<uint(bitFromLSB%8)) != 0 {
			out[i] = '1'
		} else {
			out[i] = '0'
		}
	}
	return string(out)
}

// BitStringToBytesBE parses a canonical '0'/'1' bit string into
// ceil(len(s)/8) big-endian, right-justified bytes — the shape MySQL's
// driver accepts for a BIT(N) column. A non-'0'/'1' byte is a contract
// violation (an upstream decode bug) and surfaces as a loud error
// rather than a silently wrong value.
func BitStringToBytesBE(s string) ([]byte, error) {
	n := len(s)
	if n == 0 {
		return []byte{}, nil
	}
	nbytes := (n + 7) / 8
	out := make([]byte, nbytes)
	for i := 0; i < n; i++ {
		c := s[i]
		if c != '0' && c != '1' {
			return nil, fmt.Errorf("ir: malformed bit string %q (byte %q at offset %d is not '0'/'1')", s, c, i)
		}
		if c == '1' {
			bitFromLSB := n - 1 - i
			out[nbytes-1-bitFromLSB/8] |= 1 << uint(bitFromLSB%8)
		}
	}
	return out, nil
}

// BitStringToBytesPG parses a canonical '0'/'1' bit string into
// ceil(len(s)/8) bytes *left*-justified — bit 0 (the leftmost
// character) is the MSB of byte 0 — which is PostgreSQL's `bit(n)`
// binary wire layout (the buffer pgtype.Bits.Bytes expects). A
// non-'0'/'1' byte is a loud error.
func BitStringToBytesPG(s string) ([]byte, error) {
	n := len(s)
	if n == 0 {
		return []byte{}, nil
	}
	out := make([]byte, (n+7)/8)
	for i := 0; i < n; i++ {
		c := s[i]
		if c != '0' && c != '1' {
			return nil, fmt.Errorf("ir: malformed bit string %q (byte %q at offset %d is not '0'/'1')", s, c, i)
		}
		if c == '1' {
			out[i/8] |= byte(128 >> uint(i%8))
		}
	}
	return out, nil
}
