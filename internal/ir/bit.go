// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"fmt"
	"strconv"
)

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

// BitStringToUint64 parses a canonical '0'/'1' bit string (MSB first,
// ≤64 significant chars) into its unsigned integer value — the form
// go-sql-driver/mysql binds *reliably* to a MySQL BIT(N) column.
//
// Why integer and not the big-endian byte form (catalog Bug 77):
// binding the ceil(N/8) big-endian []byte (the natural BIT(N) storage
// layout) is NOT reliable through go-sql-driver. The driver sends a
// []byte parameter as a binary string and MySQL's string→BIT coercion
// raises `1264 (22003) Out of range value` for some byte patterns
// (e.g. a leading 0x2D) even when the value fits in N bits — while
// other patterns store correctly, so it is loud for some values and
// fine for others. Binding the integer value round-trips for every
// width 2..64 (MySQL caps BIT at 64, so does [Bit]) including the
// high-bit-set and all-ones boundaries. This mirrors what the LOAD
// DATA writer already does (`CAST(CONV(v,2,10) AS UNSIGNED)`).
//
// The empty string is 0 (an empty `bit varying` value). More than 64
// significant bits is a contract violation ([Bit] caps at 64) and
// surfaces loudly rather than silently truncating.
func BitStringToUint64(s string) (uint64, error) {
	if s == "" {
		return 0, nil
	}
	v, err := strconv.ParseUint(s, 2, 64)
	if err != nil {
		return 0, fmt.Errorf("ir: bit string %q is not a valid ≤64-bit binary value: %w", s, err)
	}
	return v, nil
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
