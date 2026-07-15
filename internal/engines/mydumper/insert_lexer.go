// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/engines/mysql"
	"sluicesync.dev/sluice/internal/ir"
)

// This file lexes mydumper data-chunk INSERT statements into row literals
// and converts each literal into the canonical [ir.Row] value for its
// column type. The string decoder is the live MySQL engine's
// ([mysql.ScanQuotedString] — the full backslash escape set, doubled
// quotes), which is the entire binary-fidelity story for the pscale dump
// shape (backslash-escaped binary, NO hex-blob). Numbers are carried as
// raw decimal text and parsed straight to int64/uint64/float64 by the
// column's type — never through an intermediate float (the D1 2^53
// lesson).

// literalKind classifies one lexed SQL value literal.
type literalKind uint8

const (
	litNull   literalKind = iota
	litNumber             // raw text in literal.text
	litString             // decoded bytes in literal.bytes
	litHex                // decoded bytes in literal.bytes (0x… / x'…')
	litBit                // decoded big-endian bytes in literal.bytes (b'…')
)

func (k literalKind) String() string {
	switch k {
	case litNull:
		return "NULL"
	case litNumber:
		return "number"
	case litString:
		return "string"
	case litHex:
		return "hex"
	case litBit:
		return "bit"
	default:
		return "unknown"
	}
}

// literal is one lexed value.
type literal struct {
	kind  literalKind
	text  string
	bytes []byte
}

// insertScan walks one INSERT statement's tuple list. Built by
// [parseInsertHeader]; advanced by [insertScan.nextTuple].
type insertScan struct {
	s    string
	i    int
	file string
}

func (sc *insertScan) skipSpace() {
	for sc.i < len(sc.s) {
		switch sc.s[sc.i] {
		case ' ', '\t', '\r', '\n':
			sc.i++
		default:
			return
		}
	}
}

// errAt builds a loud lex error naming the chunk file and byte offset.
func (sc *insertScan) errAt(format string, args ...any) error {
	near := sc.s[sc.i:]
	if len(near) > 24 {
		near = near[:24] + "…"
	}
	return fmt.Errorf("mydumper: %s: offset %d (near %q): %s",
		sc.file, sc.i, near, fmt.Sprintf(format, args...))
}

// scanIdent reads a backtick-quoted or bare identifier at the cursor.
func (sc *insertScan) scanIdent() (string, error) {
	sc.skipSpace()
	if sc.i < len(sc.s) && sc.s[sc.i] == '`' {
		name, end, ok := scanBacktickIdent(sc.s[sc.i:])
		if !ok {
			return "", sc.errAt("unterminated `identifier`")
		}
		sc.i += end
		return name, nil
	}
	start := sc.i
	for sc.i < len(sc.s) && isIdentByte(sc.s[sc.i]) {
		sc.i++
	}
	if sc.i == start {
		return "", sc.errAt("expected an identifier")
	}
	return sc.s[start:sc.i], nil
}

// acceptKeyword consumes the given bare keyword (case-insensitive) if it
// is next.
func (sc *insertScan) acceptKeyword(kw string) bool {
	sc.skipSpace()
	if len(sc.s)-sc.i < len(kw) || !strings.EqualFold(sc.s[sc.i:sc.i+len(kw)], kw) {
		return false
	}
	end := sc.i + len(kw)
	if end < len(sc.s) && isIdentByte(sc.s[end]) {
		return false
	}
	sc.i = end
	return true
}

func (sc *insertScan) acceptByte(c byte) bool {
	sc.skipSpace()
	if sc.i < len(sc.s) && sc.s[sc.i] == c {
		sc.i++
		return true
	}
	return false
}

// parseInsertHeader parses `INSERT [IGNORE] INTO tbl [(cols)] VALUES` (and
// the REPLACE INTO variant mydumper emits under --replace) and returns the
// scan positioned at the first tuple. columns is nil when the statement
// has no explicit column list.
func parseInsertHeader(stmt, file string) (sc *insertScan, table string, columns []string, err error) {
	sc = &insertScan{s: stripLeadingCommentsAndSpace(stmt), file: file}
	switch {
	case sc.acceptKeyword("INSERT"):
		_ = sc.acceptKeyword("IGNORE")
	case sc.acceptKeyword("REPLACE"):
	default:
		return nil, "", nil, sc.errAt("expected INSERT or REPLACE")
	}
	if !sc.acceptKeyword("INTO") {
		return nil, "", nil, sc.errAt("expected INTO")
	}
	table, err = sc.scanIdent()
	if err != nil {
		return nil, "", nil, err
	}
	if sc.acceptByte('.') { // `db`.`table` qualification
		table, err = sc.scanIdent()
		if err != nil {
			return nil, "", nil, err
		}
	}
	if sc.acceptByte('(') {
		for {
			col, err := sc.scanIdent()
			if err != nil {
				return nil, "", nil, err
			}
			columns = append(columns, col)
			if sc.acceptByte(',') {
				continue
			}
			if sc.acceptByte(')') {
				break
			}
			return nil, "", nil, sc.errAt("expected ',' or ')' in the column list")
		}
	}
	if !sc.acceptKeyword("VALUES") && !sc.acceptKeyword("VALUE") {
		return nil, "", nil, sc.errAt("expected VALUES")
	}
	return sc, table, columns, nil
}

// nextTuple lexes the next `(v, v, …)` tuple into vals (reused across
// calls — the caller consumes each tuple before requesting the next).
// done=true when the statement's tuple list is exhausted. Trailing text
// after the final tuple is a loud refusal (an ON DUPLICATE KEY clause or
// stray garbage is outside the mydumper data shape).
func (sc *insertScan) nextTuple(vals []literal) (out []literal, done bool, err error) {
	sc.skipSpace()
	if sc.i >= len(sc.s) {
		return vals, true, nil
	}
	if !sc.acceptByte('(') {
		return vals, false, sc.errAt("expected '(' to open a values tuple")
	}
	out = vals[:0]
	for {
		lit, err := sc.scanValue()
		if err != nil {
			return out, false, err
		}
		out = append(out, lit)
		if sc.acceptByte(',') {
			continue
		}
		if sc.acceptByte(')') {
			break
		}
		return out, false, sc.errAt("expected ',' or ')' in a values tuple")
	}
	if sc.acceptByte(',') {
		return out, false, nil // more tuples follow
	}
	sc.skipSpace()
	if sc.i < len(sc.s) {
		return out, false, sc.errAt("unexpected text after the final values tuple")
	}
	return out, false, nil
}

// scanValue lexes one SQL value literal at the cursor.
func (sc *insertScan) scanValue() (literal, error) {
	sc.skipSpace()
	if sc.i >= len(sc.s) {
		return literal{}, sc.errAt("unexpected end of statement in a values tuple")
	}
	s, i := sc.s, sc.i
	c := s[i]
	switch {
	case c == '\'':
		raw, end, ok := mysql.ScanQuotedString(s[i:])
		if !ok {
			return literal{}, sc.errAt("unterminated string literal")
		}
		sc.i += end
		return literal{kind: litString, bytes: raw}, nil
	case c == '"':
		raw, end, ok := scanSQLString(s[i:])
		if !ok {
			return literal{}, sc.errAt("unterminated string literal")
		}
		sc.i += end
		return literal{kind: litString, bytes: raw}, nil
	case c == '0' && i+1 < len(s) && (s[i+1] == 'x' || s[i+1] == 'X'):
		return sc.scanBareHexValue()
	case (c == 'x' || c == 'X') && i+1 < len(s) && s[i+1] == '\'':
		return sc.scanQuotedHexValue()
	case (c == 'b' || c == 'B') && i+1 < len(s) && s[i+1] == '\'':
		return sc.scanBitValue()
	case c == 'N' || c == 'n':
		if sc.acceptKeyword("NULL") {
			return literal{kind: litNull}, nil
		}
		return literal{}, sc.errAt("unsupported bare word in a values tuple")
	case c == 'C' || c == 'c':
		if sc.acceptKeyword("CONVERT") {
			return sc.scanConvertValue()
		}
		return literal{}, sc.errAt("unsupported value literal")
	case c == '_':
		// Charset introducer (`_binary "…"` — mydumper ≥1.0 prefixes
		// binary-safe values with it; mysqldump uses `_binary X'…'`).
		// UTF-8-compatible introducers are stripped and the literal lexed
		// normally; any other charset introducer means the bytes need
		// transcoding, which is refused loudly (ADR-0161 §5).
		word, err := sc.scanIdent()
		if err != nil {
			return literal{}, err
		}
		cs := strings.ToLower(strings.TrimPrefix(word, "_"))
		if !allowedSetNames[cs] && cs != "ascii" {
			return literal{}, sc.errAt("value carries the %s charset introducer — only UTF-8-compatible "+
				"introducers (_binary, _utf8, _utf8mb3, _utf8mb4, _ascii) are supported", word)
		}
		lit, err := sc.scanValue()
		if err != nil {
			return literal{}, err
		}
		if lit.kind == litNull || lit.kind == litNumber {
			return literal{}, sc.errAt("charset introducer %s must be followed by a string/hex/bit literal", word)
		}
		return lit, nil
	case c == '-' || c == '+' || c == '.' || (c >= '0' && c <= '9'):
		return sc.scanNumberValue()
	default:
		return literal{}, sc.errAt("unsupported value literal")
	}
}

// scanConvertValue lexes `CONVERT(<literal> USING <charset>)` (the
// CONVERT keyword already consumed) — the wrapper mydumper ≥1.0 emits
// around JSON values (ground-truthed against v1.0.3). The charset is held
// to the same UTF-8-compatible allowlist as SET NAMES / introducers; any
// other target charset would be a transcode this reader refuses (ADR-0161
// §5). The inner literal decodes unchanged.
func (sc *insertScan) scanConvertValue() (literal, error) {
	if !sc.acceptByte('(') {
		return literal{}, sc.errAt("expected '(' after CONVERT")
	}
	lit, err := sc.scanValue()
	if err != nil {
		return literal{}, err
	}
	if lit.kind == litNull || lit.kind == litNumber {
		return literal{}, sc.errAt("CONVERT must wrap a string/hex literal")
	}
	if !sc.acceptKeyword("USING") {
		return literal{}, sc.errAt("expected USING in a CONVERT value")
	}
	word, err := sc.scanIdent()
	if err != nil {
		return literal{}, err
	}
	cs := strings.ToLower(word)
	if !allowedSetNames[cs] && cs != "ascii" {
		return literal{}, sc.errAt("CONVERT(… USING %s) — only UTF-8-compatible conversion charsets "+
			"(binary, utf8, utf8mb3, utf8mb4, ascii) are supported", word)
	}
	if !sc.acceptByte(')') {
		return literal{}, sc.errAt("expected ')' to close a CONVERT value")
	}
	return lit, nil
}

// scanBareHexValue lexes a `0x…` hex literal at the cursor.
func (sc *insertScan) scanBareHexValue() (literal, error) {
	s, i := sc.s, sc.i
	j := i + 2
	for j < len(s) && isHexByte(s[j]) {
		j++
	}
	digits := s[i+2 : j]
	if digits == "" || len(digits)%2 != 0 {
		return literal{}, sc.errAt("malformed hex literal (need a non-empty, even number of digits)")
	}
	raw, err := hex.DecodeString(digits)
	if err != nil {
		return literal{}, sc.errAt("malformed hex literal: %v", err)
	}
	sc.i = j
	return literal{kind: litHex, bytes: raw}, nil
}

// scanQuotedHexValue lexes an `x'…'` hex literal at the cursor.
func (sc *insertScan) scanQuotedHexValue() (literal, error) {
	digits, end, err := scanQuotedDigits(sc.s[sc.i+1:], "0123456789abcdefABCDEF")
	if err != nil {
		return literal{}, sc.errAt("malformed hex literal: %v", err)
	}
	if len(digits)%2 != 0 {
		return literal{}, sc.errAt("malformed hex literal (odd digit count)")
	}
	raw, err := hex.DecodeString(digits)
	if err != nil {
		return literal{}, sc.errAt("malformed hex literal: %v", err)
	}
	sc.i += 1 + end
	return literal{kind: litHex, bytes: raw}, nil
}

// scanBitValue lexes a `b'…'` bit literal at the cursor.
func (sc *insertScan) scanBitValue() (literal, error) {
	digits, end, err := scanQuotedDigits(sc.s[sc.i+1:], "01")
	if err != nil {
		return literal{}, sc.errAt("malformed bit literal: %v", err)
	}
	raw, err := bitsToBytes(digits)
	if err != nil {
		return literal{}, sc.errAt("malformed bit literal: %v", err)
	}
	sc.i += 1 + end
	return literal{kind: litBit, bytes: raw}, nil
}

// scanNumberValue lexes a signed numeric literal at the cursor, keeping
// its raw decimal text.
func (sc *insertScan) scanNumberValue() (literal, error) {
	s, i := sc.s, sc.i
	c := s[i]
	j := i
	if c == '-' || c == '+' {
		j++
	}
	j = scanNumberEnd(s, j)
	if j == i || (j == i+1 && (c == '-' || c == '+' || c == '.')) {
		return literal{}, sc.errAt("malformed numeric literal")
	}
	text := s[i:j]
	sc.i = j
	return literal{kind: litNumber, text: text}, nil
}

// bitsToBytes packs a '0'/'1' digit string into big-endian, right-justified
// bytes — the same wire shape the MySQL driver hands back for BIT(N)
// columns, so [mysql.DecodeRowValue]'s BIT decoder consumes it unchanged.
// MySQL BIT holds at most 64 bits.
func bitsToBytes(bits string) ([]byte, error) {
	if bits == "" {
		return []byte{0}, nil
	}
	if len(bits) > 64 {
		return nil, fmt.Errorf("bit literal is %d bits; MySQL BIT holds at most 64", len(bits))
	}
	var v uint64
	for i := 0; i < len(bits); i++ {
		v = v<<1 | uint64(bits[i]-'0')
	}
	out := make([]byte, (len(bits)+7)/8)
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = byte(v)
		v >>= 8
	}
	return out, nil
}

// literalToRowValue converts one lexed literal into the canonical [ir.Row]
// value for column col: the literal is preconditioned into the driver
// shape the live MySQL engine's decoder expects for that IR type, then
// funnelled through [mysql.DecodeRowValue] so the dump path and the live
// path share one decode contract (docs/value-types.md). Any literal-kind ×
// type pairing outside the matrix below is a loud error naming the column
// and kinds — never a guessed coercion.
func literalToRowValue(lit literal, col *ir.Column) (any, error) {
	if lit.kind == litNull {
		return nil, nil
	}
	raw, err := literalToDriverShape(lit, col.Type)
	if err != nil {
		return nil, fmt.Errorf("column %q: %w", col.Name, err)
	}
	v, err := mysql.DecodeRowValue(raw, col.Type)
	if err != nil {
		return nil, fmt.Errorf("column %q: %w", col.Name, err)
	}
	return v, nil
}

// literalToDriverShape maps (literal kind × IR type family) to the raw Go
// value shape [mysql.DecodeRowValue] expects from the live driver.
func literalToDriverShape(lit literal, t ir.Type) (any, error) {
	switch v := t.(type) {
	case ir.Boolean:
		switch lit.kind {
		case litNumber:
			n, err := strconv.ParseInt(lit.text, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot parse %q as a boolean-int literal: %w", lit.text, err)
			}
			return n, nil
		case litString, litBit, litHex:
			// A quoted value on a Boolean column is a BIT(1)'s RAW WIRE
			// BYTE (mydumper's default escape shape emits `_binary "\0"` /
			// `"\x01"`), NOT boolean text — TINYINT(1) booleans always dump
			// as bare numbers. Route it through the same []byte branch the
			// live driver's BIT(1) takes (any non-zero byte = true); the
			// string branch would misread "\x00" as true (a real bug this
			// engine's real-dump oracle caught).
			return lit.bytes, nil
		}
	case ir.Integer:
		switch lit.kind {
		case litNumber, litString:
			text := lit.text
			if lit.kind == litString {
				text = string(lit.bytes)
			}
			return parseExactInteger(text, v.Unsigned)
		}
	case ir.Decimal:
		switch lit.kind {
		case litNumber:
			return lit.text, nil
		case litString:
			return string(lit.bytes), nil
		}
	case ir.Float:
		switch lit.kind {
		case litNumber, litString:
			text := lit.text
			if lit.kind == litString {
				text = string(lit.bytes)
			}
			f, err := strconv.ParseFloat(text, 64)
			if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
				return nil, fmt.Errorf("cannot parse %q as a float literal", text)
			}
			return f, nil
		}
	case ir.Char, ir.Varchar, ir.Text, ir.Time, ir.Interval, ir.Enum, ir.Set,
		ir.UUID, ir.Inet, ir.Cidr, ir.Macaddr:
		switch lit.kind {
		case litString, litHex:
			return string(lit.bytes), nil
		case litNumber:
			return lit.text, nil
		}
	case ir.Date, ir.DateTime, ir.Timestamp:
		if lit.kind == litString {
			return string(lit.bytes), nil
		}
	case ir.Binary, ir.Varbinary, ir.Blob, ir.JSON, ir.Geometry:
		switch lit.kind {
		case litString, litHex, litBit:
			return lit.bytes, nil
		}
	case ir.Bit:
		switch lit.kind {
		case litString, litHex, litBit:
			return lit.bytes, nil
		case litNumber:
			// A bit value dumped as a bare integer: repack as the wire bytes.
			n, err := strconv.ParseUint(lit.text, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot parse %q as a bit-int literal: %w", lit.text, err)
			}
			out := make([]byte, 8)
			for i := 7; i >= 0; i-- {
				out[i] = byte(n)
				n >>= 8
			}
			return out, nil
		}
	}
	return nil, fmt.Errorf("a %s literal cannot faithfully populate an IR %T column (refusing rather "+
		"than coercing)", lit.kind, t)
}

// parseExactInteger parses decimal integer text to int64, widening to
// uint64 only for an UNSIGNED column's values above MaxInt64 (BIGINT
// UNSIGNED's upper half — the [ir.Row] contract's uint64 case). A signed
// column's out-of-int64 value is a loud refusal, not a silent widen.
// Never a float round-trip.
func parseExactInteger(text string, unsigned bool) (any, error) {
	if n, err := strconv.ParseInt(text, 10, 64); err == nil {
		return n, nil
	}
	if unsigned && !strings.HasPrefix(text, "-") {
		if u, err := strconv.ParseUint(text, 10, 64); err == nil {
			return u, nil
		}
	}
	return nil, fmt.Errorf("cannot parse %q as a 64-bit integer literal", text)
}
