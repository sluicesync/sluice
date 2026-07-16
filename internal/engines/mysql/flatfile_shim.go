// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "sluicesync.dev/sluice/internal/ir"

// This file is the deliberately NARROW exported surface the mydumper
// flat-file source engine (internal/engines/mydumper, ADR-0161) reuses so
// its MySQL-dialect dump parsing shares this package's validated type
// mapping and value decoding instead of forking them. Everything here is a
// thin re-export of an existing pure function — no new behaviour, no state.
// Widening this surface (or adding I/O-bearing exports) should go through
// its own review; the point is that a dump of a MySQL database decodes
// through the SAME code the live MySQL reader uses, so a fix to either
// path is a fix to both.

// ColumnMeta is the exported alias of the information_schema-shaped column
// metadata [translateType] consumes. The mydumper schema parser fills it
// from a CREATE TABLE column definition instead of an information_schema
// row; the field contract (lowercased DataType, raw ColumnType spelling,
// nil-able length/precision pointers) is identical.
type ColumnMeta = columnMeta

// TranslateColumnType maps MySQL column metadata to an IR type — the
// exported face of [translateType]. Pure; unrecognised types error loudly.
func TranslateColumnType(c ColumnMeta) (ir.Type, error) {
	return translateType(c)
}

// DecodeRowValue converts a driver-shaped raw value (nil, int64, uint64,
// float64, string, []byte) into the canonical [ir.Row] value for the given
// IR type — the exported face of [decodeValue]. The mydumper reader
// preconditions each lexed SQL literal into the matching driver shape and
// funnels it through here so dump values land in the IR byte-identical to
// the live reader's (docs/value-types.md is the shared contract).
//
// MySQL zero/partial dates surface as the same loud error the live reader's
// refuse default produces; the mydumper engine does not (yet) thread a
// --zero-date policy (ADR-0161 §7).
func DecodeRowValue(raw any, t ir.Type) (any, error) {
	return decodeValue(raw, t)
}

// ScanQuotedString decodes the MySQL single-quoted string literal at the
// start of s (s[0] must be the opening quote) and reports the index of the
// first byte past the closing delimiter — the exported face of
// [scanMySQLQuotedString]. The full documented MySQL escape set
// (`\0 \b \t \n \r \Z \\ \' \"`, doubled quotes, unknown-escape
// passthrough) is honoured, which is exactly the fidelity contract the
// pscale-dump no-hex-blob binary shape rides on (ADR-0161 §4).
func ScanQuotedString(s string) (raw []byte, end int, ok bool) {
	return scanMySQLQuotedString(s)
}

// ScanDoubleQuotedString is [ScanQuotedString] for a DOUBLE-quoted literal
// (s[0] must be `"`) — the default string shape mydumper ≥1.0 emits. MySQL's
// escape grammar is symmetric in the two quote chars (doubled `""` → one `"`,
// a bare `'` rides through raw, backslash escapes are delimiter-independent),
// so this is the same scanner with the delimiter swapped, not a fork.
func ScanDoubleQuotedString(s string) (raw []byte, end int, ok bool) {
	return scanQuotedStringDelim(s, '"')
}

// NormalizeExpressionText folds MySQL stored-form expression text (backtick
// identifier quotes, charset introducers, C-style apostrophe escapes) toward
// portable SQL — the exported face of [normalizeMySQLExpressionText]. Used
// by the mydumper schema parser at the same read-boundary positions the
// live reader uses it (CHECK, generated-column, index expressions, DEFAULT
// expressions).
func NormalizeExpressionText(s string) string {
	return normalizeMySQLExpressionText(s)
}

// Dialect tags for [ir.DefaultExpression] values whose surface syntax is
// engine-specific but whose value is neutral, re-exported so the mydumper
// schema parser emits the exact tags the writers' default paths dispatch on.
const (
	// BitDefaultDialect tags a BIT(N>1) bit-literal default (`b'1010'`).
	BitDefaultDialect = bitLiteralDialect
	// HexDefaultDialect tags a BINARY/VARBINARY hex-literal default (`0x…`).
	HexDefaultDialect = hexLiteralDialect
)
