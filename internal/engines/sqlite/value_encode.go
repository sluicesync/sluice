// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// This file is the WRITE-side inverse of value_decode.go: it turns an
// ir.Row value (per docs/value-types.md) into the Go value the modernc
// driver binds for a SQLite TARGET, and refuses LOUDLY any value SQLite
// cannot faithfully hold rather than letting the engine silently coerce
// it (ADR-0134 §2). The write side ALWAYS writes canonical ISO temporal
// text (the reader's default `iso` encoding, so a re-read recovers it);
// --sqlite-date-encoding is a READ concern only.

// sqliteTimestampLayout is the canonical write form for ir.Timestamp /
// ir.DateTime values: a tz-naive `YYYY-MM-DD HH:MM:SS[.fractional]`,
// emitted in UTC. The `.999999999` keeps full sub-second precision and
// drops trailing zeros (and the dot when the fraction is zero). It is one
// of the reader's accepted isoDateTimeLayouts, so a written value
// round-trips back to the same instant.
const sqliteTimestampLayout = "2006-01-02 15:04:05.999999999"

// sqliteDateLayout is the canonical write form for ir.Date values.
const sqliteDateLayout = "2006-01-02"

// sqliteTimeLayout is the canonical write form for an ir.Time value that
// arrives as a time.Time (the contract carries ir.Time as a string, but
// some target writers receive a time.Time — handle both).
const sqliteTimeLayout = "15:04:05.999999999"

// encodeValue converts one ir.Row value to its SQLite driver binding for
// the column's IR type, or returns a loud refusal naming the column for a
// value SQLite cannot faithfully store. NULL (nil) is faithful for every
// type. The row writer wraps the error with the table name.
func encodeValue(col *ir.Column, v any) (any, error) {
	if v == nil {
		return nil, nil // NULL — faithful for every IR type.
	}
	switch col.Type.(type) {
	case ir.Boolean:
		return encodeBoolean(col, v)
	case ir.Integer:
		return encodeInteger(col, v)
	case ir.Float:
		if f, ok := v.(float64); ok {
			return f, nil
		}
		return nil, encodeMismatch(col, v, "a float64")
	case ir.Decimal:
		return encodeDecimal(col, v)
	case ir.Char, ir.Varchar, ir.Text, ir.UUID, ir.Enum:
		return encodeText(col, v)
	case ir.JSON:
		// SQLite has no JSON type; the column is emitted TEXT. The value
		// MUST bind as a string (not []byte) — a []byte bound to a TEXT
		// column stays BLOB storage, which the reader would refuse on
		// read-back. string(bytes) stores TEXT and round-trips as ir.Text.
		return encodeText(col, v)
	case ir.Set:
		return encodeSet(col, v)
	case ir.Binary, ir.Varbinary, ir.Blob:
		if b, ok := v.([]byte); ok {
			return b, nil
		}
		return nil, encodeMismatch(col, v, "a []byte")
	case ir.Date:
		return encodeDate(col, v)
	case ir.DateTime, ir.Timestamp:
		return encodeTimestamp(col, v)
	case ir.Time:
		return encodeTime(col, v)
	default:
		// emitColumnType already refused this type at schema-write, so a
		// value should never reach here — but be loud, not silent.
		return nil, fmt.Errorf(
			"sqlite: column %q: no value encoder for IR type %s", col.Name, col.Type.String(),
		)
	}
}

// encodeBoolean maps a bool to SQLite's 0/1 INTEGER (the form
// decodeBoolean reads back). A non-bool value is an upstream contract
// violation, refused loudly.
func encodeBoolean(col *ir.Column, v any) (any, error) {
	b, ok := v.(bool)
	if !ok {
		return nil, encodeMismatch(col, v, "a bool")
	}
	if b {
		return int64(1), nil
	}
	return int64(0), nil
}

// encodeInteger maps an integer value to int64. An unsigned value beyond
// int64's range (uint64 > MaxInt64, e.g. a MySQL BIGINT UNSIGNED above
// 2^63) cannot be stored in SQLite's signed 64-bit INTEGER and is REFUSED
// LOUDLY rather than wrapped to a negative — SQLite has no unsigned type.
func encodeInteger(col *ir.Column, v any) (any, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case uint64:
		if n > math.MaxInt64 {
			return nil, fmt.Errorf(
				"sqlite: column %q: unsigned integer value %d exceeds SQLite's signed 64-bit "+
					"INTEGER range (SQLite has no unsigned type); refusing to wrap it to a negative",
				col.Name, n,
			)
		}
		return int64(n), nil
	case int:
		return int64(n), nil
	case int32:
		return int64(n), nil
	default:
		return nil, encodeMismatch(col, v, "an int64/uint64")
	}
}

// encodeText maps a string (or, defensively, []byte) to a string binding
// so SQLite stores TEXT.
func encodeText(col *ir.Column, v any) (any, error) {
	switch s := v.(type) {
	case string:
		return s, nil
	case []byte:
		return string(s), nil
	default:
		return nil, encodeMismatch(col, v, "a string")
	}
}

// encodeSet joins an ir.Set value ([]string) into the comma-separated TEXT
// form (the same shape the MySQL writer uses for SET columns).
func encodeSet(col *ir.Column, v any) (any, error) {
	switch s := v.(type) {
	case []string:
		return strings.Join(s, ","), nil
	case string:
		return s, nil
	default:
		return nil, encodeMismatch(col, v, "a []string")
	}
}

// encodeDecimal binds a decimal string after guarding against SQLite's
// silent NUMERIC-affinity precision loss (the named refuse-decimal-
// beyond-float64 wart, ADR-0134 §2). A decimal that fits int64 stores
// EXACTLY (INTEGER); any other decimal is coerced to REAL on insert, which
// round-trips losslessly only up to float64's 15-significant-digit
// guarantee. A decimal beyond that is REFUSED LOUDLY (SQLite cannot hold
// it without loss) — the escape hatch is to carry the column as text.
func encodeDecimal(col *ir.Column, v any) (any, error) {
	s, ok := v.(string)
	if !ok {
		return nil, encodeMismatch(col, v, "a decimal string")
	}
	if !decimalFitsSQLite(s) {
		return nil, fmt.Errorf(
			"sqlite: column %q: decimal value %q exceeds SQLite's exact storage range "+
				"(NUMERIC affinity stores non-integer decimals as float64, losing precision beyond "+
				"~15 significant digits); refusing to store it lossily — carry the column as text "+
				"(--type-override <col>=text) to preserve it byte-exact",
			col.Name, s,
		)
	}
	// Bind the string; SQLite's NUMERIC affinity coerces an integer-valued
	// decimal to an exact INTEGER and a guarded fractional one to a
	// round-trippable REAL.
	return s, nil
}

// decimalFitsSQLite reports whether a decimal string survives SQLite's
// NUMERIC-affinity coercion losslessly: an int64-range integer is exact
// (INTEGER storage); otherwise the value must parse as a float and carry
// at most 15 significant digits (float64's DBL_DIG round-trip guarantee).
func decimalFitsSQLite(s string) bool {
	s = strings.TrimSpace(s)
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(s, 64); err != nil {
		return false // not a number we can faithfully store
	}
	return decimalSignificantDigits(s) <= 15
}

// decimalSignificantDigits counts the significant decimal digits of s —
// the digits between the first and last non-zero digit, ignoring sign,
// the decimal point, any exponent, and leading/trailing zeros. Used by
// [decimalFitsSQLite] to bound a fractional decimal against float64's
// 15-digit round-trip guarantee.
func decimalSignificantDigits(s string) int {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "+")
	s = strings.TrimPrefix(s, "-")
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		s = s[:i] // drop the exponent — it scales, not adds precision
	}
	s = strings.Replace(s, ".", "", 1)
	s = strings.TrimLeft(s, "0")
	s = strings.TrimRight(s, "0")
	return len(s)
}

// encodeDate formats an ir.Date value (time.Time, UTC midnight per the
// value contract) as `YYYY-MM-DD` TEXT.
func encodeDate(col *ir.Column, v any) (any, error) {
	switch t := v.(type) {
	case time.Time:
		return t.UTC().Format(sqliteDateLayout), nil
	case string:
		return t, nil
	default:
		return nil, encodeMismatch(col, v, "a time.Time")
	}
}

// encodeTimestamp formats an ir.Timestamp / ir.DateTime value as canonical
// UTC ISO TEXT. A tz-aware source timestamp arrives already normalized to
// UTC (the value contract), so storing the UTC instant is instant-faithful
// — the original display zone is dropped (SQLite is tz-naive, ADR-0134).
func encodeTimestamp(col *ir.Column, v any) (any, error) {
	switch t := v.(type) {
	case time.Time:
		return t.UTC().Format(sqliteTimestampLayout), nil
	case string:
		return t, nil
	default:
		return nil, encodeMismatch(col, v, "a time.Time")
	}
}

// encodeTime binds an ir.Time value. The value contract carries it as a
// time-of-day string, written verbatim; a time.Time (some writers) is
// formatted to the canonical time-of-day text.
func encodeTime(col *ir.Column, v any) (any, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case time.Time:
		return t.UTC().Format(sqliteTimeLayout), nil
	default:
		return nil, encodeMismatch(col, v, "a time-of-day string")
	}
}

// encodeMismatch builds the loud refusal for a value whose Go type doesn't
// match the column's IR type contract (an upstream bug, not source data).
func encodeMismatch(col *ir.Column, v any, want string) error {
	return fmt.Errorf(
		"sqlite: column %q (IR %s): value %#v (%T) is not %s as the value contract requires; "+
			"refusing to coerce",
		col.Name, col.Type.String(), v, v, want,
	)
}
