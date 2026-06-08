// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// decodeValue converts a single value as returned by the go-sql-driver/mysql
// driver into the canonical Go type the IR uses for the given column type.
//
// SQL NULL is represented as a nil interface value, both as input and as
// output. Callers must therefore allow nil values for nullable columns.
//
// The function is pure (no I/O, no shared state) and exhaustively
// table-tested in value_decode_test.go.
func decodeValue(raw any, t ir.Type) (any, error) {
	if raw == nil {
		return nil, nil
	}

	switch v := t.(type) {
	case ir.Boolean:
		return decodeBoolean(raw)
	case ir.Integer:
		return decodeInteger(raw)
	case ir.Decimal:
		return decodeDecimal(raw)
	case ir.Float:
		return decodeFloat(raw)
	case ir.Char, ir.Varchar, ir.Text:
		return decodeString(raw)
	case ir.Binary, ir.Varbinary, ir.Blob:
		return decodeBytes(raw)
	case ir.Bit:
		// catalog Bug 75: MySQL's driver hands BIT(N) back as a byte
		// slice (big-endian, ceil(N/8) bytes, value right-justified).
		// The IR contract for ir.Bit is the canonical '0'/'1'
		// bit-string (engine-neutral; see internal/ir/bit.go) so the
		// value round-trips faithfully to a PG or MySQL target. The
		// prior raw-bytes path made the IR's bit representation engine-
		// specific and was silently corrupted by the PG writer.
		return decodeBit(raw, v.Length)
	case ir.Date, ir.DateTime, ir.Timestamp:
		return decodeTime(raw)
	case ir.Time:
		return decodeString(raw)
	case ir.JSON:
		return decodeBytes(raw)
	case ir.Enum:
		return decodeString(raw)
	case ir.Set:
		return decodeSet(raw)
	case ir.Geometry:
		// MySQL stores geometry on the wire as `<srid uint32 LE><wkb>`.
		// The IR contract for ir.Geometry values is "raw WKB" (per
		// docs/value-types.md), so strip the 4-byte SRID prefix before
		// returning. Per-row SRID is intentionally lost here — the
		// per-column SRID lives on ir.Geometry.SRID and is set at
		// schema-translation time, not per-row at decode time.
		return decodeMySQLGeometry(raw)
	case ir.UUID, ir.Inet, ir.Cidr, ir.Macaddr:
		// MySQL doesn't have native types for these; they live in
		// VARCHAR columns. The driver gives us bytes; canonical form
		// is string.
		return decodeString(raw)
	}
	return nil, fmt.Errorf("mysql: no decoder for IR type %T", t)
}

// decodeBoolean accepts the various integer widths the row source can
// produce — database/sql widens everything to int64/uint64, but the
// binlog reader hands back native widths (int8 for TINYINT, etc.) —
// plus bool, BIT(1) bytes, and string fallbacks. Non-zero numeric or
// non-empty non-zero byte sequence is true; anything else is false.
func decodeBoolean(raw any) (any, error) {
	switch v := raw.(type) {
	case int64:
		return v != 0, nil
	case int32:
		return v != 0, nil
	case int16:
		return v != 0, nil
	case int8:
		return v != 0, nil
	case int:
		return v != 0, nil
	case uint64:
		return v != 0, nil
	case uint32:
		return v != 0, nil
	case uint16:
		return v != 0, nil
	case uint8:
		return v != 0, nil
	case uint:
		return v != 0, nil
	case bool:
		return v, nil
	case []byte:
		// BIT(1) returns a single byte. Non-zero is true.
		for _, b := range v {
			if b != 0 {
				return true, nil
			}
		}
		return false, nil
	case string:
		return v != "" && v != "0", nil
	}
	return nil, fmt.Errorf("mysql: cannot decode %T as Boolean", raw)
}

// decodeInteger normalises the various integer widths a MySQL row
// source can produce into int64/uint64. database/sql already widens
// to int64/uint64 for us, but the binlog reader returns native widths
// (int8 for TINYINT, int32 for INT, etc.); both paths land here.
func decodeInteger(raw any) (any, error) {
	switch v := raw.(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int:
		return int64(v), nil
	case uint64:
		return v, nil
	case uint32:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	case []byte:
		// Some MySQL builds return integers as bytes for very large
		// values. We keep them as bytes — callers can parse on demand.
		return v, nil
	}
	return nil, fmt.Errorf("mysql: cannot decode %T as Integer", raw)
}

// decodeDecimal preserves the textual precision of a DECIMAL column
// by returning it as a string. Avoids the precision loss that float
// conversion would introduce.
//
// The integer cases carry a `BIGINT UNSIGNED` column that an operator
// overrode to a wide DECIMAL to preserve the full unsigned-64 range
// (`--type-override TABLE.COL=decimal:precision=20,scale=0`): go-sql-driver
// returns uint64 for values above 2^63-1 (and int64 otherwise), and the
// default `bigint` mapping can't hold them. Rendering the integer as its
// exact decimal text keeps the value lossless into PG `numeric(20,0)`.
func decodeDecimal(raw any) (any, error) {
	switch v := raw.(type) {
	case []byte:
		return string(v), nil
	case string:
		return v, nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	}
	return nil, fmt.Errorf("mysql: cannot decode %T as Decimal", raw)
}

// decodeFloat returns float64. Single-precision FLOAT columns are
// widened to double-precision Go floats — there is no information loss
// in this direction.
func decodeFloat(raw any) (any, error) {
	switch v := raw.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	}
	return nil, fmt.Errorf("mysql: cannot decode %T as Float", raw)
}

// decodeString converts a string-like driver value into a Go string.
//
// The integer cases let a `BIGINT UNSIGNED` column be carried as TEXT when
// an operator overrides it (`--type-override TABLE.COL=text`): go-sql-driver
// returns uint64/int64 for an integer column, which the bare []byte/string
// branches can't consume. Rendering the exact decimal text is lossless.
func decodeString(raw any) (any, error) {
	switch v := raw.(type) {
	case []byte:
		return string(v), nil
	case string:
		return v, nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	}
	return nil, fmt.Errorf("mysql: cannot decode %T as string", raw)
}

// decodeBytes returns a fresh []byte. The driver may reuse its buffers
// across rows, so we copy to make values safe to retain.
func decodeBytes(raw any) (any, error) {
	switch v := raw.(type) {
	case []byte:
		out := make([]byte, len(v))
		copy(out, v)
		return out, nil
	case string:
		return []byte(v), nil
	}
	return nil, fmt.Errorf("mysql: cannot decode %T as bytes", raw)
}

// decodeBit converts MySQL's BIT(N) wire form (a big-endian,
// right-justified ceil(N/8)-byte slice) into the IR-canonical
// '0'/'1' bit-string of exactly n characters (catalog Bug 75). A
// string input (some driver configs surface BIT as text) is parsed
// the same way. n is the column's declared bit width. NULL is
// handled by the caller (decodeValue) before reaching here.
func decodeBit(raw any, n int) (any, error) {
	switch v := raw.(type) {
	case []byte:
		return ir.BitBytesToString(v, n), nil
	case string:
		return ir.BitBytesToString([]byte(v), n), nil
	}
	return nil, fmt.Errorf("mysql: cannot decode %T as Bit", raw)
}

// decodeMySQLGeometry strips MySQL's 4-byte little-endian SRID prefix
// from a geometry value, returning the trailing WKB body as a fresh
// []byte. Matches the IR contract for [ir.Geometry] values (raw WKB,
// per docs/value-types.md).
//
// Returns an error for anything shorter than 5 bytes — the SRID prefix
// alone is 4, and a valid WKB body needs at least one more (the byte-
// order flag).
func decodeMySQLGeometry(raw any) (any, error) {
	bytesAny, err := decodeBytes(raw)
	if err != nil {
		return nil, err
	}
	b := bytesAny.([]byte)
	if len(b) < 5 {
		return nil, fmt.Errorf("mysql: geometry too short (%d bytes; need >=5)", len(b))
	}
	// Drop the 4-byte SRID prefix. The remaining bytes are WKB.
	return b[4:], nil
}

// decodeTime converts MySQL temporal values into time.Time.
//
// The schema-cache pool's DSN sets `parseTime=true` so SELECT results
// arrive as time.Time directly — that's the cold-start bulk-copy
// path. The binlog reader's RowsEvent decoder, however, hands back
// the raw string form ("YYYY-MM-DD HH:MM:SS[.ffffff]" / "YYYY-MM-DD")
// regardless of the SQL DSN setting, because the binlog protocol is
// independent of the driver's row-scan flow. Bug 12 surfaced as a
// silent CDC stall when this branch couldn't decode strings — pump
// errored on the first INSERT carrying a TIMESTAMP/DATETIME column,
// returned, defer close(out) closed the channel, and the applier
// drained zero events.
//
// The 0000-00-00 zero-value (not the same as NULL — MySQL emits it
// when strict mode is off and a date is omitted) maps to the Go
// time.Time zero value so it round-trips cleanly to PG's NULL
// (TIMESTAMPTZ rejects '0000-00-00'). Operators with strict mode on
// won't see this case.
func decodeTime(raw any) (any, error) {
	if v, ok := raw.(time.Time); ok {
		return v, nil
	}
	var s string
	switch v := raw.(type) {
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		return nil, fmt.Errorf("mysql: cannot decode %T as time.Time", raw)
	}
	if s == "0000-00-00 00:00:00" || s == "0000-00-00" || s == "" {
		return time.Time{}, nil
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return nil, fmt.Errorf("mysql: cannot parse %q as MySQL temporal value", s)
}

// decodeSet converts a MySQL SET value's textual representation into
// a []string. MySQL formats SET as a comma-separated list of selected
// members ("a,b,c"). An empty SET returns a non-nil empty slice.
func decodeSet(raw any) (any, error) {
	s, err := decodeString(raw)
	if err != nil {
		return nil, errors.New("mysql: SET value: " + err.Error())
	}
	str := s.(string)
	if str == "" {
		return []string{}, nil
	}
	return strings.Split(str, ","), nil
}
