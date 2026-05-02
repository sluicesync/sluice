package postgres

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// decodeValue converts a single value as returned by the pgx driver
// (scanned into *any) into the canonical Go type the IR uses for the
// given column type.
//
// SQL NULL is represented as a nil interface value, both as input and
// as output. Callers must therefore allow nil values for nullable
// columns.
//
// The function is pure — no I/O, no shared state — and exhaustively
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
	case ir.Date, ir.DateTime, ir.Timestamp:
		return decodeTime(raw)
	case ir.Time:
		return decodeTimeAsString(raw)
	case ir.JSON:
		return decodeBytes(raw)
	case ir.Enum:
		return decodeString(raw)
	case ir.UUID:
		return decodeUUID(raw)
	case ir.Inet, ir.Cidr:
		return decodeNetwork(raw)
	case ir.Macaddr:
		return decodeMacaddr(raw)
	case ir.Array:
		return decodeArray(raw, v.Element)
	}
	return nil, fmt.Errorf("postgres: no decoder for IR type %T", t)
}

func decodeBoolean(raw any) (any, error) {
	if v, ok := raw.(bool); ok {
		return v, nil
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as Boolean", raw)
}

// decodeInteger widens any signed integer pgx returns into int64.
// Postgres has no native unsigned integers, so we never see uint*.
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
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as Integer", raw)
}

// decodeDecimal preserves Postgres NUMERIC's precision by keeping it
// as a string. pgx's stdlib mode returns NUMERIC as string by default.
func decodeDecimal(raw any) (any, error) {
	switch v := raw.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as Decimal", raw)
}

// decodeFloat returns float64. Single-precision floats are widened —
// no information loss in this direction.
func decodeFloat(raw any) (any, error) {
	switch v := raw.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as Float", raw)
}

func decodeString(raw any) (any, error) {
	switch v := raw.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as string", raw)
}

// decodeBytes returns a fresh []byte. pgx may reuse buffers across
// rows, so we copy to make values safe to retain.
func decodeBytes(raw any) (any, error) {
	switch v := raw.(type) {
	case []byte:
		out := make([]byte, len(v))
		copy(out, v)
		return out, nil
	case string:
		return []byte(v), nil
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as bytes", raw)
}

// decodeTime accepts a time.Time directly (pgx's default for
// timestamp/timestamptz/date columns).
func decodeTime(raw any) (any, error) {
	if v, ok := raw.(time.Time); ok {
		return v, nil
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as time.Time", raw)
}

// decodeTimeAsString converts pgx's time-of-day representation (a
// time.Time with the date portion zeroed) into the IR's canonical
// string form ("HH:MM:SS" or "HH:MM:SS.ffffff").
func decodeTimeAsString(raw any) (any, error) {
	switch v := raw.(type) {
	case time.Time:
		// Format with sub-second precision when present.
		if v.Nanosecond() > 0 {
			return v.Format("15:04:05.999999"), nil
		}
		return v.Format("15:04:05"), nil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as Time string", raw)
}

// decodeUUID converts pgx's [16]byte (or pgtype.UUID-shaped) raw form
// into the canonical lowercase-hyphenated string the IR contract
// requires.
func decodeUUID(raw any) (any, error) {
	switch v := raw.(type) {
	case [16]byte:
		return formatUUIDBytes(v[:])
	case []byte:
		if len(v) != 16 {
			return nil, fmt.Errorf("postgres: UUID byte slice has length %d; want 16", len(v))
		}
		return formatUUIDBytes(v)
	case string:
		// Already a string; accept it (pgx may return string in some
		// modes). We don't validate format here — that's the source's
		// responsibility.
		return v, nil
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as UUID", raw)
}

func formatUUIDBytes(b []byte) (string, error) {
	if len(b) != 16 {
		return "", fmt.Errorf("postgres: UUID requires 16 bytes, got %d", len(b))
	}
	const groupSep = "-"
	hexed := hex.EncodeToString(b)
	// 8-4-4-4-12
	return hexed[0:8] + groupSep + hexed[8:12] + groupSep + hexed[12:16] + groupSep + hexed[16:20] + groupSep + hexed[20:32], nil
}

// decodeNetwork turns pgx's inet / cidr representation into a string.
// Different pgx versions return different concrete types: netip.Prefix
// (modern), *net.IPNet (older), or string. We accept all three.
func decodeNetwork(raw any) (any, error) {
	switch v := raw.(type) {
	case netip.Prefix:
		return v.String(), nil
	case netip.Addr:
		return v.String(), nil
	case *net.IPNet:
		if v == nil {
			return nil, nil
		}
		return v.String(), nil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as Inet/Cidr", raw)
}

// decodeMacaddr converts net.HardwareAddr (pgx's typical return for
// macaddr / macaddr8) into the canonical string form.
func decodeMacaddr(raw any) (any, error) {
	switch v := raw.(type) {
	case net.HardwareAddr:
		return v.String(), nil
	case string:
		return v, nil
	case []byte:
		// Try as HardwareAddr first; if length isn't 6 or 8, fall
		// back to string. macaddr is 6 bytes, macaddr8 is 8.
		if len(v) == 6 || len(v) == 8 {
			return net.HardwareAddr(v).String(), nil
		}
		return string(v), nil
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as Macaddr", raw)
}

// decodeArray walks a slice value and applies decodeValue to each
// element with the array's IR element type. The result is always
// []any so the IR Row can carry it without callers needing to know
// the runtime slice type.
//
// Reflection is used to handle the variety of slice types pgx may
// return ([]int32 for int4[], []string for text[], etc.) without
// having to enumerate every combination.
func decodeArray(raw any, elementType ir.Type) (any, error) {
	if elementType == nil {
		return nil, errors.New("postgres: array decode: element type is nil")
	}

	// Some pgx setups return arrays as []any directly; fast-path it.
	if asAny, ok := raw.([]any); ok {
		out := make([]any, len(asAny))
		for i, e := range asAny {
			d, err := decodeValue(e, elementType)
			if err != nil {
				return nil, fmt.Errorf("postgres: array element %d: %w", i, err)
			}
			out[i] = d
		}
		return out, nil
	}

	rv := reflect.ValueOf(raw)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, fmt.Errorf("postgres: cannot decode %T as Array (not a slice/array)", raw)
	}
	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		d, err := decodeValue(rv.Index(i).Interface(), elementType)
		if err != nil {
			return nil, fmt.Errorf("postgres: array element %d: %w", i, err)
		}
		out[i] = d
	}
	return out, nil
}
