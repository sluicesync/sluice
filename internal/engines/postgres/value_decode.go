// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"strconv"
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
	switch v := raw.(type) {
	case bool:
		return v, nil
	case []byte:
		return decodeBoolean(string(v))
	case string:
		// Postgres text form: "t"/"f", or "true"/"false". Surfaces
		// when arrays of booleans come back via the array text
		// parser, and again on every CDC tuple value (pgoutput
		// streams text-format payloads by default).
		switch v {
		case "t", "T", "true", "TRUE", "True":
			return true, nil
		case "f", "F", "false", "FALSE", "False":
			return false, nil
		}
		return nil, fmt.Errorf("postgres: cannot decode %q as Boolean", v)
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as Boolean", raw)
}

// decodeInteger widens any signed integer pgx returns into int64.
// Postgres has no native unsigned integers, so we never see uint*.
//
// String/bytes are accepted as a fallback: the array text-form
// parser hands per-element strings here, pgx's stdlib mode can
// surface integers as strings under some configurations, and
// pgoutput CDC tuples carry text-format []byte for every value.
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
	case []byte:
		return decodeInteger(string(v))
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("postgres: cannot decode %q as Integer: %w", v, err)
		}
		return n, nil
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
// no information loss in this direction. String/bytes are accepted
// as a fallback path for array text decoding and pgoutput CDC
// tuple values.
func decodeFloat(raw any) (any, error) {
	switch v := raw.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case []byte:
		return decodeFloat(string(v))
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("postgres: cannot decode %q as Float: %w", v, err)
		}
		return f, nil
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

// decodeTime accepts pgx's time.Time (the database/sql path) or a
// text-format Postgres timestamp/date string ([]byte or string,
// the pgoutput CDC path). Format detection is by-length rather than
// trial-and-error parse: the canonical Postgres text representations
// are unambiguous.
func decodeTime(raw any) (any, error) {
	switch v := raw.(type) {
	case time.Time:
		return v, nil
	case []byte:
		return parsePGTimeText(string(v))
	case string:
		return parsePGTimeText(v)
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as time.Time", raw)
}

// parsePGTimeText parses Postgres canonical text forms for DATE,
// TIMESTAMP, and TIMESTAMPTZ. Each tries in order; first to succeed
// wins. The format strings mirror pgx's internal pgTimestampFormat
// to stay consistent across pgx-driven and pgoutput-driven paths.
func parsePGTimeText(s string) (time.Time, error) {
	layouts := []string{
		// TIMESTAMPTZ — canonical Postgres output is "2006-01-02 15:04:05.999999-07".
		"2006-01-02 15:04:05.999999999-07",
		"2006-01-02 15:04:05-07",
		// TIMESTAMP without timezone.
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		// DATE.
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("postgres: cannot parse %q as time.Time", s)
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
// decodeUUID accepts the three shapes pgx + pgoutput collectively
// produce for a UUID column:
//
//   - [16]byte / 16-byte []byte — the binary form pgx returns under
//     its native type-mapping mode (and what database/sql sometimes
//     surfaces too). Hex-encoded into the canonical hyphenated string.
//   - 36-byte []byte — the canonical text form pgoutput delivers in
//     CDC tuple data. pgoutput tags every tuple value with format
//     byte 't' (text); decodeTuple's 'b' (binary) branch is a hard
//     refusal, so for the CDC path UUID values arrive here as the
//     ASCII bytes of "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx". Validated
//     and lowercased to the IR's canonical form. Bug 41 fix surface.
//   - string — passthrough for pgx modes that decode UUID directly to
//     string. Validated for canonical shape so a malformed source value
//     can't sneak past the IR contract.
//
// Any other byte length surfaces a clear error naming both the length
// and the format byte the caller delegated with, so a future format
// surprise (e.g. a Postgres change to wire encoding) is easy to triage.
func decodeUUID(raw any) (any, error) {
	switch v := raw.(type) {
	case [16]byte:
		return formatUUIDBytes(v[:])
	case []byte:
		switch len(v) {
		case 16:
			return formatUUIDBytes(v)
		case 36:
			// Text-format CDC tuple value (pgoutput). Validate that
			// the bytes really are a canonical hyphenated UUID before
			// promoting to string — bare-bytes-of-arbitrary-length
			// would otherwise pass straight through.
			return canonicalizeUUIDText(string(v))
		default:
			return nil, fmt.Errorf(
				"postgres: UUID byte slice has length %d; want 16 (binary) or 36 (canonical text)",
				len(v))
		}
	case string:
		// Already a string; pgx may return string in some modes, and
		// some codepaths land here directly. Validate the shape so the
		// IR contract holds.
		return canonicalizeUUIDText(v)
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as UUID", raw)
}

// canonicalizeUUIDText validates that s is the canonical hyphenated
// UUID form (8-4-4-4-12 hex digits) and returns it lowercased so the
// IR's UUID-as-string contract holds across engines. Pure ASCII work
// — no allocations beyond the lowercased return.
func canonicalizeUUIDText(s string) (string, error) {
	if len(s) != 36 {
		return "", fmt.Errorf("postgres: UUID text has length %d; want 36 (canonical xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)", len(s))
	}
	// Hyphens at positions 8, 13, 18, 23.
	for _, i := range []int{8, 13, 18, 23} {
		if s[i] != '-' {
			return "", fmt.Errorf("postgres: UUID text %q missing hyphen at offset %d", s, i)
		}
	}
	out := make([]byte, 36)
	for i := 0; i < 36; i++ {
		c := s[i]
		switch {
		case c == '-':
			out[i] = c
		case c >= '0' && c <= '9':
			out[i] = c
		case c >= 'a' && c <= 'f':
			out[i] = c
		case c >= 'A' && c <= 'F':
			out[i] = c + ('a' - 'A')
		default:
			return "", fmt.Errorf("postgres: UUID text %q has non-hex byte %q at offset %d", s, c, i)
		}
	}
	return string(out), nil
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

// decodeArray converts a Postgres array value into a uniform []any
// where each element has been run through [decodeValue] with the
// array's element type.
//
// Three input shapes are handled, in order:
//
//  1. []any — some pgx configurations decode arrays directly into
//     this shape.
//  2. Any other slice/array reflect kind ([]int32, []string, …) —
//     pgx with typed scan targets (e.g. *[]int32) lands here.
//  3. string — pgx in database/sql stdlib mode returns the Postgres
//     text-array form ("{1,2,3}", "{\"a\",\"b\"}") when the scan
//     target is *any. We parse that into []string tokens and then
//     decode element-by-element.
//
// (3) is the path that drove this function's existence: stdlib-mode
// arrays come back as their text representation, and the IR Row
// contract demands a typed slice. Falling back to a parser keeps the
// reader independent of pgx-version-specific scan behaviour and
// avoids forcing every column-type handler to know about arrays.
func decodeArray(raw any, elementType ir.Type) (any, error) {
	if elementType == nil {
		return nil, errors.New("postgres: array decode: element type is nil")
	}

	// (1) Some pgx setups return arrays as []any directly; fast-path it.
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

	// (3) Text form, e.g. "{10,20,30}" or "{\"a\",\"b\"}". Parsed
	// before the reflect path because string is also a reflect.Kind
	// that the slice check below would erroneously reject.
	if s, ok := raw.(string); ok {
		tokens, nullMask, err := parsePGArrayText(s)
		if err != nil {
			return nil, fmt.Errorf("postgres: array text parse: %w", err)
		}
		out := make([]any, len(tokens))
		for i, tok := range tokens {
			if nullMask[i] {
				out[i] = nil
				continue
			}
			d, err := decodeValue(tok, elementType)
			if err != nil {
				return nil, fmt.Errorf("postgres: array element %d: %w", i, err)
			}
			out[i] = d
		}
		return out, nil
	}

	// (2) Any other slice/array via reflection.
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

// parsePGArrayText parses Postgres's text representation of a
// one-dimensional array into per-element string tokens. The second
// return value is a parallel "is NULL?" mask so callers can
// distinguish a string element "NULL" from an actual SQL null —
// Postgres encodes the literal NULL keyword unquoted, while a string
// "NULL" would be wrapped in double quotes.
//
// Format reference (PostgreSQL docs, "Array Input and Output Syntax"):
//
//   - Outer braces: "{" and "}"
//   - Empty array: "{}"
//   - Element separator: "," (no whitespace expected, but tolerated)
//   - Bare element: any unquoted text up to the next "," or "}". The
//     literal NULL (case-insensitive) is the SQL null marker.
//   - Quoted element: enclosed in double quotes; "\" escapes the
//     following byte (so \" and \\ are literal). Required for
//     elements containing "{}",", or whitespace.
//
// Multi-dimensional arrays (nested braces) are not supported by this
// parser; the caller will get a parse error. The IR doesn't model
// dimensions today and adding that surface should come with a real
// use case.
func parsePGArrayText(s string) (tokens []string, isNull []bool, err error) {
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return nil, nil, fmt.Errorf("malformed array literal %q (missing braces)", s)
	}
	body := s[1 : len(s)-1]
	if body == "" {
		return []string{}, []bool{}, nil
	}

	for i := 0; i < len(body); {
		// Skip leading whitespace before the element.
		for i < len(body) && (body[i] == ' ' || body[i] == '\t') {
			i++
		}
		if i >= len(body) {
			break
		}

		var (
			tok   string
			isNil bool
		)

		switch body[i] {
		case '{', '}':
			return nil, nil, fmt.Errorf("nested arrays not supported in %q", s)
		case '"':
			// Quoted element: scan until the matching unescaped quote.
			i++ // consume opening quote
			var sb []byte
			for i < len(body) {
				c := body[i]
				if c == '\\' && i+1 < len(body) {
					sb = append(sb, body[i+1])
					i += 2
					continue
				}
				if c == '"' {
					i++ // consume closing quote
					break
				}
				sb = append(sb, c)
				i++
			}
			tok = string(sb)
		default:
			// Bare element: scan until the next "," or "}".
			start := i
			for i < len(body) && body[i] != ',' {
				i++
			}
			tok = body[start:i]
			// Trim trailing whitespace inside the bare token.
			for tok != "" && (tok[len(tok)-1] == ' ' || tok[len(tok)-1] == '\t') {
				tok = tok[:len(tok)-1]
			}
			// Unquoted NULL is the SQL null marker.
			if eqFold(tok, "NULL") {
				isNil = true
				tok = ""
			}
		}

		// Skip trailing whitespace before the comma.
		for i < len(body) && (body[i] == ' ' || body[i] == '\t') {
			i++
		}
		// Consume the comma if present.
		if i < len(body) {
			if body[i] != ',' {
				return nil, nil, fmt.Errorf("expected ',' or '}' at offset %d in %q", i+1, s)
			}
			i++
		}

		tokens = append(tokens, tok)
		isNull = append(isNull, isNil)
	}
	return tokens, isNull, nil
}

// eqFold is a tiny case-insensitive ASCII comparison; avoids pulling
// in the unicode/strings.EqualFold cost for a single keyword check.
func eqFold(s, t string) bool {
	if len(s) != len(t) {
		return false
	}
	for i := 0; i < len(s); i++ {
		a, b := s[i], t[i]
		if a >= 'A' && a <= 'Z' {
			a += 'a' - 'A'
		}
		if b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		if a != b {
			return false
		}
	}
	return true
}
