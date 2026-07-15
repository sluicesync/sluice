// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package parquetexport

// Per-family value encoders. Input shapes are exactly the
// docs/value-types.md Row contract (what the backup chunk decoder
// produces); anything else is refused loudly as an upstream-bug signal,
// never coerced. Output shapes are what parquet-go's schema
// deconstruction expects for the node each encoder is paired with in
// columnNode.

import (
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"sluicesync.dev/sluice/internal/ir"
)

func encodeBool(v any) (any, error) {
	b, ok := v.(bool)
	if !ok {
		return nil, contractViolation("bool", v)
	}
	return b, nil
}

func encodeInt64(v any) (any, error) {
	n, ok := v.(int64)
	if !ok {
		return nil, contractViolation("int64", v)
	}
	return n, nil
}

// encodeUint64 handles the unsigned-integer family's two contract
// shapes: int64 for values readers knew fit in MaxInt64, uint64 for
// the full-range BIGINT UNSIGNED case. A negative int64 in an unsigned
// column is an upstream bug, refused.
func encodeUint64(v any) (any, error) {
	switch n := v.(type) {
	case uint64:
		return n, nil
	case int64:
		if n < 0 {
			return nil, fmt.Errorf("negative value %d in an unsigned integer column", n)
		}
		return uint64(n), nil
	}
	return nil, contractViolation("int64/uint64", v)
}

func encodeFloat64(v any) (any, error) {
	f, ok := v.(float64)
	if !ok {
		return nil, contractViolation("float64", v)
	}
	return f, nil
}

func encodeString(v any) (any, error) {
	s, ok := v.(string)
	if !ok {
		return nil, contractViolation("string", v)
	}
	return s, nil
}

func encodeBytes(v any) (any, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, contractViolation("[]byte", v)
	}
	return b, nil
}

// encodeJSONBytes accepts the JSON family's []byte contract shape (raw
// JSON text bytes), tolerating string for the same bytes.
func encodeJSONBytes(v any) (any, error) {
	switch b := v.(type) {
	case []byte:
		return b, nil
	case string:
		return []byte(b), nil
	}
	return nil, contractViolation("[]byte (raw JSON)", v)
}

// encodeOpaqueText accepts the extension/verbatim families' two
// contract shapes — the type's text-output string, or the same text as
// bytes — and exports the text verbatim.
func encodeOpaqueText(v any) (any, error) {
	switch s := v.(type) {
	case string:
		return s, nil
	case []byte:
		return string(s), nil
	}
	return nil, contractViolation("string/[]byte (extension text I/O)", v)
}

// encodeStringList converts a Set value ([]string per the contract;
// empty set = non-nil empty slice) into the LIST<STRING> shape.
func encodeStringList(v any) (any, error) {
	ss, ok := v.([]string)
	if !ok {
		return nil, contractViolation("[]string", v)
	}
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out, nil
}

// encodeArray returns the encoder for an Array column: each non-nil
// element runs through the element family's encoder. A NESTED slice
// where the schema expects a scalar element is a multi-dimensional
// array — PG's type system does not declare dimensionality (int[] and
// int[][] share one column type), so the derived LIST<element> schema
// can only hold 1-D values. Refused loudly (the silent alternative is
// exactly Bug 74's flatten): exclude the table, or keep the JSON-Lines
// chunks for that table.
func encodeArray(elem encodeFunc, colName string) encodeFunc {
	return func(v any) (any, error) {
		arr, ok := v.([]any)
		if !ok {
			// The chunk codec's `list_str` shape: a text-array decode
			// can surface as []string (see blobcodec's tagged-value
			// codec); route each element through the same encoder.
			ss, isStrs := v.([]string)
			if !isStrs {
				return nil, contractViolation("[]any", v)
			}
			arr = make([]any, len(ss))
			for i, s := range ss {
				arr[i] = s
			}
		}
		out := make([]any, len(arr))
		for i, e := range arr {
			if e == nil {
				continue
			}
			// A nested element is a multi-dimensional value regardless
			// of its decode shape: []any (the generic list tag) OR
			// []string (blobcodec's list_str tag — a 2-D text array's
			// inner rows arrive this way). Both get the multi-dim
			// refusal + remedy, never the misleading contract-violation
			// message a string-leaf element encoder would emit.
			switch e.(type) {
			case []any, []string:
				return nil, fmt.Errorf("column %q holds a multi-dimensional array value: the column type declares no dimensionality, so the Parquet schema is LIST<element> and cannot hold nested lists; exclude this table (--exclude-table) or query the JSON-Lines chunks directly", colName)
			}
			enc, err := elem(e)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			out[i] = enc
		}
		return out, nil
	}
}

// secondsPerDay anchors DATE day arithmetic. Integer math, NOT
// time.Duration — a Duration overflows at ±292 years, silently
// corrupting dates like 0001-01-01 that SQL DATE columns hold legally.
const secondsPerDay = 24 * 60 * 60

// encodeDate converts the Date contract shape (time.Time at 00:00:00
// UTC) into DATE's days-since-epoch int32. A midnight-UTC instant is
// exactly divisible by the day length (pre-epoch included), so any
// remainder means a time-of-day crept in — a value-contract violation,
// refused rather than floored (flooring would silently move the date).
func encodeDate(v any) (any, error) {
	t, ok := v.(time.Time)
	if !ok {
		return nil, contractViolation("time.Time", v)
	}
	u := t.UTC()
	secs := u.Unix()
	if secs%secondsPerDay != 0 || u.Nanosecond() != 0 {
		return nil, fmt.Errorf("date value %s is not a midnight-UTC calendar date", u.Format(time.RFC3339Nano))
	}
	days := secs / secondsPerDay
	if days < math.MinInt32 || days > math.MaxInt32 {
		return nil, fmt.Errorf("date value %s is outside Parquet DATE's int32 day range", u.Format(time.RFC3339Nano))
	}
	return int32(days), nil
}

// encodeTimestampMicros converts DateTime/Timestamp values (time.Time,
// UTC transport per the contract) into TIMESTAMP(MICROS) int64. Two
// refusals keep it exact: sub-microsecond precision (Parquet micros
// would silently truncate it) and instants outside the int64-micros
// range (UnixMicro would silently wrap).
func encodeTimestampMicros(v any) (any, error) {
	t, ok := v.(time.Time)
	if !ok {
		return nil, contractViolation("time.Time", v)
	}
	u := t.UTC()
	if u.Nanosecond()%1000 != 0 {
		return nil, fmt.Errorf("timestamp value %s carries sub-microsecond precision Parquet TIMESTAMP(MICROS) cannot hold", u.Format(time.RFC3339Nano))
	}
	micros := u.UnixMicro()
	if !time.UnixMicro(micros).UTC().Equal(u) {
		return nil, fmt.Errorf("timestamp value %s is outside Parquet TIMESTAMP(MICROS)'s range", u.Format(time.RFC3339Nano))
	}
	return micros, nil
}

// microsPerDay bounds Parquet TIME's time-of-day domain.
const microsPerDay = 24 * 60 * 60 * 1_000_000

// encodeTimeOfDayMicros parses the Time contract shape — a textual
// "HH:MM:SS[.ffffff]" — into TIME(MICROS) int64 microseconds since
// midnight. SQL TIME values outside a calendar day exist (MySQL TIME
// is a ±838h duration; PG allows '24:00:00') but Parquet TIME is
// strictly a time-of-day: such values are refused loudly rather than
// wrapped modulo 24h.
func encodeTimeOfDayMicros(v any) (any, error) {
	s, ok := v.(string)
	if !ok {
		return nil, contractViolation("string (time-of-day text)", v)
	}
	micros, err := parseTimeOfDayMicros(s)
	if err != nil {
		return nil, err
	}
	return micros, nil
}

// parseTimeOfDayMicros parses "H…H:MM:SS[.f…]" into microseconds since
// midnight, refusing negatives, ≥ 24h values, and sub-microsecond
// fractions.
func parseTimeOfDayMicros(s string) (int64, error) {
	if strings.HasPrefix(s, "-") {
		return 0, fmt.Errorf("time value %q is negative (a SQL duration, not a time-of-day); Parquet TIME cannot hold it — exclude the table or carry the column as text via a type override", s)
	}
	base, frac, _ := strings.Cut(s, ".")
	parts := strings.Split(base, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("time value %q is not in HH:MM:SS form", s)
	}
	var hms [3]int64
	for i, p := range parts {
		if p == "" {
			return 0, fmt.Errorf("time value %q is not in HH:MM:SS form", s)
		}
		for _, r := range p {
			if r < '0' || r > '9' {
				return 0, fmt.Errorf("time value %q is not in HH:MM:SS form", s)
			}
			hms[i] = hms[i]*10 + int64(r-'0')
		}
	}
	if hms[1] > 59 || hms[2] > 59 {
		return 0, fmt.Errorf("time value %q has out-of-range minutes/seconds", s)
	}
	micros := ((hms[0]*60+hms[1])*60 + hms[2]) * 1_000_000
	if frac != "" {
		if len(frac) > 6 {
			return 0, fmt.Errorf("time value %q carries sub-microsecond precision Parquet TIME(MICROS) cannot hold", s)
		}
		var f int64
		for _, r := range frac {
			if r < '0' || r > '9' {
				return 0, fmt.Errorf("time value %q has a malformed fractional part", s)
			}
			f = f*10 + int64(r-'0')
		}
		for i := len(frac); i < 6; i++ {
			f *= 10
		}
		micros += f
	}
	if micros >= microsPerDay {
		return 0, fmt.Errorf("time value %q is not within a calendar day (MySQL TIME durations reach ±838h; PG allows 24:00:00); Parquet TIME cannot hold it — exclude the table or carry the column as text via a type override", s)
	}
	return micros, nil
}

// ---------------------------------------------------------------------
// Decimal
// ---------------------------------------------------------------------

// decimalNode picks the exactness-preserving physical shape for a
// Decimal column: DECIMAL over INT32 (p ≤ 9), INT64 (p ≤ 18), or
// FIXED_LEN_BYTE_ARRAY(16) (p ≤ 38). Unbounded numerics, precisions
// beyond 38, and negative scales (PG 15+ allows numeric(p, -s)) have
// no Parquet DECIMAL form — those columns export the IR's exact
// decimal string as UTF8, with an operator-visible note. Never
// float64: the one representation guaranteed to corrupt.
func (c *TableCodec) decimalNode(d ir.Decimal, colName string) (parquet.Node, encodeFunc, error) {
	switch {
	case d.Unconstrained:
		c.note("column %q: unbounded NUMERIC exported as UTF8 string (Parquet DECIMAL is bounded to precision 38; the exact decimal text is carried)", colName)
		return parquet.String(), encodeString, nil
	case d.Precision < 1 || d.Precision > 38:
		c.note("column %q: DECIMAL(%d,%d) exceeds Parquet DECIMAL's precision bound of 38; exported as UTF8 string (the exact decimal text is carried)", colName, d.Precision, d.Scale)
		return parquet.String(), encodeString, nil
	case d.Scale < 0 || d.Scale > d.Precision:
		c.note("column %q: DECIMAL(%d,%d) has a scale outside Parquet DECIMAL's [0, precision] bound; exported as UTF8 string (the exact decimal text is carried)", colName, d.Precision, d.Scale)
		return parquet.String(), encodeString, nil
	case d.Precision <= 9:
		return parquet.Decimal(d.Scale, d.Precision, parquet.Int32Type), encodeDecimalInt32(d), nil
	case d.Precision <= 18:
		return parquet.Decimal(d.Scale, d.Precision, parquet.Int64Type), encodeDecimalInt64(d), nil
	default:
		return parquet.Decimal(d.Scale, d.Precision, parquet.FixedLenByteArrayType(16)), encodeDecimalFLBA16(d), nil
	}
}

func encodeDecimalInt32(d ir.Decimal) encodeFunc {
	return func(v any) (any, error) {
		u, err := decimalUnscaled(v, d)
		if err != nil {
			return nil, err
		}
		// precision ≤ 9 ⇒ |unscaled| ≤ 999_999_999, inside int32.
		return int32(u.Int64()), nil
	}
}

func encodeDecimalInt64(d ir.Decimal) encodeFunc {
	return func(v any) (any, error) {
		u, err := decimalUnscaled(v, d)
		if err != nil {
			return nil, err
		}
		// precision ≤ 18 ⇒ |unscaled| ≤ 999…9 (18 digits), inside int64.
		return u.Int64(), nil
	}
}

func encodeDecimalFLBA16(d ir.Decimal) encodeFunc {
	return func(v any) (any, error) {
		u, err := decimalUnscaled(v, d)
		if err != nil {
			return nil, err
		}
		// precision ≤ 38 ⇒ |unscaled| < 10^38 < 2^127: always fits a
		// 16-byte two's-complement value.
		return twosComplement16(u), nil
	}
}

// decimalUnscaled parses the Decimal family's contract shape (the
// exact textual decimal) into the unscaled integer for DECIMAL(p, s):
// value == unscaled × 10^-s. Exactness-preserving by refusal:
//
//   - non-numeric text (PG NUMERIC 'NaN' / 'Infinity') has no Parquet
//     DECIMAL form;
//   - more fractional digits than the declared scale refuse unless the
//     excess digits are zeros (dropping zeros is lossless);
//   - more significant digits than the declared precision refuse (a
//     value the declared type could not have produced — corruption).
func decimalUnscaled(v any, d ir.Decimal) (*big.Int, error) {
	s, ok := v.(string)
	if !ok {
		return nil, contractViolation("string (exact decimal text)", v)
	}
	text := strings.TrimSpace(s)
	neg := false
	switch {
	case strings.HasPrefix(text, "-"):
		neg = true
		text = text[1:]
	case strings.HasPrefix(text, "+"):
		text = text[1:]
	}
	intPart, fracPart, _ := strings.Cut(text, ".")
	if intPart == "" && fracPart == "" {
		return nil, fmt.Errorf("decimal value %q is empty", s)
	}
	for _, part := range []string{intPart, fracPart} {
		for _, r := range part {
			if r < '0' || r > '9' {
				return nil, fmt.Errorf("decimal value %q is not a plain decimal number (PG NUMERIC NaN/Infinity have no Parquet DECIMAL form) — exclude the table or carry the column as text via a type override", s)
			}
		}
	}
	if len(fracPart) > d.Scale {
		excess := fracPart[d.Scale:]
		if strings.Trim(excess, "0") != "" {
			return nil, fmt.Errorf("decimal value %q carries more fractional digits than DECIMAL(%d,%d) declares — refusing to round", s, d.Precision, d.Scale)
		}
		fracPart = fracPart[:d.Scale]
	}
	digits := intPart + fracPart + strings.Repeat("0", d.Scale-len(fracPart))
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return big.NewInt(0), nil
	}
	if len(digits) > d.Precision {
		return nil, fmt.Errorf("decimal value %q has more significant digits than DECIMAL(%d,%d) declares", s, d.Precision, d.Scale)
	}
	u, ok2 := new(big.Int).SetString(digits, 10)
	if !ok2 {
		return nil, fmt.Errorf("decimal value %q failed to parse", s)
	}
	if neg {
		u.Neg(u)
	}
	return u, nil
}

// twosComplement16 renders v (|v| < 2^127, guaranteed by the p ≤ 38
// precision check) as a 16-byte big-endian two's-complement value —
// Parquet DECIMAL's FIXED_LEN_BYTE_ARRAY layout.
func twosComplement16(v *big.Int) []byte {
	out := make([]byte, 16)
	if v.Sign() >= 0 {
		v.FillBytes(out)
		return out
	}
	// 2^128 + v for v < 0.
	c := new(big.Int).Lsh(big.NewInt(1), 128)
	c.Add(c, v)
	c.FillBytes(out)
	return out
}

// contractViolation names an ir.Row value whose Go type deviates from
// the docs/value-types.md contract for its column family — a bug
// upstream (reader/chunk codec), surfaced loudly per the writer-side
// contract ("MUST error rather than coerce silently").
func contractViolation(want string, got any) error {
	return fmt.Errorf("value of Go type %T violates the IR row-value contract (want %s) — this indicates an upstream reader/codec bug, refusing to guess", got, want)
}
