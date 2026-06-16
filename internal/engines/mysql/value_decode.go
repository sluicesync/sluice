// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"
	"math"
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
	case ir.Time, ir.Interval:
		// MySQL TIME reads as its textual form; the `interval` override
		// (ir.Interval, PG target) carries the same text — PG parses it.
		return decodeString(raw)
	case ir.JSON:
		return decodeBytes(raw)
	case ir.Enum:
		return decodeEnum(raw, v.Values)
	case ir.Set:
		return decodeSet(raw, v.Values)
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

// decodeEnum maps a MySQL ENUM value to its canonical LABEL string — the
// IR contract for an enum value (engine-neutral; a PG target's enum or a
// MySQL target's ENUM both want the label).
//
// The wire shape differs by source path (Bug 145):
//
//   - The binlog (go-mysql RowsEvent) hands an ENUM back as its 1-based
//     ordinal INDEX in the integer family (int8/int64/…), NOT the label.
//     Passing that through made an INSERT carry "2" instead of "active",
//     which a PG enum column rejects (SQLSTATE 22P02) and a MySQL ENUM
//     column silently coerces by index — both wrong/fragile. We map the
//     index to its label via the column's value list.
//   - The snapshot (database/sql) path and the VStream reader hand back
//     the LABEL directly (string / []byte); pass those through.
//
// MySQL's index 0 is the special ” empty/error member (a value that
// failed ENUM validation, e.g. a non-strict-mode bad insert); it carries
// as the empty string, matching MySQL's own SELECT output.
func decodeEnum(raw any, values []string) (any, error) {
	switch v := raw.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	}
	idx, ok := enumIndex(raw)
	if !ok {
		return nil, fmt.Errorf("mysql: enum value is %T (%v); want an index (int) or a label (string/bytes)", raw, raw)
	}
	if idx == 0 {
		return "", nil
	}
	if idx < 0 || idx > int64(len(values)) {
		return nil, fmt.Errorf("mysql: enum index %d out of range [0,%d]", idx, len(values))
	}
	return values[idx-1], nil
}

// enumIndex coerces the integer-family widths the binlog row source can
// produce into an int64 ENUM ordinal. Returns ok=false for non-integer
// inputs (handled by the caller).
func enumIndex(raw any) (int64, bool) {
	switch n := raw.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int16:
		return int64(n), true
	case int8:
		return int64(n), true
	case int:
		return int64(n), true
	case uint64:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint8:
		return int64(n), true
	case uint:
		return int64(n), true
	}
	return 0, false
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

// tinyBoolOutOfRange reports the underlying integer when raw — a cell about
// to be collapsed to a Go bool for a TINYINT(1)/ir.Boolean column — holds an
// integer other than 0 or 1. MySQL's boolean convention maps every non-zero
// value to true, so 2 / 127 / -1 / -128 lose their real value (Vector D);
// the reader uses this to WARN loudly without changing the carried value.
//
// Only integer-family sources can be out of range. bool, BIT(1) bytes, and
// string sources are inherently boolean (decodeBoolean already canonicalises
// them) and are reported as in-range. A column reaches ir.Boolean only via a
// SIGNED tinyint(1) (the schema reader maps unsigned/auto-increment
// tinyint(1) to ir.Integer), so the value is always in the int8 range in
// practice; the wider integer cases are handled defensively to mirror
// decodeBoolean's accepted set.
func tinyBoolOutOfRange(raw any) (n int64, oob bool) {
	var i int64
	switch v := raw.(type) {
	case int64:
		i = v
	case int32:
		i = int64(v)
	case int16:
		i = int64(v)
	case int8:
		i = int64(v)
	case int:
		i = int64(v)
	case uint64:
		if v > 1 {
			// Far outside tinyint(1) in practice; clamp the reported
			// example to int64 max rather than overflow.
			if v > math.MaxInt64 {
				return math.MaxInt64, true
			}
			return int64(v), true
		}
		i = int64(v)
	case uint32:
		i = int64(v)
	case uint16:
		i = int64(v)
	case uint8:
		i = int64(v)
	case uint:
		i = int64(v)
	default:
		// bool, []byte, string: inherently boolean, never out of range.
		return 0, false
	}
	if i == 0 || i == 1 {
		return 0, false
	}
	return i, true
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
// (`--type-override TABLE.COL=decimal(20,0)`): go-sql-driver
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
// The bulk-copy read path reads DATE/DATETIME/TIMESTAMP columns through
// CAST(... AS CHAR) (see selectColumnExpr) so this decoder receives
// MySQL's literal text rather than a value the go-sql-driver has
// already parsed under parseTime=true. That detour is load-bearing for
// correctness: the driver decodes a partial date like "2026-00-00" via
// time.Date(2026, 0, 0, ...), which Go *silently normalizes* into the
// prior month ("2025-11-30") before sluice ever sees it — a CRITICAL
// silent-corruption class (Vector A). Reading the raw string lets this
// decoder validate the value and surface zero/partial dates explicitly.
//
// The binlog reader's RowsEvent decoder independently hands back the
// raw string form ("YYYY-MM-DD HH:MM:SS[.ffffff]" / "YYYY-MM-DD"),
// because the binlog protocol is independent of the driver's row-scan
// flow. Bug 12 surfaced as a silent CDC stall when this branch couldn't
// decode strings — the pump errored on the first INSERT carrying a
// TIMESTAMP/DATETIME column and drained zero events.
//
// MySQL zero and partial dates (0000-00-00, YYYY-00-DD, YYYY-MM-00) —
// storable only under a relaxed source sql_mode and with no valid
// calendar value — are returned as a *zeroDateValueError sentinel. The
// read paths funnel that through applyZeroDatePolicy to honor the
// operator's --zero-date choice (refuse / null / epoch). A genuinely
// out-of-range but non-zero value (month 13, Feb 30) stays a hard
// decode error.
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
	if s == "" {
		// A non-NULL temporal column rendered as an empty string has no
		// calendar value; let the --zero-date policy govern it rather
		// than silently producing the Go zero time.
		return nil, &zeroDateValueError{raw: s}
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
	if isMySQLZeroDate(s) {
		return nil, &zeroDateValueError{raw: s}
	}
	return nil, fmt.Errorf("mysql: cannot parse %q as MySQL temporal value", s)
}

// isMySQLZeroDate reports whether s is a MySQL zero or partial date: the
// all-zero 0000-00-00, or any value with a zero month or zero day
// (YYYY-00-DD, YYYY-MM-00). time.Parse rejects all of these, so this is
// only consulted after the valid-layout attempts fail — it distinguishes
// the legacy zero-date family (governed by --zero-date) from a genuinely
// malformed value (month 13, Feb 30), which stays a hard error. Year
// "0000" with an otherwise-valid month/day is a representable historical
// date and intentionally NOT treated as a zero date.
func isMySQLZeroDate(s string) bool {
	datePart := s
	if i := strings.IndexByte(s, ' '); i >= 0 {
		datePart = s[:i]
	}
	p := strings.Split(datePart, "-")
	if len(p) != 3 {
		return false
	}
	return p[1] == "00" || p[2] == "00"
}

// zeroDateMode is the process-wide policy for carrying MySQL zero and
// partial dates discovered on the read path. It is set once at startup
// from --zero-date via SetZeroDateMode (main.go), mirroring
// sessionSQLMode. The default refuses loudly: silently normalizing
// these values to a wrong calendar date was a CRITICAL silent-corruption
// class (Vector A).
type zeroDateMode int

const (
	zeroDateRefuse  zeroDateMode = iota // --zero-date=error (default)
	zeroDateAsNull                      // --zero-date=null
	zeroDateAsEpoch                     // --zero-date=epoch
)

// zeroDatePolicy is the active zero-date mode. Read on the row-decode
// path; written once at startup before any engine connects.
var zeroDatePolicy = zeroDateRefuse

// SetZeroDateMode sets the process-wide zero-date policy from the
// operator's --zero-date value. Called once from main.go before any
// engine opens a connection. An empty string keeps the refuse default.
func SetZeroDateMode(s string) error {
	switch s {
	case "", "error":
		zeroDatePolicy = zeroDateRefuse
	case "null":
		zeroDatePolicy = zeroDateAsNull
	case "epoch":
		zeroDatePolicy = zeroDateAsEpoch
	default:
		return fmt.Errorf("mysql: invalid --zero-date %q (want one of: error, null, epoch)", s)
	}
	return nil
}

// zeroDateEpochValue is the synthetic substitute for --zero-date=epoch:
// 1970-01-01 at 00:00:01 UTC. It is one second past the Unix epoch on
// purpose — MySQL's TIMESTAMP range floor is '1970-01-01 00:00:01' UTC,
// so a plain midnight epoch is one second BELOW it and unrepresentable:
// a MySQL TIMESTAMP target under a relaxed sql_mode (which reading a
// legacy zero-date source requires) would silently coerce midnight back
// to the '0000-00-00' zero sentinel — the very value epoch is meant to
// replace (Bug 133). 00:00:01 is representable by every temporal target
// (MySQL TIMESTAMP floor, MySQL DATETIME, and PG's effectively-unbounded
// timestamp/date), so a single sentinel is correct everywhere. The
// resolution stays target-agnostic (this is the source reader; per the
// IR-first separation it has no business knowing the target type), and
// the one-second offset is meaningless on what is by definition a
// placeholder for an invalid date, not real data. Writers render it per
// the target column type (date-only for DATE; with time for
// DATETIME/TIMESTAMP).
var zeroDateEpochValue = time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC)

// zeroDateValueError marks a MySQL zero/partial date surfaced by
// decodeTime. Read paths catch it with errors.As and resolve it via
// applyZeroDatePolicy; propagated unhandled it refuses loudly, which is
// the correct fallback.
type zeroDateValueError struct{ raw string }

func (e *zeroDateValueError) Error() string {
	return fmt.Sprintf("MySQL zero/partial date %q has no valid calendar value", e.raw)
}

// applyZeroDatePolicy resolves a zero/partial date per the configured
// --zero-date mode for column col. It is the single chokepoint the
// bulk-copy and CDC read paths funnel zeroDateValueError through. The
// caller adds the "mysql: column %q" context, so the returned errors
// carry only the reason.
func applyZeroDatePolicy(zd *zeroDateValueError, col *ir.Column) (any, error) {
	switch zeroDatePolicy {
	case zeroDateAsNull:
		if !col.Nullable {
			return nil, fmt.Errorf(
				"%s; --zero-date=null cannot apply to a NOT NULL column (use --zero-date=epoch, or repair the source value)",
				zd.Error(),
			)
		}
		return nil, nil
	case zeroDateAsEpoch:
		return zeroDateEpochValue, nil
	default: // zeroDateRefuse
		return nil, fmt.Errorf(
			"%s; pass --zero-date=null or --zero-date=epoch to carry it (see docs/operator/migrating-legacy-mysql.md)",
			zd.Error(),
		)
	}
}

// decodeSet converts a MySQL SET value's textual representation into
// a []string. MySQL formats SET as a comma-separated list of selected
// members ("a,b,c"). An empty SET returns a non-nil empty slice.
// decodeSet maps a MySQL SET value to its member-label slice — the IR
// contract for a SET value. The wire shape differs by source path (Bug 148,
// the sibling of the ENUM Bug 145):
//
//   - The binlog (go-mysql RowsEvent) hands a SET back as its NUMERIC
//     BITMASK in the integer family (int64), where bit i (LSB-first) is the
//     i-th declared member. Passing that through stringified the number
//     ("5" for bits {0,2}) instead of yielding the members ["a","c"]; worse,
//     decodeSet wasn't even given the member list, so it could not map it.
//     We walk the mask against the column's value list.
//   - The snapshot (database/sql) path and the VStream reader hand back the
//     canonical comma-joined LABEL text ("a,c"). MySQL SET member names
//     cannot contain a comma (it is the separator), so splitting on "," is
//     exact — no quoting/escaping to handle, unlike the ENUM/label case.
//
// An empty SET decodes to []string{} (mask 0 / empty text). A mask bit with
// no declared member is a corruption signal and errors LOUDLY rather than
// being silently dropped (loud-failure tenet).
func decodeSet(raw any, values []string) (any, error) {
	switch v := raw.(type) {
	case string:
		return splitSetText(v), nil
	case []byte:
		return splitSetText(string(v)), nil
	}
	mask, ok := setMask(raw)
	if !ok {
		return nil, fmt.Errorf("mysql: set value is %T (%v); want a bitmask (int) or comma-joined labels (string/bytes)", raw, raw)
	}
	if mask == 0 {
		return []string{}, nil
	}
	// A bit set beyond the declared members can't be mapped to a label —
	// surface it loudly. (len(values)==64 is the SET maximum; every bit is
	// then valid and a 64-bit shift would be undefined, so skip the check.)
	if len(values) < 64 && mask>>uint(len(values)) != 0 {
		return nil, fmt.Errorf("mysql: set bitmask %#x has a bit beyond the %d declared members", mask, len(values))
	}
	out := make([]string, 0, len(values))
	for i := range values {
		if mask&(uint64(1)<<uint(i)) != 0 {
			out = append(out, values[i])
		}
	}
	return out, nil
}

// splitSetText splits the canonical comma-joined SET text form into member
// labels. SET member names cannot contain a comma, so a plain split is exact;
// the empty string is the empty SET.
func splitSetText(s string) []string {
	if s == "" {
		return []string{}
	}
	return strings.Split(s, ",")
}

// setMask coerces the integer-family widths a row source can produce into a
// uint64 SET bitmask, preserving the bit pattern (NOT sign-extending — an
// int8 mask 0x80 is bit 7, not a 64-bit all-ones). go-mysql ALWAYS delivers a
// SET as int64 (sized by storage width = ceil(members/8) bytes, zero-extended
// into int64), so the int64 case is the only live one — and it is non-negative
// for ≤56 members; only a 57–64-member SET with a high bit set yields a
// negative int64, whose pattern uint64(int64) preserves. The narrower signed
// widths are handled defensively (no source produces them for SET). Returns
// ok=false for non-integer inputs (handled by the caller).
func setMask(raw any) (uint64, bool) {
	switch n := raw.(type) {
	case int64:
		return uint64(n), true
	case uint64:
		return n, true
	case int32:
		return uint64(uint32(n)), true
	case uint32:
		return uint64(n), true
	case int16:
		return uint64(uint16(n)), true
	case uint16:
		return uint64(n), true
	case int8:
		return uint64(uint8(n)), true
	case uint8:
		return uint64(n), true
	case int:
		return uint64(n), true
	case uint:
		return uint64(n), true
	}
	return 0, false
}
