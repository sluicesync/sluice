// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/orware/sluice/internal/ir"
)

// timetzOID is the fixed catalog OID for PostgreSQL's built-in
// `timetz` (`time with time zone`) type. Unlike extension types
// (pgvector, hstore) whose OIDs are assigned dynamically at CREATE
// EXTENSION time and must be looked up, built-in type OIDs are stable
// across every PostgreSQL installation (defined in pg_type.dat), so no
// catalog query is needed.
const timetzOID = 1266

// pgTimetzBinaryCodec implements [pgtype.Codec] for `timetz`. pgx v5
// ships no codec for this type at all — pgtype/time.go explicitly
// states "time with time zone type is not supported" and the default
// type map registers nothing for OID 1266. Without a codec pgx's
// CopyFrom (which requires the binary format for every column — see
// pgx copy_from.go) cannot build an encode plan and aborts the whole
// COPY with "cannot find encode plan" (catalog Bug 71). Plain `time`
// (OID 1083) is unaffected — it has a built-in codec.
//
// `timetz` binary wire format (PostgreSQL src/backend/utils/adt/date.c,
// `timetz_send` / `timetz_recv`):
//
//	int64 BE  time   — microseconds since midnight (0 .. 86_400_000_000)
//	int32 BE  zone   — UTC offset in SECONDS, PG's sign convention:
//	                   seconds WEST of UTC == -(gmtoff). For +05 the
//	                   gmtoff is +18000 so zone is -18000; for -07:30
//	                   gmtoff is -27000 so zone is +27000.
//
// The IR carries a timetz value as its canonical Postgres text form
// (the row reader scans the uncatalogued-OID column into *any under
// pgx stdlib mode, which yields the text output string, e.g.
// "13:45:30+05" or "13:45:30.123456-07:30"). Encode parses that text
// to the binary form; Decode is the inverse, used for the symmetric
// unit round-trip and any future typed-scan path.
type pgTimetzBinaryCodec struct{}

func (pgTimetzBinaryCodec) FormatSupported(format int16) bool {
	return format == pgtype.BinaryFormatCode || format == pgtype.TextFormatCode
}

// PreferredFormat reports binary — pgx's CopyFrom always uses binary,
// so that is the load-bearing path.
func (pgTimetzBinaryCodec) PreferredFormat() int16 {
	return pgtype.BinaryFormatCode
}

func (pgTimetzBinaryCodec) PlanEncode(_ *pgtype.Map, _ uint32, format int16, value any) pgtype.EncodePlan {
	switch format {
	case pgtype.BinaryFormatCode:
		switch value.(type) {
		case string:
			return encodePlanTimetzBinaryString{}
		case []byte:
			return encodePlanTimetzBinaryBytes{}
		}
	case pgtype.TextFormatCode:
		switch value.(type) {
		case string:
			return encodePlanTimetzTextString{}
		case []byte:
			return encodePlanTimetzTextBytes{}
		}
	}
	return nil
}

// PlanScan / Decode* are not on the v0.69.x hot path (the row reader
// scans into *any, which routes through pgx's default text path for an
// uncatalogued OID). They are implemented for the symmetric unit
// round-trip and to give a clear answer if a typed-scan path is added.
func (pgTimetzBinaryCodec) PlanScan(_ *pgtype.Map, _ uint32, _ int16, _ any) pgtype.ScanPlan {
	return nil
}

func (pgTimetzBinaryCodec) DecodeDatabaseSQLValue(_ *pgtype.Map, _ uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}
	switch format {
	case pgtype.TextFormatCode:
		return string(src), nil
	case pgtype.BinaryFormatCode:
		return decodeTimetzBinary(src)
	}
	return nil, fmt.Errorf("postgres: timetz codec: unsupported scan format %d", format)
}

func (pgTimetzBinaryCodec) DecodeValue(_ *pgtype.Map, _ uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}
	switch format {
	case pgtype.TextFormatCode:
		return string(src), nil
	case pgtype.BinaryFormatCode:
		return decodeTimetzBinary(src)
	}
	return nil, fmt.Errorf("postgres: timetz codec: unsupported decode format %d", format)
}

// ---------- encode plans ----------

type encodePlanTimetzBinaryString struct{}

func (encodePlanTimetzBinaryString) Encode(value any, buf []byte) ([]byte, error) {
	return appendTimetzBinaryFromText(buf, value.(string))
}

type encodePlanTimetzBinaryBytes struct{}

func (encodePlanTimetzBinaryBytes) Encode(value any, buf []byte) ([]byte, error) {
	return appendTimetzBinaryFromText(buf, string(value.([]byte)))
}

type encodePlanTimetzTextString struct{}

func (encodePlanTimetzTextString) Encode(value any, buf []byte) ([]byte, error) {
	return append(buf, value.(string)...), nil
}

type encodePlanTimetzTextBytes struct{}

func (encodePlanTimetzTextBytes) Encode(value any, buf []byte) ([]byte, error) {
	return append(buf, value.([]byte)...), nil
}

// ---------- text <-> binary helpers ----------

const (
	usecPerSecond = int64(1_000_000)
	usecPerMinute = 60 * usecPerSecond
	usecPerHour   = 60 * usecPerMinute
)

// appendTimetzBinaryFromText parses the Postgres timetz text form and
// appends the 12-byte binary wire encoding (int64 µs-since-midnight,
// int32 zone-seconds) to buf.
func appendTimetzBinaryFromText(buf []byte, s string) ([]byte, error) {
	usec, zone, err := parseTimetzText(s)
	if err != nil {
		return nil, err
	}
	var b [12]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(usec))
	binary.BigEndian.PutUint32(b[8:12], uint32(zone))
	return append(buf, b[:]...), nil
}

// parseTimetzText parses "HH:MM:SS[.ffffff][(+|-)TZ]" into microseconds
// since midnight and PG's zone field (seconds west of UTC == -gmtoff).
// The timezone, when present, is "+HH", "+HH:MM", "+HH:MM:SS" (or the
// '-' variants); PG also emits a bare offset with no minutes. A missing
// offset is treated as +00 (PG always emits one for timetz, but the
// parser stays lenient so a same-engine value that lost its suffix
// still round-trips rather than hard-failing).
func parseTimetzText(s string) (usec int64, zoneSeconds int32, err error) {
	s = strings.TrimSpace(s)
	timePart := s
	zonePart := ""

	// The zone sign is the first '+' or '-' that appears after the
	// "HH:MM:SS" head (offset 8+); it never collides with the time
	// digits or the fractional dot.
	for i := 1; i < len(s); i++ {
		if s[i] == '+' || s[i] == '-' {
			timePart = s[:i]
			zonePart = s[i:]
			break
		}
	}

	usec, err = parseTimeOfDay(timePart)
	if err != nil {
		return 0, 0, err
	}
	if zonePart != "" {
		gmtoff, zerr := parseZoneOffset(zonePart)
		if zerr != nil {
			return 0, 0, zerr
		}
		// PG stores seconds WEST of UTC, i.e. the negation of gmtoff.
		zoneSeconds = -gmtoff
	}
	return usec, zoneSeconds, nil
}

// parseTimeOfDay parses "HH:MM:SS" with an optional ".ffffff" fraction
// into microseconds since midnight.
func parseTimeOfDay(s string) (int64, error) {
	if len(s) < 8 || s[2] != ':' || s[5] != ':' {
		return 0, fmt.Errorf("postgres: timetz: cannot parse time-of-day %q", s)
	}
	h, err := strconv.ParseInt(s[0:2], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("postgres: timetz: bad hour in %q: %w", s, err)
	}
	m, err := strconv.ParseInt(s[3:5], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("postgres: timetz: bad minute in %q: %w", s, err)
	}
	sec, err := strconv.ParseInt(s[6:8], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("postgres: timetz: bad second in %q: %w", s, err)
	}
	usec := h*usecPerHour + m*usecPerMinute + sec*usecPerSecond
	if len(s) > 8 {
		if s[8] != '.' {
			return 0, fmt.Errorf("postgres: timetz: expected '.' fraction in %q", s)
		}
		frac := s[9:]
		if frac == "" || len(frac) > 6 {
			return 0, fmt.Errorf("postgres: timetz: bad fractional seconds in %q", s)
		}
		n, perr := strconv.ParseInt(frac, 10, 64)
		if perr != nil {
			return 0, fmt.Errorf("postgres: timetz: bad fractional seconds in %q: %w", s, perr)
		}
		for i := len(frac); i < 6; i++ {
			n *= 10
		}
		usec += n
	}
	return usec, nil
}

// parseZoneOffset parses "(+|-)HH[:MM[:SS]]" into a signed gmtoff in
// seconds (east of UTC positive — the conventional sign, the negation
// of what PG stores on the wire).
func parseZoneOffset(s string) (int32, error) {
	if len(s) < 3 || (s[0] != '+' && s[0] != '-') {
		return 0, fmt.Errorf("postgres: timetz: cannot parse zone offset %q", s)
	}
	sign := int32(1)
	if s[0] == '-' {
		sign = -1
	}
	body := s[1:]
	fields := strings.Split(body, ":")
	if len(fields) > 3 {
		return 0, fmt.Errorf("postgres: timetz: too many zone fields in %q", s)
	}
	var hh, mm, ss int64
	var err error
	if hh, err = strconv.ParseInt(fields[0], 10, 64); err != nil {
		return 0, fmt.Errorf("postgres: timetz: bad zone hour in %q: %w", s, err)
	}
	if len(fields) > 1 {
		if mm, err = strconv.ParseInt(fields[1], 10, 64); err != nil {
			return 0, fmt.Errorf("postgres: timetz: bad zone minute in %q: %w", s, err)
		}
	}
	if len(fields) > 2 {
		if ss, err = strconv.ParseInt(fields[2], 10, 64); err != nil {
			return 0, fmt.Errorf("postgres: timetz: bad zone second in %q: %w", s, err)
		}
	}
	return sign * int32(hh*3600+mm*60+ss), nil
}

// decodeTimetzBinary is the inverse of appendTimetzBinaryFromText:
// 12-byte binary form → canonical Postgres text ("HH:MM:SS[.ffffff]±HH[:MM[:SS]]").
func decodeTimetzBinary(src []byte) (string, error) {
	if len(src) != 12 {
		return "", fmt.Errorf("postgres: timetz decode: expected 12 bytes, got %d", len(src))
	}
	usec := int64(binary.BigEndian.Uint64(src[0:8]))
	zone := int32(binary.BigEndian.Uint32(src[8:12]))

	h := usec / usecPerHour
	usec -= h * usecPerHour
	m := usec / usecPerMinute
	usec -= m * usecPerMinute
	s := usec / usecPerSecond
	frac := usec - s*usecPerSecond

	var b strings.Builder
	fmt.Fprintf(&b, "%02d:%02d:%02d", h, m, s)
	if frac != 0 {
		fmt.Fprintf(&b, ".%06d", frac)
	}

	// On the wire `zone` is seconds west of UTC; the displayed offset is
	// gmtoff = -zone, east-positive.
	gmtoff := -zone
	sign := byte('+')
	if gmtoff < 0 {
		sign = '-'
		gmtoff = -gmtoff
	}
	zh := gmtoff / 3600
	zm := (gmtoff % 3600) / 60
	zs := gmtoff % 60
	switch {
	case zs != 0:
		fmt.Fprintf(&b, "%c%02d:%02d:%02d", sign, zh, zm, zs)
	case zm != 0:
		fmt.Fprintf(&b, "%c%02d:%02d", sign, zh, zm)
	default:
		fmt.Fprintf(&b, "%c%02d", sign, zh)
	}
	return b.String(), nil
}

// tableHasTimetzColumn reports whether table has any column whose IR
// type is a tz-aware [ir.Time] (`timetz`). Mirrors
// [tableHasHstoreColumn]; drives whether writeViaCopy registers the
// per-conn timetz codec (catalog Bug 71). Array-of-timetz is not a
// catalogued shape (the array element OID would be _timetz / 1270);
// the predicate stays scoped to the scalar column the bug repro and
// the cross-engine policy cover.
func tableHasTimetzColumn(table *ir.Table) bool {
	for _, col := range table.Columns {
		if col == nil {
			continue
		}
		if t, ok := col.Type.(ir.Time); ok && t.WithTimeZone {
			return true
		}
	}
	return false
}

// registerPGTimetzCodec registers [pgTimetzBinaryCodec] for the
// built-in `timetz` type on conn. Unlike the extension codecs there is
// no OID lookup — `timetz` is a core type with the stable catalog OID
// [timetzOID]. Idempotent: re-registering a type pgx already knows is
// harmless (pgx has no codec for timetz by default, so the first
// registration is the one that matters).
func registerPGTimetzCodec(conn *pgx.Conn) {
	tm := conn.TypeMap()
	if _, already := tm.TypeForName("timetz"); already {
		return
	}
	tm.RegisterType(&pgtype.Type{
		Name:  "timetz",
		OID:   timetzOID,
		Codec: pgTimetzBinaryCodec{},
	})
}
