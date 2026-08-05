// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"strconv"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// valueProvenance records WHERE a leaf value came from, which is the
// only sound way to tell a Postgres TEXT RENDERING of a value from a
// value the driver has already decoded. Item 135: `bytea` is the one
// family where the two are mutually ambiguous — PG's `bytea_output =
// hex` rendering `\xdead` is six perfectly legal raw bytes — so a
// decoder that decides by looking at the CONTENT silently shrinks any
// genuine binary value that happens to spell that shape (`\xdead` →
// 2 bytes; the bare 2-byte `\x` → 0 bytes). Provenance is carried
// instead of sniffed.
//
// Deliberately NOT a bool, and deliberately reached only through
// [decodeValueFromText] / [decodeValueFromBinary]: each call site then
// names its own provenance at the call, and a new caller cannot default
// into a lane by writing `false`.
type valueProvenance int

const (
	// provUnset is the zero value and is never legal. A leaf whose
	// provenance was never declared refuses loudly rather than picking
	// a lane — the zero-value trap in reverse (CLAUDE.md's v0.99.51
	// note): there is no safe default here, only a named one.
	provUnset valueProvenance = iota

	// provText: the raw form is a Postgres text rendering of the value
	// (a pgoutput text-format tuple column, or one leaf token of an
	// array's text literal). bytea in this lane is `\x`+even-hex and
	// anything else is refused.
	provText

	// provBinary: the driver already decoded the value to a Go type
	// (a database/sql scan through pgx stdlib mode, or an element of a
	// slice pgx decoded itself). bytea in this lane is already the raw
	// bytes and is copied verbatim, never inspected.
	provBinary
)

// decodeValueFromText converts a leaf whose raw form is a POSTGRES TEXT
// RENDERING — a pgoutput text-format tuple column, or one token of an
// array text literal — into the canonical Go type the IR uses for the
// given column type. See [decodeValue] for the shared contract.
func decodeValueFromText(raw any, t ir.Type) (any, error) {
	return decodeValue(raw, t, provText)
}

// decodeValueFromBinary converts a leaf the DRIVER already decoded —
// a `database/sql` scan through pgx stdlib mode, or an element of a
// slice pgx produced — into the canonical Go type the IR uses for the
// given column type. See [decodeValue] for the shared contract.
func decodeValueFromBinary(raw any, t ir.Type) (any, error) {
	return decodeValue(raw, t, provBinary)
}

// decodeValue converts a single value as returned by the pgx driver
// (scanned into *any) or by the pgoutput text protocol into the
// canonical Go type the IR uses for the given column type. prov says
// which of the two it is; call through [decodeValueFromText] /
// [decodeValueFromBinary] rather than passing it directly.
//
// SQL NULL is represented as a nil interface value, both as input and
// as output. Callers must therefore allow nil values for nullable
// columns.
//
// Provenance is INERT for every family except `bytea` — every other
// decoder accepts both the driver's Go type and PG's text spelling
// unambiguously. TestDecodeValue runs its whole family table through
// BOTH entry points and requires the same answer, so a family that newly
// becomes provenance-sensitive fails there rather than diverging
// silently between the CDC door and the copy door.
//
// The function is pure — no I/O, no shared state — and exhaustively
// table-tested in value_decode_test.go.
func decodeValue(raw any, t ir.Type, prov valueProvenance) (any, error) {
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
		return decodeBytea(raw, prov)
	case ir.Bit:
		// catalog Bug 75: PG `bit`/`varbit` surfaces under pgx stdlib
		// mode as the canonical '0'/'1' text ("10101010"). The IR
		// contract for ir.Bit is exactly that bit-string (see
		// internal/ir/bit.go) — pass it through as a string. The prior
		// decodeBytes turned the text into the ASCII bytes of the
		// digits, which the writer then truncated, silently collapsing
		// every distinct value. []byte inputs are the same ASCII text
		// on the codepaths that surface it that way.
		return decodeBitString(raw)
	case ir.Date, ir.DateTime, ir.Timestamp:
		return decodeTime(raw)
	case ir.Time, ir.Interval:
		// ir.Time (time-of-day) and ir.Interval (duration) both surface
		// from pgx stdlib as their textual form; carry the string.
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
		// prov is deliberately NOT forwarded. decodeArray is reached
		// from both lanes, but each of its three sub-paths has a FIXED
		// provenance that the caller's says nothing about: an array
		// scanned into *any arrives as PG's array TEXT whichever lane
		// we came from, and a slice pgx decoded itself has already-
		// decoded elements whichever lane we came from. Forwarding
		// would reproduce item 135 in mirror image. See decodeArray.
		return decodeArray(raw, v.Element)
	case ir.Geometry:
		// PostGIS geometry columns have a dynamic OID assigned at
		// `CREATE EXTENSION postgis` time; pgx's stdlib mode therefore
		// falls into its `default:` branch, which scans the value as
		// the text-format string PG sends — the EWKB-as-hex
		// representation `0101000020E610...`. We hex-decode to bytes,
		// then strip the EWKB framing back to raw WKB to match the
		// IR contract for ir.Geometry values (docs/value-types.md).
		// Same-engine PG → PG paths use the bytes-shape directly, so
		// we accept []byte too; pre-EWKB-framed (raw WKB) input also
		// passes through untouched via ewkbToWKB.
		return decodePGGeometry(raw)
	case ir.ExtensionType:
		// ADR-0032: extension passthrough decodes as opaque bytes
		// (or canonical text for engines/codepaths that surface
		// extension types as strings — pgvector's `vector` type
		// stringifies as `[1,2,3]` in pgx's stdlib mode). Same shape
		// as JSON's decoder: the IR's Row contract is "whatever the
		// engine round-trips natively"; downstream the writer's
		// prepareValue passes the same bytes through verbatim.
		return decodeExtensionValue(raw, v)
	case ir.VerbatimType:
		// ADR-0047: an uncatalogued PG extension type carried verbatim.
		// Values round-trip via the type's text I/O (pgx stdlib mode
		// surfaces an unknown OID as the type's text-output string, or
		// raw bytes on some codepaths). Same opaque pass-through shape
		// as ExtensionType / JSON — the IR Row contract is "whatever
		// the engine round-trips natively"; the writer's prepareValue
		// hands the same string/bytes back and PG's type input function
		// re-parses it. Only ever reached on a same-engine PG → PG /
		// PG-restore path (the cross-engine gate refuses before any
		// value moves), so text I/O fidelity is the documented same-
		// PG-major-version contract (ADR-0047).
		return decodeVerbatimValue(raw)
	case ir.Domain:
		// Bug 122 (v0.95.3): a column typed as an `ir.Domain` is a
		// thin wrapper over its base type. PG's wire / text I/O
		// surface a DOMAIN-typed column identically to its base type
		// (the DOMAIN's CHECK constraints apply at INSERT/UPDATE time
		// on the source AND target; the value's representation is
		// the base type's). So the decoder dispatches to the base
		// type's decoder recursively. Without this case the row
		// stream aborted bulk_copy on the first row with `no decoder
		// for IR type ir.Domain` — the v0.95.2 cycle's Focus A
		// failure that surfaced Bug 122 once the schema-half landed
		// correctly.
		if v.BaseType == nil {
			return nil, fmt.Errorf("postgres: decode: DOMAIN %q has nil BaseType", v.Name)
		}
		return decodeValue(raw, v.BaseType, prov)
	}
	return nil, fmt.Errorf("postgres: no decoder for IR type %T", t)
}

// decodePGGeometry normalises a PostGIS geometry column value into
// the IR's canonical "raw WKB bytes" form. It receives the value in
// one of three shapes depending on the read path:
//
//   - cold-start (pgx stdlib, unknown OID): a `string` in EWKB-as-hex
//     text form (geometry_out's output), optionally "\x"-prefixed.
//   - CDC (pgoutput text format, Bug 147): `[]byte` carrying that SAME
//     hex-EWKB text — decodeTuple hands the wire bytes straight through,
//     so the bytes are hex ASCII, NOT raw EWKB.
//   - defensive: raw `[]byte` in EWKB/WKB binary (a future binary codec
//     delivering bytes directly).
//
// The []byte hex-vs-binary ambiguity is resolved structurally: hex-EWKB
// is even-length and entirely ASCII hex digits, whereas raw EWKB begins
// with a byte-order byte (0x00/0x01) that is never an ASCII hex digit —
// so isHexASCII cleanly distinguishes them (the Bug-144 []byte-is-text
// trap, applied to geometry). Both ultimately reach [ewkbToWKB], which
// strips the SRID framing and returns raw WKB. Per-row SRID is
// intentionally dropped — the IR treats SRID as a per-column property
// (ADR-0035), recovered target-side at apply time (#20).
func decodePGGeometry(raw any) (any, error) {
	switch v := raw.(type) {
	case string:
		return decodeGeometryHexOrRaw([]byte(v))
	case []byte:
		return decodeGeometryHexOrRaw(v)
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as Geometry", raw)
}

// decodeGeometryHexOrRaw turns a geometry value's bytes into raw WKB,
// accepting either the hex-EWKB text spelling (cold-start string /
// pgoutput text-format bytes) or raw binary EWKB. See [decodePGGeometry]
// for why the two are unambiguous.
func decodeGeometryHexOrRaw(b []byte) (any, error) {
	b = bytes.TrimPrefix(b, []byte(`\x`))
	if isHexASCII(b) {
		ewkb := make([]byte, hex.DecodedLen(len(b)))
		if _, err := hex.Decode(ewkb, b); err != nil {
			return nil, fmt.Errorf("postgres: cannot decode geometry hex %q: %w", b, err)
		}
		return ewkbToWKB(ewkb)
	}
	return ewkbToWKB(b)
}

// isHexASCII reports whether b is a non-empty, even-length string of ASCII
// hex digits — the shape of PostGIS's hex-EWKB text output. Raw binary EWKB
// fails this (its leading byte-order byte 0x00/0x01 is not a hex digit), so
// it is the discriminator [decodeGeometryHexOrRaw] uses.
func isHexASCII(b []byte) bool {
	if len(b) == 0 || len(b)%2 != 0 {
		return false
	}
	for _, c := range b {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		case c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}

// decodeExtensionValue routes an extension-typed column value through
// the canonical opaque-bytes / opaque-text path. pgvector returns
// vectors as strings in pgx stdlib mode (`[1,2,3]`); when other
// extensions land we may need a per-extension decode hook, but the
// pass-through shape covers v0.26.0's pgvector path. nil maps to nil
// (NULL preserved).
func decodeExtensionValue(raw any, _ ir.ExtensionType) (any, error) {
	if raw == nil {
		return nil, nil
	}
	switch v := raw.(type) {
	case string:
		return v, nil
	case []byte:
		out := make([]byte, len(v))
		copy(out, v)
		return out, nil
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as ExtensionType", raw)
}

// decodeVerbatimValue routes an ADR-0047 verbatim-typed column value
// through the canonical opaque-text / opaque-bytes path (the same
// shape as [decodeExtensionValue]). pgx's stdlib mode hands back an
// unknown-OID value as its text-output string; some codepaths deliver
// raw bytes. Both pass through verbatim — the writer's prepareValue
// hands them back and PG's type input function re-parses on the
// (PG-only) target. nil maps to nil (NULL preserved).
func decodeVerbatimValue(raw any) (any, error) {
	if raw == nil {
		return nil, nil
	}
	switch v := raw.(type) {
	case string:
		return v, nil
	case []byte:
		out := make([]byte, len(v))
		copy(out, v)
		return out, nil
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as VerbatimType (ADR-0047)", raw)
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
//
// Used for value families whose IR contract IS the verbatim byte/text
// payload — JSON / JSONB (stored as text) and the opaque
// extension/verbatim passthroughs. The bytea family has its own decoder
// ([decodeBytea]) because the CDC path delivers bytea in `\x`-hex TEXT
// form, which must be hex-decoded rather than copied verbatim.
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

// decodeBytea decodes a PG `bytea` value to its raw bytes. The two
// reader paths deliver two shapes that are MUTUALLY AMBIGUOUS by
// content, so the shape is decided by prov (item 135), never sniffed:
//
//   - provBinary — row-reader (database/sql via pgx stdlib mode) and
//     the already-decoded array sub-paths: pgx hands back the raw Go
//     []byte. Copied verbatim, never inspected.
//   - provText — CDC (pgoutput tuple, text format) and each leaf of an
//     array text literal: the value arrives as the server's
//     `bytea_output` rendering. Hex-decoded, and REFUSED if it isn't
//     the `\x`+even-hex form.
//
// The pre-item-135 decoder disambiguated by CONTENT — hex-decode iff
// `\x` + even-length valid hex — which is unsound in the binary lane
// because those bytes are also a perfectly ordinary binary value:
// `convert_to('\xdead','UTF8')` is 6 bytes and decoded to 2, and the
// 2-byte `convert_to('\x','UTF8')` decoded to ZERO. Every lane using
// the RowReader was affected — migrate bulk copy, sync cold-start, and
// backup, where the shrunken value is durable in the chain and restore
// reproduces it faithfully.
//
// Neither half may be dropped. The audit's proposed "hex-decode only in
// decodeTuple" would have broken `bytea[]`, which is a supported family
// on both doors: a bytea[] scanned into *any comes back as a STRING in
// PG array-text form with every element already hex-rendered, in all
// four pgx exec modes. The array lane is correct today precisely
// because it hex-decodes on the scan path.
func decodeBytea(raw any, prov valueProvenance) (any, error) {
	switch prov {
	case provText:
		return decodeByteaFromText(raw)
	case provBinary:
		return decodeByteaFromBinary(raw)
	}
	return nil, fmt.Errorf(
		"postgres: bytea decode: value provenance was never declared (%T); "+
			"call decodeValueFromText or decodeValueFromBinary", raw,
	)
}

// decodeByteaFromText hex-decodes PG's `bytea_output = hex` rendering.
//
// The `bytea_output = hex` premise is not assumed here — it is ASSERTED
// PER VALUE, which is this lane's half of a two-layer argument (the
// premise-naming step). The other half is the `SET bytea_output = hex`
// pinned on both pools sluice opens: `afterConnectSessionPins` for every
// ordinary connection and the walsender pin in `replicationConn` (audit
// 2026-08-05 B-1). Those pins are what keep the stream WORKING; this
// refusal is what keeps a LOST pin from being silent — and one can be
// lost, because both are session SETs and the connect hook's own doc
// says it is insufficient under TRANSACTION-mode pooling, where the
// backend can change per statement.
//
// So a rendering that is not `\x`+even-hex is refused by name rather
// than copied verbatim. Verbatim is what the old sniffing decoder did,
// and it is silent corruption: the ASCII of the rendering stored as the
// value (`bytea_output = escape`'s `\001\002` becoming eight bytes).
//
// Named non-goal: the `escape` rendering is refused, not decoded. It is
// unambiguously distinguishable (escape output spells a backslash byte
// as `\\`, so a leading single `\x` can only ever be the hex prefix),
// so support is addable — but it is a second codec and would owe its
// own family matrix. Refusing loudly is the honest state until then.
func decodeByteaFromText(raw any) (any, error) {
	var s string
	switch v := raw.(type) {
	case []byte:
		s = string(v)
	case string:
		s = v
	default:
		return nil, fmt.Errorf("postgres: cannot decode %T as bytea", raw)
	}
	b, ok := decodeHexByteaText(s)
	if !ok {
		return nil, sluicecode.Wrap(
			sluicecode.CodeValueByteaTextUnrecognized,
			"set the source's bytea_output back to the `hex` default",
			fmt.Errorf(
				"postgres: bytea text rendering %q is not the `\\x`+even-hex form "+
					"`bytea_output = hex` produces", truncateForError(s),
			),
		)
	}
	return b, nil
}

// decodeByteaFromBinary copies the driver-decoded bytes. pgx reuses
// buffers across rows, so the copy is load-bearing.
//
// A `string` here is not a shape any known production path produces
// (pgx stdlib scans bytea into []byte — pinned against a real server by
// TestPremise_ByteaScanShapes). It is accepted as its literal bytes
// anyway, because verbatim is the only reading that cannot lose data
// without provenance below this point, and a refusal would be a new
// failure mode with no known trigger.
func decodeByteaFromBinary(raw any) (any, error) {
	switch v := raw.(type) {
	case []byte:
		out := make([]byte, len(v))
		copy(out, v)
		return out, nil
	case string:
		return []byte(v), nil
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as bytea", raw)
}

// truncateForError bounds a value echoed into an error message so a
// multi-megabyte bytea can't turn a refusal into a log flood.
func truncateForError(s string) string {
	const limit = 64
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "…"
}

// decodeHexByteaText recognises the PG `bytea_output = hex` text form
// (`\x` + even-length lowercase/uppercase hex) and returns the decoded
// bytes. The bool is false when s is not in that form, so the caller can
// fall back to a verbatim byte copy (the row-reader raw-bytes shape).
func decodeHexByteaText(s string) ([]byte, bool) {
	const prefix = `\x`
	if !strings.HasPrefix(s, prefix) {
		return nil, false
	}
	body := s[len(prefix):]
	if len(body)%2 != 0 {
		return nil, false
	}
	b, err := hex.DecodeString(body)
	if err != nil {
		return nil, false
	}
	return b, true
}

// decodeBitString returns the IR-canonical bit-string form for a PG
// `bit`/`varbit` value (catalog Bug 75). pgx stdlib mode surfaces the
// value as the canonical '0'/'1' text already; []byte on the paths
// that produce it is the same ASCII text. Anything else is an
// upstream decode bug and surfaces loudly rather than as a wrong
// value.
func decodeBitString(raw any) (any, error) {
	switch v := raw.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	}
	return nil, fmt.Errorf("postgres: cannot decode %T as bit string", raw)
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
	// PROM-P2: the non-finite / non-Gregorian timestamptz values Postgres
	// can emit have no representable target instant. Refuse them by NAME
	// (loud, coded-at-the-caller) rather than fall through to the opaque
	// "cannot parse" error — an operator seeing "infinity" needs to know it
	// is unsupported, not that the parser is broken.
	switch {
	case s == "infinity" || s == "-infinity":
		return time.Time{}, sluicecode.Wrap(sluicecode.CodeValueUnrepresentable,
			"filter or transform the source value (e.g. NULLIF / CASE on the source query)",
			fmt.Errorf("postgres: timestamptz %q is not representable as a fixed-width target timestamp (infinite value)", s))
	case strings.HasSuffix(s, " BC"):
		return time.Time{}, sluicecode.Wrap(sluicecode.CodeValueUnrepresentable,
			"filter or transform the source value (e.g. NULLIF / CASE on the source query)",
			fmt.Errorf("postgres: timestamp %q is a BC (pre-Gregorian) date with no representable target value", s))
	}
	layouts := []string{
		// TIMESTAMPTZ. Postgres renders the zone offset as ±HH, ±HH:MM, or
		// ±HH:MM:SS depending on the session TimeZone — whole-hour zones give
		// "-07", half/quarter-hour zones (Asia/Kolkata +05:30, Asia/Kathmandu
		// +05:45, America/St_Johns -03:30) give "-07:00", and second-level
		// historical LMT offsets (e.g. Europe/Amsterdam pre-1937 = +00:19:32)
		// give "-07:00:00". PROM-P1: only the ±HH form was handled, so the
		// pgoutput CDC pump aborted on the FIRST timestamptz under any
		// fractional-offset server timezone. The ".999999999" makes the
		// fractional seconds optional, so each offset width needs one layout.
		"2006-01-02 15:04:05.999999999-07:00:00",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999-07",
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
				len(v),
			)
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
//
// decodeArray takes NO provenance, and that is the load-bearing part of
// item 135. It is reached from both lanes, yet each sub-path's leaves
// have a FIXED provenance the caller's says nothing about: (1) and (2)
// hold values pgx already decoded (binary, whoever called us), (3)/(3b)
// hold PG's array text whose leaves are text renderings (text, whoever
// called us). So each sub-path SETS its own lane. A `decodeValue(raw,
// t, prov)` that forwarded prov here would reproduce the very bug this
// change fixes, in mirror image — a `bytea[]` on the scan path would
// stop hex-decoding and store the ASCII of every element.
func decodeArray(raw any, elementType ir.Type) (any, error) {
	if elementType == nil {
		return nil, errors.New("postgres: array decode: element type is nil")
	}

	// (1) Some pgx setups return arrays as []any directly; fast-path it.
	// Elements are values pgx decoded — the binary lane.
	if asAny, ok := raw.([]any); ok {
		out := make([]any, len(asAny))
		for i, e := range asAny {
			d, err := decodeValueFromBinary(e, elementType)
			if err != nil {
				return nil, fmt.Errorf("postgres: array element %d: %w", i, err)
			}
			out[i] = d
		}
		return out, nil
	}

	// (3) Text form, e.g. "{10,20,30}" or "{\"a\",\"b\"}", or the
	// multi-dimensional nested form "{{1,2},{3,4}}". Parsed before the
	// reflect path because string is also a reflect.Kind that the
	// slice check below would erroneously reject. Nested arrays decode
	// to nested []any (Bug 68): the IR Row contract for an array value
	// is "[]any, recursively"; the cross-engine MySQL writer's
	// convertArrayLikeToJSON json.Marshals the nested []any to nested
	// JSON faithfully ([[1,2],[3,4]]), and same-engine PG→PG hands the
	// nested []any back to the array writer unchanged.
	if s, ok := raw.(string); ok {
		v, err := decodePGArrayText(s, elementType)
		if err != nil {
			return nil, fmt.Errorf("postgres: array text parse: %w", err)
		}
		return v, nil
	}

	// (3b) The pgoutput CDC path (Bug 144) delivers the array as its TEXT
	// encoding in a []byte, not a string (the cold-start path yields []any or
	// a string). Treat it as the same text form. This is unambiguous: a real
	// array VALUE never arrives as a raw []byte — bytea is ir.Blob, not
	// ir.Array — so there is no collision with a byte-slice element. Without
	// this, the reflect path below would walk the text's bytes and try to
	// decode each uint8 as an element ("cannot decode uint8 as Integer").
	if b, ok := raw.([]byte); ok {
		v, err := decodePGArrayText(string(b), elementType)
		if err != nil {
			return nil, fmt.Errorf("postgres: array text parse: %w", err)
		}
		return v, nil
	}

	// (2) Any other slice/array via reflection. Elements are values pgx
	// decoded into a typed slice — the binary lane.
	rv := reflect.ValueOf(raw)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, fmt.Errorf("postgres: cannot decode %T as Array (not a slice/array)", raw)
	}
	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		d, err := decodeValueFromBinary(rv.Index(i).Interface(), elementType)
		if err != nil {
			return nil, fmt.Errorf("postgres: array element %d: %w", i, err)
		}
		out[i] = d
	}
	return out, nil
}

// decodePGArrayText parses Postgres's text representation of an array
// — one-dimensional ("{1,2,3}") or multi-dimensional ("{{1,2},{3,4}}")
// — into a (recursively) nested []any. Leaf elements are decoded
// through [decodeValue] with elementType; sub-arrays recurse and land
// as nested []any so the IR Row contract ("[]any, recursively") holds
// for any number of dimensions (Bug 68). SQL null elements (the
// unquoted NULL keyword) decode to a nil slot.
//
// Format reference (PostgreSQL docs, "Array Input and Output Syntax"):
//
//   - Outer braces: "{" and "}"
//   - Empty array: "{}"
//   - Element separator: "," (no whitespace expected, but tolerated)
//   - Sub-array element: a nested "{...}" (multi-dimensional arrays)
//   - Bare element: any unquoted text up to the next "," or "}". The
//     literal NULL (case-insensitive) is the SQL null marker.
//   - Quoted element: enclosed in double quotes; "\" escapes the
//     following byte (so \" and \\ are literal). Required for
//     elements containing "{}",", or whitespace.
//
// Postgres always emits rectangular multi-dimensional arrays (every
// sub-array at a given depth has equal length); the parser does not
// enforce that — it faithfully reproduces whatever shape PG emitted,
// which is exactly what a faithful migration wants.
func decodePGArrayText(s string, elementType ir.Type) (any, error) {
	p := &pgArrayParser{src: s, buf: []byte(s)}
	p.skipSpace()
	v, err := p.parseArray(elementType)
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if p.pos != len(p.buf) {
		return nil, fmt.Errorf("trailing characters after array at offset %d in %q", p.pos+1, s)
	}
	return v, nil
}

// pgArrayParser is a tiny recursive-descent cursor over a Postgres
// array text literal. It is single-use (one literal per instance);
// src is retained only for error messages.
type pgArrayParser struct {
	src string
	buf []byte
	pos int
}

func (p *pgArrayParser) skipSpace() {
	for p.pos < len(p.buf) && (p.buf[p.pos] == ' ' || p.buf[p.pos] == '\t') {
		p.pos++
	}
}

// parseArray consumes a "{...}" group at the cursor and returns its
// elements as []any. Elements are either nested arrays (recursing) or
// leaf values decoded via [decodeValue].
func (p *pgArrayParser) parseArray(elementType ir.Type) (any, error) {
	if p.pos >= len(p.buf) || p.buf[p.pos] != '{' {
		return nil, fmt.Errorf("malformed array literal %q (expected '{' at offset %d)", p.src, p.pos+1)
	}
	p.pos++ // consume '{'

	out := []any{}
	p.skipSpace()
	if p.pos < len(p.buf) && p.buf[p.pos] == '}' {
		p.pos++ // empty array
		return out, nil
	}

	for {
		p.skipSpace()
		if p.pos >= len(p.buf) {
			return nil, fmt.Errorf("unterminated array literal %q", p.src)
		}

		var elem any
		if p.buf[p.pos] == '{' {
			// Nested sub-array → recurse; lands as nested []any.
			sub, err := p.parseArray(elementType)
			if err != nil {
				return nil, err
			}
			elem = sub
		} else {
			tok, isNull, err := p.parseScalar()
			if err != nil {
				return nil, err
			}
			if isNull {
				elem = nil
			} else {
				// Leaf of an array TEXT literal: text lane, always —
				// the array's own arrival shape says nothing about it
				// (item 135). A `bytea[]` element arrives here as the
				// unescaped `\xdead` and MUST be hex-decoded.
				d, err := decodeValueFromText(tok, elementType)
				if err != nil {
					return nil, fmt.Errorf("array element %d: %w", len(out), err)
				}
				elem = d
			}
		}
		out = append(out, elem)

		p.skipSpace()
		if p.pos >= len(p.buf) {
			return nil, fmt.Errorf("unterminated array literal %q", p.src)
		}
		switch p.buf[p.pos] {
		case ',':
			p.pos++ // consume separator, parse next element
		case '}':
			p.pos++ // end of this array group
			return out, nil
		default:
			return nil, fmt.Errorf("expected ',' or '}' at offset %d in %q", p.pos+1, p.src)
		}
	}
}

// parseScalar consumes one leaf element (quoted or bare) at the
// cursor and returns its unescaped string token plus whether it was
// the unquoted NULL marker. A quoted element is never NULL (a quoted
// "NULL" is the literal string).
func (p *pgArrayParser) parseScalar() (tok string, isNull bool, err error) {
	if p.buf[p.pos] == '"' {
		p.pos++ // consume opening quote
		var sb []byte
		for p.pos < len(p.buf) {
			c := p.buf[p.pos]
			if c == '\\' && p.pos+1 < len(p.buf) {
				sb = append(sb, p.buf[p.pos+1])
				p.pos += 2
				continue
			}
			if c == '"' {
				p.pos++ // consume closing quote
				return string(sb), false, nil
			}
			sb = append(sb, c)
			p.pos++
		}
		return "", false, fmt.Errorf("unterminated quoted element in %q", p.src)
	}

	// Bare element: scan until the next ",", "}", or "{" (the latter
	// would be malformed at this position but stops the scan cleanly).
	start := p.pos
	for p.pos < len(p.buf) && p.buf[p.pos] != ',' && p.buf[p.pos] != '}' && p.buf[p.pos] != '{' {
		p.pos++
	}
	raw := string(p.buf[start:p.pos])
	// Trim trailing whitespace inside the bare token.
	for raw != "" && (raw[len(raw)-1] == ' ' || raw[len(raw)-1] == '\t') {
		raw = raw[:len(raw)-1]
	}
	if eqFold(raw, "NULL") {
		return "", true, nil
	}
	return raw, false, nil
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
