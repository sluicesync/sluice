// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// storageClass names the SQLite storage class of a raw value as returned
// by modernc.org/sqlite when scanned into an *any: the driver hands back
// the value's ACTUAL on-disk storage class as a Go type:
//
//	NULL    → nil
//	INTEGER → int64
//	REAL    → float64
//	TEXT    → string
//	BLOB    → []byte
//
// This is what gives sluice per-row storage-class fidelity for free: the
// reported Go type IS the row's storage class, independent of the
// column's declared affinity.
func storageClass(raw any) string {
	switch raw.(type) {
	case nil:
		return "NULL"
	case int64:
		return "INTEGER"
	case float64:
		return "REAL"
	case string:
		return "TEXT"
	case []byte:
		return "BLOB"
	default:
		return fmt.Sprintf("unexpected-driver-type(%T)", raw)
	}
}

// decodeCell maps one SQLite value to its IR Row value, dispatching on
// the value's ACTUAL storage class and the column's RESOLVED affinity
// (carried as the IR type t). When the storage class cannot be
// faithfully represented in t, it returns a storage-class-mismatch error
// (see [mismatchError]) — the loud-failure tenet: a value the target
// can't faithfully hold is refused, never silently coerced to a
// wrong-but-plausible value.
//
// NULL is faithful for every type. The accepted (faithful) storage
// classes per affinity — and why the others are refused — are:
//
//	INTEGER affinity (ir.Integer):  INTEGER→int64.
//	  REAL/TEXT/BLOB refused (a fractional/non-numeric/binary value
//	  cannot be an int64 without loss; SQLite leaves such values in
//	  their original class rather than coercing on insert).
//	REAL affinity (ir.Float):       REAL→float64.
//	  INTEGER/TEXT/BLOB refused (REAL affinity coerces integers to
//	  float on insert, so a stored INTEGER here is anomalous; TEXT/BLOB
//	  are non-numeric).
//	TEXT affinity (ir.Text):        TEXT→string.
//	  BLOB refused (a blob may not be valid UTF-8 text); INTEGER/REAL
//	  do not occur (TEXT affinity coerces numbers to text on insert).
//	BLOB affinity (ir.Blob):        BLOB→[]byte.
//	  INTEGER/REAL/TEXT refused — BLOB affinity (and the no-declared-
//	  type case) applies NO coercion, so a column can hold any class;
//	  the prototype refuses non-blob values rather than choosing a byte
//	  encoding for them.
//	NUMERIC affinity (ir.Decimal):  INTEGER→decimal-string, REAL→decimal-string.
//	  TEXT/BLOB refused (a TEXT value in a NUMERIC column is one SQLite
//	  could not parse as a number; a BLOB is binary).
//
// The declared-temporal/bool IR types (resolved by [resolveColumnType] per
// ADR-0129) decode by the operator's date encoding (enc) for temporal, and
// by the fixed 0/1/truthy-text rule for ir.Boolean — both refusing loudly on
// any value that cannot be faithfully interpreted (see decodeTemporal /
// decodeBoolean). enc is the already-resolved encoding (never the inherit
// sentinel) and is ignored for non-temporal types.
func decodeCell(raw any, t ir.Type, enc dateEncoding) (any, error) {
	if raw == nil {
		return nil, nil // NULL — faithful for every IR type.
	}

	switch t.(type) {
	case ir.Date, ir.Timestamp, ir.Time:
		return decodeTemporal(raw, t, enc)

	case ir.Boolean:
		return decodeBoolean(raw)

	case ir.Integer:
		if v, ok := raw.(int64); ok {
			return v, nil
		}
		return nil, mismatchError(raw, t)

	case ir.Float:
		if v, ok := raw.(float64); ok {
			return v, nil
		}
		return nil, mismatchError(raw, t)

	// The string-affinity family. ir.Text is what resolveColumnType
	// produces for a declared text column; the others (Varchar, Char,
	// JSON, UUID) never come from the SQLite reader's own affinity
	// resolution but DO arrive via `--type-override` (Bug 161): the
	// override rewrites the column's IR type before decode, and a SQLite
	// TEXT value carries faithfully into any of them. Same refuse-not-
	// coerce contract as Text — a non-TEXT storage class is a loud
	// mismatch, never a silent coercion.
	case ir.Text, ir.Varchar, ir.Char, ir.JSON, ir.UUID:
		if v, ok := raw.(string); ok {
			return v, nil
		}
		return nil, mismatchError(raw, t)

	// The binary-affinity family — Blob from affinity resolution, Binary/
	// Varbinary via `--type-override`; a SQLite BLOB value carries into
	// any of them.
	case ir.Blob, ir.Binary, ir.Varbinary:
		if v, ok := raw.([]byte); ok {
			// modernc may reuse the backing buffer across rows; copy so
			// the IR Row value is safe to retain.
			out := make([]byte, len(v))
			copy(out, v)
			return out, nil
		}
		return nil, mismatchError(raw, t)

	case ir.Decimal:
		// Both exact-numeric storage classes carry losslessly into the
		// IR's decimal-as-string contract: an int64 is an exact integer,
		// and FormatFloat(g, -1) is the shortest round-trippable decimal
		// for the float64. TEXT/BLOB are refused by falling through.
		switch v := raw.(type) {
		case int64:
			return strconv.FormatInt(v, 10), nil
		case float64:
			return strconv.FormatFloat(v, 'g', -1, 64), nil
		}
		return nil, mismatchError(raw, t)
	}

	return nil, fmt.Errorf("sqlite: no decoder for IR type %T", t)
}

// mismatchError builds the loud refusal for a storage-class / affinity
// mismatch. It names the offending storage class and the resolved IR
// type; the row reader wraps it with the table, column, and rowid so the
// operator can find the exact offending cell. The message points at the
// future opt-in override that may relax the refusal.
func mismatchError(raw any, t ir.Type) error {
	return fmt.Errorf(
		"storage-class mismatch: value stored as %s cannot be faithfully represented as IR %s; "+
			"refusing to coerce (loud-failure prototype — a future --sqlite-relax-affinity override may relax this)",
		storageClass(raw), t.String(),
	)
}

// dateEncoding is the policy for interpreting the VALUE of a declared
// temporal column (ir.Date / ir.Timestamp / ir.Time, ADR-0129). SQLite has
// no native temporal storage class — dates live as ISO TEXT, unix INTEGER,
// or Julian REAL by application convention — and guessing wrong silently
// produces a plausible-but-wrong date (a value-fidelity violation). So the
// IR type is inferred from the declared type, but the encoding is an
// EXPLICIT operator choice that refuses loudly on a storage-class mismatch.
//
// Two scopes share the type, mirroring the MySQL zero-date pattern
// (ADR-0127):
//
//   - the process-wide default ([defaultDateEncoding]) set once at startup
//     from --sqlite-date-encoding via [SetDefaultDateEncoding] (main.go);
//   - each row reader can override it PER SOURCE via the
//     `sqlite_date_encoding` DSN param (resolved at OpenRowReader, see
//     connect.go).
type dateEncoding int

const (
	// dateEncodingInherit is the STRUCT-FIELD zero value: a reader that
	// leaves its encoding unset defers to the process-global
	// [defaultDateEncoding]. It is the iota-0 sentinel ON PURPOSE so every
	// construction site that does not set the field (tests, future callers)
	// inherits the operator's --sqlite-date-encoding rather than silently
	// flipping to a fixed encoding (the v0.99.51 zero-value-safety lesson).
	dateEncodingInherit    dateEncoding = iota
	dateEncodingISO                     // iso (default): ISO-8601 TEXT
	dateEncodingUnixEpoch               // unixepoch: INTEGER/REAL unix seconds
	dateEncodingUnixMillis              // unixmillis: INTEGER/REAL unix milliseconds
	dateEncodingJulian                  // julian: REAL/INTEGER Julian day number
)

func (e dateEncoding) String() string {
	switch e {
	case dateEncodingUnixEpoch:
		return "unixepoch"
	case dateEncodingUnixMillis:
		return "unixmillis"
	case dateEncodingJulian:
		return "julian"
	default:
		// dateEncodingISO and the inherit sentinel both present as "iso"
		// (inherit resolves to the iso default unless overridden).
		return "iso"
	}
}

// defaultDateEncoding is the active PROCESS-GLOBAL temporal encoding (the
// --sqlite-date-encoding default). Read by [resolveDateEncoding] whenever a
// reader's own encoding is dateEncodingInherit; written once at startup
// before any engine connects. It is never dateEncodingInherit itself
// (SetDefaultDateEncoding maps an absent/empty value to dateEncodingISO), so
// inherit resolution terminates.
var defaultDateEncoding = dateEncodingISO

// errInvalidDateEncoding names the valid set for a bad encoding value;
// callers wrap it with their own %q context (the --sqlite-date-encoding flag
// or the DSN param). It is an errors.New value (gofumpt: errors.New over
// fmt.Errorf when there is no format verb).
var errInvalidDateEncoding = errors.New("want one of: iso, unixepoch, unixmillis, julian")

// parseDateEncoding maps a date-encoding string to its enum. Empty and "iso"
// both mean ISO text (the default), matching the --sqlite-date-encoding
// flag's enum. It is the SINGLE parser shared by the global flag
// ([SetDefaultDateEncoding]) and the per-source DSN param so the two can
// never drift on accepted values.
func parseDateEncoding(s string) (dateEncoding, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "iso":
		return dateEncodingISO, nil
	case "unixepoch":
		return dateEncodingUnixEpoch, nil
	case "unixmillis":
		return dateEncodingUnixMillis, nil
	case "julian":
		return dateEncodingJulian, nil
	default:
		return dateEncodingInherit, errInvalidDateEncoding
	}
}

// SetDefaultDateEncoding sets the process-wide temporal encoding from the
// operator's --sqlite-date-encoding value. Called once from main.go before
// any engine opens a connection. An empty string keeps the iso default.
//
// Concurrency: process-wide global state set once at startup, before any
// engine opens a connection. Don't call it from long-lived goroutines.
func SetDefaultDateEncoding(s string) error {
	enc, err := parseDateEncoding(s)
	if err != nil {
		return fmt.Errorf("sqlite: invalid --sqlite-date-encoding %q (%w)", s, err)
	}
	defaultDateEncoding = enc
	return nil
}

// resolveDateEncoding collapses a reader's per-source encoding to a concrete
// one: dateEncodingInherit defers to the process-global [defaultDateEncoding]
// (which is never inherit), so the result is always a real encoding.
func resolveDateEncoding(enc dateEncoding) dateEncoding {
	if enc == dateEncodingInherit {
		return defaultDateEncoding
	}
	return enc
}

// decodeTemporal decodes a SQLite cell into its IR temporal value per the
// resolved encoding enc. t is one of ir.Date / ir.Timestamp / ir.Time. The
// produced value follows the IR value contract (docs/value-types.md): a
// time.Time (UTC) for ir.Date and ir.Timestamp, and a textual time-of-day
// string for ir.Time. A storage class the encoding cannot faithfully
// interpret — non-TEXT under iso, non-numeric under the unix/julian
// encodings, or text matching no ISO layout — is REFUSED LOUDLY; never a
// guessed date.
func decodeTemporal(raw any, t ir.Type, enc dateEncoding) (any, error) {
	tm, err := temporalInstant(raw, t, enc)
	if err != nil {
		return nil, err
	}
	switch t.(type) {
	case ir.Date:
		// Truncate to the calendar date; the IR Date value is a UTC
		// time.Time with a 00:00:00 time portion (value-types.md).
		y, m, d := tm.Date()
		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC), nil
	case ir.Time:
		// The IR Time value is a textual time-of-day (value-types.md).
		return formatTimeOfDay(raw, tm, enc), nil
	default: // ir.Timestamp
		return tm.UTC(), nil
	}
}

// temporalInstant resolves a SQLite cell to a UTC instant per the encoding,
// refusing loudly on a storage class the encoding cannot interpret. The
// caller (decodeTemporal) then projects it onto the target IR type.
func temporalInstant(raw any, t ir.Type, enc dateEncoding) (time.Time, error) {
	switch enc {
	case dateEncodingISO:
		s, ok := raw.(string)
		if !ok {
			return time.Time{}, temporalStorageError(raw, t, enc)
		}
		tm, ok := parseISOTemporal(s, t)
		if !ok {
			return time.Time{}, isoParseError(s, t)
		}
		return tm, nil
	case dateEncodingUnixEpoch:
		f, ok := numericValue(raw)
		if !ok {
			return time.Time{}, temporalStorageError(raw, t, enc)
		}
		return secondsToTime(f), nil
	case dateEncodingUnixMillis:
		f, ok := numericValue(raw)
		if !ok {
			return time.Time{}, temporalStorageError(raw, t, enc)
		}
		return secondsToTime(f / 1000.0), nil
	case dateEncodingJulian:
		f, ok := numericValue(raw)
		if !ok {
			return time.Time{}, temporalStorageError(raw, t, enc)
		}
		return julianToTime(f), nil
	default:
		// Unreachable: enc is always a concrete encoding here (resolved).
		return time.Time{}, fmt.Errorf("sqlite: unknown date encoding %d", enc)
	}
}

// isoDateTimeLayouts are the accepted ISO layouts for ir.Date / ir.Timestamp
// values under the iso encoding (ADR-0129). Tried in order; the first that
// parses wins. The `.999999999` form is an OPTIONAL fraction, so each
// fractional layout also matches its no-fraction input — covering SQLite's
// own date()/datetime() output, the space- and T-separated forms, RFC3339
// (with zone), and a bare date.
var isoDateTimeLayouts = []string{
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999Z07:00",
	time.RFC3339,
	"2006-01-02",
}

// isoTimeLayouts are the accepted ISO layouts for an ir.Time (time-of-day)
// value under the iso encoding.
var isoTimeLayouts = []string{
	"15:04:05.999999999",
	"15:04:05",
}

// parseISOTemporal parses an ISO temporal text into a UTC instant, choosing
// the layout set by the target IR type (time-of-day layouts for ir.Time,
// date/datetime layouts otherwise). ok is false when no layout matches.
func parseISOTemporal(s string, t ir.Type) (time.Time, bool) {
	s = strings.TrimSpace(s)
	layouts := isoDateTimeLayouts
	if _, isTime := t.(ir.Time); isTime {
		layouts = isoTimeLayouts
	}
	for _, layout := range layouts {
		if tm, err := time.Parse(layout, s); err == nil {
			return tm.UTC(), true
		}
	}
	return time.Time{}, false
}

// formatTimeOfDay renders the IR Time string value. Under iso it carries the
// validated source text VERBATIM (raw is guaranteed TEXT here) so no
// sub-microsecond precision is reformatted away; under the numeric encodings
// it renders the time-of-day component of the decoded instant. The
// `.999999999` form keeps full nanosecond precision and drops trailing zeros
// (and the dot entirely when the fraction is zero).
func formatTimeOfDay(raw any, tm time.Time, enc dateEncoding) string {
	if enc == dateEncodingISO {
		if s, ok := raw.(string); ok {
			return strings.TrimSpace(s)
		}
	}
	return tm.UTC().Format("15:04:05.999999999")
}

// numericValue extracts a float64 from an INTEGER or REAL storage class. ok
// is false for any other class (TEXT/BLOB), which the unix/julian encodings
// refuse. float64 unifies the seconds/millis/julian math; a unix-seconds or
// unix-millis integer is exactly representable as float64 well past any
// realistic calendar date (|value| ≪ 2^53), so the integer path is lossless.
func numericValue(raw any) (float64, bool) {
	switch v := raw.(type) {
	case int64:
		return float64(v), true
	case float64:
		return v, true
	}
	return 0, false
}

// secondsToTime converts a unix time expressed in seconds (with an optional
// fractional part for sub-second precision) to a UTC time.Time.
func secondsToTime(secs float64) time.Time {
	whole := math.Floor(secs)
	frac := secs - whole
	return time.Unix(int64(whole), int64(math.Round(frac*1e9))).UTC()
}

// julianToTime converts a Julian day number to a UTC time.Time. Julian day
// 2440587.5 is the unix epoch (1970-01-01 00:00:00 UTC), so the conversion
// is the standard (jd − epoch) × seconds-per-day.
func julianToTime(jd float64) time.Time {
	const unixEpochJulianDay = 2440587.5
	return secondsToTime((jd - unixEpochJulianDay) * 86400.0)
}

// temporalStorageError is the loud refusal when a declared temporal column
// holds a storage class the active encoding cannot interpret (a non-TEXT
// value under iso, or a non-numeric value under unixepoch/unixmillis/julian).
// The row reader wraps it with table/column/rowid.
func temporalStorageError(raw any, t ir.Type, enc dateEncoding) error {
	return fmt.Errorf(
		"date/time decode mismatch: value stored as %s (%v) is not valid for IR %s under --sqlite-date-encoding=%s; "+
			"refusing to guess (set --sqlite-date-encoding to the encoding this column actually uses, "+
			"or --type-override <col>=text to carry the raw value)",
		storageClass(raw), raw, t.String(), enc,
	)
}

// isoParseError is the loud refusal when an iso-encoded temporal column holds
// TEXT that matches none of the accepted ISO layouts.
func isoParseError(s string, t ir.Type) error {
	return fmt.Errorf(
		"date/time decode mismatch: text value %q matches no ISO layout for IR %s under --sqlite-date-encoding=iso; "+
			"refusing to guess (set --sqlite-date-encoding to the encoding this column actually uses, "+
			"or --type-override <col>=text to carry the raw value)",
		s, t.String(),
	)
}

// decodeBoolean decodes a SQLite cell for an ir.Boolean column (a column
// DECLARED BOOL/BOOLEAN, ADR-0129). The mapping is fixed and needs no
// encoding flag: INTEGER 0/1 → false/true; TEXT true|false|t|f|yes|no|1|0
// (case-insensitive) → bool. ANY other value — an INTEGER other than 0/1,
// any REAL, any BLOB, or non-truthy TEXT — is REFUSED LOUDLY rather than
// coerced: a non-boolean value in a bool column is a data problem the
// operator must see (never silently mapped to true/false). NULL is handled by
// the caller (decodeCell) before dispatch.
func decodeBoolean(raw any) (any, error) {
	switch v := raw.(type) {
	case int64:
		switch v {
		case 0:
			return false, nil
		case 1:
			return true, nil
		}
		return nil, booleanError(raw)
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true", "t", "yes", "1":
			return true, nil
		case "false", "f", "no", "0":
			return false, nil
		}
		return nil, booleanError(raw)
	default: // float64, []byte
		return nil, booleanError(raw)
	}
}

// booleanError is the loud refusal for a non-boolean value in an ir.Boolean
// column. The row reader wraps it with table/column/rowid.
func booleanError(raw any) error {
	return fmt.Errorf(
		"boolean decode mismatch: value stored as %s (%v) is not a boolean "+
			"(expected INTEGER 0/1 or TEXT true|false|t|f|yes|no|1|0); refusing to coerce "+
			"(fix the source value, or --type-override <col>=text to carry it raw)",
		storageClass(raw), raw,
	)
}
