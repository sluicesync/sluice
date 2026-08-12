// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"unicode/utf8"
)

// d1StorageValue reconstructs the SAME Go storage-class value the modernc file
// path hands back (int64 / float64 / string / []byte / nil) from D1's
// (typeof, exact-text/hex) pair — the LOAD-BEARING fidelity step (ADR-0132 §4).
// The row projection returns, per user column, `typeof(c)` AND
// `CASE typeof(c) WHEN 'blob' THEN hex(c) ELSE CAST(c AS TEXT) END`, so:
//
//	null    → nil                      (faithful for every IR type)
//	integer → strconv.ParseInt(text)   → int64  — EXACT, the whole point: a
//	          value > 2^53 (snowflake ID, ns timestamp) round-trips with no
//	          rounding, unlike a bare JSON number.
//	real    → strconv.ParseFloat(text) → float64
//	text    → the string
//	blob    → hex.DecodeString(text)   → []byte
//
// The reconstructed value is then handed to the shared [decodeCell], which
// applies the column's resolved IR type, the ADR-0129 date/bool policy, and the
// SAME loud-failure storage-class fidelity as the file engine (a class that
// can't be faithfully held in the resolved IR type is refused). An int/real
// text that fails to parse, or an unrecognised typeof, is itself a loud error
// (the caller wraps it with table/column/row) — never a silent nil or guess.
func d1StorageValue(typeofText string, valueRaw json.RawMessage) (any, error) {
	// typeof never returns NULL for an existing cell — it returns the literal
	// text "null" for a NULL value — but treat an explicit JSON null defensively
	// the same as the "null" class.
	if typeofText == "null" || isJSONNull(valueRaw) {
		return nil, nil
	}

	text, ok, err := jsonString(valueRaw)
	if err != nil {
		return nil, fmt.Errorf("decode value: %w", err)
	}
	if !ok {
		// A non-null, non-string value column means the projection did not run
		// (a bare JSON number would be the lossy default path this engine exists
		// to avoid) — refuse loudly rather than silently round it.
		return nil, fmt.Errorf(
			"expected a CAST(... AS TEXT) string value but got %s "+
				"(the lossless projection was not applied)", string(valueRaw),
		)
	}

	switch typeofText {
	case "integer":
		n, perr := strconv.ParseInt(text, 10, 64)
		if perr != nil {
			return nil, fmt.Errorf("integer text %q is not a valid int64: %w", text, perr)
		}
		return n, nil
	case "real":
		f, perr := strconv.ParseFloat(text, 64)
		if perr != nil {
			return nil, fmt.Errorf("real text %q is not a valid float64: %w", text, perr)
		}
		return f, nil
	case "text":
		return text, nil
	case "blob":
		b, derr := hex.DecodeString(text)
		if derr != nil {
			return nil, fmt.Errorf("blob hex %q is not valid hex: %w", text, derr)
		}
		return b, nil
	default:
		return nil, fmt.Errorf(
			"unrecognised typeof %q (want integer/real/text/blob/null)", typeofText,
		)
	}
}

// isJSONNull reports whether raw is the JSON null literal (or absent).
func isJSONNull(raw json.RawMessage) bool {
	return len(raw) == 0 || string(raw) == "null"
}

// jsonString extracts a Go string from a JSON string value. ok is false for
// JSON null (so the caller can distinguish absent from ""); a non-string,
// non-null JSON value is an error.
//
// encoding/json.Unmarshal never errors on the two shapes it cannot represent
// faithfully — it silently substitutes U+FFFD for each invalid-UTF-8 byte and
// for each lone-surrogate \u escape. SQLite TEXT is byte-agnostic and legally
// carries invalid UTF-8 (`CAST(x'FFFE61' AS TEXT)`), the trigger capture and
// the D1 API pass those bytes through verbatim, and the cold-start snapshot
// (direct rows.Scan) carries them faithfully — so the silent substitution here
// made the SAME logical row diverge between the snapshot and CDC phases at
// exit 0, with a mangled value no target could refuse (audit 2026-08-11
// SQT-1, observed). Both vectors are refused loudly BEFORE Unmarshal; the
// caller wraps the error with table/column/row context.
func jsonString(raw json.RawMessage) (s string, ok bool, err error) {
	if isJSONNull(raw) {
		return "", false, nil
	}
	if !utf8.Valid(raw) {
		return "", false, fmt.Errorf(
			"value is not valid UTF-8 (encoding/json would silently rewrite each invalid byte to U+FFFD, "+
				"diverging from the bytes the snapshot phase carried); the source value is intact — store "+
				"raw bytes as BLOB rather than TEXT, or repair the value at the source: %q", raw,
		)
	}
	if off, found := loneSurrogateEscapeAt(raw); found {
		return "", false, fmt.Errorf(
			"value carries a lone UTF-16 surrogate escape at byte %d (encoding/json would silently rewrite "+
				"it to U+FFFD); the captured JSON is malformed — repair the value at the source: %q", off, raw,
		)
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false, err
	}
	return s, true, nil
}

// loneSurrogateEscapeAt scans a JSON string token for a \uD800–\uDFFF escape
// that does not form a valid surrogate pair — the second shape
// encoding/json.Unmarshal silently rewrites to U+FFFD instead of erroring.
// Escape-aware by construction: it only inspects sequences introduced by an
// unescaped backslash, so literal text can never false-positive. Malformed
// escapes (`\uZZZZ`, truncated) are left for Unmarshal to refuse loudly — this
// scan exists only for the shape Unmarshal accepts-and-mangles.
func loneSurrogateEscapeAt(raw []byte) (offset int, found bool) {
	hexVal := func(b []byte) (int, bool) {
		if len(b) < 4 {
			return 0, false
		}
		v := 0
		for _, c := range b[:4] {
			switch {
			case c >= '0' && c <= '9':
				v = v<<4 | int(c-'0')
			case c >= 'a' && c <= 'f':
				v = v<<4 | int(c-'a'+10)
			case c >= 'A' && c <= 'F':
				v = v<<4 | int(c-'A'+10)
			default:
				return 0, false
			}
		}
		return v, true
	}
	for i := 0; i < len(raw)-1; i++ {
		if raw[i] != '\\' {
			continue
		}
		if raw[i+1] != 'u' {
			i++ // skip the escaped character (covers \\ so a literal \u never matches)
			continue
		}
		v, ok := hexVal(raw[i+2:])
		if !ok {
			return i, false // malformed escape — Unmarshal refuses this loudly itself
		}
		switch {
		case v >= 0xD800 && v <= 0xDBFF:
			// High surrogate: valid only when immediately followed by a low one.
			if len(raw) >= i+12 && raw[i+6] == '\\' && raw[i+7] == 'u' {
				if w, ok2 := hexVal(raw[i+8:]); ok2 && w >= 0xDC00 && w <= 0xDFFF {
					i += 11 // consume the full pair
					continue
				}
			}
			return i, true
		case v >= 0xDC00 && v <= 0xDFFF:
			// Low surrogate with no preceding high one (a preceding valid pair
			// was consumed above).
			return i, true
		}
		i += 5 // consume the ordinary \uXXXX
	}
	return 0, false
}

// jsonScalarString renders a catalog (PRAGMA) scalar as a string regardless of
// whether D1 serialised it as a JSON string or number. The catalog columns
// sluice reads (type names, default text) are small and safe as plain JSON
// (ADR-0132 §5 — only the DATA read needs the CAST/typeof exactness). A JSON
// null yields ("", false).
func jsonScalarString(raw json.RawMessage) (s string, ok bool, err error) {
	if isJSONNull(raw) {
		return "", false, nil
	}
	// The catalog strings this reads (type names, DEFAULT text) feed emitted
	// DDL, so the same two silent-U+FFFD vectors jsonString refuses would land
	// a silently ALTERED default expression on the target. Numbers and bools
	// are always valid UTF-8, so the guard cannot false-refuse those arms
	// (SQT-1 sibling, enumerated in the fix commit).
	if !utf8.Valid(raw) {
		return "", false, fmt.Errorf("catalog scalar is not valid UTF-8 (encoding/json would silently "+
			"rewrite each invalid byte to U+FFFD): %q", raw)
	}
	if off, found := loneSurrogateEscapeAt(raw); found {
		return "", false, fmt.Errorf("catalog scalar carries a lone UTF-16 surrogate escape at byte %d "+
			"(encoding/json would silently rewrite it to U+FFFD): %q", off, raw)
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return "", false, err
	}
	switch t := v.(type) {
	case string:
		return t, true, nil
	case float64:
		// A PRAGMA scalar that came back numeric (rare for the string columns
		// sluice reads, but be robust): render it without a trailing ".0".
		return strconv.FormatFloat(t, 'f', -1, 64), true, nil
	case bool:
		if t {
			return "1", true, nil
		}
		return "0", true, nil
	default:
		return "", false, fmt.Errorf("unexpected JSON scalar %s", string(raw))
	}
}

// jsonInt extracts an integer from a catalog (PRAGMA) scalar. PRAGMA emits
// these (cid, notnull, pk, seq, unique, id) as small integers well within
// float64's exact range, so a JSON-number round-trip is safe here (unlike the
// DATA path, which must use CAST/typeof). A JSON string of digits is also
// accepted (some PRAGMA columns can serialise either way).
func jsonInt(raw json.RawMessage) (int64, error) {
	if isJSONNull(raw) {
		return 0, nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return 0, err
	}
	switch t := v.(type) {
	case float64:
		return int64(t), nil
	case string:
		return strconv.ParseInt(t, 10, 64)
	case bool:
		if t {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("unexpected JSON scalar %s for an integer field", string(raw))
	}
}

// rowString reads a required string field from a catalog row, erroring if it is
// absent or null. Used for the PRAGMA columns sluice depends on (names, types).
func rowString(row d1Row, key string) (string, error) {
	s, ok, err := jsonScalarString(row[key])
	if err != nil {
		return "", fmt.Errorf("d1: catalog field %q: %w", key, err)
	}
	if !ok {
		return "", fmt.Errorf("d1: catalog field %q is missing or null", key)
	}
	return s, nil
}

// rowNullString reads an optional string field from a catalog row (e.g. a
// PRAGMA table_info default, an FK parent column, an index column name), mapping
// JSON null → an invalid sql.NullString-like (value "", ok false).
func rowNullString(row d1Row, key string) (value string, ok bool, err error) {
	v, present, err := jsonScalarString(row[key])
	if err != nil {
		return "", false, fmt.Errorf("d1: catalog field %q: %w", key, err)
	}
	return v, present, nil
}

// rowInt reads an integer catalog field.
func rowInt(row d1Row, key string) (int64, error) {
	n, err := jsonInt(row[key])
	if err != nil {
		return 0, fmt.Errorf("d1: catalog field %q: %w", key, err)
	}
	return n, nil
}
