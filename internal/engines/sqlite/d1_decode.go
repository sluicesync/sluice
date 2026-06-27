// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
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
func jsonString(raw json.RawMessage) (s string, ok bool, err error) {
	if isJSONNull(raw) {
		return "", false, nil
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false, err
	}
	return s, true, nil
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
