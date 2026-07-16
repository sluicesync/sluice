// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package flatfile

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"unicode/utf8"
)

// stageNDJSON stages a newline-delimited-JSON file: one top-level JSON
// OBJECT per line; keys are columns (first-seen order, the column set may
// grow mid-file — later lines add columns, earlier rows read as NULL there);
// values follow the raw-text contract described below. Blank lines are
// skipped; anything that is not exactly one object per line is refused
// loudly naming the line.
func (e Engine) stageNDJSON(ctx context.Context, r *bufio.Reader, path string, st *stager) error {
	// Strip a UTF-8 BOM at file start (same lossless-with-WARN posture as CSV).
	if head, err := r.Peek(len(utf8BOM)); err == nil && string(head) == utf8BOM {
		_, _ = r.Discard(len(utf8BOM))
		slog.Warn("ndjson: stripped a UTF-8 byte-order mark at file start", slog.String("file", path))
	}

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), maxFieldBytes)

	line := 0
	rows := 0
	for sc.Scan() {
		line++
		raw := bytes.TrimRight(sc.Bytes(), "\r") // tolerate CRLF files
		if len(bytes.TrimSpace(raw)) == 0 {
			continue // blank line
		}
		if i := bytes.IndexByte(raw, 0x00); i >= 0 {
			return fmt.Errorf("ndjson: %q line %d %s", path, line, errNULByte.Error())
		}
		if !utf8.Valid(raw) {
			return fmt.Errorf("ndjson: %q line %d is not valid UTF-8 — sluice reads UTF-8 only; "+
				"transcode the file first (e.g. `iconv -t UTF-8`)", path, line)
		}
		keys, vals, err := parseNDJSONLine(raw)
		if err != nil {
			return fmt.Errorf("ndjson: %q line %d: %w", path, line, err)
		}
		if len(keys) == 0 && st.columnCount() == 0 {
			return fmt.Errorf("ndjson: %q line %d is an empty object before any column is known — "+
				"no columns can be derived", path, line)
		}
		if err := st.upsertColumns(ctx, keys); err != nil {
			return fmt.Errorf("ndjson: %q line %d: %w", path, line, err)
		}
		if err := st.insertByName(ctx, keys, vals); err != nil {
			return fmt.Errorf("ndjson: %q line %d: %w", path, line, err)
		}
		rows++
	}
	if err := sc.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return fmt.Errorf("ndjson: %q line %d exceeds %d bytes — not a line-delimited file?", path, line+1, maxFieldBytes)
		}
		return fmt.Errorf("ndjson: read %q: %w", path, err)
	}
	if st.columnCount() == 0 {
		return fmt.Errorf("ndjson: %q has no records (no columns can be derived)", path)
	}
	return nil
}

// parseNDJSONLine parses exactly one JSON object off a line, returning its
// keys in appearance order and each value rendered per the ADR-0163 raw-text
// contract:
//
//   - string  → the JSON-decoded string (\u escapes and all)
//   - number  → its RAW source text, verbatim — NEVER through a float64, so
//     int64 > 2^53 and arbitrary-precision decimals land exact (the D1
//     lesson)
//   - true/false → the text "true"/"false"
//   - null    → SQL NULL (nil)
//   - object/array → the raw JSON text of the value, verbatim
//
// A duplicate key within the object is refused loudly (encoding/json's
// map decode would silently keep the last — a silent-loss shape), as is
// a non-object top level or trailing content after the object.
func parseNDJSONLine(raw []byte) (keys []string, vals []any, err error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	tok, err := dec.Token()
	if err != nil {
		return nil, nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, nil, fmt.Errorf("top-level value is %s; NDJSON requires one JSON OBJECT per line", tokenName(tok))
	}

	// seen maps the LOWER-CASED key to the spelling first seen: staged SQLite
	// column names are case-insensitive, so `{"a":…,"A":…}` cannot be held —
	// refuse it here with a named message instead of leaking a raw
	// duplicate-column error from the staging database.
	seen := map[string]string{}
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return nil, nil, fmt.Errorf("invalid JSON: %w", err)
		}
		key, ok := kt.(string)
		if !ok {
			return nil, nil, fmt.Errorf("invalid JSON object key %v", kt)
		}
		if key == "" {
			return nil, nil, errors.New(`empty object key "" — a column needs a name; rename the key`)
		}
		if prior, dup := seen[strings.ToLower(key)]; dup {
			if prior == key {
				return nil, nil, fmt.Errorf("duplicate key %q in one object — a last-wins parse would silently drop the first value", key)
			}
			return nil, nil, fmt.Errorf("keys %q and %q collide case-insensitively — staged SQLite column names are "+
				"case-insensitive, so both cannot be held; rename one", prior, key)
		}
		seen[strings.ToLower(key)] = key

		var rawVal json.RawMessage
		if err := dec.Decode(&rawVal); err != nil {
			return nil, nil, fmt.Errorf("invalid JSON value for key %q: %w", key, err)
		}
		v, err := ndjsonValue(rawVal)
		if err != nil {
			return nil, nil, fmt.Errorf("key %q: %w", key, err)
		}
		keys = append(keys, key)
		vals = append(vals, v)
	}
	if _, err := dec.Token(); err != nil { // consume '}'
		return nil, nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, nil, errors.New("trailing content after the object — NDJSON is exactly one object per line")
	}
	return keys, vals, nil
}

// ndjsonValue renders one raw JSON value per the raw-text contract.
func ndjsonValue(raw json.RawMessage) (any, error) {
	t := bytes.TrimSpace(raw)
	if len(t) == 0 {
		return nil, errors.New("empty JSON value")
	}
	switch t[0] {
	case '"':
		var s string
		if err := json.Unmarshal(t, &s); err != nil {
			return nil, fmt.Errorf("invalid JSON string: %w", err)
		}
		// The \u0000 escape decodes to a NUL the raw-byte scan can never
		// see (audit L-D0-14) — and PostgreSQL text cannot hold NUL, so
		// letting it through dies later as a generic COPY error with no
		// line coordinates. Refuse HERE, matching the CSV raw-NUL posture:
		// the wrapping callers name the file, line, and key. (A NUL escape
		// inside a NESTED object/array stays as its 6-character escape
		// text — TEXT-representable — so only the decoded-string case is
		// unholdable.)
		if strings.ContainsRune(s, 0x00) {
			return nil, errors.New(`string decodes to a NUL (the \u0000 escape) — ` +
				"the staged TEXT value cannot carry NUL to the target; remove the escape or re-encode the value")
		}
		return s, nil
	case '{', '[':
		return string(t), nil // nested document: raw JSON text, verbatim
	case 't':
		return "true", nil
	case 'f':
		return "false", nil
	case 'n':
		return nil, nil // JSON null → SQL NULL
	default:
		// A number: carry the RAW token text (already syntax-validated by the
		// decoder), never a float64 round-trip.
		return string(t), nil
	}
}

// tokenName renders a JSON token kind for the non-object refusal.
func tokenName(tok json.Token) string {
	switch v := tok.(type) {
	case json.Delim:
		return "an array" // '{' was handled; '[' is the only other opener
	case string:
		return "a string"
	case json.Number:
		return "a number"
	case bool:
		return "a boolean"
	case nil:
		return "null"
	default:
		return fmt.Sprintf("%T", v)
	}
}
