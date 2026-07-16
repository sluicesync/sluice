// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// JSON marshalling for the per-table progress entries within
// [MigrationState.TableProgress].
//
// The wire shape supports two forms inside the table_progress JSON
// map: a bare string ("complete", "no_pk_truncate_and_redo", or the
// legacy v0.3.0 "in_progress") and an object form
// ({"state":"in_progress","last_pk":[...],"rows_copied":N}).
//
// Two reasons the bare-string form survives:
//
//  1. **Backward compatibility with v0.3.0 state rows.** A v0.3.0
//     row stored just `"in_progress"` / `"complete"`. v0.4.0 has to
//     read those rows and decide what to do — see
//     [TableProgress] for the upgrade path. Without
//     accept-string-or-object on the unmarshal side, a mid-flight
//     v0.3.0 → v0.4.0 binary swap would explode on the first
//     resume.
//  2. **Operators do `psql -c "SELECT table_progress FROM
//     sluice_migrate_state"`** to debug. Compact "complete" entries
//     keep the JSON readable; only the cursor-bearing entries pay
//     the cost of the object form.
//
// MarshalJSON emits the bare string for `complete` and
// `no_pk_truncate_and_redo` (no cursor data to preserve), and the
// object form for `in_progress` (the cursor and row count are the
// load-bearing fields). UnmarshalJSON accepts either shape on input.
//
// # The cursor-value envelope (audit 2026-07-15 CRITICAL-2 / HIGH-1)
//
// The PK cursor slices (LastPK, and LowerPK/UpperPK/LastPK on each
// chunk) carry raw driver-scanned PK values, and plain encoding/json
// silently corrupts two of their classes — the same trap blobcodec's
// Bug-172 envelope closed for backup row values:
//
//   - []byte marshals to base64 (rebinding garbage), and a []byte
//     smuggled as a Go string gets every invalid-UTF-8 byte replaced
//     with U+FFFD at Marshal — either way a silently misplaced cursor,
//     so a resumed walk skips arbitrary BINARY/VARBINARY/BLOB PK
//     ranges;
//   - integers above 2^53 decode through float64 and drift, so a
//     resumed walk skips rows (forward drift) or replays far beyond
//     the at-most-one-chunk bound (backward drift).
//
// Marshal therefore wraps the non-JSON-native kinds in blobcodec-style
// tagged envelopes (`{"_t":"i64","v":9007199254740995}`,
// `{"_t":"bytes","v":"<base64>"}`, u64/f64/time likewise); valid-UTF-8
// strings, bools, and nulls stay bare. Unmarshal is envelope-aware and
// parses LEGACY bare numbers exact-int64-first so pre-envelope integer
// cursors keep reading losslessly; legacy values that are provably
// untrustworthy (a U+FFFD-bearing string, a float where the PK column
// is integral) are the RESUME sites' call — they know the PK column
// types — via migcore.SuspectLegacyCursor. Forward compat is one-way,
// like every state-shape change before it: an old binary reading an
// enveloped row tries to bind a map as a SQL parameter and fails
// loudly, never silently.

package ir

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// tableProgressObject is the on-wire representation of the object form.
// Field tags lock the wire keys so Go-side renames don't break
// deployed state rows.
//
// The Chunks field is the v0.5.0 addition; older readers ignore it and
// older writers omit it (omitempty). A v0.4.0 binary reading a v0.5.0
// row sees the single-chunk fields it knows about and skips the
// chunked layout — so resume falls back to a single-reader copy of
// whatever the chunked-LastPK state was, which is wrong but not
// catastrophic. The v0.5.0 release notes call out the same caveat as
// v0.4.0 → v0.3.0: state-row forward-compat is one-way.
//
// The IndexesBuilt field is the ADR-0077 addition (index-build overlap).
// It is omitempty, so an old state row that never wrote it decodes to
// false — read as "copy done, indexes not yet built", which re-feeds the
// table to the index pool on resume (a no-op under CREATE INDEX IF NOT
// EXISTS). A `complete` table with IndexesBuilt=true forces the object
// form (the bare-string `complete` can't carry the flag); a `complete`
// table with IndexesBuilt=false stays the compact bare string (false is
// the wire default anyway).
type tableProgressObject struct {
	State        TableProgressState   `json:"state"`
	LastPK       []any                `json:"last_pk,omitempty"`
	RowsCopied   int64                `json:"rows_copied,omitempty"`
	Chunks       []TableChunkProgress `json:"chunks,omitempty"`
	IndexesBuilt bool                 `json:"indexes_built,omitempty"`
}

// tableChunkProgressObject is the on-wire representation for one entry
// within [TableProgress.Chunks]. Mirrors [TableChunkProgress] field-for-
// field; held as its own type so the JSON tag locking is co-located
// with the marshalling code. The PK slices hold cursor-ENCODED values
// (see [encodeCursorValues]) on the marshal side.
type tableChunkProgressObject struct {
	ChunkIndex int                `json:"chunk_index"`
	LowerPK    []any              `json:"lower_pk,omitempty"`
	UpperPK    []any              `json:"upper_pk,omitempty"`
	LastPK     []any              `json:"last_pk,omitempty"`
	RowsCopied int64              `json:"rows_copied,omitempty"`
	State      TableProgressState `json:"state"`
}

// tableChunkProgressWire is the unmarshal-side twin of
// [tableChunkProgressObject]: the PK slices stay raw so each element
// can go through the envelope-aware [decodeCursorValue] (a typed []any
// decode would have already collapsed large integers through float64).
type tableChunkProgressWire struct {
	ChunkIndex int                `json:"chunk_index"`
	LowerPK    []json.RawMessage  `json:"lower_pk"`
	UpperPK    []json.RawMessage  `json:"upper_pk"`
	LastPK     []json.RawMessage  `json:"last_pk"`
	RowsCopied int64              `json:"rows_copied"`
	State      TableProgressState `json:"state"`
}

// MarshalJSON for [TableChunkProgress] emits the object form with
// JSON-tagged keys. Chunks always serialise as objects — there is no
// compact bare-string form for chunks because every chunk needs its
// range bounds preserved across resume runs.
func (c TableChunkProgress) MarshalJSON() ([]byte, error) {
	obj := tableChunkProgressObject(c)
	var err error
	if obj.LowerPK, err = encodeCursorValues(c.LowerPK); err != nil {
		return nil, fmt.Errorf("table chunk progress: lower_pk: %w", err)
	}
	if obj.UpperPK, err = encodeCursorValues(c.UpperPK); err != nil {
		return nil, fmt.Errorf("table chunk progress: upper_pk: %w", err)
	}
	if obj.LastPK, err = encodeCursorValues(c.LastPK); err != nil {
		return nil, fmt.Errorf("table chunk progress: last_pk: %w", err)
	}
	return json.Marshal(obj)
}

// UnmarshalJSON for [TableChunkProgress] decodes the object form,
// unwrapping cursor envelopes on the PK slices.
func (c *TableChunkProgress) UnmarshalJSON(b []byte) error {
	var wire tableChunkProgressWire
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(&wire); err != nil {
		return fmt.Errorf("table chunk progress: decode: %w", err)
	}
	out := TableChunkProgress{
		ChunkIndex: wire.ChunkIndex,
		RowsCopied: wire.RowsCopied,
		State:      wire.State,
	}
	var err error
	if out.LowerPK, err = decodeCursorValues(wire.LowerPK); err != nil {
		return fmt.Errorf("table chunk progress: lower_pk: %w", err)
	}
	if out.UpperPK, err = decodeCursorValues(wire.UpperPK); err != nil {
		return fmt.Errorf("table chunk progress: upper_pk: %w", err)
	}
	if out.LastPK, err = decodeCursorValues(wire.LastPK); err != nil {
		return fmt.Errorf("table chunk progress: last_pk: %w", err)
	}
	*c = out
	return nil
}

// MarshalJSON emits the compact bare-string form for terminal states
// (`complete`, `no_pk_truncate_and_redo`) and the object form for
// `in_progress` (where the cursor and row count are the load-bearing
// fields). An empty State marshals as the bare string "" so a
// zero-value TableProgress round-trips unchanged.
func (p TableProgress) MarshalJSON() ([]byte, error) {
	switch p.State {
	case TableProgressComplete:
		// Terminal copy state. Emit the compact bare string UNLESS the
		// table also carries IndexesBuilt=true (ADR-0077) — the bare
		// string can't carry that flag, so promote to the object form so
		// a resume reads "copy done AND indexes done" and skips the table
		// entirely. IndexesBuilt=false stays the compact bare string
		// (false is the wire default, decoded back the same way).
		if p.IndexesBuilt {
			return p.marshalObject()
		}
		return json.Marshal(string(p.State))
	case TableProgressNoPKTruncateAndRedo:
		// Terminal-style state: nothing useful to carry beyond the
		// label. Emit the bare string for compact wire output.
		return json.Marshal(string(p.State))
	case TableProgressInProgress:
		// In-progress entries carry cursor data; emit the object form
		// so resume can pick the cursor up. omitempty on LastPK and
		// RowsCopied keeps a v0.3.0-style "in_progress with no cursor"
		// degenerate-case write compact, even though the orchestrator
		// shouldn't be producing those — defensive against future
		// callers that pass a zero LastPK.
		return p.marshalObject()
	case "":
		// Zero value. Emit an empty bare string so the round-trip is
		// stable; callers should populate State before writing in
		// practice.
		return json.Marshal("")
	default:
		// Unknown future state. Round-trip via the object form so the
		// information isn't lost.
		return p.marshalObject()
	}
}

// marshalObject emits the object form with the LastPK cursor values
// envelope-encoded. Chunk entries encode their own PK slices via
// [TableChunkProgress.MarshalJSON].
func (p TableProgress) marshalObject() ([]byte, error) {
	obj := tableProgressObject(p)
	var err error
	if obj.LastPK, err = encodeCursorValues(p.LastPK); err != nil {
		return nil, fmt.Errorf("table progress: last_pk: %w", err)
	}
	return json.Marshal(obj)
}

// UnmarshalJSON accepts either the bare-string form (v0.3.0 wire
// shape, plus the v0.4.0 compact form for terminal states) or the
// object form (v0.4.0 wire shape with cursor data). Empty input
// returns a zero-valued TableProgress.
//
// A v0.3.0 row with `"in_progress"` decodes into State=in_progress
// with a nil LastPK; the orchestrator interprets the missing cursor
// as "fall back to truncate-and-redo for this table".
func (p *TableProgress) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		*p = TableProgress{}
		return nil
	}

	switch b[0] {
	case '"':
		// Bare string form.
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return fmt.Errorf("table progress: decode string form: %w", err)
		}
		*p = TableProgress{State: TableProgressState(s)}
		return nil
	case '{':
		// Object form. Decode strictly enough that an unknown field
		// surfaces during development (and not a silent loss of data
		// on disk) but lenient enough that a future-version row with
		// an extra field doesn't fail the read. LastPK stays raw here
		// so each element takes the envelope-aware [decodeCursorValue]
		// path (a typed []any decode would collapse large integers
		// through float64 — the HIGH-1 drift).
		var wire struct {
			State        TableProgressState   `json:"state"`
			LastPK       []json.RawMessage    `json:"last_pk"`
			RowsCopied   int64                `json:"rows_copied"`
			Chunks       []TableChunkProgress `json:"chunks"`
			IndexesBuilt bool                 `json:"indexes_built"`
		}
		dec := json.NewDecoder(bytes.NewReader(b))
		// DisallowUnknownFields would be too strict for forward-
		// compat; an older binary reading a newer row should keep
		// going, just without the new field. The contract is
		// documented in the file-level comment.
		if err := dec.Decode(&wire); err != nil {
			return fmt.Errorf("table progress: decode object form: %w", err)
		}
		lastPK, err := decodeCursorValues(wire.LastPK)
		if err != nil {
			return fmt.Errorf("table progress: last_pk: %w", err)
		}
		*p = TableProgress{
			State:        wire.State,
			LastPK:       lastPK,
			RowsCopied:   wire.RowsCopied,
			Chunks:       wire.Chunks,
			IndexesBuilt: wire.IndexesBuilt,
		}
		return nil
	}

	return errors.New("table progress: expected JSON string or object")
}

// ============================================================
// Cursor-value codec — round-trips PK cursor values through JSON.
// ============================================================
//
// The on-wire shape for values JSON cannot carry exactly is the
// blobcodec tagged envelope `{"_t":"<kind>","v":<payload>}`:
//
//   - `{"_t":"bytes","v":"<base64>"}` for []byte (and for strings
//     that are invalid UTF-8 or contain U+FFFD — a bare JSON string
//     is exact only for clean valid UTF-8, and keeping U+FFFD out of
//     the bare form makes a STORED bare string containing it a
//     definitive legacy-mangling fingerprint for the resume sites).
//   - `{"_t":"i64","v":<number>}` / `{"_t":"u64","v":"<decimal>"}` so
//     integer cursors never pass through float64.
//   - `{"_t":"f64","v":<number>}` for explicit floats (bare numbers
//     on the wire are therefore always legacy).
//   - `{"_t":"time","v":"<RFC3339Nano>"}` for time.Time.
//
// Bools, JSON null, and clean valid-UTF-8 strings stay bare — exact
// under encoding/json and debuggable in psql/mysql output.

// encodeCursorValues maps a PK cursor slice to its wire form. A nil or
// empty slice stays nil so omitempty keeps degenerate writes compact.
func encodeCursorValues(vals []any) ([]any, error) {
	if len(vals) == 0 {
		return nil, nil
	}
	out := make([]any, len(vals))
	for i, v := range vals {
		ev, err := encodeCursorValue(v)
		if err != nil {
			return nil, fmt.Errorf("cursor value [%d]: %w", i, err)
		}
		out[i] = ev
	}
	return out, nil
}

// encodeCursorValue returns the wire form of one PK cursor value.
// Unsupported types are a loud error: a cursor that cannot round-trip
// losslessly must fail the checkpoint write, not corrupt the resume.
func encodeCursorValue(v any) (any, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case bool:
		return x, nil
	case string:
		if utf8.ValidString(x) && !strings.ContainsRune(x, utf8.RuneError) {
			return x, nil
		}
		return map[string]any{"_t": "bytes", "v": base64.StdEncoding.EncodeToString([]byte(x))}, nil
	case []byte:
		return map[string]any{"_t": "bytes", "v": base64.StdEncoding.EncodeToString(x)}, nil
	case int:
		return map[string]any{"_t": "i64", "v": int64(x)}, nil
	case int8:
		return map[string]any{"_t": "i64", "v": int64(x)}, nil
	case int16:
		return map[string]any{"_t": "i64", "v": int64(x)}, nil
	case int32:
		return map[string]any{"_t": "i64", "v": int64(x)}, nil
	case int64:
		return map[string]any{"_t": "i64", "v": x}, nil
	case uint:
		return map[string]any{"_t": "u64", "v": strconv.FormatUint(uint64(x), 10)}, nil
	case uint8:
		return map[string]any{"_t": "u64", "v": strconv.FormatUint(uint64(x), 10)}, nil
	case uint16:
		return map[string]any{"_t": "u64", "v": strconv.FormatUint(uint64(x), 10)}, nil
	case uint32:
		return map[string]any{"_t": "u64", "v": strconv.FormatUint(uint64(x), 10)}, nil
	case uint64:
		return map[string]any{"_t": "u64", "v": strconv.FormatUint(x, 10)}, nil
	case float32:
		return encodeCursorFloat(float64(x))
	case float64:
		return encodeCursorFloat(x)
	case time.Time:
		return map[string]any{"_t": "time", "v": x.Format(time.RFC3339Nano)}, nil
	default:
		return nil, fmt.Errorf("PK cursor value of type %T has no lossless JSON form", v)
	}
}

// encodeCursorFloat envelopes a finite float. Non-finite floats can't
// be PK values on any shipping engine's orderable-PK path and have no
// JSON number form; refuse rather than invent a sentinel.
func encodeCursorFloat(f float64) (any, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, fmt.Errorf("non-finite float %v cannot be a PK cursor value", f)
	}
	return map[string]any{"_t": "f64", "v": f}, nil
}

// decodeCursorValues is the inverse of [encodeCursorValues].
func decodeCursorValues(raws []json.RawMessage) ([]any, error) {
	if len(raws) == 0 {
		return nil, nil
	}
	out := make([]any, len(raws))
	for i, raw := range raws {
		v, err := decodeCursorValue(raw)
		if err != nil {
			return nil, fmt.Errorf("cursor value [%d]: %w", i, err)
		}
		out[i] = v
	}
	return out, nil
}

// decodeCursorValue decodes one wire element: tagged envelopes unwrap
// to their native Go shape; bare values (including everything a
// pre-envelope release wrote) decode with legacy semantics — numbers
// parse exact-int64-first so a large legacy integer cursor does not
// drift through float64.
func decodeCursorValue(raw json.RawMessage) (any, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	switch raw[0] {
	case '{':
		var env struct {
			Tag     string          `json:"_t"`
			Payload json.RawMessage `json:"v"`
		}
		if err := json.Unmarshal(raw, &env); err != nil {
			return nil, fmt.Errorf("decode envelope: %w", err)
		}
		if env.Tag == "" {
			return nil, errors.New("object cursor value carries no _t tag; not a sluice cursor envelope")
		}
		return decodeCursorEnvelope(env.Tag, env.Payload)
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("decode string: %w", err)
		}
		return s, nil
	case 't', 'f':
		var b bool
		if err := json.Unmarshal(raw, &b); err != nil {
			return nil, fmt.Errorf("decode bool: %w", err)
		}
		return b, nil
	case '[':
		return nil, errors.New("array is not a valid cursor value")
	default:
		// Bare number — always legacy (new writers envelope every
		// numeric kind). Exact int64 first (HIGH-1: 9007199254740995
		// must not become ...996); anything non-integral falls to
		// float64, which the resume sites treat as suspect when the
		// PK column is integral.
		s := string(raw)
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n, nil
		}
		// Legacy BIGINT UNSIGNED cursors above MaxInt64 were persisted
		// bare too — recover them losslessly instead of letting them
		// fall to float64 and trip the float-over-integer suspect gate.
		// This recovery window is only as good as the persisted digits:
		// a pre-envelope binary that already drifted the value through
		// float64 and re-persisted it wrote integral digits that decode
		// "exactly" wrong here, indistinguishable from a faithful write
		// (see migcore.SuspectLegacyCursor's named residual).
		if u, err := strconv.ParseUint(s, 10, 64); err == nil {
			return u, nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("cursor number %q: %w", s, err)
		}
		return f, nil
	}
}

// decodeCursorEnvelope converts a tagged envelope back to its Go-native
// shape. Unknown tags are a loud error — a state row written by a newer
// sluice (or corrupted on disk) must not decode into a wrong cursor.
func decodeCursorEnvelope(tag string, payload json.RawMessage) (any, error) {
	switch tag {
	case "bytes":
		var s string
		if err := json.Unmarshal(payload, &s); err != nil {
			return nil, fmt.Errorf("bytes payload: %w", err)
		}
		out, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("bytes base64: %w", err)
		}
		return out, nil
	case "i64":
		var n int64
		if err := json.Unmarshal(payload, &n); err != nil {
			return nil, fmt.Errorf("i64 payload: %w", err)
		}
		return n, nil
	case "u64":
		var s string
		if err := json.Unmarshal(payload, &s); err != nil {
			return nil, fmt.Errorf("u64 payload: %w", err)
		}
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("u64 parse: %w", err)
		}
		return n, nil
	case "f64":
		var f float64
		if err := json.Unmarshal(payload, &f); err != nil {
			return nil, fmt.Errorf("f64 payload: %w", err)
		}
		return f, nil
	case "time":
		var s string
		if err := json.Unmarshal(payload, &s); err != nil {
			return nil, fmt.Errorf("time payload: %w", err)
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return nil, fmt.Errorf("time parse: %w", err)
		}
		return t, nil
	default:
		return nil, fmt.Errorf("unknown cursor value tag %q (state row written by a newer sluice?)", tag)
	}
}
