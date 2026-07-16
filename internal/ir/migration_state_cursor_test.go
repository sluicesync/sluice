// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the cursor-value envelope (audit 2026-07-15 CRITICAL-2 /
// HIGH-1): the two OBSERVED corruption probes round-trip byte/value-
// exact, every kind the executors and PKTracker produce round-trips
// type-exact, legacy bare values keep decoding (integers exactly), and
// malformed envelopes fail loudly.

package ir

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// roundTripLastPK marshals an in-progress TableProgress carrying vals
// and unmarshals it back, returning the decoded cursor.
func roundTripLastPK(t *testing.T, vals []any) []any {
	t.Helper()
	b, err := json.Marshal(TableProgress{State: TableProgressInProgress, LastPK: vals, RowsCopied: 1})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out TableProgress
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("Unmarshal(%s): %v", b, err)
	}
	return out.LastPK
}

// TestCursorEnvelope_ObservedProbes pins the audit's two OBSERVED
// corruptions against the fixed codec:
//
//   - CRITICAL-2: cursor bytes 0x9F8041FE10 round-tripped to
//     0xEFBFBDEFBFBD41EFBFBD10 (U+FFFD replacement, 5→11 bytes,
//     bytewise GREATER — a resumed walk skipped the 0xA0…0xEF range);
//   - HIGH-1: 9007199254740995 decoded as ...996 (+1) and
//     1750000000000000123 drifted −123 through float64.
func TestCursorEnvelope_ObservedProbes(t *testing.T) {
	rawBytes := []byte{0x9F, 0x80, 0x41, 0xFE, 0x10}

	got := roundTripLastPK(t, []any{rawBytes, int64(9007199254740995), int64(1750000000000000123)})
	if len(got) != 3 {
		t.Fatalf("LastPK len = %d; want 3", len(got))
	}
	b, ok := got[0].([]byte)
	if !ok || !bytes.Equal(b, rawBytes) {
		t.Errorf("bytes probe: got %x (%T); want byte-exact %x", got[0], got[0], rawBytes)
	}
	if got[1] != int64(9007199254740995) {
		t.Errorf("2^53+3 probe: got %v (%T); want int64 9007199254740995", got[1], got[1])
	}
	if got[2] != int64(1750000000000000123) {
		t.Errorf("snowflake probe: got %v (%T); want int64 1750000000000000123", got[2], got[2])
	}
}

// TestCursorEnvelope_KindMatrix round-trips every value kind the
// executors and the migrate PKTracker can put in a cursor slice —
// type-exact, not just value-exact.
func TestCursorEnvelope_KindMatrix(t *testing.T) {
	ts := time.Date(2026, 7, 14, 10, 30, 0, 123456789, time.FixedZone("", 5*3600))
	in := []any{
		nil,
		true,
		"plain valid UTF-8 — café",
		"\x9f\x80A\xfe\x10", // invalid UTF-8 smuggled as a string
		"has a genuine � rune",
		[]byte{0x00, 0xFF, 0x7F},
		int64(-42),
		uint64(1) << 63, // above MaxInt64 — must not collapse to int64
		float64(3.5),
		ts,
	}
	got := roundTripLastPK(t, in)
	if len(got) != len(in) {
		t.Fatalf("LastPK len = %d; want %d", len(got), len(in))
	}
	if got[0] != nil {
		t.Errorf("nil: got %v", got[0])
	}
	if got[1] != true {
		t.Errorf("bool: got %v (%T)", got[1], got[1])
	}
	if got[2] != in[2] {
		t.Errorf("string: got %q", got[2])
	}
	// Invalid-UTF-8 and U+FFFD-bearing strings wear the bytes envelope
	// and come back as []byte — byte-exact, and bind-equivalent to the
	// string form. (Keeping U+FFFD out of bare stored strings is what
	// makes a stored bare U+FFFD a definitive legacy fingerprint.)
	if b, ok := got[3].([]byte); !ok || string(b) != "\x9f\x80A\xfe\x10" {
		t.Errorf("invalid-UTF-8 string: got %x (%T); want byte-exact []byte", got[3], got[3])
	}
	if b, ok := got[4].([]byte); !ok || string(b) != "has a genuine � rune" {
		t.Errorf("U+FFFD string: got %x (%T); want byte-exact []byte", got[4], got[4])
	}
	if b, ok := got[5].([]byte); !ok || !bytes.Equal(b, []byte{0x00, 0xFF, 0x7F}) {
		t.Errorf("[]byte: got %x (%T)", got[5], got[5])
	}
	if got[6] != int64(-42) {
		t.Errorf("int64: got %v (%T)", got[6], got[6])
	}
	if got[7] != uint64(1)<<63 {
		t.Errorf("uint64: got %v (%T); want uint64 %d", got[7], got[7], uint64(1)<<63)
	}
	if got[8] != float64(3.5) {
		t.Errorf("float64: got %v (%T)", got[8], got[8])
	}
	tt, ok := got[9].(time.Time)
	if !ok || !tt.Equal(ts) {
		t.Errorf("time: got %v (%T); want instant-equal %v", got[9], got[9], ts)
	}
}

// TestCursorEnvelope_ChunkSlices confirms all three per-chunk PK
// slices ride the envelope too (the migrate parallel path's
// boundaries are the same silent-loss surface).
func TestCursorEnvelope_ChunkSlices(t *testing.T) {
	in := TableProgress{
		State: TableProgressInProgress,
		Chunks: []TableChunkProgress{{
			ChunkIndex: 1,
			LowerPK:    []any{int64(9007199254740993)},
			UpperPK:    []any{int64(1750000000000000123)},
			LastPK:     []any{[]byte{0x9F, 0x80}},
			RowsCopied: 5,
			State:      TableProgressInProgress,
		}},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out TableProgress
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(out.Chunks) != 1 {
		t.Fatalf("Chunks len = %d; want 1", len(out.Chunks))
	}
	ch := out.Chunks[0]
	if ch.LowerPK[0] != int64(9007199254740993) {
		t.Errorf("LowerPK: got %v (%T)", ch.LowerPK[0], ch.LowerPK[0])
	}
	if ch.UpperPK[0] != int64(1750000000000000123) {
		t.Errorf("UpperPK: got %v (%T)", ch.UpperPK[0], ch.UpperPK[0])
	}
	if bts, ok := ch.LastPK[0].([]byte); !ok || !bytes.Equal(bts, []byte{0x9F, 0x80}) {
		t.Errorf("LastPK: got %x (%T)", ch.LastPK[0], ch.LastPK[0])
	}
}

// TestCursorEnvelope_LegacyBareValues confirms rows written by a
// pre-envelope release keep decoding: bare integers parse EXACTLY
// (int64-first, never through float64), bare strings/bools/floats
// keep their legacy shapes. Whether a legacy value is TRUSTED is the
// resume sites' call, not the decoder's.
func TestCursorEnvelope_LegacyBareValues(t *testing.T) {
	legacy := `{"state":"in_progress","last_pk":[9007199254740995,1750000000000000123,"abc",3.5,true],"rows_copied":7}`
	var out TableProgress
	if err := json.Unmarshal([]byte(legacy), &out); err != nil {
		t.Fatalf("Unmarshal legacy: %v", err)
	}
	want := []any{int64(9007199254740995), int64(1750000000000000123), "abc", float64(3.5), true}
	if len(out.LastPK) != len(want) {
		t.Fatalf("LastPK len = %d; want %d", len(out.LastPK), len(want))
	}
	for i := range want {
		if out.LastPK[i] != want[i] {
			t.Errorf("LastPK[%d]: got %v (%T); want %v (%T)",
				i, out.LastPK[i], out.LastPK[i], want[i], want[i])
		}
	}
	// A float-shaped legacy number (an old binary re-persisting its
	// drifted float64 cursor) survives decode as float64 — the resume
	// sites flag it against an integral PK column.
	legacyFloat := `{"state":"in_progress","last_pk":[1.75e+18],"rows_copied":7}`
	if err := json.Unmarshal([]byte(legacyFloat), &out); err != nil {
		t.Fatalf("Unmarshal legacy float: %v", err)
	}
	if out.LastPK[0] != float64(1.75e18) {
		t.Errorf("legacy float: got %v (%T); want float64 1.75e18", out.LastPK[0], out.LastPK[0])
	}
}

// TestCursorEnvelope_LoudFailures pins the refusal shapes: unknown
// envelope tags, tagless objects, and unsupported Go types must error
// loudly rather than decode (or encode) into a wrong cursor.
func TestCursorEnvelope_LoudFailures(t *testing.T) {
	var out TableProgress
	err := json.Unmarshal([]byte(`{"state":"in_progress","last_pk":[{"_t":"i128","v":"1"}]}`), &out)
	if err == nil || !strings.Contains(err.Error(), "unknown cursor value tag") {
		t.Errorf("unknown tag: err = %v; want unknown-cursor-value-tag", err)
	}
	err = json.Unmarshal([]byte(`{"state":"in_progress","last_pk":[{"v":"1"}]}`), &out)
	if err == nil || !strings.Contains(err.Error(), "no _t tag") {
		t.Errorf("tagless object: err = %v; want no-_t-tag", err)
	}
	_, err = json.Marshal(TableProgress{State: TableProgressInProgress, LastPK: []any{struct{}{}}})
	if err == nil || !strings.Contains(err.Error(), "no lossless JSON form") {
		t.Errorf("unsupported type: err = %v; want no-lossless-JSON-form", err)
	}
}
