//go:build jsonbench

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package jsonbench

// Format-agnostic serializer abstraction.
//
// The original harness (libs.go / fidelity.go / harness.go) is
// JSON-string-shaped: a Lib is a Marshal/Unmarshal pair on `any`, and
// decodeLine hard-codes JSON-specific recovery (probe into
// `map[string]json.RawMessage`, `bytes.TrimSpace`, the `{`-prefix
// sniff). That is exactly faithful to what sluice's production decode
// does today — and it stays the baseline, unchanged, so the JSON rows
// remain apples-to-apples.
//
// To answer the real question — "is msgpack worth adding as an option,
// or replacing the JSON-Lines default?" — the harness needs a wider
// contract than "a JSON Marshal/Unmarshal pair": encode a slice of the
// SAME logical records to bytes, decode those bytes back to the SAME
// Go-native value contract (docs/value-types.md), regardless of whether
// the wire form is JSON text or a msgpack binary. That is the
// Serializer interface.
//
// Two msgpack ENCODING MODELS are measured, because they answer two
// different questions:
//
//   - Drop-in (modelDropIn): encode/decode the byte-for-byte SAME
//     tagged-value envelope sluice uses today ({"_t":"i64","v":N},
//     base64 bytes, RFC3339Nano time strings) — just via msgpack
//     instead of JSON. Pure format-swap. Keeps the two-hop typed
//     decode contract. This is the "could we flip the codec without
//     touching the record model" measurement.
//   - Native (modelNative): exploit msgpack's NATIVE int64 / uint64 /
//     binary / bool / nil so the `_t`/`v` tagging and base64-for-bytes
//     are unnecessary. Smaller, fewer allocations, and — critically —
//     int64 is precision-exact with no string/float games. This is NOT
//     a drop-in: it changes the on-disk record model and the decode
//     contract. It is the honest "replace the default" measurement, and
//     its format-redesign implications are called out in the report.
//
// time.Time is deliberately kept as an RFC3339Nano STRING in BOTH
// models. sluice's value contract stores timestamps as RFC3339Nano
// strings in the envelope; a msgpack library's native timestamp
// extension (msgpack ext -1) is a DIFFERENT on-disk representation and
// a DIFFERENT decode contract (it would decode to time.Time directly,
// losing the string the format currently guarantees). The fidelity
// gate asserts the string survives byte-exact; a candidate that
// silently rewrites it to the ext form is reported.

import (
	"fmt"
	"time"
)

// encModel identifies which of the two msgpack encoding models a
// serializer exercises (JSON serializers are modelJSONLines — the
// shipping format, the baseline every other row is judged against).
type encModel int

const (
	// modelJSONLines is the shipping format: JSON-Lines tagged-value
	// envelope, stdlib encoding/json semantics. The baseline.
	modelJSONLines encModel = iota
	// modelDropIn is the msgpack format-swap: identical tagged-value
	// envelope, msgpack on the wire instead of JSON. Drop-in.
	modelDropIn
	// modelNative is the msgpack record-model redesign: native
	// int64/uint64/bin/bool/nil, no `_t`/`v` tagging, no base64. NOT a
	// drop-in — changes the on-disk record model + decode contract.
	modelNative
)

func (m encModel) String() string {
	switch m {
	case modelJSONLines:
		return "JSON-Lines (ships today)"
	case modelDropIn:
		return "msgpack drop-in (same envelope)"
	case modelNative:
		return "msgpack-native (redesigned record model)"
	default:
		return "unknown"
	}
}

// Serializer is the format-agnostic candidate the extended harness +
// fidelity gate consume. It encodes a slice of the SAME logical records
// the JSON harness uses (one []byte per record, as the per-line chunk
// path produces) and decodes those bytes back to the value contract.
//
// DecodeRecord returns the decoded record as a map[string]any whose
// values match docs/value-types.md (int64 stays int64, []byte stays
// []byte, etc.) — the SAME post-decodeValue shape the JSON harness's
// decodeLine produces, so the fidelity gate compares both formats
// against one expectation table.
type Serializer struct {
	// Name is the human-readable identifier the markdown emitter uses.
	Name string

	// Surface is the short note rendered in the report (which library /
	// API / model this row exercises).
	Surface string

	// Model is the encoding model (baseline JSON, msgpack drop-in, or
	// msgpack-native). The report groups rows by this so the
	// "format-swap vs format-redesign" distinction is never lost.
	Model encModel

	// HumanInspectable reports whether the on-disk bytes are
	// eyeball-readable with standard tools (the `--compression=none`
	// `head file.jsonl | jq .` operator case sluice deliberately
	// supports). JSON: yes. msgpack: no (binary). Surfaced in the
	// report's tradeoff section, not a fidelity failure.
	HumanInspectable bool

	// EncodeRecord serialises one record (the map[string]any the
	// production WriteRow/WriteChange builds) to its wire bytes.
	EncodeRecord func(rec map[string]any) ([]byte, error)

	// DecodeRecord decodes one record's wire bytes back through the
	// format's faithful two-hop-equivalent decode to the value
	// contract. Returns map[string]any (post-decodeValue shape).
	DecodeRecord func(b []byte) (map[string]any, error)
}

// allSerializers is the registry the extended harness benchmarks and
// fidelity-gates. Built at first use from the JSON Lib registry (every
// JSON candidate, re-expressed as a Serializer over the existing
// faithful decodeLine path so the baseline numbers are unchanged) plus
// the msgpack drop-in and native candidates.
func allSerializers() []Serializer {
	out := make([]Serializer, 0, len(allLibs)+4)

	// --- JSON baselines: every Lib, re-expressed as a Serializer over
	// the EXISTING faithful two-hop decodeLine path. Unchanged numbers;
	// they remain the apples-to-apples reference. ---
	for _, l := range allLibs {
		l := l
		out = append(out, Serializer{
			Name:             l.Name,
			Surface:          l.Surface,
			Model:            modelJSONLines,
			HumanInspectable: true,
			EncodeRecord: func(rec map[string]any) ([]byte, error) {
				return l.Marshal(rec)
			},
			DecodeRecord: func(b []byte) (map[string]any, error) {
				v, err := decodeLine(b, l)
				if err != nil {
					return nil, err
				}
				if v == nil {
					return nil, nil
				}
				m, ok := v.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("decoded to %T, want map", v)
				}
				return m, nil
			},
		})
	}

	// --- msgpack candidates: drop-in (same envelope) + native
	// (redesigned model), for each library. Registered in
	// msgpack_*.go behind the same jsonbench build tag. ---
	out = append(out, msgpackSerializers()...)

	return out
}

// rfc3339NanoUTC formats t the way sluice's encodeValue does for the
// "time" tag. Shared by both msgpack models so the time-as-string
// contract is identical to production (NOT the msgpack timestamp ext).
func rfc3339NanoUTC(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}
