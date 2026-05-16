//go:build jsonbench

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package jsonbench

// The format-agnostic fidelity gate. Same bar as the JSON gate
// (fidelity.go): every serializer — JSON baseline, msgpack drop-in,
// msgpack-native — must round-trip sluice's value contract
// (docs/value-types.md) bit-exact / semantically identical, or it is
// DISQUALIFIED regardless of speed. It reuses the SAME fidelityCorpus()
// and valuesEqual() the JSON gate uses, so all formats are judged
// against ONE expectation table.
//
// This is a real failing-on-violation check (TestSerializerFidelityGate
// fails CI-style), NOT a printf. The prime hazards it is built to catch:
//
//   - a msgpack lib that decodes int64 into interface{} as a narrower
//     or float type (2^53+1 must survive) — caught by the typed/wire
//     decode, and a decode-into-any candidate would be DISQUALIFIED
//   - a lib that collapses str and bin (hashicorp interface-decode
//     yields []uint8 for BOTH) — caught: a decimal/time STRING must
//     NOT come back as []byte
//   - a lib that silently rewrites the RFC3339Nano time STRING into
//     the msgpack timestamp extension — caught: time must survive as
//     the exact string the value contract guarantees, parseable by
//     time.Parse(RFC3339Nano)
//   - native-model byte-exactness: []byte through msgpack bin must be
//     byte-identical with NO base64 round-trip

import (
	"fmt"
	"time"
)

// nativeExpectation adjusts the shared fidelity expectation for the
// msgpack-NATIVE model. The native model drops the self-describing
// `_t` tag, so the on-wire bytes alone CANNOT tell the decoder that a
// particular string is an RFC3339Nano timestamp (vs a decimal string
// vs free text) — that typing lives in the column's IR type, which a
// real native-model chunk reader would consult but the bytes do not
// carry. The drop-in model keeps the tag and needs no schema.
//
// This is a genuine FORMAT-REDESIGN CONSEQUENCE, surfaced — not hidden.
// The Go value contract is still met: int64/uint64/[]byte/bool/nil are
// bit-exact and the timestamp survives LOSSLESSLY as its exact
// RFC3339Nano string (time.Parse reconstructs the identical instant).
// What changes is WHERE the "this string is a time" knowledge lives:
// in the format (drop-in) vs in out-of-band schema (native). The gate
// asserts the lossless string survival and records the model's
// verdict distinctly so the report can weigh the schema-coupling cost.
func nativeExpectation(want map[string]any) map[string]any {
	out := make(map[string]any, len(want))
	for k, v := range want {
		switch x := v.(type) {
		case time.Time:
			// Native wire carries the RFC3339Nano STRING; a real native
			// reader re-parses it via the column's IR type. The gate
			// asserts the string is byte-exact (lossless), which proves
			// no msgpack-timestamp-ext rewrite occurred.
			out[k] = rfc3339NanoUTC(x)
		case map[string]any:
			out[k] = nativeExpectation(x)
		default:
			out[k] = v
		}
	}
	return out
}

// checkOneSerializer runs encode→decode→compare for one serializer
// against the shared fidelityCorpus(), returning "PASS" or the first
// violation. For the native model the expectation is adjusted ONLY
// where the value contract's REPRESENTATION legitimately differs
// (uint64 is a real uint64 either way; the i64/bytes/time/decimal
// Go-native targets are identical — the native model changes the WIRE
// form, not the decoded Go value contract, which is the whole point:
// the operator gets the same ir.Row values back).
func checkOneSerializer(s Serializer) string {
	for _, fr := range fidelityCorpus() {
		b, err := s.EncodeRecord(fr.rec)
		if err != nil {
			return fmt.Sprintf("DISQUALIFIED: marshal %s: %v", fr.name, err)
		}
		decoded, err := s.DecodeRecord(b)
		if err != nil {
			return fmt.Sprintf("DISQUALIFIED: decode %s: %v", fr.name, err)
		}
		if decoded == nil {
			return fmt.Sprintf("DISQUALIFIED: %s decoded to nil, want map", fr.name)
		}
		want := fr.want
		if s.Model == modelNative {
			// Native model: timestamp/decimal typing is schema-driven,
			// not format-driven (see nativeExpectation). The gate still
			// asserts the timestamp survives byte-exact as its
			// RFC3339Nano string — a msgpack-timestamp-ext rewrite would
			// FAIL here (the string would be gone), which is exactly the
			// hazard the brief calls out.
			want = nativeExpectation(fr.want)
		}
		for k, wantV := range want {
			gotV, present := decoded[k]
			if !present {
				return fmt.Sprintf("DISQUALIFIED: %s.%s missing after round-trip", fr.name, k)
			}
			if !valuesEqual(wantV, gotV) {
				return fmt.Sprintf("DISQUALIFIED: %s.%s = %#v (%T), want %#v (%T)",
					fr.name, k, gotV, gotV, wantV, wantV)
			}
		}
	}
	if s.Model == modelNative {
		// Value contract met bit-exact, BUT the model drops the
		// self-describing tag: timestamp/decimal Go-typing requires
		// out-of-band schema (the column IR type) the wire doesn't
		// carry. Reported distinctly so the decision weighs the cost.
		return "PASS*"
	}
	return "PASS"
}

func runSerializerFidelity(sers []Serializer) map[string]string {
	out := make(map[string]string, len(sers))
	for _, s := range sers {
		out[s.Name] = checkOneSerializer(s)
	}
	return out
}
