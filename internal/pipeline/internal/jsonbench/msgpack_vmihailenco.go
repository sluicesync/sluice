//go:build jsonbench

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package jsonbench

// msgpack candidates backed by github.com/vmihailenco/msgpack/v5 — the
// de-facto-standard standalone Go msgpack library.
//
// Two models (see serializers.go):
//
//   - Drop-in: encode the byte-for-byte SAME tagged-value envelope
//     sluice uses today (map[string]any with {"_t":..,"v":..}), via
//     msgpack instead of JSON, decoded through the SAME faithful
//     two-hop typed-decode contract sluice's decodeValue performs
//     (probe into map[string]RawMessage, branch on `_t`, typed
//     sub-decode of each payload). This proves int64 precision-safety
//     comes from the typed second hop, not the format.
//   - Native: drop the `_t`/`v` envelope entirely and let msgpack carry
//     int64 / uint64 / []byte (native bin) / bool / nil natively;
//     time + decimal stay strings (the value contract; NOT the msgpack
//     timestamp ext). Smaller + fewer allocs, but a format redesign.
//
// vmihailenco/v5 hazard the gate probes: by default its
// interface-decode widens / narrows integers (an int that fits a
// smaller width comes back as int8/int16/...; UseLooseInterfaceDecoding
// changes this again). The native model therefore does NOT decode into
// `any` — it mirrors what a realistic typed sluice decoder would do
// (decode each value into its typed Go target), which is the honest
// production-faithful path and exactly the methodology point the JSON
// harness already makes for the two-hop JSON decode.

import (
	"fmt"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

// vmihMarshal encodes v with vmihailenco msgpack at library defaults.
func vmihMarshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

// --- Drop-in: same tagged-value envelope, msgpack on the wire. ---

// vmihEncodeDropIn marshals the production envelope map verbatim.
func vmihEncodeDropIn(rec map[string]any) ([]byte, error) {
	return vmihMarshal(rec)
}

// vmihDecodeDropIn mirrors sluice's decodeValue two-hop contract using
// msgpack RawMessage as the deferred-payload type (the msgpack analogue
// of json.RawMessage): decode into map[string]RawMessage, probe `_t`,
// then run a SECOND typed Decode on each tagged payload. This is the
// load-bearing methodology point — the i64 precision-safety is the
// typed second hop, not the format.
func vmihDecodeDropIn(b []byte) (map[string]any, error) {
	return vmihDecodeEnvelope(b)
}

func vmihDecodeEnvelope(b []byte) (map[string]any, error) {
	var probe map[string]msgpack.RawMessage
	if err := msgpack.Unmarshal(b, &probe); err != nil {
		return nil, fmt.Errorf("vmih probe: %w", err)
	}
	out := make(map[string]any, len(probe))
	for k, raw := range probe {
		dv, err := vmihDecodeValue(raw)
		if err != nil {
			return nil, fmt.Errorf("vmih key %q: %w", k, err)
		}
		out[k] = dv
	}
	return out, nil
}

// vmihDecodeValue is the faithful mirror of decodeValue/decodeTaggedValue
// (backup_chunk.go) over a msgpack RawMessage payload: probe `_t`,
// typed sub-decode. Bare scalars decode to their value-contract type.
func vmihDecodeValue(raw msgpack.RawMessage) (any, error) {
	// Probe: is this a {"_t":..,"v":..} / {"_t":kind,..} object?
	var probe map[string]msgpack.RawMessage
	if err := msgpack.Unmarshal(raw, &probe); err == nil {
		if tagRaw, ok := probe["_t"]; ok {
			var tag string
			if msgpack.Unmarshal(tagRaw, &tag) == nil {
				if vRaw, hasV := probe["v"]; hasV {
					return vmihDecodeTagged(tag, vRaw)
				}
				if isChangeKind(tag) {
					return vmihDecodeChangeWrapper(tag, probe)
				}
			}
		}
		// Plain object — recurse each field.
		out := make(map[string]any, len(probe))
		for k, v := range probe {
			dv, err := vmihDecodeValue(v)
			if err != nil {
				return nil, err
			}
			out[k] = dv
		}
		return out, nil
	}
	// Not a map — a bare scalar. Try the value-contract types in order
	// that does NOT lose precision: int64 first (msgpack ints decode
	// exactly into int64), then bool, then string, then float64.
	var i int64
	if msgpack.Unmarshal(raw, &i) == nil {
		return i, nil
	}
	var bl bool
	if msgpack.Unmarshal(raw, &bl) == nil {
		return bl, nil
	}
	var s string
	if msgpack.Unmarshal(raw, &s) == nil {
		return s, nil
	}
	var f float64
	if msgpack.Unmarshal(raw, &f) == nil {
		return f, nil
	}
	// nil / unknown.
	return nil, nil
}

// vmihDecodeTagged mirrors decodeTaggedValue: each payload is decoded
// into its TYPED target (the hop that makes i64 precision-safe).
func vmihDecodeTagged(tag string, payload msgpack.RawMessage) (any, error) {
	switch tag {
	case "i64":
		var n int64
		if err := msgpack.Unmarshal(payload, &n); err != nil {
			return nil, fmt.Errorf("i64: %w", err)
		}
		return n, nil
	case "u64":
		var s string
		if err := msgpack.Unmarshal(payload, &s); err != nil {
			return nil, fmt.Errorf("u64: %w", err)
		}
		return parseU64(s)
	case "f64":
		var f float64
		if err := msgpack.Unmarshal(payload, &f); err != nil {
			return nil, fmt.Errorf("f64: %w", err)
		}
		return f, nil
	case "bytes":
		var s string
		if err := msgpack.Unmarshal(payload, &s); err != nil {
			return nil, fmt.Errorf("bytes: %w", err)
		}
		return b64Decode(s)
	case "time":
		var s string
		if err := msgpack.Unmarshal(payload, &s); err != nil {
			return nil, fmt.Errorf("time: %w", err)
		}
		return parseRFC3339Nano(s)
	case "map":
		var m map[string]msgpack.RawMessage
		if err := msgpack.Unmarshal(payload, &m); err != nil {
			return nil, fmt.Errorf("map: %w", err)
		}
		out := make(map[string]any, len(m))
		for k, v := range m {
			dv, err := vmihDecodeValue(v)
			if err != nil {
				return nil, err
			}
			out[k] = dv
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown tag %q", tag)
	}
}

func vmihDecodeChangeWrapper(kind string, probe map[string]msgpack.RawMessage) (any, error) {
	out := map[string]any{"_t": kind}
	for _, f := range []string{"schema", "table"} {
		if rv, ok := probe[f]; ok {
			var s string
			if err := msgpack.Unmarshal(rv, &s); err != nil {
				return nil, fmt.Errorf("change %s: %w", f, err)
			}
			out[f] = s
		}
	}
	if pv, ok := probe["position"]; ok {
		var pos any
		if err := msgpack.Unmarshal(pv, &pos); err != nil {
			return nil, fmt.Errorf("change position: %w", err)
		}
		out["position"] = pos
	}
	for _, rf := range []string{"row", "before", "after"} {
		rv, ok := probe[rf]
		if !ok {
			continue
		}
		var rm map[string]msgpack.RawMessage
		if err := msgpack.Unmarshal(rv, &rm); err != nil {
			return nil, fmt.Errorf("change %s: %w", rf, err)
		}
		dr := make(map[string]any, len(rm))
		for k, v := range rm {
			dv, err := vmihDecodeValue(v)
			if err != nil {
				return nil, err
			}
			dr[k] = dv
		}
		out[rf] = dr
	}
	return out, nil
}

// --- Native: no `_t`/`v` envelope; native int64/uint64/bin/bool/nil.
// time + decimal stay strings (value contract). ---

// vmihEncodeNative strips the tagged-value envelope and re-expresses
// the record as native msgpack types via toNative (shared with the
// hashicorp candidate so the on-wire model is identical across libs).
func vmihEncodeNative(rec map[string]any) ([]byte, error) {
	return vmihMarshal(toNative(rec))
}

// vmihDecodeNative decodes the native-model bytes into a typed shape.
// It does NOT decode into `any` (that is the precise vmihailenco
// integer-width hazard the gate exists to catch); it decodes into
// map[string]msgpack.RawMessage then resolves each value to its
// value-contract Go type via a typed second hop — the realistic
// production-faithful native decoder.
func vmihDecodeNative(b []byte) (map[string]any, error) {
	var probe map[string]msgpack.RawMessage
	if err := msgpack.Unmarshal(b, &probe); err != nil {
		return nil, fmt.Errorf("vmih native probe: %w", err)
	}
	out := make(map[string]any, len(probe))
	for k, raw := range probe {
		dv, err := vmihDecodeNativeValue(raw)
		if err != nil {
			return nil, fmt.Errorf("vmih native key %q: %w", k, err)
		}
		out[k] = dv
	}
	return out, nil
}

func vmihDecodeNativeValue(raw msgpack.RawMessage) (any, error) {
	// Discriminate on the msgpack WIRE TYPE byte, not by trial-decode:
	// vmihailenco lenient-decodes a msgpack str into a Go []byte (and
	// bin into string), so a probe-order strategy would misclassify
	// every decimal/time string as bytes. The wire byte is
	// unambiguous; a correct native decoder must key on it. This is the
	// native-model analogue of the JSON two-hop typed decode — the
	// methodology point the harness exists to make.
	switch {
	case msgpackIsNil(raw):
		return nil, nil
	case msgpackIsBin(raw):
		var by []byte
		if err := msgpack.Unmarshal(raw, &by); err != nil {
			return nil, fmt.Errorf("native bin: %w", err)
		}
		return by, nil
	case msgpackIsStr(raw):
		var s string
		if err := msgpack.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("native str: %w", err)
		}
		return s, nil
	case msgpackIsBool(raw):
		var bl bool
		if err := msgpack.Unmarshal(raw, &bl); err != nil {
			return nil, fmt.Errorf("native bool: %w", err)
		}
		return bl, nil
	case msgpackIsMap(raw):
		var nested map[string]msgpack.RawMessage
		if err := msgpack.Unmarshal(raw, &nested); err != nil {
			return nil, fmt.Errorf("native map: %w", err)
		}
		out := make(map[string]any, len(nested))
		for k, v := range nested {
			dv, err := vmihDecodeNativeValue(v)
			if err != nil {
				return nil, err
			}
			out[k] = dv
		}
		return out, nil
	}
	// Numeric: discriminate uint vs int ON THE WIRE. A uint64 >
	// MaxInt64 decoded into int64 silently overflows to negative with
	// no error (the native-int hazard); the wire type is unambiguous,
	// so route uint→uint64, int→int64. The value contract makes the
	// uint64-vs-int64 distinction at the column level; the native
	// decoder recovers it from the wire (a real native-model decoder
	// would consult the column's IR type — modelled here by the wire
	// type, which carries the same signal).
	switch {
	case msgpackIsUint(raw):
		var u uint64
		if err := msgpack.Unmarshal(raw, &u); err != nil {
			return nil, fmt.Errorf("native uint: %w", err)
		}
		return u, nil
	case msgpackIsInt(raw):
		var i int64
		if err := msgpack.Unmarshal(raw, &i); err != nil {
			return nil, fmt.Errorf("native int: %w", err)
		}
		return i, nil
	}
	var f float64
	if msgpack.Unmarshal(raw, &f) == nil {
		return f, nil
	}
	return nil, fmt.Errorf("native: undecodable value (wire 0x%02x)", firstByteOf(raw))
}

func firstByteOf(b []byte) byte {
	if len(b) == 0 {
		return 0
	}
	return b[0]
}
