//go:build jsonbench

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package jsonbench

// Shared native-model record transform + msgpack wire-type
// discrimination, used by both msgpack candidates so the
// msgpack-native ON-WIRE model is identical regardless of which
// library encodes it (the comparison is library-vs-library at a fixed
// model, not model-vs-model confounded with library).
//
// The native model is the "replace the default" redesign: it drops the
// {"_t":..,"v":..} tagging and the base64-for-bytes entirely and lets
// msgpack carry the value-contract types natively:
//
//   - int64   → msgpack native int (precision-exact; no float64
//     coercion, no string box — the 2^53 hazard simply does not exist)
//   - uint64  → msgpack native uint (no decimal-string box; values
//     above MaxInt64 survive exactly)
//   - []byte  → msgpack native bin (NO base64 — byte-exact, ~25%
//     smaller than base64 before compression)
//   - bool    → msgpack native bool
//   - nil     → msgpack nil (SQL NULL)
//   - string  → msgpack native str
//   - decimal → msgpack native str (value contract keeps decimals as
//     strings; unchanged)
//   - time    → msgpack native str, RFC3339Nano (value contract; NOT
//     the msgpack timestamp extension — see serializers.go)
//   - nested map → msgpack native map (recursive)
//
// This is NOT a drop-in. Changing the record model changes the on-disk
// format (a format-version bump), the decode contract (no `_t` probe;
// the decoder must know the value-contract type per column or rely on
// the msgpack wire type), and the operator-inspectability story (binary
// — `head file | jq` no longer works; see the report's tradeoff
// section). The harness measures it because it is the honest answer to
// "is replacing the JSON-Lines default worth it" — the drop-in number
// alone understates msgpack's ceiling.

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"
)

// toNative re-expresses a production tagged-value-envelope record as
// the native model: it UNWRAPS every {"_t":..,"v":..} envelope into a
// raw Go-native value the msgpack encoder serialises natively. This is
// the encode-side of the format redesign — it models exactly what a
// native-model chunk writer would hand the encoder.
func toNative(rec map[string]any) map[string]any {
	out := make(map[string]any, len(rec))
	for k, v := range rec {
		out[k] = toNativeValue(v)
	}
	return out
}

func toNativeValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case map[string]any:
		// Tagged envelope? Unwrap it to the native Go value.
		if tag, ok := x["_t"].(string); ok {
			if _, hasV := x["v"]; hasV {
				return unwrapTagged(tag, x["v"])
			}
			// CDC change wrapper: recurse the nested row maps, keep the
			// scalar fields, leave `_t` as the kind discriminator (the
			// native change model still needs the kind — it is data, not
			// a value-type tag).
			out := make(map[string]any, len(x))
			for kk, vv := range x {
				switch kk {
				case "row", "before", "after":
					if rm, ok := vv.(map[string]any); ok {
						out[kk] = toNative(rm)
						continue
					}
					out[kk] = toNativeValue(vv)
				default:
					out[kk] = vv
				}
			}
			return out
		}
		// Plain map — recurse.
		out := make(map[string]any, len(x))
		for kk, vv := range x {
			out[kk] = toNativeValue(vv)
		}
		return out
	default:
		return v
	}
}

// unwrapTagged converts one {"_t":tag,"v":payload} envelope to its
// native Go value. int64 stays int64 (msgpack encodes native int),
// u64 becomes a real uint64 (not a string box), bytes becomes raw
// []byte (msgpack native bin — no base64), time/decimal stay strings.
func unwrapTagged(tag string, payload any) any {
	switch tag {
	case "i64":
		switch n := payload.(type) {
		case int64:
			return n
		case int:
			return int64(n)
		case float64:
			return int64(n)
		}
		return payload
	case "u64":
		// Production boxes uint64 as a decimal string to dodge JSON's
		// 2^53. The native model has no such limit — store a real
		// uint64 so msgpack encodes it natively.
		if s, ok := payload.(string); ok {
			if u, err := strconv.ParseUint(s, 10, 64); err == nil {
				return u
			}
		}
		return payload
	case "f64":
		return payload
	case "bytes":
		// Production base64-encodes []byte for JSON. The native model
		// stores raw bytes (msgpack native bin) — decode the base64 the
		// corpus generated back to the raw bytes the writer would hold.
		if s, ok := payload.(string); ok {
			if b, err := base64.StdEncoding.DecodeString(s); err == nil {
				return b
			}
		}
		return payload
	case "time", "list_str":
		return payload
	case "map":
		if m, ok := payload.(map[string]any); ok {
			out := make(map[string]any, len(m))
			for k, v := range m {
				out[k] = toNativeValue(v)
			}
			return out
		}
		return payload
	case "list":
		if l, ok := payload.([]any); ok {
			out := make([]any, len(l))
			for i, e := range l {
				out[i] = toNativeValue(e)
			}
			return out
		}
		return payload
	default:
		return payload
	}
}

// msgpack first-byte families (spec
// https://github.com/msgpack/msgpack/blob/master/spec.md). The native
// decoder uses these to discriminate str from bin WITHOUT trial-decode
// — both vmihailenco and hashicorp will lenient-decode a msgpack str
// into a Go []byte (verified empirically), so a probe-[]byte-first
// strategy would misclassify every decimal/time string as bytes. The
// wire type byte is unambiguous; that is what a correct native decoder
// must key on.
func msgpackIsBin(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	switch b[0] {
	case 0xc4, 0xc5, 0xc6: // bin 8 / bin 16 / bin 32
		return true
	}
	return false
}

func msgpackIsStr(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	c := b[0]
	switch {
	case c >= 0xa0 && c <= 0xbf: // fixstr
		return true
	case c == 0xd9 || c == 0xda || c == 0xdb: // str 8 / 16 / 32
		return true
	}
	return false
}

func msgpackIsMap(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	c := b[0]
	switch {
	case c >= 0x80 && c <= 0x8f: // fixmap
		return true
	case c == 0xde || c == 0xdf: // map 16 / map 32
		return true
	}
	return false
}

// msgpackIsNil reports a msgpack nil. A zero-length deferred payload
// also counts: vmihailenco's RawMessage (and hashicorp's codec.Raw)
// for a msgpack nil decodes to an EMPTY slice, not the 0xc0 byte —
// verified empirically. A native decoder that only checked b[0]==0xc0
// would mis-handle every SQL NULL (the value contract's nil), so the
// empty-slice case is the realistic, correct nil discriminator.
func msgpackIsNil(b []byte) bool {
	if len(b) == 0 {
		return true
	}
	return b[0] == 0xc0
}

func msgpackIsBool(b []byte) bool {
	return len(b) > 0 && (b[0] == 0xc2 || b[0] == 0xc3)
}

// msgpackIsUint reports an UNSIGNED integer wire type: positive fixint
// (0x00–0x7f) or uint 8/16/32/64 (0xcc–0xcf). The native model's
// uint64 columns encode here. Discriminating uint from int on the WIRE
// is load-bearing: a uint64 > MaxInt64 decoded into an int64 target
// SILENTLY OVERFLOWS to a negative value with NO error (verified
// empirically — the exact native-int precision hazard). A correct
// native decoder MUST route a uint wire type to a uint64 target.
func msgpackIsUint(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	c := b[0]
	switch {
	case c <= 0x7f: // positive fixint
		return true
	case c >= 0xcc && c <= 0xcf: // uint 8 / 16 / 32 / 64
		return true
	}
	return false
}

// msgpackIsInt reports a SIGNED integer wire type: negative fixint
// (0xe0–0xff) or int 8/16/32/64 (0xd0–0xd3). Go int64 encodes here.
func msgpackIsInt(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	c := b[0]
	switch {
	case c >= 0xe0: // negative fixint
		return true
	case c >= 0xd0 && c <= 0xd3: // int 8 / 16 / 32 / 64
		return true
	}
	return false
}

// --- small shared decode helpers (keep the per-lib files terse) ---

func parseU64(s string) (uint64, error) {
	u, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("u64 parse %q: %w", s, err)
	}
	return u, nil
}

func b64Decode(s string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("bytes base64: %w", err)
	}
	return b, nil
}

func parseRFC3339Nano(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("time parse %q: %w", s, err)
	}
	return t, nil
}
