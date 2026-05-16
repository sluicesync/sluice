//go:build jsonbench

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package jsonbench

// msgpack candidates backed by github.com/hashicorp/go-msgpack/v2/codec
// — the ugorji/codec-based library battle-tested in Consul / Nomad /
// Raft (the task brief's reference implementation).
//
// Same two models as the vmihailenco candidate (drop-in: byte-for-byte
// same tagged-value envelope via msgpack + faithful two-hop typed
// decode; native: redesigned model with native int64/uint64/bin). The
// MsgpackHandle is configured WriteExt=true so the new-spec str/bin
// distinction is honored — without it []byte and string collapse to
// one wire type and the native bin advantage (and the str/bin fidelity
// distinction) would be unmeasurable. Canonical=true makes map-key
// order deterministic so the size + any SHA comparison is stable
// across runs (the JSON baseline's stdlib marshal emits sorted keys).
//
// hashicorp interface-decode hazard the gate proves: decoding a
// msgpack str OR bin into `interface{}` both yield Go []uint8
// (verified empirically) — i.e. a decode-into-any path would turn
// every string column into []byte, a value-contract violation. The
// typed two-hop / wire-discriminated decode used here is the realistic
// production-faithful path and avoids it; a naive decoder would be
// DISQUALIFIED. This is the same methodology point the JSON harness
// makes for the JSON two-hop decode.

import (
	"bytes"
	"fmt"

	hcodec "github.com/hashicorp/go-msgpack/v2/codec"
)

// hcHandle is the shared codec handle. WriteExt enables the new-spec
// bin type (so []byte is a real msgpack bin, distinct from str);
// Canonical makes map output deterministic.
var hcHandle = func() *hcodec.MsgpackHandle {
	h := &hcodec.MsgpackHandle{}
	h.WriteExt = true
	h.Canonical = true
	return h
}()

func hcMarshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := hcodec.NewEncoder(&buf, hcHandle).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func hcUnmarshal(b []byte, v any) error {
	return hcodec.NewDecoderBytes(b, hcHandle).Decode(v)
}

// --- Drop-in: same tagged-value envelope, msgpack on the wire. ---

func hcEncodeDropIn(rec map[string]any) ([]byte, error) {
	return hcMarshal(rec)
}

func hcDecodeDropIn(b []byte) (map[string]any, error) {
	var probe map[string]hcodec.Raw
	if err := hcUnmarshal(b, &probe); err != nil {
		return nil, fmt.Errorf("hc probe: %w", err)
	}
	out := make(map[string]any, len(probe))
	for k, raw := range probe {
		dv, err := hcDecodeValue(raw)
		if err != nil {
			return nil, fmt.Errorf("hc key %q: %w", k, err)
		}
		out[k] = dv
	}
	return out, nil
}

// hcDecodeValue mirrors decodeValue/decodeTaggedValue over a codec.Raw
// payload: probe `_t`, typed sub-decode (the precision-safe hop).
func hcDecodeValue(raw hcodec.Raw) (any, error) {
	rb := []byte(raw)
	if msgpackIsMap(rb) {
		var probe map[string]hcodec.Raw
		if err := hcUnmarshal(rb, &probe); err != nil {
			return nil, fmt.Errorf("hc map probe: %w", err)
		}
		if tagRaw, ok := probe["_t"]; ok {
			var tag string
			if hcUnmarshal([]byte(tagRaw), &tag) == nil {
				if vRaw, hasV := probe["v"]; hasV {
					return hcDecodeTagged(tag, vRaw)
				}
				if isChangeKind(tag) {
					return hcDecodeChangeWrapper(tag, probe)
				}
			}
		}
		out := make(map[string]any, len(probe))
		for k, v := range probe {
			dv, err := hcDecodeValue(v)
			if err != nil {
				return nil, err
			}
			out[k] = dv
		}
		return out, nil
	}
	// Bare scalar — discriminate on wire type (str must NOT collapse
	// to []byte, the hashicorp interface-decode hazard).
	switch {
	case msgpackIsNil(rb):
		return nil, nil
	case msgpackIsBool(rb):
		var bl bool
		if err := hcUnmarshal(rb, &bl); err != nil {
			return nil, err
		}
		return bl, nil
	case msgpackIsStr(rb):
		var s string
		if err := hcUnmarshal(rb, &s); err != nil {
			return nil, err
		}
		return s, nil
	case msgpackIsBin(rb):
		var by []byte
		if err := hcUnmarshal(rb, &by); err != nil {
			return nil, err
		}
		return by, nil
	}
	var i int64
	if hcUnmarshal(rb, &i) == nil {
		return i, nil
	}
	var f float64
	if hcUnmarshal(rb, &f) == nil {
		return f, nil
	}
	return nil, nil
}

func hcDecodeTagged(tag string, payload hcodec.Raw) (any, error) {
	pb := []byte(payload)
	switch tag {
	case "i64":
		var n int64
		if err := hcUnmarshal(pb, &n); err != nil {
			return nil, fmt.Errorf("i64: %w", err)
		}
		return n, nil
	case "u64":
		var s string
		if err := hcUnmarshal(pb, &s); err != nil {
			return nil, fmt.Errorf("u64: %w", err)
		}
		return parseU64(s)
	case "f64":
		var f float64
		if err := hcUnmarshal(pb, &f); err != nil {
			return nil, fmt.Errorf("f64: %w", err)
		}
		return f, nil
	case "bytes":
		var s string
		if err := hcUnmarshal(pb, &s); err != nil {
			return nil, fmt.Errorf("bytes: %w", err)
		}
		return b64Decode(s)
	case "time":
		var s string
		if err := hcUnmarshal(pb, &s); err != nil {
			return nil, fmt.Errorf("time: %w", err)
		}
		return parseRFC3339Nano(s)
	case "map":
		var m map[string]hcodec.Raw
		if err := hcUnmarshal(pb, &m); err != nil {
			return nil, fmt.Errorf("map: %w", err)
		}
		out := make(map[string]any, len(m))
		for k, v := range m {
			dv, err := hcDecodeValue(v)
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

func hcDecodeChangeWrapper(kind string, probe map[string]hcodec.Raw) (any, error) {
	out := map[string]any{"_t": kind}
	for _, f := range []string{"schema", "table"} {
		if rv, ok := probe[f]; ok {
			var s string
			if err := hcUnmarshal([]byte(rv), &s); err != nil {
				return nil, fmt.Errorf("change %s: %w", f, err)
			}
			out[f] = s
		}
	}
	if pv, ok := probe["position"]; ok {
		var pos any
		if err := hcUnmarshal([]byte(pv), &pos); err != nil {
			return nil, fmt.Errorf("change position: %w", err)
		}
		out["position"] = pos
	}
	for _, rf := range []string{"row", "before", "after"} {
		rv, ok := probe[rf]
		if !ok {
			continue
		}
		var rm map[string]hcodec.Raw
		if err := hcUnmarshal([]byte(rv), &rm); err != nil {
			return nil, fmt.Errorf("change %s: %w", rf, err)
		}
		dr := make(map[string]any, len(rm))
		for k, v := range rm {
			dv, err := hcDecodeValue(v)
			if err != nil {
				return nil, err
			}
			dr[k] = dv
		}
		out[rf] = dr
	}
	return out, nil
}

// --- Native: no `_t`/`v`; native int64/uint64/bin/bool/nil. ---

func hcEncodeNative(rec map[string]any) ([]byte, error) {
	return hcMarshal(toNative(rec))
}

func hcDecodeNative(b []byte) (map[string]any, error) {
	var probe map[string]hcodec.Raw
	if err := hcUnmarshal(b, &probe); err != nil {
		return nil, fmt.Errorf("hc native probe: %w", err)
	}
	out := make(map[string]any, len(probe))
	for k, raw := range probe {
		dv, err := hcDecodeNativeValue(raw)
		if err != nil {
			return nil, fmt.Errorf("hc native key %q: %w", k, err)
		}
		out[k] = dv
	}
	return out, nil
}

func hcDecodeNativeValue(raw hcodec.Raw) (any, error) {
	rb := []byte(raw)
	switch {
	case msgpackIsNil(rb):
		return nil, nil
	case msgpackIsBin(rb):
		var by []byte
		if err := hcUnmarshal(rb, &by); err != nil {
			return nil, fmt.Errorf("native bin: %w", err)
		}
		return by, nil
	case msgpackIsStr(rb):
		var s string
		if err := hcUnmarshal(rb, &s); err != nil {
			return nil, fmt.Errorf("native str: %w", err)
		}
		return s, nil
	case msgpackIsBool(rb):
		var bl bool
		if err := hcUnmarshal(rb, &bl); err != nil {
			return nil, fmt.Errorf("native bool: %w", err)
		}
		return bl, nil
	case msgpackIsMap(rb):
		var nested map[string]hcodec.Raw
		if err := hcUnmarshal(rb, &nested); err != nil {
			return nil, fmt.Errorf("native map: %w", err)
		}
		out := make(map[string]any, len(nested))
		for k, v := range nested {
			dv, err := hcDecodeNativeValue(v)
			if err != nil {
				return nil, err
			}
			out[k] = dv
		}
		return out, nil
	}
	switch {
	case msgpackIsUint(rb):
		var u uint64
		if err := hcUnmarshal(rb, &u); err != nil {
			return nil, fmt.Errorf("native uint: %w", err)
		}
		return u, nil
	case msgpackIsInt(rb):
		var i int64
		if err := hcUnmarshal(rb, &i); err != nil {
			return nil, fmt.Errorf("native int: %w", err)
		}
		return i, nil
	}
	var f float64
	if hcUnmarshal(rb, &f) == nil {
		return f, nil
	}
	return nil, fmt.Errorf("native: undecodable (wire 0x%02x)", firstByteOf(rb))
}

// msgpackSerializers returns every msgpack candidate (both libraries ×
// both models). Wired into allSerializers().
func msgpackSerializers() []Serializer {
	return []Serializer{
		{
			Name:             "msgpack_vmihailenco_dropin",
			Surface:          "vmihailenco/msgpack v5 — same envelope, two-hop typed decode",
			Model:            modelDropIn,
			HumanInspectable: false,
			EncodeRecord:     vmihEncodeDropIn,
			DecodeRecord:     vmihDecodeDropIn,
		},
		{
			Name:             "msgpack_vmihailenco_native",
			Surface:          "vmihailenco/msgpack v5 — native int/uint/bin, wire-discriminated decode",
			Model:            modelNative,
			HumanInspectable: false,
			EncodeRecord:     vmihEncodeNative,
			DecodeRecord:     vmihDecodeNative,
		},
		{
			Name:             "msgpack_hashicorp_dropin",
			Surface:          "hashicorp/go-msgpack v2 — same envelope, two-hop typed decode",
			Model:            modelDropIn,
			HumanInspectable: false,
			EncodeRecord:     hcEncodeDropIn,
			DecodeRecord:     hcDecodeDropIn,
		},
		{
			Name:             "msgpack_hashicorp_native",
			Surface:          "hashicorp/go-msgpack v2 — native int/uint/bin, wire-discriminated decode",
			Model:            modelNative,
			HumanInspectable: false,
			EncodeRecord:     hcEncodeNative,
			DecodeRecord:     hcDecodeNative,
		},
	}
}
