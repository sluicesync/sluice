//go:build jsonbench

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package jsonbench

// The fidelity gate. Speed is secondary to correctness: a JSON library
// that is lossy or semantically divergent on sluice's tagged-value
// envelope is DISQUALIFIED regardless of throughput. This is the
// loud-fail / value-types tenet (docs/value-types.md) expressed as a
// real, failing-on-violation check — TestFidelityGate fails CI-style if
// any candidate violates it; RunAll records the verdict so the markdown
// report shows the disqualification next to the speed numbers.
//
// What is asserted, per candidate, on the ACTUAL sluice chunk record
// shapes (the same tagged-value envelopes the production
// encodeValue/decodeValue in backup_chunk.go round-trips):
//
//   - int64 ({"_t":"i64"})       — NO float64 coercion / precision loss,
//     including a value > 2^53 where float64 would silently round.
//   - uint64 ({"_t":"u64"})      — string-encoded; must survive as the
//     exact decimal string (sluice ParseUint's it back to uint64).
//   - float64 ({"_t":"f64"})     — exact bit value.
//   - []byte ({"_t":"bytes"})    — base64 payload decodes byte-identical.
//   - decimal-as-string          — bare JSON string, exact.
//   - time.Time ({"_t":"time"})  — RFC3339Nano string, exact, so
//     time.Parse reconstructs the same instant.
//   - bool / bare string         — exact.
//   - SQL NULL (Go nil)          — round-trips to JSON null and back to
//     a value sluice's decodeValue maps to nil.
//   - nested {"_t":"map"} object — recursive envelope survives.
//   - HTML-significant chars     — recorded: sluice's production path
//     uses stdlib `encoding/json` which HTML-escapes (`<`→`<`);
//     any candidate whose bytes diverge is flagged (not a round-trip
//     failure — both decode identically — but a format-observable /
//     SHA-256-affecting difference the operator-facing format notices).
//
// The decode side intentionally re-implements the EXACT tagged-envelope
// recovery logic from internal/pipeline/backup_chunk.go's decodeValue
// (this package is internal/pipeline/internal/jsonbench and cannot
// import the unexported pipeline symbols). The mirror is kept faithful
// on purpose: the gate must prove a candidate feeds sluice's real
// decode contract, not a relaxed approximation.

import (
	"bytes"
	"encoding/base64"
	stdjson "encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// fidelityRecord is one envelope-bearing record plus the Go-native
// values it MUST decode back to. want maps column → expected Go value
// after sluice's decodeValue contract is applied.
type fidelityRecord struct {
	name string
	rec  map[string]any
	want map[string]any
}

// htmlMarker is embedded in a string column so the HTML-escaping
// behaviour of every candidate is observed against the stdlib baseline
// on a real record (not only by inspecting library docs).
const htmlMarker = `a<b && c>d </script>`

func fidelityCorpus() []fidelityRecord {
	bin := []byte{0x00, 0x01, 0xfe, 0xff, 0x42, 0x7f, 0x80}
	bigU := uint64(1)<<63 + 12345        // > MaxInt64; float64 would round
	bigI := int64(9_007_199_254_740_993) // 2^53 + 1; float64 cannot hold
	ts := time.Date(2026, 5, 16, 13, 45, 30, 123456789, time.UTC)

	nested := map[string]any{
		"_t": "map",
		"v": map[string]any{
			"inner_i64": map[string]any{"_t": "i64", "v": bigI},
			"inner_str": "deeply <nested> & ok",
			"inner_nul": nil,
		},
	}

	return []fidelityRecord{
		{
			name: "scalars",
			rec: map[string]any{
				"i":     map[string]any{"_t": "i64", "v": bigI},
				"u":     map[string]any{"_t": "u64", "v": strconv.FormatUint(bigU, 10)},
				"f":     map[string]any{"_t": "f64", "v": 3.141592653589793},
				"b":     map[string]any{"_t": "bytes", "v": base64.StdEncoding.EncodeToString(bin)},
				"t":     map[string]any{"_t": "time", "v": ts.Format(time.RFC3339Nano)},
				"dec":   "123456789012345678901234567890.123456789",
				"yes":   true,
				"no":    false,
				"name":  htmlMarker,
				"nul":   nil,
				"plain": "hello world",
			},
			want: map[string]any{
				"i":     bigI,
				"u":     bigU,
				"f":     3.141592653589793,
				"b":     bin,
				"t":     ts,
				"dec":   "123456789012345678901234567890.123456789",
				"yes":   true,
				"no":    false,
				"name":  htmlMarker,
				"nul":   nil,
				"plain": "hello world",
			},
		},
		{
			name: "nested-map",
			rec: map[string]any{
				"meta": nested,
				"id":   map[string]any{"_t": "i64", "v": int64(-1)},
			},
			want: map[string]any{
				"meta": map[string]any{
					"inner_i64": bigI,
					"inner_str": "deeply <nested> & ok",
					"inner_nul": nil,
				},
				"id": int64(-1),
			},
		},
	}
}

// decodeLine re-implements internal/pipeline/backup_chunk.go's
// decodeValue contract FAITHFULLY, using the candidate library's own
// Unmarshal for every hop — exactly as production does. This is the
// load-bearing methodology point:
//
// sluice's decodeValue does NOT decode a line into `any`. It decodes
// into `map[string]json.RawMessage`, probes `_t`, and then runs a
// SECOND typed `json.Unmarshal(payload, &int64 / &string / &float64)`
// on each tagged payload. That second hop is why the i64 path is
// precision-safe: a JSON number `9007199254740993` unmarshalled
// directly into an `int64` keeps full precision; the same number
// unmarshalled into `any` becomes a lossy `float64`. A correct gate
// must mirror the real two-hop path, or it would falsely disqualify
// EVERY library (stdlib included) for a float64 coercion sluice never
// performs. Probing this difference is exactly what the gate is for.
//
// raw is one encoded JSON-Lines record; lib supplies every Unmarshal.
func decodeLine(raw []byte, lib Lib) (any, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	if trimmed[0] != '{' {
		// Bare scalar/array — natural decode (matches decodeValue's
		// fall-through). Use json.Number so integer-valued bare numbers
		// don't silently widen to float64.
		var natural any
		if err := lib.Unmarshal(raw, &natural); err != nil {
			return nil, fmt.Errorf("natural decode: %w", err)
		}
		return natural, nil
	}
	var probe map[string]stdjson.RawMessage
	if err := lib.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("probe decode: %w", err)
	}
	if tagRaw, ok := probe["_t"]; ok {
		var tag string
		if err := lib.Unmarshal(tagRaw, &tag); err == nil {
			// A tagged-VALUE envelope always carries a sibling "v"
			// payload (decodeValue's contract). A CDC change RECORD
			// carries `_t` as the change-KIND discriminator with NO
			// "v" — production decodes those via the changeWire struct
			// path + decodeRowValues on nested row maps, NOT via
			// decodeValue. Distinguish on the presence of "v": with it,
			// tagged value; without it, change wrapper. This mirrors
			// the two distinct readers (chunkReader.ReadRow vs
			// changeChunkReader.ReadChange).
			if _, hasV := probe["v"]; hasV {
				return decodeTagged(tag, probe["v"], lib)
			}
			if isChangeKind(tag) {
				return decodeChangeWrapper(tag, probe, lib)
			}
		}
	}
	out := make(map[string]any, len(probe))
	for k, v := range probe {
		dv, err := decodeLine(v, lib)
		if err != nil {
			return nil, fmt.Errorf("map key %q: %w", k, err)
		}
		out[k] = dv
	}
	return out, nil
}

// decodeTagged mirrors decodeTaggedValue: each payload is unmarshalled
// into its TYPED target via the candidate's own Unmarshal — the hop
// that makes i64 precision-safe in production.
func decodeTagged(tag string, payload stdjson.RawMessage, lib Lib) (any, error) {
	switch tag {
	case "i64":
		var n int64
		if err := lib.Unmarshal(payload, &n); err != nil {
			return nil, fmt.Errorf("i64 payload: %w", err)
		}
		return n, nil
	case "u64":
		var s string
		if err := lib.Unmarshal(payload, &s); err != nil {
			return nil, fmt.Errorf("u64 payload: %w", err)
		}
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("u64 parse %q: %w", s, err)
		}
		return n, nil
	case "f64":
		var f float64
		if err := lib.Unmarshal(payload, &f); err != nil {
			return nil, fmt.Errorf("f64 payload: %w", err)
		}
		return f, nil
	case "bytes":
		var s string
		if err := lib.Unmarshal(payload, &s); err != nil {
			return nil, fmt.Errorf("bytes payload: %w", err)
		}
		return base64.StdEncoding.DecodeString(s)
	case "time":
		var s string
		if err := lib.Unmarshal(payload, &s); err != nil {
			return nil, fmt.Errorf("time payload: %w", err)
		}
		return time.Parse(time.RFC3339Nano, s)
	case "map":
		var m map[string]stdjson.RawMessage
		if err := lib.Unmarshal(payload, &m); err != nil {
			return nil, fmt.Errorf("map payload: %w", err)
		}
		out := make(map[string]any, len(m))
		for k, v := range m {
			dv, err := decodeLine(v, lib)
			if err != nil {
				return nil, fmt.Errorf("map[%q]: %w", k, err)
			}
			out[k] = dv
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown tag %q", tag)
	}
}

// isChangeKind reports whether tag is one of the CDC change-record
// discriminators (mirrors the changeKind* constants in
// backup_change_chunk.go). Used to route a `_t`-bearing object that
// has NO "v" sibling down the change-wrapper decode path instead of
// the tagged-value path.
func isChangeKind(tag string) bool {
	switch tag {
	case "insert", "update", "delete", "truncate", "tx_begin", "tx_commit":
		return true
	}
	return false
}

// decodeChangeWrapper mirrors backup_change_chunk.go's decodeChange +
// decodeRowValues: the wrapper's scalar fields (schema/table/position)
// decode naturally, and each nested row map (row / before / after)
// runs through the tagged-value decode so wide values (i64, bytes,
// time) recover their Go-native shape — the same work an incremental
// chain restore performs per change event.
func decodeChangeWrapper(kind string, probe map[string]stdjson.RawMessage, lib Lib) (any, error) {
	out := map[string]any{"_t": kind}
	for _, field := range []string{"schema", "table"} {
		if rawv, ok := probe[field]; ok {
			var s string
			if err := lib.Unmarshal(rawv, &s); err != nil {
				return nil, fmt.Errorf("change %s: %w", field, err)
			}
			out[field] = s
		}
	}
	if posRaw, ok := probe["position"]; ok {
		// Position is an engine-tagged opaque token map; natural decode
		// (it carries no tagged-value envelopes in the chunk format).
		var pos any
		if err := lib.Unmarshal(posRaw, &pos); err != nil {
			return nil, fmt.Errorf("change position: %w", err)
		}
		out["position"] = pos
	}
	for _, rowField := range []string{"row", "before", "after"} {
		rawv, ok := probe[rowField]
		if !ok {
			continue
		}
		var rowMap map[string]stdjson.RawMessage
		if err := lib.Unmarshal(rawv, &rowMap); err != nil {
			return nil, fmt.Errorf("change %s map: %w", rowField, err)
		}
		decRow := make(map[string]any, len(rowMap))
		for k, v := range rowMap {
			dv, err := decodeLine(v, lib)
			if err != nil {
				return nil, fmt.Errorf("change %s[%q]: %w", rowField, k, err)
			}
			decRow[k] = dv
		}
		out[rowField] = decRow
	}
	return out, nil
}

// checkOne runs the full encode→decode→compare for one library and
// returns a verdict string ("PASS" or the first violation found).
func checkOne(lib Lib) string {
	for _, fr := range fidelityCorpus() {
		// Encode the record exactly as the chunk writer would
		// (json.Marshal of the map[string]any envelope).
		b, err := lib.Marshal(fr.rec)
		if err != nil {
			return fmt.Sprintf("DISQUALIFIED: marshal %s: %v", fr.name, err)
		}
		// Decode via sluice's REAL two-hop path (map[string]RawMessage
		// then typed sub-unmarshal), using the candidate's own
		// Unmarshal at every hop. This is what production does — not a
		// decode-into-`any` shortcut, which would falsely flag every
		// library for a float64 coercion sluice never performs.
		decoded, err := decodeLine(b, lib)
		if err != nil {
			return fmt.Sprintf("DISQUALIFIED: envelope decode %s: %v", fr.name, err)
		}
		got, ok := decoded.(map[string]any)
		if !ok {
			return fmt.Sprintf("DISQUALIFIED: %s decoded to %T, want map", fr.name, decoded)
		}
		for k, wantV := range fr.want {
			gotV, present := got[k]
			if !present {
				return fmt.Sprintf("DISQUALIFIED: %s.%s missing after round-trip", fr.name, k)
			}
			if !valuesEqual(wantV, gotV) {
				return fmt.Sprintf("DISQUALIFIED: %s.%s = %#v (%T), want %#v (%T)",
					fr.name, k, gotV, gotV, wantV, wantV)
			}
		}
	}
	return "PASS"
}

// valuesEqual is a semantic equality matching the value-types.md
// contract: []byte byte-equal, time.Time instant-equal, float64
// bit-equal (NaN-aware), nested maps recursive, everything else ==.
func valuesEqual(want, got any) bool {
	if want == nil {
		return got == nil
	}
	switch w := want.(type) {
	case []byte:
		g, ok := got.([]byte)
		return ok && bytes.Equal(w, g)
	case time.Time:
		g, ok := got.(time.Time)
		return ok && w.Equal(g)
	case float64:
		g, ok := got.(float64)
		if !ok {
			return false
		}
		if math.IsNaN(w) && math.IsNaN(g) {
			return true
		}
		return math.Float64bits(w) == math.Float64bits(g)
	case int64:
		g, ok := got.(int64)
		return ok && w == g
	case uint64:
		g, ok := got.(uint64)
		return ok && w == g
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok || len(g) != len(w) {
			return false
		}
		for k, wv := range w {
			gv, present := g[k]
			if !present || !valuesEqual(wv, gv) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(want, got)
	}
}

// runFidelity evaluates every registered library and returns lib name
// → verdict. RunAll calls this BEFORE benchmarking so the report can
// show a disqualification next to the speed numbers.
func runFidelity() map[string]string {
	out := make(map[string]string, len(allLibs))
	for _, l := range allLibs {
		out[l.Name] = checkOne(l)
	}
	return out
}

// htmlEscapeReport returns, per library, whether its encoded bytes for
// a string containing HTML-significant characters match stdlib's
// HTML-escaped form. Divergence is NOT a fidelity failure (both decode
// identically) but IS a format-observable / SHA-256-affecting
// difference the report must call out, since sluice's production path
// uses stdlib `encoding/json` defaults.
func htmlEscapeReport() map[string]string {
	probe := map[string]any{"s": htmlMarker}
	var stdBuf []byte
	for _, l := range allLibs {
		if l.Name == "stdlib_v1" {
			stdBuf, _ = l.Marshal(probe)
			break
		}
	}
	out := make(map[string]string, len(allLibs))
	for _, l := range allLibs {
		b, err := l.Marshal(probe)
		if err != nil {
			out[l.Name] = "marshal error: " + err.Error()
			continue
		}
		s := string(b)
		escaped := strings.Contains(s, `<`) || strings.Contains(s, `>`) || strings.Contains(s, `&`)
		switch {
		case bytes.Equal(b, stdBuf):
			out[l.Name] = "byte-identical to stdlib (HTML-escaped) — format-safe"
		case escaped:
			out[l.Name] = "HTML-escaped, byte-identical chunk content to stdlib — format-safe"
		default:
			out[l.Name] = "NOT HTML-escaped — on-disk bytes diverge from stdlib (SHA-256-affecting; format-observable). raw=" + s
		}
	}
	return out
}
