// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Fast row codec for the Phase 1 chunk format — the per-row hot path
// of `backup full` (encode) and `restore` (decode).
//
// Why this exists (tasks #51/#52, profiled 2026-06-11 on the 136 GB
// bench corpus): the legacy path builds a fresh `map[string]any` per
// row plus a two-key envelope map per non-native value, then hands the
// lot to reflection-based `encoding/json` — 49% of backup CPU
// (`json.Marshal` 40% + `encodeValue` 9%) and 69% of restore CPU
// (nested `json.Unmarshal` per envelope re-runs `checkValid` on every
// payload). This file replaces the hot path with direct buffer
// append/parse over the SAME wire format.
//
// Correctness contract (the load-bearing part):
//
//   - The fast encoder emits bytes IDENTICAL to the legacy
//     `json.Marshal(map)` output — same sorted-key order, same
//     stdlib string escaping (HTML escapes on), same stdlib float
//     formatting — or it reports !ok and the caller falls back to the
//     legacy marshal. No third behavior.
//   - The fast decoder either produces EXACTLY the row the legacy
//     `json.Unmarshal`+`decodeValue` path would produce, or it bails
//     (!ok) and the caller re-decodes the line via the legacy path.
//     Every anomaly — escapes it doesn't model, alien envelope
//     shapes, grammar violations, out-of-range numbers — bails. The
//     legacy path therefore remains the single semantic + error
//     oracle; the fast path is a pure shortcut, never a fork.
//
// Both directions are pinned by differential tests/fuzzers in
// backup_chunk_fast_test.go (fast vs legacy on generated rows and on
// arbitrary line bytes). The wire format itself is unchanged — no
// chunk-format version bump; old binaries read new chunks and vice
// versa, byte-for-byte.

import (
	"encoding/base64"
	"math"
	"slices"
	"strconv"
	"time"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"sluicesync.dev/sluice/internal/ir"
)

// maxFastDepth bounds fast-codec recursion on nested lists/maps. Rows
// deeper than this bail to the legacy path (stdlib's own nesting limit
// is far larger; this is a stack-safety bound, not a format rule).
const maxFastDepth = 64

// ============================================================
// Encode side
// ============================================================

// appendRowJSON appends one JSON record for row — byte-identical to
// `json.Marshal` of the map the legacy WriteRow builds — using
// sortedNames (column names in ascending bytewise order, i.e. stdlib
// map-key order). On !ok the caller must discard the appended bytes
// and fall back to the legacy marshal; the partial dst is unusable.
func appendRowJSON(dst []byte, row ir.Row, sortedNames []string) ([]byte, bool) {
	dst = append(dst, '{')
	for i, name := range sortedNames {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendJSONString(dst, name)
		dst = append(dst, ':')
		v, present := row[name]
		if !present || v == nil {
			dst = append(dst, "null"...)
			continue
		}
		var ok bool
		dst, ok = appendEncodedValue(dst, v, 0)
		if !ok {
			return dst, false
		}
	}
	return append(dst, '}'), true
}

// appendEncodedValue appends the wire form of one value — exactly the
// bytes `json.Marshal(encodeValue(v))` would produce. Types outside
// the value contract's known set (and non-finite floats, which stdlib
// rejects) report !ok so the caller can fall back to the legacy
// marshal for identical error behavior.
func appendEncodedValue(dst []byte, v any, depth int) ([]byte, bool) {
	if depth > maxFastDepth {
		return dst, false
	}
	switch x := v.(type) {
	case nil:
		return append(dst, "null"...), true
	case string:
		return appendJSONString(dst, x), true
	case bool:
		return strconv.AppendBool(dst, x), true
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return appendF64sEnvelope(dst, x), true
		}
		return appendJSONFloat(dst, x, 64)
	case float32:
		if f := float64(x); math.IsNaN(f) || math.IsInf(f, 0) {
			return appendF64sEnvelope(dst, f), true
		}
		return appendJSONFloat(dst, float64(x), 32)
	case []byte:
		dst = append(dst, `{"_t":"bytes","v":"`...)
		n := base64.StdEncoding.EncodedLen(len(x))
		dst = slices.Grow(dst, n)
		e := len(dst)
		dst = dst[:e+n]
		base64.StdEncoding.Encode(dst[e:], x)
		return append(dst, `"}`...), true
	case time.Time:
		dst = append(dst, `{"_t":"time","v":"`...)
		dst = x.UTC().AppendFormat(dst, time.RFC3339Nano)
		return append(dst, `"}`...), true
	case int:
		return appendI64Envelope(dst, int64(x)), true
	case int8:
		return appendI64Envelope(dst, int64(x)), true
	case int16:
		return appendI64Envelope(dst, int64(x)), true
	case int32:
		return appendI64Envelope(dst, int64(x)), true
	case int64:
		return appendI64Envelope(dst, x), true
	case uint:
		return appendU64Envelope(dst, uint64(x)), true
	case uint8:
		return appendU64Envelope(dst, uint64(x)), true
	case uint16:
		return appendU64Envelope(dst, uint64(x)), true
	case uint32:
		return appendU64Envelope(dst, uint64(x)), true
	case uint64:
		return appendU64Envelope(dst, x), true
	case []any:
		dst = append(dst, `{"_t":"list","v":[`...)
		for i, e := range x {
			if i > 0 {
				dst = append(dst, ',')
			}
			var ok bool
			dst, ok = appendEncodedValue(dst, e, depth+1)
			if !ok {
				return dst, false
			}
		}
		return append(dst, `]}`...), true
	case []string:
		if x == nil {
			// Legacy marshals the nil slice itself, producing
			// `"v":null` — not `"v":[]`. Bail so legacy stays the
			// single source of those bytes (fidelity-review finding).
			return dst, false
		}
		dst = append(dst, `{"_t":"list_str","v":[`...)
		for i, s := range x {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendJSONString(dst, s)
		}
		return append(dst, `]}`...), true
	case map[string]any:
		dst = append(dst, `{"_t":"map","v":{`...)
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for i, k := range keys {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendJSONString(dst, k)
			dst = append(dst, ':')
			var ok bool
			dst, ok = appendEncodedValue(dst, x[k], depth+1)
			if !ok {
				return dst, false
			}
		}
		return append(dst, `}}`...), true
	}
	// Unknown type: the legacy path marshals it raw (or errors) —
	// let it.
	return dst, false
}

func appendI64Envelope(dst []byte, n int64) []byte {
	dst = append(dst, `{"_t":"i64","v":`...)
	dst = strconv.AppendInt(dst, n, 10)
	return append(dst, '}')
}

func appendU64Envelope(dst []byte, u uint64) []byte {
	dst = append(dst, `{"_t":"u64","v":"`...)
	dst = strconv.AppendUint(dst, u, 10)
	return append(dst, `"}`...)
}

// appendF64sEnvelope emits the non-finite float sentinel envelope
// (Bug 138) — byte-identical to the legacy map marshal.
func appendF64sEnvelope(dst []byte, f float64) []byte {
	dst = append(dst, `{"_t":"f64s","v":"`...)
	dst = append(dst, nonFiniteString(f)...)
	return append(dst, `"}`...)
}

// appendJSONFloat mirrors stdlib encoding/json's float encoder: 'f'
// format except very small/large magnitudes which use 'e' with the
// two-digit exponent's leading zero trimmed. Non-finite values report
// !ok (stdlib rejects them; the legacy fallback owns that error).
func appendJSONFloat(dst []byte, f float64, bits int) ([]byte, bool) {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return dst, false
	}
	abs := math.Abs(f)
	format := byte('f')
	if abs != 0 {
		if bits == 64 && (abs < 1e-6 || abs >= 1e21) ||
			bits == 32 && (float32(abs) < 1e-6 || float32(abs) >= 1e21) {
			format = 'e'
		}
	}
	dst = strconv.AppendFloat(dst, f, format, -1, bits)
	if format == 'e' {
		// Trim "e-09" to "e-9", exactly as stdlib does.
		if n := len(dst); n >= 4 && dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
			dst[n-2] = dst[n-1]
			dst = dst[:n-1]
		}
	}
	return dst, true
}

// jsonHTMLSafeSet mirrors stdlib encoding/json's htmlSafeSet: ASCII
// bytes that need no escaping when HTML escaping is on (the stdlib
// default the legacy path uses).
var jsonHTMLSafeSet = [utf8.RuneSelf]bool{}

func init() {
	for b := 0x20; b < utf8.RuneSelf; b++ {
		switch b {
		case '"', '\\', '<', '>', '&':
			continue
		}
		jsonHTMLSafeSet[b] = true
	}
}

const hexDigits = "0123456789abcdef"

// appendJSONString appends s as a JSON string with stdlib
// encoding/json's exact escaping (HTML escapes on, short escapes for
// \b \f \n \r \t, \u00XX for other control bytes, U+2028/U+2029
// escaped, invalid UTF-8 replaced with �).
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if jsonHTMLSafeSet[b] {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			switch b {
			case '\\', '"':
				dst = append(dst, '\\', b)
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[b>>4], hexDigits[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			// stdlib emits the U+FFFD ESCAPE here, not the raw
			// replacement-character bytes.
			dst = append(dst, '\\', 'u', 'f', 'f', 'f', 'd')
			i += size
			start = i
			continue
		}
		if c == ' ' || c == ' ' {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', hexDigits[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

// ============================================================
// Decode side
// ============================================================

// fastRowDecoder is the per-chunkReader fast decode state: a bounded
// cache canonicalizing repeated JSON object keys (column names repeat
// every row; the cache turns the per-row key allocation into a no-op
// map lookup — `m[string(b)]` does not allocate).
type fastRowDecoder struct {
	keyCache map[string]string
}

// maxKeyCacheEntries bounds keyCache so alien chunks with unbounded
// distinct keys (e.g. JSON columns holding row-unique object keys)
// can't grow it without limit. Past the cap, keys are allocated
// per-occurrence — correct, just not cached.
const maxKeyCacheEntries = 4096

func (d *fastRowDecoder) key(b []byte) string {
	if s, ok := d.keyCache[string(b)]; ok {
		return s
	}
	s := string(b)
	if d.keyCache == nil {
		d.keyCache = make(map[string]string, 64)
	}
	if len(d.keyCache) < maxKeyCacheEntries {
		d.keyCache[s] = s
	}
	return s
}

// decodeRow attempts the fast single-pass decode of one JSON-Lines
// row. ok=false means "fall back to the legacy path" — the line is
// either invalid (legacy owns the error) or uses a shape the fast
// path doesn't model (legacy owns the semantics). It never returns a
// row that differs from the legacy decode of the same bytes.
func (d *fastRowDecoder) decodeRow(line []byte) (ir.Row, bool) {
	p := &fastParser{in: line, dec: d}
	p.skipWS()
	if !p.consume('{') {
		return nil, false
	}
	row := make(ir.Row, 16)
	p.skipWS()
	if p.consume('}') {
		p.skipWS()
		if p.pos != len(p.in) {
			return nil, false
		}
		return row, true
	}
	for {
		p.skipWS()
		kb, ok := p.parseStringBytes()
		if !ok {
			return nil, false
		}
		k := d.key(kb)
		p.skipWS()
		if !p.consume(':') {
			return nil, false
		}
		v, ok := p.parseValue(0)
		if !ok {
			return nil, false
		}
		row[k] = v
		p.skipWS()
		if p.consume(',') {
			continue
		}
		if p.consume('}') {
			break
		}
		return nil, false
	}
	p.skipWS()
	if p.pos != len(p.in) {
		return nil, false
	}
	return row, true
}

// fastParser is a strict, allocation-light JSON parser specialized to
// the chunk wire format. It is deliberately conservative: any input
// it is not POSITIVE the legacy path decodes identically makes it
// bail, so its accept-set is a strict subset of valid JSON.
type fastParser struct {
	in  []byte
	pos int
	dec *fastRowDecoder

	// scratch backs parseStringBytes results that need unescaping;
	// reused across calls within one row. Callers must copy before
	// the next parseStringBytes call (d.key and string(...) both do).
	scratch []byte
}

func (p *fastParser) skipWS() {
	for p.pos < len(p.in) {
		switch p.in[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *fastParser) consume(c byte) bool {
	if p.pos < len(p.in) && p.in[p.pos] == c {
		p.pos++
		return true
	}
	return false
}

// parseValue decodes one value with the legacy path's envelope
// semantics: tagged envelopes unwrap to native Go shapes, other
// objects decode as maps (unless a late "_t" key appears — legacy
// would have treated that as an envelope, so bail), scalars decode
// naturally. Bare arrays bail: the legacy path decodes those
// envelope-blind (natural decode), a semantic this fast path doesn't
// model.
func (p *fastParser) parseValue(depth int) (any, bool) {
	if depth > maxFastDepth {
		return nil, false
	}
	p.skipWS()
	if p.pos >= len(p.in) {
		return nil, false
	}
	switch p.in[p.pos] {
	case 'n':
		if p.literal("null") {
			return nil, true
		}
		return nil, false
	case 't':
		if p.literal("true") {
			return true, true
		}
		return nil, false
	case 'f':
		if p.literal("false") {
			return false, true
		}
		return nil, false
	case '"':
		b, ok := p.parseStringBytes()
		if !ok {
			return nil, false
		}
		return string(b), true
	case '{':
		return p.parseObjectValue(depth)
	default:
		return p.parseNumber()
	}
}

func (p *fastParser) literal(lit string) bool {
	if len(p.in)-p.pos < len(lit) || string(p.in[p.pos:p.pos+len(lit)]) != lit {
		return false
	}
	p.pos += len(lit)
	return true
}

// parseObjectValue handles '{'-rooted values: the tagged envelope if
// the FIRST key is "_t" (the only order the production encoder emits;
// any other envelope arrangement bails to legacy), else a plain map.
func (p *fastParser) parseObjectValue(depth int) (any, bool) {
	p.pos++ // past '{'
	p.skipWS()
	if p.consume('}') {
		return map[string]any{}, true
	}
	kb, ok := p.parseStringBytes()
	if !ok {
		return nil, false
	}
	if string(kb) == "_t" {
		return p.parseEnvelope(depth)
	}
	// Plain map. Legacy probes "_t" anywhere in the object, so a
	// late "_t" key means legacy would have envelope-decoded — bail.
	out := make(map[string]any, 8)
	k := p.dec.key(kb)
	for {
		p.skipWS()
		if !p.consume(':') {
			return nil, false
		}
		v, ok := p.parseValue(depth + 1)
		if !ok {
			return nil, false
		}
		out[k] = v
		p.skipWS()
		if p.consume('}') {
			return out, true
		}
		if !p.consume(',') {
			return nil, false
		}
		p.skipWS()
		kb, ok = p.parseStringBytes()
		if !ok || string(kb) == "_t" {
			return nil, false
		}
		k = p.dec.key(kb)
	}
}

// parseEnvelope decodes `"_t":"<tag>","v":<payload>}` (the leading
// `{"_t"` already consumed). Only the canonical two-key sorted shape
// the production encoder emits is accepted; everything else bails.
func (p *fastParser) parseEnvelope(depth int) (any, bool) {
	p.skipWS()
	if !p.consume(':') {
		return nil, false
	}
	p.skipWS()
	tagB, ok := p.parseStringBytes()
	if !ok {
		return nil, false
	}
	// Tags are few and short; switch on the bytes without allocating.
	var tag string
	switch string(tagB) {
	case "bytes":
		tag = "bytes"
	case "time":
		tag = "time"
	case "i64":
		tag = "i64"
	case "u64":
		tag = "u64"
	case "f64":
		tag = "f64"
	case "f64s":
		tag = "f64s"
	case "list":
		tag = "list"
	case "list_str":
		tag = "list_str"
	case "map":
		tag = "map"
	default:
		return nil, false
	}
	p.skipWS()
	if !p.consume(',') {
		return nil, false
	}
	p.skipWS()
	kb, ok := p.parseStringBytes()
	if !ok || string(kb) != "v" {
		return nil, false
	}
	p.skipWS()
	if !p.consume(':') {
		return nil, false
	}
	v, ok := p.parseEnvelopePayload(tag, depth)
	if !ok {
		return nil, false
	}
	p.skipWS()
	if !p.consume('}') {
		return nil, false
	}
	return v, true
}

func (p *fastParser) parseEnvelopePayload(tag string, depth int) (any, bool) {
	p.skipWS()
	switch tag {
	case "bytes":
		sb, ok := p.parseStringBytes()
		if !ok {
			return nil, false
		}
		out, err := base64.StdEncoding.DecodeString(string(sb))
		if err != nil {
			return nil, false
		}
		return out, true
	case "time":
		sb, ok := p.parseStringBytes()
		if !ok {
			return nil, false
		}
		t, err := time.Parse(time.RFC3339Nano, string(sb))
		if err != nil {
			return nil, false
		}
		return t, true
	case "i64":
		nb, ok := p.parseNumberBytes()
		if !ok {
			return nil, false
		}
		n, err := strconv.ParseInt(string(nb), 10, 64)
		if err != nil {
			return nil, false
		}
		return n, true
	case "u64":
		sb, ok := p.parseStringBytes()
		if !ok {
			return nil, false
		}
		u, err := strconv.ParseUint(string(sb), 10, 64)
		if err != nil {
			return nil, false
		}
		return u, true
	case "f64s":
		sb, ok := p.parseStringBytes()
		if !ok {
			return nil, false
		}
		f, err := nonFiniteFromString(string(sb))
		if err != nil {
			// Alien sentinel — bail; legacy owns the loud error.
			return nil, false
		}
		return f, true
	case "f64":
		nb, ok := p.parseNumberBytes()
		if !ok {
			return nil, false
		}
		f, err := strconv.ParseFloat(string(nb), 64)
		if err != nil {
			return nil, false
		}
		return f, true
	case "list":
		if !p.consume('[') {
			return nil, false
		}
		out := []any{}
		p.skipWS()
		if p.consume(']') {
			return out, true
		}
		for {
			v, ok := p.parseValue(depth + 1)
			if !ok {
				return nil, false
			}
			out = append(out, v)
			p.skipWS()
			if p.consume(']') {
				return out, true
			}
			if !p.consume(',') {
				return nil, false
			}
		}
	case "list_str":
		if !p.consume('[') {
			return nil, false
		}
		out := []string{}
		p.skipWS()
		if p.consume(']') {
			return out, true
		}
		for {
			p.skipWS()
			// Legacy `json.Unmarshal` into []string leaves "" for a
			// JSON null element without error — replicate.
			if p.pos < len(p.in) && p.in[p.pos] == 'n' {
				if !p.literal("null") {
					return nil, false
				}
				out = append(out, "")
			} else {
				sb, ok := p.parseStringBytes()
				if !ok {
					return nil, false
				}
				out = append(out, string(sb))
			}
			p.skipWS()
			if p.consume(']') {
				return out, true
			}
			if !p.consume(',') {
				return nil, false
			}
		}
	case "map":
		if !p.consume('{') {
			return nil, false
		}
		// NOTE: unlike a bare object, the "map" payload is decoded
		// per-value with NO "_t" probe on the payload object itself
		// (legacy decodeTaggedValue's map branch) — a payload key
		// named "_t" is data here, not an envelope marker.
		out := make(map[string]any, 8)
		p.skipWS()
		if p.consume('}') {
			return out, true
		}
		for {
			p.skipWS()
			kb, ok := p.parseStringBytes()
			if !ok {
				return nil, false
			}
			k := p.dec.key(kb)
			p.skipWS()
			if !p.consume(':') {
				return nil, false
			}
			v, ok := p.parseValue(depth + 1)
			if !ok {
				return nil, false
			}
			out[k] = v
			p.skipWS()
			if p.consume('}') {
				return out, true
			}
			if !p.consume(',') {
				return nil, false
			}
		}
	}
	return nil, false
}

// parseNumber parses a strict-JSON-grammar number as float64 (the
// type the legacy natural decode produces for bare numbers).
func (p *fastParser) parseNumber() (any, bool) {
	nb, ok := p.parseNumberBytes()
	if !ok {
		return nil, false
	}
	f, err := strconv.ParseFloat(string(nb), 64)
	if err != nil {
		// Out-of-range (e.g. 1e999) — legacy errors; let it.
		return nil, false
	}
	return f, true
}

// parseNumberBytes scans one number with the exact JSON grammar
// (`-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?`). Anything looser
// would accept input the legacy path rejects.
func (p *fastParser) parseNumberBytes() ([]byte, bool) {
	start := p.pos
	p.consume('-')
	switch {
	case p.consume('0'):
		// no further int digits allowed
	case p.pos < len(p.in) && p.in[p.pos] >= '1' && p.in[p.pos] <= '9':
		p.pos++
		for p.pos < len(p.in) && isDigit(p.in[p.pos]) {
			p.pos++
		}
	default:
		return nil, false
	}
	if p.consume('.') {
		if p.pos >= len(p.in) || !isDigit(p.in[p.pos]) {
			return nil, false
		}
		for p.pos < len(p.in) && isDigit(p.in[p.pos]) {
			p.pos++
		}
	}
	if p.pos < len(p.in) && (p.in[p.pos] == 'e' || p.in[p.pos] == 'E') {
		p.pos++
		if p.pos < len(p.in) && (p.in[p.pos] == '+' || p.in[p.pos] == '-') {
			p.pos++
		}
		if p.pos >= len(p.in) || !isDigit(p.in[p.pos]) {
			return nil, false
		}
		for p.pos < len(p.in) && isDigit(p.in[p.pos]) {
			p.pos++
		}
	}
	return p.in[start:p.pos], true
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

// parseStringBytes parses one JSON string and returns its decoded
// bytes. The result aliases either the input (no-escape fast path) or
// p.scratch (escape path) — callers must copy before the parser is
// used again. Unescaping matches stdlib: standard short escapes,
// \uXXXX with UTF-16 surrogate pairing (unpaired → U+FFFD), invalid
// UTF-8 in the source replaced with U+FFFD, raw control bytes
// rejected (bail; the line is invalid JSON and legacy owns the
// error).
func (p *fastParser) parseStringBytes() ([]byte, bool) {
	if !p.consume('"') {
		return nil, false
	}
	start := p.pos
	// Fast scan: bail-out points are escape, closing quote, control
	// byte, or non-ASCII (which forces the UTF-8 validity walk).
	for p.pos < len(p.in) {
		b := p.in[p.pos]
		switch {
		case b == '"':
			s := p.in[start:p.pos]
			p.pos++
			return s, true
		case b == '\\' || b >= utf8.RuneSelf:
			return p.parseStringSlow(start)
		case b < 0x20:
			return nil, false
		default:
			p.pos++
		}
	}
	return nil, false
}

// parseStringSlow finishes a string that contains escapes and/or
// non-ASCII bytes, writing decoded bytes into p.scratch.
func (p *fastParser) parseStringSlow(start int) ([]byte, bool) {
	p.scratch = append(p.scratch[:0], p.in[start:p.pos]...)
	for p.pos < len(p.in) {
		b := p.in[p.pos]
		switch {
		case b == '"':
			p.pos++
			return p.scratch, true
		case b == '\\':
			p.pos++
			if p.pos >= len(p.in) {
				return nil, false
			}
			esc := p.in[p.pos]
			switch esc {
			case '"', '\\', '/':
				p.scratch = append(p.scratch, esc)
				p.pos++
			case 'b':
				p.scratch = append(p.scratch, '\b')
				p.pos++
			case 'f':
				p.scratch = append(p.scratch, '\f')
				p.pos++
			case 'n':
				p.scratch = append(p.scratch, '\n')
				p.pos++
			case 'r':
				p.scratch = append(p.scratch, '\r')
				p.pos++
			case 't':
				p.scratch = append(p.scratch, '\t')
				p.pos++
			case 'u':
				r, ok := p.parseU4()
				if !ok {
					return nil, false
				}
				if utf16.IsSurrogate(r) {
					r2 := rune(unicode.ReplacementChar)
					if p.pos+1 < len(p.in) && p.in[p.pos] == '\\' && p.in[p.pos+1] == 'u' {
						save := p.pos
						p.pos++ // onto 'u'; parseU4 steps past it
						lo, ok2 := p.parseU4()
						if !ok2 {
							return nil, false
						}
						if dec := utf16.DecodeRune(r, lo); dec != unicode.ReplacementChar {
							r2 = dec
						} else {
							// Not a valid pair: the second escape is
							// its own rune; rewind so it re-parses.
							p.pos = save
						}
					}
					r = r2
				}
				p.scratch = utf8.AppendRune(p.scratch, r)
			default:
				return nil, false
			}
		case b < 0x20:
			return nil, false
		case b < utf8.RuneSelf:
			p.scratch = append(p.scratch, b)
			p.pos++
		default:
			c, size := utf8.DecodeRune(p.in[p.pos:])
			if c == utf8.RuneError && size == 1 {
				p.scratch = utf8.AppendRune(p.scratch, utf8.RuneError)
				p.pos++
				continue
			}
			p.scratch = append(p.scratch, p.in[p.pos:p.pos+size]...)
			p.pos += size
		}
	}
	return nil, false
}

// parseU4 reads 4 hex digits after a consumed `\u`, returning the
// code unit. p.pos is on the first hex digit at entry.
func (p *fastParser) parseU4() (rune, bool) {
	p.pos++ // past 'u'
	if p.pos+4 > len(p.in) {
		return 0, false
	}
	var r rune
	for _, c := range p.in[p.pos : p.pos+4] {
		switch {
		case c >= '0' && c <= '9':
			c -= '0'
		case c >= 'a' && c <= 'f':
			c = c - 'a' + 10
		case c >= 'A' && c <= 'F':
			c = c - 'A' + 10
		default:
			return 0, false
		}
		r = r*16 + rune(c)
	}
	p.pos += 4
	return r, true
}

// sortedColumnNames returns the column names in ascending order — the
// stdlib map-key marshal order the fast encoder must reproduce.
func sortedColumnNames(columns []*ir.Column) []string {
	names := make([]string, len(columns))
	for i, c := range columns {
		names[i] = c.Name
	}
	slices.Sort(names)
	// Duplicate column names can't occur in a real schema, but the
	// legacy map-based encode dedupes them — match it exactly.
	return slices.Compact(names)
}
