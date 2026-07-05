// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package blobcodec

// Differential pins for the fast chunk row codec
// (backup_chunk_fast.go). The contract under test:
//
//   - Encode: appendRowJSON output is byte-identical to the legacy
//     `json.Marshal(map)` path for every value the fast path accepts,
//     and every fast-path bail is a case the legacy marshal errors on
//     (non-finite floats, alien types).
//   - Decode: decodeRow either produces exactly what readRowLegacy
//     produces (deep-equal, including concrete types) or bails — it
//     NEVER accepts a line the legacy path rejects, and never decodes
//     one differently.
//
// Per the Bug-74 lesson the generators cover every value family ×
// shape (scalar / list element / map value / NULL), not one
// representative — plus a curated corpus of nasty edges (control
// bytes, HTML escapes, U+2028/29, invalid UTF-8, surrogate pairs,
// 2^53 neighbors, MaxUint64, -0.0, float32, exponent boundaries).

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// ---------------------------------------------------------------
// Generators
// ---------------------------------------------------------------

// nastyStrings is the curated string corpus: every class the escaper
// special-cases, plus boundary shapes.
var nastyStrings = []string{
	"",
	"plain",
	`with "quotes" and \backslashes\`,
	"html <tag> & amp",
	"newline\nreturn\rtab\tbackspace\bformfeed\f",
	"\x00\x01\x1f control bytes",
	"line sep   para sep  ",
	"invalid utf8: \xff\xfe partial: \xc3",
	"emoji 🚀 cjk 漢字 combining é",
	"trailing backslash \\",
	"é́ nfd-ish",
	strings.Repeat("long ", 100),
	"null", "true", "_t", // confusable literals/keys
}

// genString picks or mutates a nasty string.
func genString(r *rand.Rand) string {
	s := nastyStrings[r.IntN(len(nastyStrings))]
	if r.IntN(4) == 0 {
		// Random bytes — exercises arbitrary (in)valid UTF-8.
		b := make([]byte, r.IntN(24))
		for i := range b {
			b[i] = byte(r.UintN(256))
		}
		return string(b)
	}
	return s
}

var nastyFloats = []float64{
	0, math.Copysign(0, -1), 1, -1, 0.5, 1e-6, 9.999999e-7, 1e21,
	9.999999999999998e20, 1e-7, 3.14159265358979, 1 << 53, (1 << 53) + 2,
	-(1 << 53), 2.2250738585072014e-308, 1.7976931348623157e308,
	5e-324, 123456.789e100, math.NaN(), math.Inf(1), math.Inf(-1),
}

var nastyInts = []int64{
	0, 1, -1, 42, -42, math.MaxInt64, math.MinInt64,
	1<<53 + 1, -(1<<53 + 1), 1 << 31, -(1 << 31),
}

var nastyUints = []uint64{0, 1, math.MaxUint64, math.MaxInt64 + 1, 1 << 53}

var nastyTimes = []time.Time{
	time.Date(2026, 6, 11, 20, 0, 0, 0, time.UTC),
	time.Date(2026, 6, 11, 20, 0, 0, 123456789, time.UTC),
	time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
	time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
	time.Date(2026, 6, 11, 12, 0, 0, 1, time.FixedZone("PDT", -7*3600)),
	time.Unix(0, 0).UTC(),
}

// genValue produces one value-contract-shaped value: every family the
// encoder dispatches on, nested shapes included.
func genValue(r *rand.Rand, depth int) any {
	variants := 14
	if depth > 2 {
		variants = 10 // leaves only below depth 2
	}
	// Nil typed slices/maps take a DIFFERENT legacy marshal path than
	// their empty counterparts (json.Marshal of a nil slice → null) —
	// the []string(nil) divergence was missed precisely because the
	// generator only ever make()'d non-nil values. Keep these in.
	if r.IntN(16) == 0 {
		switch r.IntN(4) {
		case 0:
			return []string(nil)
		case 1:
			return []any(nil)
		case 2:
			return []byte(nil)
		default:
			return map[string]any(nil)
		}
	}
	switch r.IntN(variants) {
	case 0:
		return nil
	case 1:
		return genString(r)
	case 2:
		return r.IntN(2) == 0
	case 3:
		return nastyFloats[r.IntN(len(nastyFloats))]
	case 4:
		return float32(nastyFloats[r.IntN(len(nastyFloats))])
	case 5:
		return nastyInts[r.IntN(len(nastyInts))]
	case 6:
		switch r.IntN(4) { // narrow int widths
		case 0:
			return int(int32(nastyInts[r.IntN(len(nastyInts))]))
		case 1:
			return int8(nastyInts[r.IntN(len(nastyInts))])
		case 2:
			return int16(nastyInts[r.IntN(len(nastyInts))])
		default:
			return int32(nastyInts[r.IntN(len(nastyInts))])
		}
	case 7:
		return nastyUints[r.IntN(len(nastyUints))]
	case 8:
		switch r.IntN(4) { // narrow uint widths
		case 0:
			return uint(uint32(nastyUints[r.IntN(len(nastyUints))]))
		case 1:
			return uint8(nastyUints[r.IntN(len(nastyUints))])
		case 2:
			return uint16(nastyUints[r.IntN(len(nastyUints))])
		default:
			return uint32(nastyUints[r.IntN(len(nastyUints))])
		}
	case 9:
		b := make([]byte, r.IntN(20))
		for i := range b {
			b[i] = byte(r.UintN(256))
		}
		return b
	case 10:
		return nastyTimes[r.IntN(len(nastyTimes))]
	case 11:
		n := r.IntN(4)
		out := make([]any, n)
		for i := range out {
			out[i] = genValue(r, depth+1)
		}
		return out
	case 12:
		n := r.IntN(4)
		out := make([]string, n)
		for i := range out {
			out[i] = genString(r)
		}
		return out
	default:
		n := r.IntN(4)
		out := make(map[string]any, n)
		for i := 0; i < n; i++ {
			out[genString(r)] = genValue(r, depth+1)
		}
		return out
	}
}

func genRow(r *rand.Rand) (ir.Row, []*ir.Column) {
	n := 1 + r.IntN(8)
	row := make(ir.Row, n)
	cols := make([]*ir.Column, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("col_%c%d", 'a'+rune(r.IntN(26)), i)
		if r.IntN(8) == 0 {
			name = genString(r) // nasty column names too
		}
		cols = append(cols, &ir.Column{Name: name})
		if r.IntN(10) == 0 {
			continue // absent from row → encoder emits null
		}
		row[name] = genValue(r, 0)
	}
	return row, cols
}

// legacyMarshalRow reproduces the legacy WriteRow encode (the oracle).
func legacyMarshalRow(row ir.Row, columns []*ir.Column) ([]byte, error) {
	enc := make(map[string]any, len(columns))
	for _, c := range columns {
		v, ok := row[c.Name]
		if !ok {
			enc[c.Name] = nil
			continue
		}
		enc[c.Name] = encodeValue(v)
	}
	return json.Marshal(enc)
}

// checkEncodeDifferential asserts the fast/legacy encode contract for
// one row: fast output, when produced, is byte-identical to legacy;
// a fast bail is always allowed (WriteRow falls back, so legacy owns
// the bytes AND the errors for bailed shapes — e.g. []string(nil),
// NaN/Inf, alien types). Returns the line that would land on disk
// (fast==legacy bytes, or legacy's fallback bytes), nil if the row
// doesn't encode at all.
func checkEncodeDifferential(t *testing.T, row ir.Row, cols []*ir.Column) (line []byte, viaFast bool) {
	t.Helper()
	fast, ok := appendRowJSON(nil, row, sortedColumnNames(cols))
	legacy, err := legacyMarshalRow(row, cols)
	if !ok {
		if err != nil {
			return nil, false // both reject: legacy's error ships
		}
		return legacy, false // fallback bytes ship
	}
	if err != nil {
		t.Fatalf("fast encoder accepted a row the legacy marshal rejects: %v\nrow: %#v", err, row)
	}
	if !bytes.Equal(fast, legacy) {
		t.Fatalf("fast/legacy encode divergence\nrow:    %#v\nfast:   %s\nlegacy: %s", row, fast, legacy)
	}
	return legacy, true
}

// checkDecodeDifferential asserts the fast/legacy decode contract for
// one line.
func checkDecodeDifferential(t *testing.T, line []byte) {
	t.Helper()
	var dec fastRowDecoder
	fastRow, ok := dec.decodeRow(line)
	if !ok {
		return // bail is always safe — legacy owns the line
	}
	legacyRow, err := readRowLegacy(line)
	if err != nil {
		t.Fatalf("fast decoder accepted a line the legacy decoder rejects: %v\nline: %s", err, line)
	}
	if !reflect.DeepEqual(normalizeFloats(fastRow), normalizeFloats(legacyRow)) {
		t.Fatalf("fast/legacy decode divergence\nline:   %s\nfast:   %#v\nlegacy: %#v", line, fastRow, legacyRow)
	}
}

// normalizeFloats deep-copies v with every float64 replaced by its
// IEEE bit pattern, so reflect.DeepEqual can compare structures
// containing NaN (NaN != NaN under ==, and DeepEqual uses ==). Bit
// equality is STRICTER than ==: it also distinguishes -0 from 0,
// which the codec contract preserves.
func normalizeFloats(v any) any {
	switch x := v.(type) {
	case float64:
		return math.Float64bits(x)
	case ir.Row:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = normalizeFloats(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = normalizeFloats(e)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = normalizeFloats(e)
		}
		return out
	default:
		return v
	}
}

// checkFastDecodesCanonical pins the perf path itself: a line the
// fast ENCODER produced is canonical by construction, so the fast
// DECODER must accept it (an always-bail decoder regression would
// otherwise pass every correctness test while silently reverting the
// hot path to legacy speed).
func checkFastDecodesCanonical(t *testing.T, line []byte) {
	t.Helper()
	var dec fastRowDecoder
	if _, ok := dec.decodeRow(line); !ok {
		t.Fatalf("fast decoder bailed on canonical fast-encoder output\nline: %s", line)
	}
}

// TestFastRowCodec_FastPathEngages pins that a canonical row takes
// the fast path in BOTH directions (an always-bail regression would
// silently revert the hot path to legacy speed while passing every
// equivalence test).
func TestFastRowCodec_FastPathEngages(t *testing.T) {
	row, cols, _ := benchRow()
	line, ok := appendRowJSON(nil, row, sortedColumnNames(cols))
	if !ok {
		t.Fatal("fast encoder bailed on a canonical row")
	}
	checkFastDecodesCanonical(t, line)
}

// ---------------------------------------------------------------
// Deterministic differential sweeps (run in normal CI)
// ---------------------------------------------------------------

func TestFastRowCodec_DifferentialSweep(t *testing.T) {
	for seed := uint64(0); seed < 200; seed++ {
		r := rand.New(rand.NewPCG(seed, seed^0x5eed))
		for i := 0; i < 100; i++ {
			row, cols := genRow(r)
			line, viaFast := checkEncodeDifferential(t, row, cols)
			if line == nil {
				continue
			}
			if viaFast {
				checkFastDecodesCanonical(t, line)
			}
			checkDecodeDifferential(t, line)
		}
	}
}

// TestFastRowCodec_DecodeAlienShapes pins the bail behavior on lines
// the production encoder never emits but the legacy decoder has
// defined semantics for — the fast path must either match or bail,
// never mis-decode.
func TestFastRowCodec_DecodeAlienShapes(t *testing.T) {
	lines := []string{
		``,
		`null`,
		`[1,2]`,
		`{}`,
		`  { }  `,
		`{"a":1}`,
		`{"a":1.5e10,"b":-0.0,"c":1e999}`,
		`{"a":01}`,                       // invalid: leading zero
		`{"a":+1}`,                       // invalid: leading plus
		`{"a":1.}`,                       // invalid: bare decimal point
		`{"a":"𝄞"}`,                      // valid surrogate pair
		`{"a":"\ud834"}`,                 // unpaired high surrogate
		`{"a":"\udd1e"}`,                 // unpaired low surrogate
		`{"a":"\ud834𝄞"}`,                // high + valid pair
		`{"a":"\ud834\uZZZZ"}`,           // pair attempt, bad hex
		`{"a":"\q"}`,                     // invalid escape
		`{"a":"<script>"}`,               // HTML escapes (encoder-emitted)
		"{\"a\":\"raw\tcontrol\"}",       // raw control byte in string: invalid
		`{"a":{"v":1,"_t":"i64"}}`,       // reversed envelope key order
		`{"a":{"_t":"i64","v":1,"x":2}}`, // envelope with extra key
		`{"a":{"_t":"i64"}}`,             // envelope missing v
		`{"a":{"_t":"nope","v":1}}`,      // unknown tag
		`{"a":{"_t":7,"v":1}}`,           // non-string tag
		`{"a":{"_t":"i64","v":"1"}}`,     // wrong payload type
		`{"a":{"_t":"i64","v":1.5}}`,     // non-integer i64 payload
		`{"a":{"_t":"i64","v":99999999999999999999}}`,   // i64 overflow
		`{"a":{"_t":"u64","v":"18446744073709551616"}}`, // u64 overflow
		`{"a":{"_t":"u64","v":"-1"}}`,
		`{"a":{"_t":"bytes","v":"!!!"}}`, // invalid base64
		`{"a":{"_t":"bytes","v":"QUJD"}}`,
		`{"a":{"_t":"time","v":"not a time"}}`,
		`{"a":{"_t":"time","v":"2026-06-11T20:00:00.5+07:00"}}`,
		`{"a":{"_t":"list","v":null}}`,
		`{"a":{"_t":"list_str","v":["x",null]}}`, // null elem → ""
		`{"a":{"_t":"list_str","v":[1]}}`,
		`{"a":{"_t":"map","v":{"_t":"data not a tag"}}}`, // _t as payload-map data
		`{"a":{"nested":{"_t":"i64","v":7}}}`,            // envelope under plain map
		`{"a":1}{"b":2}`,                                 // trailing garbage
		`{"a":1} `,                                       // trailing whitespace (valid)
		`{"a":1,"a":2}`,                                  // duplicate keys: last wins
		`{"a":{"_t":"i64","v":1},"a":"x"}`,               // dup with envelope first
		`{"":""}`,                                        // empty key
		"{\"a\":\"\\u0000\"}",                            // escaped NUL
		`{"a":"☃","b":"☃"}`,
	}
	for _, l := range lines {
		checkDecodeDifferential(t, []byte(l))
	}
}

// TestFastRowCodec_FamilyMatrixRoundTrip is the Bug-74-style pin:
// every value family × {scalar, list element, map value, NULL} must
// survive writer→reader through the real chunk machinery.
func TestFastRowCodec_FamilyMatrixRoundTrip(t *testing.T) {
	families := map[string]any{
		"nil":     nil,
		"string":  "héllo <world> &   \n",
		"bool":    true,
		"f64":     3.14159e-9,
		"f32":     float32(2.5),
		"i64":     int64(1<<53 + 1),
		"int":     int(-7),
		"u64":     uint64(math.MaxUint64),
		"bytes":   []byte{0, 1, 254, 255},
		"time":    time.Date(2026, 6, 11, 20, 0, 0, 123456789, time.UTC),
		"liststr": []string{"a", "", "c<&>"},
		"list":    []any{int64(1), "two", nil, []byte{3}},
		"map":     map[string]any{"k1": int64(9), "k2": nil, "_t": "data"},
		// Nil typed slices/maps: distinct marshal shapes from their
		// empty counterparts (the []string(nil) fidelity finding).
		"liststr_nil": []string(nil),
		"list_nil":    []any(nil),
		"bytes_nil":   []byte(nil),
		"map_nil":     map[string]any(nil),
	}
	for name, v := range families {
		t.Run(name, func(t *testing.T) {
			row := ir.Row{
				"scalar": v,
				"inlist": []any{v},
				"inmap":  map[string]any{"x": v},
				"isnull": nil,
			}
			cols := []*ir.Column{
				{Name: "scalar"}, {Name: "inlist"}, {Name: "inmap"}, {Name: "isnull"},
			}
			var buf bytes.Buffer
			w, err := NewChunkWriter(&buf, []string{"scalar", "inlist", "inmap", "isnull"}, nil, CodecGzip)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.WriteRow(row, cols); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			r, err := NewChunkReader(io.NopCloser(bytes.NewReader(buf.Bytes())), w.Hash(), nil, CodecGzip)
			if err != nil {
				t.Fatal(err)
			}
			got, err := r.ReadRow()
			if err != nil {
				t.Fatal(err)
			}
			want := ir.Row{
				"scalar": normalizeExpected(v),
				"inlist": []any{normalizeExpected(v)},
				"inmap":  map[string]any{"x": normalizeExpected(v)},
				"isnull": nil,
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("round-trip divergence\ngot:  %#v\nwant: %#v", got, want)
			}
			if _, err := r.ReadRow(); !errors.Is(err, io.EOF) {
				t.Fatalf("expected EOF, got %v", err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestChunkCodec_NonFiniteFloats is the Bug-138 pin: every IEEE
// special × float width × shape must survive the chunk writer→reader
// round trip (the pre-fix codec REFUSED the whole table with
// "json: unsupported value: NaN" — loud, but it made any database
// holding one NaN row un-backupable while `migrate` carried the same
// value fine). Per the Bug-74 lesson the matrix covers every family ×
// shape, not one representative. Bit-pattern assertions (NaN != NaN
// under ==), and a numeric-NaN STRING must stay a string — the
// sentinel envelope must never capture it.
func TestChunkCodec_NonFiniteFloats(t *testing.T) {
	specials := map[string]float64{
		"nan":     math.NaN(),
		"posinf":  math.Inf(1),
		"neginf":  math.Inf(-1),
		"neg0":    math.Copysign(0, -1), // finite control: bare number path
		"finite1": 6.25,                 // finite control
	}
	for name, f := range specials {
		for _, width := range []string{"f64", "f32"} {
			t.Run(name+"_"+width, func(t *testing.T) {
				var v any = f
				want := f
				if width == "f32" {
					v = float32(f)
					want = float64(float32(f))
				}
				if math.IsNaN(want) {
					// The "NaN" sentinel cannot carry payload bits, so
					// every NaN round-trips to the IEEE-canonical quiet
					// NaN — the same bit pattern PG's own text format
					// produces, keeping sluice restores float8send-
					// identical to pg_restore. ±Inf and finite values
					// stay bit-exact.
					want = canonicalNaN
				}
				row := ir.Row{
					"scalar":  v,
					"inlist":  []any{v},
					"inmap":   map[string]any{"x": v},
					"numstr":  "NaN", // numeric-as-string control
					"sibling": int64(7),
				}
				cols := []*ir.Column{
					{Name: "scalar"},
					{Name: "inlist"},
					{Name: "inmap"},
					{Name: "numstr"},
					{Name: "sibling"},
				}
				var buf bytes.Buffer
				w, err := NewChunkWriter(&buf, []string{"scalar", "inlist", "inmap", "numstr", "sibling"}, nil, CodecGzip)
				if err != nil {
					t.Fatal(err)
				}
				if err := w.WriteRow(row, cols); err != nil {
					t.Fatalf("WriteRow refused the row (the Bug-138 shape): %v", err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				r, err := NewChunkReader(io.NopCloser(bytes.NewReader(buf.Bytes())), w.Hash(), nil, CodecGzip)
				if err != nil {
					t.Fatal(err)
				}
				got, err := r.ReadRow()
				if err != nil {
					t.Fatal(err)
				}
				assertBits := func(label string, gv any) {
					t.Helper()
					gf, ok := gv.(float64)
					if !ok {
						t.Fatalf("%s: got %T (%#v); want float64", label, gv, gv)
					}
					if math.Float64bits(gf) != math.Float64bits(want) {
						t.Fatalf("%s: got %v (bits %x); want %v (bits %x)",
							label, gf, math.Float64bits(gf), want, math.Float64bits(want))
					}
				}
				assertBits("scalar", got["scalar"])
				assertBits("inlist[0]", got["inlist"].([]any)[0])
				assertBits("inmap.x", got["inmap"].(map[string]any)["x"])
				if s, ok := got["numstr"].(string); !ok || s != "NaN" {
					t.Fatalf("numstr: got %T %#v; want the literal string \"NaN\"", got["numstr"], got["numstr"])
				}
				if err := r.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// TestChunkCodec_F64sStrictDecode pins the loud-failure ladder of the
// sentinel: an alien payload is corruption, not a zero.
func TestChunkCodec_F64sStrictDecode(t *testing.T) {
	for _, line := range []string{
		`{"a":{"_t":"f64s","v":"nan"}}`,      // wrong case
		`{"a":{"_t":"f64s","v":"Infinity"}}`, // wrong spelling
		`{"a":{"_t":"f64s","v":1}}`,          // wrong type
		`{"a":{"_t":"f64s","v":""}}`,
	} {
		if _, err := readRowLegacy([]byte(line)); err == nil {
			t.Fatalf("legacy decoder accepted alien f64s payload: %s", line)
		}
		var dec fastRowDecoder
		if _, ok := dec.decodeRow([]byte(line)); ok {
			t.Fatalf("fast decoder accepted alien f64s payload instead of bailing: %s", line)
		}
		checkDecodeDifferential(t, []byte(line))
	}
	// And the three canonical sentinels decode on the FAST path (the
	// canonical-accept guarantee for encoder-produced lines).
	for _, line := range []string{
		`{"a":{"_t":"f64s","v":"NaN"}}`,
		`{"a":{"_t":"f64s","v":"+Inf"}}`,
		`{"a":{"_t":"f64s","v":"-Inf"}}`,
	} {
		checkFastDecodesCanonical(t, []byte(line))
		checkDecodeDifferential(t, []byte(line))
	}
}

// normalizeExpected maps a written value to its contract round-trip
// shape: narrow ints widen to int64, narrow uints to uint64, float32
// comes back float64, bool/string/nil/bytes/time survive as-is. This
// IS the documented value contract (docs/value-types.md), not a
// fast-path artifact — the legacy codec round-trips identically.
func normalizeExpected(v any) any {
	switch x := v.(type) {
	case []byte:
		if x == nil {
			// nil []byte encodes as base64 "" and decodes to the
			// empty (non-nil) slice — legacy behavior.
			return []byte{}
		}
		return x
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case uint:
		return uint64(x)
	case uint8:
		return uint64(x)
	case uint16:
		return uint64(x)
	case uint32:
		return uint64(x)
	case float32:
		return float64(x)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = normalizeExpected(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = normalizeExpected(e)
		}
		return out
	default:
		return v
	}
}

// ---------------------------------------------------------------
// Fuzz targets (corpus seeds run in normal `go test`; the scheduled
// fuzz workflow drives them deeper)
// ---------------------------------------------------------------

func FuzzFastRowEncodeDifferential(f *testing.F) {
	for s := uint64(0); s < 16; s++ {
		f.Add(s, s^0xbeef)
	}
	f.Fuzz(func(t *testing.T, s1, s2 uint64) {
		r := rand.New(rand.NewPCG(s1, s2))
		row, cols := genRow(r)
		line, viaFast := checkEncodeDifferential(t, row, cols)
		if line == nil {
			return
		}
		if viaFast {
			checkFastDecodesCanonical(t, line)
		}
		checkDecodeDifferential(t, line)
	})
}

func FuzzFastRowDecodeDifferential(f *testing.F) {
	f.Add([]byte(`{"a":{"_t":"i64","v":1},"b":"x","c":1.5,"d":null}`))
	f.Add([]byte(`{"a":{"_t":"list","v":[{"_t":"bytes","v":"QUJD"}]}}`))
	f.Add([]byte(`{"a":{"_t":"map","v":{"k":{"_t":"time","v":"2026-06-11T20:00:00Z"}}}}`))
	f.Add([]byte(`{"a":{"_t":"u64","v":"18446744073709551615"}}`))
	f.Add([]byte(`{"a":"𝄞   <&> �"}`))
	f.Fuzz(func(t *testing.T, line []byte) {
		checkDecodeDifferential(t, line)
	})
}

// ---------------------------------------------------------------
// Benchmarks (fast vs legacy, realistic mixed row)
// ---------------------------------------------------------------

func benchRow() (ir.Row, []*ir.Column, []string) {
	row := ir.Row{
		"id":         int64(1234567),
		"uuid":       "3b241101-e2bb-4255-8caf-4136c566a962",
		"name":       "Ada Lovelace <analyst> & engineer",
		"balance":    "12345.67",
		"active":     true,
		"score":      98.6,
		"payload":    []byte("binary\x00payload\xff bytes here"),
		"created_at": time.Date(2026, 6, 11, 20, 0, 0, 123456789, time.UTC),
		"tags":       []string{"alpha", "beta"},
		"meta":       map[string]any{"k": int64(1), "s": "v"},
	}
	names := make([]string, 0, len(row))
	cols := make([]*ir.Column, 0, len(row))
	for k := range row {
		names = append(names, k)
	}
	// Deterministic order for the writer header.
	for _, k := range []string{"id", "uuid", "name", "balance", "active", "score", "payload", "created_at", "tags", "meta"} {
		cols = append(cols, &ir.Column{Name: k})
	}
	return row, cols, names
}

func BenchmarkChunkRowEncodeFast(b *testing.B) {
	row, cols, _ := benchRow()
	sorted := sortedColumnNames(cols)
	var buf []byte
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var ok bool
		buf, ok = appendRowJSON(buf[:0], row, sorted)
		if !ok {
			b.Fatal("bail")
		}
	}
}

func BenchmarkChunkRowEncodeLegacy(b *testing.B) {
	row, cols, _ := benchRow()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := legacyMarshalRow(row, cols); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChunkRowDecodeFast(b *testing.B) {
	row, cols, _ := benchRow()
	line, ok := appendRowJSON(nil, row, sortedColumnNames(cols))
	if !ok {
		b.Fatal("bail")
	}
	var dec fastRowDecoder
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := dec.decodeRow(line); !ok {
			b.Fatal("bail")
		}
	}
}

func BenchmarkChunkRowDecodeLegacy(b *testing.B) {
	row, cols, _ := benchRow()
	line, ok := appendRowJSON(nil, row, sortedColumnNames(cols))
	if !ok {
		b.Fatal("bail")
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := readRowLegacy(line); err != nil {
			b.Fatal(err)
		}
	}
}
