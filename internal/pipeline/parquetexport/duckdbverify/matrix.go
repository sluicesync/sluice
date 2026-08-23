// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

// The family × shape matrix and its hand-derived expected values.
//
// Coverage mirrors the roundtrip pin matrix in
// internal/pipeline/parquetexport/roundtrip_pin_test.go (the Bug-74
// discipline: every family the codec dispatches on, every shape
// variant), re-checked here against an EXTERNAL reader: native
// (bool / signed / unsigned-max / float bit-shapes incl. -0.0 signbit,
// NaN, ±Inf, denormal), string leaves (incl. empty-vs-NULL, PAD-SPACE
// trailing spaces, an embedded NUL byte, and a decomposed combining
// sequence that must not normalize), temporals (DATE incl. year 1, µs
// timestamps naive + tz, time-of-day), all three DECIMAL physical
// tiers + the string fallback, JSON (incl. the
// present-"null"-body-vs-SQL-NULL distinction and a >2^53 integer
// body), bytes (incl. a trailing 0x00) / WKB, and lists (SET + every
// array element leaf the codec can nest: signed/unsigned int, float,
// text, all three decimal tiers, tz + naive timestamps, date,
// time-of-day, bool, bytes, JSON — each × {values incl. NULL element,
// empty list, NULL list}), plus the structural contracts: multi-chunk
// → row-group placement, footer kv metadata, and DuckDB's decoded
// column + list-element types. (Multi-dimensional array values cannot
// appear here: the encoder REFUSES them loudly before any file exists
// — pinned per element family in roundtrip_pin_test.go's
// TestPin_ArrayElementFamilies.)
//
// Expected values are DERIVED BY HAND from the Parquet spec + DuckDB's
// documented renderings (ground-truthed once against DuckDB v1.5.4),
// never computed by reading the files back — a read-back oracle would
// re-introduce exactly the self-consistency this gate exists to break.

import (
	"encoding/json"
	"math"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// tableSpec is one generated .parquet file: an IR table and its rows,
// split into chunks (one writer Flush — hence one row group — each).
type tableSpec struct {
	name   string
	table  *ir.Table
	chunks [][]ir.Row
}

// check pairs one DuckDB query with its expected -json result rows.
type check struct {
	Name  string           `json:"name"`
	Query string           `json:"query"`
	Want  []map[string]any `json:"want"`
}

func col(name string, typ ir.Type) *ir.Column {
	return &ir.Column{Name: name, Type: typ, Nullable: true}
}

func date(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// tableSpecs returns the full generated matrix, deterministic across
// runs (fixed values, fixed order).
func tableSpecs() []tableSpec {
	return []tableSpec{nativeSpec(), strsSpec(), temporalSpec(), decimalsSpec(), jsonbSpec(), listsSpec()}
}

// allChecks returns every DuckDB check, in script order.
func allChecks() []check {
	out := make([]check, 0, 16)
	out = append(out, nativeChecks()...)
	out = append(out, strsChecks()...)
	out = append(out, temporalChecks()...)
	out = append(out, decimalsChecks()...)
	out = append(out, jsonbChecks()...)
	out = append(out, listsChecks()...)
	return out
}

// ---------------------------------------------------------------------
// native: bool / signed / unsigned / float — split into three chunks so
// the chunk→row-group placement is externally verified too.
// ---------------------------------------------------------------------

func nativeSpec() tableSpec {
	table := &ir.Table{Name: "native", Columns: []*ir.Column{
		col("id", ir.Integer{Width: 64}),
		col("b", ir.Boolean{}),
		col("i", ir.Integer{Width: 64}),
		col("u", ir.Integer{Width: 64, Unsigned: true}),
		col("f", ir.Float{Precision: ir.FloatDouble}),
	}}
	rows := []ir.Row{
		{"id": int64(1), "b": true, "i": int64(math.MinInt64), "u": uint64(math.MaxUint64), "f": math.NaN()},
		{"id": int64(2), "b": false, "i": int64(-1), "u": uint64(0), "f": math.Inf(1)},
		{"id": int64(3), "b": nil, "i": int64(0), "u": nil, "f": math.Inf(-1)},
		{"id": int64(4), "b": true, "i": int64(1), "u": int64(1), "f": math.Copysign(0, -1)},
		{"id": int64(5), "b": nil, "i": int64(math.MaxInt64), "u": uint64(9223372036854775808), "f": 0.0},
		{"id": int64(6), "b": false, "i": nil, "u": uint64(42), "f": 5e-324},
		{"id": int64(7), "b": true, "i": int64(-8388608), "u": nil, "f": math.MaxFloat64},
		{"id": int64(8), "b": nil, "i": int64(42), "u": uint64(18446744073709551614), "f": 1.5},
		{"id": int64(9), "b": true, "i": nil, "u": uint64(7), "f": nil},
	}
	return tableSpec{name: "native", table: table, chunks: [][]ir.Row{rows[0:3], rows[3:6], rows[6:9]}}
}

func nativeChecks() []check {
	// Floats are projected to printf('%.17e', f): 17 fractional digits
	// round-trip a float64 exactly, so text equality here IS bit
	// equality (NaN/±Inf branch first; a present ±0.0 additionally
	// pins its sign via signbit — the -0.0 case).
	const floatProj = `CASE WHEN f IS NULL THEN NULL WHEN isnan(f) THEN 'NaN' WHEN isinf(f) AND f > 0 THEN 'Inf' WHEN isinf(f) THEN '-Inf' ELSE printf('%.17e', f) END`
	nrow := func(id int, b any, i, u, f any, sb any) map[string]any {
		return map[string]any{"id": id, "b": b, "i": i, "u": u, "f": f, "f_signbit": sb}
	}
	return []check{
		{
			Name: "native/values",
			Query: `SELECT id, b, CAST(i AS VARCHAR) AS i, CAST(u AS VARCHAR) AS u, ` + floatProj + ` AS f, ` +
				`CASE WHEN f = 0 THEN signbit(f) END AS f_signbit FROM read_parquet('native.parquet') ORDER BY id;`,
			Want: []map[string]any{
				nrow(1, true, "-9223372036854775808", "18446744073709551615", "NaN", nil),
				nrow(2, false, "-1", "0", "Inf", nil),
				nrow(3, nil, "0", nil, "-Inf", nil),
				nrow(4, true, "1", "1", "-0.00000000000000000e+00", true),
				nrow(5, nil, "9223372036854775807", "9223372036854775808", "0.00000000000000000e+00", false),
				nrow(6, false, nil, "42", "4.94065645841246544e-324", nil),
				nrow(7, true, "-8388608", nil, "1.79769313486231571e+308", nil),
				nrow(8, nil, "42", "18446744073709551614", "1.50000000000000000e+00", nil),
				nrow(9, true, nil, "7", nil, nil),
			},
		},
		{
			// UBIGINT is the load-bearing one: without the UINT_64
			// annotation surviving, uint64 max reads as -1.
			Name:  "native/types",
			Query: `SELECT typeof(id) AS id, typeof(b) AS b, typeof(i) AS i, typeof(u) AS u, typeof(f) AS f FROM read_parquet('native.parquet') LIMIT 1;`,
			Want:  []map[string]any{{"id": "BIGINT", "b": "BOOLEAN", "i": "BIGINT", "u": "UBIGINT", "f": "DOUBLE"}},
		},
		{
			// One row group per source chunk (the ADR-0164 alignment
			// contract), as an external reader counts them.
			Name:  "native/row-groups",
			Query: `SELECT DISTINCT row_group_id, row_group_num_rows FROM parquet_metadata('native.parquet') ORDER BY row_group_id;`,
			Want: []map[string]any{
				{"row_group_id": 0, "row_group_num_rows": 3},
				{"row_group_id": 1, "row_group_num_rows": 3},
				{"row_group_id": 2, "row_group_num_rows": 3},
			},
		},
		{
			Name:  "native/kv-metadata",
			Query: `SELECT decode(key) AS key, decode(value) AS value FROM parquet_kv_metadata('native.parquet') WHERE decode(key) LIKE 'sluice:%' ORDER BY decode(key);`,
			Want: []map[string]any{
				{"key": "sluice:format", "value": "1"},
				{"key": "sluice:table", "value": "native"},
			},
		},
	}
}

// ---------------------------------------------------------------------
// strs: every string-leaf family member, verbatim text + empty-vs-NULL.
// ---------------------------------------------------------------------

func strsSpec() tableSpec {
	table := &ir.Table{Name: "strs", Columns: []*ir.Column{
		col("id", ir.Integer{Width: 64}),
		col("ch", ir.Char{Length: 5}),
		col("vc", ir.Varchar{Length: 40}),
		col("txt", ir.Text{}),
		col("uid", ir.UUID{}),
		col("ine", ir.Inet{}),
		col("cid", ir.Cidr{}),
		col("mac", ir.Macaddr{}),
		col("en", ir.Enum{Values: []string{"red", "green"}}),
		col("bt", ir.Bit{Length: 5}),
		col("itv", ir.Interval{}),
		col("ttz", ir.Time{WithTimeZone: true}),
		col("ext", ir.ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{3}}),
		col("dom", ir.Domain{Name: "email", BaseType: ir.Text{}}),
	}}
	rows := []ir.Row{
		{
			"id": int64(1), "ch": "abc", "vc": "héllo→ wörld", "txt": "text",
			"uid": "01234567-89ab-cdef-0123-456789abcdef", "ine": "2001:db8::1",
			"cid": "192.168.1.0/24", "mac": "08:00:2b:01:02:03", "en": "red",
			"bt": "10101", "itv": "838:59:59", "ttz": "08:30:00+02",
			"ext": "[1,2,3]", "dom": "a@b",
		},
		{
			// txt = "" is the empty-vs-NULL row: an empty string must
			// read back PRESENT under DuckDB, never as NULL.
			"id": int64(2), "ch": "xy", "vc": "plain", "txt": "",
			"uid": nil, "ine": "192.168.0.1", "cid": nil, "mac": nil,
			"en": "green", "bt": "0", "itv": "1 day", "ttz": nil,
			"ext": nil, "dom": nil,
		},
		{
			"id": int64(3), "ch": nil, "vc": nil, "txt": nil, "uid": nil, "ine": nil,
			"cid": nil, "mac": nil, "en": nil, "bt": nil, "itv": nil, "ttz": nil,
			"ext": nil, "dom": nil,
		},
		{
			// The adversarial-text row: CHAR with trailing spaces (the
			// PAD-SPACE edge — the padding is data and must survive
			// verbatim), a 4-byte astral emoji + a DECOMPOSED combining
			// sequence (must not be unicode-normalized), and an embedded
			// NUL byte (MySQL text allows U+0000; the byte is data).
			"id": int64(4), "ch": "ab  ", "vc": "\U0001F980é", "txt": "a\x00b",
			"uid": nil, "ine": nil, "cid": nil, "mac": nil, "en": nil,
			"bt": nil, "itv": nil, "ttz": nil, "ext": nil, "dom": nil,
		},
	}
	return tableSpec{name: "strs", table: table, chunks: [][]ir.Row{rows}}
}

func strsChecks() []check {
	return []check{
		{
			Name:  "strs/values",
			Query: `SELECT id, ch, vc, txt, uid, ine, cid, mac, en, bt, itv, ttz, ext, dom FROM read_parquet('strs.parquet') ORDER BY id;`,
			Want: []map[string]any{
				{
					"id": 1, "ch": "abc", "vc": "héllo→ wörld", "txt": "text",
					"uid": "01234567-89ab-cdef-0123-456789abcdef", "ine": "2001:db8::1",
					"cid": "192.168.1.0/24", "mac": "08:00:2b:01:02:03", "en": "red",
					"bt": "10101", "itv": "838:59:59", "ttz": "08:30:00+02",
					"ext": "[1,2,3]", "dom": "a@b",
				},
				{
					"id": 2, "ch": "xy", "vc": "plain", "txt": "",
					"uid": nil, "ine": "192.168.0.1", "cid": nil, "mac": nil,
					"en": "green", "bt": "0", "itv": "1 day", "ttz": nil,
					"ext": nil, "dom": nil,
				},
				{
					"id": 3, "ch": nil, "vc": nil, "txt": nil, "uid": nil, "ine": nil,
					"cid": nil, "mac": nil, "en": nil, "bt": nil, "itv": nil, "ttz": nil,
					"ext": nil, "dom": nil,
				},
				{
					"id": 4, "ch": "ab  ", "vc": "\U0001F980é", "txt": "a\x00b",
					"uid": nil, "ine": nil, "cid": nil, "mac": nil, "en": nil,
					"bt": nil, "itv": nil, "ttz": nil, "ext": nil, "dom": nil,
				},
			},
		},
		{
			// Empty string vs NULL, asserted structurally rather than
			// through JSON rendering alone. Row 4 additionally pins the
			// adversarial-text bytes structurally: the trailing-space CHAR
			// keeps its padding (octet_length 4), the decomposed 🦀e+U+0301
			// sequence keeps all 7 bytes (no unicode normalization), and
			// the NUL-bearing txt keeps its 3 bytes (no C-string
			// truncation at the NUL).
			Name: "strs/empty-vs-null",
			Query: `SELECT id, txt IS NULL AS is_null, strlen(txt) AS len, strlen(ch) AS ch_len, ` +
				`strlen(vc) AS vc_len FROM read_parquet('strs.parquet') ORDER BY id;`,
			Want: []map[string]any{
				{"id": 1, "is_null": false, "len": 4, "ch_len": 3, "vc_len": 16},
				{"id": 2, "is_null": false, "len": 0, "ch_len": 2, "vc_len": 5},
				{"id": 3, "is_null": true, "len": nil, "ch_len": nil, "vc_len": nil},
				{"id": 4, "is_null": false, "len": 3, "ch_len": 4, "vc_len": 7},
			},
		},
		{
			// The enum value list rides the footer metadata; external
			// tooling must be able to recover it.
			Name:  "strs/kv-enum-values",
			Query: `SELECT decode(value) AS value FROM parquet_kv_metadata('strs.parquet') WHERE decode(key) = 'sluice:enum_values';`,
			Want:  []map[string]any{{"value": `{"en":["red","green"]}`}},
		},
	}
}

// ---------------------------------------------------------------------
// temporal: DATE (incl. year 1 and pre-epoch), naive + tz µs
// timestamps, time-of-day.
// ---------------------------------------------------------------------

func temporalSpec() tableSpec {
	table := &ir.Table{Name: "temporal", Columns: []*ir.Column{
		col("id", ir.Integer{Width: 64}),
		col("d", ir.Date{}),
		col("ts", ir.DateTime{Precision: 6}),
		col("tz", ir.Timestamp{Precision: 6, WithTimeZone: true}),
		col("tod", ir.Time{Precision: 6}),
	}}
	rows := []ir.Row{
		{
			"id": int64(1), "d": date(1970, 1, 1),
			"ts":  time.Date(2026, 7, 15, 12, 34, 56, 123456000, time.UTC),
			"tz":  time.Date(2026, 7, 15, 12, 34, 56, 123456000, time.UTC),
			"tod": "00:00:00",
		},
		{
			"id": int64(2), "d": date(1969, 12, 31),
			"ts":  time.Date(1899, 12, 31, 23, 59, 59, 999999000, time.UTC),
			"tz":  time.Date(1899, 12, 31, 23, 59, 59, 999999000, time.UTC),
			"tod": "23:59:59.999999",
		},
		{
			"id": int64(3), "d": date(2026, 7, 15),
			"ts":  time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			"tz":  time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			"tod": "08:30:00.5",
		},
		{
			"id": int64(4), "d": date(1, 1, 1),
			"ts":  time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
			"tz":  time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
			"tod": "12:00:00.000001",
		},
		{"id": int64(5), "d": nil, "ts": nil, "tz": nil, "tod": nil},
	}
	return tableSpec{name: "temporal", table: table, chunks: [][]ir.Row{rows}}
}

func temporalChecks() []check {
	return []check{
		{
			// tz renders with the +00 suffix because script.sql SETs
			// TimeZone='UTC' — the isAdjustedToUTC annotation is what
			// makes DuckDB treat it as an instant at all.
			Name:  "temporal/values",
			Query: `SELECT id, CAST(d AS VARCHAR) AS d, CAST(ts AS VARCHAR) AS ts, CAST(tz AS VARCHAR) AS tz, CAST(tod AS VARCHAR) AS tod FROM read_parquet('temporal.parquet') ORDER BY id;`,
			Want: []map[string]any{
				{"id": 1, "d": "1970-01-01", "ts": "2026-07-15 12:34:56.123456", "tz": "2026-07-15 12:34:56.123456+00", "tod": "00:00:00"},
				{"id": 2, "d": "1969-12-31", "ts": "1899-12-31 23:59:59.999999", "tz": "1899-12-31 23:59:59.999999+00", "tod": "23:59:59.999999"},
				{"id": 3, "d": "2026-07-15", "ts": "1970-01-01 00:00:00", "tz": "1970-01-01 00:00:00+00", "tod": "08:30:00.5"},
				{"id": 4, "d": "0001-01-01", "ts": "0001-01-01 00:00:00", "tz": "0001-01-01 00:00:00+00", "tod": "12:00:00.000001"},
				{"id": 5, "d": nil, "ts": nil, "tz": nil, "tod": nil},
			},
		},
		{
			Name:  "temporal/types",
			Query: `SELECT typeof(d) AS d, typeof(ts) AS ts, typeof(tz) AS tz, typeof(tod) AS tod FROM read_parquet('temporal.parquet') LIMIT 1;`,
			Want:  []map[string]any{{"d": "DATE", "ts": "TIMESTAMP", "tz": "TIMESTAMP WITH TIME ZONE", "tod": "TIME"}},
		},
	}
}

// ---------------------------------------------------------------------
// decimals: all three DECIMAL physical tiers (INT32 / INT64 / FLBA16)
// plus the unbounded-NUMERIC string fallback.
// ---------------------------------------------------------------------

func decimalsSpec() tableSpec {
	table := &ir.Table{Name: "decimals", Columns: []*ir.Column{
		col("id", ir.Integer{Width: 64}),
		col("d9", ir.Decimal{Precision: 9, Scale: 2}),
		col("d18", ir.Decimal{Precision: 18, Scale: 6}),
		col("d38", ir.Decimal{Precision: 38, Scale: 10}),
		col("dstr", ir.Decimal{Unconstrained: true}),
	}}
	rows := []ir.Row{
		{
			"id": int64(1), "d9": "1234567.89", "d18": "999999999999.999999",
			"d38":  "9999999999999999999999999999.9999999999",
			"dstr": "123456789012345678901234567890123456789012345.67",
		},
		{
			"id": int64(2), "d9": "-0.01", "d18": "-999999999999.999999",
			"d38": "-1.0000000001",
			// PG NUMERIC NaN has no DECIMAL form; the unbounded column's
			// string downgrade carries it verbatim.
			"dstr": "NaN",
		},
		{
			"id": int64(3), "d9": "0.10", "d18": "0.000001", "d38": "0",
			"dstr": "-0.000000000000000000000000000001",
		},
		{"id": int64(4), "d9": nil, "d18": nil, "d38": nil, "dstr": nil},
	}
	return tableSpec{name: "decimals", table: table, chunks: [][]ir.Row{rows}}
}

func decimalsChecks() []check {
	return []check{
		{
			Name:  "decimals/values",
			Query: `SELECT id, CAST(d9 AS VARCHAR) AS d9, CAST(d18 AS VARCHAR) AS d18, CAST(d38 AS VARCHAR) AS d38, dstr FROM read_parquet('decimals.parquet') ORDER BY id;`,
			Want: []map[string]any{
				{
					"id": 1, "d9": "1234567.89", "d18": "999999999999.999999",
					"d38":  "9999999999999999999999999999.9999999999",
					"dstr": "123456789012345678901234567890123456789012345.67",
				},
				{"id": 2, "d9": "-0.01", "d18": "-999999999999.999999", "d38": "-1.0000000001", "dstr": "NaN"},
				{"id": 3, "d9": "0.10", "d18": "0.000001", "d38": "0.0000000000", "dstr": "-0.000000000000000000000000000001"},
				{"id": 4, "d9": nil, "d18": nil, "d38": nil, "dstr": nil},
			},
		},
		{
			// The three physical tiers must decode as real DECIMALs of
			// the declared precision/scale; the unbounded fallback is a
			// plain VARCHAR by design (documented downgrade).
			Name:  "decimals/types",
			Query: `SELECT typeof(d9) AS d9, typeof(d18) AS d18, typeof(d38) AS d38, typeof(dstr) AS dstr FROM read_parquet('decimals.parquet') LIMIT 1;`,
			Want:  []map[string]any{{"d9": "DECIMAL(9,2)", "d18": "DECIMAL(18,6)", "d38": "DECIMAL(38,10)", "dstr": "VARCHAR"}},
		},
	}
}

// ---------------------------------------------------------------------
// jsonb: JSON bodies (incl. the present-"null" body), raw bytes with
// empty-vs-NULL, WKB geometry + the GeoParquet footer block.
// ---------------------------------------------------------------------

// wkbPoint12 is POINT(1 2) as little-endian WKB.
var wkbPoint12 = []byte{
	0x01, 0x01, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
}

func jsonbSpec() tableSpec {
	table := &ir.Table{Name: "jsonb", Columns: []*ir.Column{
		col("id", ir.Integer{Width: 64}),
		col("j", ir.JSON{}),
		col("bl", ir.Blob{}),
		col("geo", ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326}),
	}}
	rows := []ir.Row{
		{"id": int64(1), "j": []byte(`{"a": 1, "b": [true, null]}`), "bl": []byte{0x00, 0x01, 0xFF}, "geo": wkbPoint12},
		{"id": int64(2), "j": `"str"`, "bl": []byte{}, "geo": nil},
		// A JSON body of `null` is a PRESENT SQL value, distinct from
		// SQL NULL — the distinction must survive an external reader.
		{"id": int64(3), "j": []byte("null"), "bl": nil, "geo": nil},
		{"id": int64(4), "j": nil, "bl": nil, "geo": nil},
		// 2^53+1 inside a JSON body: any reader stage that routes JSON
		// numbers through float64 lands …992 — the exact digits are the
		// assertion. The blob's TRAILING 0x00 is the pad-strip class:
		// trailing NULs are data and must survive byte-exact.
		{"id": int64(5), "j": []byte(`{"n": 9007199254740993}`), "bl": []byte{0x01, 0x00, 0x00}, "geo": nil},
	}
	return tableSpec{name: "jsonb", table: table, chunks: [][]ir.Row{rows}}
}

func jsonbChecks() []check {
	return []check{
		{
			// geo is projected via ST_AsText because DuckDB's spatial
			// extension AUTO-DETECTS the GeoParquet footer block and
			// decodes the column as a real GEOMETRY — the strongest
			// external proof the geo metadata + WKB bytes work together.
			Name:  "jsonb/values",
			Query: `SELECT id, CAST(j AS VARCHAR) AS j, j IS NULL AS j_null, hex(bl) AS bl, bl IS NULL AS bl_null, ST_AsText(geo) AS geo FROM read_parquet('jsonb.parquet') ORDER BY id;`,
			Want: []map[string]any{
				{"id": 1, "j": `{"a": 1, "b": [true, null]}`, "j_null": false, "bl": "0001FF", "bl_null": false, "geo": "POINT (1 2)"},
				{"id": 2, "j": `"str"`, "j_null": false, "bl": "", "bl_null": false, "geo": nil},
				{"id": 3, "j": "null", "j_null": false, "bl": nil, "bl_null": true, "geo": nil},
				{"id": 4, "j": nil, "j_null": true, "bl": nil, "bl_null": true, "geo": nil},
				{"id": 5, "j": `{"n": 9007199254740993}`, "j_null": false, "bl": "010000", "bl_null": false, "geo": nil},
			},
		},
		{
			// The GeoParquet block must reach external readers with the
			// WKB encoding + a present crs (EPSG:4326's PROJJSON here).
			Name:  "jsonb/kv-geo",
			Query: `SELECT decode(value) LIKE '%"encoding":"WKB"%' AS wkb, decode(value) LIKE '%"code":4326%' AS crs4326, decode(value) LIKE '%"primary_column":"geo"%' AS pcol FROM parquet_kv_metadata('jsonb.parquet') WHERE decode(key) = 'geo';`,
			Want:  []map[string]any{{"wkb": true, "crs4326": true, "pcol": true}},
		},
	}
}

// ---------------------------------------------------------------------
// lists: SET → LIST<STRING> plus every array element family × {values
// incl. NULL element, empty list, NULL list}.
// ---------------------------------------------------------------------

func listsSpec() tableSpec {
	table := &ir.Table{Name: "lists", Columns: []*ir.Column{
		col("id", ir.Integer{Width: 64}),
		col("st", ir.Set{Values: []string{"a", "b", "c"}}),
		col("ai", ir.Array{Element: ir.Integer{Width: 64}}),
		col("au", ir.Array{Element: ir.Integer{Width: 64, Unsigned: true}}),
		col("af", ir.Array{Element: ir.Float{Precision: ir.FloatDouble}}),
		col("atext", ir.Array{Element: ir.Text{}}),
		col("ad", ir.Array{Element: ir.Decimal{Precision: 9, Scale: 2}}),
		col("ad18", ir.Array{Element: ir.Decimal{Precision: 18, Scale: 6}}),
		col("ad38", ir.Array{Element: ir.Decimal{Precision: 38, Scale: 10}}),
		col("atz", ir.Array{Element: ir.Timestamp{Precision: 6, WithTimeZone: true}}),
		col("ats", ir.Array{Element: ir.DateTime{Precision: 6}}),
		col("adate", ir.Array{Element: ir.Date{}}),
		col("atod", ir.Array{Element: ir.Time{Precision: 6}}),
		col("ab", ir.Array{Element: ir.Boolean{}}),
		col("abl", ir.Array{Element: ir.Blob{}}),
		col("aj", ir.Array{Element: ir.JSON{}}),
	}}
	rows := []ir.Row{
		{
			"id":    int64(1),
			"st":    []string{"a", "c"},
			"ai":    []any{int64(1), nil, int64(-3), int64(0)},
			"au":    []any{uint64(math.MaxUint64), nil, uint64(0)},
			"af":    []any{1.5, nil, math.Inf(1)},
			"atext": []any{"a", nil, ""},
			"ad":    []any{"1.23", nil, "-0.01"},
			"ad18":  []any{"999999999999.999999", nil, "-0.000001"},
			"ad38":  []any{"1.2345678901", nil},
			"atz":   []any{time.Date(2026, 7, 15, 1, 2, 3, 4000, time.UTC), nil},
			"ats":   []any{time.Date(1899, 12, 31, 23, 59, 59, 999999000, time.UTC), nil},
			"adate": []any{date(1969, 12, 31), nil},
			"atod":  []any{"08:00:00.25", nil},
			"ab":    []any{true, nil, false},
			"abl":   []any{[]byte{0x01, 0x00}, nil, []byte{}},
			"aj":    []any{[]byte(`{"a": 1}`), nil},
		},
		{
			// Empty lists are PRESENT empties, distinct from NULL lists.
			"id": int64(2), "st": []string{}, "ai": []any{}, "au": []any{}, "af": []any{},
			"atext": []any{}, "ad": []any{}, "ad18": []any{}, "ad38": []any{}, "atz": []any{}, "ats": []any{},
			"adate": []any{}, "atod": []any{}, "ab": []any{}, "abl": []any{}, "aj": []any{},
		},
		{
			"id": int64(3), "st": nil, "ai": nil, "au": nil, "af": nil,
			"atext": nil, "ad": nil, "ad18": nil, "ad38": nil, "atz": nil, "ats": nil,
			"adate": nil, "atod": nil, "ab": nil, "abl": nil, "aj": nil,
		},
	}
	return tableSpec{name: "lists", table: table, chunks: [][]ir.Row{rows}}
}

func listsChecks() []check {
	// Float elements go through the same bit-exact printf projection as
	// native/values (to_json of ±Inf is not JSON, so project first).
	const afProj = `to_json(list_transform(af, lambda f: CASE WHEN f IS NULL THEN NULL WHEN isnan(f) THEN 'NaN' WHEN isinf(f) AND f > 0 THEN 'Inf' WHEN isinf(f) THEN '-Inf' ELSE printf('%.17e', f) END))`
	// BLOB elements are projected to per-element hex so the byte content
	// (incl. a TRAILING 0x00 and the empty-vs-NULL element distinction)
	// compares exactly through the JSON transport.
	const ablProj = `to_json(list_transform(abl, lambda b: CASE WHEN b IS NULL THEN NULL ELSE hex(b) END))`
	return []check{
		{
			Name: "lists/values",
			Query: `SELECT id, to_json(st) AS st, to_json(ai) AS ai, to_json(au) AS au, ` + afProj + ` AS af, ` +
				`to_json(atext) AS atext, to_json(ad) AS ad, to_json(ad18) AS ad18, to_json(ad38) AS ad38, ` +
				`to_json(atz) AS atz, to_json(ats) AS ats, to_json(adate) AS adate, to_json(atod) AS atod, ` +
				`to_json(ab) AS ab, ` + ablProj + ` AS abl, to_json(aj) AS aj ` +
				`FROM read_parquet('lists.parquet') ORDER BY id;`,
			Want: []map[string]any{
				{
					"id":    1,
					"st":    []any{"a", "c"},
					"ai":    []any{1, nil, -3, 0},
					"au":    []any{uint64(math.MaxUint64), nil, 0},
					"af":    []any{"1.50000000000000000e+00", nil, "Inf"},
					"atext": []any{"a", nil, ""},
					"ad":    []any{1.23, nil, -0.01},
					"ad18":  []any{json.Number("999999999999.999999"), nil, json.Number("-0.000001")},
					"ad38":  []any{1.2345678901, nil},
					"atz":   []any{"2026-07-15 01:02:03.000004+00", nil},
					"ats":   []any{"1899-12-31 23:59:59.999999", nil},
					"adate": []any{"1969-12-31", nil},
					"atod":  []any{"08:00:00.25", nil},
					"ab":    []any{true, nil, false},
					"abl":   []any{"0100", nil, ""},
					"aj":    []any{map[string]any{"a": 1}, nil},
				},
				{
					"id": 2, "st": []any{}, "ai": []any{}, "au": []any{}, "af": []any{},
					"atext": []any{}, "ad": []any{}, "ad18": []any{}, "ad38": []any{}, "atz": []any{},
					"ats": []any{}, "adate": []any{}, "atod": []any{}, "ab": []any{}, "abl": []any{}, "aj": []any{},
				},
				{
					"id": 3, "st": nil, "ai": nil, "au": nil, "af": nil,
					"atext": nil, "ad": nil, "ad18": nil, "ad38": nil, "atz": nil,
					"ats": nil, "adate": nil, "atod": nil, "ab": nil, "abl": nil, "aj": nil,
				},
			},
		},
		{
			// Every array element leaf must decode as the RIGHT DuckDB
			// list type — UBIGINT[] and DECIMAL(p,s)[] are the load-bearing
			// annotations (a dropped UINT_64/DECIMAL annotation would
			// silently reinterpret the raw physical values).
			Name: "lists/element-types",
			Query: `SELECT typeof(st) AS st, typeof(ai) AS ai, typeof(au) AS au, typeof(af) AS af, ` +
				`typeof(atext) AS atext, typeof(ad) AS ad, typeof(ad18) AS ad18, typeof(ad38) AS ad38, ` +
				`typeof(atz) AS atz, typeof(ats) AS ats, typeof(adate) AS adate, typeof(atod) AS atod, ` +
				`typeof(ab) AS ab, typeof(abl) AS abl, typeof(aj) AS aj FROM read_parquet('lists.parquet') LIMIT 1;`,
			Want: []map[string]any{{
				"st": "VARCHAR[]", "ai": "BIGINT[]", "au": "UBIGINT[]", "af": "DOUBLE[]",
				"atext": "VARCHAR[]", "ad": "DECIMAL(9,2)[]", "ad18": "DECIMAL(18,6)[]", "ad38": "DECIMAL(38,10)[]",
				"atz": "TIMESTAMP WITH TIME ZONE[]", "ats": "TIMESTAMP[]", "adate": "DATE[]", "atod": "TIME[]",
				"ab": "BOOLEAN[]", "abl": "BLOB[]", "aj": "JSON[]",
			}},
		},
		{
			Name:  "lists/kv-set-values",
			Query: `SELECT decode(value) AS value FROM parquet_kv_metadata('lists.parquet') WHERE decode(key) = 'sluice:set_values';`,
			Want:  []map[string]any{{"value": `{"st":["a","b","c"]}`}},
		},
	}
}
