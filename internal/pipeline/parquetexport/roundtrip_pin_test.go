// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package parquetexport

// The ADR-0164 value-fidelity pin matrix (the Bug-74 discipline): the
// IR→Parquet encoder dispatches on the type FAMILY, so every family —
// and every shape variant (value / NULL / empty-vs-NULL / negative /
// extreme / refusal) — is pinned here, ground-truthed by reading the
// produced bytes back with a real Parquet reader (parquet-go's file
// reader) at the RAW VALUE level, not just through sluice's own
// encoder. A green test on one family does not cover its siblings.

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"sluicesync.dev/sluice/internal/ir"
)

// oneColTable builds a single-column table named "t" with column "v".
func oneColTable(typ ir.Type) *ir.Table {
	return &ir.Table{Name: "t", Columns: []*ir.Column{{Name: "v", Type: typ, Nullable: true}}}
}

// row wraps a single "v" value.
func row(v any) ir.Row { return ir.Row{"v": v} }

// encodeParquet runs rows through a fresh TableCodec + the same
// GenericWriter shape the exporter uses, returning the file bytes and
// the codec (for Notes/Metadata assertions).
func encodeParquet(t *testing.T, table *ir.Table, rows []ir.Row) ([]byte, *TableCodec) {
	t.Helper()
	tc, err := NewTableCodec(table)
	if err != nil {
		t.Fatalf("NewTableCodec: %v", err)
	}
	var buf bytes.Buffer
	opts := make([]parquet.WriterOption, 0, 2+len(tc.Metadata))
	opts = append(opts, parquet.WriterOption(tc.Schema), parquet.Compression(&parquet.Zstd))
	for k, v := range tc.Metadata {
		opts = append(opts, parquet.KeyValueMetadata(k, v))
	}
	w := parquet.NewGenericWriter[map[string]any](&buf, opts...)
	for i, r := range rows {
		enc, err := tc.EncodeRow(r)
		if err != nil {
			t.Fatalf("EncodeRow(%d): %v", i, err)
		}
		if _, err := w.Write([]map[string]any{enc}); err != nil {
			t.Fatalf("Write(%d): %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes(), tc
}

// encodeErr asserts that encoding one row REFUSES, returning the error.
func encodeErr(t *testing.T, table *ir.Table, r ir.Row) error {
	t.Helper()
	tc, err := NewTableCodec(table)
	if err != nil {
		t.Fatalf("NewTableCodec: %v", err)
	}
	_, err = tc.EncodeRow(r)
	if err == nil {
		t.Fatalf("EncodeRow(%v) succeeded; want a loud refusal", r["v"])
	}
	return err
}

func openParquet(t *testing.T, data []byte) *parquet.File {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	return f
}

// rawLeafValues reads back the raw parquet.Values (including nulls) of
// the leaf column at path, across every row group — the ground truth a
// downstream Parquet reader sees.
func rawLeafValues(t *testing.T, data []byte, path ...string) []parquet.Value {
	t.Helper()
	f := openParquet(t, data)
	lc, ok := f.Schema().Lookup(path...)
	if !ok {
		t.Fatalf("column %v not found in schema %v", path, f.Schema())
	}
	var out []parquet.Value
	for _, rg := range f.RowGroups() {
		rows := rg.Rows()
		buf := make([]parquet.Row, 16)
		for {
			n, err := rows.ReadRows(buf)
			for _, r := range buf[:n] {
				for _, v := range r {
					if v.Column() == lc.ColumnIndex {
						out = append(out, v.Clone())
					}
				}
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				_ = rows.Close()
				t.Fatalf("ReadRows: %v", err)
			}
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("rows.Close: %v", err)
		}
	}
	return out
}

// readBack reconstructs rows as map[string]any via parquet-go's
// generic reader — used for LIST-shaped assertions where raw
// repetition levels would obscure the semantic content.
func readBack(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	f := openParquet(t, data)
	r := parquet.NewGenericReader[map[string]any](bytes.NewReader(data), f.Schema())
	defer func() { _ = r.Close() }()
	out := make([]map[string]any, f.NumRows())
	for i := range out {
		out[i] = map[string]any{}
	}
	if len(out) == 0 {
		return out
	}
	n, err := r.Read(out)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("Read: %v", err)
	}
	if int64(n) != f.NumRows() {
		t.Fatalf("Read returned %d rows; want %d", n, f.NumRows())
	}
	return out
}

// logicalTypeOf returns the leaf node at path for logical-type
// assertions.
func logicalTypeOf(t *testing.T, data []byte, path ...string) leafInfo {
	t.Helper()
	f := openParquet(t, data)
	lc, ok := f.Schema().Lookup(path...)
	if !ok {
		t.Fatalf("column %v not found", path)
	}
	return leafInfo{node: lc.Node}
}

type leafInfo struct{ node parquet.Node }

func TestPin_BooleanFamily(t *testing.T) {
	data, _ := encodeParquet(t, oneColTable(ir.Boolean{}), []ir.Row{row(true), row(false), row(nil)})
	vals := rawLeafValues(t, data, "v")
	if len(vals) != 3 || vals[0].Boolean() != true || vals[1].Boolean() != false || !vals[2].IsNull() {
		t.Fatalf("boolean round-trip = %v", vals)
	}
	// The zero-value trap (parquet-go isNullValue collapses Go zero
	// values to NULL without the pointer boxing): false must be a
	// PRESENT false, not NULL — a null's Boolean() also reads false,
	// so this pin is what actually distinguishes them.
	if vals[1].IsNull() {
		t.Fatal("false exported as NULL (the zero-value-as-null trap)")
	}
	if err := encodeErr(t, oneColTable(ir.Boolean{}), row(int64(1))); !strings.Contains(err.Error(), "contract") {
		t.Fatalf("non-bool refusal message: %v", err)
	}
}

func TestPin_SignedIntegerFamily(t *testing.T) {
	// Every declared width shares the INT64 mapping; pin each width's
	// extreme so a future per-width dispatch can't silently narrow.
	cases := []struct {
		width int8
		val   int64
	}{
		{8, -128}, {16, -32768}, {24, -8388608}, {32, -2147483648}, {64, math.MinInt64}, {64, math.MaxInt64}, {64, 0},
	}
	for _, tc := range cases {
		data, _ := encodeParquet(t, oneColTable(ir.Integer{Width: tc.width}), []ir.Row{row(tc.val), row(nil)})
		vals := rawLeafValues(t, data, "v")
		if len(vals) != 2 || vals[0].Int64() != tc.val || !vals[1].IsNull() {
			t.Fatalf("width %d: round-trip = %v; want [%d NULL]", tc.width, vals, tc.val)
		}
		// Zero-value trap pin: 0 is a PRESENT 0, never NULL.
		if vals[0].IsNull() {
			t.Fatalf("width %d: %d exported as NULL", tc.width, tc.val)
		}
	}
	if err := encodeErr(t, oneColTable(ir.Integer{Width: 64}), row(uint64(1))); !strings.Contains(err.Error(), "contract") {
		t.Fatalf("uint64-in-signed refusal: %v", err)
	}
}

func TestPin_UnsignedIntegerFamily(t *testing.T) {
	table := oneColTable(ir.Integer{Width: 64, Unsigned: true})
	data, _ := encodeParquet(t, table, []ir.Row{
		row(uint64(math.MaxUint64)), // the >MaxInt64 contract shape
		row(int64(42)),              // the fits-in-int64 contract shape
		row(uint64(0)),              // zero-value trap: PRESENT 0, never NULL
		row(nil),
	})
	vals := rawLeafValues(t, data, "v")
	if len(vals) != 4 {
		t.Fatalf("len = %d", len(vals))
	}
	if got := uint64(vals[0].Int64()); got != math.MaxUint64 {
		t.Fatalf("uint64 max round-trip = %d", got)
	}
	if got := uint64(vals[1].Int64()); got != 42 {
		t.Fatalf("small unsigned round-trip = %d", got)
	}
	if vals[2].IsNull() || vals[2].Int64() != 0 {
		t.Fatalf("uint64 zero = %v; want present 0", vals[2])
	}
	if !vals[3].IsNull() {
		t.Fatal("NULL lost")
	}
	// The UINT_64 logical annotation is what tells readers the bits
	// are unsigned; without it MaxUint64 reads as -1.
	info := logicalTypeOf(t, data, "v")
	lt := info.node.Type().LogicalType()
	if lt == nil || lt.Integer == nil || lt.Integer.BitWidth != 64 || lt.Integer.IsSigned {
		t.Fatalf("logical type = %v; want unsigned INT(64)", lt)
	}
	// Negative into unsigned is an upstream bug: refuse.
	if err := encodeErr(t, table, row(int64(-1))); !strings.Contains(err.Error(), "negative") {
		t.Fatalf("negative-in-unsigned refusal: %v", err)
	}
}

func TestPin_FloatFamily(t *testing.T) {
	for _, prec := range []ir.FloatPrecision{ir.FloatSingle, ir.FloatDouble} {
		table := oneColTable(ir.Float{Precision: prec})
		in := []float64{1.5, math.NaN(), math.Inf(1), math.Inf(-1), math.Copysign(0, -1), 0, math.MaxFloat64, 5e-324}
		rows := make([]ir.Row, 0, len(in)+1)
		for _, f := range in {
			rows = append(rows, row(f))
		}
		rows = append(rows, row(nil))
		data, _ := encodeParquet(t, table, rows)
		vals := rawLeafValues(t, data, "v")
		if len(vals) != len(in)+1 {
			t.Fatalf("prec %v: len = %d", prec, len(vals))
		}
		for i, want := range in {
			// Bit-exact comparison: NaN payloads, -0.0's sign, and
			// denormals must all survive (DOUBLE is a bit carrier) —
			// and every value is PRESENT (±0.0 are the zero-value-as-
			// null trap's canonical float victims).
			if vals[i].IsNull() {
				t.Errorf("prec %v value %d (%v): exported as NULL", prec, i, want)
				continue
			}
			if got := vals[i].Double(); math.Float64bits(got) != math.Float64bits(want) {
				t.Errorf("prec %v value %d: bits %x != %x", prec, i, math.Float64bits(got), math.Float64bits(want))
			}
		}
		if !vals[len(in)].IsNull() {
			t.Fatal("NULL lost")
		}
	}
	if err := encodeErr(t, oneColTable(ir.Float{}), row("1.5")); !strings.Contains(err.Error(), "contract") {
		t.Fatalf("string-in-float refusal: %v", err)
	}
}

// TestPin_StringLeafFamily covers every IR type that maps to a UTF8
// string leaf carrying the IR value verbatim.
func TestPin_StringLeafFamily(t *testing.T) {
	cases := []struct {
		name string
		typ  ir.Type
		val  string
	}{
		{"char", ir.Char{Length: 5}, "abc"},
		{"varchar", ir.Varchar{Length: 20}, "héllo→ wörld"},
		{"text-empty", ir.Text{}, ""}, // empty string, distinct from NULL
		{"uuid", ir.UUID{}, "01234567-89ab-cdef-0123-456789abcdef"},
		{"inet", ir.Inet{}, "2001:db8::1"},
		{"cidr", ir.Cidr{}, "192.168.1.0/24"},
		{"macaddr", ir.Macaddr{}, "08:00:2b:01:02:03"},
		{"enum", ir.Enum{Values: []string{"red", "green"}}, "red"},
		{"bit", ir.Bit{Length: 5}, "10101"},
		{"interval", ir.Interval{}, "838:59:59"},
		{"timetz", ir.Time{WithTimeZone: true}, "08:30:00+02"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, codec := encodeParquet(t, oneColTable(tc.typ), []ir.Row{row(tc.val), row(nil)})
			vals := rawLeafValues(t, data, "v")
			if len(vals) != 2 || string(vals[0].ByteArray()) != tc.val || !vals[1].IsNull() {
				t.Fatalf("round-trip = %v; want [%q NULL]", vals, tc.val)
			}
			// Zero-value trap pin: the empty string is a PRESENT ""
			// (a null's ByteArray() also reads as "").
			if vals[0].IsNull() {
				t.Fatalf("%q exported as NULL", tc.val)
			}
			info := logicalTypeOf(t, data, "v")
			lt := info.node.Type().LogicalType()
			if lt == nil || lt.UTF8 == nil {
				t.Fatalf("logical type = %v; want STRING", lt)
			}
			if tc.name == "timetz" && len(codec.Notes) == 0 {
				t.Fatal("timetz string downgrade must carry an operator-visible note")
			}
			if err := encodeErr(t, oneColTable(tc.typ), row(int64(7))); !strings.Contains(err.Error(), "contract") {
				t.Fatalf("non-string refusal: %v", err)
			}
		})
	}
}

// TestPin_BytesFamily covers the raw BYTE_ARRAY carriers.
func TestPin_BytesFamily(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  ir.Type
	}{
		{"binary", ir.Binary{Length: 3}},
		{"varbinary", ir.Varbinary{Length: 16}},
		{"blob", ir.Blob{}},
		{"geometry", ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			val := []byte{0x00, 0x01, 0xFF}
			data, codec := encodeParquet(t, oneColTable(tc.typ), []ir.Row{row(val), row([]byte{}), row(nil)})
			vals := rawLeafValues(t, data, "v")
			if len(vals) != 3 || !bytes.Equal(vals[0].ByteArray(), val) {
				t.Fatalf("round-trip = %v", vals)
			}
			// Empty []byte and NULL are DISTINCT — collapsing them is
			// silent loss.
			if vals[1].IsNull() || len(vals[1].ByteArray()) != 0 {
				t.Fatalf("empty []byte round-trip = %v; want empty non-null", vals[1])
			}
			if !vals[2].IsNull() {
				t.Fatal("NULL lost")
			}
			if tc.name == "geometry" {
				geo, ok := codec.Metadata[MetaGeo]
				if !ok || !strings.Contains(geo, `"encoding":"WKB"`) || !strings.Contains(geo, `"primary_column":"v"`) {
					t.Fatalf("geo metadata = %q", geo)
				}
			}
			if err := encodeErr(t, oneColTable(tc.typ), row("not-bytes")); !strings.Contains(err.Error(), "contract") {
				t.Fatalf("non-bytes refusal: %v", err)
			}
		})
	}
}

// TestPin_GeoParquetCRS pins the geo block's per-column `crs` stamp
// (audit MED-D0-4) across the SRID class matrix on ONE multi-geometry
// table: a bundled geographic SRID (4326), the bundled PROJECTED SRID
// (3857 — the case an omitted crs would silently misread as lon/lat
// degrees), SRID 0 (the engines' "none declared" → explicit null,
// silently — that IS the faithful translation), and an unbundled SRID
// (32633 → explicit null + an operator-visible note naming it). The
// crs key must be PRESENT for every column — omission means "OGC:CRS84
// degrees" per the spec, which is the exact silent-loss shape.
func TestPin_GeoParquetCRS(t *testing.T) {
	table := &ir.Table{Name: "t", Columns: []*ir.Column{
		{Name: "g4326", Type: ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326}, Nullable: true},
		{Name: "g3857", Type: ir.Geometry{Subtype: ir.GeometryPoint, SRID: 3857}, Nullable: true},
		{Name: "g0", Type: ir.Geometry{Subtype: ir.GeometryPoint}, Nullable: true},
		{Name: "g32633", Type: ir.Geometry{Subtype: ir.GeometryPoint, SRID: 32633}, Nullable: true},
	}}
	codec, err := NewTableCodec(table)
	if err != nil {
		t.Fatalf("NewTableCodec: %v", err)
	}
	var block struct {
		Version       string `json:"version"`
		PrimaryColumn string `json:"primary_column"`
		Columns       map[string]struct {
			Encoding string          `json:"encoding"`
			CRS      json.RawMessage `json:"crs"`
		} `json:"columns"`
	}
	raw := codec.Metadata[MetaGeo]
	if err := json.Unmarshal([]byte(raw), &block); err != nil {
		t.Fatalf("geo metadata does not parse: %v\n%s", err, raw)
	}
	if block.PrimaryColumn != "g4326" || len(block.Columns) != 4 {
		t.Fatalf("geo block = %s", raw)
	}

	// crsID extracts id.authority/id.code from a column's crs, requiring
	// the key to be PRESENT (json.RawMessage is nil only when absent).
	type crsID struct {
		ID struct {
			Authority string `json:"authority"`
			Code      int    `json:"code"`
		} `json:"id"`
		Type string `json:"type"`
	}
	col := func(name string) json.RawMessage {
		c, ok := block.Columns[name]
		if !ok {
			t.Fatalf("column %q missing from geo block: %s", name, raw)
		}
		if c.Encoding != "WKB" {
			t.Fatalf("column %q encoding = %q", name, c.Encoding)
		}
		if c.CRS == nil {
			t.Fatalf("column %q has NO crs key — the spec defaults that to OGC:CRS84 degrees (the silent-loss shape): %s", name, raw)
		}
		return c.CRS
	}

	var g4326 crsID
	if err := json.Unmarshal(col("g4326"), &g4326); err != nil || g4326.ID.Authority != "EPSG" || g4326.ID.Code != 4326 || g4326.Type != "GeographicCRS" {
		t.Fatalf("g4326 crs = %s (err %v); want PROJJSON with id EPSG:4326", col("g4326"), err)
	}
	var g3857 crsID
	if err := json.Unmarshal(col("g3857"), &g3857); err != nil || g3857.ID.Authority != "EPSG" || g3857.ID.Code != 3857 || g3857.Type != "ProjectedCRS" {
		t.Fatalf("g3857 crs = %s (err %v); want PROJJSON with id EPSG:3857", col("g3857"), err)
	}
	if got := string(col("g0")); got != "null" {
		t.Fatalf("g0 crs = %s; want explicit null (SRID 0 = undefined, NOT the CRS84 default)", got)
	}
	if got := string(col("g32633")); got != "null" {
		t.Fatalf("g32633 crs = %s; want explicit null (unbundled SRID must not imply CRS84)", got)
	}

	// The unbundled SRID is loud: exactly one note, naming column + SRID.
	// SRID 0 and the bundled SRIDs add none.
	if len(codec.Notes) != 1 || !strings.Contains(codec.Notes[0], `"g32633"`) || !strings.Contains(codec.Notes[0], "32633") {
		t.Fatalf("Notes = %v; want exactly one note naming g32633's SRID", codec.Notes)
	}
}

func TestPin_JSONFamily(t *testing.T) {
	for _, binary := range []bool{false, true} {
		table := oneColTable(ir.JSON{Binary: binary})
		val := []byte(`{"a": 1, "b": [true, null]}`)
		data, _ := encodeParquet(t, table, []ir.Row{row(val), row("\"str\""), row([]byte("null")), row(nil)})
		vals := rawLeafValues(t, data, "v")
		if len(vals) != 4 || !bytes.Equal(vals[0].ByteArray(), val) || string(vals[1].ByteArray()) != "\"str\"" || !vals[3].IsNull() {
			t.Fatalf("binary=%v round-trip = %v", binary, vals)
		}
		// A JSON body of `null` is a PRESENT SQL value, distinct from
		// SQL NULL. Tripwire: parquet-go special-cases
		// json.RawMessage("null") → parquet NULL in its map
		// deconstruction; the encoder emits plain []byte so it is safe
		// today, but a refactor to json.RawMessage would silently
		// collapse this value — this pin is what catches it.
		if vals[2].IsNull() || string(vals[2].ByteArray()) != "null" {
			t.Fatalf(`binary=%v JSON body null = %v; want a PRESENT "null" body, not SQL NULL`, binary, vals[2])
		}
		info := logicalTypeOf(t, data, "v")
		if lt := info.node.Type().LogicalType(); lt == nil || lt.Json == nil {
			t.Fatalf("logical type = %v; want JSON", lt)
		}
	}
	if err := encodeErr(t, oneColTable(ir.JSON{}), row(map[string]any{"a": 1})); !strings.Contains(err.Error(), "contract") {
		t.Fatalf("map-in-json refusal: %v", err)
	}
}

func TestPin_DateFamily(t *testing.T) {
	day := func(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }
	table := oneColTable(ir.Date{})
	data, _ := encodeParquet(t, table, []ir.Row{
		row(day(1970, 1, 1)),
		row(day(1969, 12, 31)), // pre-epoch: floor semantics, -1 not 0
		row(day(2026, 7, 15)),
		row(day(1, 1, 1)),
		row(nil),
	})
	vals := rawLeafValues(t, data, "v")
	wants := []int32{0, -1, 20649, -719162}
	for i, want := range wants {
		if vals[i].IsNull() {
			t.Errorf("date %d exported as NULL (day 0 is the zero-value trap's victim)", i)
			continue
		}
		if got := vals[i].Int32(); got != want {
			t.Errorf("date %d = %d; want %d", i, got, want)
		}
	}
	if !vals[4].IsNull() {
		t.Fatal("NULL lost")
	}
	if lt := logicalTypeOf(t, data, "v").node.Type().LogicalType(); lt == nil || lt.Date == nil {
		t.Fatalf("logical type = %v; want DATE", lt)
	}
	// A Date value with a time-of-day violates the IR contract; a
	// silent floor would MOVE the date for some zones. Refuse.
	if err := encodeErr(t, table, row(time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC))); !strings.Contains(err.Error(), "midnight") {
		t.Fatalf("non-midnight refusal: %v", err)
	}
}

func TestPin_TimestampFamilies(t *testing.T) {
	cases := []struct {
		name         string
		typ          ir.Type
		wantAdjusted bool
	}{
		{"datetime", ir.DateTime{Precision: 6}, false},
		{"timestamp-naive", ir.Timestamp{Precision: 6}, false},
		{"timestamptz", ir.Timestamp{Precision: 6, WithTimeZone: true}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := []time.Time{
				time.Date(2026, 7, 15, 12, 34, 56, 123456000, time.UTC),
				time.Date(1899, 12, 31, 23, 59, 59, 999999000, time.UTC), // pre-epoch, negative micros
				time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			}
			rows := []ir.Row{row(in[0]), row(in[1]), row(in[2]), row(nil)}
			data, _ := encodeParquet(t, oneColTable(tc.typ), rows)
			vals := rawLeafValues(t, data, "v")
			for i, want := range in {
				if vals[i].IsNull() {
					t.Errorf("value %d exported as NULL (the epoch is the zero-value trap's victim)", i)
					continue
				}
				if got := vals[i].Int64(); got != want.UnixMicro() {
					t.Errorf("value %d = %d; want %d", i, got, want.UnixMicro())
				}
			}
			if !vals[3].IsNull() {
				t.Fatal("NULL lost")
			}
			lt := logicalTypeOf(t, data, "v").node.Type().LogicalType()
			if lt == nil || lt.Timestamp == nil || lt.Timestamp.Unit.Micros == nil {
				t.Fatalf("logical type = %v; want TIMESTAMP(MICROS)", lt)
			}
			if lt.Timestamp.IsAdjustedToUTC != tc.wantAdjusted {
				t.Fatalf("IsAdjustedToUTC = %v; want %v", lt.Timestamp.IsAdjustedToUTC, tc.wantAdjusted)
			}
			// Sub-microsecond precision would be silently truncated by
			// TIMESTAMP(MICROS): refuse.
			if err := encodeErr(t, oneColTable(tc.typ), row(time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC))); !strings.Contains(err.Error(), "sub-microsecond") {
				t.Fatalf("sub-micro refusal: %v", err)
			}
			// Beyond the int64-micros horizon UnixMicro silently wraps:
			// refuse (PG's calendar reaches further than Parquet's).
			if err := encodeErr(t, oneColTable(tc.typ), row(time.Date(294277, 1, 1, 0, 0, 0, 0, time.UTC))); !strings.Contains(err.Error(), "range") {
				t.Fatalf("out-of-range refusal: %v", err)
			}
		})
	}
}

func TestPin_TimeOfDayFamily(t *testing.T) {
	table := oneColTable(ir.Time{Precision: 6})
	data, _ := encodeParquet(t, table, []ir.Row{
		row("00:00:00"),
		row("23:59:59.999999"),
		row("08:30:00.5"), // fraction padded to micros, not mis-scaled
		row(nil),
	})
	vals := rawLeafValues(t, data, "v")
	wants := []int64{0, 86399999999, 30600500000}
	for i, want := range wants {
		if vals[i].IsNull() {
			t.Errorf("time %d exported as NULL (midnight is the zero-value trap's victim)", i)
			continue
		}
		if got := vals[i].Int64(); got != want {
			t.Errorf("time %d = %d; want %d", i, got, want)
		}
	}
	if !vals[3].IsNull() {
		t.Fatal("NULL lost")
	}
	lt := logicalTypeOf(t, data, "v").node.Type().LogicalType()
	if lt == nil || lt.Time == nil || lt.Time.Unit.Micros == nil || lt.Time.IsAdjustedToUTC {
		t.Fatalf("logical type = %v; want TIME(MICROS, isAdjustedToUTC=false)", lt)
	}
	// The refusal matrix: SQL TIME values with no time-of-day form.
	for _, bad := range []string{"-01:00:00", "24:00:00", "838:59:59", "00:00:00.1234567", "garbage"} {
		if _, err := parseTimeOfDayMicros(bad); err == nil {
			t.Errorf("parseTimeOfDayMicros(%q) succeeded; want refusal", bad)
		}
	}
	if err := encodeErr(t, table, row("24:00:00")); !strings.Contains(err.Error(), "calendar day") {
		t.Fatalf("24h refusal: %v", err)
	}
}

func TestPin_DecimalFamilies(t *testing.T) {
	t.Run("int32-backed p<=9", func(t *testing.T) {
		table := oneColTable(ir.Decimal{Precision: 9, Scale: 2})
		data, _ := encodeParquet(t, table, []ir.Row{
			row("1234567.89"), row("-0.01"), row("5"), row("0.10"), row(nil),
		})
		vals := rawLeafValues(t, data, "v")
		wants := []int32{123456789, -1, 500, 10}
		for i, want := range wants {
			if vals[i].IsNull() {
				t.Errorf("decimal %d exported as NULL", i)
				continue
			}
			if got := vals[i].Int32(); got != want {
				t.Errorf("decimal %d = %d; want %d", i, got, want)
			}
		}
		if !vals[4].IsNull() {
			t.Fatal("NULL lost")
		}
		lt := logicalTypeOf(t, data, "v").node.Type().LogicalType()
		if lt == nil || lt.Decimal == nil || lt.Decimal.Precision != 9 || lt.Decimal.Scale != 2 {
			t.Fatalf("logical type = %v; want DECIMAL(9,2)", lt)
		}
	})

	t.Run("int64-backed p<=18", func(t *testing.T) {
		table := oneColTable(ir.Decimal{Precision: 18, Scale: 6})
		data, _ := encodeParquet(t, table, []ir.Row{
			row("999999999999.999999"), row("-999999999999.999999"), row(nil),
		})
		vals := rawLeafValues(t, data, "v")
		if got := vals[0].Int64(); got != 999999999999999999 {
			t.Errorf("max = %d", got)
		}
		if got := vals[1].Int64(); got != -999999999999999999 {
			t.Errorf("min = %d", got)
		}
		if !vals[2].IsNull() {
			t.Fatal("NULL lost")
		}
	})

	t.Run("flba16-backed p<=38", func(t *testing.T) {
		table := oneColTable(ir.Decimal{Precision: 38, Scale: 10})
		pos := "9999999999999999999999999999.9999999999" // 38 nines
		neg := "-1.0000000001"
		data, _ := encodeParquet(t, table, []ir.Row{row(pos), row(neg), row("0"), row(nil)})
		vals := rawLeafValues(t, data, "v")
		wantPos, _ := new(big.Int).SetString(strings.Repeat("9", 38), 10)
		wantNeg, _ := new(big.Int).SetString("-10000000001", 10)
		for i, want := range []*big.Int{wantPos, wantNeg, big.NewInt(0)} {
			if vals[i].IsNull() {
				t.Errorf("flba %d exported as NULL", i)
				continue
			}
			got := flbaToBigInt(vals[i].ByteArray())
			if got.Cmp(want) != 0 {
				t.Errorf("flba %d = %s; want %s", i, got, want)
			}
		}
		if !vals[3].IsNull() {
			t.Fatal("NULL lost")
		}
	})

	t.Run("string fallbacks carry the exact text + a note", func(t *testing.T) {
		for name, typ := range map[string]ir.Decimal{
			"unconstrained":  {Unconstrained: true},
			"precision > 38": {Precision: 65, Scale: 2},
			"negative scale": {Precision: 10, Scale: -3},
		} {
			val := "123456789012345678901234567890123456789012345.67"
			data, codec := encodeParquet(t, oneColTable(typ), []ir.Row{row(val), row(nil)})
			vals := rawLeafValues(t, data, "v")
			if string(vals[0].ByteArray()) != val || !vals[1].IsNull() {
				t.Errorf("%s: round-trip = %v", name, vals)
			}
			if len(codec.Notes) != 1 || !strings.Contains(codec.Notes[0], "UTF8 string") {
				t.Errorf("%s: notes = %v; want one string-downgrade note", name, codec.Notes)
			}
		}
	})

	t.Run("refusals", func(t *testing.T) {
		table := oneColTable(ir.Decimal{Precision: 9, Scale: 2})
		for _, bad := range []string{"NaN", "Infinity", "-Infinity", "1.234", "12345678.99", ""} {
			if err := encodeErr(t, table, row(bad)); err == nil {
				t.Errorf("decimal %q: want refusal", bad)
			}
		}
		// A rounded representation is exactly the corruption this
		// refuses: excess NON-ZERO fraction digits.
		if err := encodeErr(t, table, row("1.239")); !strings.Contains(err.Error(), "refusing to round") {
			t.Fatalf("excess-fraction refusal: %v", err)
		}
		// Trailing zeros beyond the scale ARE lossless: accepted.
		data, _ := encodeParquet(t, table, []ir.Row{row("1.2300000")})
		if got := rawLeafValues(t, data, "v")[0].Int32(); got != 123 {
			t.Fatalf("trailing-zero fraction = %d; want 123", got)
		}
		if err := encodeErr(t, table, row(float64(1.23))); !strings.Contains(err.Error(), "contract") {
			t.Fatalf("float-in-decimal refusal: %v", err)
		}
	})
}

func TestPin_SetFamily(t *testing.T) {
	typ := ir.Set{Values: []string{"a", "b", "c"}}
	data, codec := encodeParquet(t, oneColTable(typ), []ir.Row{
		row([]string{"a", "c"}),
		row([]string{}), // empty set: non-null empty list, distinct from NULL
		row(nil),
	})
	rows := readBack(t, data)
	if got := listStrings(t, rows[0]["v"]); len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Fatalf("set round-trip = %v", rows[0]["v"])
	}
	if got := listStrings(t, rows[1]["v"]); rows[1]["v"] == nil || len(got) != 0 {
		t.Fatalf("empty set = %#v; want non-null empty list", rows[1]["v"])
	}
	if rows[2]["v"] != nil {
		t.Fatalf("NULL set = %#v", rows[2]["v"])
	}
	var meta map[string][]string
	if err := json.Unmarshal([]byte(codec.Metadata[MetaSetValues]), &meta); err != nil || len(meta["v"]) != 3 {
		t.Fatalf("set metadata = %q (%v)", codec.Metadata[MetaSetValues], err)
	}
}

func TestPin_EnumMetadata(t *testing.T) {
	_, codec := encodeParquet(t, oneColTable(ir.Enum{Values: []string{"x", "y"}}), []ir.Row{row("x")})
	var meta map[string][]string
	if err := json.Unmarshal([]byte(codec.Metadata[MetaEnumValues]), &meta); err != nil || len(meta["v"]) != 2 {
		t.Fatalf("enum metadata = %q (%v)", codec.Metadata[MetaEnumValues], err)
	}
}

// TestPin_ArrayElementFamilies is the array-specific Bug-74 matrix:
// each element FAMILY (native / string-leaf / temporal / decimal) ×
// {1-D value, NULL element, empty, NULL array, multi-dim refusal}.
func TestPin_ArrayElementFamilies(t *testing.T) {
	type check func(t *testing.T, got []any)
	cases := []struct {
		name  string
		elem  ir.Type
		val   []any
		check check
	}{
		{
			"int64",
			ir.Integer{Width: 64},
			[]any{int64(1), nil, int64(-3), int64(0)},
			func(t *testing.T, got []any) {
				if len(got) != 4 || asInt64(t, got[0]) != 1 || got[1] != nil || asInt64(t, got[2]) != -3 {
					t.Fatalf("int64 elements = %#v", got)
				}
				// A ZERO element must stay a present 0 (nil would mean
				// the zero-value-as-null trap reached list elements).
				if got[3] == nil || asInt64(t, got[3]) != 0 {
					t.Fatalf("zero element = %#v; want present 0", got[3])
				}
			},
		},
		{
			"float",
			ir.Float{Precision: ir.FloatDouble},
			[]any{1.5, nil, math.Inf(1)},
			func(t *testing.T, got []any) {
				if len(got) != 3 || got[0].(float64) != 1.5 || got[1] != nil || !math.IsInf(got[2].(float64), 1) {
					t.Fatalf("float elements = %#v", got)
				}
			},
		},
		{
			"bool",
			ir.Boolean{},
			[]any{true, nil, false},
			func(t *testing.T, got []any) {
				if len(got) != 3 || got[0].(bool) != true || got[1] != nil || got[2].(bool) != false {
					t.Fatalf("bool elements = %#v", got)
				}
			},
		},
		{
			"text",
			ir.Text{},
			[]any{"a", nil, ""},
			func(t *testing.T, got []any) {
				if len(got) != 3 || got[0].(string) != "a" || got[1] != nil || got[2].(string) != "" {
					t.Fatalf("text elements = %#v", got)
				}
			},
		},
		{
			"uuid",
			ir.UUID{},
			[]any{"01234567-89ab-cdef-0123-456789abcdef", nil},
			func(t *testing.T, got []any) {
				if len(got) != 2 || got[0].(string) != "01234567-89ab-cdef-0123-456789abcdef" || got[1] != nil {
					t.Fatalf("uuid elements = %#v", got)
				}
			},
		},
		{
			"timestamptz",
			ir.Timestamp{WithTimeZone: true},
			[]any{time.Date(2026, 7, 15, 1, 2, 3, 4000, time.UTC), nil},
			func(t *testing.T, got []any) {
				want := time.Date(2026, 7, 15, 1, 2, 3, 4000, time.UTC).UnixMicro()
				if len(got) != 2 || asInt64(t, got[0]) != want || got[1] != nil {
					t.Fatalf("timestamptz elements = %#v; want micros %d", got, want)
				}
			},
		},
		{
			"date",
			ir.Date{},
			[]any{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), nil},
			func(t *testing.T, got []any) {
				if len(got) != 2 || asInt64(t, got[0]) != -1 || got[1] != nil {
					t.Fatalf("date elements = %#v; want [-1 nil]", got)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := oneColTable(ir.Array{Element: tc.elem})
			data, _ := encodeParquet(t, table, []ir.Row{row(tc.val), row([]any{}), row(nil)})
			rows := readBack(t, data)
			got, ok := rows[0]["v"].([]any)
			if !ok {
				t.Fatalf("row 0 = %#v; want []any", rows[0]["v"])
			}
			tc.check(t, got)
			if empty, ok := rows[1]["v"].([]any); !ok || len(empty) != 0 {
				t.Fatalf("empty array = %#v; want non-null empty list", rows[1]["v"])
			}
			if rows[2]["v"] != nil {
				t.Fatalf("NULL array = %#v", rows[2]["v"])
			}
			// Multi-dim refusal per element family — the type declares
			// no dimensionality, so a nested value must refuse LOUDLY
			// (the silent alternative is Bug 74's flatten).
			nested := ir.Row{"v": []any{[]any{tc.val[0]}}}
			if err := encodeErr(t, table, nested); !strings.Contains(err.Error(), "multi-dimensional") {
				t.Fatalf("multi-dim refusal: %v", err)
			}
		})
	}

	t.Run("decimal elements keep exactness", func(t *testing.T) {
		table := oneColTable(ir.Array{Element: ir.Decimal{Precision: 9, Scale: 2}})
		data, _ := encodeParquet(t, table, []ir.Row{row([]any{"1.23", nil, "-0.01"})})
		vals := rawLeafValues(t, data, "v", "list", "element")
		if len(vals) != 3 || vals[0].Int32() != 123 || !vals[1].IsNull() || vals[2].Int32() != -1 {
			t.Fatalf("decimal elements = %v", vals)
		}
		// The Bug-74 canonical case: a NESTED numeric array must
		// refuse, never flatten.
		if err := encodeErr(t, table, row([]any{[]any{"1.23"}})); !strings.Contains(err.Error(), "multi-dimensional") {
			t.Fatalf("numeric multi-dim refusal: %v", err)
		}
	})

	t.Run("decimal int64-tier elements keep exactness", func(t *testing.T) {
		table := oneColTable(ir.Array{Element: ir.Decimal{Precision: 18, Scale: 6}})
		data, _ := encodeParquet(t, table, []ir.Row{row([]any{"999999999999.999999", nil, "0"})})
		vals := rawLeafValues(t, data, "v", "list", "element")
		if len(vals) != 3 || vals[0].Int64() != 999999999999999999 || !vals[1].IsNull() {
			t.Fatalf("int64-decimal elements = %v", vals)
		}
		if vals[2].IsNull() || vals[2].Int64() != 0 {
			t.Fatalf("zero decimal element = %v; want present 0", vals[2])
		}
	})

	t.Run("decimal flba16-tier elements keep exactness", func(t *testing.T) {
		// numeric(38,10)[] — the closest structural analog of Bug 74's
		// numeric[][] (a wide-decimal codec behind a list shape).
		table := oneColTable(ir.Array{Element: ir.Decimal{Precision: 38, Scale: 10}})
		data, _ := encodeParquet(t, table, []ir.Row{row([]any{"1.2345678901", nil, "-1.0000000001"})})
		vals := rawLeafValues(t, data, "v", "list", "element")
		if len(vals) != 3 || !vals[1].IsNull() {
			t.Fatalf("flba-decimal elements = %v", vals)
		}
		if got := flbaToBigInt(vals[0].ByteArray()); got.Cmp(big.NewInt(12345678901)) != 0 {
			t.Fatalf("flba element 0 = %s; want 12345678901", got)
		}
		if got := flbaToBigInt(vals[2].ByteArray()); got.Cmp(big.NewInt(-10000000001)) != 0 {
			t.Fatalf("flba element 2 = %s; want -10000000001", got)
		}
		if err := encodeErr(t, table, row([]any{[]any{"1.2345678901"}})); !strings.Contains(err.Error(), "multi-dimensional") {
			t.Fatalf("flba multi-dim refusal: %v", err)
		}
	})

	t.Run("bytes elements: empty element is not a NULL element", func(t *testing.T) {
		table := oneColTable(ir.Array{Element: ir.Blob{}})
		data, _ := encodeParquet(t, table, []ir.Row{row([]any{[]byte{0x01, 0x02}, []byte{}, nil})})
		vals := rawLeafValues(t, data, "v", "list", "element")
		if len(vals) != 3 || !bytes.Equal(vals[0].ByteArray(), []byte{0x01, 0x02}) {
			t.Fatalf("bytes elements = %v", vals)
		}
		if vals[1].IsNull() || len(vals[1].ByteArray()) != 0 {
			t.Fatalf("empty bytes element = %v; want present empty", vals[1])
		}
		if !vals[2].IsNull() {
			t.Fatal("NULL element lost")
		}
	})

	t.Run("json elements carry raw bytes", func(t *testing.T) {
		table := oneColTable(ir.Array{Element: ir.JSON{Binary: true}})
		data, _ := encodeParquet(t, table, []ir.Row{row([]any{[]byte(`{"a": 1}`), nil})})
		vals := rawLeafValues(t, data, "v", "list", "element")
		if len(vals) != 2 || string(vals[0].ByteArray()) != `{"a": 1}` || !vals[1].IsNull() {
			t.Fatalf("json elements = %v", vals)
		}
	})

	t.Run("time-of-day elements parse to micros", func(t *testing.T) {
		table := oneColTable(ir.Array{Element: ir.Time{Precision: 6}})
		data, _ := encodeParquet(t, table, []ir.Row{row([]any{"08:00:00.25", nil, "00:00:00"})})
		vals := rawLeafValues(t, data, "v", "list", "element")
		if len(vals) != 3 || vals[0].Int64() != 28800250000 || !vals[1].IsNull() {
			t.Fatalf("time elements = %v", vals)
		}
		if vals[2].IsNull() || vals[2].Int64() != 0 {
			t.Fatalf("midnight element = %v; want present 0", vals[2])
		}
		if err := encodeErr(t, table, row([]any{"24:00:00"})); !strings.Contains(err.Error(), "calendar day") {
			t.Fatalf("out-of-day element refusal: %v", err)
		}
	})

	t.Run("unsigned elements keep the full uint64 range", func(t *testing.T) {
		table := oneColTable(ir.Array{Element: ir.Integer{Width: 64, Unsigned: true}})
		data, _ := encodeParquet(t, table, []ir.Row{row([]any{uint64(math.MaxUint64), nil, uint64(0)})})
		vals := rawLeafValues(t, data, "v", "list", "element")
		if len(vals) != 3 || uint64(vals[0].Int64()) != math.MaxUint64 || !vals[1].IsNull() {
			t.Fatalf("unsigned elements = %v", vals)
		}
		if vals[2].IsNull() || vals[2].Int64() != 0 {
			t.Fatalf("zero unsigned element = %v; want present 0", vals[2])
		}
	})

	t.Run("nested list_str is the multi-dim refusal, not a contract error", func(t *testing.T) {
		// A 2-D text array's inner rows can arrive as []string via
		// blobcodec's list_str tag; the refusal must be the
		// multi-dimensional one (with the --exclude-table remedy),
		// never the misleading "upstream codec bug" contract message.
		table := oneColTable(ir.Array{Element: ir.Text{}})
		err := encodeErr(t, table, row([]any{[]string{"a", "b"}}))
		if !strings.Contains(err.Error(), "multi-dimensional") || !strings.Contains(err.Error(), "--exclude-table") {
			t.Fatalf("nested list_str refusal = %v; want the multi-dim refusal + remedy", err)
		}
	})

	t.Run("list_str shape accepted", func(t *testing.T) {
		// blobcodec's list_str tag round-trips a text-array decode as
		// []string; the encoder accepts it as the same content.
		table := oneColTable(ir.Array{Element: ir.Text{}})
		data, _ := encodeParquet(t, table, []ir.Row{row([]string{"x", "y"})})
		rows := readBack(t, data)
		if got := listStrings(t, rows[0]["v"]); len(got) != 2 || got[0] != "x" || got[1] != "y" {
			t.Fatalf("list_str round-trip = %#v", rows[0]["v"])
		}
	})
}

func TestPin_DomainRidesItsBaseType(t *testing.T) {
	typ := ir.Domain{Name: "email", BaseType: ir.Text{}, Checks: []ir.DomainCheck{{Body: "VALUE ~ '@'"}}}
	data, _ := encodeParquet(t, oneColTable(typ), []ir.Row{row("a@b"), row(nil)})
	vals := rawLeafValues(t, data, "v")
	if string(vals[0].ByteArray()) != "a@b" || !vals[1].IsNull() {
		t.Fatalf("domain round-trip = %v", vals)
	}
}

func TestPin_ExtensionAndVerbatimText(t *testing.T) {
	for _, typ := range []ir.Type{
		ir.ExtensionType{Extension: "vector", Name: "vector", Modifiers: []int{3}},
		ir.VerbatimType{Definition: "ltree"},
	} {
		data, _ := encodeParquet(t, oneColTable(typ), []ir.Row{row("[1,2,3]"), row([]byte("bytes-of-text")), row(nil)})
		vals := rawLeafValues(t, data, "v")
		if string(vals[0].ByteArray()) != "[1,2,3]" || string(vals[1].ByteArray()) != "bytes-of-text" || !vals[2].IsNull() {
			t.Fatalf("%s round-trip = %v", typ, vals)
		}
	}
}

// TestPin_RowGroupPerFlush pins the chunk→row-group alignment contract
// the exporter relies on (Flush cuts a row group).
func TestPin_RowGroupPerFlush(t *testing.T) {
	tc, err := NewTableCodec(oneColTable(ir.Integer{Width: 64}))
	if err != nil {
		t.Fatalf("NewTableCodec: %v", err)
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[map[string]any](&buf, tc.Schema, parquet.Compression(&parquet.Zstd))
	for group := 0; group < 3; group++ {
		enc, err := tc.EncodeRow(row(int64(group)))
		if err != nil {
			t.Fatalf("EncodeRow: %v", err)
		}
		if _, err := w.Write([]map[string]any{enc}); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	f := openParquet(t, buf.Bytes())
	if got := len(f.RowGroups()); got != 3 {
		t.Fatalf("row groups = %d; want 3 (one per flush)", got)
	}
}

// TestPin_UnboxedZeroCollapsesToNull proves the boxLeafValue wart is
// REAL in the pinned parquet-go version: writing a bare Go zero value
// (here `false`) through the map deconstruction WITHOUT the pointer
// boxing exports it as parquet NULL. This is the tripwire that keeps
// the IsNull()==false pins honest — if a parquet-go upgrade makes this
// fail (the unboxed zero reads back PRESENT), upstream changed its
// zero-value semantics: re-read boxLeafValue's rationale before
// removing either (the boxing itself stays harmless).
func TestPin_UnboxedZeroCollapsesToNull(t *testing.T) {
	tc, err := NewTableCodec(oneColTable(ir.Boolean{}))
	if err != nil {
		t.Fatalf("NewTableCodec: %v", err)
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[map[string]any](&buf, tc.Schema)
	// Bypass EncodeRow's boxing deliberately: a raw false.
	if _, err := w.Write([]map[string]any{{"v": false}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	vals := rawLeafValues(t, buf.Bytes(), "v")
	if len(vals) != 1 || !vals[0].IsNull() {
		t.Fatalf("unboxed false read back %v; the parquet-go zero-value-as-null wart is gone — upstream semantics changed, re-evaluate boxLeafValue (see its comment) before adjusting this pin", vals)
	}
}

// ---- helpers ----

// flbaToBigInt decodes a 16-byte two's-complement DECIMAL payload.
func flbaToBigInt(b []byte) *big.Int {
	v := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 {
		v.Sub(v, new(big.Int).Lsh(big.NewInt(1), uint(len(b))*8))
	}
	return v
}

// asInt64 tolerates the generic reader's integer reconstruction kinds.
func asInt64(t *testing.T, v any) int64 {
	t.Helper()
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	}
	t.Fatalf("value %#v is not an integer kind", v)
	return 0
}

// listStrings tolerates the generic reader's list reconstruction.
func listStrings(t *testing.T, v any) []string {
	t.Helper()
	if v == nil {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		if ss, ok2 := v.([]string); ok2 {
			return ss
		}
		t.Fatalf("value %#v is not a list", v)
	}
	out := make([]string, len(arr))
	for i, e := range arr {
		s, ok := e.(string)
		if !ok {
			t.Fatalf("element %#v is not a string", e)
		}
		out[i] = s
	}
	return out
}
