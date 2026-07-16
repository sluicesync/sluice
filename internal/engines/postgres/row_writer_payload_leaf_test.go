// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for convertArray's trigger-payload leaf shapes (RDS
// validation F3, 2026-07-16). The pgtrigger change payload delivers
// array elements as to_jsonb() + UseNumber leaves — json.Number,
// "Infinity"/"-Infinity"/"NaN" strings for non-finite floats, int64 for
// whole numerics (the decoder's loss-free integer rule), ISO-8601
// strings for temporals. Per the Bug-74 discipline the matrix below
// covers EVERY family arm that gained a payload shape — float, numeric,
// date, datetime, timestamp, timestamptz — plus the loud-refusal
// boundaries; the families whose payload leaves were already canonical
// (int/bool/text-string leaves/time) are pinned by the existing
// TestConvertArrayPerFamilyLeafAndDims and by the real-path integration
// matrix in pgtrigger/cdc_apply_array_families_integration_test.go.

package postgres

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"sluicesync.dev/sluice/internal/ir"
)

// TestConvertArrayFloatPayloadLeaves pins every payload shape the Float
// arm must accept, value-exact: finite json.Number (incl. a denormal
// and negative zero — sign bit preserved), a whole value as int64, the
// three non-finite spellings, and a NULL element.
func TestConvertArrayFloatPayloadLeaves(t *testing.T) {
	got, err := convertArray([]any{
		json.Number("1.5"),
		int64(2),
		json.Number("5e-324"),
		json.Number("-0"),
		"Infinity",
		"-Infinity",
		"NaN",
		nil,
	}, ir.Float{})
	if err != nil {
		t.Fatalf("convertArray: %v", err)
	}
	arr, ok := got.(pgtype.Array[*float64])
	if !ok {
		t.Fatalf("got %T; want pgtype.Array[*float64]", got)
	}
	els := arr.Elements
	if len(els) != 8 {
		t.Fatalf("got %d elements; want 8", len(els))
	}
	if *els[0] != 1.5 || *els[1] != 2 || *els[2] != 5e-324 {
		t.Errorf("finite values = %v %v %v; want 1.5 2 5e-324", *els[0], *els[1], *els[2])
	}
	if *els[3] != 0 || !math.Signbit(*els[3]) {
		t.Errorf("negative zero = %v (signbit=%v); want -0 with sign bit set", *els[3], math.Signbit(*els[3]))
	}
	if !math.IsInf(*els[4], 1) || !math.IsInf(*els[5], -1) || !math.IsNaN(*els[6]) {
		t.Errorf("non-finite values = %v %v %v; want +Inf -Inf NaN", *els[4], *els[5], *els[6])
	}
	if els[7] != nil {
		t.Errorf("NULL element = %v; want nil", els[7])
	}
}

// TestConvertArrayFloatPayload2D pins the multi-dim shape with payload
// leaves — the Bug-74 flatten class: dims must survive alongside the
// widened leaf acceptance.
func TestConvertArrayFloatPayload2D(t *testing.T) {
	got, err := convertArray([]any{
		[]any{json.Number("1.5"), "NaN"},
		[]any{"Infinity", nil},
	}, ir.Float{})
	if err != nil {
		t.Fatalf("convertArray 2-D: %v", err)
	}
	arr := got.(pgtype.Array[*float64])
	if len(arr.Dims) != 2 || arr.Dims[0].Length != 2 || arr.Dims[1].Length != 2 {
		t.Fatalf("dims = %+v; want 2x2", arr.Dims)
	}
	if *arr.Elements[0] != 1.5 || !math.IsNaN(*arr.Elements[1]) ||
		!math.IsInf(*arr.Elements[2], 1) || arr.Elements[3] != nil {
		t.Errorf("2-D elements = %v; want [1.5 NaN +Inf nil]", arr.Elements)
	}
}

// TestConvertArrayFloatRefusesArbitraryString pins the loud boundary:
// only the three to_jsonb non-finite spellings are strings-in-disguise;
// anything else is an upstream mis-decode and must refuse, not parse.
func TestConvertArrayFloatRefusesArbitraryString(t *testing.T) {
	if _, err := convertArray([]any{"1.5"}, ir.Float{}); err == nil {
		t.Error("expected refusal for numeric-looking string in float array; got nil")
	}
	if _, err := convertArray([]any{"infinity"}, ir.Float{}); err == nil {
		t.Error("expected refusal for lowercase 'infinity' (not a to_jsonb spelling); got nil")
	}
	if _, err := convertArray([]any{true}, ir.Float{}); err == nil {
		t.Error("expected refusal for bool in float array; got nil")
	}
}

// TestConvertArrayNumericPayloadLeaves pins the Decimal arm's payload
// shapes: json.Number carrying more precision than float64 could hold
// (digit-lossless — the reason the decoder never parses these to
// float64), int64 whole values, and the canonical-string NaN/±Infinity
// spellings the SQL path already delivers.
func TestConvertArrayNumericPayloadLeaves(t *testing.T) {
	const highPrecision = "123456789.123456789012345678901234567890"
	got, err := convertArray([]any{
		json.Number(highPrecision),
		int64(42),
		"NaN",
		"Infinity",
		"-Infinity",
		nil,
	}, ir.Decimal{})
	if err != nil {
		t.Fatalf("convertArray: %v", err)
	}
	arr, ok := got.(pgtype.Array[*pgtype.Numeric])
	if !ok {
		t.Fatalf("got %T; want pgtype.Array[*pgtype.Numeric]", got)
	}
	els := arr.Elements

	var want pgtype.Numeric
	if err := want.Scan(highPrecision); err != nil {
		t.Fatalf("reference Scan: %v", err)
	}
	if els[0].Int.Cmp(want.Int) != 0 || els[0].Exp != want.Exp {
		t.Errorf("high-precision element = %+v; want digits of %s (json.Number must convert digit-lossless)", els[0], highPrecision)
	}
	if els[1].Int.Int64() != 42 || els[1].Exp != 0 {
		t.Errorf("int64 element = %+v; want 42", els[1])
	}
	if !els[2].NaN {
		t.Errorf("NaN element = %+v; want NaN=true", els[2])
	}
	if els[3].InfinityModifier != pgtype.Infinity || els[4].InfinityModifier != pgtype.NegativeInfinity {
		t.Errorf("infinity elements = %+v / %+v; want +Inf / -Inf modifiers", els[3], els[4])
	}
	if els[5] != nil {
		t.Errorf("NULL element = %v; want nil", els[5])
	}
}

// TestConvertArrayTemporalPayloadLeaves pins the ISO-8601 string shape
// for each temporal family arm — date, datetime, timestamp,
// timestamptz — with and without fractional seconds, and a non-UTC
// offset for the tz family.
func TestConvertArrayTemporalPayloadLeaves(t *testing.T) {
	utc := time.UTC

	// date
	got, err := convertArray([]any{"2026-07-16", nil}, ir.Date{})
	if err != nil {
		t.Fatalf("date: %v", err)
	}
	dates := got.(pgtype.Array[*pgtype.Date])
	if want := time.Date(2026, 7, 16, 0, 0, 0, 0, utc); !dates.Elements[0].Time.Equal(want) {
		t.Errorf("date = %v; want %v", dates.Elements[0].Time, want)
	}

	// datetime + timestamp (same layout, separate IR arms — pin both)
	for _, elem := range []ir.Type{ir.DateTime{}, ir.Timestamp{}} {
		got, err := convertArray([]any{"2026-01-02T03:04:05.123456", "2026-01-02T03:04:05"}, elem)
		if err != nil {
			t.Fatalf("%T: %v", elem, err)
		}
		tss := got.(pgtype.Array[*pgtype.Timestamp])
		if want := time.Date(2026, 1, 2, 3, 4, 5, 123456000, utc); !tss.Elements[0].Time.Equal(want) {
			t.Errorf("%T fractional = %v; want %v", elem, tss.Elements[0].Time, want)
		}
		if want := time.Date(2026, 1, 2, 3, 4, 5, 0, utc); !tss.Elements[1].Time.Equal(want) {
			t.Errorf("%T whole-second = %v; want %v", elem, tss.Elements[1].Time, want)
		}
	}

	// timestamptz: to_jsonb emits a full ±hh:mm offset (or Z); the
	// parsed instant must be offset-exact.
	got, err = convertArray([]any{
		"2026-01-02T03:04:05.123456+00:00",
		"2026-01-02T10:04:05+07:00",
	}, ir.Timestamp{WithTimeZone: true})
	if err != nil {
		t.Fatalf("timestamptz: %v", err)
	}
	tstzs := got.(pgtype.Array[*pgtype.Timestamptz])
	if want := time.Date(2026, 1, 2, 3, 4, 5, 123456000, utc); !tstzs.Elements[0].Time.Equal(want) {
		t.Errorf("timestamptz utc = %v; want %v", tstzs.Elements[0].Time, want)
	}
	if want := time.Date(2026, 1, 2, 3, 4, 5, 0, utc); !tstzs.Elements[1].Time.Equal(want) {
		t.Errorf("timestamptz +07:00 = %v; want instant %v", tstzs.Elements[1].Time, want)
	}
}

// TestConvertArrayTemporalRefusesUnparseable pins the loud boundary for
// temporal strings: PG's special 'infinity' timestamps and BC dates
// have no time.Time form and must refuse naming the value, never guess.
func TestConvertArrayTemporalRefusesUnparseable(t *testing.T) {
	for _, c := range []struct {
		elem ir.Type
		val  string
	}{
		{ir.Timestamp{}, "infinity"},
		{ir.Timestamp{WithTimeZone: true}, "-infinity"},
		{ir.Date{}, "0044-01-01 BC"},
		{ir.DateTime{}, "not-a-timestamp"},
	} {
		_, err := convertArray([]any{c.val}, c.elem)
		if err == nil {
			t.Errorf("%T(%q): expected loud refusal; got nil", c.elem, c.val)
		}
	}
}
