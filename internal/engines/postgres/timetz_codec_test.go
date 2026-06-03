// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"encoding/binary"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"sluicesync.dev/sluice/internal/ir"
)

// TestPGTimetzCodec_RoundTrip pins catalog Bug 71: the timetz value
// string PG hands the reader must encode to the 12-byte binary wire
// form (int64 µs-since-midnight, int32 zone-seconds-west-of-UTC) and
// decode back to the canonical text — so a timetz column survives
// PG→PG COPY instead of "cannot find encode plan".
func TestPGTimetzCodec_RoundTrip(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		wantUsec int64
		wantZone int32 // PG wire field: seconds WEST of UTC == -gmtoff
		wantText string
	}{
		{
			name:     "positive offset whole hours",
			in:       "13:45:30+05",
			wantUsec: (13*3600 + 45*60 + 30) * 1_000_000,
			wantZone: -5 * 3600,
			wantText: "13:45:30+05",
		},
		{
			name:     "negative offset with minutes",
			in:       "08:00:00-07:30",
			wantUsec: (8 * 3600) * 1_000_000,
			wantZone: 7*3600 + 30*60,
			wantText: "08:00:00-07:30",
		},
		{
			name:     "fractional seconds, UTC",
			in:       "23:59:59.123456+00",
			wantUsec: (23*3600+59*60+59)*1_000_000 + 123456,
			wantZone: 0,
			wantText: "23:59:59.123456+00",
		},
		{
			name:     "midnight, positive offset",
			in:       "00:00:00+02",
			wantUsec: 0,
			wantZone: -2 * 3600,
			wantText: "00:00:00+02",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			plan := pgTimetzBinaryCodec{}.PlanEncode(nil, 0, pgtype.BinaryFormatCode, c.in)
			if plan == nil {
				t.Fatal("PlanEncode returned nil for binary + string")
			}
			out, err := plan.Encode(c.in, nil)
			if err != nil {
				t.Fatalf("Encode(%q): %v", c.in, err)
			}
			if len(out) != 12 {
				t.Fatalf("encoded length = %d; want 12", len(out))
			}
			gotUsec := int64(binary.BigEndian.Uint64(out[0:8]))
			gotZone := int32(binary.BigEndian.Uint32(out[8:12]))
			if gotUsec != c.wantUsec {
				t.Errorf("usec = %d; want %d", gotUsec, c.wantUsec)
			}
			if gotZone != c.wantZone {
				t.Errorf("zone = %d; want %d", gotZone, c.wantZone)
			}
			back, err := decodeTimetzBinary(out)
			if err != nil {
				t.Fatalf("decodeTimetzBinary: %v", err)
			}
			if back != c.wantText {
				t.Errorf("round-trip text = %q; want %q", back, c.wantText)
			}
		})
	}
}

// TestTableHasTimetzColumn pins the predicate that drives whether
// writeViaCopy registers the per-conn timetz codec (Bug 71): it must
// fire for a tz-aware ir.Time and stay quiet for plain ir.Time.
func TestTableHasTimetzColumn(t *testing.T) {
	tz := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "v", Type: ir.Time{Precision: 6, WithTimeZone: true}},
		},
	}
	if !tableHasTimetzColumn(tz) {
		t.Error("tableHasTimetzColumn = false for a timetz column; want true")
	}

	plain := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "v", Type: ir.Time{Precision: 6}},
		},
	}
	if tableHasTimetzColumn(plain) {
		t.Error("tableHasTimetzColumn = true for a plain time column; want false")
	}
}
