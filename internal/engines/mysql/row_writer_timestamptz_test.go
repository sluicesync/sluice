// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestPrepareValue_TimestamptzInstantConversion pins PROM-M1: a postgres-trigger
// `timestamptz` arrives as an ISO string carrying a numeric zone offset (the
// to_jsonb T-separated form, or the ::text space-separated form). The offset
// encodes the INSTANT, so prepareValue must convert it to the UTC instant (a
// time.Time) — matching the bulk-copy path, which binds a pgx time.Time —
// instead of stripping the offset and storing the source session's wall clock.
// Every offset-width and separator maps to the same stored instant.
func TestPrepareValue_TimestamptzInstantConversion(t *testing.T) {
	tz := &ir.Column{Name: "ts", Type: ir.Timestamp{WithTimeZone: true}}
	cases := []struct {
		in      any
		wantUTC string
	}{
		{"2026-02-02T07:32:02.020202+05:30", "2026-02-02 02:02:02.020202"},         // Kolkata, to_jsonb (T-separated)
		{"2026-02-02T07:47:02.020202+05:45", "2026-02-02 02:02:02.020202"},         // Kathmandu +05:45
		{"2026-02-01T22:32:02.020202-03:30", "2026-02-02 02:02:02.020202"},         // St Johns -03:30
		{"1900-06-02T02:21:34+00:19:32", "1900-06-02 02:02:02"},                    // second-level LMT +HH:MM:SS (T)
		{"1900-06-02 02:21:34+00:19:32", "1900-06-02 02:02:02"},                    // second-level LMT (space)
		{"2026-02-02T02:02:02.020202+00", "2026-02-02 02:02:02.020202"},            // whole-hour, T-separated
		{"2026-02-02 02:02:02.020202+00", "2026-02-02 02:02:02.020202"},            // whole-hour, space-separated (::text)
		{[]byte("2026-02-02T07:32:02.020202+05:30"), "2026-02-02 02:02:02.020202"}, // []byte path
	}
	for _, c := range cases {
		got, err := prepareValue(c.in, tz)
		if err != nil {
			t.Errorf("%v: unexpected error %v", c.in, err)
			continue
		}
		inst, ok := got.(time.Time)
		if !ok {
			t.Errorf("%v => %T; want a time.Time instant", c.in, got)
			continue
		}
		if g := inst.UTC().Format("2006-01-02 15:04:05.999999"); g != c.wantUTC {
			t.Errorf("%v => %s UTC; want the instant %s", c.in, g, c.wantUTC)
		}
	}
}

// TestPrepareValue_TimestamptzSpecialValuesRefused pins the loud-failure gate on
// the pgtrigger->MySQL write path (PROM-P2, MySQL side): the non-finite /
// pre-Gregorian values have no representable MySQL instant and must REFUSE by
// name, not silently strip (which would drop the BC era — 44 BC -> 44 AD).
func TestPrepareValue_TimestamptzSpecialValuesRefused(t *testing.T) {
	tz := &ir.Column{Name: "ts", Type: ir.Timestamp{WithTimeZone: true}}
	for _, in := range []any{
		"infinity",
		"-infinity",
		"0044-03-15T12:00:00+00:00 BC", // to_jsonb form
		"0044-03-15 12:00:00+00 BC",    // ::text form
	} {
		if _, err := prepareValue(in, tz); err == nil {
			t.Errorf("%v: want a loud refusal, got nil (silent coercion)", in)
		}
	}
}

// TestPrepareValue_PlainTimestampWallClockKept pins that a plain `timestamp`
// (WithTimeZone=false) is unchanged — no offset, the wall clock IS the value,
// so the historical strip stays (time-of-day has no instant semantics).
func TestPrepareValue_PlainTimestampWallClockKept(t *testing.T) {
	ts := &ir.Column{Name: "ts", Type: ir.Timestamp{WithTimeZone: false}}
	got, err := prepareValue("2026-02-02 07:32:02.020202", ts)
	if err != nil {
		t.Fatal(err)
	}
	if s, _ := got.(string); s != "2026-02-02 07:32:02.020202" {
		t.Errorf("plain timestamp => %v; want the wall clock unchanged", got)
	}
}

// TestPrepareValue_TimestamptzTimeTimePassthrough pins that the pgoutput /
// bulk-copy path (which already produces a time.Time instant) passes through
// untouched — the string-parse branch must not disturb it.
func TestPrepareValue_TimestamptzTimeTimePassthrough(t *testing.T) {
	tz := &ir.Column{Name: "ts", Type: ir.Timestamp{WithTimeZone: true}}
	in := time.Date(2026, 2, 2, 2, 2, 2, 20202000, time.UTC)
	got, err := prepareValue(in, tz)
	if err != nil {
		t.Fatal(err)
	}
	inst, ok := got.(time.Time)
	if !ok {
		t.Fatalf("time.Time input => %T; want time.Time passthrough", got)
	}
	if !inst.Equal(in) {
		t.Errorf("passthrough changed the instant: %v != %v", inst, in)
	}
}
