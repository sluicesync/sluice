// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import "testing"

// TestParsePGTimeText_ZoneOffsetWidths pins PROM-P1: Postgres renders a
// timestamptz zone offset as ±HH, ±HH:MM, or ±HH:MM:SS depending on the
// session TimeZone (whole-hour, half/quarter-hour, and second-level historical
// LMT zones respectively — every form below was observed from real Postgres).
// Before the fix only the ±HH form parsed, so the pgoutput CDC pump aborted on
// the first timestamptz under any fractional-offset server timezone. Every form
// must parse to the SAME underlying instant (they are one instant rendered in
// different zones).
func TestParsePGTimeText_ZoneOffsetWidths(t *testing.T) {
	cases := []struct{ in, wantUTC string }{
		{"2026-02-02 02:02:02.020202+00", "2026-02-02 02:02:02.020202"},    // whole-hour (UTC)
		{"2026-02-02 07:32:02.020202+05:30", "2026-02-02 02:02:02.020202"}, // Asia/Kolkata +05:30
		{"2026-02-02 07:47:02.020202+05:45", "2026-02-02 02:02:02.020202"}, // Asia/Kathmandu +05:45
		{"2026-02-01 22:32:02.020202-03:30", "2026-02-02 02:02:02.020202"}, // America/St_Johns -03:30
		{"1900-06-02 02:21:34+00:19:32", "1900-06-02 02:02:02"},            // Europe/Amsterdam LMT +00:19:32
		{"2026-02-02 07:32:02.020202-08", "2026-02-02 15:32:02.020202"},    // whole-hour negative
		{"2026-02-02 02:02:02", "2026-02-02 02:02:02"},                     // TIMESTAMP (no zone)
		{"2026-02-02", "2026-02-02 00:00:00"},                              // DATE
	}
	for _, c := range cases {
		got, err := parsePGTimeText(c.in)
		if err != nil {
			t.Errorf("%q: unexpected error %v", c.in, err)
			continue
		}
		if g := got.UTC().Format("2006-01-02 15:04:05.999999999"); g != c.wantUTC {
			t.Errorf("%q => %s UTC; want %s", c.in, g, c.wantUTC)
		}
	}
}

// TestParsePGTimeText_SpecialValuesRefusedByName pins PROM-P2: the non-finite /
// pre-Gregorian timestamptz values Postgres can emit have no representable
// fixed-width target instant, so they refuse by NAME (loud) rather than fall
// through to the opaque "cannot parse".
func TestParsePGTimeText_SpecialValuesRefusedByName(t *testing.T) {
	for _, s := range []string{"infinity", "-infinity", "0044-03-15 BC"} {
		if _, err := parsePGTimeText(s); err == nil {
			t.Errorf("%q: want a named refusal, got nil", s)
		}
	}
}
