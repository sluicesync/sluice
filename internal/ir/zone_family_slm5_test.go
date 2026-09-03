// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "testing"

// Audit 2026-09-01 SLM-5, measured 2026-09-03 on mysql:8.0.46 and
// postgres:16. Every cell below is a MEASUREMENT, not a derivation — the
// table in each block records what the server actually did, and the
// expectation follows from it.
//
// The measured facts, for a value stored at 2026-06-15 20:00:00 UTC, with
// the ALTER run under +09:00 / Asia/Tokyo and the control under UTC:
//
//	MySQL 8.0.46 (TIMESTAMP →)      +09:00                  UTC
//	  VARCHAR(32)                   2026-06-16 05:00:00     2026-06-15 20:00:00
//	  DATE                          2026-06-16              2026-06-15
//	  TIME                          05:00:00                20:00:00
//	  BIGINT                        20260616050000          20260615200000
//	  DATETIME                      2026-06-16 05:00:00     2026-06-15 20:00:00
//	MySQL 8.0.46 (→ TIMESTAMP, read back at UTC)
//	  VARCHAR/DATETIME/BIGINT       2026-06-15 11:00:00     2026-06-15 20:00:00
//	  DATE                          2026-06-14 15:00:00     2026-06-15 00:00:00
//	postgres:16 (timestamptz →)
//	  text / varchar                2026-06-16 05:00:00+09  2026-06-15 20:00:00+00
//	  date                          2026-06-16              2026-06-15
//	  timestamp                     2026-06-16 05:00:00     2026-06-15 20:00:00
//	postgres:16 (time family)
//	  time → timetz                 20:00:00+09             20:00:00+00   ← session-dependent
//	  timetz → time                 20:00:00                20:00:00      ← NOT
//	  timetz → text/varchar         20:00:00+00             20:00:00+00   ← NOT
//	  time → text                   20:00:00                20:00:00      ← NOT
//
// The last block is why the predicate distinguishes "session-normalised"
// (stored UTC, rendered through a zone) from "carries a zone": timetz
// stores its offset per value, so reading one out needs no session zone.

func TestSessionZoneCast_MeasuredFamilyMatrix(t *testing.T) {
	var (
		tsz    = Timestamp{WithTimeZone: true}  // PG timestamptz, MySQL TIMESTAMP
		tsn    = Timestamp{WithTimeZone: false} // a zone-naive timestamp
		dt     = DateTime{}                     // PG timestamp, MySQL DATETIME
		timez  = Time{WithTimeZone: true}       // PG timetz
		timen  = Time{WithTimeZone: false}      // PG time, MySQL TIME
		text   = Varchar{Length: 32}
		txt    = Text{}
		date   = Date{}
		bigint = Integer{Width: 64}
	)

	for _, tc := range []struct {
		name     string
		prev     Type
		cur      Type
		want     bool
		measured string
	}{
		// --- the timestamp family: every cast off or onto the
		// session-normalised member is session-dependent (SLM-5's gap) ---
		{"TIMESTAMP -> VARCHAR", tsz, text, true, "05:00 vs 20:00 on mysql 8.0.46"},
		{"TIMESTAMP -> TEXT", tsz, txt, true, "timestamptz -> text differed on pg16"},
		{"TIMESTAMP -> DATE", tsz, date, true, "2026-06-16 vs 2026-06-15, crosses midnight"},
		{"TIMESTAMP -> TIME", tsz, timen, true, "05:00:00 vs 20:00:00"},
		{"TIMESTAMP -> BIGINT", tsz, bigint, true, "20260616050000 vs 20260615200000"},
		{"VARCHAR -> TIMESTAMP", text, tsz, true, "11:00 vs 20:00 read back at UTC"},
		{"DATE -> TIMESTAMP", date, tsz, true, "2026-06-14 15:00 vs 2026-06-15 00:00"},
		{"BIGINT -> TIMESTAMP", bigint, tsz, true, "11:00 vs 20:00"},

		// --- the sibling half ZoneSiblingSwap already covered; these must
		// keep refusing, or SLM-1/SL-2 regress ---
		{"TIMESTAMP -> DATETIME", tsz, dt, true, "the SL-2 shape"},
		{"DATETIME -> TIMESTAMP", dt, tsz, true, "the SL-2 shape, reversed"},
		{"timestamptz -> timestamp(naive)", tsz, tsn, true, "the PG SL-2 shape"},

		// --- the time family, where the measurement forced a distinction ---
		{"time -> timetz", timen, timez, true, "20:00:00+09 vs +00 — an offset is invented"},
		{"timetz -> time", timez, timen, true, "NOT session-dependent, but ZoneSiblingSwap still refuses it (SLM-5b)"},
		{"timetz -> VARCHAR", timez, text, false, "20:00:00+00 under both zones"},
		{"time -> VARCHAR", timen, text, false, "20:00:00 under both zones"},

		// --- shapes that must NOT refuse, or a working configuration breaks ---
		{"precision-only TIMESTAMP -> TIMESTAMP", tsz, tsz, false, "same type, no cast"},
		{"DATETIME -> DATETIME", dt, dt, false, "same type"},
		{"DATETIME -> VARCHAR", dt, text, false, "no zone on either side"},
		{"VARCHAR -> DATE", text, date, false, "no zone on either side"},
		{"INT -> BIGINT", Integer{Width: 32}, bigint, false, "not temporal at all"},
		{"VARCHAR -> TEXT", text, txt, false, "not temporal at all"},

		// --- arrays carry the same rule one level down ---
		{"timestamptz[] -> text[]", Array{Element: tsz}, Array{Element: text}, true, "element-level cast, same mechanism"},
		{"text[] -> timestamptz[]", Array{Element: text}, Array{Element: tsz}, true, "element-level, zone invented"},
		{"timetz[] -> text[]", Array{Element: timez}, Array{Element: text}, false, "offset travels with each value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := SessionZoneCast(tc.prev, tc.cur); got != tc.want {
				t.Errorf("SessionZoneCast(%v, %v) = %v; want %v — measured: %s",
					tc.prev, tc.cur, got, tc.want, tc.measured)
			}
		})
	}
}

// TestSessionZoneCast_NeverNarrowsZoneSiblingSwap is the containment
// floor. SessionZoneCast is allowed to refuse strictly more than
// ZoneSiblingSwap and never less: the sibling predicate is what the
// shipped SL-2 / SLM-1 / SLM-1c refusals are built on, and a widening
// that quietly dropped one of them would reopen a silent-divergence class
// while looking like an improvement.
func TestSessionZoneCast_NeverNarrowsZoneSiblingSwap(t *testing.T) {
	types := []Type{
		Timestamp{WithTimeZone: true},
		Timestamp{WithTimeZone: false},
		DateTime{},
		Time{WithTimeZone: true},
		Time{WithTimeZone: false},
		Varchar{Length: 32},
		Text{},
		Date{},
		Integer{Width: 64},
		Array{Element: Timestamp{WithTimeZone: true}},
		Array{Element: DateTime{}},
		Array{Element: Time{WithTimeZone: true}},
		Array{Element: Varchar{Length: 32}},
	}
	var siblings int
	for _, prev := range types {
		for _, cur := range types {
			if !ZoneSiblingSwap(prev, cur) {
				continue
			}
			siblings++
			if !SessionZoneCast(prev, cur) {
				t.Errorf("ZoneSiblingSwap(%v, %v) refuses but SessionZoneCast does not — the widening dropped a shipped refusal", prev, cur)
			}
		}
	}
	// Anti-vacuity: if the sibling predicate stops matching anything, the
	// containment claim above is empty and must fail rather than pass.
	if siblings < 6 {
		t.Fatalf("only %d sibling swaps found across %d types; the containment check is near-vacuous — ZoneSiblingSwap or the type list has changed",
			siblings, len(types))
	}
}
