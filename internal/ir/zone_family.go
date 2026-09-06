// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "strings"

// ZoneFamily classifies a type as (temporal family, carries-a-zone) for
// the session-zone cast class: the ALTER COLUMN TYPE shapes an engine
// resolves against the EXECUTING session's zone setting (PG `TimeZone`,
// MySQL `time_zone`) rather than anything the replication wire carries,
// so a forwarded ALTER re-casts the target's pre-existing rows against a
// different session's setting and every one of them silently diverges.
//
// Only the families with a zone-sibling are members; everything else
// reports ok=false and can never pair:
//
//   - "timestamp": [Timestamp] (zoned iff WithTimeZone) and [DateTime],
//     which is the zone-naive sibling on every engine that produces it
//     (MySQL `DATETIME`, PG `timestamp without time zone` — both project
//     to DateTime; the Postgres CDC and schema readers agree, pinned by
//     postgres.TestSchemaSeed_ColdStartProjectionsAgree).
//   - "time": [Time] (zoned iff WithTimeZone; PG `time`⇄`timetz`).
//
// Array-ness is folded into the family ("array:" prefix, one level per
// dimension of the IR shape) so an element swap inside arrays matches and
// a scalar⇄array dimension change does not — PG needs an explicit USING
// to express the latter, so a forwarded bare ALTER fails loudly on the
// target instead of diverging.
//
// It is the one predicate every consumer of the class shares — the
// pipeline's own door (pipeline.sessionZoneSiblingSwap) and the Postgres
// reader's seeded first-boundary check (postgres.seededSessionTZSwapPair)
// — so the family universe cannot drift between them. The two engine
// boundary emitters keep their own lane-local declarations
// (postgres.sessionTZSwapPair keys on wire OIDs; mysql.sessionTZSwapPair
// on its two IR types) because the cross-engine roster
// (docsync.TestSessionGUCCastRoster_EveryCDCLane) reads each lane's pair
// universe out of that lane's declaration by AST.
func ZoneFamily(t Type) (family string, zoned, ok bool) {
	switch v := t.(type) {
	case Array:
		f, z, ok := ZoneFamily(v.Element)
		return "array:" + f, z, ok
	case Timestamp:
		return "timestamp", v.WithTimeZone, true
	case DateTime:
		return "timestamp", false, true
	case Time:
		return "time", v.WithTimeZone, true
	}
	return "", false, false
}

// baseFamily strips the "array:" dimension prefixes [ZoneFamily] adds,
// leaving the scalar family name ("timestamp", "time", or "").
func baseFamily(family string) string {
	for {
		rest, ok := strings.CutPrefix(family, "array:")
		if !ok {
			return family
		}
		family = rest
	}
}

// SessionDependentZoneSwap reports whether a column type change between
// prev and cur moves a temporal column across its zone-sibling pair — the
// shape [ZoneFamily] exists to name — IN A DIRECTION WHOSE CAST RESOLVES
// THROUGH THE EXECUTING SESSION'S ZONE. Precision-only changes and
// same-zone changes are not swaps; a scalar⇄array dimension change is not
// this class either (the family carries the array depth, so the two sides
// cannot match).
//
// The direction qualifier is the whole point of the name, and it is
// asymmetric BY FAMILY, measured on postgres:16 and mysql:8.0.46
// (2026-09-03 for the drop direction, 2026-09-05 for the add direction;
// audit SLM-5 and SLM-5b):
//
//   - The "timestamp" family is SYMMETRIC. `timestamptz` and MySQL
//     `TIMESTAMP` are stored NORMALISED to UTC, so both directions consult
//     the session: dropping the zone renders UTC through it, adding one
//     interprets a naive wall-clock value in it. Both directions refuse.
//   - The "time" family is NOT. `timetz` stores the offset alongside each
//     value, so DROPPING it (`timetz` to `time`) needs no session zone at
//     all and returned byte-identical values under `UTC` and `Asia/Tokyo`.
//     Only the ADD direction (`time` to `timetz`) invents an offset, and
//     Postgres takes it from the executing session. So only the add
//     direction refuses.
//
// Refusing the `timetz` to `time` direction was over-broad — a loud
// refusal on a boundary that could always have been forwarded safely — and
// this predicate stops doing it. Every other cell of the matrix is
// unchanged; see TestSessionDependentZoneSwap_DirectionMatrix, which pins
// all four directions of both families, scalar and array.
func SessionDependentZoneSwap(prev, cur Type) bool {
	prevFamily, prevZoned, prevOK := ZoneFamily(prev)
	curFamily, curZoned, curOK := ZoneFamily(cur)
	if !prevOK || !curOK || prevFamily != curFamily || prevZoned == curZoned {
		return false
	}
	if baseFamily(curFamily) == "time" {
		return !prevZoned && curZoned
	}
	return true
}

// sessionNormalized reports whether a type is stored NORMALISED TO UTC, so
// that rendering one of its values into anything else has to pick a zone —
// and picks the executing session's. That is the property that makes a
// cast session-dependent, and it is narrower than "carries a zone":
//
//   - PG `timestamptz` and MySQL `TIMESTAMP` are session-normalised. Both
//     store UTC and render in the session's zone.
//   - PG `timetz` is NOT. It stores the offset alongside each value, so
//     reading one out needs no session zone at all.
//
// Measured on postgres:16 and mysql:8.0.46 (2026-09-03, audit SLM-5), which
// is the only reason this distinction is drawn rather than assumed:
// `timestamptz` to text rendered `2026-06-16 05:00:00+09` under Asia/Tokyo
// against `2026-06-15 20:00:00+00` under UTC, while `timetz` to text and
// `timetz` to `time` returned byte-identical values under both.
//
// The asymmetry this note used to flag as open (audit SLM-5b) is now
// resolved in [SessionDependentZoneSwap]: the add direction was measured
// under two session zones, IS session-dependent, and still refuses, while
// the drop direction no longer does.
func sessionNormalized(t Type) bool {
	family, zoned, ok := ZoneFamily(t)
	if !ok || !zoned {
		return false
	}
	// The time family's zoned member (timetz) is deliberately excluded,
	// per the measurements above.
	return baseFamily(family) == "timestamp"
}

// arrayDepth counts the IR array dimensions wrapping t.
func arrayDepth(t Type) int {
	depth := 0
	for {
		arr, ok := t.(Array)
		if !ok {
			return depth
		}
		depth++
		t = arr.Element
	}
}

// SessionZoneCast reports whether an ALTER COLUMN TYPE from prev to cur
// resolves through the EXECUTING session's zone setting — the full class,
// of which [SessionDependentZoneSwap] is the same-family half.
//
// A cast is session-dependent when either side of it has to invent or
// interpret a zone:
//
//   - prev is session-normalised and cur is not the same thing: every
//     stored value is UTC and must be rendered through some zone to
//     become a string, a date, a wall-clock time, or a number.
//   - cur carries a zone and prev does not: the values have no offset, so
//     one is invented, and it is the session's.
//
// Measured in both directions on real servers (audit 2026-09-01 SLM-5,
// measured 2026-09-03). On MySQL 8.0.46, `TIMESTAMP` to VARCHAR / DATE /
// TIME / BIGINT / DATETIME each shifted by the ALTER session's offset
// (a value stored at 20:00 UTC became 2026-06-16 05:00:00, 2026-06-16,
// 05:00:00 and 20260616050000 respectively under `+09:00`), and the
// reverse casts into `TIMESTAMP` shifted the stored instant by the same
// nine hours. On postgres:16, `timestamptz` to text / varchar / date /
// timestamp behaved identically, as did `time` to `timetz` (which stamps
// the session's offset onto a naive value).
//
// The time family's asymmetry is real and measured, not an oversight:
// `time` to `timetz` IS session-dependent (an offset is invented) while
// `timetz` to `time`, `timetz` to text and `timetz` to varchar are NOT
// (the offset travels with the value). Both halves of this function now
// agree on that: the sibling half stopped refusing the drop direction in
// SLM-5b, and the fall-through below never refused it.
func SessionZoneCast(prev, cur Type) bool {
	if SessionDependentZoneSwap(prev, cur) {
		return true
	}
	// A scalar ⇄ array dimension change is not this class, and the
	// carve-out is inherited deliberately from [SessionDependentZoneSwap]:
	// Postgres needs an explicit USING to express one, so a forwarded bare
	// ALTER fails LOUDLY on the target instead of diverging. Refusing it
	// here would trade a loud target error for a sluice refusal on a shape
	// that was never at risk of silent divergence.
	if arrayDepth(prev) != arrayDepth(cur) {
		return false
	}
	prevNorm, curNorm := sessionNormalized(prev), sessionNormalized(cur)
	if prevNorm != curNorm {
		return true
	}
	// The zone-inventing direction for the rest of the zone families: cur
	// carries a zone that prev did not. (The both-session-normalised and
	// neither-carries-a-zone cases fall through to false.)
	_, prevZoned, prevOK := ZoneFamily(prev)
	_, curZoned, curOK := ZoneFamily(cur)
	return curOK && curZoned && (!prevOK || !prevZoned)
}
