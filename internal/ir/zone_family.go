// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

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

// ZoneSiblingSwap reports whether a column type change between prev and
// cur moves a temporal column across its zone-sibling pair, in either
// direction, at any precision, scalar or array-of — the shape
// [ZoneFamily] exists to name. Precision-only changes and same-zone
// changes are not swaps.
func ZoneSiblingSwap(prev, cur Type) bool {
	prevFamily, prevZoned, prevOK := ZoneFamily(prev)
	curFamily, curZoned, curOK := ZoneFamily(cur)
	return prevOK && curOK && prevFamily == curFamily && prevZoned != curZoned
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
// `timestamptz → text` rendered `2026-06-16 05:00:00+09` under Asia/Tokyo
// against `2026-06-15 20:00:00+00` under UTC, while `timetz → text` and
// `timetz → time` returned byte-identical values under both.
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

func sessionNormalized(t Type) bool {
	family, zoned, ok := ZoneFamily(t)
	if !ok || !zoned {
		return false
	}
	// "timestamp" or "array:…:timestamp" — the time family's zoned member
	// (timetz) is deliberately excluded, per the measurements above.
	for len(family) > len("timestamp") {
		i := 0
		for i < len(family) && family[i] != ':' {
			i++
		}
		if i == len(family) {
			break
		}
		family = family[i+1:]
	}
	return family == "timestamp"
}

// SessionZoneCast reports whether an ALTER COLUMN TYPE from prev to cur
// resolves through the EXECUTING session's zone setting — the full class,
// of which [ZoneSiblingSwap] is the same-family half.
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
// measured 2026-09-03). On MySQL 8.0.46, `TIMESTAMP` → VARCHAR / DATE /
// TIME / BIGINT / DATETIME each shifted by the ALTER session's offset
// (a value stored at 20:00 UTC became 2026-06-16 05:00:00, 2026-06-16,
// 05:00:00 and 20260616050000 respectively under `+09:00`), and the
// reverse casts into `TIMESTAMP` shifted the stored instant by the same
// nine hours. On postgres:16, `timestamptz` → text / varchar / date /
// timestamp behaved identically, as did `time` → `timetz` (which stamps
// the session's offset onto a naive value).
//
// The asymmetry in the time family is real and measured, not an oversight:
// `time → timetz` IS session-dependent (an offset is invented) while
// `timetz → time`, `timetz → text` and `timetz → varchar` are NOT (the
// offset travels with the value). [ZoneSiblingSwap] still refuses the
// `timetz → time` direction, so this function cannot be used to NARROW an
// existing refusal — it only ever adds. Narrowing that direction is a
// separate, deliberately-reviewed change (filed as SLM-5b).
func SessionZoneCast(prev, cur Type) bool {
	if ZoneSiblingSwap(prev, cur) {
		return true
	}
	// A scalar ⇄ array dimension change is not this class, and the
	// carve-out is inherited deliberately from [ZoneSiblingSwap]: Postgres
	// needs an explicit USING to express one, so a forwarded bare ALTER
	// fails LOUDLY on the target instead of diverging. Refusing it here
	// would trade a loud target error for a sluice refusal on a shape that
	// was never at risk of silent divergence.
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
