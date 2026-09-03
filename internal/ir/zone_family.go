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
