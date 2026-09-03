// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The pipeline's own door on the session-zone cast class (SLM-1, audit
// 2026-09-01) — the belt behind the readers' braces.
//
// Each CDC lane refuses a zone-sibling swap at its own boundary emitter
// (mysql.unforwardableSessionTZColumn, postgres.unforwardableSessionTZCast),
// and that is where the refusal belongs: the reader is the party that knows
// the source. But a reader's refusal needs a PRIOR shape for the table, and
// SLM-1 was precisely a boundary where the reader had none while the
// pipeline did — Shape A's router classified the first CDC snapshot against
// its cold-start seed and forwarded an ALTER COLUMN TYPE the reader had
// never been able to check. So every path that turns a classified
// [ShapeKindAlterColumnType] into target DDL runs this predicate over the
// (before, after) pair it is about to apply. It is engine-neutral because
// the IR is: a zone-aware temporal and its zone-naive sibling are IR facts
// ([ir.Timestamp].WithTimeZone, [ir.DateTime], [ir.Time].WithTimeZone),
// and the mechanism — the executing session's zone setting decides the
// cast, and no wire carries the source's — holds on every engine sluice
// reads.
//
// The roster that keeps every applyShapeDelta caller behind this door is
// TestSessionZoneSwapDoorRoster_EveryShapeApplyCallSite.

import (
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// sessionZoneSiblingSwap reports whether a column type change between
// prev and cur moves a temporal column across its zone-sibling pair — a
// zone-aware timestamp against a zone-naive one (MySQL `TIMESTAMP` ⇄
// `DATETIME`, PG `timestamptz` ⇄ `timestamp`), or `timetz` ⇄ `time`, in
// either direction, at any precision, scalar or array-of. Precision-only
// changes and same-zone family changes are not swaps; a scalar ⇄ array
// dimension change is not a swap either (PG needs an explicit USING to
// express it, so a forwarded bare ALTER fails loudly rather than
// diverging — mirrors the PG lane's own predicate).
//
// Widened for audit 2026-09-01 SLM-5 (measured 2026-09-03): the predicate
// is now [ir.SessionZoneCast], which also covers a change where only ONE
// side is a session-normalised timestamp — `TIMESTAMP` → VARCHAR / DATE /
// TIME / BIGINT and the reverse, `timestamptz` → text / date, and `time` →
// `timetz`. Those were measured to shift by the ALTER session's offset on
// mysql:8.0.46 and postgres:16 exactly as the sibling swap does, and they
// forwarded unrefused. [ir.SessionZoneCast] contains [ir.ZoneSiblingSwap]
// by construction (pinned by ir.TestSessionZoneCast_NeverNarrowsZoneSiblingSwap),
// so nothing this door refused before is now allowed through.
//
// SCOPE, stated so the name cannot be read wider than the truth: this is
// the PIPELINE door, reached when a boundary is FORWARDED — which is the
// mode that re-casts the target's pre-existing rows, so it is where the
// widened class does its work. The two READER-side arms that refuse at a
// table's first boundary (mysql.sessionTZSwapPair on its IR types,
// postgres.sessionTZSwapPair on wire OIDs, with the seeded arm bound to
// it) still carry only the sibling pair. Widening those means reshaping
// the Postgres wire declaration from a pair list into a predicate over
// "is this OID timestamptz", along with the test that binds the seeded
// arm to it — filed as SLM-5c rather than half-done here.
func sessionZoneSiblingSwap(prev, cur ir.Type) bool {
	return ir.SessionZoneCast(prev, cur)
}

// zoneFamily classifies a type as (temporal family, carries-a-zone) —
// [ir.ZoneFamily], the one declaration this door shares with the Postgres
// reader's seeded first-boundary check (SLM-1c) so the two cannot drift.
func zoneFamily(t ir.Type) (family string, zoned, ok bool) {
	return ir.ZoneFamily(t)
}

// refuseSessionZoneSwap is the door: nil for every shape that is not a
// zone-sibling ALTER COLUMN TYPE, the loud refusal otherwise. hint is the
// caller's recovery text (Shape A's [RecoveryHint] or the single-stream
// [forwardRecoveryHint]) so the operator lands in the workflow of the path
// they are actually on.
func refuseSessionZoneSwap(tableName string, shape Shape, hint string) error {
	if shape.Kind != ShapeKindAlterColumnType || shape.AlteredColumnBefore == nil || shape.AlteredColumn == nil {
		return nil
	}
	before, after := shape.AlteredColumnBefore, shape.AlteredColumn
	if !sessionZoneSiblingSwap(before.Type, after.Type) {
		return nil
	}
	return fmt.Errorf(
		"pipeline: schema change on %q is detected but cannot be forwarded: column %q changed between %s and %s — "+
			"the source's ALTER resolved every stored value against the SOURCE session's zone setting "+
			"(MySQL time_zone, Postgres TimeZone) and a forwarded ALTER would re-cast the target's pre-existing rows "+
			"against the TARGET session's; when the two differ the casts silently disagree at exit 0, so this swap refuses. "+
			"Apply the same ALTER on the target via the drained model. %s",
		tableName, after.Name, before.Type, after.Type, hint,
	)
}
