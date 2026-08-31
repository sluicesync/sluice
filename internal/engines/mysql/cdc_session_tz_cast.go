// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// The MySQL lane's member of the session-GUC-cast refusal class
// (audit 2026-08-31 SL-2; the Postgres member is
// postgres.unforwardableSessionTZCast).
//
// # The mechanism
//
// `ALTER TABLE t MODIFY c DATETIME` on a `TIMESTAMP` column — and the
// reverse — converts every stored value using the EXECUTING SESSION's
// `time_zone`, not the server's and not anything carried on the wire.
// Observed on MySQL 8.0.46: one stored value written at UTC reads back as
// `2020-01-01 21:00:00` when the ALTER ran under `SET time_zone='+09:00'`
// and stays `2020-01-01 12:00:00` when it ran at `+00:00`. The reverse
// direction diverges by the same offset with the opposite sign.
//
// # Why this is worse than the Postgres twin, not better
//
// sluice pins `time_zone='+00:00'` on every connection it opens
// (connect.go's finishParseDSN), so the TARGET side of a forwarded MODIFY
// is deterministic. The SOURCE operator's ALTER ran under THEIR session,
// and MySQL's shipped default is `time_zone=SYSTEM` — the host zone. So
// the divergence does not need two deliberately-mismatched settings the
// way the PG case does; it is the DEFAULT outcome on any non-UTC source
// host. Every pre-existing target row of that column ends up off by the
// source host's UTC offset, every row applied after the ALTER is correct,
// and the sync exits 0. `verify --depth count` is unaffected — the row
// counts match exactly.
//
// Nothing on the binlog or the VStream wire carries the ALTER session's
// `time_zone`, and no target-side pin can close the hazard, because the
// source setting stays unknowable. So the shape refuses, matching the PG
// lane's posture: loud, naming the column and the mechanism, pointing at
// the drained model.
//
// # Scope, stated so it cannot be read as broader
//
// This covers the ZONE-SIBLING SWAP only: `TIMESTAMP` (MySQL's one
// zone-aware temporal, `ir.Timestamp{WithTimeZone:true}`) against
// `DATETIME` (its zone-naive sibling, `ir.DateTime`), either direction, at
// any precision. It deliberately does NOT cover:
//
//   - precision-only changes within one type (`DATETIME(3)→DATETIME(6)`),
//     which carry no zone conversion and must keep forwarding;
//   - temporal→string MODIFYs (`TIMESTAMP`→`VARCHAR`), which also render
//     in the session zone but are a different class (a representation
//     change, not a re-cast in place) and are not enumerated here;
//   - `DATETIME`→`TIME`/`DATE`, which truncate rather than re-zone.
//
// MySQL has no array types, so there is no array axis here — that half of
// the class is Postgres-only.

// schemaDeltaTargetApplySetter mirrors the pipeline's optional
// arming surface (pipeline.schemaDeltaTargetApplySetter) so all THREE of
// this engine's ir.SchemaSnapshot emitters are pinned at compile time to
// implement it. The pipeline discovers the surface by type assertion, so a
// method renamed on one lane would silently stop arming that lane's
// refusal — the exact "capability DISCOVERY fails quietly" shape
// pipeline.TestEveryRuntimeDispatchedPipelineSurfaceIsPinnedOrFrozen
// exists for. That gate can only reach the EXPORTED CDCReader from outside
// the package; these two unexported lanes can only be pinned here.
type schemaDeltaTargetApplySetter interface {
	SetSchemaDeltaAppliesToTarget(enabled bool)
}

var (
	_ schemaDeltaTargetApplySetter = (*CDCReader)(nil)
	_ schemaDeltaTargetApplySetter = (*vstreamCDCReader)(nil)
	_ schemaDeltaTargetApplySetter = (*vstreamSnapshotChanges)(nil)
)

// sessionTZSwapPair reports a MySQL column type change between the
// zone-aware temporal type and its zone-naive sibling, in either
// direction, naming the pair for the refusal text. Precision is ignored on
// purpose: `TIMESTAMP(3)`→`DATETIME(6)` re-zones every value exactly as
// the bare pair does, while `DATETIME(3)`→`DATETIME(6)` is not a swap at
// all (both sides are the same Go type with the same zone flag) and keeps
// forwarding.
//
// It keys on the IR projection rather than on the information_schema
// `data_type` string so all three of this engine's CDC lanes share one
// predicate: the binlog reader projects through translateType off
// information_schema, and both VStream lanes project through
// projectVStreamFields → the same translateType. A future MySQL flavor
// that resolves types by yet another route is covered the moment its
// projection lands in the IR.
func sessionTZSwapPair(prev, cur ir.Type) (string, bool) {
	zoned := func(t ir.Type) (family string, withZone, ok bool) {
		switch v := t.(type) {
		case ir.Timestamp:
			return "timestamp", v.WithTimeZone, true
		case ir.DateTime:
			// MySQL DATETIME is wall-clock: stored and returned
			// unconverted, which is exactly why the cast to TIMESTAMP
			// has to invent a zone to interpret it in.
			return "timestamp", false, true
		}
		return "", false, false
	}
	prevFamily, prevZoned, prevOK := zoned(prev)
	curFamily, curZoned, curOK := zoned(cur)
	if !prevOK || !curOK || prevFamily != curFamily || prevZoned == curZoned {
		return "", false
	}
	return "TIMESTAMP and DATETIME", true
}

// unforwardableSessionTZColumn scans a rebuilt table against the last
// snapshotted signature for a column whose type moved across the
// zone-sibling pair, returning that column and the pair name.
//
// prev is the reader's own [ir.SchemaSignature] memo — the LAST
// SNAPSHOTTED shape of this table, which is precisely the pre-state the
// pipeline's forward intercept will classify this boundary against
// (schema_forward_intercept.go caches each emitted snapshot as the next
// boundary's `pre`). That alignment is deliberate: it means this refusal
// fires on exactly the boundaries a forward path would act on, and stays
// silent on the ones it would not.
//
// A column absent from prev (ADD COLUMN, or a table the reader has never
// snapshotted) is skipped — there is no prior type to have swapped away
// from, and inventing one would be a guess. The reader having no memo for
// a table is the honest "no prior knowledge" case, the same shape as PG's
// missing relation-cache entry; it is also the boundary the intercept
// treats as a cache prime or a seed-guarded no-op rather than an ALTER.
func unforwardableSessionTZColumn(prev ir.SchemaSignature, cur *ir.Table) (col, pair string, found bool) {
	if cur == nil {
		return "", "", false
	}
	prevTypes := prev.ColumnTypes()
	for _, c := range cur.Columns {
		if c == nil {
			continue
		}
		prevType, ok := prevTypes[c.Name]
		if !ok {
			continue
		}
		if p, matched := sessionTZSwapPair(prevType, c.Type); matched {
			return c.Name, p, true
		}
	}
	return "", "", false
}

// sessionTZCastRefusal is the loud stream-killing error the three MySQL
// CDC boundary paths raise for a zone-sibling swap under a mode that would
// re-apply the delta to the target. Shape and honesty match the Postgres
// twin's (postgres.checkSchemaRace's unforwardableSessionTZCast arm): name
// the table, name the column, name the MECHANISM rather than the symptom,
// and point at the drained model.
func sessionTZCastRefusal(schema, table, col, pair string) error {
	qualified := table
	if schema != "" {
		qualified = schema + "." + table
	}
	return fmt.Errorf(
		"mysql: cdc: schema change mid-stream on %s is detected but cannot be forwarded: column %q changed between %s — MySQL converts the stored values using the EXECUTING SESSION's time_zone, so the source's ALTER resolved every row against the SOURCE session's time_zone (the shipped default is SYSTEM, i.e. the source host's zone) while a forwarded ALTER would re-cast the target's pre-existing rows against sluice's own pinned time_zone='+00:00' — when the two differ the casts silently disagree at exit 0, so this swap refuses. Apply the same ALTER on the target via the drained model. %s",
		qualified, col, pair, schemaChangeRecoveryHint,
	)
}

// schemaChangeRecoveryHint is the operator-actionable recovery text this
// engine's mid-stream schema refusals append. Deliberately worded to match
// the Postgres reader's schemaRaceRecoveryHint so an operator running both
// directions sees one workflow; the position wording differs because MySQL
// resumes from a GTID set / binlog coordinate rather than an LSN.
const schemaChangeRecoveryHint = "sluice does not support this DDL shape mid-stream. Drained-model recovery: " +
	"(1) `sluice sync stop --wait` on every shard, " +
	"(2) apply the schema change via your migration tool on source AND target, " +
	"(3) `sluice sync start` with the SAME --stream-id to continue from the last applied position " +
	"(there is no resume flag: a restart looks up the persisted position and skips the " +
	"snapshot + bulk-copy phase)."
