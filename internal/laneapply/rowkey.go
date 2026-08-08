// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"reflect"

	"sluicesync.dev/sluice/internal/ir"
)

// # Shared PK-change / row-identity helpers (ADR-0105 single-source)
//
// These pure helpers are correctness-relevant to the lane-routing decision:
// RowChangeSchemaTable pulls a row change's source schema+table, and
// PKChangedUpdate detects a key-migrating UPDATE that must take the barrier
// path (its old/new keys could hash to different lanes, so the global
// ordering of the old-key delete vs the new-key insert must be preserved).
// They lived MySQL-private in change_applier_concurrent.go; ADR-0105 moves
// them here so BOTH engine adapters single-source the PK-change-detection
// logic — a future subtlety in the key-change rule lands in exactly one
// place, not two (the Bug-74-class divergence risk a shared exactly-once
// core exists to remove).

// RowChangeSchemaTable returns the source schema + table of a row-bearing
// change (Insert/Update/Delete). Barrier-class events (Tx*/Truncate/
// SchemaSnapshot) never reach a lane adapter's routing decision, so they
// return ("", "") — the caller then routes ok=false to the barrier path.
func RowChangeSchemaTable(c ir.Change) (schema, table string) {
	switch v := c.(type) {
	case ir.Insert:
		return v.Schema, v.Table
	case ir.Update:
		return v.Schema, v.Table
	case ir.Delete:
		return v.Schema, v.Table
	}
	return "", ""
}

// PKChangedUpdate reports whether an Update changes any primary-key column
// value (Before vs After). A nil Before image (a source without before-rows)
// cannot be compared, so it returns false (route by the After key). Such
// PK-changing updates are rare; the caller routes a true result through the
// barrier path so the old-key and new-key effects stay globally ordered.
func PKChangedUpdate(u ir.Update, pkCols []string) bool {
	if u.Before == nil || u.After == nil {
		return false
	}
	for _, col := range pkCols {
		b, bok := u.Before[col]
		a, aok := u.After[col]
		if bok != aok || !valuesEqualForKey(b, a) {
			return true
		}
	}
	return false
}

// valuesEqualForKey compares two primary-key values for the PK-change check.
//
// It uses STRUCTURAL equality, not `==`, because a primary-key value is not
// necessarily a comparable Go kind. `==` on two interfaces holding an
// uncomparable dynamic type PANICS, and two such kinds reach a PK column on
// real, supported configurations (roadmap item 154):
//
//   - `[]any` — the IR contract for an ARRAY value (docs/value-types.md).
//     Postgres permits a PRIMARY KEY on an array column (btree `array_ops`),
//     both PG readers produce `[]any` for one (`postgres`'s decodeArray, and
//     pgtrigger's to_jsonb payload, whose `text[]` columns are explicitly
//     ACCEPTED by setup — only `json[]`/`jsonb[]` are refused), and PG's
//     Update always carries a Before image (a real OldTuple, or the key-only
//     one synthesizeKeyOnlyBefore copies out of After). So every UPDATE on
//     such a table reached `==` and crashed the concurrent apply path.
//   - `map[string]any` — pgtrigger's decode of a `jsonb` column, which PG
//     also permits as a PRIMARY KEY (btree `jsonb_ops`).
//
// The one behavioural divergence from the previous []byte special case,
// named rather than implied: a nil []byte and an empty []byte compared EQUAL
// under bytes.Equal and compare UNEQUAL here. That direction is the safe one
// — both callers treat "changed" as "take the barrier / don't coalesce" —
// and a PK column is NOT NULL, so the pair is not reachable from a live
// reader anyway. Pinned by TestValuesEqualForKey_FamilyMatrix.
//
// EQUALITY HERE MUST IMPLY SAME LANE. Both callers use a false result to
// SKIP the barrier, and the barrier exists because a key migration's old and
// new keys "could hash to different lanes" — so reporting equal for a pair
// the router would hash APART would silently reintroduce exactly the hazard.
// reflect.DeepEqual satisfies that (equal values encode identically under
// [WriteCanonicalKeyValue], which is deterministic on the value), and
// TestValuesEqualForKey_ImpliesSameLane binds the two functions so a change
// to EITHER side fails the build rather than splitting a row across lanes.
func valuesEqualForKey(a, b any) bool {
	return reflect.DeepEqual(a, b)
}
