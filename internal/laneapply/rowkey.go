// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"bytes"

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
// Byte slices ([]byte keys) need content comparison; everything else is a
// comparable scalar the decode path produces, so == is correct.
func valuesEqualForKey(a, b any) bool {
	ab, aIsBytes := a.([]byte)
	bb, bIsBytes := b.([]byte)
	if aIsBytes || bIsBytes {
		if !aIsBytes || !bIsBytes {
			return false
		}
		return bytes.Equal(ab, bb)
	}
	return a == b
}
