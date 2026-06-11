// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import "sluicesync.dev/sluice/internal/ir"

// TableReplayIdempotent reports whether replaying a chain window of
// [Change] events onto table converges regardless of overlap — i.e.
// whether the engines' idempotent applier path (ADR-0010: INSERT
// upserts on a key; UPDATE/DELETE tolerate zero affected rows) has a
// key to collide on. True when the table declares a PRIMARY KEY or
// carries at least one all-NOT-NULL plain-column UNIQUE index; false
// for a truly keyless table, whose applier fallback is plain INSERT —
// replaying an overlapping window there duplicates rows silently.
//
// The keyed-ness derivation mirrors the engines' Bug-125 cold-start
// guard (effectiveUpsertKeyColumns / pickNonNullUniqueIndex in
// internal/engines/{mysql,postgres}/row_writer_batch.go), with the
// same two exclusions and the same reasoning:
//
//   - a UNIQUE index over a NULLABLE column is NOT eligible — both
//     engines allow multiple rows with NULL in a UNIQUE column, so the
//     key wouldn't reliably collide on replay (the same silent-
//     duplicate hazard as no key at all);
//   - an expression/functional index member (IndexColumn.Expression
//     set) is NOT eligible — it can't be a stable upsert conflict key.
//
// Used by the backup orchestrator's anchored-resume guard (task #42,
// ADR-0085): a resumed full's re-streamed tables overlap the chain's
// replay window, which is sound only for tables this reports true for.
func TableReplayIdempotent(table *ir.Table) bool {
	if table == nil {
		return false
	}
	if table.PrimaryKey != nil && len(table.PrimaryKey.Columns) > 0 {
		return true
	}
	notNull := make(map[string]bool, len(table.Columns))
	for _, c := range table.Columns {
		if c != nil && !c.Nullable {
			notNull[c.Name] = true
		}
	}
	for _, idx := range table.Indexes {
		if idx == nil || !idx.Unique || len(idx.Columns) == 0 {
			continue
		}
		eligible := true
		for _, c := range idx.Columns {
			if c.Expression != "" || !notNull[c.Column] {
				eligible = false
				break
			}
		}
		if eligible {
			return true
		}
	}
	return false
}
