// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// TableReplayIdempotent reports whether replaying a chain window of
// [ir.Change] events onto table converges regardless of overlap — i.e.
// whether the engines' idempotent applier path (ADR-0010: INSERT
// upserts on a key; UPDATE/DELETE tolerate zero affected rows) has a
// key to collide on. True when the table declares a PRIMARY KEY or
// carries at least one all-NOT-NULL plain-column non-partial UNIQUE
// index; false for a truly keyless table, whose applier fallback is
// plain INSERT — replaying an overlapping window there duplicates rows
// silently.
//
// It judges the RECORDED table, whose columns are by definition the
// columns a replayed row carries, so every key it names is supplied by
// the rows. Whether the TARGET's key is supplied is a separate question,
// answered by each engine's [ir.ReplayKeyProber] against
// [ReplaySuppliedColumns].
//
// The keyed-ness derivation mirrors the engines' Bug-125 cold-start
// guard (effectiveUpsertKeyColumns / pickNonNullUniqueIndex in
// internal/engines/{mysql,postgres}/row_writer_batch.go), with the
// same three exclusions and the same reasoning:
//
//   - a UNIQUE index over a NULLABLE column is NOT eligible — both
//     engines allow multiple rows with NULL in a UNIQUE column, so the
//     key wouldn't reliably collide on replay (the same silent-
//     duplicate hazard as no key at all);
//   - an expression/functional index member (IndexColumn.Expression
//     set) is NOT eligible — it can't be a stable upsert conflict key;
//   - a PARTIAL index (Index.Predicate set) is NOT eligible — it
//     constrains only the rows its predicate selects, so a replayed row
//     outside the predicate collides with nothing, and Postgres's
//     picker skips it for the same reason (an unconditional ON CONFLICT
//     cannot infer it). Only Postgres and SQLite sources record one; a
//     MySQL target refuses a partial UNIQUE at translation
//     (checkIndexPredicate), which is why the MySQL picker never meets
//     one and does not test for it.
//
// DEFERRABLE keys are deliberately still eligible. Whether a key can
// arbitrate an upsert is a property of the TARGET's catalog, not of the
// recorded schema: a recorded DEFERRABLE UNIQUE constraint lands as a
// plain immediate UNIQUE on every target (no emitter reads
// [ir.Index.ConstraintDeferrable] on a UNIQUE index), a MySQL target has
// no deferrable keys at all, and a Postgres target that does carry a
// DEFERRABLE primary key makes the applier — and the F-E1 target probe —
// refuse loudly (SLUICE-E-TARGET-DEFERRABLE-KEY) rather than fall back to
// a plain INSERT. None of those is silent duplication, so excluding them
// here would only refuse working configurations (the Bug 211 reasoning).
//
// It is the recorded half of [JudgeReplayKey], which is what every path
// that re-writes rows onto a TARGET must call — the F-E1 replay doors and
// the engines' in-run retry gates (audit B-9). Called alone it answers
// only for the recorded table, which is right for exactly one caller: the
// backup orchestrator's anchored-resume guard (task #42, ADR-0085), which
// runs where no target exists. A new caller that gates a re-write onto a
// target with this function alone repeats the F-E1 retry-gate defect.
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
		if idx == nil || !idx.Unique || len(idx.Columns) == 0 || strings.TrimSpace(idx.Predicate) != "" {
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

// ReplaySuppliedColumns returns the names of the columns a replayed row of
// the recorded table carries: its non-generated columns, in declaration
// order. It is the "supplied" side of every [ir.ReplayKeyProber]
// judgment — a target key collides with a replayed row only when every one
// of its columns is among these. A generated column is excluded because no
// writer binds one: the restore writers insert only non-generated columns
// and the appliers drop generated ones from the INSERT
// (internal/engines/appliershared.NonGeneratedRowKeys).
func ReplaySuppliedColumns(table *ir.Table) []string {
	if table == nil {
		return nil
	}
	out := make([]string, 0, len(table.Columns))
	for _, c := range table.Columns {
		if c != nil && !c.IsGenerated() {
			out = append(out, c.Name)
		}
	}
	return out
}
