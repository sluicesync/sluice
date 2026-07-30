// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import "sluicesync.dev/sluice/internal/ir"

// ArmForeignKeyConsistency declares to a freshly-opened target
// [ir.SchemaWriter] (via the optional [ir.ForeignKeyConsistencyDeclarer]
// surface) whether the bulk-copied child rows are already consistent with the
// foreign keys the constraints phase will create — roadmap item 109.
//
// consistent is true ONLY for the safe-by-construction path: an UNFILTERED
// migrate (no `--where` row filters) from a source that enforced those FKs,
// whose reparent reconciliation (ADR-0141) re-derives every touched table to
// match that source before the constraints phase. A `--where` run can orphan
// children by filtering a parent, so it passes false and the target keeps its
// loud FK validation. Only the migrate orchestrator calls this — sync
// cold-start / add-table open their own writers and never arm it.
//
// Engine-neutral: a target without the setter (today: PG — it adds FKs
// `NOT VALID` then validates, with no statement-time wall to route around —
// and SQLite) skips cleanly, and the orchestrator never learns what the
// declaration does. A MySQL/Vitess target uses it to add a constraint that
// would otherwise blow PlanetScale's errno-3024 statement wall metadata-only.
func ArmForeignKeyConsistency(target any, consistent bool) {
	if setter, ok := target.(ir.ForeignKeyConsistencyDeclarer); ok {
		setter.SetCopiedRowsForeignKeyConsistent(consistent)
	}
}
