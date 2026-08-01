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
// migrate OR an unfiltered sync cold-start (item 112; no `--where` row filters)
// from a source that enforced those FKs. A `--where` run can orphan children by
// filtering a parent, so it passes false and the target keeps its loud FK
// validation. Both the migrate orchestrator (migrate.go / migrate_multidb.go)
// and the sync cold-start (streamer_coldstart.go / streamer_multidb.go) call
// this on their freshly-opened writers; add-table and warm-resume do not (they
// create no constraints). The metadata-only add it enables is always followed
// by the writer's bounded chunked orphan scan, which is the load-bearing
// FK-correctness net — identical guarantee to full validation, minus the
// re-validation that would blow the statement-time wall.
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
