// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/engines/internal/namecollide"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// validateSQLiteViewNamespace refuses a schema in which a source VIEW would
// land on a name a TABLE — or an earlier view — already occupies on the
// SQLite target (roadmap item 147).
//
// # Why this is a silent-loss refusal, and why the asymmetry hid it
//
// SQLite's object namespace is flat: tables and views share it. [emitCreateView]
// emits `CREATE VIEW IF NOT EXISTS` (which keeps the view phase idempotent
// under `--resume`, the same reason the table and index emits carry it), and
// SQLite's IF-NOT-EXISTS test for a view asks whether a table-or-view of that
// name exists. So the second CREATE returns OK and creates NOTHING: the view
// the source declared is simply absent on the target, at exit 0.
//
// The reason nobody found this is the asymmetry with item 134's neighbour: the
// INDEX-vs-table case is LOUD, so an operator who has seen sluice refuse a name
// collision reasonably assumes the class is covered. Ground truth on
// modernc.org/sqlite, the driver this engine uses (pinned by
// TestSQLiteViewNamespaceGroundTruth so this comment is a checked claim rather
// than a hypothesis — the 2026-07-28 lesson):
//
//	CREATE TABLE "a" (v INT)                        -> ok
//	CREATE VIEW IF NOT EXISTS "a" AS SELECT 1       -> ok, NO-OP     <- item 147
//	CREATE VIEW            "a" AS SELECT 1          -> error: table "a" already exists
//	CREATE VIEW IF NOT EXISTS "A" AS SELECT 1       -> ok, NO-OP     (ASCII case)
//	CREATE VIEW IF NOT EXISTS "v1" AS SELECT 1      -> ok
//	CREATE VIEW IF NOT EXISTS "V1" AS SELECT 2      -> ok, NO-OP     (view vs view)
//	CREATE INDEX IF NOT EXISTS "a" ON "a"("v")      -> error: there is already a table named a
//	CREATE VIEW IF NOT EXISTS "ix" AS SELECT 1      -> error: there is already an index named ix
//
// The third line is the whole finding in one comparison: `IF NOT EXISTS` is
// exactly what converts a loud refusal into a silent drop. Lines 4 and 6 are why
// the comparison folds ASCII case, same as the index sibling. Lines 7–8 are why
// this check does NOT walk index names: a view colliding with an index, and an
// index colliding with a table or view, are both LOUD and therefore not this
// class.
//
// # Why a separate code from the index collision
//
// It shares item 134's mechanism (`IF NOT EXISTS` + a flat, case-folded
// namespace) and nothing else an operator acts on. The object lost is a view,
// the namespace is the table/view one rather than the index one, and the remedy
// renames a different object — so it carries
// [sluicecode.CodeSchemaViewNameCollision], not the index code. Borrowing the
// index code would route an operator to a remedy that does not apply.
//
// # Claim ORDER is the message's accuracy, not a detail
//
// Tables are claimed before views because that is the phase order the target
// sees (create tables → copy → indexes → constraints → views), so the prior
// claimant this reports is the object that would actually have survived. Views
// are claimed in schema order for the same reason.
func validateSQLiteViewNamespace(tables []*ir.Table, views []*ir.View) error {
	// The same ASCII fold as [validateSQLiteIndexNamespace], for the same
	// reason and with the same residual (it folds a superset of what SQLite
	// folds, so it over-refuses a non-ASCII case pair rather than
	// under-refusing).
	seen := namecollide.New[sqliteObject](strings.ToLower)
	for _, table := range tables {
		if table == nil || table.Name == "" {
			continue
		}
		// A duplicate TABLE name is deliberately not this function's refusal.
		// It is a real and worse collision (`CREATE TABLE IF NOT EXISTS`
		// no-ops and the second table's rows land in the first), but it is a
		// different object kind with a different message, and claiming
		// first-wins here keeps this function's report about views.
		seen.Claim(table.Name, sqliteObject{kind: "table", name: table.Name})
	}
	for _, view := range views {
		if view == nil || view.Name == "" {
			// A nameless view never reaches emitCreateView as a distinct
			// object, so there is nothing to collide.
			continue
		}
		prior, dup := seen.Claim(view.Name, sqliteObject{kind: "view", name: view.Name})
		if !dup {
			continue
		}
		return sluicecode.Wrap(
			sluicecode.CodeSchemaViewNameCollision,
			"rename the source view (or the object it collides with) so the two no longer resolve to the "+
				"same SQLite name, then re-run",
			fmt.Errorf(
				"sqlite: view-name collision: source view %q collides with source %s %q; SQLite keeps tables "+
					"and views in ONE namespace (compared case-insensitively) and sluice emits CREATE VIEW IF "+
					"NOT EXISTS, so creating the view would SILENTLY no-op and the target would be missing the "+
					"view the source declared, at exit 0 — rename one of the two source objects",
				view.Name, prior.kind, prior.name,
			),
		)
	}
	return nil
}

// sqliteObject is one claimant in SQLite's flat table/view namespace. The kind
// is carried because the refusal has to say WHICH object the view collided with
// — a table and a view lead the operator to different renames — which is the
// object-kind field namecollide's own doc anticipated.
type sqliteObject struct{ kind, name string }
