// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"fmt"

	"sluicesync.dev/sluice/internal/engines/internal/namecollide"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// SQLite keeps tables and views in ONE flat, ASCII-case-folded namespace, and
// this file is the single claim pass over it. Two refusals are reported off
// that one pass — a TABLE that lands on a name already taken (roadmap item
// 148) and a VIEW that does (item 147) — because they are two answers to one
// question about one namespace, and writing them as two independent walks is
// exactly how the two would drift.
//
// # Ground truth, measured rather than assumed
//
// modernc.org/sqlite, the driver this engine uses, pinned by
// [TestSQLiteObjectNamespaceGroundTruth]:
//
//	CREATE TABLE IF NOT EXISTS "a"  vs table a  -> ok, NO-OP   <- item 148
//	CREATE TABLE            "a"     vs table a  -> error: table "a" already exists
//	CREATE TABLE IF NOT EXISTS "A"  vs table a  -> ok, NO-OP   (ASCII case)
//	CREATE TABLE IF NOT EXISTS "vv" vs view vv  -> ok, NO-OP
//	CREATE TABLE            "vv"    vs view vv  -> error: view "vv" already exists
//	CREATE TABLE IF NOT EXISTS "ix" vs index ix -> error: there is already an index named ix
//	CREATE TABLE IF NOT EXISTS "É"  vs table é  -> ok, CREATES BOTH (no Unicode fold)
//	CREATE TABLE IF NOT EXISTS "CAFÉ_ORDER" vs table Café_Order
//	                                            -> ok, CREATES BOTH (the ASCII
//	                                               letters fold, the É does not)
//	CREATE VIEW IF NOT EXISTS "a"   vs table a  -> ok, NO-OP   <- item 147
//	CREATE VIEW            "a"      vs table a  -> error: table "a" already exists
//	CREATE VIEW IF NOT EXISTS "ix"  vs index ix -> error: there is already an index named ix
//	CREATE INDEX IF NOT EXISTS "a"  vs table a  -> error: there is already a table named a
//
// Three things that matrix settles, none of which was safe to assume:
//
//   - `IF NOT EXISTS` is what converts a loud refusal into a silent drop. Lines
//     2 and 5 are the same collisions as lines 1 and 4 without it, and they are
//     errors. The mechanism is sluice's emit form, not SQLite's flat namespace.
//   - The silence is NOT symmetric across object kinds, so this walk must not
//     claim index names. A table (or a view) landing on an INDEX name is a LOUD
//     error, with or without IF NOT EXISTS — refusing it here would break runs
//     that already fail correctly and visibly. That direction is pinned, in
//     both this file's tests and [validateSQLiteIndexNamespace]'s.
//   - The fold is ASCII-only on SQLite's side, so this walk folds with
//     [foldSQLiteIdentifier] and NOT with [strings.ToLower]. ToLower is
//     Unicode-aware and folds a strict SUPERSET, which made rows 7 and 8 above
//     — pairs a real SQLite target keeps as two objects — refuse. That was a
//     documented residual until the v0.115.0 regression cycle measured its
//     cost, and it is now closed (roadmap item 150).
//
// # Why the TABLE member is the worst of the three IF-NOT-EXISTS siblings
//
// A lost index or view is a MISSING object. A lost table is not missing: the
// rows destined for it are INSERTed under a name that resolves — by the same
// fold — to the table that won, so both tables' rows land in one table. The
// count is right for the surviving name and the run exits 0 with no failing
// statement and no warning.
//
// Corrected 2026-08-07, because this comment used to add "and `verify`
// compares that name against that name, and agrees" — which is FALSE, and was
// repeated into five other homes including the published operator doc.
// [Verify] iterates SOURCE tables and compares `srcCount != tgtCount`
// (`pipeline/verify.go:376`), so with `orders`=2 and `Orders`=2 against a
// merged target of 4, BOTH source tables mismatch. A later `verify --depth
// count` would catch this. What is silent is the MIGRATION: it reports
// success. That is still worth a preflight — an operator should not need a
// separate opt-in step to discover that two tables became one — but the
// stronger claim was not checked and was not true.
//
// # Claim ORDER is the message's accuracy, not a detail
//
// Tables are claimed before views because that is the phase order the target
// sees (create tables → copy → indexes → constraints → views), so the prior
// claimant reported is the object that would actually have survived. Within
// each kind, schema order, for the same reason.

// sqliteObject is one claimant in SQLite's flat table/view namespace. The kind
// is carried because a refusal has to say WHICH object was collided with — a
// table and a view lead the operator to different renames — which is the
// object-kind field namecollide's own doc anticipated.
type sqliteObject struct{ kind, name string }

// objectCollision is one loser and the claimant that beat it to the name.
type objectCollision struct {
	loser sqliteObject
	prior sqliteObject
}

// scanSQLiteObjectNamespace makes ONE claim pass over SQLite's flat table/view
// namespace in target phase order and returns every collision it found, in the
// order the emit would have hit them.
//
// It reports rather than refuses because the two callers below want different
// subsets of the same pass: the table door reports table losers, the view door
// reports view losers, and neither wants to pre-empt the other's message with
// a collision on an object kind it was not asked about.
//
// A nil/nameless entry is skipped: it never reaches [emitTableDef] or
// [emitCreateView] as a distinct object, so there is nothing to collide.
func scanSQLiteObjectNamespace(tables []*ir.Table, views []*ir.View) []objectCollision {
	seen := namecollide.New[sqliteObject](foldSQLiteIdentifier)
	var out []objectCollision
	claim := func(obj sqliteObject) {
		prior, dup := seen.Claim(obj.name, obj)
		if dup {
			out = append(out, objectCollision{loser: obj, prior: prior})
		}
	}
	for _, table := range tables {
		if table == nil || table.Name == "" {
			continue
		}
		claim(sqliteObject{kind: "table", name: table.Name})
	}
	for _, view := range views {
		if view == nil || view.Name == "" {
			continue
		}
		claim(sqliteObject{kind: "view", name: view.Name})
	}
	return out
}

// firstLoserOfKind returns the first collision whose LOSING object is of kind,
// so each door reports on the object kind its own error code names.
func firstLoserOfKind(collisions []objectCollision, kind string) (objectCollision, bool) {
	for _, c := range collisions {
		if c.loser.kind == kind {
			return c, true
		}
	}
	return objectCollision{}, false
}

// validateSQLiteTableNamespace refuses a schema in which two source TABLES
// would land on one SQLite table name (roadmap item 148).
//
// # Why this is the silent-loss refusal that loses ROWS
//
// A Postgres source legitimately holds `public.orders` and `public."Orders"`
// as two distinct relations. SQLite compares object names case-insensitively
// for ASCII, so both resolve to one name there, and [emitTableDef] writes
// `CREATE TABLE IF NOT EXISTS` with a BARE (never schema-qualified) name — so
// the second CREATE returns OK and creates nothing. The copy then INSERTs the
// second table's rows under a name that resolves to the FIRST table. Both
// tables' rows are in one table, the surviving name's count is right, and the
// run exits 0.
//
// The same collision arrives by a second route with no case-folding involved:
// a multi-namespace fan-out into SQLite would write every source namespace's
// bare names into one file. That route is refused earlier and by
// configuration, at [migcore.ValidateMultiNamespaceTarget], because a flat
// target cannot keep namespaces apart at all — not merely when two names
// happen to collide.
//
// # Views are NOT claimed against here, deliberately
//
// A table CAN silently no-op against an existing VIEW (line 4 of the ground
// truth above), but not within one sluice run: tables are created in the FIRST
// phase and views in the LAST, so no view sluice creates can be in the way of
// a table sluice creates. The reachable shape is a view that already exists on
// the target, which is target STATE and not a property of the source schema
// this connection-free check is given. Stated rather than left implied: that
// residual is uncovered here, and it fails loudly at the first INSERT (SQLite
// refuses INSERT into a view without an INSTEAD OF trigger) rather than
// silently.
func validateSQLiteTableNamespace(tables []*ir.Table) error {
	c, found := firstLoserOfKind(scanSQLiteObjectNamespace(tables, nil), "table")
	if !found {
		return nil
	}
	return sluicecode.Wrap(
		sluicecode.CodeSchemaTableNameCollision,
		"rename one of the two source tables so they no longer resolve to the same SQLite name, "+
			"then re-run",
		fmt.Errorf(
			"sqlite: table-name collision: source tables %q and %q both resolve to the SQLite "+
				"identifier %q; SQLite compares object names case-insensitively and sluice emits "+
				"CREATE TABLE IF NOT EXISTS with a bare name, so creating the second table would "+
				"SILENTLY no-op and ITS ROWS WOULD BE INSERTED INTO THE FIRST TABLE — both tables' "+
				"rows in one table, the right count under the surviving name, at exit 0 — rename one "+
				"of the two source tables",
			// The effective identifier is rendered with the SAME fold the
			// check keyed on, or the message names a name nothing computed.
			c.prior.name, c.loser.name, foldSQLiteIdentifier(c.loser.name),
		),
	)
}

// validateSQLiteViewNamespace refuses a schema in which a source VIEW would
// land on a name a TABLE — or an earlier view — already occupies on the
// SQLite target (roadmap item 147).
//
// # Why this is a silent-loss refusal, and why the asymmetry hid it
//
// [emitCreateView] emits `CREATE VIEW IF NOT EXISTS` (which keeps the view
// phase idempotent under `--resume`, the same reason the table and index emits
// carry it), and SQLite's IF-NOT-EXISTS test for a view asks whether a
// table-or-view of that name exists. So the second CREATE returns OK and
// creates NOTHING: the view the source declared is simply absent on the
// target, at exit 0.
//
// The reason nobody found this is the asymmetry with item 134's neighbour: the
// INDEX-vs-table case is LOUD, so an operator who has seen sluice refuse a name
// collision reasonably assumes the class is covered. See this file's ground
// truth for the measured matrix.
//
// # Why a separate code from the index collision, and from the table one
//
// All three share the `IF NOT EXISTS` mechanism and nothing an operator acts
// on. The object lost differs, the namespace differs for the index case, and
// the remedy renames a different object — so this carries
// [sluicecode.CodeSchemaViewNameCollision], the table case carries
// [sluicecode.CodeSchemaTableNameCollision], and neither borrows the index
// code. Borrowing would route an operator to a remedy that does not apply.
func validateSQLiteViewNamespace(tables []*ir.Table, views []*ir.View) error {
	c, found := firstLoserOfKind(scanSQLiteObjectNamespace(tables, views), "view")
	if !found {
		return nil
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
			c.loser.name, c.prior.kind, c.prior.name,
		),
	)
}
