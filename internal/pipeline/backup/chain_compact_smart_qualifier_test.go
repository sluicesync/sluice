// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Smart compaction was INERT on every MySQL-family source, and said so in a
// way that read as a property of the operator's schema (Bug 223 / roadmap
// item 119).
//
// A MySQL change event carries Schema = the DATABASE name, because that is
// what the binlog gives it. The manifest's IR records Schema = "" for a
// single-database MySQL source — the schema reader's namespaceName() returns
// the database name only in multi-database mode. The lookup compared the two
// for equality, so it could never match: every table fell through to
// passthrough, `events_collapsed` was always 0, and the report listed tables
// with a plain INT PRIMARY KEY under "tables without a primary key".
//
// Both halves are pinned here, because the second is what kept the first
// hidden. A green "collapse ran" cell alone would have let the misleading
// report survive, and the misleading report is why three releases went by.
//
// The non-vacuity requirement the item spelled out: assert the collapse
// HAPPENED (events out < events in, rowsCollapsed > 0), not merely that the
// run succeeded. A MySQL leg that is byte-identical on two binaries scores a
// clean pass when nothing ever collapsed.

package backup

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// unqualifiedUsersSchema is the shape a single-database MySQL source
// produces: a real PK, and NO schema qualifier on the table.
func unqualifiedUsersSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Schema: "", Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "name", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{
			Name: "PRIMARY", Unique: true,
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}}}
}

// mysqlShapedEvents are what the MySQL CDC reader emits: the DATABASE name in
// Schema, against a manifest that recorded no qualifier at all.
func mysqlShapedEvents(db string) []ir.Change {
	return []ir.Change{
		ir.Insert{Position: pos(100), Schema: db, Table: "users", Row: ir.Row{"id": int64(1), "name": "v0"}},
		ir.Update{
			Position: pos(110), Schema: db, Table: "users",
			Before: ir.Row{"id": int64(1), "name": "v0"},
			After:  ir.Row{"id": int64(1), "name": "v1"},
		},
	}
}

func TestSmartCompact_MySQLQualifierStillCollapses(t *testing.T) {
	emitted, res := runPolicy(t, unqualifiedUsersSchema(), mysqlShapedEvents("app_production"))

	// The INSERT+UPDATE pair must collapse to one INSERT carrying the
	// later value — the ADR-0064 policy this whole transform exists for.
	if got := kindsOf(emitted); got != "I" {
		t.Fatalf("emitted kinds = %q; want %q.\n\n"+
			"The events carry the DATABASE name as their schema and the manifest recorded none, which is "+
			"the ordinary single-database MySQL shape. If they do not collapse, smart compaction is inert "+
			"on every MySQL-family source (Bug 223).", got, "I")
	}
	if res.rowsCollapsed == 0 {
		t.Error("rowsCollapsed = 0; the collapse did not run.\n\n" +
			"This is the non-vacuity assertion: a cell that only checks the run succeeded is green even " +
			"when nothing was ever collapsed, which is exactly how this defect scored a clean pass for " +
			"three releases.")
	}
	if res.eventsAfter >= res.eventsBefore {
		t.Errorf("eventsBefore=%d eventsAfter=%d; the stream did not shrink", res.eventsBefore, res.eventsAfter)
	}

	// And the report must not describe this table at all — it has a PK and
	// it was matched.
	if len(res.tablesWithoutPKList()) != 0 {
		t.Errorf("tablesWithoutPK = %v; the table has an INT PRIMARY KEY. Reporting it as PK-less is the "+
			"half of Bug 223 that made the inertness look like the operator's schema problem",
			res.tablesWithoutPKList())
	}
	if len(res.tablesUnmatchedList()) != 0 {
		t.Errorf("tablesUnmatched = %v; the table WAS matched", res.tablesUnmatchedList())
	}
}

// A qualifier that matches nothing must be reported as a DEFECT, in its own
// bucket — not as "this table has no primary key", which is what the operator
// reads as "nothing to do here".
func TestSmartCompact_UnmatchedQualifierIsItsOwnBucket(t *testing.T) {
	schema := unqualifiedUsersSchema()
	schema.Tables[0].Name = "something_else" // nothing will match "users"

	_, res := runPolicy(t, schema, mysqlShapedEvents("app_production"))

	if got := res.tablesUnmatchedList(); len(got) != 1 || got[0] != "app_production.users" {
		t.Errorf("tablesUnmatched = %v; want exactly [app_production.users].\n\n"+
			"An event naming a table the manifest does not carry means the compaction silently did "+
			"nothing for it — a sluice defect, which needs a different response from a table that "+
			"genuinely declares no PK.", got)
	}
	if got := res.tablesWithoutPKList(); len(got) != 0 {
		t.Errorf("tablesWithoutPK = %v; an unmatched qualifier is NOT a missing primary key, and "+
			"conflating them is the reporting half of Bug 223", got)
	}
}

// The genuine no-PK case must keep its own bucket and stay out of the defect
// one — the control that stops the fix from simply renaming the problem.
func TestSmartCompact_GenuineNoPKStaysInItsOwnBucket(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Schema: "", Name: "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}, {Name: "name", Type: ir.Text{}}},
	}}}

	_, res := runPolicy(t, schema, mysqlShapedEvents("app_production"))

	if got := res.tablesWithoutPKList(); len(got) != 1 || got[0] != "app_production.users" {
		t.Errorf("tablesWithoutPK = %v; want exactly [app_production.users] — the table was FOUND and "+
			"genuinely declares no PK, which is expected and the operator's to act on", got)
	}
	if got := res.tablesUnmatchedList(); len(got) != 0 {
		t.Errorf("tablesUnmatched = %v; the table was matched, it just has no PK", got)
	}
}

// An EXACT qualifier match still wins, so a Postgres or multi-database MySQL
// manifest — where the recorded schema is real — is unaffected by the
// unqualified fallback.
func TestSmartCompact_ExactQualifierMatchIsUnchanged(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Schema: "public", Name: "users",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}, {Name: "name", Type: ir.Text{}}},
			PrimaryKey: &ir.Index{
				Name: "users_pkey", Unique: true,
				Columns: []ir.IndexColumn{{Column: "id"}},
			},
		},
	}}
	_, res := runPolicy(t, schema, mysqlShapedEvents("public"))
	if res.rowsCollapsed == 0 {
		t.Error("an exactly-qualified match stopped collapsing")
	}
}

// The hazard the item named: a name-only fallback can select the WRONG
// table's PK columns, and a wrong PK tuple means a wrong collapse — silent
// data loss on a BACKUP path, strictly worse than the inertness being fixed.
// Ambiguity must REFUSE to pick, and land in the defect bucket.
func TestSmartCompact_AmbiguousUnqualifiedNameRefusesToPick(t *testing.T) {
	mk := func(pkCol string) *ir.Table {
		return &ir.Table{
			Schema: "", Name: "users",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "other_id", Type: ir.Integer{Width: 64}},
				{Name: "name", Type: ir.Text{}},
			},
			PrimaryKey: &ir.Index{
				Name: "PRIMARY", Unique: true,
				Columns: []ir.IndexColumn{{Column: pkCol}},
			},
		}
	}
	// Two unqualified tables sharing a name cannot arise from a
	// single-namespace reader — which is precisely why it is asserted
	// rather than assumed. The whole item exists because an argument of
	// that shape had already been wrong once.
	schema := &ir.Schema{Tables: []*ir.Table{mk("id"), mk("other_id")}}

	emitted, res := runPolicy(t, schema, mysqlShapedEvents("app_production"))

	if res.rowsCollapsed != 0 {
		t.Fatalf("rowsCollapsed = %d; an ambiguous name must NOT be collapsed under a guessed PK.\n\n"+
			"Picking one of two candidate PK tuples yields a WRONG collapse, which is silent data loss "+
			"on a backup path — strictly worse than the inertness this item fixes.", res.rowsCollapsed)
	}
	if got := kindsOf(emitted); got != "IU" {
		t.Errorf("emitted kinds = %q; want IU (both events verbatim)", got)
	}
	if got := res.tablesUnmatchedList(); len(got) != 1 {
		t.Errorf("tablesUnmatched = %v; an ambiguous match belongs in the defect bucket so it is visible", got)
	}
}
