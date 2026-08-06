// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func vnsTable(name string) *ir.Table {
	return &ir.Table{
		Name:    name,
		Columns: []*ir.Column{{Name: "v", Type: ir.Text{Size: ir.TextLong}}},
	}
}

func vnsView(name string) *ir.View {
	return &ir.View{Name: name, Definition: "SELECT v FROM " + name + "_src"}
}

// TestSQLiteViewNamespace_RefusesTheSilentNoOp walks the shapes SQLite's flat,
// case-folding table/view namespace actually produces (roadmap item 147) — and,
// in the same table, the neighbours that must still PASS, so the refusal cannot
// be satisfied by refusing everything.
func TestSQLiteViewNamespace_RefusesTheSilentNoOp(t *testing.T) {
	cases := []struct {
		name    string
		tables  []*ir.Table
		views   []*ir.View
		refuses bool
	}{
		{
			// THE HEADLINE. `CREATE VIEW IF NOT EXISTS "orders"` against a
			// table `orders` returns OK and creates nothing.
			name:    "view name taken by a table",
			tables:  []*ir.Table{vnsTable("orders")},
			views:   []*ir.View{vnsView("orders")},
			refuses: true,
		},
		{
			// The same no-op wearing a different case — SQLite compares
			// identifiers case-insensitively for ASCII even inside double
			// quotes. A PostgreSQL source can hold both spellings at once.
			name:    "view name taken by a table differing only in ASCII case",
			tables:  []*ir.Table{vnsTable("Orders")},
			views:   []*ir.View{vnsView("orders")},
			refuses: true,
		},
		{
			// View versus view: two source views that fold to one SQLite name.
			name:    "two views folding to one name",
			views:   []*ir.View{vnsView("v_daily"), vnsView("V_Daily")},
			refuses: true,
		},
		{
			name:    "distinct names are fine",
			tables:  []*ir.Table{vnsTable("orders")},
			views:   []*ir.View{vnsView("orders_summary"), vnsView("v_daily")},
			refuses: false,
		},
		{
			// The pair that is LOUD on SQLite and therefore NOT this check's
			// business: an INDEX named like the view. `CREATE VIEW IF NOT
			// EXISTS "ix"` against an index `ix` errors ("there is already an
			// index named ix"), so refusing it here would over-refuse — and
			// the index namespace is item 134's, with its own code.
			name: "a view sharing an index name is left to the loud path",
			tables: []*ir.Table{{
				Name:    "orders",
				Columns: []*ir.Column{{Name: "v", Type: ir.Text{Size: ir.TextLong}}},
				Indexes: []*ir.Index{{Name: "ix", Columns: idxCol("v")}},
			}},
			views:   []*ir.View{vnsView("ix")},
			refuses: false,
		},
		{
			name:    "a nil view is skipped, not dereferenced",
			tables:  []*ir.Table{vnsTable("orders")},
			views:   []*ir.View{nil, vnsView("orders_summary")},
			refuses: false,
		},
		{
			name:    "a nil table is skipped, not dereferenced",
			tables:  []*ir.Table{nil, vnsTable("orders")},
			views:   []*ir.View{vnsView("orders_summary")},
			refuses: false,
		},
		{
			// An unnamed view never reaches emitCreateView as a distinct
			// object; it must not claim the empty string and collide with the
			// next unnamed one.
			name:    "unnamed views do not collide with each other",
			views:   []*ir.View{{Definition: "SELECT 1"}, {Definition: "SELECT 2"}},
			refuses: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSQLiteViewNamespace(tc.tables, tc.views)
			if !tc.refuses {
				if err != nil {
					t.Fatalf("must not refuse: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("accepted a colliding view namespace; CREATE VIEW IF NOT EXISTS would SILENTLY " +
					"no-op and the target would be left without the view the source declared")
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeSchemaViewNameCollision {
				t.Errorf("refusal must carry %s (NOT the index code — different object, different remedy); "+
					"got %+v (ok=%v)", sluicecode.CodeSchemaViewNameCollision, ce, ok)
			}
			// The message must name the view and the object it collided with,
			// or the operator cannot act on it.
			for _, v := range tc.views {
				if v == nil || v.Name == "" {
					continue
				}
				if !strings.Contains(err.Error(), v.Name) {
					t.Errorf("refusal must name source view %q; got %q", v.Name, err.Error())
				}
			}
		})
	}
}

// TestSQLiteViewNamespace_NamesTheColldingObjectKind pins the half that makes
// the refusal actionable: a view colliding with a TABLE and a view colliding
// with another VIEW lead the operator to different renames, so the message has
// to say which it was.
func TestSQLiteViewNamespace_NamesTheColldingObjectKind(t *testing.T) {
	err := validateSQLiteViewNamespace([]*ir.Table{vnsTable("orders")}, []*ir.View{vnsView("orders")})
	if err == nil || !strings.Contains(err.Error(), `source table "orders"`) {
		t.Errorf("table collision must name the prior claimant as a TABLE; got %v", err)
	}
	err = validateSQLiteViewNamespace(nil, []*ir.View{vnsView("v_daily"), vnsView("V_DAILY")})
	if err == nil || !strings.Contains(err.Error(), `source view "v_daily"`) {
		t.Errorf("view collision must name the prior claimant as a VIEW; got %v", err)
	}
}

// TestSQLiteViewNamespace_ReachesEveryWriteDoor is the sibling-sweep half: the
// check is wired into the pre-copy phase, the phase where the no-op would
// actually happen, AND the connection-free preflight — because they are reached
// by different runs (`--resume`, restore and a `sync` cold start onto an
// existing target each skip one of the first two).
func TestSQLiteViewNamespace_ReachesEveryWriteDoor(t *testing.T) {
	schema := &ir.Schema{
		Tables: []*ir.Table{vnsTable("orders")},
		Views:  []*ir.View{vnsView("orders")},
	}

	db, err := sql.Open("sqlite", "file:viewns?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	w := &SchemaWriter{db: db, path: "viewns"}
	ctx := context.Background()

	doors := map[string]func() error{
		"CreateTablesWithoutConstraints": func() error { return w.CreateTablesWithoutConstraints(ctx, schema) },
		"CreateViews":                    func() error { return w.CreateViews(ctx, schema) },
		"Engine.PreflightViews":          func() error { return Engine{}.PreflightViews(schema) },
	}
	for name, run := range doors {
		t.Run(name, func(t *testing.T) {
			err := run()
			if err == nil {
				t.Fatal("door accepted the colliding view namespace")
			}
			if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeSchemaViewNameCollision {
				t.Errorf("refusal must carry %s; got %v", sluicecode.CodeSchemaViewNameCollision, err)
			}
		})
	}
}

// TestSQLiteViewNamespaceGroundTruth is the premise-naming step for every fact
// the refusal — and its deliberate SCOPE — rests on, asserted against the REAL
// driver rather than left as a comment. Four claims:
//
//  1. `CREATE VIEW IF NOT EXISTS` against a name a TABLE holds is a SILENT
//     no-op. That silence is item 147.
//  2. Without IF NOT EXISTS the same statement is LOUD — so the no-op is a
//     property of the form sluice emits, not of SQLite's namespace alone.
//  3. The comparison folds ASCII case, for a table and for a view alike.
//  4. The pairs this check deliberately does NOT walk are loud: a view named
//     like an INDEX, and an index named like a TABLE. If either became silent,
//     the scope note in [validateSQLiteViewNamespace] would be wrong and this
//     is what would say so.
func TestSQLiteViewNamespaceGroundTruth(t *testing.T) {
	db, err := sql.Open("sqlite", "file:viewfold?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	for _, stmt := range []string{
		`CREATE TABLE "a" (v TEXT)`,
		`CREATE INDEX "ix" ON "a" ("v")`,
		`CREATE VIEW IF NOT EXISTS "v1" AS SELECT 1 AS x`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	// (1) and (3): silent no-ops, exact-case and case-folded, against a table
	//     and against a view.
	for _, stmt := range []string{
		`CREATE VIEW IF NOT EXISTS "a" AS SELECT 1`,
		`CREATE VIEW IF NOT EXISTS "A" AS SELECT 1`,
		`CREATE VIEW IF NOT EXISTS "V1" AS SELECT 2 AS y`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("SQLite now ERRORS on %s (%v). The refusal exists because this is silent; if it has "+
				"become loud, re-derive the finding before relaxing anything.", stmt, err)
		}
	}
	var views int
	if err := db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_schema WHERE type='view'`,
	).Scan(&views); err != nil {
		t.Fatalf("count views: %v", err)
	}
	if views != 1 {
		t.Fatalf("database holds %d view(s); the three CREATE VIEW statements above were expected to be "+
			"silent no-ops on top of the one real view, which is what makes the loss invisible", views)
	}

	// (2) and (4): the neighbours that are LOUD. Each carries the scope note
	//     it is evidence for.
	for _, tc := range []struct{ stmt, why string }{
		{
			`CREATE VIEW "a" AS SELECT 1`,
			"IF NOT EXISTS is what makes the view-vs-table collision silent; without it SQLite refuses",
		},
		{
			`CREATE VIEW IF NOT EXISTS "ix" AS SELECT 1`,
			"a view colliding with an INDEX is loud, which is why validateSQLiteViewNamespace does not walk index names",
		},
		{
			`CREATE INDEX IF NOT EXISTS "a" ON "a" ("v")`,
			"an index colliding with a TABLE is loud — item 134's own scope note",
		},
	} {
		if _, err := db.ExecContext(ctx, tc.stmt); err == nil {
			t.Errorf("%s now SUCCEEDS; expected a loud error (%s). A pair that became silent is a new "+
				"instance of this class, not a relaxation.", tc.stmt, tc.why)
		}
	}
}
