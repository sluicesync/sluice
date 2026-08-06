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

// TestSQLiteTableNamespace_RefusesTheSilentNoOp walks the TABLE half of the one
// namespace (roadmap item 148) — and, in the same table, the neighbours that
// must still PASS, because a check that refused every schema would satisfy the
// refusal half on its own.
func TestSQLiteTableNamespace_RefusesTheSilentNoOp(t *testing.T) {
	cases := []struct {
		name    string
		tables  []*ir.Table
		refuses bool
	}{
		{
			// THE HEADLINE, and the shape a PostgreSQL source hands over
			// without doing anything exotic: `public.orders` and
			// `public."Orders"` are two relations there and one name here.
			name:    "two tables differing only in ASCII case",
			tables:  []*ir.Table{vnsTable("orders"), vnsTable("Orders")},
			refuses: true,
		},
		{
			// The same collision with no case-folding involved — the shape the
			// multi-namespace fan-out produced before its own refusal landed.
			name:    "two tables with the identical name",
			tables:  []*ir.Table{vnsTable("orders"), vnsTable("orders")},
			refuses: true,
		},
		{
			// Table.Schema is NOT part of the key, deliberately: emitTableDef
			// writes a bare, never-qualified name, so two tables from two
			// source namespaces land on one SQLite name however different
			// their Schema fields are.
			name: "two tables in different source schemas still collide",
			tables: []*ir.Table{
				{Schema: "app", Name: "audit", Columns: []*ir.Column{{Name: "v", Type: ir.Text{Size: ir.TextLong}}}},
				{Schema: "billing", Name: "audit", Columns: []*ir.Column{{Name: "v", Type: ir.Text{Size: ir.TextLong}}}},
			},
			refuses: true,
		},
		{
			name:    "distinct names are fine",
			tables:  []*ir.Table{vnsTable("orders"), vnsTable("order_items")},
			refuses: false,
		},
		{
			// The over-refusal direction, pinned: a table sharing an INDEX
			// name is a LOUD SQLite error, so this walk must not claim index
			// names. Refusing here would break a run that already fails
			// visibly.
			name: "a table sharing an index name is left to the loud path",
			tables: []*ir.Table{
				{
					Name:    "orders",
					Columns: []*ir.Column{{Name: "v", Type: ir.Text{Size: ir.TextLong}}},
					Indexes: []*ir.Index{{Name: "ix", Columns: idxCol("v")}},
				},
				vnsTable("ix"),
			},
			refuses: false,
		},
		{
			name:    "a nil table is skipped, not dereferenced",
			tables:  []*ir.Table{nil, vnsTable("orders")},
			refuses: false,
		},
		{
			// An unnamed table never reaches emitTableDef as a distinct object;
			// it must not claim the empty string and collide with the next one.
			name:    "unnamed tables do not collide with each other",
			tables:  []*ir.Table{{}, {}},
			refuses: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSQLiteTableNamespace(tc.tables)
			if !tc.refuses {
				if err != nil {
					t.Fatalf("must not refuse: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("accepted a colliding table namespace; CREATE TABLE IF NOT EXISTS would SILENTLY " +
					"no-op and the second table's ROWS would be inserted into the first")
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeSchemaTableNameCollision {
				t.Errorf("refusal must carry %s (NOT the index or view code — different object, different "+
					"remedy); got %+v (ok=%v)", sluicecode.CodeSchemaTableNameCollision, ce, ok)
			}
			for _, tbl := range tc.tables {
				if tbl == nil || tbl.Name == "" {
					continue
				}
				if !strings.Contains(err.Error(), tbl.Name) {
					t.Errorf("refusal must name source table %q; got %q", tbl.Name, err.Error())
				}
			}
		})
	}
}

// TestSQLiteObjectNamespace_EachDoorReportsItsOwnObjectKind is the anti-drift
// pin for the unification: the table and view refusals are two reports off ONE
// claim pass, so a schema carrying BOTH kinds of collision must still give each
// door its own code and its own object. Without this, folding the two walks
// together could silently make one door answer for the other.
func TestSQLiteObjectNamespace_EachDoorReportsItsOwnObjectKind(t *testing.T) {
	tables := []*ir.Table{vnsTable("orders"), vnsTable("ORDERS"), vnsTable("customers")}
	views := []*ir.View{vnsView("customers")}

	tblErr := validateSQLiteTableNamespace(tables)
	if ce, ok := sluicecode.FromError(tblErr); !ok || ce.Code != sluicecode.CodeSchemaTableNameCollision {
		t.Fatalf("table door must report the TABLE collision with %s; got %v",
			sluicecode.CodeSchemaTableNameCollision, tblErr)
	}
	if !strings.Contains(tblErr.Error(), "ORDERS") || strings.Contains(tblErr.Error(), "view") {
		t.Errorf("table door reported the wrong object: %v", tblErr)
	}

	viewErr := validateSQLiteViewNamespace(tables, views)
	if ce, ok := sluicecode.FromError(viewErr); !ok || ce.Code != sluicecode.CodeSchemaViewNameCollision {
		t.Fatalf("view door must report the VIEW collision with %s even though a table collision is also "+
			"present in the same schema; got %v", sluicecode.CodeSchemaViewNameCollision, viewErr)
	}
	if !strings.Contains(viewErr.Error(), `source view "customers"`) {
		t.Errorf("view door reported the wrong object: %v", viewErr)
	}
}

// TestSQLiteTableNamespace_ReachesEveryWriteDoor is the sibling-sweep half for
// the table refusal. Note the asymmetry with the view sibling and why it is not
// an omission: the view check has a second in-engine door (CreateViews, the
// phase where the no-op happens), and the TABLE loss does not happen in a
// schema phase at all — it happens at INSERT, on a path with no schema
// argument. So the connection-free preflight is the only door that reaches a
// run whose tables already exist (`--resume`, restore, a cold start onto a
// populated target), which is exactly why item 148 needed a preflight surface
// rather than one more writer check.
func TestSQLiteTableNamespace_ReachesEveryWriteDoor(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{vnsTable("orders"), vnsTable("Orders")}}

	db, err := sql.Open("sqlite", "file:tablens?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	w := &SchemaWriter{db: db, path: "tablens"}
	ctx := context.Background()

	doors := map[string]func() error{
		"CreateTablesWithoutConstraints": func() error { return w.CreateTablesWithoutConstraints(ctx, schema) },
		"Engine.PreflightTables":         func() error { return Engine{}.PreflightTables(schema) },
	}
	for name, run := range doors {
		t.Run(name, func(t *testing.T) {
			err := run()
			if err == nil {
				t.Fatal("door accepted the colliding table namespace")
			}
			if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeSchemaTableNameCollision {
				t.Errorf("refusal must carry %s; got %v", sluicecode.CodeSchemaTableNameCollision, err)
			}
		})
	}
}

// TestSQLiteTableNamespaceGroundTruth is the premise-naming step for item 148,
// asserted against the REAL driver. It ends by REPRODUCING the corruption —
// rows written under the losing name landing in the winning table — because a
// refusal justified by a described failure mode is weaker than one justified by
// an observed one, and this failure mode's whole character is that nothing
// observes it.
func TestSQLiteTableNamespaceGroundTruth(t *testing.T) {
	db, err := sql.Open("sqlite", "file:tablefold?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	for _, stmt := range []string{
		`CREATE TABLE "orders" (v TEXT)`,
		`CREATE INDEX "ix" ON "orders" ("v")`,
		`CREATE VIEW "vv" AS SELECT v FROM "orders"`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	// (1) The silent no-ops: against the same table exactly, against it
	//     case-folded, and against a VIEW. Each returns OK.
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS "orders" (w INT)`,
		`CREATE TABLE IF NOT EXISTS "Orders" (w INT)`,
		`CREATE TABLE IF NOT EXISTS "vv" (w INT)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("SQLite now ERRORS on %s (%v). The refusal exists because this is silent; if it has "+
				"become loud, re-derive the finding before relaxing anything.", stmt, err)
		}
	}
	// …and created nothing: the ORIGINAL definition survives, which is what
	// makes the rows land in the wrong shape as well as the wrong table.
	var ddl string
	if err := db.QueryRowContext(
		ctx, `SELECT sql FROM sqlite_schema WHERE type='table' AND name='orders'`,
	).Scan(&ddl); err != nil {
		t.Fatalf("read orders DDL: %v", err)
	}
	if !strings.Contains(ddl, "v TEXT") || strings.Contains(ddl, "w INT") {
		t.Errorf("the second CREATE TABLE was expected to be a no-op leaving the FIRST definition in "+
			"place; sqlite_schema holds %q", ddl)
	}
	var tables int
	if err := db.QueryRowContext(
		ctx, `SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'`,
	).Scan(&tables); err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if tables != 1 {
		t.Fatalf("database holds %d table(s); the three CREATE TABLE statements above were expected to be "+
			"silent no-ops on top of the one real table", tables)
	}

	// (2) The neighbours that are LOUD, each carrying the scope note it is
	//     evidence for. Rows 1-2 are why the mechanism is sluice's emit FORM;
	//     rows 3-4 are why this walk does not claim index names.
	for _, tc := range []struct{ stmt, why string }{
		{
			`CREATE TABLE "orders" (w INT)`,
			"IF NOT EXISTS is what makes the table-vs-table collision silent; without it SQLite refuses",
		},
		{
			`CREATE TABLE "vv" (w INT)`,
			"same, for the table-vs-view pair",
		},
		{
			`CREATE TABLE IF NOT EXISTS "ix" (w INT)`,
			"a table colliding with an INDEX is loud, which is why validateSQLiteTableNamespace does not walk index names",
		},
		{
			`CREATE TABLE IF NOT EXISTS "IX" (w INT)`,
			"and it is loud case-folded too, so the over-refusal would not even be limited to the exact spelling",
		},
	} {
		if _, err := db.ExecContext(ctx, tc.stmt); err == nil {
			t.Errorf("%s now SUCCEEDS; expected a loud error (%s). A pair that became silent is a new "+
				"instance of this class, not a relaxation.", tc.stmt, tc.why)
		}
	}

	// (3) The fold is ASCII-ONLY on SQLite's side. strings.ToLower is
	//     Unicode-aware, so sluice folds a SUPERSET — a named, deliberate
	//     over-refusal. This is what says so rather than a comment claiming it.
	for _, stmt := range []string{
		`CREATE TABLE "é" (v TEXT)`,
		`CREATE TABLE IF NOT EXISTS "É" (v TEXT)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	var unicodeTables int
	if err := db.QueryRowContext(
		ctx, `SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name IN ('é','É')`,
	).Scan(&unicodeTables); err != nil {
		t.Fatalf("count unicode tables: %v", err)
	}
	if unicodeTables != 2 {
		t.Errorf("SQLite folded a NON-ASCII case pair (%d of 2 tables survived). sluice's strings.ToLower "+
			"fold is documented as an over-refusal precisely because SQLite's is ASCII-only; if SQLite "+
			"now folds Unicode, the over-refusal note is wrong in the other direction.", unicodeTables)
	}
	if err := validateSQLiteTableNamespace([]*ir.Table{vnsTable("é"), vnsTable("É")}); err == nil {
		t.Error("sluice's fold is documented as a SUPERSET of SQLite's — it should refuse this " +
			"non-ASCII case pair even though SQLite would have kept both. If that changed, the residual " +
			"note in validateSQLiteTableNamespace's file comment is stale.")
	}

	// (4) THE CORRUPTION, reproduced. An INSERT under the losing name resolves
	//     — by the same fold — to the table that won, so the rows of two source
	//     tables end up in one. The counts below are the independent oracle:
	//     they are read from the target's own catalog and data, not from any
	//     report sluice produces.
	if _, err := db.ExecContext(ctx, `INSERT INTO "orders" (v) VALUES ('from-orders')`); err != nil {
		t.Fatalf("insert into orders: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO "Orders" (v) VALUES ('from-Orders')`); err != nil {
		t.Fatalf("insert into Orders: %v", err)
	}
	var merged int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM "orders"`).Scan(&merged); err != nil {
		t.Fatalf("count orders: %v", err)
	}
	if merged != 2 {
		t.Fatalf("expected BOTH tables' rows to land in `orders` (that is the whole finding); got %d", merged)
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
