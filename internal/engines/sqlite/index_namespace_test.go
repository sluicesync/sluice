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

// idxCol is the one-column index list every fixture below uses.
func idxCol(name string) []ir.IndexColumn { return []ir.IndexColumn{{Column: name}} }

func nsTable(name string, idx ...*ir.Index) *ir.Table {
	return &ir.Table{
		Name:    name,
		Columns: []*ir.Column{{Name: "v", Type: ir.Text{Size: ir.TextLong}}},
		Indexes: idx,
	}
}

// TestSQLiteIndexNamespace_RefusesTheSilentNoOp walks the shapes SQLite's
// schema-scoped, case-folding index namespace actually produces (roadmap
// item 134) — and, in the same table, the neighbours that must still PASS, so
// the refusal cannot be satisfied by refusing everything.
func TestSQLiteIndexNamespace_RefusesTheSilentNoOp(t *testing.T) {
	cases := []struct {
		name    string
		tables  []*ir.Table
		refuses bool
	}{
		{
			// THE HEADLINE. MySQL auto-names a single-column index after its
			// column, so two tables carrying `user_id` is routine — and on
			// SQLite both emit `CREATE INDEX IF NOT EXISTS "user_id"`, so the
			// second is a silent no-op.
			name: "same index name on two tables",
			tables: []*ir.Table{
				nsTable("posts", &ir.Index{Name: "user_id", Columns: idxCol("v")}),
				nsTable("comments", &ir.Index{Name: "user_id", Unique: true, Columns: idxCol("v")}),
			},
			refuses: true,
		},
		{
			// SQLite compares identifiers case-insensitively for ASCII even
			// inside double quotes, so this is the same no-op wearing a
			// different case. Ground-truthed on modernc.org/sqlite; see
			// TestSQLiteIndexNamespace_CaseFoldIsGroundTruth below.
			name: "same index name differing only in ASCII case",
			tables: []*ir.Table{
				nsTable("posts", &ir.Index{Name: "idx_v", Columns: idxCol("v")}),
				nsTable("comments", &ir.Index{Name: "IDX_V", Columns: idxCol("v")}),
			},
			refuses: true,
		},
		{
			name: "duplicate index name within ONE table",
			tables: []*ir.Table{nsTable(
				"posts",
				&ir.Index{Name: "dup", Columns: idxCol("v")},
				&ir.Index{Name: "dup", Unique: true, Columns: idxCol("v")},
			)},
			refuses: true,
		},
		{
			// The counterpart to Postgres's transform: sluice prepends
			// `<table>_` there and NOT here, so a pair Postgres collapses is
			// two distinct SQLite names. Pinning it keeps the SQLite check
			// from being "whatever Postgres does".
			name: "the Postgres transform collision is NOT a SQLite collision",
			tables: []*ir.Table{nsTable(
				"posts",
				&ir.Index{Name: "user_id", Columns: idxCol("v")},
				&ir.Index{Name: "posts_user_id", Columns: idxCol("v")},
			)},
			refuses: false,
		},
		{
			name: "distinct index names on two tables",
			tables: []*ir.Table{
				nsTable("posts", &ir.Index{Name: "posts_v", Columns: idxCol("v")}),
				nsTable("comments", &ir.Index{Name: "comments_v", Columns: idxCol("v")}),
			},
			refuses: false,
		},
		{
			// An unnamed index never reaches emitCreateIndex (it refuses one
			// outright), so it must not be claimed here either — two of them
			// would otherwise "collide" on the empty string and refuse a
			// schema the run handles differently.
			name: "two unnamed indexes do not collide",
			tables: []*ir.Table{
				nsTable("posts", &ir.Index{Columns: idxCol("v")}),
				nsTable("comments", &ir.Index{Columns: idxCol("v")}),
			},
			refuses: false,
		},
		{
			// Two tables' PRIMARY KEYs carrying the same source name. SQLite
			// auto-names the backing index `sqlite_autoindex_<table>_N`, so
			// the source PK name never enters this namespace — unlike
			// Postgres, where a source-named PK takes a pg_class row.
			name: "same PRIMARY KEY name on two tables does not collide on SQLite",
			tables: []*ir.Table{
				func() *ir.Table {
					tb := nsTable("posts")
					tb.PrimaryKey = &ir.Index{Name: "pk", ConstraintNamed: true, Columns: idxCol("v")}
					return tb
				}(),
				func() *ir.Table {
					tb := nsTable("comments")
					tb.PrimaryKey = &ir.Index{Name: "pk", ConstraintNamed: true, Columns: idxCol("v")}
					return tb
				}(),
			},
			refuses: false,
		},
		{
			name:    "a nil table is skipped, not dereferenced",
			tables:  []*ir.Table{nil, nsTable("posts", &ir.Index{Name: "v", Columns: idxCol("v")})},
			refuses: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSQLiteIndexNamespace(tc.tables)
			if !tc.refuses {
				if err != nil {
					t.Fatalf("must not refuse: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("accepted a colliding index namespace; the second CREATE INDEX IF NOT EXISTS " +
					"would SILENTLY no-op and the target would be left without that index")
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeSchemaIndexNameCollision {
				t.Errorf("refusal must carry %s; got %+v (ok=%v)",
					sluicecode.CodeSchemaIndexNameCollision, ce, ok)
			}
			// The message must name BOTH source indexes and BOTH tables, or
			// the operator cannot act on it — "something collided" is not a
			// remedy.
			for _, tb := range tc.tables {
				if tb == nil {
					continue
				}
				if !strings.Contains(err.Error(), tb.Name) {
					t.Errorf("refusal must name table %q; got %q", tb.Name, err.Error())
				}
				for _, idx := range tb.Indexes {
					if idx.Name == "" {
						continue
					}
					if !strings.Contains(err.Error(), idx.Name) {
						t.Errorf("refusal must name source index %q; got %q", idx.Name, err.Error())
					}
				}
			}
		})
	}
}

// TestSQLiteIndexNamespace_ReachesBothWritePhases is the sibling-sweep half:
// the check is wired into the pre-copy phase AND the phase where the no-op
// would actually happen, because they are reached by different runs
// (`--resume` and a pre-existing target schema skip the first).
func TestSQLiteIndexNamespace_ReachesBothWritePhases(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{
		nsTable("posts", &ir.Index{Name: "user_id", Columns: idxCol("v")}),
		nsTable("comments", &ir.Index{Name: "user_id", Unique: true, Columns: idxCol("v")}),
	}}

	db, err := sql.Open("sqlite", "file:idxns?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	w := &SchemaWriter{db: db, path: "idxns"}
	ctx := context.Background()

	phases := map[string]func() error{
		"CreateTablesWithoutConstraints": func() error { return w.CreateTablesWithoutConstraints(ctx, schema) },
		"CreateIndexes":                  func() error { return w.CreateIndexes(ctx, schema) },
		"Engine.PreflightIndexes":        func() error { return Engine{}.PreflightIndexes(schema) },
	}
	for name, run := range phases {
		t.Run(name, func(t *testing.T) {
			err := run()
			if err == nil {
				t.Fatal("phase accepted the colliding namespace")
			}
			if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeSchemaIndexNameCollision {
				t.Errorf("refusal must carry %s; got %v", sluicecode.CodeSchemaIndexNameCollision, err)
			}
		})
	}
}

// TestSQLiteIndexNamespace_CaseFoldIsGroundTruth is the premise-naming step for
// the two facts the refusal rests on, asserted against the REAL driver rather
// than left as a comment:
//
//  1. `CREATE INDEX IF NOT EXISTS` on an already-taken index name is a SILENT
//     no-op — no error, and the index is not created.
//  2. That comparison folds ASCII case, so a name differing only in case is
//     the same silent no-op.
//
// If either stopped being true the refusal would be over-strict, and this is
// what would say so.
func TestSQLiteIndexNamespace_CaseFoldIsGroundTruth(t *testing.T) {
	db, err := sql.Open("sqlite", "file:idxfold?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	for _, stmt := range []string{
		`CREATE TABLE a (v TEXT)`,
		`CREATE TABLE b (v TEXT)`,
		`CREATE INDEX IF NOT EXISTS "idx_v" ON "a" ("v")`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	// (1) and (2): both must succeed WITHOUT error and WITHOUT creating an
	//     index — that silence is the whole defect.
	for _, stmt := range []string{
		`CREATE UNIQUE INDEX IF NOT EXISTS "idx_v" ON "b" ("v")`,
		`CREATE UNIQUE INDEX IF NOT EXISTS "IDX_V" ON "b" ("v")`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("SQLite now ERRORS on %s (%v). The refusal exists because this is silent; if it has "+
				"become loud, re-derive the finding before relaxing anything.", stmt, err)
		}
	}

	var n int
	if err := db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_schema WHERE type='index' AND tbl_name='b'`,
	).Scan(&n); err != nil {
		t.Fatalf("count indexes on b: %v", err)
	}
	if n != 0 {
		t.Fatalf("table b has %d index(es); the two CREATE statements above were expected to be silent "+
			"no-ops, which is what makes the loss invisible", n)
	}
}
