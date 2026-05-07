// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the MySQL schema writer's view-emit path. These don't
// need a live MySQL — they cover the DDL string the writer would
// produce for a given IR view shape, which is the load-bearing surface
// for both the apply path (CreateViews) and the preview path
// (PreviewDDL).

package mysql

import (
	"context"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestEmitCreateView covers the single-view DDL string. The shape is
// always `CREATE OR REPLACE VIEW <name> AS <definition>;` for regular
// views; materialized views error out at the CreateViews layer (MySQL
// has no materialized-view concept).
func TestEmitCreateView(t *testing.T) {
	v := &ir.View{
		Name:              "active_users",
		Definition:        "SELECT id, email FROM users WHERE active = 1",
		DefinitionDialect: dialectName,
	}
	got := emitCreateView(v)
	want := "CREATE OR REPLACE VIEW `active_users` AS SELECT id, email FROM users WHERE active = 1;"
	if got != want {
		t.Errorf("emitCreateView mismatch\n got: %q\nwant: %q", got, want)
	}
}

// TestEmitCreateView_QuotesIdentifier covers the identifier-quoting
// path: a view whose name happens to be a MySQL reserved word should
// still emit a parseable statement.
func TestEmitCreateView_QuotesIdentifier(t *testing.T) {
	v := &ir.View{Name: "select", Definition: "SELECT 1"}
	got := emitCreateView(v)
	if !strings.HasPrefix(got, "CREATE OR REPLACE VIEW `select` ") {
		t.Errorf("expected backtick-quoted reserved-word identifier; got: %q", got)
	}
}

// TestPreviewDDL_IncludesViews covers the integration of view emission
// into the preview path. A schema with one table + one view should
// produce both a CREATE TABLE and a CREATE VIEW statement, with views
// emitted last.
func TestPreviewDDL_IncludesViews(t *testing.T) {
	w := &SchemaWriter{schema: "testdb"}
	s := &ir.Schema{
		Tables: []*ir.Table{
			{
				Name: "users",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
				},
				PrimaryKey: &ir.Index{
					Name:    "PRIMARY",
					Columns: []ir.IndexColumn{{Column: "id"}},
				},
			},
		},
		Views: []*ir.View{
			{
				Name:              "user_ids",
				Definition:        "SELECT id FROM users",
				DefinitionDialect: dialectName,
			},
		},
	}
	stmts, err := w.PreviewDDL(context.Background(), s)
	if err != nil {
		t.Fatalf("PreviewDDL: %v", err)
	}
	// Find the view statement.
	var viewStmt *ir.DDLStatement
	for i := range stmts {
		if stmts[i].Kind == "CREATE VIEW" {
			viewStmt = &stmts[i]
			break
		}
	}
	if viewStmt == nil {
		t.Fatalf("PreviewDDL did not emit a CREATE VIEW statement; got %d stmts", len(stmts))
	}
	if viewStmt.Table != "user_ids" {
		t.Errorf("view statement Table = %q; want user_ids", viewStmt.Table)
	}
	if !strings.Contains(viewStmt.SQL, "CREATE OR REPLACE VIEW") {
		t.Errorf("view SQL %q missing CREATE OR REPLACE VIEW", viewStmt.SQL)
	}

	// Order: views must come after tables (so referenced base tables
	// exist by the time the view is created).
	tableIdx, viewIdx := -1, -1
	for i, s := range stmts {
		switch s.Kind {
		case "CREATE TABLE":
			tableIdx = i
		case "CREATE VIEW":
			viewIdx = i
		}
	}
	if tableIdx < 0 || viewIdx < 0 || tableIdx > viewIdx {
		t.Errorf("CREATE TABLE must precede CREATE VIEW; got tableIdx=%d viewIdx=%d", tableIdx, viewIdx)
	}
}

// TestPreviewDDL_SkipsMaterializedView verifies that a materialized
// flag on a view is silently dropped from MySQL preview output (MySQL
// has no materialized view support; the actual apply-path errors
// loudly via CreateViews. PreviewDDL is best-effort and skips rather
// than failing the whole preview render over one unsupported entry).
func TestPreviewDDL_SkipsMaterializedView(t *testing.T) {
	w := &SchemaWriter{schema: "testdb"}
	s := &ir.Schema{
		Views: []*ir.View{
			{Name: "mv", Definition: "SELECT 1", Materialized: true},
		},
	}
	stmts, err := w.PreviewDDL(context.Background(), s)
	if err != nil {
		t.Fatalf("PreviewDDL: %v", err)
	}
	for _, st := range stmts {
		if st.Kind == "CREATE VIEW" {
			t.Errorf("materialized view should be skipped from MySQL preview; got %+v", st)
		}
	}
}
