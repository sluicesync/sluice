// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the Postgres schema writer's view-emit path. These
// don't need a live Postgres — they cover the DDL string the writer
// would produce for a given IR view shape.

package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestEmitCreateView_Regular covers the regular view DDL shape.
// Regular views use `CREATE OR REPLACE VIEW`; this lets a re-run of
// CreateViews succeed against an existing target.
func TestEmitCreateView_Regular(t *testing.T) {
	v := &ir.View{
		Name:              "active_users",
		Schema:            "public",
		Definition:        "SELECT id, email FROM users WHERE active",
		DefinitionDialect: dialectName,
	}
	got := emitCreateView("public", v)
	want := `CREATE OR REPLACE VIEW "public"."active_users" AS SELECT id, email FROM users WHERE active;`
	if got != want {
		t.Errorf("emitCreateView mismatch\n got: %q\nwant: %q", got, want)
	}
}

// TestEmitCreateView_Materialized covers the materialized view DDL
// shape. PG matviews use `CREATE MATERIALIZED VIEW ... WITH DATA` so
// the matview is populated immediately from the just-loaded target
// tables on cold-start. Phase 2 will add CDC-driven refresh.
func TestEmitCreateView_Materialized(t *testing.T) {
	v := &ir.View{
		Name:         "mv_summary",
		Schema:       "public",
		Definition:   "SELECT count(*) AS total FROM users",
		Materialized: true,
	}
	got := emitCreateView("public", v)
	if !strings.Contains(got, "CREATE MATERIALIZED VIEW") {
		t.Errorf("expected CREATE MATERIALIZED VIEW; got: %q", got)
	}
	if !strings.HasSuffix(got, " WITH DATA;") {
		t.Errorf("expected WITH DATA suffix; got: %q", got)
	}
}

// TestEmitCreateView_QualifiesIdentifier verifies that the schema
// is included in the view's qualified name. PG's namespace-aware
// schemas mean an unqualified name would land in whatever schema the
// connection's search_path happens to point at; the writer is
// explicit about target placement to avoid that footgun.
func TestEmitCreateView_QualifiesIdentifier(t *testing.T) {
	v := &ir.View{Name: "v", Schema: "myapp", Definition: "SELECT 1"}
	got := emitCreateView("myapp", v)
	if !strings.Contains(got, `"myapp"."v"`) {
		t.Errorf("expected schema-qualified identifier; got: %q", got)
	}
}

// TestPreviewDDL_IncludesViews_PG covers the integration of view
// emission into the PG preview path. Both regular and materialized
// views land in the output with the right Kind tag.
func TestPreviewDDL_IncludesViews_PG(t *testing.T) {
	w := &SchemaWriter{schema: "public"}
	s := &ir.Schema{
		Views: []*ir.View{
			{Name: "regular", Schema: "public", Definition: "SELECT 1"},
			{Name: "matview", Schema: "public", Definition: "SELECT 2", Materialized: true},
		},
	}
	stmts, err := w.PreviewDDL(context.Background(), s)
	if err != nil {
		t.Fatalf("PreviewDDL: %v", err)
	}
	kinds := map[string]bool{}
	for _, st := range stmts {
		kinds[st.Kind] = true
	}
	if !kinds["CREATE VIEW"] {
		t.Errorf("PreviewDDL missing CREATE VIEW kind; stmts: %+v", stmts)
	}
	if !kinds["CREATE MATERIALIZED VIEW"] {
		t.Errorf("PreviewDDL missing CREATE MATERIALIZED VIEW kind; stmts: %+v", stmts)
	}
}
