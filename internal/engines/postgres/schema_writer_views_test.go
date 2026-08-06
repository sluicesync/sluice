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

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// mustEmitCreateView is the shape-test spelling of [emitCreateView]: these
// cases all use short names, so the identifier-length refusal it grew in item
// 148 is never the subject — [TestEmitCreateView_RefusesAnOverLongName] is.
func mustEmitCreateView(t *testing.T, schema string, v *ir.View) string {
	t.Helper()
	stmt, err := emitCreateView(schema, v)
	if err != nil {
		t.Fatalf("emitCreateView(%q, %q): %v", schema, v.Name, err)
	}
	return stmt
}

// TestEmitCreateView_RefusesAnOverLongName closes the "also named, not fixed"
// half of roadmap item 148: validatePGIdentifier had call sites for tables,
// columns, indexes, constraints and enum types, and NONE for views.
//
// The consequence is worse for views than for the kinds it did cover, and the
// reason is the statement. Every other kind emits `IF NOT EXISTS`, so a
// truncation collision no-ops and the target keeps the FIRST object. Views emit
// `CREATE OR REPLACE VIEW`, which has no no-op branch — it REPLACES. So two
// source views sharing their first 63 bytes leave one view on the target
// carrying the SECOND one's body, under a name that belongs to the first, at
// exit 0.
func TestEmitCreateView_RefusesAnOverLongName(t *testing.T) {
	// 63 bytes is the ceiling; these two differ only past it, which is exactly
	// the pair PG would silently merge.
	base := strings.Repeat("v", maxPGIdentifierLen)
	for _, tc := range []struct {
		name         string
		viewName     string
		materialized bool
		refuses      bool
	}{
		{"at the limit is accepted", base, false, false},
		{"one byte over is refused", base + "a", false, true},
		{"one byte over is refused for a matview too", base + "b", true, true},
		{
			// Multibyte: PG counts BYTES, so a name of 40 runes can exceed 63.
			"a multibyte name over the BYTE limit is refused",
			strings.Repeat("é", 40),
			false,
			true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := emitCreateView("public", &ir.View{
				Name: tc.viewName, Schema: "public",
				Definition: "SELECT 1", Materialized: tc.materialized,
			})
			if !tc.refuses {
				if err != nil {
					t.Fatalf("must not refuse a %d-byte view name: %v", len(tc.viewName), err)
				}
				return
			}
			if err == nil {
				t.Fatalf("emitCreateView accepted a %d-byte view name; PostgreSQL truncates it at %d and "+
					"CREATE OR REPLACE VIEW would then overwrite whichever view got there first",
					len(tc.viewName), maxPGIdentifierLen)
			}
			ce, coded := sluicecode.FromError(err)
			if !coded || ce.Code != sluicecode.CodeSchemaIdentifierTooLong {
				t.Errorf("refusal must carry %s; got %+v (coded=%v)",
					sluicecode.CodeSchemaIdentifierTooLong, ce, coded)
			}
			if !strings.Contains(err.Error(), "view name") {
				t.Errorf("refusal must name the object KIND as a view (the operator renames a view, not a "+
					"table); got %q", err.Error())
			}
		})
	}
}

// TestCreateViews_PropagatesTheIdentifierRefusal is the door half: the refusal
// must reach the phase, not just the emitter. CreateViews needs no live
// database for this case — the refusal fires before any Exec.
func TestCreateViews_PropagatesTheIdentifierRefusal(t *testing.T) {
	w := &SchemaWriter{schema: "public"}
	s := &ir.Schema{Views: []*ir.View{{
		Name: strings.Repeat("v", maxPGIdentifierLen+1), Schema: "public", Definition: "SELECT 1",
	}}}
	if err := w.CreateViews(context.Background(), s); err == nil {
		t.Fatal("CreateViews accepted an over-long view name")
	} else if ce, coded := sluicecode.FromError(err); !coded ||
		ce.Code != sluicecode.CodeSchemaIdentifierTooLong {
		t.Errorf("refusal must carry %s; got %v", sluicecode.CodeSchemaIdentifierTooLong, err)
	}
	// The preview path is the second caller and gets the same answer, so a
	// `--dry-run` cannot show a plan the real run refuses.
	if _, err := w.PreviewDDL(context.Background(), s); err == nil {
		t.Error("PreviewDDL accepted an over-long view name; the plan would promise DDL the run refuses")
	}
}

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
	got := mustEmitCreateView(t, "public", v)
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
	got := mustEmitCreateView(t, "public", v)
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
	got := mustEmitCreateView(t, "myapp", v)
	if !strings.Contains(got, `"myapp"."v"`) {
		t.Errorf("expected schema-qualified identifier; got: %q", got)
	}
}

// TestEmitCreateView_TrailingSemicolonInDefinition pins the v0.14.1
// fix for Bug 31. PG's pg_views.definition / pg_matviews.definition
// catalog columns return the SELECT body with a trailing `;`. Pre-fix,
// the writer appended " WITH DATA;" or ";" directly, producing
// "... ; WITH DATA;" (rejected by PG with SQLSTATE 42601 — blocks
// matview round-trip) or "... ;;" (silently parsed but ugly DDL).
// Post-fix, the trailing `;` is trimmed before the trailer is appended.
func TestEmitCreateView_TrailingSemicolonInDefinition(t *testing.T) {
	t.Run("regular view with trailing semicolon", func(t *testing.T) {
		v := &ir.View{
			Name:       "v",
			Schema:     "public",
			Definition: "SELECT id FROM t WHERE active;", // trailing ;
		}
		got := mustEmitCreateView(t, "public", v)
		// No `;;` — exactly one trailing ;
		if strings.Contains(got, ";;") {
			t.Errorf("regular view emit should not produce double-semicolon; got: %q", got)
		}
		want := `CREATE OR REPLACE VIEW "public"."v" AS SELECT id FROM t WHERE active;`
		if got != want {
			t.Errorf("regular view emit mismatch\n got: %q\nwant: %q", got, want)
		}
	})
	t.Run("matview with trailing semicolon — Bug 31", func(t *testing.T) {
		v := &ir.View{
			Name:         "mv",
			Schema:       "public",
			Definition:   "SELECT id FROM t;", // trailing ; from pg_matviews.definition
			Materialized: true,
		}
		got := mustEmitCreateView(t, "public", v)
		// Pre-fix would emit "... ;\nWITH DATA;" which PG rejects.
		// Post-fix: exactly one ; before WITH DATA.
		want := `CREATE MATERIALIZED VIEW "public"."mv" AS SELECT id FROM t WITH DATA;`
		if got != want {
			t.Errorf("matview emit mismatch\n got: %q\nwant: %q", got, want)
		}
	})
	t.Run("matview with trailing whitespace + semicolon", func(t *testing.T) {
		v := &ir.View{
			Name:         "mv",
			Schema:       "public",
			Definition:   "SELECT id FROM t  ;\n", // pg_matviews can include trailing whitespace
			Materialized: true,
		}
		got := mustEmitCreateView(t, "public", v)
		want := `CREATE MATERIALIZED VIEW "public"."mv" AS SELECT id FROM t WITH DATA;`
		if got != want {
			t.Errorf("trailing whitespace+; should be trimmed\n got: %q\nwant: %q", got, want)
		}
	})
	t.Run("definition without trailing semicolon stays clean", func(t *testing.T) {
		v := &ir.View{
			Name:       "v",
			Schema:     "public",
			Definition: "SELECT id FROM t",
		}
		got := mustEmitCreateView(t, "public", v)
		want := `CREATE OR REPLACE VIEW "public"."v" AS SELECT id FROM t;`
		if got != want {
			t.Errorf("no-trailing-; case regression\n got: %q\nwant: %q", got, want)
		}
	})
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
