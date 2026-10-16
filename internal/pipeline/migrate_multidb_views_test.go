// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// viewCatalogEngine is a target whose schema writers share one catalog.
// A view's Definition is a space-separated list of "<namespace>.<name>"
// objects it reads; CreateViews succeeds only when all of them exist, and
// then records "<dsn>.<view>". It models exactly the property the Bug 317
// deferred pass depends on — a view cannot be created before what it reads.
type viewCatalogEngine struct {
	stubEngineBase
	catalog map[string]bool
}

func (*viewCatalogEngine) Name() string { return "view-catalog" }

func (e *viewCatalogEngine) OpenSchemaWriter(_ context.Context, dsn string) (ir.SchemaWriter, error) {
	return &viewCatalogWriter{catalog: e.catalog, ns: dsn}, nil
}

type viewCatalogWriter struct {
	catalog map[string]bool
	ns      string
}

func (*viewCatalogWriter) CreateTablesWithoutConstraints(context.Context, *ir.Schema) error {
	return nil
}
func (*viewCatalogWriter) CreateIndexes(context.Context, *ir.Schema) error         { return nil }
func (*viewCatalogWriter) CreateConstraints(context.Context, *ir.Schema) error     { return nil }
func (*viewCatalogWriter) SyncIdentitySequences(context.Context, *ir.Schema) error { return nil }

func (w *viewCatalogWriter) CreateViews(_ context.Context, s *ir.Schema) error {
	for _, v := range s.Views {
		for _, dep := range strings.Fields(v.Definition) {
			if !w.catalog[dep] {
				return errors.New("relation " + dep + " does not exist")
			}
		}
		w.catalog[w.ns+"."+v.Name] = true
	}
	return nil
}

// TestCreateDeferredViews_CrossNamespaceViewOnView pins the round loop: a
// view in a namespace that sorts FIRST reads a view in a later namespace,
// which reads a table. One pass in namespace order cannot create it; the
// second round must.
func TestCreateDeferredViews_CrossNamespaceViewOnView(t *testing.T) {
	eng := &viewCatalogEngine{catalog: map[string]bool{"c.t": true}}
	err := createDeferredViews(context.Background(), eng, []namespaceViews{
		{database: "a", targetDSN: "a", views: []*ir.View{{Name: "v1", Definition: "b.v2"}}},
		{database: "b", targetDSN: "b", views: []*ir.View{{Name: "v2", Definition: "c.t"}}},
		{database: "c", targetDSN: "c"}, // tables only: nothing deferred
	})
	if err != nil {
		t.Fatalf("createDeferredViews: %v", err)
	}
	for _, want := range []string{"a.v1", "b.v2"} {
		if !eng.catalog[want] {
			t.Errorf("view %s was not created", want)
		}
	}
}

// TestCreateDeferredViews_UnresolvableIsLoud pins the other half: a view
// that can never be created fails the pass, naming its namespace and view,
// and a resolvable sibling is still created.
func TestCreateDeferredViews_UnresolvableIsLoud(t *testing.T) {
	eng := &viewCatalogEngine{catalog: map[string]bool{"app.t": true}}
	err := createDeferredViews(context.Background(), eng, []namespaceViews{
		{database: "bad", targetDSN: "bad", views: []*ir.View{{Name: "orphan", Definition: "gone.t"}}},
		{database: "rpt", targetDSN: "rpt", views: []*ir.View{{Name: "ok", Definition: "app.t"}}},
	})
	if err == nil {
		t.Fatal("createDeferredViews returned nil for a view whose relation never exists; want a loud failure")
	}
	if !strings.Contains(err.Error(), `database "bad"`) || !strings.Contains(err.Error(), `view "orphan"`) {
		t.Errorf("error does not name the namespace and view: %v", err)
	}
	if !eng.catalog["rpt.ok"] {
		t.Error("the resolvable sibling view was not created")
	}
}

// TestDeferredViewsTake pins the hand-off: a nil collector (single-database
// run) leaves the schema untouched, and a live one moves the views out
// without mutating the caller's schema.
func TestDeferredViewsTake(t *testing.T) {
	schema := &ir.Schema{
		Tables: []*ir.Table{{Name: "t"}},
		Views:  []*ir.View{{Name: "v"}},
	}
	var none *deferredViews
	if got := none.take(schema); got != schema {
		t.Fatal("nil collector must return the schema unchanged")
	}
	d := &deferredViews{}
	got := d.take(schema)
	if len(got.Views) != 0 || len(got.Tables) != 1 {
		t.Errorf("take returned %d views / %d tables; want 0 / 1", len(got.Views), len(got.Tables))
	}
	if len(d.views) != 1 || d.views[0].Name != "v" {
		t.Errorf("collector holds %v; want [v]", d.views)
	}
	if len(schema.Views) != 1 {
		t.Error("take mutated the caller's schema")
	}
}
