// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// viewsOnlyFilterSource is a source whose catalog holds `tables` and one
// view. Its schema reader honours a scope push-down the way the Postgres
// reader does (a scoped-out table never reaches the schema), and it can
// declare engine-default exclusions the way a VStream flavor does.
type viewsOnlyFilterSource struct {
	stubEngineBase
	tables   []string
	defaults []string
}

func (*viewsOnlyFilterSource) Name() string { return "views-only-filter-src" }

func (s *viewsOnlyFilterSource) DefaultExcludePatterns(string) []string { return s.defaults }

func (s *viewsOnlyFilterSource) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return &viewsOnlyFilterReader{tables: s.tables}, nil
}

type viewsOnlyFilterReader struct {
	tables []string
	scope  func(string) bool
}

func (r *viewsOnlyFilterReader) SetTableScope(allow func(string) bool) { r.scope = allow }

func (r *viewsOnlyFilterReader) ReadSchema(context.Context) (*ir.Schema, error) {
	s := &ir.Schema{Views: []*ir.View{{Name: "v", Definition: "SELECT 1 AS id"}}}
	for _, name := range r.tables {
		if r.scope != nil && !r.scope(name) {
			continue
		}
		s.Tables = append(s.Tables, &ir.Table{Name: name, Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}}})
	}
	return s, nil
}

func mustFilter(t *testing.T, include, exclude []string) migcore.TableFilter {
	t.Helper()
	f, err := migcore.NewTableFilter(include, exclude)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// TestPhaseReadSourceSchema_ViewsOnlyNamespaceUnderATableFilter pins the
// Bug 317 review's HIGH: a views-only namespace reached the "table filter
// excluded every source table" refusal with zero tables, so any table
// filter — and every VStream-flavor source with no flags, through its
// engine-default `_vt_*` exclusion — aborted the run there. A namespace
// that HAD tables and lost them all to the filter must still refuse, on
// both the post-read prune (MySQL) and the push-down (Postgres) shape.
func TestPhaseReadSourceSchema_ViewsOnlyNamespaceUnderATableFilter(t *testing.T) {
	vitess, ok := engines.Get("vitess")
	if !ok {
		t.Fatal("vitess engine not registered")
	}
	vtDefaults := vitess.(ir.DefaultTableExcluder).DefaultExcludePatterns("")
	if len(vtDefaults) == 0 {
		t.Fatal("the vitess flavor declares no default exclusion; this cell would be vacuous")
	}

	cases := []struct {
		name      string
		src       *viewsOnlyFilterSource
		filter    migcore.TableFilter
		wantError bool
	}{
		{"views-only, --exclude-table", &viewsOnlyFilterSource{}, mustFilter(t, nil, []string{"audit_log"}), false},
		{"views-only, --include-table", &viewsOnlyFilterSource{}, mustFilter(t, []string{"orders"}, nil), false},
		{"views-only, vstream-flavor default _vt_* and no flags", &viewsOnlyFilterSource{defaults: vtDefaults}, migcore.TableFilter{}, false},
		{"tables all filtered by the push-down, views remain", &viewsOnlyFilterSource{tables: []string{"t"}}, mustFilter(t, []string{"orders"}, nil), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := &Migrator{Source: c.src, Filter: c.filter}
			_, schema, err := m.phaseReadSourceSchema(context.Background(), nil)
			if c.wantError {
				if err == nil || !strings.Contains(err.Error(), "table filter excluded every source table") {
					t.Fatalf("want the filter refusal for a namespace whose tables were all filtered out; got err=%v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Bug 317 review: a views-only namespace under a table filter refused: %v", err)
			}
			if schema == nil || len(schema.Views) != 1 {
				t.Fatalf("views-only namespace lost its view: %+v", schema)
			}
		})
	}
}

// TestApplyTableFilter_PostReadPruneStillRefusesEmptiedTableSet pins the
// MySQL-shaped half: no push-down, the prune itself empties a non-empty
// table set — still the refusal, views or not.
func TestApplyTableFilter_PostReadPruneStillRefusesEmptiedTableSet(t *testing.T) {
	schema := &ir.Schema{
		Tables: []*ir.Table{{Name: "t"}},
		Views:  []*ir.View{{Name: "v"}},
	}
	err := migcore.ApplyTableFilter(context.Background(), schema, mustFilter(t, []string{"orders"}, nil))
	if err == nil {
		t.Fatal("a filter that emptied a non-empty table set did not refuse")
	}
}
