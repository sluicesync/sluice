//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// TestSchemaReader_NonDefaultCoreOpclasses_Bug115 pins the v0.95.0
// Bug 115 closure. Pre-fix the PG schema reader populated
// `ir.IndexColumn.OperatorClass` only for extension-introduced access
// methods (pgvector hnsw), extension-owned opclasses on core AMs
// (pg_trgm gin_trgm_ops), or uncatalogued extension-owned opclasses
// under the verbatim tier. Operator-explicit NON-DEFAULT core PG
// opclasses on core AMs — `text_pattern_ops`, `varchar_pattern_ops`,
// `jsonb_path_ops`, and similar — fell through every branch and were
// silently dropped, downgrading the target's index to the default
// opclass and changing the index size + supported-operator footprint
// with no WARN. Post-fix, `opc.opcdefault=false` on a non-extension-
// owned opclass carries the bareword forward via the dedicated
// dispatch branch.
//
// The matrix exercises the three documented Bug 115 cases + one
// negative control (a default-opclass index whose IR must remain
// empty so the writer doesn't gratuitously emit a redundant opclass
// and clutter the DDL diffs / surface internal opcname differences
// across PG versions per the pre-existing Bug 47 invariant).
func TestSchemaReader_NonDefaultCoreOpclasses_Bug115(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	const ddl = `
		DROP TABLE IF EXISTS opclass_test CASCADE;
		CREATE TABLE opclass_test (
		  id    int PRIMARY KEY,
		  name  text,
		  code  varchar(64),
		  tags  jsonb,
		  notes text
		);
		-- The three Bug 115 cases — all on CORE AMs (btree / gin) with
		-- NON-DEFAULT core opclasses.
		CREATE INDEX opclass_text_pattern    ON opclass_test (name text_pattern_ops);
		CREATE INDEX opclass_varchar_pattern ON opclass_test (code varchar_pattern_ops);
		CREATE INDEX opclass_jsonb_path      ON opclass_test USING gin (tags jsonb_path_ops);
		-- Negative control: a default-opclass index on the same shape.
		-- text_ops is the default for btree-on-text in core PG; the
		-- reader MUST leave OperatorClass empty so the writer emits
		-- nothing extra (preserves Bug 47 invariant + diff stability
		-- against pg_get_indexdef across PG major versions).
		CREATE INDEX opclass_text_default    ON opclass_test (notes);
	`
	applyDDL(t, dsn, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	r, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(r)

	schema, err := r.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}

	tab := findTable(schema, "opclass_test")
	if tab == nil {
		t.Fatalf("missing opclass_test table; have %v", tableNames(schema))
	}

	want := map[string]string{
		"opclass_text_pattern":    "text_pattern_ops",
		"opclass_varchar_pattern": "varchar_pattern_ops",
		"opclass_jsonb_path":      "jsonb_path_ops",
		"opclass_text_default":    "", // negative control
	}
	got := map[string]string{}
	for _, idx := range tab.Indexes {
		if _, tracked := want[idx.Name]; !tracked {
			continue
		}
		if len(idx.Columns) != 1 {
			t.Errorf("index %q: got %d columns; want 1", idx.Name, len(idx.Columns))
			continue
		}
		got[idx.Name] = idx.Columns[0].OperatorClass
	}
	for name, wantOpclass := range want {
		gotOpclass, found := got[name]
		if !found {
			t.Errorf("index %q: not found on opclass_test (have indexes: %s)",
				name, indexNames(tab.Indexes))
			continue
		}
		if gotOpclass != wantOpclass {
			t.Errorf("index %q: OperatorClass = %q; want %q (Bug 115 silent-drop signature)",
				name, gotOpclass, wantOpclass)
		}
	}
}

func indexNames(idxs []*ir.Index) string {
	if len(idxs) == 0 {
		return "<none>"
	}
	out := ""
	for i, ix := range idxs {
		if i > 0 {
			out += ", "
		}
		out += ix.Name
	}
	return out
}
