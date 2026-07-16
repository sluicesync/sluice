// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the MED-D0-8 skipped-index definition-drift advisory: the
// detect-then-skip index paths (direct build + the ADR-0148 fallback's
// re-probe) exclude by NAME only, so a pre-existing same-name index with
// a different definition used to be silently accepted as built. The
// advisory compares the catalog definition against the intended one and
// WARNs on divergence — a WARN, not a refusal, because a differing
// definition can be deliberate operator customization. Pin matrix:
// same-name-same-def skips silently (the pre-existing behavior);
// diff-columns WARNs naming both definitions; diff-uniqueness WARNs
// naming the duplicate-acceptance hazard; and the fallback path carries
// the same advisory.

package mysql

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// captureWarnLog swaps the default slog handler for a buffer at WARN
// level for the duration of fn.
func captureWarnLog(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)
	fn()
	return buf.String()
}

// driftTestWriter wires a SchemaWriter over the fake driver with every
// probed index reported existing and the given scripted definitions.
func driftTestWriter(t *testing.T, defs map[string]fakeIndexDef) (*SchemaWriter, *indexRecorder) {
	t.Helper()
	rec := &indexRecorder{exists: true, driftDefs: defs}
	db := newIndexFakeDB(t, rec)
	return &SchemaWriter{db: db, schema: "testdb", flavor: FlavorVanilla}, rec
}

// driftSchema returns a one-table schema whose single secondary index is
// idx (over PK table columns id, v, w).
func driftSchema(idx *ir.Index) *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Integer{Width: 64}},
			{Name: "w", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: pk(),
		Indexes:    []*ir.Index{idx},
	}}}
}

func TestIndexDrift_SameDefinitionSkipsSilently(t *testing.T) {
	w, rec := driftTestWriter(t, nil) // default served def: non-unique (v)
	schema := driftSchema(&ir.Index{Name: "t_v_idx", Columns: []ir.IndexColumn{{Column: "v"}}})

	var runErr error
	logged := captureWarnLog(t, func() { runErr = w.CreateIndexes(context.Background(), schema) })
	if runErr != nil {
		t.Fatalf("CreateIndexes: %v", runErr)
	}
	if len(rec.snapshot()) != 0 {
		t.Errorf("ALTERs emitted for an existing matching index: %v", rec.snapshot())
	}
	if strings.Contains(logged, "DIFFERENT") {
		t.Errorf("drift WARN fired on a matching definition:\n%s", logged)
	}
}

func TestIndexDrift_DifferentColumnsWarnsNamingBothDefinitions(t *testing.T) {
	// The target already has t_v_idx — but over column w, not v.
	w, rec := driftTestWriter(t, map[string]fakeIndexDef{
		"t_v_idx": {columns: []string{"w"}},
	})
	schema := driftSchema(&ir.Index{Name: "t_v_idx", Columns: []ir.IndexColumn{{Column: "v"}}})

	var runErr error
	logged := captureWarnLog(t, func() { runErr = w.CreateIndexes(context.Background(), schema) })
	if runErr != nil {
		t.Fatalf("CreateIndexes: %v", runErr)
	}
	// The skip itself is unchanged — advisory only, never a rebuild.
	if len(rec.snapshot()) != 0 {
		t.Errorf("ALTERs emitted despite the name match: %v", rec.snapshot())
	}
	for _, want := range []string{
		"DIFFERENT definition",
		"index=t_v_idx", "table=t",
		`existing_definition=(w)`,
		`intended_definition=(v)`,
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("drift WARN missing %q:\n%s", want, logged)
		}
	}
}

func TestIndexDrift_DifferentUniquenessWarnsNamingTheDuplicateHazard(t *testing.T) {
	// Intended UNIQUE(v); the target holds a plain (v) under the same name
	// — the silent-loss-adjacent shape: the existing index decides which
	// duplicate writes the target accepts.
	w, _ := driftTestWriter(t, nil) // default served def: NON-unique (v)
	schema := driftSchema(&ir.Index{Name: "t_v_uq", Unique: true, Columns: []ir.IndexColumn{{Column: "v"}}})

	var runErr error
	logged := captureWarnLog(t, func() { runErr = w.CreateIndexes(context.Background(), schema) })
	if runErr != nil {
		t.Fatalf("CreateIndexes: %v", runErr)
	}
	for _, want := range []string{
		"DIFFERENT UNIQUENESS",
		"duplicate writes",
		`existing_definition=(v)`,
		`intended_definition="UNIQUE (v)"`,
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("uniqueness WARN missing %q:\n%s", want, logged)
		}
	}
}

// TestIndexDrift_DifferentTypeWarnsNamingTheAccessMethod pins the
// audit-2026-07-16 type-blindness fix: a same-name index over the SAME
// columns but a different access method (here intended BTREE vs an
// existing FULLTEXT — whose SUB_PART/COLLATION normalization used to
// erase every distinguishing signal) compared EQUAL before the kind
// field existed. The compare fires only when BOTH sides report a type;
// the intended side reports one exactly when the IR kind reaches
// [emitAddIndexClause]'s DDL (BTREE/HASH/FULLTEXT/SPATIAL).
func TestIndexDrift_DifferentTypeWarnsNamingTheAccessMethod(t *testing.T) {
	w, _ := driftTestWriter(t, map[string]fakeIndexDef{
		"t_v_idx": {columns: []string{"v"}, indexType: "FULLTEXT"},
	})
	schema := driftSchema(&ir.Index{Name: "t_v_idx", Kind: ir.IndexKindBTree, Columns: []ir.IndexColumn{{Column: "v"}}})

	var runErr error
	logged := captureWarnLog(t, func() { runErr = w.CreateIndexes(context.Background(), schema) })
	if runErr != nil {
		t.Fatalf("CreateIndexes: %v", runErr)
	}
	for _, want := range []string{
		"DIFFERENT TYPE",
		"access method",
		`existing_definition="FULLTEXT (v)"`,
		`intended_definition=(v)`, // BTREE (the default) renders nothing
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("type WARN missing %q:\n%s", want, logged)
		}
	}
}

// TestIndexDrift_UnspecifiedIntendedKindStaysQuietOnType pins the
// both-sides-report-it carve-out: an intended index whose IR kind is
// unspecified emits no USING clause — the server picks the access
// method — so the catalog's BTREE must not be flagged against it.
func TestIndexDrift_UnspecifiedIntendedKindStaysQuietOnType(t *testing.T) {
	w, _ := driftTestWriter(t, nil) // default served def: non-unique BTREE (v)
	schema := driftSchema(&ir.Index{Name: "t_v_idx", Columns: []ir.IndexColumn{{Column: "v"}}})

	var runErr error
	logged := captureWarnLog(t, func() { runErr = w.CreateIndexes(context.Background(), schema) })
	if runErr != nil {
		t.Fatalf("CreateIndexes: %v", runErr)
	}
	if strings.Contains(logged, "DIFFERENT") {
		t.Errorf("type compare fired for an unspecified intended kind:\n%s", logged)
	}
}

// TestIndexDrift_FallbackReprobeCarriesTheAdvisory pins the second
// audit-named site: the ADR-0148 fallback's still-pending re-probe skips
// by name too, and a same-name-diff-def index gets the same WARN there.
func TestIndexDrift_FallbackReprobeCarriesTheAdvisory(t *testing.T) {
	w, _ := driftTestWriter(t, map[string]fakeIndexDef{
		"t_v_idx": {columns: []string{"w"}},
	})
	job := indexBuildJob{
		tableName: "t",
		idxs:      []*ir.Index{{Name: "t_v_idx", Columns: []ir.IndexColumn{{Column: "v"}}}},
	}

	var runErr error
	logged := captureWarnLog(t, func() { runErr = w.routeIndexJobToFallback(context.Background(), job, nil) })
	if runErr != nil {
		t.Fatalf("routeIndexJobToFallback: %v", runErr)
	}
	if !strings.Contains(logged, "DIFFERENT definition") || !strings.Contains(logged, "t_v_idx") {
		t.Errorf("fallback re-probe missing the drift WARN:\n%s", logged)
	}
}

// TestIntendedIndexCatalogDef pins the intended-side derivation against
// [emitAddIndexClause]'s emit rules per index kind — the definition
// compared must be what sluice would BUILD, not the raw IR (FULLTEXT/
// SPATIAL drop UNIQUE and per-column prefixes at emit time).
func TestIntendedIndexCatalogDef(t *testing.T) {
	cases := []struct {
		name string
		idx  *ir.Index
		want string // formatIndexCatalogDef rendering
	}{
		{
			"btree multi-column with prefix and desc",
			&ir.Index{Name: "i", Kind: ir.IndexKindBTree, Columns: []ir.IndexColumn{
				{Column: "A"}, {Column: "b", Length: 10, Desc: true},
			}},
			"(a, b(10) DESC)",
		},
		{
			"unique btree",
			&ir.Index{Name: "i", Unique: true, Columns: []ir.IndexColumn{{Column: "v"}}},
			"UNIQUE (v)",
		},
		{
			"fulltext drops unique and prefix, renders its kind",
			&ir.Index{Name: "i", Kind: ir.IndexKindFullText, Unique: true, Columns: []ir.IndexColumn{
				{Column: "txt", Length: 32},
			}},
			"FULLTEXT (txt)",
		},
		{
			"spatial drops prefix, renders its kind",
			&ir.Index{Name: "i", Kind: ir.IndexKindSpatial, Columns: []ir.IndexColumn{
				{Column: "pt", Length: 32},
			}},
			"SPATIAL (pt)",
		},
		{
			"hash renders its kind as the USING clause",
			&ir.Index{Name: "i", Kind: ir.IndexKindHash, Columns: []ir.IndexColumn{{Column: "k"}}},
			"(k) USING HASH",
		},
		{
			"expression entry matches positionally",
			&ir.Index{Name: "i", Columns: []ir.IndexColumn{
				{Expression: "lower(`email`)", ExpressionDialect: "mysql"},
			}},
			"((<expression>))",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatIndexCatalogDef(intendedIndexCatalogDef(tc.idx)); got != tc.want {
				t.Errorf("intended def = %s; want %s", got, tc.want)
			}
		})
	}
}
