// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the batched catalog probes (audit V-1): the three formerly
// N+1 detect/verify sites — VerifyIndexes, buildTableIndexes'
// detect-then-skip, and CreateConstraints' per-FK probe — now issue ONE
// information_schema read per [catalogProbeChunk] wanted objects, with every
// value riding a parameter. On a vtgate/PlanetScale target each probe is a
// serial cluster round trip, so the N+1 shape cost hundreds of RTTs per
// phase at scale; these pins make a silent regression back to per-object
// probing (or to string-built SQL) fail loudly.

package mysql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// catalogQueries filters the recorded probes down to the ones that touched
// catalog. All probes hit information_schema, so in these tests every
// recorded query qualifies; the helper exists to make intent explicit.
func catalogQueries(rec *indexRecorder) []recordedQuery {
	var out []recordedQuery
	for _, q := range rec.querySnapshot() {
		if strings.Contains(q.query, "information_schema") {
			out = append(out, q)
		}
	}
	return out
}

// multiIndexTable returns a PK table carrying nIdx secondary indexes named
// <name>_i<k>_idx, so the batched probes have a multi-object work-list.
func multiIndexTable(name string, nIdx int) *ir.Table {
	t := &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: pk(),
	}
	for k := 0; k < nIdx; k++ {
		t.Indexes = append(t.Indexes, &ir.Index{
			Name:    fmt.Sprintf("%s_i%d_idx", name, k),
			Columns: []ir.IndexColumn{{Column: "v"}},
		})
	}
	return t
}

// TestVerifyIndexes_BatchedProbe_OneQuery pins the V-1 acceptance shape: a
// multi-table, multi-index schema verifies with EXACTLY ONE catalog query —
// not one per index — and that query is the batched statistics form with
// every table and index name riding a parameter (schema + tables + names).
func TestVerifyIndexes_BatchedProbe_OneQuery(t *testing.T) {
	rec := &indexRecorder{exists: true}
	db := newIndexFakeDB(t, rec)
	w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorPlanetScale}

	// 3 tables × 3 indexes = 9 expected objects (25 probes would have been
	// 9 here pre-fix; the point is the count no longer scales with indexes).
	schema := &ir.Schema{Tables: []*ir.Table{
		multiIndexTable("orders", 3), multiIndexTable("users", 3), multiIndexTable("events", 3),
	}}
	if err := w.VerifyIndexes(context.Background(), schema); err != nil {
		t.Fatalf("VerifyIndexes on a fully-indexed target must pass; got %v", err)
	}

	qs := catalogQueries(rec)
	if len(qs) != 1 {
		t.Fatalf("VerifyIndexes issued %d catalog queries; want exactly 1 (the batched probe)", len(qs))
	}
	q := qs[0]
	for _, want := range []string{"information_schema.statistics", "table_schema = ?", "table_name IN (", "index_name IN ("} {
		if !strings.Contains(q.query, want) {
			t.Errorf("batched probe missing %q: %s", want, q.query)
		}
	}
	// schema + 3 tables + 9 distinct index names, all parameterized.
	if want := 1 + 3 + 9; q.args != want {
		t.Errorf("batched probe args = %d; want %d (schema + tables + index names)", q.args, want)
	}
	// No value may be string-built into the query (the parameterization pin).
	for _, name := range []string{"orders", "users", "events", "_idx", "testdb"} {
		if strings.Contains(q.query, name) {
			t.Errorf("batched probe embeds value %q instead of a placeholder: %s", name, q.query)
		}
	}
}

// TestCreateIndexes_BatchedDetectThenSkip pins that the build path's
// idempotent detect-then-skip survives the batching UNCHANGED: an index whose
// ALTER already landed (probeBuilt — the committed-but-unacked resume shape)
// is skipped, the absent siblings still build, and the whole table costs ONE
// catalog probe.
func TestCreateIndexes_BatchedDetectThenSkip(t *testing.T) {
	rec := &indexRecorder{
		probeBuilt: true, // probes answer from recorded ALTERs
		// Seed: t_i0_idx's ALTER already landed in a prior attempt.
		execs: []string{"ALTER TABLE `t` ADD INDEX `t_i0_idx` (`v`)"},
	}
	db := newIndexFakeDB(t, rec)
	w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorVanilla}

	schema := &ir.Schema{Tables: []*ir.Table{multiIndexTable("t", 3)}}
	if err := w.CreateIndexes(context.Background(), schema); err != nil {
		t.Fatalf("CreateIndexes: %v", err)
	}

	if qs := catalogQueries(rec); len(qs) != 1 {
		t.Fatalf("CreateIndexes issued %d catalog queries for one table; want exactly 1", len(qs))
	}
	execs := rec.snapshot()[1:] // drop the seeded statement
	if len(execs) != 1 {
		t.Fatalf("CreateIndexes emitted %d ALTERs; want 1 combined ALTER for the two absent indexes: %v", len(execs), execs)
	}
	if strings.Contains(execs[0], "`t_i0_idx`") {
		t.Errorf("detect-then-skip violated: the already-built t_i0_idx was re-created (a 1061 in production): %s", execs[0])
	}
	for _, want := range []string{"`t_i1_idx`", "`t_i2_idx`"} {
		if !strings.Contains(execs[0], want) {
			t.Errorf("absent index %s not built: %s", want, execs[0])
		}
	}
}

// TestProbeCatalogPairs_ChunksLargeSets pins the IN-list chunking: a wanted
// set larger than catalogProbeChunk splits into ceil(n/chunk) queries, each
// within the placeholder budget, and the union still covers every pair.
func TestProbeCatalogPairs_ChunksLargeSets(t *testing.T) {
	rec := &indexRecorder{exists: true}
	db := newIndexFakeDB(t, rec)

	n := 2*catalogProbeChunk + 7 // → 3 chunks
	wanted := make([]catalogPair, 0, n)
	for i := 0; i < n; i++ {
		wanted = append(wanted, catalogPair{
			table: fmt.Sprintf("t%02d", i%20),
			name:  fmt.Sprintf("idx_%04d", i),
		})
	}
	got, err := probeCatalogPairs(context.Background(), db, "testdb", wanted, statisticsPairsQuery)
	if err != nil {
		t.Fatalf("probeCatalogPairs: %v", err)
	}

	qs := catalogQueries(rec)
	if len(qs) != 3 {
		t.Fatalf("probeCatalogPairs issued %d queries for %d pairs; want 3 (chunk = %d)", len(qs), n, catalogProbeChunk)
	}
	for i, q := range qs {
		if maxArgs := 1 + 2*catalogProbeChunk; q.args > maxArgs {
			t.Errorf("chunk %d carries %d args; want <= %d (placeholder budget)", i, q.args, maxArgs)
		}
	}
	for _, p := range wanted {
		if _, ok := got[foldCatalogPair(p.table, p.name)]; !ok {
			t.Fatalf("pair %s.%s lost across the chunk boundary", p.table, p.name)
		}
	}

	// And the inverse: an all-absent target yields an empty set (no false
	// positives from the chunking machinery itself).
	rec2 := &indexRecorder{exists: false}
	db2 := newIndexFakeDB(t, rec2)
	got2, err := probeCatalogPairs(context.Background(), db2, "testdb", wanted, statisticsPairsQuery)
	if err != nil {
		t.Fatalf("probeCatalogPairs (absent): %v", err)
	}
	if len(got2) != 0 {
		t.Fatalf("all-absent probe returned %d pairs; want 0", len(got2))
	}
}

// TestProbeCatalogPairs_EmptyWantedIssuesNoQuery pins the degenerate case: no
// wanted objects → zero catalog round trips (the pre-fix code also issued
// zero probes here; the batch must not add a gratuitous empty-IN query,
// which would be a SQL syntax error anyway).
func TestProbeCatalogPairs_EmptyWantedIssuesNoQuery(t *testing.T) {
	rec := &indexRecorder{exists: true}
	db := newIndexFakeDB(t, rec)
	got, err := probeCatalogPairs(context.Background(), db, "testdb", nil, statisticsPairsQuery)
	if err != nil {
		t.Fatalf("probeCatalogPairs(nil): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty wanted returned %d pairs; want 0", len(got))
	}
	if qs := catalogQueries(rec); len(qs) != 0 {
		t.Fatalf("empty wanted issued %d queries; want 0", len(qs))
	}
}

// TestVerifyIndexes_CaseFoldedCatalogAnswer pins the foldCatalogPair wart:
// a target whose catalog reports identifier case differently than the IR
// (lower_case_table_names variance) must still verify green — the old
// per-object probes compared DB-side under information_schema's ci
// collation, and the Go-side set compare must keep that semantic rather
// than false-flag every index as missing.
func TestVerifyIndexes_CaseFoldedCatalogAnswer(t *testing.T) {
	rec := &indexRecorder{exists: true, answerUppercase: true}
	db := newIndexFakeDB(t, rec)
	w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorVanilla}

	schema := &ir.Schema{Tables: []*ir.Table{indexedTable("orders")}}
	if err := w.VerifyIndexes(context.Background(), schema); err != nil {
		t.Fatalf("VerifyIndexes must match a case-shifted catalog answer (ci identifier compare); got %v", err)
	}
}

// fkTable returns a PK table carrying nFK foreign keys named <name>_fk<k>,
// each referencing parent(id).
func fkTable(name string, nFK int) *ir.Table {
	t := &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "pid", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: pk(),
	}
	for k := 0; k < nFK; k++ {
		t.ForeignKeys = append(t.ForeignKeys, &ir.ForeignKey{
			Name:              fmt.Sprintf("%s_fk%d", name, k),
			Columns:           []string{"pid"},
			ReferencedTable:   "parent",
			ReferencedColumns: []string{"id"},
		})
	}
	return t
}

// TestCreateConstraints_BatchedProbe pins the FK leg of V-1: a multi-table,
// multi-FK schema probes with EXACTLY ONE batched TABLE_CONSTRAINTS query,
// and the detect-then-skip semantics are preserved in both directions —
// all-absent builds every FK, all-present builds none.
func TestCreateConstraints_BatchedProbe(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{fkTable("orders", 2), fkTable("items", 2)}}

	t.Run("absent-builds-all", func(t *testing.T) {
		rec := &indexRecorder{exists: false}
		db := newIndexFakeDB(t, rec)
		w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorPlanetScale}
		if err := w.CreateConstraints(context.Background(), schema); err != nil {
			t.Fatalf("CreateConstraints: %v", err)
		}
		qs := catalogQueries(rec)
		if len(qs) != 1 {
			t.Fatalf("CreateConstraints issued %d catalog queries; want exactly 1 (the batched probe)", len(qs))
		}
		for _, want := range []string{"information_schema.TABLE_CONSTRAINTS", "CONSTRAINT_TYPE = 'FOREIGN KEY'", "TABLE_NAME IN (", "CONSTRAINT_NAME IN ("} {
			if !strings.Contains(qs[0].query, want) {
				t.Errorf("batched FK probe missing %q: %s", want, qs[0].query)
			}
		}
		// schema + 2 tables + 4 distinct FK names, all parameterized.
		if want := 1 + 2 + 4; qs[0].args != want {
			t.Errorf("batched FK probe args = %d; want %d", qs[0].args, want)
		}
		execs := rec.snapshot()
		if len(execs) != 4 {
			t.Fatalf("CreateConstraints emitted %d ALTERs; want 4 (every FK absent): %v", len(execs), execs)
		}
		for _, stmt := range execs {
			if !strings.Contains(stmt, "FOREIGN KEY") {
				t.Errorf("recorded statement is not an ADD FOREIGN KEY: %q", stmt)
			}
		}
	})

	t.Run("present-skips-all", func(t *testing.T) {
		rec := &indexRecorder{exists: true}
		db := newIndexFakeDB(t, rec)
		w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorPlanetScale}
		if err := w.CreateConstraints(context.Background(), schema); err != nil {
			t.Fatalf("CreateConstraints: %v", err)
		}
		if execs := rec.snapshot(); len(execs) != 0 {
			t.Fatalf("CreateConstraints re-added %d existing FKs (a 1826 in production): %v", len(execs), execs)
		}
	})

	t.Run("no-fks-no-query", func(t *testing.T) {
		rec := &indexRecorder{exists: false}
		db := newIndexFakeDB(t, rec)
		w := &SchemaWriter{db: db, schema: "testdb", flavor: FlavorVanilla}
		plain := &ir.Schema{Tables: []*ir.Table{multiIndexTable("p0", 0)}}
		if err := w.CreateConstraints(context.Background(), plain); err != nil {
			t.Fatalf("CreateConstraints (no FKs): %v", err)
		}
		if qs := catalogQueries(rec); len(qs) != 0 {
			t.Fatalf("a schema with no FKs issued %d catalog queries; want 0", len(qs))
		}
	})
}
