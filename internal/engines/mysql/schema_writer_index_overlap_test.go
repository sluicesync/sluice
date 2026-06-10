// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestResolveIndexBuildWorkers pins the MySQL-specific worker-sizing policy
// (ADR-0080): min(default-N, jobCount), clamped to [floor, ceil]. MySQL has
// no connection-slot prober, so the budget (always 0) is NOT an input — this
// fixed-N policy is the only sizing lever.
func TestResolveIndexBuildWorkers(t *testing.T) {
	w := &SchemaWriter{}
	cases := []struct {
		jobCount int
		want     int
	}{
		{jobCount: 0, want: 1},   // floored to 1 even with no jobs
		{jobCount: 1, want: 1},   // one job → one worker
		{jobCount: 3, want: 3},   // fewer jobs than default → jobCount
		{jobCount: 4, want: 4},   // exactly default
		{jobCount: 7, want: 4},   // capped at default N=4
		{jobCount: 100, want: 4}, // capped at default N=4
	}
	for _, c := range cases {
		if got := w.resolveIndexBuildWorkers(c.jobCount); got != c.want {
			t.Errorf("resolveIndexBuildWorkers(%d) = %d; want %d", c.jobCount, got, c.want)
		}
	}
}

// TestResolveIndexBuildWorkers_ClampInvariant pins the [floor, ceil] clamp
// holds regardless of the default policy — a guard so a future bump of
// indexBuildWorkerDefault above the ceil still clamps.
func TestResolveIndexBuildWorkers_ClampInvariant(t *testing.T) {
	w := &SchemaWriter{}
	for jobs := 0; jobs <= 50; jobs++ {
		got := w.resolveIndexBuildWorkers(jobs)
		if got < indexBuildWorkerFloor || got > indexBuildWorkerCeil {
			t.Errorf("resolveIndexBuildWorkers(%d) = %d; outside [%d,%d]",
				jobs, got, indexBuildWorkerFloor, indexBuildWorkerCeil)
		}
	}
}

// TestIndexBuildJobsForTables_Parity pins that indexBuildJobsForTables
// produces exactly the (table, index) work-list the prior CreateIndexes loop
// did: inline-skip indexes (the AUTO_INCREMENT supporting key) are dropped,
// surviving indexes are sorted alphabetically within each table, and PRIMARY
// is never a job. Same SQL on both the whole-schema and overlap paths.
func TestIndexBuildJobsForTables_Parity(t *testing.T) {
	// Table with an AUTO_INCREMENT column `seq` that is NOT the leading PK
	// column, plus an operator index `seq_idx` on it → inlineAutoIncrementIndex
	// returns seq_idx, so inlineSkipIndexNames includes it and the job list
	// must drop it. The two other secondary indexes survive, sorted.
	table := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "seq", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "a", Type: ir.Integer{Width: 64}},
			{Name: "b", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{
			{Name: "z_idx", Columns: []ir.IndexColumn{{Column: "b"}}},
			{Name: "seq_idx", Unique: true, Columns: []ir.IndexColumn{{Column: "seq"}}},
			{Name: "a_idx", Columns: []ir.IndexColumn{{Column: "a"}}},
		},
	}

	// Precondition: the inline-skip set actually contains seq_idx, else the
	// parity assertion below proves nothing.
	if _, skipped := inlineSkipIndexNames(table)["seq_idx"]; !skipped {
		t.Fatalf("test setup: expected seq_idx in inlineSkipIndexNames, got %v",
			inlineSkipIndexNames(table))
	}

	w := &SchemaWriter{}
	jobs := w.indexBuildJobsForTables([]*ir.Table{table})

	// One job per table now (combined-ALTER model): the single job carries
	// the table's full, sorted, skip-filtered index set.
	if len(jobs) != 1 {
		t.Fatalf("indexBuildJobsForTables = %d jobs; want 1 (one per table)", len(jobs))
	}
	if jobs[0].tableName != "t" {
		t.Errorf("job for unexpected table %q", jobs[0].tableName)
	}
	gotNames := make([]string, 0, len(jobs[0].idxs))
	for _, idx := range jobs[0].idxs {
		gotNames = append(gotNames, idx.Name)
	}
	want := []string{"a_idx", "z_idx"} // sorted, seq_idx dropped
	if !reflect.DeepEqual(gotNames, want) {
		t.Errorf("indexBuildJobsForTables names = %v; want %v", gotNames, want)
	}

	// Cross-check against the reference: the same surviving set the prior
	// CreateIndexes loop computed by hand (sorted, inline-skip applied).
	ref := referenceCreateIndexNames(table)
	if !reflect.DeepEqual(gotNames, ref) {
		t.Errorf("indexBuildJobsForTables names = %v; reference loop = %v", gotNames, ref)
	}
}

// referenceCreateIndexNames reproduces the pre-ADR-0080 CreateIndexes
// per-table loop body (skip-inline + sort) independently, so the parity test
// compares two implementations rather than the helper against itself.
func referenceCreateIndexNames(table *ir.Table) []string {
	skip := inlineSkipIndexNames(table)
	indexes := append([]*ir.Index(nil), table.Indexes...)
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name })
	var out []string
	for _, idx := range indexes {
		if _, s := skip[idx.Name]; s {
			continue
		}
		out = append(out, idx.Name)
	}
	return out
}

// TestBuildTableIndexesFromChannel_FlavorGate pins the ADR-0080 flavor gate:
// a PlanetScale/Vitess writer (flavor.usesVStream()) DECLINES the overlap —
// it drains the channel WITHOUT touching the database (no ALTER … ADD INDEX),
// returns nil, but STILL fires the per-table callback for every table so
// resume IndexesBuilt accounting stays correct (the post-copy CreateIndexes
// then builds the indexes).
//
// The decline path never touches w.db, so a nil-db writer is a valid probe:
// any attempt to build would panic on the nil pool, making "no DB touch" an
// observable invariant rather than just an assertion on counts.
func TestBuildTableIndexesFromChannel_FlavorGate(t *testing.T) {
	for _, flavor := range []Flavor{FlavorPlanetScale, FlavorVitess} {
		t.Run(flavor.String(), func(t *testing.T) {
			var mu sync.Mutex
			var fired []string
			w := &SchemaWriter{
				db:     nil, // building would panic — proves the gate declines
				flavor: flavor,
			}
			w.SetTableIndexedCallback(func(table *ir.Table) {
				mu.Lock()
				fired = append(fired, table.Name)
				mu.Unlock()
			})

			// Tables that DO carry secondary indexes — if the gate leaked into
			// the build path it would deref the nil db and panic.
			tables := []*ir.Table{
				indexedTable("t0"),
				indexedTable("t1"),
				indexedTable("t2"),
			}
			schema := &ir.Schema{Tables: tables}
			ch := make(chan *ir.Table, len(tables))
			for _, tbl := range tables {
				ch <- tbl
			}
			close(ch)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := w.BuildTableIndexesFromChannel(ctx, schema, ch); err != nil {
				t.Fatalf("BuildTableIndexesFromChannel (declined): %v", err)
			}

			mu.Lock()
			sort.Strings(fired)
			mu.Unlock()
			want := []string{"t0", "t1", "t2"}
			if !reflect.DeepEqual(fired, want) {
				t.Errorf("callback fired for %v; want %v (every table, so resume accounting holds)", fired, want)
			}
		})
	}
}

// TestBuildTableIndexesFromChannel_NoIndexesDrains pins that a vanilla writer
// fed a schema with NO secondary indexes drains the channel and fires the
// per-table callback for every table without touching the database (no jobs
// to build). nil db again makes the no-build invariant observable.
func TestBuildTableIndexesFromChannel_NoIndexesDrains(t *testing.T) {
	var mu sync.Mutex
	var fired []string
	w := &SchemaWriter{db: nil, flavor: FlavorVanilla}
	w.SetTableIndexedCallback(func(table *ir.Table) {
		mu.Lock()
		fired = append(fired, table.Name)
		mu.Unlock()
	})

	tables := []*ir.Table{
		{Name: "p0", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}, PrimaryKey: pk()},
		{Name: "p1", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}, PrimaryKey: pk()},
	}
	schema := &ir.Schema{Tables: tables}
	ch := make(chan *ir.Table, len(tables))
	for _, tbl := range tables {
		ch <- tbl
	}
	close(ch)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.BuildTableIndexesFromChannel(ctx, schema, ch); err != nil {
		t.Fatalf("BuildTableIndexesFromChannel (no indexes): %v", err)
	}

	mu.Lock()
	sort.Strings(fired)
	mu.Unlock()
	want := []string{"p0", "p1"}
	if !reflect.DeepEqual(fired, want) {
		t.Errorf("callback fired for %v; want %v", fired, want)
	}
}

// indexedTable returns a PK table carrying one secondary index, so the
// build path (if wrongly entered) would have work to do.
func indexedTable(name string) *ir.Table {
	return &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: pk(),
		Indexes:    []*ir.Index{{Name: name + "_v_idx", Columns: []ir.IndexColumn{{Column: "v"}}}},
	}
}

func pk() *ir.Index {
	return &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}
}
