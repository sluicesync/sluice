// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for ADR-0123: budget-wide intra-table PK-range work-stealing on
// the migrate / PG-cold-start parallel copy.
//
// These pin the SCHEDULING change only — the finer deterministic chunk count
// (resolveParallelChunkCount) and that resolveChunks feeds it into the SAME
// pinned boundary functions across the PK families, so a large table now
// splits into enough chunks to fill the whole budget while resume reloads the
// persisted set unchanged. The half-open (lower, upper] tiling itself stays
// pinned by chunk_test.go / chunk_keyset_test.go.

package pipeline

import (
	"context"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// chunkCountReader is a stub ir.RowReader that also implements the boundary
// surfaces (RangeBoundsQuerier + KeysetSampler) and RowCounter, so it can drive
// resolveChunks for BOTH the integer (MIN/MAX/divide) and keyset strategies
// without a database. The integer span is [1, count]; the keyset boundaries are
// n-1 evenly-spaced synthetic single-column tuples over [1, count].
type chunkCountReader struct {
	count int64
}

func (r *chunkCountReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	ch := make(chan ir.Row)
	close(ch)
	return ch, nil
}
func (r *chunkCountReader) Err() error { return nil }

func (r *chunkCountReader) CountRows(context.Context, *ir.Table) (int64, error) {
	return r.count, nil
}

func (r *chunkCountReader) RangeBounds(context.Context, *ir.Table, string) (minVal, maxVal any, err error) {
	if r.count == 0 {
		return nil, nil, nil
	}
	return int64(1), r.count, nil
}

func (r *chunkCountReader) SampleKeysetBoundaries(_ context.Context, _ *ir.Table, _ []string, n int) ([][]any, error) {
	// n-1 interior boundaries, evenly spaced over [1, count], as 1-wide tuples
	// (the test keyset table has a single non-integer PK). Returned in order.
	if r.count == 0 || n <= 1 {
		return nil, nil
	}
	out := make([][]any, 0, n-1)
	step := r.count / int64(n)
	if step < 1 {
		step = 1
	}
	for k := int64(1); k < int64(n); k++ {
		out = append(out, []any{k * step})
	}
	return out, nil
}

func keysetPKTable() *ir.Table {
	return &ir.Table{
		Name: "tokens",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.UUID{}},
			{Name: "label", Type: ir.Varchar{Length: 64}},
		},
		PrimaryKey: &ir.Index{Name: "pk", Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

func TestResolveParallelChunkCount(t *testing.T) {
	cases := []struct {
		name        string
		est         int64
		threshold   int64
		parallelism int
		budget      int
		want        int
	}{
		// Below/at threshold floors to parallelism (the old fixed width) — never
		// fewer chunks than pre-ADR-0123.
		{"at_threshold_floors_to_parallelism", 100_000, 100_000, 4, 16, 4},
		{"just_above_threshold_floors", 150_000, 100_000, 4, 16, 4},
		// est/threshold above parallelism but below the cap: ceil-divide wins.
		{"ceil_divide_wins", 1_000_000, 100_000, 4, 64, 10},
		// Dominant table: ceil far above the cap → clamped to max(64, budget).
		{"dominant_clamped_to_64", 10_000_000, 100_000, 4, 16, 64},
		// Budget > 64 raises the cap so the table can still fill the budget.
		{"cap_raised_to_budget", 100_000_000, 100_000, 4, 100, 100},
		// Defensive: zero threshold treated as 1 (no div-by-zero), still capped.
		{"zero_threshold", 10_000, 0, 4, 16, 64},
		// Defensive floor at 2 if parallelism somehow < 2.
		{"floor_two", 50_000, 100_000, 1, 16, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deps := &parallelBulkCopyDeps{
				parallelism: tc.parallelism,
				minRows:     tc.threshold,
				copyBudget:  tc.budget,
			}
			got := resolveParallelChunkCount(context.Background(), &chunkCountReader{count: tc.est}, integerPKTable(), deps)
			if got != tc.want {
				t.Errorf("resolveParallelChunkCount(est=%d,threshold=%d,parallelism=%d,budget=%d) = %d; want %d",
					tc.est, tc.threshold, tc.parallelism, tc.budget, got, tc.want)
			}
		})
	}
}

// TestResolveChunks_FinerThanParallelism is the load-bearing pin: a fresh LARGE
// table resolves to MANY more than --bulk-parallelism chunks (filling the
// budget), tiled contiguously and exactly-once, across BOTH PK families.
func TestResolveChunks_FinerThanParallelism(t *testing.T) {
	for _, tc := range []struct {
		name  string
		table *ir.Table
	}{
		{"integer_pk", integerPKTable()},
		{"keyset_pk", keysetPKTable()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// budget 16, parallelism 4: old code produced exactly 4 chunks.
			deps := &parallelBulkCopyDeps{parallelism: 4, minRows: 100_000, copyBudget: 16}
			// est = 10M with threshold 100k ⇒ ceil 100 ⇒ clamp to max(64,16)=64.
			rdr := &chunkCountReader{count: 10_000_000}
			state := &ir.MigrationState{TableProgress: map[string]ir.TableProgress{}}
			var mu sync.Mutex
			chunks, err := resolveChunks(context.Background(), state, &mu, resumeContext{}, rdr, tc.table, deps, false)
			if err != nil {
				t.Fatalf("resolveChunks: %v", err)
			}
			if len(chunks) <= deps.parallelism {
				t.Fatalf("got %d chunks; want FINER than --bulk-parallelism (%d) so the big table can fill the budget", len(chunks), deps.parallelism)
			}
			if len(chunks) != 64 {
				t.Fatalf("got %d chunks; want 64 (clamp(ceil(10M/100k),4,max(64,16)))", len(chunks))
			}
			assertChunkTilingContiguous(t, chunks)
			// Persisted under the table's key so resume reloads it.
			if got := len(state.TableProgress[tc.table.Name].Chunks); got != len(chunks) {
				t.Fatalf("persisted %d chunks; want %d", got, len(chunks))
			}
		})
	}
}

// TestResolveChunks_ResumeReloadsPersisted pins that resume re-derives the
// IDENTICAL chunk set by reloading the persisted boundaries — independent of
// the est-driven M a fresh run would now choose (ADR-0123 Decision 3, the
// ADR-0099 §5 partition-stability requirement). A migration started under the
// old fixed-M=4 code must resume with exactly those 4 chunks.
func TestResolveChunks_ResumeReloadsPersisted(t *testing.T) {
	persisted := []ir.TableChunkProgress{
		{ChunkIndex: 0, UpperPK: []any{int64(2500)}, State: ir.TableProgressInProgress},
		{ChunkIndex: 1, LowerPK: []any{int64(2500)}, UpperPK: []any{int64(5000)}, State: ir.TableProgressInProgress},
		{ChunkIndex: 2, LowerPK: []any{int64(5000)}, UpperPK: []any{int64(7500)}, State: ir.TableProgressComplete},
		{ChunkIndex: 3, LowerPK: []any{int64(7500)}, State: ir.TableProgressInProgress},
	}
	state := &ir.MigrationState{TableProgress: map[string]ir.TableProgress{
		"users": {State: ir.TableProgressInProgress, Chunks: persisted},
	}}
	var mu sync.Mutex
	// A fresh run with this est would pick 64; resume must IGNORE that and
	// reload the 4 persisted chunks unchanged.
	deps := &parallelBulkCopyDeps{parallelism: 4, minRows: 100_000, copyBudget: 16}
	rdr := &chunkCountReader{count: 10_000_000}
	chunks, err := resolveChunks(context.Background(), state, &mu, resumeContext{}, rdr, integerPKTable(), deps, true /* resuming */)
	if err != nil {
		t.Fatalf("resolveChunks (resume): %v", err)
	}
	if len(chunks) != len(persisted) {
		t.Fatalf("resume re-derived %d chunks; want the %d persisted (boundary set must be stable across resumes)", len(chunks), len(persisted))
	}
	for i := range chunks {
		if chunks[i].ChunkIndex != persisted[i].ChunkIndex || chunks[i].State != persisted[i].State {
			t.Errorf("chunk %d changed on resume: got %+v want %+v", i, chunks[i], persisted[i])
		}
	}
}

// assertChunkTilingContiguous verifies the chunk set tiles its PK range with no
// gap and no overlap: chunk 0 has nil lower, the last has nil upper, and each
// interior chunk's lower equals the previous chunk's upper (the half-open
// (lower, upper] convention) — the exactly-once coverage property the finer
// split must preserve.
func assertChunkTilingContiguous(t *testing.T, chunks []ir.TableChunkProgress) {
	t.Helper()
	if len(chunks) == 0 {
		t.Fatal("no chunks")
	}
	if chunks[0].LowerPK != nil {
		t.Errorf("chunk 0 lower = %v; want nil (open start)", chunks[0].LowerPK)
	}
	if chunks[len(chunks)-1].UpperPK != nil {
		t.Errorf("last chunk upper = %v; want nil (open end)", chunks[len(chunks)-1].UpperPK)
	}
	for i := 1; i < len(chunks); i++ {
		if migcore.ComparePKTuple(chunks[i].LowerPK, chunks[i-1].UpperPK) != 0 {
			t.Errorf("chunk %d lower %v != chunk %d upper %v (gap/overlap)", i, chunks[i].LowerPK, i-1, chunks[i-1].UpperPK)
		}
	}
}
