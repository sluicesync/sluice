// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// --- ADR-0119 intra-table PK-range work-stealing: work-item construction pins ---
//
// These pin [buildCopyWorkItems] / [chunkItemsFor] WITHOUT a real DB: the
// load-bearing facts are (1) an eligible large table tiles into M chunk items
// whose half-open bounds cover the keyspace with no gap/overlap (chunk 0
// lowerPK==nil, last upperPK==nil, contiguous); (2) every ineligible / small /
// opted-out table stays a single whole-table item (so the keyless at-least-once
// contract + tier-(a) behaviour are unchanged); (3) the M clamp holds; (4) the
// per-work-item cursor key disambiguates concurrent chunks of one table.

// chunkedWSReader is a fake [ir.ChunkedWorkStealingCopyReader] for the
// work-item-construction pins. It serves no rows (these tests exercise only the
// build-time tiling/eligibility decision, not the read path) — ReadRows* return
// a closed channel. CountRows / RangeBounds / SampleKeysetBoundaries are seeded
// per table so a test controls the chunk decision deterministically.
type chunkedWSReader struct {
	n       int
	counts  map[string]int64    // CountRows estimate per table
	ranges  map[string][2]int64 // integer RangeBounds [min,max] per table
	keysets map[string][][]any  // keyset interior boundary tuples per table
}

func (r *chunkedWSReader) ConcurrentCopyGroups() [][]string { return nil }
func (r *chunkedWSReader) ConcurrentReaderCount() int       { return r.n }
func (r *chunkedWSReader) Err() error                       { return nil }

func (r *chunkedWSReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	return closedRowChan(), nil
}

func (r *chunkedWSReader) ReadRowsOn(context.Context, *ir.Table, int) (<-chan ir.Row, error) {
	return closedRowChan(), nil
}

func (r *chunkedWSReader) ReadRowsRangeOn(context.Context, *ir.Table, []any, []any, int, int) (<-chan ir.Row, error) {
	return closedRowChan(), nil
}

func (r *chunkedWSReader) CountRows(_ context.Context, table *ir.Table) (int64, error) {
	return r.counts[table.Name], nil
}

func (r *chunkedWSReader) RangeBounds(_ context.Context, table *ir.Table, _ string) (minVal, maxVal any, err error) {
	mm, ok := r.ranges[table.Name]
	if !ok {
		return nil, nil, nil
	}
	return mm[0], mm[1], nil
}

func (r *chunkedWSReader) SampleKeysetBoundaries(_ context.Context, table *ir.Table, _ []string, _ int) ([][]any, error) {
	return r.keysets[table.Name], nil
}

func closedRowChan() <-chan ir.Row {
	ch := make(chan ir.Row)
	close(ch)
	return ch
}

func byNameOf(tables ...*ir.Table) map[string]*ir.Table {
	m := make(map[string]*ir.Table, len(tables))
	for _, t := range tables {
		m[t.Name] = t
	}
	return m
}

func namesOf(tables ...*ir.Table) []string {
	out := make([]string, len(tables))
	for i, t := range tables {
		out[i] = t.Name
	}
	return out
}

// strPKTable builds a single non-integer (varchar) orderable PK table → the
// keyset chunk strategy.
func strPKTable(name string) *ir.Table {
	return &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Varchar{Length: 64}},
			{Name: "v", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

// noPKChunkTable has no primary key → never chunked (keyless at-least-once).
func noPKChunkTable(name string) *ir.Table {
	return &ir.Table{
		Name:    name,
		Columns: []*ir.Column{{Name: "v", Type: ir.Text{}}},
	}
}

// jsonPKTable's PK column is a non-orderable type → never chunked.
func jsonPKTable(name string) *ir.Table {
	return &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.JSON{}},
			{Name: "v", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

// injectedPKTable's leading PK column is sluice-injected (not present on the
// source) → never chunked (the Bug-80 class).
func injectedPKTable(name string) *ir.Table {
	return &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "shard", Type: ir.Integer{Width: 64}, SluiceInjected: true},
			{Name: "v", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "shard"}}},
	}
}

// assertContiguousTiling pins the half-open coverage invariant: chunk 0 has a
// nil lower bound, the last chunk a nil upper bound, indices are 0..M-1 in
// order, and each chunk's lower bound EQUALS the previous chunk's upper bound
// (no gap, no overlap) under the same total order the engines' cursor uses.
func assertContiguousTiling(t *testing.T, items []copyWorkItem, wantTable string) {
	t.Helper()
	if len(items) < 2 {
		t.Fatalf("want ≥2 chunk items, got %d", len(items))
	}
	for i, it := range items {
		if it.table.Name != wantTable {
			t.Fatalf("item %d table = %q; want %q", i, it.table.Name, wantTable)
		}
		if it.chunkIndex != i {
			t.Fatalf("item %d chunkIndex = %d; want %d (in order)", i, it.chunkIndex, i)
		}
	}
	if items[0].lowerPK != nil {
		t.Errorf("chunk 0 lowerPK = %v; want nil (no lower bound)", items[0].lowerPK)
	}
	if last := items[len(items)-1]; last.upperPK != nil {
		t.Errorf("last chunk upperPK = %v; want nil (no upper bound)", last.upperPK)
	}
	for i := 1; i < len(items); i++ {
		// previous chunk's INCLUSIVE upper == this chunk's EXCLUSIVE lower.
		if migcore.ComparePKTuple(items[i-1].upperPK, items[i].lowerPK) != 0 {
			t.Errorf("gap/overlap between chunk %d (upper=%v) and chunk %d (lower=%v)",
				i-1, items[i-1].upperPK, i, items[i].lowerPK)
		}
	}
}

func TestBuildCopyWorkItems_IntegerChunkTiling(t *testing.T) {
	tbl := concTable("big")
	reader := &chunkedWSReader{
		n:      4,
		counts: map[string]int64{"big": 400_000}, // threshold 80k (1 table) → M = 5
		ranges: map[string][2]int64{"big": {1, 1000}},
	}
	items, err := buildCopyWorkItems(context.Background(), namesOf(tbl), byNameOf(tbl), reader, false)
	if err != nil {
		t.Fatalf("buildCopyWorkItems: %v", err)
	}
	if len(items) != 5 {
		t.Fatalf("integer chunking produced %d items; want 5 (ceil(400k/80k))", len(items))
	}
	assertContiguousTiling(t, items, "big")
}

func TestBuildCopyWorkItems_KeysetChunkTiling(t *testing.T) {
	tbl := strPKTable("s")
	reader := &chunkedWSReader{
		n:       4,
		counts:  map[string]int64{"s": 400_000},
		keysets: map[string][][]any{"s": {{"m"}, {"t"}}}, // 2 interior boundaries → 3 chunks
	}
	items, err := buildCopyWorkItems(context.Background(), namesOf(tbl), byNameOf(tbl), reader, false)
	if err != nil {
		t.Fatalf("buildCopyWorkItems: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("keyset chunking produced %d items; want 3 (2 boundaries + 1)", len(items))
	}
	assertContiguousTiling(t, items, "s")
	// Ground the actual split points.
	if migcore.ComparePKTuple(items[0].upperPK, []any{"m"}) != 0 {
		t.Errorf("chunk 0 upper = %v; want [m]", items[0].upperPK)
	}
	if migcore.ComparePKTuple(items[1].upperPK, []any{"t"}) != 0 {
		t.Errorf("chunk 1 upper = %v; want [t]", items[1].upperPK)
	}
}

func TestBuildCopyWorkItems_WholeWhenIneligibleOrSmall(t *testing.T) {
	cases := []struct {
		name  string
		table *ir.Table
		est   int64
		rng   [2]int64
	}{
		{"sub-threshold integer", concTable("small"), 79_999, [2]int64{1, 1000}},
		{"no primary key", noPKChunkTable("nopk"), 5_000_000, [2]int64{}},
		{"non-orderable PK (json)", jsonPKTable("j"), 5_000_000, [2]int64{}},
		{"sluice-injected PK", injectedPKTable("shrd"), 5_000_000, [2]int64{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reader := &chunkedWSReader{
				n:      4,
				counts: map[string]int64{c.table.Name: c.est},
				ranges: map[string][2]int64{c.table.Name: c.rng},
			}
			items, err := buildCopyWorkItems(context.Background(), namesOf(c.table), byNameOf(c.table), reader, false)
			if err != nil {
				t.Fatalf("buildCopyWorkItems: %v", err)
			}
			if len(items) != 1 {
				t.Fatalf("got %d items; want 1 whole-table item", len(items))
			}
			if items[0].chunkIndex != -1 || items[0].lowerPK != nil || items[0].upperPK != nil {
				t.Errorf("got %+v; want whole-table item (chunkIndex -1, nil bounds)", items[0])
			}
		})
	}
}

func TestBuildCopyWorkItems_NoIntraTableStealing_AllWhole(t *testing.T) {
	// Two eligible large tables, but the opt-out forces both to whole items.
	a := concTable("a")
	b := strPKTable("b")
	reader := &chunkedWSReader{
		n:       4,
		counts:  map[string]int64{"a": 400_000, "b": 400_000},
		ranges:  map[string][2]int64{"a": {1, 1000}},
		keysets: map[string][][]any{"b": {{"m"}, {"t"}}},
	}
	items, err := buildCopyWorkItems(context.Background(), namesOf(a, b), byNameOf(a, b), reader, true /* noIntraTableStealing */)
	if err != nil {
		t.Fatalf("buildCopyWorkItems: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("opt-out produced %d items; want 2 whole-table items", len(items))
	}
	for _, it := range items {
		if it.chunkIndex != -1 {
			t.Errorf("table %q chunkIndex = %d; want -1 (whole) under --no-intra-table-stealing", it.table.Name, it.chunkIndex)
		}
	}
}

// TestBuildCopyWorkItems_NonChunkCapableReaderStaysWhole pins that a reader
// implementing only the WHOLE-table work-stealing surface (not the chunked one)
// never chunks — the type-assert gate, so tier-(a) readers are unaffected.
func TestBuildCopyWorkItems_NonChunkCapableReaderStaysWhole(t *testing.T) {
	groups := [][]string{{"a"}}
	// wsConcReader implements ir.WorkStealingCopyReader but NOT
	// ir.ChunkedWorkStealingCopyReader, so the chunk-capable type-assert fails
	// and the table stays whole BEFORE the CountRows estimate is even consulted
	// — the row count is irrelevant, so seed just one.
	reader := newWSConcReader(groups, 4, map[string]int{"a": 1})
	tbl := concTable("a")
	items, err := buildCopyWorkItems(context.Background(), namesOf(tbl), byNameOf(tbl), reader, false)
	if err != nil {
		t.Fatalf("buildCopyWorkItems: %v", err)
	}
	if len(items) != 1 || items[0].chunkIndex != -1 {
		t.Fatalf("non-chunk-capable reader produced %+v; want 1 whole-table item", items)
	}
}

func TestBuildCopyWorkItems_MClamp(t *testing.T) {
	tbl := concTable("huge")
	reader := &chunkedWSReader{
		n: 4,
		// est / threshold = 1000, far above the cap.
		counts: map[string]int64{"huge": 80_000 * 1000},
		// span 100_000 ≥ migcore.MaxChunksPerTable, so the boundary code does not collapse n.
		ranges: map[string][2]int64{"huge": {1, 100_000}},
	}
	items, err := buildCopyWorkItems(context.Background(), namesOf(tbl), byNameOf(tbl), reader, false)
	if err != nil {
		t.Fatalf("buildCopyWorkItems: %v", err)
	}
	if len(items) != migcore.MaxChunksPerTable {
		t.Fatalf("M clamp produced %d items; want %d (migcore.MaxChunksPerTable)", len(items), migcore.MaxChunksPerTable)
	}
	assertContiguousTiling(t, items, "huge")
}

// TestBuildCopyWorkItems_MissingTableIsLoud pins the partition/scope-mismatch
// guard: a table the engine surfaced but the schema lacks fails LOUDLY at build
// time (before any goroutine spawns), never silently dropping a table.
func TestBuildCopyWorkItems_MissingTableIsLoud(t *testing.T) {
	reader := &chunkedWSReader{n: 2, counts: map[string]int64{}}
	_, err := buildCopyWorkItems(context.Background(), []string{"ghost"}, map[string]*ir.Table{}, reader, false)
	if err == nil {
		t.Fatal("missing table did not fail loudly — a silently un-copied table is the worst silent-loss class")
	}
}

// chunkServeReader is an in-memory [ir.ChunkedWorkStealingCopyReader] that
// actually SERVES the half-open PK range requested — so a test can drive the
// full chunked work-stealing copy (boundary compute → claim → range read →
// write) WITHOUT a DB and assert exactly-once coverage across chunks. Rows are
// integer-keyed 0..count-1 on the "id" column.
type chunkServeReader struct {
	n     int
	count int64
}

func (r *chunkServeReader) ConcurrentCopyGroups() [][]string { return nil }
func (r *chunkServeReader) ConcurrentReaderCount() int       { return r.n }
func (r *chunkServeReader) Err() error                       { return nil }
func (r *chunkServeReader) CountRows(context.Context, *ir.Table) (int64, error) {
	return r.count, nil
}

func (r *chunkServeReader) RangeBounds(context.Context, *ir.Table, string) (minVal, maxVal any, err error) {
	if r.count == 0 {
		return nil, nil, nil
	}
	return int64(0), r.count - 1, nil
}

func (r *chunkServeReader) SampleKeysetBoundaries(context.Context, *ir.Table, []string, int) ([][]any, error) {
	return nil, nil // integer PK → MIN/MAX path, never called
}

func (r *chunkServeReader) ReadRows(ctx context.Context, _ *ir.Table) (<-chan ir.Row, error) {
	return r.serve(ctx, nil, nil), nil
}

func (r *chunkServeReader) ReadRowsOn(ctx context.Context, _ *ir.Table, _ int) (<-chan ir.Row, error) {
	return r.serve(ctx, nil, nil), nil
}

func (r *chunkServeReader) ReadRowsRangeOn(ctx context.Context, _ *ir.Table, lowerPK, upperPK []any, _, _ int) (<-chan ir.Row, error) {
	return r.serve(ctx, lowerPK, upperPK), nil
}

// serve emits the rows whose id is in the half-open range (lower, upper] — the
// SAME predicate the engine pushes into SQL, so the union of the M chunks is
// every row exactly once.
func (r *chunkServeReader) serve(ctx context.Context, lowerPK, upperPK []any) <-chan ir.Row {
	out := make(chan ir.Row)
	go func() {
		defer close(out)
		for id := int64(0); id < r.count; id++ {
			if len(lowerPK) > 0 && id <= lowerPK[0].(int64) {
				continue
			}
			if len(upperPK) > 0 && id > upperPK[0].(int64) {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case out <- ir.Row{"id": id, "v": "x"}:
			}
		}
	}()
	return out
}

// idCapturingWriter records how many times each "id" value was written, so a
// test can assert exactly-once coverage (every id written exactly once — no
// gap, no dup) across the chunked copy.
type idCapturingWriter struct {
	mu  sync.Mutex
	ids map[int64]int
}

func (w *idCapturingWriter) WriteRows(ctx context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	for row := range rows {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		w.mu.Lock()
		w.ids[row["id"].(int64)]++
		w.mu.Unlock()
	}
	return nil
}

// TestRunWorkStealingTableCopy_ChunkedExactlyOnce drives the full chunked
// work-stealing copy end-to-end (in-memory): a large integer-PK table tiles into
// M chunks read concurrently across N readers, and EVERY source row lands on the
// target exactly once — the exactly-once silent-loss surface (ADR-0119 Decision
// 5). It also pins via the observer that chunking actually engaged.
func TestRunWorkStealingTableCopy_ChunkedExactlyOnce(t *testing.T) {
	// One table, so migcore.ResolveBulkParallelMinRows(0,1) == 80_000. Make it large
	// enough to chunk: 5× threshold → 5 chunks.
	const rows = 80_000 * 5
	groups := [][]string{{"big"}}
	schema := concSchema("big")
	reader := &chunkServeReader{n: 4, count: rows}
	writer := &idCapturingWriter{ids: map[int64]int{}}

	var (
		obsMu      sync.Mutex
		chunkCount int
	)
	intraTableChunkObserver = func(table string, chunks int) {
		obsMu.Lock()
		if table == "big" {
			chunkCount = chunks
		}
		obsMu.Unlock()
	}
	defer func() { intraTableChunkObserver = nil }()

	if err := runConcurrentTableCopy(context.Background(), groups, schema, reader, writer, nil, ShardColumnSpec{}, 1, false, false); err != nil {
		t.Fatalf("runConcurrentTableCopy (chunked): %v", err)
	}

	if chunkCount < 2 {
		t.Fatalf("intra-table chunking did not engage: observer saw %d chunks for the large table", chunkCount)
	}
	// Exactly-once: every id 0..rows-1 written exactly once.
	if len(writer.ids) != rows {
		t.Fatalf("distinct ids written = %d; want %d (a gap or extra id is silent loss/dup)", len(writer.ids), rows)
	}
	for id := int64(0); id < rows; id++ {
		if c := writer.ids[id]; c != 1 {
			t.Fatalf("id %d written %d times; want exactly 1", id, c)
		}
	}
}
