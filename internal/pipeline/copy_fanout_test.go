// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// --- resolveCopyFanoutDegree: the zero-value-safe default trap (v0.99.51) ---

func TestResolveCopyFanoutDegree_ZeroValueSafe(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{0, defaultCopyFanoutDegree},   // the Go zero value → default, NEVER 0 workers
		{-1, defaultCopyFanoutDegree},  // negative → default
		{-99, defaultCopyFanoutDegree}, // negative → default
		{1, 1},                         // explicit serial
		{2, 2},
		{4, 4},
		{maxCopyFanoutDegree, maxCopyFanoutDegree},
		{maxCopyFanoutDegree + 1, maxCopyFanoutDegree}, // capped
		{10000, maxCopyFanoutDegree},                   // capped
	}
	for _, c := range cases {
		if got := resolveCopyFanoutDegree(c.in); got != c.want {
			t.Errorf("resolveCopyFanoutDegree(%d) = %d; want %d", c.in, got, c.want)
		}
		// The load-bearing invariant: no input ever resolves to <1.
		if resolveCopyFanoutDegree(c.in) < 1 {
			t.Fatalf("resolveCopyFanoutDegree(%d) resolved to < 1 — the zero-value trap", c.in)
		}
	}
}

func TestZeroValueStreamerResolvesToUsableDegree(t *testing.T) {
	// A zero-value Streamer (every test, every non-CLI constructor) must
	// resolve to a usable degree, not "zero workers".
	var s Streamer
	if got := resolveCopyFanoutDegree(s.CopyFanoutDegree); got != defaultCopyFanoutDegree {
		t.Fatalf("zero-value Streamer resolved degree = %d; want %d", got, defaultCopyFanoutDegree)
	}
}

// --- exactly-once routing + same-PK→same-worker (silent-loss class) ---

func pkTable() *ir.Table {
	return &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

func TestPartitionRowsByPK_ExactlyOnce(t *testing.T) {
	const degree = 4
	table := pkTable()

	// Build an input multiset that includes DUPLICATE / re-emitted PKs
	// (the Bug-125 VStream COPY catchup shape).
	var input []ir.Row
	for id := 0; id < 200; id++ {
		input = append(input, ir.Row{"id": int64(id), "v": fmt.Sprintf("v%d", id)})
	}
	// Re-emit a subset of PKs (duplicates), as VStream does during catchup.
	for id := 0; id < 50; id++ {
		input = append(input, ir.Row{"id": int64(id), "v": fmt.Sprintf("v%d-reemit", id)})
	}

	ctx := context.Background()
	src := make(chan ir.Row)
	go func() {
		defer close(src)
		for _, r := range input {
			src <- r
		}
	}()

	workers := partitionRowsByPK(ctx, src, table, degree)
	if len(workers) != degree {
		t.Fatalf("partitionRowsByPK returned %d channels; want %d", len(workers), degree)
	}

	// Drain every worker channel concurrently, recording per-worker the
	// set of PKs each saw.
	type seen struct {
		mu    sync.Mutex
		total int
		pkTo  map[int64]int // pk → worker index (must be stable)
	}
	s := &seen{pkTo: map[int64]int{}}
	var wg sync.WaitGroup
	for wi, ch := range workers {
		wg.Add(1)
		go func(wi int, ch <-chan ir.Row) {
			defer wg.Done()
			for row := range ch {
				id := row["id"].(int64)
				s.mu.Lock()
				s.total++
				if prev, ok := s.pkTo[id]; ok && prev != wi {
					t.Errorf("PK %d landed on worker %d AND %d — same PK must pin to one worker", id, prev, wi)
				}
				s.pkTo[id] = wi
				s.mu.Unlock()
			}
		}(wi, ch)
	}
	wg.Wait()

	// Exactly-once: the union of all worker channels equals the input
	// EXACTLY (count, including duplicates — no drop, no dup).
	if s.total != len(input) {
		t.Fatalf("routed %d rows; want exactly %d (no drop, no dup)", s.total, len(input))
	}
	// Every distinct PK appeared (none dropped).
	for id := int64(0); id < 200; id++ {
		if _, ok := s.pkTo[id]; !ok {
			t.Fatalf("PK %d never routed to any worker", id)
		}
	}
}

func TestPkWorkerIndex_StableForSamePK(t *testing.T) {
	table := pkTable()
	pkCols := migcore.TablePKColumns(table)
	const degree = 7
	// The same PK value must always map to the same worker (re-emission
	// safety), regardless of the non-PK columns.
	r1 := ir.Row{"id": int64(42), "v": "first"}
	r2 := ir.Row{"id": int64(42), "v": "second-reemit"}
	if pkWorkerIndex(r1, pkCols, degree) != pkWorkerIndex(r2, pkCols, degree) {
		t.Fatal("same PK hashed to different workers across re-emission")
	}
	// Index always in range.
	for id := 0; id < 1000; id++ {
		idx := pkWorkerIndex(ir.Row{"id": int64(id)}, pkCols, degree)
		if idx < 0 || idx >= degree {
			t.Fatalf("pkWorkerIndex out of range: %d (degree %d)", idx, degree)
		}
	}
}

func TestPkWorkerIndex_CompositePKNoCollisionFromConcat(t *testing.T) {
	table := &ir.Table{
		Name:    "t",
		Columns: []*ir.Column{{Name: "a", Type: ir.Text{}}, {Name: "b", Type: ir.Text{}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{
			{Column: "a"}, {Column: "b"},
		}},
	}
	pkCols := migcore.TablePKColumns(table)
	// ("a","bc") vs ("ab","c") must not be forced to the same hash by
	// naive concatenation — the NUL separator prevents that. They CAN
	// coincidentally collide mod degree, so use degree large enough that
	// the raw 64-bit hashes are what we compare.
	h1 := pkWorkerIndex(ir.Row{"a": "a", "b": "bc"}, pkCols, 1<<31)
	h2 := pkWorkerIndex(ir.Row{"a": "ab", "b": "c"}, pkCols, 1<<31)
	if h1 == h2 {
		t.Fatal("composite-PK concatenation ambiguity: ('a','bc') and ('ab','c') hashed identically")
	}
}

// --- dispatcher lifecycle: ctx-cancel leaks no goroutine ---

func TestPartitionRowsByPK_CancelNoLeak(t *testing.T) {
	table := pkTable()
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	// A source that blocks forever (never sends, never closes).
	src := make(chan ir.Row)
	workers := partitionRowsByPK(ctx, src, table, 3)

	cancel()

	// All worker channels must close after cancel (the dispatcher's defer).
	var wg sync.WaitGroup
	for _, ch := range workers {
		wg.Add(1)
		go func(ch <-chan ir.Row) {
			defer wg.Done()
			for range ch { //nolint:revive // drain to close
			}
		}(ch)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker channels did not close after ctx cancel — dispatcher leak")
	}

	// Give the dispatcher goroutine a moment to unwind, then check the
	// count returned to baseline (best-effort; allow a small slack).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before {
			return
		}
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > before+1 {
		t.Fatalf("goroutine leak after cancel: before=%d after=%d", before, got)
	}
}

// --- dispatch eligibility: serial fallback when not parallel-capable ---

// fanoutFakeReader streams a fixed set of rows once.
type fanoutFakeReader struct {
	rows []ir.Row
	err  error
}

func (r *fanoutFakeReader) ReadRows(ctx context.Context, _ *ir.Table) (<-chan ir.Row, error) {
	out := make(chan ir.Row)
	go func() {
		defer close(out)
		for _, row := range r.rows {
			select {
			case <-ctx.Done():
				return
			case out <- row:
			}
		}
	}()
	return out, nil
}
func (r *fanoutFakeReader) Err() error { return r.err }

// serialOnlyWriter implements ir.IdempotentRowWriter but NOT
// ParallelIdempotentCopyWriter — so the dispatch must fall through to
// serial.
type serialOnlyWriter struct {
	mu        sync.Mutex
	rowsSeen  int
	serialHit int32
}

func (w *serialOnlyWriter) WriteRows(ctx context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	return w.drain(ctx, rows)
}

func (w *serialOnlyWriter) WriteRowsIdempotent(ctx context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	atomic.AddInt32(&w.serialHit, 1)
	return w.drain(ctx, rows)
}

// HandlesNoPKIdempotentCopy makes the fake a full ir.IdempotentCopyWriter
// so the serial no-PK cold-start path drains rather than loud-refusing —
// lets the no-PK-routes-serial test observe the serial drain.
func (w *serialOnlyWriter) HandlesNoPKIdempotentCopy() bool { return true }

func (w *serialOnlyWriter) drain(ctx context.Context, rows <-chan ir.Row) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-rows:
			if !ok {
				return nil
			}
			w.mu.Lock()
			w.rowsSeen++
			w.mu.Unlock()
		}
	}
}

func TestMaybeParallel_FallsBackToSerialWhenNotCapable(t *testing.T) {
	table := pkTable()
	rr := &fanoutFakeReader{rows: []ir.Row{
		{"id": int64(1), "v": "a"}, {"id": int64(2), "v": "b"}, {"id": int64(3), "v": "c"},
	}}
	w := &serialOnlyWriter{}
	if err := copyTableColdStartIdempotentMaybeParallel(context.Background(), rr, w, table, nil, ShardColumnSpec{}, 4); err != nil {
		t.Fatalf("maybe-parallel (serial fallback): %v", err)
	}
	if atomic.LoadInt32(&w.serialHit) != 1 {
		t.Fatalf("expected the serial WriteRowsIdempotent path; serialHit=%d", w.serialHit)
	}
	if w.rowsSeen != 3 {
		t.Fatalf("serial path saw %d rows; want 3", w.rowsSeen)
	}
}

// parallelFakeWriter implements ParallelIdempotentCopyWriter, recording
// per-worker rows + asserting the flush-before-return contract.
type parallelFakeWriter struct {
	serialOnlyWriter
	mu          sync.Mutex
	perWorker   []int
	parallelHit int32
	failWorker  int // worker index to fail; -1 = none
	returned    atomic.Bool
}

func (w *parallelFakeWriter) WriteRowsIdempotentParallel(ctx context.Context, _ *ir.Table, workers []<-chan ir.Row) error {
	atomic.AddInt32(&w.parallelHit, 1)
	w.mu.Lock()
	w.perWorker = make([]int, len(workers))
	w.mu.Unlock()

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var firstErr error
	var once sync.Once
	for i, ch := range workers {
		wg.Add(1)
		go func(i int, ch <-chan ir.Row) {
			defer wg.Done()
			for {
				select {
				case <-wctx.Done():
					return
				case row, ok := <-ch:
					if !ok {
						return
					}
					_ = row
					if w.failWorker == i {
						once.Do(func() { firstErr = fmt.Errorf("injected worker %d failure", i) })
						cancel()
						return
					}
					w.mu.Lock()
					w.perWorker[i]++
					w.mu.Unlock()
				}
			}
		}(i, ch)
	}
	wg.Wait() // flush-before-return: join every worker before returning
	w.returned.Store(true)
	return firstErr
}

func TestMaybeParallel_FansOutWhenCapable(t *testing.T) {
	table := pkTable()
	var rows []ir.Row
	for id := 0; id < 100; id++ {
		rows = append(rows, ir.Row{"id": int64(id), "v": "x"})
	}
	rr := &fanoutFakeReader{rows: rows}
	w := &parallelFakeWriter{failWorker: -1}

	if err := copyTableColdStartIdempotentMaybeParallel(context.Background(), rr, w, table, nil, ShardColumnSpec{}, 4); err != nil {
		t.Fatalf("maybe-parallel (fan-out): %v", err)
	}
	if atomic.LoadInt32(&w.parallelHit) != 1 {
		t.Fatalf("expected the parallel path; parallelHit=%d", w.parallelHit)
	}
	if !w.returned.Load() {
		t.Fatal("parallel writer did not record return after worker join")
	}
	w.mu.Lock()
	total := 0
	for _, n := range w.perWorker {
		total += n
	}
	w.mu.Unlock()
	if total != 100 {
		t.Fatalf("fan-out workers saw %d rows total; want 100 (exactly-once)", total)
	}
}

func TestMaybeParallel_FallsBackToSerialForNoPKTable(t *testing.T) {
	// A no-PK table has no partition key → must route serial even though
	// the writer is parallel-capable.
	table := &ir.Table{
		Name:    "logs",
		Columns: []*ir.Column{{Name: "msg", Type: ir.Text{}}},
	}
	rr := &fanoutFakeReader{rows: []ir.Row{{"msg": "a"}, {"msg": "b"}}}
	w := &parallelFakeWriter{failWorker: -1}
	if err := copyTableColdStartIdempotentMaybeParallel(context.Background(), rr, w, table, nil, ShardColumnSpec{}, 4); err != nil {
		t.Fatalf("maybe-parallel (no-PK serial): %v", err)
	}
	if atomic.LoadInt32(&w.parallelHit) != 0 {
		t.Fatal("no-PK table must NOT take the parallel path")
	}
	if atomic.LoadInt32(&w.serialHit) != 1 {
		t.Fatalf("no-PK table should take serial WriteRowsIdempotent; serialHit=%d", w.serialHit)
	}
}

func TestMaybeParallel_SerialWhenDegreeOne(t *testing.T) {
	table := pkTable()
	rr := &fanoutFakeReader{rows: []ir.Row{{"id": int64(1), "v": "a"}}}
	w := &parallelFakeWriter{failWorker: -1}
	if err := copyTableColdStartIdempotentMaybeParallel(context.Background(), rr, w, table, nil, ShardColumnSpec{}, 1); err != nil {
		t.Fatalf("maybe-parallel (degree 1): %v", err)
	}
	if atomic.LoadInt32(&w.parallelHit) != 0 {
		t.Fatal("degree 1 must NOT fan out")
	}
	if atomic.LoadInt32(&w.serialHit) != 1 {
		t.Fatalf("degree 1 should take serial path; serialHit=%d", w.serialHit)
	}
}

// --- loud abort: a worker error fails the copy ---

func TestParallelCopy_WorkerErrorFailsLoudly(t *testing.T) {
	table := pkTable()
	var rows []ir.Row
	for id := 0; id < 500; id++ {
		rows = append(rows, ir.Row{"id": int64(id), "v": "x"})
	}
	rr := &fanoutFakeReader{rows: rows}
	w := &parallelFakeWriter{failWorker: 1} // worker 1 errors

	err := copyTableColdStartIdempotentParallel(context.Background(), rr, w, table, nil, ShardColumnSpec{}, 4)
	if err == nil {
		t.Fatal("expected a loud error when a worker fails; got nil")
	}
	if !contains(err.Error(), "injected worker 1 failure") {
		t.Fatalf("error did not surface the worker failure: %v", err)
	}
}

// --- reader stream error surfaces (Bug 68 gate preserved) ---

func TestParallelCopy_ReaderStreamErrSurfaces(t *testing.T) {
	table := pkTable()
	rr := &fanoutFakeReader{
		rows: []ir.Row{{"id": int64(1), "v": "a"}},
		err:  errors.New("mysql: scan: boom"),
	}
	w := &parallelFakeWriter{failWorker: -1}
	err := copyTableColdStartIdempotentParallel(context.Background(), rr, w, table, nil, ShardColumnSpec{}, 4)
	if err == nil {
		t.Fatal("expected the Bug-68 loud-failure gate to surface the reader stream error")
	}
	if !contains(err.Error(), "boom") {
		t.Fatalf("error did not surface the reader stream error: %v", err)
	}
}
