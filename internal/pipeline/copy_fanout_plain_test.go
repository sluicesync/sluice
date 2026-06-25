// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0102: plain-INSERT WRITE-side fan-out for the native-MySQL concurrent
// cold-copy. These pins mirror the ADR-0097 copy_fanout_test.go idempotent
// pins, on the plain (gap-free / fresh-target) path. The PK-hash routing
// (partitionRowsByPK / pkWorkerIndex) is shared verbatim and already pinned
// there; here we pin the plain dispatch helper + the plain N-worker writer
// contract.

// plainParallelFakeWriter implements ir.ParallelCopyWriter (plain, NOT
// idempotent), recording per-worker rows + asserting the flush-before-return
// contract. It deliberately does NOT implement IdempotentRowWriter, so it can
// only be selected by the plain path.
type plainParallelFakeWriter struct {
	mu          sync.Mutex
	rowsSeen    int
	perWorker   []int
	serialHit   int32
	parallelHit int32
	failWorker  int // worker index to fail; -1 = none
	returned    atomic.Bool
}

func (w *plainParallelFakeWriter) WriteRows(ctx context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	atomic.AddInt32(&w.serialHit, 1)
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

func (w *plainParallelFakeWriter) WriteRowsParallel(ctx context.Context, _ *ir.Table, workers []<-chan ir.Row) error {
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
						once.Do(func() { firstErr = fmt.Errorf("injected plain worker %d failure", i) })
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

// --- dispatch eligibility ---

func TestPlainMaybeParallel_FansOutWhenCapable(t *testing.T) {
	table := pkTable()
	var rows []ir.Row
	for id := 0; id < 100; id++ {
		rows = append(rows, ir.Row{"id": int64(id), "v": "x"})
	}
	rr := &fanoutFakeReader{rows: rows}
	w := &plainParallelFakeWriter{failWorker: -1}

	if err := copyTablePlainMaybeParallel(context.Background(), rr, w, table, nil, ShardColumnSpec{}, 4); err != nil {
		t.Fatalf("plain maybe-parallel (fan-out): %v", err)
	}
	if atomic.LoadInt32(&w.parallelHit) != 1 {
		t.Fatalf("expected the plain parallel path; parallelHit=%d", w.parallelHit)
	}
	if !w.returned.Load() {
		t.Fatal("plain parallel writer did not record return after worker join")
	}
	w.mu.Lock()
	total := 0
	for _, n := range w.perWorker {
		total += n
	}
	w.mu.Unlock()
	if total != 100 {
		t.Fatalf("plain fan-out workers saw %d rows total; want 100 (exactly-once)", total)
	}
}

func TestPlainMaybeParallel_SerialWhenNotCapable(t *testing.T) {
	// A writer that is NOT a ParallelCopyWriter must route serial (plain
	// WriteRows), never fan out.
	table := pkTable()
	rr := &fanoutFakeReader{rows: []ir.Row{
		{"id": int64(1), "v": "a"}, {"id": int64(2), "v": "b"}, {"id": int64(3), "v": "c"},
	}}
	w := &plainOnlyWriter{}
	if err := copyTablePlainMaybeParallel(context.Background(), rr, w, table, nil, ShardColumnSpec{}, 4); err != nil {
		t.Fatalf("plain maybe-parallel (serial fallback): %v", err)
	}
	if atomic.LoadInt32(&w.serialHit) != 1 {
		t.Fatalf("expected the serial WriteRows path; serialHit=%d", w.serialHit)
	}
	if w.rowsSeen != 3 {
		t.Fatalf("serial path saw %d rows; want 3", w.rowsSeen)
	}
}

func TestPlainMaybeParallel_SerialForNoPKTable(t *testing.T) {
	// A no-PK table has no partition key → must route serial even though the
	// writer is parallel-capable. (Plain path: fully copied, NOT refused —
	// the gap-free snapshot has no re-emission to duplicate.)
	table := &ir.Table{
		Name:    "logs",
		Columns: []*ir.Column{{Name: "msg", Type: ir.Text{}}},
	}
	rr := &fanoutFakeReader{rows: []ir.Row{{"msg": "a"}, {"msg": "b"}}}
	w := &plainParallelFakeWriter{failWorker: -1}
	if err := copyTablePlainMaybeParallel(context.Background(), rr, w, table, nil, ShardColumnSpec{}, 4); err != nil {
		t.Fatalf("plain maybe-parallel (no-PK serial): %v", err)
	}
	if atomic.LoadInt32(&w.parallelHit) != 0 {
		t.Fatal("no-PK table must NOT take the plain parallel path")
	}
	if atomic.LoadInt32(&w.serialHit) != 1 {
		t.Fatalf("no-PK table should take serial WriteRows; serialHit=%d", w.serialHit)
	}
	if w.rowsSeen != 2 {
		t.Fatalf("no-PK serial path saw %d rows; want 2 (fully copied, not refused)", w.rowsSeen)
	}
}

func TestPlainMaybeParallel_SerialWhenDegreeOne(t *testing.T) {
	// D = 1 must be byte-identical to the single-writer path (ADR-0102 §4).
	table := pkTable()
	rr := &fanoutFakeReader{rows: []ir.Row{{"id": int64(1), "v": "a"}}}
	w := &plainParallelFakeWriter{failWorker: -1}
	if err := copyTablePlainMaybeParallel(context.Background(), rr, w, table, nil, ShardColumnSpec{}, 1); err != nil {
		t.Fatalf("plain maybe-parallel (degree 1): %v", err)
	}
	if atomic.LoadInt32(&w.parallelHit) != 0 {
		t.Fatal("degree 1 must NOT fan out")
	}
	if atomic.LoadInt32(&w.serialHit) != 1 {
		t.Fatalf("degree 1 should take serial WriteRows; serialHit=%d", w.serialHit)
	}
}

// --- loud abort: a worker error fails the copy ---

func TestPlainParallelCopy_WorkerErrorFailsLoudly(t *testing.T) {
	table := pkTable()
	var rows []ir.Row
	for id := 0; id < 500; id++ {
		rows = append(rows, ir.Row{"id": int64(id), "v": "x"})
	}
	rr := &fanoutFakeReader{rows: rows}
	w := &plainParallelFakeWriter{failWorker: 1} // worker 1 errors

	err := copyTablePlainParallel(context.Background(), rr, w, table, nil, ShardColumnSpec{}, 4)
	if err == nil {
		t.Fatal("expected a loud error when a plain worker fails; got nil")
	}
	if !contains(err.Error(), "injected plain worker 1 failure") {
		t.Fatalf("error did not surface the worker failure: %v", err)
	}
}

// --- reader stream error surfaces (Bug 68 gate preserved) ---

func TestPlainParallelCopy_ReaderStreamErrSurfaces(t *testing.T) {
	table := pkTable()
	rr := &fanoutFakeReader{
		rows: []ir.Row{{"id": int64(1), "v": "a"}},
		err:  errors.New("mysql: scan: boom"),
	}
	w := &plainParallelFakeWriter{failWorker: -1}
	err := copyTablePlainParallel(context.Background(), rr, w, table, nil, ShardColumnSpec{}, 4)
	if err == nil {
		t.Fatal("expected the Bug-68 loud-failure gate to surface the reader stream error")
	}
	if !contains(err.Error(), "boom") {
		t.Fatalf("error did not surface the reader stream error: %v", err)
	}
}

// --- W × D composition through runConcurrentTableCopy ---

// wxdRecordingWriter implements ir.ParallelCopyWriter and records, per table,
// the rows seen across ALL D workers + the peak number of concurrent workers
// it ran for that table (proving the per-table fan-out). It is the plain
// (NOT idempotent) writer the native concurrent path selects.
type wxdRecordingWriter struct {
	mu          sync.Mutex
	counts      map[string]int // table → rows written (summed across D workers)
	maxWorkers  map[string]int // table → peak concurrent workers
	parallelHit int32
}

func newWXDWriter() *wxdRecordingWriter {
	return &wxdRecordingWriter{counts: map[string]int{}, maxWorkers: map[string]int{}}
}

func (w *wxdRecordingWriter) WriteRows(ctx context.Context, t *ir.Table, rows <-chan ir.Row) error {
	// Single-writer fallback (degree 1 / no-PK). Count as one worker.
	return w.runWorkers(ctx, t, []<-chan ir.Row{rows})
}

func (w *wxdRecordingWriter) WriteRowsParallel(ctx context.Context, t *ir.Table, workers []<-chan ir.Row) error {
	atomic.AddInt32(&w.parallelHit, 1)
	return w.runWorkers(ctx, t, workers)
}

func (w *wxdRecordingWriter) runWorkers(ctx context.Context, t *ir.Table, workers []<-chan ir.Row) error {
	w.mu.Lock()
	if got := len(workers); got > w.maxWorkers[t.Name] {
		w.maxWorkers[t.Name] = got
	}
	w.mu.Unlock()

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var firstErr error
	var once sync.Once
	for _, ch := range workers {
		wg.Add(1)
		go func(ch <-chan ir.Row) {
			defer wg.Done()
			n := 0
			for {
				select {
				case <-wctx.Done():
					once.Do(func() { firstErr = wctx.Err() })
					return
				case _, ok := <-ch:
					if !ok {
						w.mu.Lock()
						w.counts[t.Name] += n
						w.mu.Unlock()
						return
					}
					n++
				}
			}
		}(ch)
	}
	wg.Wait()
	return firstErr
}

// TestRunConcurrentTableCopy_NativeWxD pins the ADR-0102 composition: a native
// (non-idempotent) concurrent reader with W groups AND a degree-D fan-out →
// the plain WriteRowsParallel path runs, each table is written exactly once
// (summed across D workers), and the per-table peak worker count is D (proving
// W × D, not W × 1).
func TestRunConcurrentTableCopy_NativeWxD(t *testing.T) {
	const degree = 4
	groups := [][]string{{"a", "c"}, {"b", "d"}}
	schema := concSchema("a", "b", "c", "d")
	rowsPer := map[string]int{"a": 100, "b": 137, "c": 211, "d": 89}
	reader := newNativeConcReader(groups, rowsPer)
	writer := newWXDWriter()

	// needsIdempotent=false → the plain path; degree=4 → per-table fan-out.
	if err := runConcurrentTableCopy(context.Background(), groups, schema, reader, writer, nil, ShardColumnSpec{}, degree, false, false); err != nil {
		t.Fatalf("runConcurrentTableCopy (native W×D): %v", err)
	}
	if atomic.LoadInt32(&writer.parallelHit) == 0 {
		t.Fatal("expected the plain WriteRowsParallel path (W × D); it was never hit")
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	for tbl, want := range rowsPer {
		if got := writer.counts[tbl]; got != want {
			t.Errorf("table %q rows written = %d; want %d (exactly-once across D workers)", tbl, got, want)
		}
		if got := writer.maxWorkers[tbl]; got != degree {
			t.Errorf("table %q peak workers = %d; want %d (per-table D-way fan-out)", tbl, got, degree)
		}
	}
}

// TestRunConcurrentTableCopy_NativeDegreeOneByteIdentical pins ADR-0102 §4:
// D = 1 takes the single-writer path (no fan-out), byte-identical to ADR-0101
// W × 1 — even with a parallel-capable writer.
func TestRunConcurrentTableCopy_NativeDegreeOneByteIdentical(t *testing.T) {
	groups := [][]string{{"a"}, {"b"}}
	schema := concSchema("a", "b")
	rowsPer := map[string]int{"a": 5, "b": 9}
	reader := newNativeConcReader(groups, rowsPer)
	writer := newWXDWriter()

	if err := runConcurrentTableCopy(context.Background(), groups, schema, reader, writer, nil, ShardColumnSpec{}, 1, false, false); err != nil {
		t.Fatalf("runConcurrentTableCopy (native D=1): %v", err)
	}
	if atomic.LoadInt32(&writer.parallelHit) != 0 {
		t.Fatal("D = 1 must NOT take the fan-out path (byte-identical to W × 1)")
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	for tbl, want := range rowsPer {
		if got := writer.counts[tbl]; got != want {
			t.Errorf("table %q rows = %d; want %d", tbl, got, want)
		}
		if got := writer.maxWorkers[tbl]; got != 1 {
			t.Errorf("table %q peak workers = %d; want 1 (single-writer)", tbl, got)
		}
	}
}

// plainOnlyWriter implements ir.RowWriter but NOT ir.ParallelCopyWriter — so
// the plain dispatch must fall through to serial WriteRows.
type plainOnlyWriter struct {
	mu        sync.Mutex
	rowsSeen  int
	serialHit int32
}

func (w *plainOnlyWriter) WriteRows(ctx context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	atomic.AddInt32(&w.serialHit, 1)
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
