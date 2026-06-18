// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// --- ADR-0100 cross-table WRITE concurrency: unit pins ---
//
// These pin the pipeline-side write-concurrency driver (runConcurrentTableCopy)
// + the dispatch gate (concurrentCopyGroups), without a real DB. The
// load-bearing pins are: (1) multiple tables' write-windows OVERLAP under W>1
// (the missing proof — the serial loop wrote one at a time); (2) every table
// written exactly once; (3) any consumer error aborts the whole copy; (4)
// ctx-cancel leaks nothing; (5) the zero-value / no-partition path stays
// serial byte-identically.

// concTable builds a PK'd table.
func concTable(name string) *ir.Table {
	return &ir.Table{
		Name: name,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

func concSchema(tables ...string) *ir.Schema {
	s := &ir.Schema{}
	for _, t := range tables {
		s.Tables = append(s.Tables, concTable(t))
	}
	return s
}

// concPartReader is a snapshot reader fake that (a) declares the idempotent
// copy contract, (b) surfaces a concurrent-copy partition, and (c) serves a
// per-table row stream. ReadRows blocks until released for that table (via
// the per-table gate) so a test can control inter-table timing and prove
// write-window overlap.
type concPartReader struct {
	groups [][]string

	mu      sync.Mutex
	rows    map[string][]ir.Row      // per-table rows to serve
	gate    map[string]chan struct{} // per-table release gate (nil ⇒ no gate)
	readErr error                    // sticky reader stream error (Bug 68)
}

func newConcPartReader(groups [][]string, rowsPerTable map[string]int) *concPartReader {
	r := &concPartReader{groups: groups, rows: map[string][]ir.Row{}, gate: map[string]chan struct{}{}}
	// Seed rows from the rowsPerTable map directly (not the groups) so the
	// reader serves rows on BOTH the concurrent path (groups non-nil) and the
	// serial path (groups nil but tables still need rows).
	for tbl, n := range rowsPerTable {
		rows := make([]ir.Row, 0, n)
		for i := 0; i < n; i++ {
			rows = append(rows, ir.Row{"id": int64(i), "v": fmt.Sprintf("%s-%d", tbl, i)})
		}
		r.rows[tbl] = rows
	}
	return r
}

func (r *concPartReader) ConcurrentCopyGroups() [][]string { return r.groups }
func (r *concPartReader) CopyNeedsIdempotentWriter() bool  { return true }
func (r *concPartReader) Err() error                       { return r.readErr }

func (r *concPartReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	r.mu.Lock()
	rows := r.rows[table.Name]
	gate := r.gate[table.Name]
	r.mu.Unlock()

	out := make(chan ir.Row)
	go func() {
		defer close(out)
		if gate != nil {
			select {
			case <-gate:
			case <-ctx.Done():
				return
			}
		}
		for _, row := range rows {
			select {
			case <-ctx.Done():
				return
			case out <- row:
			}
		}
	}()
	return out, nil
}

// recordingIdemWriter is an IdempotentRowWriter that records, per table, the
// open/close timestamps of its write window + the rows it saw — so a test can
// assert window OVERLAP (concurrency) and per-table write-count (exactly-once).
type recordingIdemWriter struct {
	mu      sync.Mutex
	windows map[string][2]time.Time // table → [open, close]
	counts  map[string]int          // table → rows written
	failOn  string                  // table name to fail on (loud-abort pin)

	active     int // currently-open write windows
	maxActive  int // peak concurrent windows
	blockUntil chan struct{}
}

func newRecordingWriter() *recordingIdemWriter {
	return &recordingIdemWriter{windows: map[string][2]time.Time{}, counts: map[string]int{}}
}

func (w *recordingIdemWriter) WriteRows(ctx context.Context, t *ir.Table, rows <-chan ir.Row) error {
	return w.WriteRowsIdempotent(ctx, t, rows)
}

func (w *recordingIdemWriter) WriteRowsIdempotent(ctx context.Context, t *ir.Table, rows <-chan ir.Row) error {
	w.mu.Lock()
	open := time.Now()
	win := w.windows[t.Name]
	win[0] = open
	w.windows[t.Name] = win
	w.active++
	if w.active > w.maxActive {
		w.maxActive = w.active
	}
	block := w.blockUntil
	w.mu.Unlock()

	if w.failOn == t.Name {
		// Drain a bit then fail loudly.
		w.mu.Lock()
		w.active--
		w.mu.Unlock()
		return fmt.Errorf("forced write error on table %q", t.Name)
	}

	// If a barrier is set, hold the window open until released — this is how
	// the overlap pin forces both tables' windows to coexist.
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			w.mu.Lock()
			w.active--
			w.mu.Unlock()
			return ctx.Err()
		}
	}

	n := 0
	for {
		select {
		case row, ok := <-rows:
			if !ok {
				w.mu.Lock()
				w.counts[t.Name] += n
				win := w.windows[t.Name]
				win[1] = time.Now()
				w.windows[t.Name] = win
				w.active--
				w.mu.Unlock()
				return nil
			}
			_ = row
			n++
		case <-ctx.Done():
			w.mu.Lock()
			w.active--
			w.mu.Unlock()
			return ctx.Err()
		}
	}
}

func (w *recordingIdemWriter) peakConcurrent() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.maxActive
}

// TestConcurrentCopyGroups_DispatchGate pins the eligibility gate: a reader
// with ≥2 groups → those groups; nil / 1-group / non-implementing reader →
// nil (serial). The zero-value (no partition) path is the serial loop.
func TestConcurrentCopyGroups_DispatchGate(t *testing.T) {
	t.Run("two groups engages concurrency", func(t *testing.T) {
		r := newConcPartReader([][]string{{"a"}, {"b"}}, map[string]int{"a": 1, "b": 1})
		if got := concurrentCopyGroups(r); len(got) != 2 {
			t.Fatalf("concurrentCopyGroups = %v; want 2 groups", got)
		}
	})
	t.Run("single group is serial", func(t *testing.T) {
		r := newConcPartReader([][]string{{"a", "b"}}, map[string]int{"a": 1, "b": 1})
		if got := concurrentCopyGroups(r); got != nil {
			t.Fatalf("concurrentCopyGroups = %v; want nil (1 group ⇒ serial)", got)
		}
	})
	t.Run("nil groups is serial", func(t *testing.T) {
		r := newConcPartReader(nil, nil)
		if got := concurrentCopyGroups(r); got != nil {
			t.Fatalf("concurrentCopyGroups = %v; want nil", got)
		}
	})
	t.Run("non-implementing reader is serial", func(t *testing.T) {
		r := &fanoutFakeReader{}
		if got := concurrentCopyGroups(r); got != nil {
			t.Fatalf("concurrentCopyGroups = %v; want nil (no surface ⇒ serial)", got)
		}
	})
}

// TestRunConcurrentTableCopy_WindowsOverlap is THE load-bearing write-
// concurrency pin (ADR-0100): with W=2 over 4 tables, ≥2 tables' write
// windows must be open at the same instant. This is exactly what the target
// PROCESSLIST showed MISSING (one table at a time). It FAILS on a serial
// consumer (peak concurrent == 1) and PASSES on the W-pipeline path.
func TestRunConcurrentTableCopy_WindowsOverlap(t *testing.T) {
	groups := [][]string{{"a", "c"}, {"b", "d"}}
	schema := concSchema("a", "b", "c", "d")
	rowsPer := map[string]int{"a": 10, "b": 10, "c": 10, "d": 10}
	reader := newConcPartReader(groups, rowsPer)
	writer := newRecordingWriter()

	// Hold every write window open until we release, so concurrent windows
	// must coexist before any closes. With W=2 the two leading tables (one
	// per group) open together → peak ≥ 2.
	writer.blockUntil = make(chan struct{})

	ctx := context.Background()
	done := make(chan error, 1)
	go func() {
		// degree=1: isolate cross-TABLE concurrency from per-table fan-out.
		done <- runConcurrentTableCopy(ctx, groups, schema, reader, writer, nil, ShardColumnSpec{}, 1, true)
	}()

	// Wait until two windows are concurrently open (one per group).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && writer.peakConcurrent() < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	if writer.peakConcurrent() < 2 {
		close(writer.blockUntil)
		<-done
		t.Fatalf("peak concurrent write windows = %d; want ≥ 2 "+
			"(serial-consumer regression: only one table written at a time)", writer.peakConcurrent())
	}
	close(writer.blockUntil)
	if err := <-done; err != nil {
		t.Fatalf("runConcurrentTableCopy: %v", err)
	}
}

// TestRunConcurrentTableCopy_ExactlyOnce pins that every table is written
// EXACTLY once with the full row count — no table dropped (silent loss) or
// double-written.
func TestRunConcurrentTableCopy_ExactlyOnce(t *testing.T) {
	groups := [][]string{{"a", "c"}, {"b", "d"}}
	schema := concSchema("a", "b", "c", "d")
	rowsPer := map[string]int{"a": 7, "b": 11, "c": 13, "d": 17}
	reader := newConcPartReader(groups, rowsPer)
	writer := newRecordingWriter()

	if err := runConcurrentTableCopy(context.Background(), groups, schema, reader, writer, nil, ShardColumnSpec{}, 1, true); err != nil {
		t.Fatalf("runConcurrentTableCopy: %v", err)
	}

	writer.mu.Lock()
	defer writer.mu.Unlock()
	if len(writer.counts) != 4 {
		t.Fatalf("tables written = %d; want 4 (a dropped table is silent loss): %v", len(writer.counts), writer.counts)
	}
	for tbl, want := range rowsPer {
		if got := writer.counts[tbl]; got != want {
			t.Errorf("table %q rows written = %d; want %d", tbl, got, want)
		}
	}
}

// TestRunConcurrentTableCopy_ErrorAbortsLoudly pins that any consumer's
// error fails the whole copy (errgroup), so the streamer advances no
// position.
func TestRunConcurrentTableCopy_ErrorAbortsLoudly(t *testing.T) {
	groups := [][]string{{"a"}, {"b"}}
	schema := concSchema("a", "b")
	reader := newConcPartReader(groups, map[string]int{"a": 5, "b": 5})
	writer := newRecordingWriter()
	writer.failOn = "b"

	err := runConcurrentTableCopy(context.Background(), groups, schema, reader, writer, nil, ShardColumnSpec{}, 1, true)
	if err == nil {
		t.Fatal("expected error from failing consumer; got nil (silent partial success)")
	}
}

// TestRunConcurrentTableCopy_MissingTableLoud pins that a group naming a
// table not in the schema is a LOUD failure (a partition/scope mismatch),
// never a silently un-copied table.
func TestRunConcurrentTableCopy_MissingTableLoud(t *testing.T) {
	groups := [][]string{{"a"}, {"ghost"}}
	schema := concSchema("a", "b") // "ghost" is not present
	reader := newConcPartReader(groups, map[string]int{"a": 1, "ghost": 1})
	writer := newRecordingWriter()

	err := runConcurrentTableCopy(context.Background(), groups, schema, reader, writer, nil, ShardColumnSpec{}, 1, true)
	if err == nil {
		t.Fatal("expected loud error for a group table missing from the schema; got nil")
	}
}

// TestRunConcurrentTableCopy_CancelNoLeak pins that ctx-cancel mid-copy
// unwinds every consumer goroutine and reports the cancel (not success).
func TestRunConcurrentTableCopy_CancelNoLeak(t *testing.T) {
	groups := [][]string{{"a"}, {"b"}}
	schema := concSchema("a", "b")
	reader := newConcPartReader(groups, map[string]int{"a": 1000, "b": 1000})
	// Gate both tables forever so the consumers block in ReadRows.
	reader.gate["a"] = make(chan struct{})
	reader.gate["b"] = make(chan struct{})
	writer := newRecordingWriter()

	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runConcurrentTableCopy(ctx, groups, schema, reader, writer, nil, ShardColumnSpec{}, 1, true)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancel mid-copy reported success; want a non-nil (cancel) error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runConcurrentTableCopy did not return after cancel — consumer leak")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > before+4 {
		t.Fatalf("goroutine leak after cancel: before=%d after=%d", before, got)
	}
}

// nativeConcReader is the ADR-0101 native-MySQL concurrent reader fake: it
// surfaces a concurrent-copy partition (ir.ConcurrentCopyPartitioner) but
// does NOT declare the idempotent contract (no CopyNeedsIdempotentWriter) —
// the binlog snapshot is gap-free + overlap-free, so the cold-copy plain-
// INSERTs each table exactly once. It embeds concPartReader for ReadRows /
// ConcurrentCopyGroups but deliberately re-declares Err and omits the
// idempotent declaration by NOT being assertable as ir.IdempotentCopyReader.
type nativeConcReader struct {
	groups [][]string
	rows   map[string][]ir.Row
}

func newNativeConcReader(groups [][]string, rowsPerTable map[string]int) *nativeConcReader {
	r := &nativeConcReader{groups: groups, rows: map[string][]ir.Row{}}
	for tbl, n := range rowsPerTable {
		rows := make([]ir.Row, 0, n)
		for i := 0; i < n; i++ {
			rows = append(rows, ir.Row{"id": int64(i), "v": fmt.Sprintf("%s-%d", tbl, i)})
		}
		r.rows[tbl] = rows
	}
	return r
}

func (r *nativeConcReader) ConcurrentCopyGroups() [][]string { return r.groups }
func (r *nativeConcReader) Err() error                       { return nil }

func (r *nativeConcReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	rows := r.rows[table.Name]
	out := make(chan ir.Row)
	go func() {
		defer close(out)
		for _, row := range rows {
			select {
			case <-ctx.Done():
				return
			case out <- row:
			}
		}
	}()
	return out, nil
}

// TestRunConcurrentTableCopy_NativePlainInsert pins the ADR-0101 native path:
// a NON-idempotent reader surfacing ≥2 groups drives the concurrent PLAIN-
// INSERT path (needsIdempotent=false → copyTable), every table written
// exactly once. This is the gap-free native-MySQL snapshot — concurrently
// plain-INSERTing it is safe because the disjoint partition means each table
// is written by exactly one pipeline.
func TestRunConcurrentTableCopy_NativePlainInsert(t *testing.T) {
	groups := [][]string{{"a", "c"}, {"b", "d"}}
	schema := concSchema("a", "b", "c", "d")
	rowsPer := map[string]int{"a": 7, "b": 11, "c": 13, "d": 17}
	reader := newNativeConcReader(groups, rowsPer)
	writer := newRecordingWriter()

	// needsIdempotent=false → the plain copyTable path.
	if err := runConcurrentTableCopy(context.Background(), groups, schema, reader, writer, nil, ShardColumnSpec{}, 1, false); err != nil {
		t.Fatalf("runConcurrentTableCopy (native plain): %v", err)
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if len(writer.counts) != 4 {
		t.Fatalf("tables written = %d; want 4: %v", len(writer.counts), writer.counts)
	}
	for tbl, want := range rowsPer {
		if got := writer.counts[tbl]; got != want {
			t.Errorf("table %q rows written = %d; want %d", tbl, got, want)
		}
	}
}

// TestRunBulkCopyWithOpts_NativeConcurrentNotRefused pins that a NON-
// idempotent reader surfacing ≥2 groups is NOT refused (the ADR-0101 guard
// widening) — it takes the concurrent path. Before ADR-0101 this combination
// was a loud refusal ("not an idempotent copy reader; refusing to
// concurrently plain-INSERT").
func TestRunBulkCopyWithOpts_NativeConcurrentNotRefused(t *testing.T) {
	prev := concurrentCopyDispatchObserver
	defer func() { concurrentCopyDispatchObserver = prev }()
	var got int
	var mu sync.Mutex
	concurrentCopyDispatchObserver = func(groups int) {
		mu.Lock()
		got = groups
		mu.Unlock()
	}

	groups := [][]string{{"a"}, {"b"}}
	schema := concSchema("a", "b")
	reader := newNativeConcReader(groups, map[string]int{"a": 2, "b": 2})
	writer := newRecordingWriter()
	if err := runBulkCopyWithOpts(context.Background(), schema, reader, noopSchemaWriter{}, writer, bulkCopyOpts{CopyFanoutDegree: 1}); err != nil {
		t.Fatalf("runBulkCopyWithOpts (native concurrent): %v (a non-idempotent concurrent reader must NOT be refused)", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got != 2 {
		t.Fatalf("dispatch observed groups=%d; want 2 (native concurrent path engaged)", got)
	}
	if writer.counts["a"] != 2 || writer.counts["b"] != 2 {
		t.Fatalf("native concurrent counts = %v; want a:2 b:2", writer.counts)
	}
}

// --- runBulkCopyWithOpts dispatch: concurrent vs serial (the integration
// of the gate into the orchestrator) ---

// schemaApplyWriter is a no-op SchemaWriter so runBulkCopyWithOpts's DDL
// phases don't panic; the data path is what we exercise.
type noopSchemaWriter struct{}

func (noopSchemaWriter) CreateTablesWithoutConstraints(context.Context, *ir.Schema) error { return nil }

func (noopSchemaWriter) CreateIndexes(context.Context, *ir.Schema) error { return nil }

func (noopSchemaWriter) CreateConstraints(context.Context, *ir.Schema) error { return nil }

func (noopSchemaWriter) CreateViews(context.Context, *ir.Schema) error { return nil }

func (noopSchemaWriter) SyncIdentitySequences(context.Context, *ir.Schema) error { return nil }

func (noopSchemaWriter) Close() error { return nil }

// TestRunBulkCopyWithOpts_DispatchesConcurrent pins that runBulkCopyWithOpts
// takes the concurrent path when the reader surfaces ≥2 groups, and the
// serial path otherwise — observed via the dispatch seam (not timing).
func TestRunBulkCopyWithOpts_DispatchesConcurrent(t *testing.T) {
	prev := concurrentCopyDispatchObserver
	defer func() { concurrentCopyDispatchObserver = prev }()
	var got int
	var mu sync.Mutex
	concurrentCopyDispatchObserver = func(groups int) {
		mu.Lock()
		got = groups
		mu.Unlock()
	}

	t.Run("concurrent when ≥2 groups", func(t *testing.T) {
		groups := [][]string{{"a"}, {"b"}}
		schema := concSchema("a", "b")
		reader := newConcPartReader(groups, map[string]int{"a": 2, "b": 2})
		writer := newRecordingWriter()
		if err := runBulkCopyWithOpts(context.Background(), schema, reader, noopSchemaWriter{}, writer, bulkCopyOpts{CopyFanoutDegree: 1}); err != nil {
			t.Fatalf("runBulkCopyWithOpts: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if got != 2 {
			t.Fatalf("dispatch observed groups=%d; want 2 (concurrent path)", got)
		}
	})

	t.Run("serial when no partition (byte-identical path)", func(t *testing.T) {
		schema := concSchema("a", "b")
		reader := newConcPartReader(nil, map[string]int{"a": 2, "b": 2}) // nil groups
		writer := newRecordingWriter()
		got = -1
		if err := runBulkCopyWithOpts(context.Background(), schema, reader, noopSchemaWriter{}, writer, bulkCopyOpts{CopyFanoutDegree: 1}); err != nil {
			t.Fatalf("runBulkCopyWithOpts: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if got != 0 {
			t.Fatalf("dispatch observed groups=%d; want 0 (serial path)", got)
		}
		// Both tables still written exactly once via the serial loop.
		if writer.counts["a"] != 2 || writer.counts["b"] != 2 {
			t.Fatalf("serial path counts = %v; want a:2 b:2", writer.counts)
		}
	})
}

// TestRunConcurrentTableCopy_CoversAllGroupTables is a belt-and-suspenders
// coverage pin: the union of tables the writer saw == the union of the
// partition groups (no group silently skipped).
func TestRunConcurrentTableCopy_CoversAllGroupTables(t *testing.T) {
	groups := [][]string{{"a", "b"}, {"c"}, {"d", "e"}}
	schema := concSchema("a", "b", "c", "d", "e")
	rowsPer := map[string]int{"a": 1, "b": 1, "c": 1, "d": 1, "e": 1}
	reader := newConcPartReader(groups, rowsPer)
	writer := newRecordingWriter()

	if err := runConcurrentTableCopy(context.Background(), groups, schema, reader, writer, nil, ShardColumnSpec{}, 1, true); err != nil {
		t.Fatalf("runConcurrentTableCopy: %v", err)
	}

	var want []string
	for _, g := range groups {
		want = append(want, g...)
	}
	writer.mu.Lock()
	got := make([]string, 0, len(writer.counts))
	for tbl := range writer.counts {
		got = append(got, tbl)
	}
	writer.mu.Unlock()
	sort.Strings(want)
	sort.Strings(got)
	if len(want) != len(got) {
		t.Fatalf("tables written = %v; want %v (a missing group table is silent loss)", got, want)
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("tables written = %v; want %v", got, want)
		}
	}
}
