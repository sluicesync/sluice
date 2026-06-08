// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// poolFakeReader is a minimal [ir.RowReader] that streams a fixed number
// of trivial rows for any table, then closes the channel. It tracks
// nothing — the concurrency instrumentation lives on the writer (where
// the copy spends its time).
type poolFakeReader struct {
	rowsPerTable int
}

func (r *poolFakeReader) ReadRows(ctx context.Context, _ *ir.Table) (<-chan ir.Row, error) {
	out := make(chan ir.Row)
	go func() {
		defer close(out)
		for i := 0; i < r.rowsPerTable; i++ {
			select {
			case <-ctx.Done():
				return
			case out <- ir.Row{"id": int64(i + 1)}:
			}
		}
	}()
	return out, nil
}

func (r *poolFakeReader) Err() error { return nil }

// poolFakeWriter drains the row channel and counts the rows for its
// table. It increments a shared concurrency gauge on entry and decrements
// on exit, recording the running peak so the test can assert the pool
// never ran more tables at once than tableParallelism. A small dwell
// makes overlaps observable without making the test slow.
type poolFakeWriter struct {
	gauge *concurrencyGauge
	dwell time.Duration

	mu     sync.Mutex
	counts map[string]int
}

func newPoolFakeWriter(gauge *concurrencyGauge, dwell time.Duration) *poolFakeWriter {
	return &poolFakeWriter{gauge: gauge, dwell: dwell, counts: map[string]int{}}
}

func (w *poolFakeWriter) WriteRows(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	w.gauge.enter()
	defer w.gauge.leave()
	var n int
	for range rows {
		n++
	}
	if w.dwell > 0 {
		select {
		case <-time.After(w.dwell):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w.mu.Lock()
	w.counts[table.Name] += n
	w.mu.Unlock()
	return nil
}

func (w *poolFakeWriter) count(name string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.counts[name]
}

// concurrencyGauge tracks the running number of in-flight copies and the
// observed peak, all under atomics so the peak assertion is race-free.
type concurrencyGauge struct {
	cur  atomic.Int64
	peak atomic.Int64
}

func (g *concurrencyGauge) enter() {
	c := g.cur.Add(1)
	for {
		p := g.peak.Load()
		if c <= p || g.peak.CompareAndSwap(p, c) {
			break
		}
	}
}

func (g *concurrencyGauge) leave() { g.cur.Add(-1) }

// poolFakeEngine hands out fresh poolFakeReader / poolFakeWriter pairs for
// the dedicated per-table connections the pool opens via openTablePair
// when a table can't claim the orchestrator's free pair. Every writer it
// hands out shares the same gauge so the peak is global across the free
// pair and the dedicated pairs.
type poolFakeEngine struct {
	stubEngine
	rowsPerTable int
	gauge        *concurrencyGauge
	dwell        time.Duration

	mu      sync.Mutex
	writers []*poolFakeWriter
}

func (e *poolFakeEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return &poolFakeReader{rowsPerTable: e.rowsPerTable}, nil
}

func (e *poolFakeEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	w := newPoolFakeWriter(e.gauge, e.dwell)
	e.mu.Lock()
	e.writers = append(e.writers, w)
	e.mu.Unlock()
	return w, nil
}

// allWriterCount sums one table's row count across every writer the
// engine handed out plus the primary writer (the dedicated pairs and the
// free pair each only ever copy one table, so exactly one of them has the
// count for a given table).
func (e *poolFakeEngine) allWriterCount(primary *poolFakeWriter, name string) int {
	total := primary.count(name)
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, w := range e.writers {
		total += w.count(name)
	}
	return total
}

// runTablePoolForTest drives runBulkCopyTablePool over a schema of n
// PK-bearing tables with the resume store disabled (rc.enabled=false, so
// state writes are pure no-ops and the test stays hermetic). Returns the
// migration state so the caller can assert every table's TableProgress
// entry is Complete.
func runTablePoolForTest(t *testing.T, n, rowsPerTable, tableParallelism int, dwell time.Duration) (*ir.MigrationState, *poolFakeEngine, *poolFakeWriter) {
	t.Helper()

	gauge := &concurrencyGauge{}
	eng := &poolFakeEngine{rowsPerTable: rowsPerTable, gauge: gauge, dwell: dwell}
	primaryWriter := newPoolFakeWriter(gauge, dwell)
	primaryReader := &poolFakeReader{rowsPerTable: rowsPerTable}

	tables := make([]*ir.Table, n)
	for i := range tables {
		tables[i] = &ir.Table{
			Name:    fmt.Sprintf("t%02d", i),
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			PrimaryKey: &ir.Index{
				Columns: []ir.IndexColumn{{Column: "id"}},
			},
		}
	}
	schema := &ir.Schema{Tables: tables}

	// parallelism=1 so tryParallelCopyTable declines (single-reader path);
	// non-resume so canResumePerBatch is false and each table goes through
	// the plain copyTable path. This isolates the CROSS-table axis under
	// test from the within-table chunk machinery.
	deps := &parallelBulkCopyDeps{
		source:      eng,
		target:      eng,
		parallelism: 1,
	}

	state := &ir.MigrationState{TableProgress: map[string]ir.TableProgress{}}
	var stateMu sync.Mutex
	rc := resumeContext{enabled: false} // no store → writeState is a no-op

	if err := runBulkCopyTablePool(
		context.Background(), rc, state, &stateMu, schema,
		primaryReader, primaryWriter,
		false, 0, deps, tableParallelism, nil, ShardColumnSpec{},
	); err != nil {
		t.Fatalf("runBulkCopyTablePool: %v", err)
	}
	return state, eng, primaryWriter
}

// TestRunBulkCopyTablePool_AllComplete drives the pool over many tables
// and asserts (1) every table's final TableProgress entry is Complete and
// (2) every table's rows landed exactly once.
func TestRunBulkCopyTablePool_AllComplete(t *testing.T) {
	const (
		n            = 30
		rowsPerTable = 100
		tableP       = 4
	)
	state, eng, primary := runTablePoolForTest(t, n, rowsPerTable, tableP, 2*time.Millisecond)

	for i := 0; i < n; i++ {
		name := fmt.Sprintf("t%02d", i)
		entry, ok := state.TableProgress[name]
		if !ok {
			t.Errorf("table %s missing from TableProgress", name)
			continue
		}
		if entry.State != ir.TableProgressComplete {
			t.Errorf("table %s state = %q; want complete", name, entry.State)
		}
		if got := eng.allWriterCount(primary, name); got != rowsPerTable {
			t.Errorf("table %s copied %d rows; want %d", name, got, rowsPerTable)
		}
	}
}

// TestRunBulkCopyTablePool_PeakBoundedByParallelism asserts the pool never
// runs more concurrent copies than tableParallelism — the bounded-pool
// invariant. Run across several widths including 1 (serial) and a width
// larger than the table count.
func TestRunBulkCopyTablePool_PeakBoundedByParallelism(t *testing.T) {
	for _, tableP := range []int{1, 2, 4, 8} {
		tableP := tableP
		t.Run(fmt.Sprintf("tableParallelism=%d", tableP), func(t *testing.T) {
			const (
				n            = 20
				rowsPerTable = 50
			)
			gauge := &concurrencyGauge{}
			eng := &poolFakeEngine{rowsPerTable: rowsPerTable, gauge: gauge, dwell: 3 * time.Millisecond}
			primaryWriter := newPoolFakeWriter(gauge, 3*time.Millisecond)
			primaryReader := &poolFakeReader{rowsPerTable: rowsPerTable}

			tables := make([]*ir.Table, n)
			for i := range tables {
				tables[i] = &ir.Table{
					Name:    fmt.Sprintf("t%02d", i),
					Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
					PrimaryKey: &ir.Index{
						Columns: []ir.IndexColumn{{Column: "id"}},
					},
				}
			}
			schema := &ir.Schema{Tables: tables}
			deps := &parallelBulkCopyDeps{source: eng, target: eng, parallelism: 1}
			state := &ir.MigrationState{TableProgress: map[string]ir.TableProgress{}}
			var stateMu sync.Mutex
			rc := resumeContext{enabled: false}

			if err := runBulkCopyTablePool(
				context.Background(), rc, state, &stateMu, schema,
				primaryReader, primaryWriter,
				false, 0, deps, tableP, nil, ShardColumnSpec{},
			); err != nil {
				t.Fatalf("runBulkCopyTablePool: %v", err)
			}

			peak := gauge.peak.Load()
			if peak > int64(tableP) {
				t.Errorf("observed peak concurrency %d exceeds tableParallelism %d", peak, tableP)
			}
			// Sanity: with >1 parallelism and a dwell, we expect to
			// actually overlap (peak >= 2). Skip this assertion for the
			// serial case where peak must be exactly 1.
			if tableP == 1 && peak != 1 {
				t.Errorf("serial pool peak = %d; want 1", peak)
			}
			if tableP > 1 && peak < 2 {
				t.Errorf("parallel pool (tableParallelism=%d) never overlapped; peak=%d", tableP, peak)
			}

			// Every table complete.
			for i := 0; i < n; i++ {
				name := fmt.Sprintf("t%02d", i)
				if state.TableProgress[name].State != ir.TableProgressComplete {
					t.Errorf("table %s not complete: %q", name, state.TableProgress[name].State)
				}
			}
		})
	}
}
