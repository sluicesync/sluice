// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// wsConcReader is a native-style concurrent snapshot reader (non-idempotent,
// gap-free) that ALSO implements [ir.WorkStealingCopyReader]: its N readers all
// observe the same consistent snapshot, so any can read any table. It records
// which tables were read on which reader index (via ReadRowsOn) and whether the
// static ReadRows was ever called — so a test can prove the work-stealing path
// engaged and that work crossed the static partition boundary. An optional
// per-table gate lets a test hold one read open to force a deterministic skew.
type wsConcReader struct {
	groups [][]string
	n      int

	mu               sync.Mutex
	rows             map[string][]ir.Row
	gate             map[string]chan struct{} // per-table read gate (nil ⇒ none)
	readsByIdx       map[int][]string         // reader index → tables read via ReadRowsOn
	staticReadCalled bool                     // set if the static ReadRows was used (WS path NOT taken)
}

func newWSConcReader(groups [][]string, n int, rowsPerTable map[string]int) *wsConcReader {
	r := &wsConcReader{
		groups:     groups,
		n:          n,
		rows:       map[string][]ir.Row{},
		gate:       map[string]chan struct{}{},
		readsByIdx: map[int][]string{},
	}
	for tbl, cnt := range rowsPerTable {
		rows := make([]ir.Row, 0, cnt)
		for i := 0; i < cnt; i++ {
			rows = append(rows, ir.Row{"id": int64(i), "v": fmt.Sprintf("%s-%d", tbl, i)})
		}
		r.rows[tbl] = rows
	}
	return r
}

func (r *wsConcReader) ConcurrentCopyGroups() [][]string { return r.groups }
func (r *wsConcReader) ConcurrentReaderCount() int       { return r.n }
func (r *wsConcReader) Err() error                       { return nil }

// ReadRows is the static-partition entry point — it MUST NOT be called on the
// work-stealing path; we flag it so the test can assert the path taken.
func (r *wsConcReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	r.mu.Lock()
	r.staticReadCalled = true
	r.mu.Unlock()
	return r.serve(ctx, table.Name), nil
}

func (r *wsConcReader) ReadRowsOn(ctx context.Context, table *ir.Table, reader int) (<-chan ir.Row, error) {
	r.mu.Lock()
	r.readsByIdx[reader] = append(r.readsByIdx[reader], table.Name)
	r.mu.Unlock()
	return r.serve(ctx, table.Name), nil
}

func (r *wsConcReader) serve(ctx context.Context, name string) <-chan ir.Row {
	r.mu.Lock()
	rows := r.rows[name]
	gate := r.gate[name]
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
	return out
}

// TestRunConcurrentTableCopy_WorkStealing_ExactlyOnce pins that a reader
// implementing ir.WorkStealingCopyReader takes the work-stealing path (reads
// via ReadRowsOn, never the static ReadRows) and copies EVERY table exactly
// once — none dropped, none double-read.
func TestRunConcurrentTableCopy_WorkStealing_ExactlyOnce(t *testing.T) {
	groups := [][]string{{"a", "b", "c", "d", "e"}, {"f"}}
	schema := concSchema("a", "b", "c", "d", "e", "f")
	rowsPer := map[string]int{"a": 7, "b": 11, "c": 13, "d": 17, "e": 5, "f": 19}
	reader := newWSConcReader(groups, 2, rowsPer)
	writer := newRecordingWriter()

	// needsIdempotent=false → the native plain-INSERT path (work-stealing only
	// applies to the native shared-snapshot reader).
	if err := runConcurrentTableCopy(context.Background(), groups, schema, reader, writer, nil, ShardColumnSpec{}, 1, false); err != nil {
		t.Fatalf("runConcurrentTableCopy (work-stealing): %v", err)
	}

	// Exactly-once coverage on the WRITE side.
	writer.mu.Lock()
	if len(writer.counts) != 6 {
		writer.mu.Unlock()
		t.Fatalf("tables written = %d; want 6 (a dropped table is silent loss): %v", len(writer.counts), writer.counts)
	}
	for tbl, want := range rowsPer {
		if got := writer.counts[tbl]; got != want {
			t.Errorf("table %q rows written = %d; want %d", tbl, got, want)
		}
	}
	writer.mu.Unlock()

	// Work-stealing path engaged: ReadRowsOn used, static ReadRows never.
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.staticReadCalled {
		t.Error("static ReadRows was called — the work-stealing path was not taken")
	}
	// Every table read EXACTLY once across all reader indices.
	seen := map[string]int{}
	for _, tbls := range reader.readsByIdx {
		for _, tn := range tbls {
			seen[tn]++
		}
	}
	for tbl := range rowsPer {
		if seen[tbl] != 1 {
			t.Errorf("table %q read %d times via ReadRowsOn; want exactly 1", tbl, seen[tbl])
		}
	}
}

// TestRunConcurrentTableCopy_WorkStealing_CrossesGroupBoundary deterministically
// proves WORK-STEALING (not the static per-group drain): with a skewed partition
// (group0 = 5 tables, group1 = 1) and the first claimed table gated open, the
// other pipeline must pull tables from BOTH original groups — crossing the static
// partition boundary, which the fixed-group drain can never do.
func TestRunConcurrentTableCopy_WorkStealing_CrossesGroupBoundary(t *testing.T) {
	group0 := []string{"a", "b", "c", "d", "e"}
	group1 := []string{"f"}
	groups := [][]string{group0, group1}
	schema := concSchema("a", "b", "c", "d", "e", "f")
	rowsPer := map[string]int{"a": 3, "b": 3, "c": 3, "d": 3, "e": 3, "f": 3}
	reader := newWSConcReader(groups, 2, rowsPer)
	writer := newRecordingWriter()

	// Gate "a" (the first table in the flattened work list, so it is claim 0):
	// whichever pipeline claims it blocks reading it, leaving the other pipeline
	// to drain b,c,d,e,f — which spans BOTH groups.
	gateA := make(chan struct{})
	reader.mu.Lock()
	reader.gate["a"] = gateA
	reader.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- runConcurrentTableCopy(context.Background(), groups, schema, reader, writer, nil, ShardColumnSpec{}, 1, false)
	}()

	// Wait until the 5 non-gated tables (b..f) are all written by the free
	// pipeline, then release "a".
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		writer.mu.Lock()
		n := len(writer.counts)
		writer.mu.Unlock()
		if n >= 5 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(gateA)
	if err := <-done; err != nil {
		t.Fatalf("runConcurrentTableCopy (work-stealing skew): %v", err)
	}

	in0 := map[string]bool{}
	for _, tn := range group0 {
		in0[tn] = true
	}
	// Some reader index must have read a table from group0 AND a table from
	// group1 — i.e. it crossed the static partition boundary. The static
	// per-group drain assigns each pipeline ONE group, so it never could.
	reader.mu.Lock()
	defer reader.mu.Unlock()
	crossed := false
	for idx, tbls := range reader.readsByIdx {
		sawG0, sawG1 := false, false
		for _, tn := range tbls {
			if in0[tn] {
				sawG0 = true
			} else {
				sawG1 = true
			}
		}
		if sawG0 && sawG1 {
			crossed = true
			t.Logf("reader index %d crossed the group boundary, read: %v", idx, tbls)
		}
	}
	if !crossed {
		t.Errorf("no reader crossed the static group boundary — work-stealing did not redistribute; reads = %v", reader.readsByIdx)
	}
}
