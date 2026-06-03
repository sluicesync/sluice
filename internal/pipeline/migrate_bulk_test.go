// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestPKTracker_LastPK confirms the tracker captures the most recent
// row's PK columns and ignores rows with missing PK keys gracefully.
func TestPKTracker_LastPK(t *testing.T) {
	tr := newPKTracker([]string{"tenant", "id"})

	tr.observe(ir.Row{"tenant": "a", "id": int64(1), "name": "x"})
	tr.observe(ir.Row{"tenant": "a", "id": int64(2), "name": "y"})
	tr.observe(ir.Row{"tenant": "b", "id": int64(3), "name": "z"})

	got, ok := tr.lastPK()
	if !ok {
		t.Fatal("lastPK ok=false after observe(); want true")
	}
	want := []any{"b", int64(3)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("lastPK = %v; want %v", got, want)
	}
}

// TestPKTracker_NoRows confirms the tracker reports ok=false before
// any rows pass through.
func TestPKTracker_NoRows(t *testing.T) {
	tr := newPKTracker([]string{"id"})
	got, ok := tr.lastPK()
	if ok {
		t.Errorf("lastPK ok=true on empty tracker; want false (got %v)", got)
	}
}

// TestPKTracker_NilRowDefensive confirms the observe call on a nil
// row is a no-op rather than a panic. Defensive against pipeline
// edge cases where a closed channel is mishandled.
func TestPKTracker_NilRowDefensive(t *testing.T) {
	tr := newPKTracker([]string{"id"})
	tr.observe(nil)
	if _, ok := tr.lastPK(); ok {
		t.Error("nil row produced a captured PK; want untouched tracker")
	}
}

// TestPKTracker_MissingPKColumn covers the corner where a row is
// missing one of the PK columns (shouldn't happen with a correctly
// configured reader, but the tracker must not panic). The captured
// value is whatever the row returns for the missing key (nil).
func TestPKTracker_MissingPKColumn(t *testing.T) {
	tr := newPKTracker([]string{"tenant", "id"})
	tr.observe(ir.Row{"id": int64(7)}) // tenant missing
	got, ok := tr.lastPK()
	if !ok {
		t.Fatal("lastPK ok=false; want true")
	}
	if len(got) != 2 {
		t.Fatalf("lastPK len = %d; want 2", len(got))
	}
	if got[0] != nil {
		t.Errorf("missing PK col captured as %v; want nil", got[0])
	}
}

// TestTeePKAndCount confirms the teeing wrapper forwards rows
// downstream while updating the tracker and the count atomically.
// Closing the source channel closes the downstream channel.
func TestTeePKAndCount(t *testing.T) {
	src := make(chan ir.Row, 3)
	src <- ir.Row{"id": int64(1)}
	src <- ir.Row{"id": int64(2)}
	src <- ir.Row{"id": int64(3)}
	close(src)

	tr := newPKTracker([]string{"id"})
	var count int64
	tickCount := int64(0)
	out := teePKAndCount(context.Background(), src, tr, &count, func(_ ir.Row) {
		atomic.AddInt64(&tickCount, 1)
	})

	var seen []ir.Row
	for r := range out {
		seen = append(seen, r)
	}
	if len(seen) != 3 {
		t.Fatalf("downstream rows = %d; want 3", len(seen))
	}
	if c := atomic.LoadInt64(&count); c != 3 {
		t.Errorf("count = %d; want 3", c)
	}
	if c := atomic.LoadInt64(&tickCount); c != 3 {
		t.Errorf("ticker calls = %d; want 3", c)
	}
	got, ok := tr.lastPK()
	if !ok || got[0] != int64(3) {
		t.Errorf("lastPK = %v ok=%v; want [3] true", got, ok)
	}
}

// TestCanResumePerBatch_Classification covers every combination of
// the four input axes.
func TestCanResumePerBatch_Classification(t *testing.T) {
	tableWithPK := &ir.Table{
		Name:       "users",
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	tableNoPK := &ir.Table{Name: "events"}

	cases := []struct {
		name     string
		rw       ir.RowWriter
		rr       ir.RowReader
		table    *ir.Table
		resuming bool
		wantOK   bool
		wantWhy  cursorBlockReason
	}{
		{
			name:     "not resuming",
			rw:       fakeBatchableWriter{},
			rr:       fakeBatchableReader{},
			table:    tableWithPK,
			resuming: false,
			wantOK:   false,
			wantWhy:  cursorBlockedNotResuming,
		},
		{
			name:     "no PK",
			rw:       fakeBatchableWriter{},
			rr:       fakeBatchableReader{},
			table:    tableNoPK,
			resuming: true,
			wantOK:   false,
			wantWhy:  cursorBlockedNoPK,
		},
		{
			name:     "reader doesn't implement BatchedRowReader",
			rw:       fakeBatchableWriter{},
			rr:       fakePlainReader{},
			table:    tableWithPK,
			resuming: true,
			wantOK:   false,
			wantWhy:  cursorBlockedReaderNotImpl,
		},
		{
			name:     "writer doesn't implement IdempotentRowWriter",
			rw:       fakePlainWriter{},
			rr:       fakeBatchableReader{},
			table:    tableWithPK,
			resuming: true,
			wantOK:   false,
			wantWhy:  cursorBlockedWriterNotImpl,
		},
		{
			name:     "all conditions met",
			rw:       fakeBatchableWriter{},
			rr:       fakeBatchableReader{},
			table:    tableWithPK,
			resuming: true,
			wantOK:   true,
			wantWhy:  cursorBlockedAvailable,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ok, why := canResumePerBatch(c.rw, c.rr, c.table, c.resuming)
			if ok != c.wantOK || why != c.wantWhy {
				t.Errorf("canResumePerBatch: got (%v, %d); want (%v, %d)", ok, why, c.wantOK, c.wantWhy)
			}
		})
	}
}

// TestPrimaryKeyColumnNames confirms the helper returns nil for
// tables without a PK and the declaration order otherwise.
func TestPrimaryKeyColumnNames(t *testing.T) {
	if got := primaryKeyColumnNames(nil); got != nil {
		t.Errorf("nil table: got %v; want nil", got)
	}
	if got := primaryKeyColumnNames(&ir.Table{}); got != nil {
		t.Errorf("table without PK: got %v; want nil", got)
	}
	table := &ir.Table{
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{
			{Column: "a"}, {Column: "b"},
		}},
	}
	got := primaryKeyColumnNames(table)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("got %v; want [a b]", got)
	}
}

// ---- fakes ----

// fakePlainReader implements only ir.RowReader; missing
// BatchedRowReader so the canResumePerBatch type-assertion fails
// the resume-from-cursor classification.
type fakePlainReader struct{}

func (fakePlainReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	ch := make(chan ir.Row)
	close(ch)
	return ch, nil
}

func (fakePlainReader) Err() error { return nil }

// fakeBatchableReader implements both RowReader and BatchedRowReader.
type fakeBatchableReader struct{}

func (fakeBatchableReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	ch := make(chan ir.Row)
	close(ch)
	return ch, nil
}

func (fakeBatchableReader) ReadRowsBatch(context.Context, *ir.Table, []any, int) (<-chan ir.Row, error) {
	ch := make(chan ir.Row)
	close(ch)
	return ch, nil
}

func (fakeBatchableReader) Err() error { return nil }

// fakePlainWriter implements only ir.RowWriter.
type fakePlainWriter struct{}

func (fakePlainWriter) WriteRows(context.Context, *ir.Table, <-chan ir.Row) error { return nil }

// fakeBatchableWriter implements both RowWriter and IdempotentRowWriter.
type fakeBatchableWriter struct{}

func (fakeBatchableWriter) WriteRows(context.Context, *ir.Table, <-chan ir.Row) error { return nil }

func (fakeBatchableWriter) WriteRowsIdempotent(_ context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	for range rows {
	}
	return nil
}
