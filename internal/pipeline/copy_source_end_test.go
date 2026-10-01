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
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// GC-41 (i): a copy counts as complete ONLY when its source drained to
// its natural end.
//
// The defect these cells pin: a stop landing mid-copy closes the row
// stream (the reader, the tee, or a stage between them unwinds on the
// cancelled context), and a writer whose "channel closed" select arm
// wins over its ctx.Done arm on an empty buffer returns nil. Before the
// fix nothing downstream could tell that nil from a finished table, so
// the caller recorded the table COMPLETE — with the rows it had so far —
// and kept iterating, recording every later table COMPLETE with 0 rows.
// The only thing that kept it from becoming durable was that the
// COMPLETE write ran on the same cancelled context and failed; the
// detached write GC-41 (i) wanted (recordCommittedWorkCtx) removed that
// accident, and a resume then skipped the table's tail for good.
//
// The cells are the class, not one representative: every copy shape the
// serial and group loops dispatch to (plain and idempotent, single
// writer and the ADR-0097 fan-out) × both ways a reader ends a stream on
// a cancel of the copy's context (closes quietly with Err nil, as the
// VStream queue and mydumper do; or records ctx.Err() on its sticky
// error, as the PG, MySQL, SQLite and D1 readers do) × {the serial table
// loop, the ADR-0100 group loop}, plus `migrate`'s whole-table copy. A
// reader stopped by ITS OWN shutdown, with the copy's context still live,
// is the separate cell TestStopMidCopy_AReaderStoppedByItsOwnShutdownIsRefused.

// stopMidCopyReader emits rowsBeforeStop rows per table, signals that it
// has, and then behaves as a table with more rows to come: it blocks
// until its context ends. stickyCancel picks how it then ends the
// stream. Rows are all on the "id" PK so the fan-out can hash them.
type stopMidCopyReader struct {
	rowsBeforeStop int
	stickyCancel   bool
	groups         [][]string // non-nil: surface an ADR-0100 partition

	reached func() // called once per table that has emitted its rows

	mu  sync.Mutex
	err error
}

func (r *stopMidCopyReader) ReadRows(ctx context.Context, _ *ir.Table) (<-chan ir.Row, error) {
	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()
	ch := make(chan ir.Row)
	go func() {
		defer close(ch)
		for i := 0; i < r.rowsBeforeStop; i++ {
			select {
			case ch <- ir.Row{"id": int64(i)}:
			case <-ctx.Done():
				r.ended(ctx)
				return
			}
		}
		r.reached()
		<-ctx.Done()
		r.ended(ctx)
	}()
	return ch, nil
}

func (r *stopMidCopyReader) ended(ctx context.Context) {
	if !r.stickyCancel {
		return
	}
	r.mu.Lock()
	r.err = ctx.Err()
	r.mu.Unlock()
}

func (r *stopMidCopyReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *stopMidCopyReader) ConcurrentCopyGroups() [][]string { return r.groups }

// idempotentStopMidCopyReader is the same reader declaring the Bug-125
// idempotent COPY, which routes the loops through the upsert copies.
type idempotentStopMidCopyReader struct{ *stopMidCopyReader }

func (idempotentStopMidCopyReader) CopyNeedsIdempotentWriter() bool { return true }

// closedWinsWriter is every writer select race resolved the bad way, made
// deterministic: it ranges over its input, ignores its context, and
// returns nil when the channel closes — exactly what the MySQL and
// Postgres write loops do whenever "closed" wins on an empty buffer. It
// implements every bulk surface the loops can dispatch to.
type closedWinsWriter struct{ written atomic.Int64 }

func (w *closedWinsWriter) drain(rows <-chan ir.Row) {
	for range rows {
		w.written.Add(1)
	}
}

func (w *closedWinsWriter) WriteRows(_ context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	w.drain(rows)
	return nil
}

func (w *closedWinsWriter) WriteRowsIdempotent(_ context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	w.drain(rows)
	return nil
}

func (w *closedWinsWriter) drainAll(workers []<-chan ir.Row) {
	var wg sync.WaitGroup
	for _, ch := range workers {
		wg.Add(1)
		go func(ch <-chan ir.Row) {
			defer wg.Done()
			w.drain(ch)
		}(ch)
	}
	wg.Wait()
}

func (w *closedWinsWriter) WriteRowsParallel(_ context.Context, _ *ir.Table, workers []<-chan ir.Row) error {
	w.drainAll(workers)
	return nil
}

func (w *closedWinsWriter) WriteRowsIdempotentParallel(_ context.Context, _ *ir.Table, workers []<-chan ir.Row) error {
	w.drainAll(workers)
	return nil
}

// drainRowsLikeAWriter is what every real writer does with its input
// before returning nil: consume it to the close. Test writers that only
// record a call drain through it, because a copy now refuses a nil from a
// writer that returned before its source reached the end (GC-41 (i)) —
// that is a writer that dropped rows, and it is exactly what a stub that
// ignores its channel looks like.
func drainRowsLikeAWriter(rows <-chan ir.Row) {
	for range rows { //nolint:revive // draining is the point
	}
}

// ctxHonouringStateStore is a fakeStateStore whose per-table write fails on
// a done context, the way a real driver does.
type ctxHonouringStateStore struct{ *fakeStateStore }

func (s ctxHonouringStateStore) WriteTableProgress(ctx context.Context, id, table string, p ir.TableProgress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.fakeStateStore.WriteTableProgress(ctx, id, table, p)
}

func stopMidCopySchema(names ...string) *ir.Schema {
	s := &ir.Schema{}
	for _, n := range names {
		s.Tables = append(s.Tables, &ir.Table{
			Name:       n,
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		})
	}
	return s
}

// cancelWhenLanesReached returns the reader's reached hook: it cancels the
// run once every lane has an in-flight table holding rows, so the stop
// lands mid-copy on all of them at once.
func cancelWhenLanesReached(lanes int, cancel context.CancelFunc) func() {
	var n atomic.Int64
	return func() {
		if n.Add(1) == int64(lanes) {
			cancel()
		}
	}
}

// assertNoCompleteRow is the pin's verdict: no table the stop interrupted
// — the in-flight one or any the loop reached after it — may be recorded
// COMPLETE, however the store was written.
func assertNoCompleteRow(t *testing.T, store *fakeStateStore, migrationID string, tables []string) {
	t.Helper()
	state, _ := store.get(migrationID)
	for _, name := range tables {
		if e := state.TableProgress[name]; e.State == ir.TableProgressComplete {
			t.Errorf("table %q was recorded COMPLETE with %d rows after a stop interrupted its copy: a resume skips "+
				"whatever it had not read yet — silent loss (GC-41 (i))", name, e.RowsCopied)
		}
	}
}

func TestStopMidCopyRecordsNoCompleteRow(t *testing.T) {
	type loopShape struct {
		name   string
		tables []string
		groups [][]string
		lanes  int
	}
	loops := []loopShape{
		{name: "serial-loop", tables: []string{"a", "b"}, lanes: 1},
		{name: "group-loop", tables: []string{"a", "b", "c", "d"}, groups: [][]string{{"a", "b"}, {"c", "d"}}, lanes: 2},
	}
	for _, loop := range loops {
		for _, idempotent := range []bool{false, true} {
			for _, fanout := range []int{1, 2} {
				for _, sticky := range []bool{false, true} {
					name := fmt.Sprintf("%s/idempotent=%t/fanout=%d/sticky-cancel=%t", loop.name, idempotent, fanout, sticky)
					t.Run(name, func(t *testing.T) {
						t.Parallel()
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						base := &stopMidCopyReader{
							rowsBeforeStop: 5,
							stickyCancel:   sticky,
							groups:         loop.groups,
							reached:        cancelWhenLanesReached(loop.lanes, cancel),
						}
						var rows ir.RowReader = base
						if idempotent {
							rows = idempotentStopMidCopyReader{base}
						}
						store := ctxHonouringStateStore{newFakeStateStore()}
						var phaseLog []string
						err := runBulkCopyWithOpts(ctx, stopMidCopySchema(loop.tables...), rows,
							&recordingSchemaWriter{phaseLog: &phaseLog}, &closedWinsWriter{}, bulkCopyOpts{
								Recording:        resumeContext{store: store, migrationID: "s", enabled: true, noResume: true},
								CopyFanoutDegree: fanout,
							})
						if err == nil {
							t.Error("a cold start a stop interrupted mid-copy returned nil — its caller reads that as a finished copy")
						}
						assertNoCompleteRow(t, store.fakeStateStore, "s", loop.tables)
					})
				}
			}
		}
	}

	// migrate's whole-table copy (copyOneTableData → copyTable), which
	// writes its COMPLETE row through setTableProgressAndWrite.
	for _, sticky := range []bool{false, true} {
		t.Run(fmt.Sprintf("migrate-whole-table/sticky-cancel=%t", sticky), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reader := &stopMidCopyReader{rowsBeforeStop: 5, stickyCancel: sticky, reached: cancelWhenLanesReached(1, cancel)}
			store := ctxHonouringStateStore{newFakeStateStore()}
			rc := resumeContext{store: store, migrationID: "m", enabled: true}
			state := &ir.MigrationState{MigrationID: "m", TableProgress: map[string]ir.TableProgress{}}
			var mu sync.Mutex
			table := stopMidCopySchema("a").Tables[0]
			if err := copyOneTableData(ctx, rc, state, &mu, reader, &closedWinsWriter{}, table, false, 0, nil, nil, ShardColumnSpec{}); err == nil {
				t.Error("migrate's whole-table copy returned nil after a stop interrupted it")
			}
			assertNoCompleteRow(t, store.fakeStateStore, "m", []string{"a"})
		})
	}
}

// drainedThenClosedReader emits n rows and ends NATURALLY, then signals.
type drainedThenClosedReader struct {
	n      int
	closed chan struct{}
}

func (r *drainedThenClosedReader) ReadRows(ctx context.Context, _ *ir.Table) (<-chan ir.Row, error) {
	ch := make(chan ir.Row)
	go func() {
		defer close(r.closed)
		defer close(ch)
		for i := 0; i < r.n; i++ {
			select {
			case ch <- ir.Row{"id": int64(i)}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

func (*drainedThenClosedReader) Err() error { return nil }

// heldWriter does not start draining until released — a target that is
// slow while the stop lands.
type heldWriter struct {
	release chan struct{}
	closedWinsWriter
}

func (w *heldWriter) WriteRows(ctx context.Context, t *ir.Table, rows <-chan ir.Row) error {
	<-w.release
	return w.closedWinsWriter.WriteRows(ctx, t, rows)
}

// The source drained, but a stage BETWEEN the tee and the writer dropped
// rows on the stop: the tee saw a natural end, so the source-end half of
// the verdict alone would pass. Only Confirm's live-run clause catches it.
// The shard stamp is engaged so a buffered stage sits in that gap; 100
// rows over-fill the stamp's buffer, so it is parked mid-send when the
// stop lands and drops what it holds. (Should the tee not yet have seen
// the end when the stop lands, the cell still refuses, on the other
// clause — it can be weaker than intended, never red without cause.)
func TestStopMidCopy_DropBetweenADrainedTeeAndTheWriterIsRefused(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &drainedThenClosedReader{n: 100, closed: make(chan struct{})}
	writer := &heldWriter{release: make(chan struct{})}
	go func() {
		<-reader.closed
		time.Sleep(50 * time.Millisecond) // let the tee observe the close
		cancel()
		close(writer.release)
	}()
	store := ctxHonouringStateStore{newFakeStateStore()}
	var phaseLog []string
	err := runBulkCopyWithOpts(ctx, stopMidCopySchema("a"), reader,
		&recordingSchemaWriter{phaseLog: &phaseLog}, writer, bulkCopyOpts{
			Recording:        resumeContext{store: store, migrationID: "s", enabled: true, noResume: true},
			Shard:            ShardColumnSpec{Name: "shard", Value: "s1"},
			CopyFanoutDegree: 1, // the single-writer WriteRows below is the one held
		})
	if err == nil {
		t.Errorf("a copy whose stamp stage dropped rows on the stop returned nil; the writer wrote %d of 100",
			writer.written.Load())
	}
	assertNoCompleteRow(t, store.fakeStateStore, "s", []string{"a"})
}

// earlyReturnWriter takes `take` rows and returns nil — a writer that
// stopped consuming without an error, with the run still live. When the
// whole source fits in the stage buffers (total <= 64) it first waits
// for the rest to be buffered for it, so the source has really ended by
// the time it returns; otherwise the cell would race the tee to the end.
type earlyReturnWriter struct{ take, total int }

func (w earlyReturnWriter) WriteRows(_ context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	for i := 0; i < w.take; i++ {
		<-rows
	}
	if w.total <= 64 {
		for len(rows) < w.total-w.take {
			time.Sleep(time.Millisecond)
		}
		time.Sleep(10 * time.Millisecond) // the tee's close follows its last send
	}
	return nil
}

// The other clauses of the verdict: no stop at all, the run live
// throughout, and a writer that returned nil having taken 2 rows. Two
// source sizes, because they are caught by different clauses: 1000 rows
// over-fill the stage buffers, so the tee never reaches the source's end
// (the source-end clause); 10 rows sit wholly in the buffers, so the
// source DID end and only the handed-channel consumption clause sees the
// 8 rows the writer left behind.
func TestStopMidCopy_AWriterThatReturnsBeforeTheEndIsRefused(t *testing.T) {
	for _, n := range []int{10, 1000} {
		t.Run(fmt.Sprintf("source-rows=%d", n), func(t *testing.T) {
			t.Parallel()
			reader := &drainedThenClosedReader{n: n, closed: make(chan struct{})}
			store := ctxHonouringStateStore{newFakeStateStore()}
			var phaseLog []string
			err := runBulkCopyWithOpts(context.Background(), stopMidCopySchema("a"), reader,
				&recordingSchemaWriter{phaseLog: &phaseLog}, earlyReturnWriter{take: 2, total: n}, bulkCopyOpts{
					Recording:        resumeContext{store: store, migrationID: "s", enabled: true, noResume: true},
					CopyFanoutDegree: 1,
				})
			if err == nil {
				t.Errorf("a copy whose writer returned nil after 2 of %d rows returned nil", n)
			}
			assertNoCompleteRow(t, store.fakeStateStore, "s", []string{"a"})
		})
	}
}

// shutdownStoppedReader is the VStream snapshot stream's shutdown shape
// (cancelCopyForShutdown): it emits some rows, records context.Canceled
// on its sticky error, and closes — while the copy's own context is live.
type shutdownStoppedReader struct {
	mu  sync.Mutex
	err error
}

func (r *shutdownStoppedReader) ReadRows(context.Context, *ir.Table) (<-chan ir.Row, error) {
	ch := make(chan ir.Row, 8)
	for i := 0; i < 5; i++ {
		ch <- ir.Row{"id": int64(i)}
	}
	r.mu.Lock()
	r.err = context.Canceled
	r.mu.Unlock()
	close(ch)
	return ch, nil
}

func (r *shutdownStoppedReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// A reader stopped by its own shutdown: the stream closes with nothing
// dropped downstream and the copy's context still live, so every clause
// of the source-end verdict passes. Only ReaderStreamErr refusing the
// recorded cancel stands between this and a COMPLETE row for a table
// that was not fully read.
func TestStopMidCopy_AReaderStoppedByItsOwnShutdownIsRefused(t *testing.T) {
	t.Parallel()
	store := ctxHonouringStateStore{newFakeStateStore()}
	var phaseLog []string
	err := runBulkCopyWithOpts(context.Background(), stopMidCopySchema("a"), &shutdownStoppedReader{},
		&recordingSchemaWriter{phaseLog: &phaseLog}, &closedWinsWriter{}, bulkCopyOpts{
			Recording:        resumeContext{store: store, migrationID: "s", enabled: true, noResume: true},
			CopyFanoutDegree: 1,
		})
	if !errors.Is(err, migcore.ErrCopyInterrupted) {
		t.Errorf("a copy whose reader recorded its own shutdown's cancel returned %v, want ErrCopyInterrupted", err)
	}
	assertNoCompleteRow(t, store.fakeStateStore, "s", []string{"a"})
}

// The positive control: the same reader, writer and store with nothing
// stopping the run record every table COMPLETE with its real row count.
// Without it, a harness that could never record COMPLETE would pass every
// cell above.
func TestStopMidCopyControl_AnUninterruptedCopyRecordsComplete(t *testing.T) {
	t.Parallel()
	reader := &rowEmittingReader{rowsPerTable: 5}
	store := ctxHonouringStateStore{newFakeStateStore()}
	var phaseLog []string
	if err := runBulkCopyWithOpts(context.Background(), stopMidCopySchema("a", "b"), reader,
		&recordingSchemaWriter{phaseLog: &phaseLog}, &closedWinsWriter{}, bulkCopyOpts{
			Recording: resumeContext{store: store, migrationID: "s", enabled: true, noResume: true},
		}); err != nil {
		t.Fatalf("runBulkCopyWithOpts: %v", err)
	}
	state, _ := store.get("s")
	for _, name := range []string{"a", "b"} {
		if e := state.TableProgress[name]; e.State != ir.TableProgressComplete || e.RowsCopied != 5 {
			t.Errorf("control: table %q recorded %+v, want COMPLETE with 5 rows", name, e)
		}
	}
}
