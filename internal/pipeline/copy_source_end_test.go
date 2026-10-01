// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
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
// a cancel (closes quietly with Err nil, as the VStream queue and
// mydumper do; or records ctx.Err() on its sticky error, as the PG,
// MySQL, SQLite and D1 readers do) × {the serial table loop, the
// ADR-0100 group loop}, plus `migrate`'s whole-table copy.

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
