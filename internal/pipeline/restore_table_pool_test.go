// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the cross-table restore pool (ADR-0084 restore side):
// the serial collapse, the free-writer claim/release accounting, the
// first-error peer cancellation, the parallelism resolution, and the
// DataOnly idempotent-writer dispatch under parallelism. CI runs these
// under -race; locally (CGO=0 Windows) they pin shape only.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// restorePoolFakeWriter is a per-test [ir.RowWriter] whose behaviour is
// keyed on the table name:
//
//   - "bad"      → WriteRows returns an immediate error WITHOUT
//     draining (the Bug-40b producer-cancel shape).
//   - "block_*"  → WriteRows blocks without draining until ctx is
//     cancelled — a peer's error must unwind it via the errgroup ctx.
//   - else       → drains every row, then returns nil.
//
// Close increments closes so dedicated-writer accounting is pinnable;
// drained counts applied tables (across all writers via the shared
// pointer).
type restorePoolFakeWriter struct {
	closes  *atomic.Int64
	drained *atomic.Int64
}

func (w *restorePoolFakeWriter) WriteRows(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	switch {
	case table.Name == "bad":
		return errors.New("injected write failure")
	case strings.HasPrefix(table.Name, "block"):
		<-ctx.Done()
		return ctx.Err()
	}
	for range rows {
	}
	if w.drained != nil {
		w.drained.Add(1)
	}
	return nil
}

func (w *restorePoolFakeWriter) Close() error {
	if w.closes != nil {
		w.closes.Add(1)
	}
	return nil
}

// restorePoolFixture backs up nTables one-row tables named by names
// into a fresh local store (so restoreTable has real chunks to
// stream), then returns a Restore wired to that store plus the pool
// tasks in schema order.
func restorePoolFixture(t *testing.T, names []string) (*Restore, []restoreTableTask) {
	t.Helper()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{}
	rows := map[string][]ir.Row{}
	for i, name := range names {
		schema.Tables = append(schema.Tables, &ir.Table{
			Name:    name,
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		})
		rows[name] = []ir.Row{{"id": int64(i)}}
	}
	src := newBackupRecorderEngine("postgres", schema, rows)
	if err := (&Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	manifest, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	r := &Restore{Target: stubEngine{}, TargetDSN: "dsn", Store: store}
	r.segCodec, err = r.rootSegmentCodec(context.Background())
	if err != nil {
		t.Fatalf("rootSegmentCodec: %v", err)
	}
	tablesByName := indexManifestTables(manifest.Tables)
	tasks := make([]restoreTableTask, 0, len(names))
	for _, table := range schema.Tables {
		entry, ok := tablesByName[manifestTableKey(table.Schema, table.Name)]
		if !ok {
			t.Fatalf("table %s missing from manifest", table.Name)
		}
		tasks = append(tasks, restoreTableTask{table: table, entry: entry})
	}
	return r, tasks
}

// TestRestoreTablePool_SerialCollapseNeverOpensWriters pins that
// tableParallelism=1 applies every table through the free (primary)
// writer and the dedicated-writer factory is never invoked — the
// one-code-path serial collapse.
func TestRestoreTablePool_SerialCollapseNeverOpensWriters(t *testing.T) {
	r, tasks := restorePoolFixture(t, []string{"t00", "t01", "t02", "t03"})
	var drained atomic.Int64
	primary := &restorePoolFakeWriter{drained: &drained}
	var factoryCalls atomic.Int64
	factory := func(context.Context) (ir.RowWriter, error) {
		factoryCalls.Add(1)
		return nil, errors.New("factory must not be called on the serial path")
	}
	if err := r.runRestoreTablePool(context.Background(), tasks, primary, factory, 1, 1); err != nil {
		t.Fatalf("runRestoreTablePool: %v", err)
	}
	if n := factoryCalls.Load(); n != 0 {
		t.Errorf("factory called %d times on the serial path; want 0", n)
	}
	if n := drained.Load(); n != int64(len(tasks)) {
		t.Errorf("tables applied = %d; want %d", n, len(tasks))
	}
}

// TestRestoreTablePool_DedicatedWritersClosed pins the free-writer
// claim/release accounting: with N tables and full parallelism, every
// writer the factory opened is closed exactly once by the release
// path, and the primary (free) writer is never closed by the pool.
func TestRestoreTablePool_DedicatedWritersClosed(t *testing.T) {
	r, tasks := restorePoolFixture(t, []string{"t00", "t01", "t02", "t03", "t04", "t05"})
	var primaryCloses, dedicatedCloses, drained atomic.Int64
	primary := &restorePoolFakeWriter{closes: &primaryCloses, drained: &drained}
	var opened atomic.Int64
	factory := func(context.Context) (ir.RowWriter, error) {
		opened.Add(1)
		return &restorePoolFakeWriter{closes: &dedicatedCloses, drained: &drained}, nil
	}
	if err := r.runRestoreTablePool(context.Background(), tasks, primary, factory, 4, 1); err != nil {
		t.Fatalf("runRestoreTablePool: %v", err)
	}
	if primaryCloses.Load() != 0 {
		t.Errorf("primary writer closed %d times by the pool; want 0 (caller owns it)", primaryCloses.Load())
	}
	if o, c := opened.Load(), dedicatedCloses.Load(); o != c {
		t.Errorf("opened %d dedicated writers but closed %d; want equal", o, c)
	}
	if n := drained.Load(); n != int64(len(tasks)) {
		t.Errorf("tables applied = %d; want %d", n, len(tasks))
	}
}

// TestRestoreTablePool_FirstErrorCancelsPeers pins the errgroup
// contract: one table's write failure cancels the derived ctx so peer
// tables blocked mid-apply unwind (through restoreTable's Bug-40b
// producer-cancel path), and the pool returns the original error
// naming the failed table.
func TestRestoreTablePool_FirstErrorCancelsPeers(t *testing.T) {
	r, tasks := restorePoolFixture(t, []string{"block_0", "block_1", "block_2", "bad"})
	primary := &restorePoolFakeWriter{}
	factory := func(context.Context) (ir.RowWriter, error) {
		return &restorePoolFakeWriter{}, nil
	}

	done := make(chan error, 1)
	go func() {
		done <- r.runRestoreTablePool(context.Background(), tasks, primary, factory, 4, 1)
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), `table "bad"`) || !strings.Contains(err.Error(), "injected write failure") {
			t.Fatalf("pool error = %v; want the injected failure naming table \"bad\"", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("pool did not return after the failing table; peer cancellation broken")
	}
}

// TestRestoreTablePool_NilFactoryDedicatedBranchIsLoud pins the
// programming-error guard: reaching the dedicated-writer branch with a
// nil factory surfaces errRestorePoolNoFactory rather than a nil deref.
func TestRestoreTablePool_NilFactoryDedicatedBranchIsLoud(t *testing.T) {
	free := make(chan ir.RowWriter, 1) // empty: free writer held by a peer
	_, release, err := acquireRestoreWriter(context.Background(), free, nil)
	release()
	if !errors.Is(err, errRestorePoolNoFactory) {
		t.Fatalf("err = %v; want errRestorePoolNoFactory", err)
	}
}

// observeRestoreDispatch installs the test-only dispatch observer and
// returns pointers to the captured decision. Restores the seam via
// t.Cleanup. Tests using it must not run in t.Parallel (package
// precedent: backupDispatchObserver).
func observeRestoreDispatch(t *testing.T) (gotParallelism *int, gotReason *string) {
	t.Helper()
	p, r := 0, ""
	restoreDispatchObserver = func(tableParallelism int, reason string) {
		p, r = tableParallelism, reason
	}
	t.Cleanup(func() { restoreDispatchObserver = nil })
	return &p, &r
}

// TestResolveRestoreParallelism pins the cross-table (ADR-0084) dispatch
// matrix: 0 = auto = 4, clamp to the table count, the operator-serial
// collapse, and an explicit value passing through unclamped on a
// budget-less target — no engine eligibility gate (restore parallelism
// is engine-generic). The within-table chunk axis is left at default
// (auto) here; budget-less stubEngine passes both axes through, so the
// table axis is unaffected by the two-axis split.
func TestResolveRestoreParallelism(t *testing.T) {
	cases := []struct {
		name       string
		configured int
		taskCount  int
		want       int
		wantReason string
	}{
		{"auto default", 0, 10, migcore.DefaultTableParallelism, ""},
		{"clamp to task count", 0, 2, 2, ""},
		{"single table collapses", 4, 1, 1, "at most one table"},
		{"operator serial", 1, 5, 1, "--table-parallelism=1"},
		{"explicit passes unclamped", 8, 12, 8, ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			r := &Restore{Target: stubEngine{}, TargetDSN: "dsn", Store: &blobcodec.LocalStore{}, TableParallelism: c.configured}
			gotP, gotReason := observeRestoreDispatch(t)
			got, _, err := r.resolveRestoreParallelism(context.Background(), c.taskCount)
			if err != nil {
				t.Fatalf("resolveRestoreParallelism: %v", err)
			}
			if got != c.want || *gotP != c.want {
				t.Errorf("tableParallelism = %d (observer %d); want %d", got, *gotP, c.want)
			}
			if !strings.Contains(*gotReason, c.wantReason) {
				t.Errorf("reason = %q; want contains %q", *gotReason, c.wantReason)
			}
		})
	}
}

// idempotentRestoreEngine wraps restoreRecorderEngine so its row
// writers also implement [ir.IdempotentRowWriter], counting which
// write surface each per-table apply dispatched to.
type idempotentRestoreEngine struct {
	*restoreRecorderEngine

	plainCalls      atomic.Int64
	idempotentCalls atomic.Int64
}

func (e *idempotentRestoreEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return &idempotentRecordingRowWriter{engine: e}, nil
}

type idempotentRecordingRowWriter struct {
	engine *idempotentRestoreEngine
}

func (w *idempotentRecordingRowWriter) WriteRows(_ context.Context, table *ir.Table, rows <-chan ir.Row) error {
	w.engine.plainCalls.Add(1)
	for r := range rows {
		w.engine.recordRow(table.Name, r)
	}
	return nil
}

func (w *idempotentRecordingRowWriter) WriteRowsIdempotent(_ context.Context, table *ir.Table, rows <-chan ir.Row) error {
	w.engine.idempotentCalls.Add(1)
	for r := range rows {
		w.engine.recordRow(table.Name, r)
	}
	return nil
}

// TestRestore_DataOnlyParallel_DispatchesIdempotentPerWorker pins the
// DataOnly × parallelism composition: every table of a parallel
// DataOnly restore routes through WriteRowsIdempotent (the per-worker
// type assertion inside restoreTable — each dedicated writer makes its
// own dispatch decision), never plain WriteRows, and every row still
// arrives.
func TestRestore_DataOnlyParallel_DispatchesIdempotentPerWorker(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{}
	rows := map[string][]ir.Row{}
	names := []string{"d00", "d01", "d02"}
	for i, name := range names {
		schema.Tables = append(schema.Tables, &ir.Table{
			Name:    name,
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		})
		rows[name] = []ir.Row{{"id": int64(i)}, {"id": int64(i + 100)}}
	}
	src := newBackupRecorderEngine("postgres", schema, rows)
	if err := (&Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	tgt := &idempotentRestoreEngine{restoreRecorderEngine: newRestoreRecorderEngine("postgres")}
	gotP, gotReason := observeRestoreDispatch(t)
	if err := (&Restore{
		Target:           tgt,
		TargetDSN:        "tgt",
		Store:            store,
		DataOnly:         true,
		TableParallelism: 4,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	if *gotP != len(names) {
		t.Errorf("dispatch parallelism = %d (reason %q); want %d (clamped to table count)", *gotP, *gotReason, len(names))
	}
	if n := tgt.plainCalls.Load(); n != 0 {
		t.Errorf("plain WriteRows called %d times in DataOnly mode; want 0", n)
	}
	if n := tgt.idempotentCalls.Load(); n != int64(len(names)) {
		t.Errorf("WriteRowsIdempotent called %d times; want %d (one per table)", n, len(names))
	}
	_, gotRows := tgt.snapshot()
	for _, name := range names {
		if len(gotRows[name]) != 2 {
			t.Errorf("table %s rows = %d; want 2", name, len(gotRows[name]))
		}
	}
}

// TestRestore_ParallelRun_RoundTripAllRowsArrive pins the Run-level
// wiring (tasks built from the manifest → resolve → pool with the
// openTargetRowWriter factory): a TableParallelism=4 restore of a
// 6-table backup engages the pool (observer-asserted) and every
// table's rows arrive intact.
func TestRestore_ParallelRun_RoundTripAllRowsArrive(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := &ir.Schema{}
	rows := map[string][]ir.Row{}
	const nTables = 6
	names := make([]string, 0, nTables)
	for i := 0; i < nTables; i++ {
		name := fmt.Sprintf("p%02d", i)
		names = append(names, name)
		schema.Tables = append(schema.Tables, &ir.Table{
			Name:    name,
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		})
		rows[name] = []ir.Row{{"id": int64(i)}, {"id": int64(i + 1000)}}
	}
	src := newBackupRecorderEngine("postgres", schema, rows)
	if err := (&Backup{Source: src, SourceDSN: "src", Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	tgt := newRestoreRecorderEngine("postgres")
	gotP, gotReason := observeRestoreDispatch(t)
	if err := (&Restore{
		Target:           tgt,
		TargetDSN:        "tgt",
		Store:            store,
		TableParallelism: 4,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	if *gotP != 4 {
		t.Errorf("dispatch parallelism = %d (reason %q); want 4", *gotP, *gotReason)
	}
	_, gotRows := tgt.snapshot()
	for _, name := range names {
		if len(gotRows[name]) != 2 {
			t.Errorf("table %s rows = %d; want 2", name, len(gotRows[name]))
		}
	}
}
