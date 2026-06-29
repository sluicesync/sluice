// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0141: migrate reparent-reconciliation. These tests pin the migrate
// analog of ADR-0113 — a PlanetScale storage-grow reparent silently
// under-copies (drops committed-but-unreplicated rows the reactive grow-gate
// cannot recover); the reconciliation phase must re-derive exactly the
// reparent-touched tables from the SOURCE so each matches it exactly, leaving
// untouched tables alone, converging on a clean pass, and surfacing loudly
// when a target reparents on every redo.
//
// The fakes drive the real Migrator.Run end-to-end through the MySQL fallback
// branch (the target schema writer is NOT an IncrementalIndexBuilder, so the
// non-overlapped runBulkCopyTablePool path runs — exactly where the
// reconciliation slots in). BulkParallelism / TableParallelism are pinned to 1
// so the copy is serial and deterministic (every table flushes through the one
// primary writer, the writer that carries the run-shared reparent observer).

// --- fake source engine (emits rows per table) ---

type reparentSourceEngine struct {
	name   string
	schema *ir.Schema
	rows   map[string][]ir.Row
}

func (e *reparentSourceEngine) Name() string                  { return e.name }
func (e *reparentSourceEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }

func (e *reparentSourceEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return &recordingSchemaReader{schema: e.schema}, nil
}

func (e *reparentSourceEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, errors.New("reparentSourceEngine: write side not used")
}

func (e *reparentSourceEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return &reparentSourceReader{rows: e.rows}, nil
}

func (e *reparentSourceEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, errors.New("reparentSourceEngine: write side not used")
}

func (*reparentSourceEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("not implemented")
}

func (*reparentSourceEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("not implemented")
}

func (*reparentSourceEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("not implemented")
}

// reparentSourceReader is a stateless (per-call) reader: each ReadRows
// re-emits the table's full row set, so the reconciliation redo re-reads the
// live source exactly the way a real migrate re-read would (ADR-0141's
// static-source replay).
type reparentSourceReader struct {
	rows map[string][]ir.Row
}

func (r *reparentSourceReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	ch := make(chan ir.Row)
	src := r.rows[table.Name]
	go func() {
		defer close(ch)
		for _, row := range src {
			select {
			case ch <- row:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

func (r *reparentSourceReader) Err() error { return nil }

// --- fake target engine (records writes + simulates reparent drops) ---

type reparentTargetEngine struct {
	name string
	mu   sync.Mutex

	got           map[string][]ir.Row // rows currently landed in the target
	writeCount    map[string]int      // WriteRows calls per table
	truncateCount map[string]int      // TruncateTable calls per table
	observe       func(table string)  // the run-shared reparent observer

	// dropOnce names tables that drop their last row + report a reparent on
	// their FIRST WriteRows (a one-shot grow-reparent silently under-copying);
	// the reconciliation redo recovers the full set.
	dropOnce map[string]bool
	dropped  map[string]bool
	// dropEvery names tables that drop + report on EVERY WriteRows (a target
	// reparenting on every serial redo) — the non-convergence / loud-bound case.
	dropEvery map[string]bool
}

func newReparentTargetEngine(name string) *reparentTargetEngine {
	return &reparentTargetEngine{
		name:          name,
		got:           map[string][]ir.Row{},
		writeCount:    map[string]int{},
		truncateCount: map[string]int{},
		dropOnce:      map[string]bool{},
		dropped:       map[string]bool{},
		dropEvery:     map[string]bool{},
	}
}

func (e *reparentTargetEngine) Name() string                  { return e.name }
func (e *reparentTargetEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }

func (e *reparentTargetEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return nil, errors.New("reparentTargetEngine: read side not used")
}

func (e *reparentTargetEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	// noopSchemaWriter (copy_concurrent_tables_test.go) deliberately does NOT
	// implement ir.IncrementalIndexBuilder, so Migrator.Run takes the MySQL
	// fallback branch — the path the reconciliation slots into.
	return noopSchemaWriter{}, nil
}

func (e *reparentTargetEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return nil, errors.New("reparentTargetEngine: read side not used")
}

func (e *reparentTargetEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return &reparentTargetWriter{engine: e}, nil
}

func (*reparentTargetEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("not implemented")
}

func (*reparentTargetEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("not implemented")
}

func (*reparentTargetEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("not implemented")
}

type reparentTargetWriter struct {
	engine *reparentTargetEngine
}

func (w *reparentTargetWriter) WriteRows(_ context.Context, table *ir.Table, rows <-chan ir.Row) error {
	got := make([]ir.Row, 0)
	for r := range rows {
		got = append(got, r)
	}

	e := w.engine
	e.mu.Lock()
	e.writeCount[table.Name]++
	drop := e.dropEvery[table.Name] || (e.dropOnce[table.Name] && !e.dropped[table.Name])
	if drop {
		e.dropped[table.Name] = true
		if len(got) > 0 {
			got = got[:len(got)-1] // the reparent-dropped (silently lost) row
		}
	}
	observe := e.observe
	e.got[table.Name] = append(e.got[table.Name], got...)
	e.mu.Unlock()

	// Report the reparent OUTSIDE the engine lock (the observer takes the
	// tracker's own mutex). Mirrors restoreRecordingRowWriter.
	if drop && observe != nil {
		observe(table.Name)
	}
	return nil
}

// SetReparentObserver implements [ir.ReparentObserverSetter] — the pipeline
// wires the run-shared observer here so the drop simulation can mark its table
// for reconciliation (ADR-0141).
func (w *reparentTargetWriter) SetReparentObserver(observe func(table string)) {
	w.engine.mu.Lock()
	defer w.engine.mu.Unlock()
	w.engine.observe = observe
}

// TruncateTable implements [ir.TableTruncator] — clears the table's landed
// rows so the reconciliation redo re-derives the full set into an empty target
// (the cold TRUNCATE+redo path).
func (w *reparentTargetWriter) TruncateTable(_ context.Context, table *ir.Table) error {
	e := w.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.got, table.Name)
	e.truncateCount[table.Name]++
	return nil
}

func reparentTestSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		{Name: "u", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
	}}
}

func reparentTestRows() map[string][]ir.Row {
	return map[string][]ir.Row{
		"t": {{"id": int64(1)}, {"id": int64(2)}, {"id": int64(3)}, {"id": int64(4)}, {"id": int64(5)}},
		"u": {{"id": int64(10)}, {"id": int64(11)}, {"id": int64(12)}},
	}
}

// runReparentMigrate runs a serial (parallelism 1) migrate from a row-emitting
// source into the supplied reparent-simulating target.
func runReparentMigrate(t *testing.T, tgt *reparentTargetEngine) error {
	t.Helper()
	src := &reparentSourceEngine{name: tgt.name, schema: reparentTestSchema(), rows: reparentTestRows()}
	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		// Same engine name on both sides → no cross-engine gate. Serial copy
		// → deterministic single-writer flush path.
		BulkParallelism:  1,
		TableParallelism: 1,
	}
	return m.Run(context.Background())
}

// TestMigrate_ReparentReconciliation_RecoversAndScopes pins (a) every touched
// table is TRUNCATEd then re-copied serially, (b) untouched tables are NOT
// re-copied, and (c) the loop converges and the migrate succeeds.
func TestMigrate_ReparentReconciliation_RecoversAndScopes(t *testing.T) {
	tgt := newReparentTargetEngine("fakemysql")
	tgt.dropOnce["t"] = true // table t loses a row to a one-shot reparent

	if err := runReparentMigrate(t, tgt); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgt.mu.Lock()
	defer tgt.mu.Unlock()

	// (a) touched table fully recovered to the source's 5 rows.
	if n := len(tgt.got["t"]); n != 5 {
		t.Errorf("table t after reconciliation: got %d rows, want 5 (reparent-dropped row not recovered — silent under-copy)", n)
	}
	if tgt.truncateCount["t"] != 1 {
		t.Errorf("table t truncates = %d; want exactly 1 (TRUNCATE before the serial redo)", tgt.truncateCount["t"])
	}
	if tgt.writeCount["t"] != 2 {
		t.Errorf("table t WriteRows = %d; want exactly 2 (initial copy + one reconciliation redo)", tgt.writeCount["t"])
	}

	// (b) untouched table is left alone: written once, never truncated.
	if n := len(tgt.got["u"]); n != 3 {
		t.Errorf("untouched table u: got %d rows, want 3", n)
	}
	if tgt.truncateCount["u"] != 0 {
		t.Errorf("untouched table u truncates = %d; want 0 (reconciliation must not touch unmarked tables)", tgt.truncateCount["u"])
	}
	if tgt.writeCount["u"] != 1 {
		t.Errorf("untouched table u WriteRows = %d; want exactly 1 (no redo)", tgt.writeCount["u"])
	}
}

// TestMigrate_ReparentReconciliation_NoTouchIsByteIdentical pins (e): with no
// reparent, the reconciliation phase is a zero-cost no-op — no TRUNCATE, no
// extra copy — so the run is byte-identical to the pre-ADR-0141 behaviour.
func TestMigrate_ReparentReconciliation_NoTouchIsByteIdentical(t *testing.T) {
	tgt := newReparentTargetEngine("fakemysql") // no drops configured

	if err := runReparentMigrate(t, tgt); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	tgt.mu.Lock()
	defer tgt.mu.Unlock()

	for _, tbl := range []string{"t", "u"} {
		if tgt.truncateCount[tbl] != 0 {
			t.Errorf("table %s truncates = %d; want 0 (no reparent ⇒ no reconciliation)", tbl, tgt.truncateCount[tbl])
		}
		if tgt.writeCount[tbl] != 1 {
			t.Errorf("table %s WriteRows = %d; want exactly 1 (single cold copy, no redo)", tbl, tgt.writeCount[tbl])
		}
	}
	if n := len(tgt.got["t"]); n != 5 {
		t.Errorf("table t: got %d rows, want 5", n)
	}
	if n := len(tgt.got["u"]); n != 3 {
		t.Errorf("table u: got %d rows, want 3", n)
	}
}

// TestMigrate_ReparentReconciliation_NonConvergenceFailsLoudly pins (d): a
// target that reparents on EVERY serial redo never converges, so the bounded
// loop surfaces a LOUD error naming the still-touched table rather than
// looping forever or exiting 0 with a short copy.
func TestMigrate_ReparentReconciliation_NonConvergenceFailsLoudly(t *testing.T) {
	tgt := newReparentTargetEngine("fakemysql")
	tgt.dropEvery["t"] = true // t reparents on every redo → never converges

	err := runReparentMigrate(t, tgt)
	if err == nil {
		t.Fatal("Migrator.Run: nil err; want a loud non-convergence failure")
	}
	es := err.Error()
	if !strings.Contains(es, "did not converge") {
		t.Errorf("err = %v; want a 'did not converge' reconciliation failure", err)
	}
	if !strings.Contains(es, `"t"`) && !strings.Contains(es, "[t]") {
		t.Errorf("err = %v; want the still-touched table t named", err)
	}
	// The bound is reconcileMaxRounds redos before giving up (one TRUNCATE per
	// round) — never an unbounded spin.
	tgt.mu.Lock()
	redos := tgt.truncateCount["t"]
	tgt.mu.Unlock()
	if redos != reconcileMaxRounds {
		t.Errorf("table t redos = %d; want exactly %d (the bounded round cap)", redos, reconcileMaxRounds)
	}
}

// TestReconcileMigrateReparentTouched_NilTrackerIsNoOp pins that the
// reconciliation entrypoint no-ops (and never type-asserts a TRUNCATE) when no
// tracker is constructed — the pre-ADR-0141 / non-migrate callers' path.
func TestReconcileMigrateReparentTouched_NilTrackerIsNoOp(t *testing.T) {
	schema := reparentTestSchema()
	// nil deps.
	if err := reconcileMigrateReparentTouched(context.Background(), schema, nil, nil, nil, nil, ShardColumnSpec{}); err != nil {
		t.Fatalf("nil parallel: got err %v; want nil no-op", err)
	}
	// non-nil deps but nil tracker.
	if err := reconcileMigrateReparentTouched(context.Background(), schema, nil, nil, &parallelBulkCopyDeps{}, nil, ShardColumnSpec{}); err != nil {
		t.Fatalf("nil tracker: got err %v; want nil no-op", err)
	}
}
