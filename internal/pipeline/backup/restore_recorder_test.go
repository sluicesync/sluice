// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"errors"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
)

// restoreRecorderEngine is a fake [ir.Engine] for restore tests: a
// schema writer that records phase calls and a row writer that
// captures all written rows by table.
type restoreRecorderEngine struct {
	name string
	mu   sync.Mutex

	// Schema-write calls in order — for asserting phase ordering.
	phases []string
	// Per-table rows recorded by the row writer.
	rows map[string][]ir.Row
	// growGateSets counts SetGrowGate calls with a NON-nil gate across all
	// row writers this engine handed out — pins the ADR-0110 grow-gate
	// wiring into the restore path (the Track-C silent-under-copy fix).
	growGateSets int

	// indexFallback records the value threaded through the optional
	// [ir.IndexBuildFallbackSetter] surface (ADR-0148 / audit MED-A1) —
	// pins that restore arms the deploy-request fallback on the schema
	// writer BEFORE the index phase, and never touches the setter unarmed.
	indexFallback     ir.IndexBuildFallback
	indexFallbackSets int

	// --- ADR-0113 reparent-reconciliation simulation ---
	// dropTable (when non-empty) names a table whose FIRST WriteRows drops
	// its last row AND reports the table reparent-touched (via the wired
	// observer) — modelling PlanetScale's grow-reparent dropping a
	// committed-but-unreplicated row. The reconciliation phase must then
	// TRUNCATE + redo the table and recover the full set. Empty ⇒ no drop
	// (every other test is unaffected).
	dropTable       string
	dropped         map[string]bool
	reparentObserve func(table string)
	reconcileRedos  int // counts truncate+redo reapplies for assertions
}

func newRestoreRecorderEngine(name string) *restoreRecorderEngine {
	return &restoreRecorderEngine{name: name, rows: map[string][]ir.Row{}, dropped: map[string]bool{}}
}

func (e *restoreRecorderEngine) Name() string                  { return e.name }
func (e *restoreRecorderEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }

func (e *restoreRecorderEngine) OpenSchemaReader(_ context.Context, _ string) (ir.SchemaReader, error) {
	return nil, errors.New("restoreRecorderEngine: read side not used")
}

func (e *restoreRecorderEngine) OpenSchemaWriter(_ context.Context, _ string) (ir.SchemaWriter, error) {
	return &restoreRecordingSchemaWriter{engine: e}, nil
}

func (e *restoreRecorderEngine) OpenRowReader(_ context.Context, _ string) (ir.RowReader, error) {
	return nil, errors.New("restoreRecorderEngine: read side not used")
}

func (e *restoreRecorderEngine) OpenRowWriter(_ context.Context, _ string) (ir.RowWriter, error) {
	return &restoreRecordingRowWriter{engine: e}, nil
}

func (*restoreRecorderEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("not implemented")
}

func (*restoreRecorderEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("not implemented")
}

func (*restoreRecorderEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("not implemented")
}

func (e *restoreRecorderEngine) recordPhase(name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.phases = append(e.phases, name)
}

func (e *restoreRecorderEngine) recordRow(table string, row ir.Row) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rows[table] = append(e.rows[table], row)
}

func (e *restoreRecorderEngine) snapshot() (phases []string, rows map[string][]ir.Row) {
	e.mu.Lock()
	defer e.mu.Unlock()
	phases = append(phases, e.phases...)
	rows = make(map[string][]ir.Row, len(e.rows))
	for k, v := range e.rows {
		rows[k] = append(rows[k], v...)
	}
	return phases, rows
}

type restoreRecordingSchemaWriter struct {
	engine *restoreRecorderEngine
}

func (w *restoreRecordingSchemaWriter) CreateTablesWithoutConstraints(context.Context, *ir.Schema) error {
	w.engine.recordPhase("CreateTablesWithoutConstraints")
	return nil
}

func (w *restoreRecordingSchemaWriter) CreateIndexes(context.Context, *ir.Schema) error {
	w.engine.recordPhase("CreateIndexes")
	return nil
}

func (w *restoreRecordingSchemaWriter) CreateConstraints(context.Context, *ir.Schema) error {
	w.engine.recordPhase("CreateConstraints")
	return nil
}

func (w *restoreRecordingSchemaWriter) SyncIdentitySequences(context.Context, *ir.Schema) error {
	w.engine.recordPhase("SyncIdentitySequences")
	return nil
}

func (w *restoreRecordingSchemaWriter) CreateViews(context.Context, *ir.Schema) error {
	w.engine.recordPhase("CreateViews")
	return nil
}

// SetIndexBuildFallback implements [ir.IndexBuildFallbackSetter] so the
// recorder can pin the ADR-0148 threading (audit MED-A1): the restore
// orchestrator must arm the writer before CreateIndexes runs, and must
// not call the setter at all when unarmed (nil skips in the helper).
func (w *restoreRecordingSchemaWriter) SetIndexBuildFallback(f ir.IndexBuildFallback) {
	w.engine.mu.Lock()
	w.engine.indexFallback = f
	w.engine.indexFallbackSets++
	w.engine.mu.Unlock()
	w.engine.recordPhase("SetIndexBuildFallback")
}

type restoreRecordingRowWriter struct {
	engine *restoreRecorderEngine
}

func (w *restoreRecordingRowWriter) WriteRows(_ context.Context, table *ir.Table, rows <-chan ir.Row) error {
	got := make([]ir.Row, 0)
	for r := range rows {
		got = append(got, r)
	}

	// ADR-0113 simulation: on the FIRST write of the configured drop-table,
	// drop the last row and report a reparent — modelling a grow-reparent
	// silently dropping a committed-but-unreplicated row. The reconciliation
	// phase must TRUNCATE + redo and recover it.
	w.engine.mu.Lock()
	if w.engine.dropTable != "" && table.Name == w.engine.dropTable && !w.engine.dropped[table.Name] {
		w.engine.dropped[table.Name] = true
		observe := w.engine.reparentObserve
		w.engine.mu.Unlock()
		if len(got) > 0 {
			got = got[:len(got)-1] // the dropped (lost) row
		}
		if observe != nil {
			observe(table.Name) // mark reparent-touched
		}
		w.engine.mu.Lock()
	}
	w.engine.rows[table.Name] = append(w.engine.rows[table.Name], got...)
	w.engine.mu.Unlock()

	w.engine.recordPhase("WriteRows:" + table.Name)
	return nil
}

// SetReparentObserver implements [ir.ReparentObserverSetter] — the engine
// stores the latest observer so the drop-table's simulated reparent can
// report itself for reconciliation.
func (w *restoreRecordingRowWriter) SetReparentObserver(observe func(table string)) {
	w.engine.mu.Lock()
	defer w.engine.mu.Unlock()
	w.engine.reparentObserve = observe
}

// TruncateTable implements [ir.TableTruncator] — clears the recorded rows
// for the table so the reconciliation redo re-derives the full set into an
// empty table (the cold-restore TRUNCATE+redo path).
func (w *restoreRecordingRowWriter) TruncateTable(_ context.Context, table *ir.Table) error {
	w.engine.mu.Lock()
	defer w.engine.mu.Unlock()
	delete(w.engine.rows, table.Name)
	w.engine.reconcileRedos++
	return nil
}

// SetGrowGate implements [ir.GrowGateSetter] so the recorder can pin that
// restore wires the ADR-0110 coordinated grow-gate onto every writer it
// opens. A non-nil gate increments the engine's counter.
func (w *restoreRecordingRowWriter) SetGrowGate(gate ir.GrowGate) {
	if gate == nil {
		return
	}
	w.engine.mu.Lock()
	defer w.engine.mu.Unlock()
	w.engine.growGateSets++
}
