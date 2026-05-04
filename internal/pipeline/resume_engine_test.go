package pipeline

import (
	"context"

	"github.com/orware/sluice/internal/ir"
)

// recordingEngineWithStore wraps recordingEngine with the
// optional-interface implementations the resume tests need:
//
//   - ir.MigrationStateStoreOpener so the orchestrator finds a state
//     store.
//   - ir.TableTruncator on the row writer so resume's truncate-and-
//     redo path has somewhere to dispatch.
//
// We keep the original recordingEngine free of these implementations
// so the pre-existing tests still exercise the "engine doesn't
// support resumable migrations" code path naturally.
type recordingEngineWithStore struct {
	*recordingEngine
	store *fakeStateStore
}

func newRecordingEngineWithStore(name string) *recordingEngineWithStore {
	return &recordingEngineWithStore{
		recordingEngine: newRecordingEngine(name),
		store:           newFakeStateStore(),
	}
}

// OpenMigrationStateStore implements [ir.MigrationStateStoreOpener].
// The store is shared across the engine and the test so assertions
// can inspect the rows it persisted after Migrator.Run.
func (e *recordingEngineWithStore) OpenMigrationStateStore(_ context.Context, _ string) (ir.MigrationStateStore, error) {
	return e.store, nil
}

// OpenRowWriter overrides the embedded recordingEngine to return a
// truncate-aware writer. The returned writer still appends WriteRows
// entries to the same phaseLog so the existing assertion shape
// holds.
func (e *recordingEngineWithStore) OpenRowWriter(_ context.Context, _ string) (ir.RowWriter, error) {
	e.openRowWriterCalls++
	return &recordingTruncatingRowWriter{phaseLog: &e.phaseLog}, nil
}

// recordingTruncatingRowWriter is a recording writer that also
// implements [ir.TableTruncator]. Each TruncateTable call appends
// "TruncateTable:<table>" to the shared phase log so the test's
// expected-phase list can include the truncation step explicitly.
type recordingTruncatingRowWriter struct {
	phaseLog *[]string
}

func (w *recordingTruncatingRowWriter) WriteRows(_ context.Context, table *ir.Table, _ <-chan ir.Row) error {
	*w.phaseLog = append(*w.phaseLog, "WriteRows:"+table.Name)
	return nil
}

func (w *recordingTruncatingRowWriter) TruncateTable(_ context.Context, table *ir.Table) error {
	*w.phaseLog = append(*w.phaseLog, "TruncateTable:"+table.Name)
	return nil
}
