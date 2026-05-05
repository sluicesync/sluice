package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// stubDroppingWriter is a fake [ir.RowWriter] + [ir.TableDropper] for
// reset-path unit tests. Records each DropTable call.
type stubDroppingWriter struct {
	dropped []string
	dropErr error
}

func (s *stubDroppingWriter) WriteRows(_ context.Context, _ *ir.Table, _ <-chan ir.Row) error {
	return errors.New("stubDroppingWriter.WriteRows should not be called by reset")
}

func (s *stubDroppingWriter) DropTable(_ context.Context, table *ir.Table) error {
	s.dropped = append(s.dropped, table.Name)
	return s.dropErr
}

// stubWriterNoDropper is a RowWriter that intentionally does NOT
// implement TableDropper, so the reset path errors clearly.
type stubWriterNoDropper struct{}

func (stubWriterNoDropper) WriteRows(_ context.Context, _ *ir.Table, _ <-chan ir.Row) error {
	return nil
}

// stubStreamCleaner combines ChangeApplier with the optional
// StreamCleaner surface.
type stubStreamCleaner struct {
	stubChangeApplier
	cleared    []string
	clearErr   error
	clearCalls int
}

func (s *stubStreamCleaner) ClearStream(_ context.Context, streamID string) error {
	s.clearCalls++
	if s.clearErr != nil {
		return s.clearErr
	}
	s.cleared = append(s.cleared, streamID)
	return nil
}

// stubChangeApplier is a no-op ir.ChangeApplier (panics on calls; reset
// shouldn't reach Apply).
type stubChangeApplier struct{}

func (stubChangeApplier) EnsureControlTable(context.Context) error { return nil }
func (stubChangeApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (stubChangeApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) { return nil, nil }
func (stubChangeApplier) Apply(context.Context, string, <-chan ir.Change) error  { return nil }
func (stubChangeApplier) RequestStop(context.Context, string) error              { return nil }
func (stubChangeApplier) ClearStopRequested(context.Context, string) error       { return nil }

// TestResetTargetData_DropsAllTables verifies the reset clears the
// migrate-state row and drops every table in the schema, in order.
func TestResetTargetData_DropsAllTables(t *testing.T) {
	captureSlog(t)
	schema := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "users"}, {Name: "orders"}, {Name: "comments"},
		},
	}
	rw := &stubDroppingWriter{}
	store := newFakeStateStore()
	store.rows["m1"] = ir.MigrationState{MigrationID: "m1", Phase: ir.MigrationPhaseFailed}

	if err := resetTargetData(context.Background(), schema, rw, store, "m1"); err != nil {
		t.Fatalf("resetTargetData: %v", err)
	}
	if got := rw.dropped; len(got) != 3 || got[0] != "users" || got[1] != "orders" || got[2] != "comments" {
		t.Errorf("dropped order = %v; want [users orders comments]", got)
	}
	if _, ok := store.get("m1"); ok {
		t.Errorf("migrate-state row not cleared")
	}
}

// TestResetTargetData_NoDropperSurfaceErrors verifies an engine whose
// row writer doesn't implement TableDropper surfaces a clear refusal
// without touching the store.
func TestResetTargetData_NoDropperSurfaceErrors(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	store := newFakeStateStore()

	err := resetTargetData(context.Background(), schema, stubWriterNoDropper{}, store, "m1")
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	if !strings.Contains(err.Error(), "does not support DROP TABLE") {
		t.Errorf("err = %v; want 'does not support DROP TABLE' wording", err)
	}
	if store.writes != 0 {
		t.Errorf("store written despite refusal: %d writes", store.writes)
	}
}

// TestResetTargetData_DropErrorPropagates verifies a DropTable error
// surfaces with the table name attached.
func TestResetTargetData_DropErrorPropagates(t *testing.T) {
	captureSlog(t)
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	rw := &stubDroppingWriter{dropErr: errors.New("permission denied")}
	store := newFakeStateStore()

	err := resetTargetData(context.Background(), schema, rw, store, "m1")
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	if !strings.Contains(err.Error(), `"users"`) {
		t.Errorf("err = %v; want users in message", err)
	}
	if !errors.Is(err, rw.dropErr) {
		t.Errorf("drop error not wrapped: %v", err)
	}
}

// TestResetTargetDataForStream_DropsAllAndClearsRow verifies the
// streamer-side reset clears the cdc-state row and drops every table.
func TestResetTargetDataForStream_DropsAllAndClearsRow(t *testing.T) {
	captureSlog(t)
	schema := &ir.Schema{
		Tables: []*ir.Table{{Name: "users"}, {Name: "orders"}},
	}
	rw := &stubDroppingWriter{}
	applier := &stubStreamCleaner{}

	if err := resetTargetDataForStream(context.Background(), schema, rw, applier, "stream-1"); err != nil {
		t.Fatalf("resetTargetDataForStream: %v", err)
	}
	if applier.clearCalls != 1 {
		t.Errorf("clear calls = %d; want 1", applier.clearCalls)
	}
	if len(rw.dropped) != 2 {
		t.Errorf("dropped = %v; want 2 tables", rw.dropped)
	}
}

// TestResetTargetDataForStream_NoCleanerSurfaceErrors verifies a
// ChangeApplier without StreamCleaner surfaces a clear refusal.
func TestResetTargetDataForStream_NoCleanerSurfaceErrors(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	rw := &stubDroppingWriter{}

	err := resetTargetDataForStream(context.Background(), schema, rw, stubChangeApplier{}, "stream-1")
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	if !strings.Contains(err.Error(), "clearing the cdc-state row") {
		t.Errorf("err = %v; want 'clearing the cdc-state row' wording", err)
	}
}

// stubBulkDroppingWriter implements both [ir.TableDropper] and
// [ir.BulkTableDropper]. Records the bulk dispatch so the test asserts
// the optional surface is preferred over per-table calls.
type stubBulkDroppingWriter struct {
	stubDroppingWriter
	bulkCalls   int
	bulkBatches [][]string
}

func (s *stubBulkDroppingWriter) DropTables(_ context.Context, tables []*ir.Table) error {
	s.bulkCalls++
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		names = append(names, t.Name)
	}
	s.bulkBatches = append(s.bulkBatches, names)
	return nil
}

// TestResetTargetData_PrefersBulkDropper confirms that when the row
// writer implements [ir.BulkTableDropper], the reset path uses the
// single-statement bulk DROP rather than calling DropTable per table.
// The recovery flow on a 500-table source pays one round-trip instead
// of 500.
func TestResetTargetData_PrefersBulkDropper(t *testing.T) {
	captureSlog(t)
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "a"}, {Name: "b"}, {Name: "c"}}}
	rw := &stubBulkDroppingWriter{}
	store := newFakeStateStore()

	if err := resetTargetData(context.Background(), schema, rw, store, "m1"); err != nil {
		t.Fatalf("resetTargetData: %v", err)
	}
	if rw.bulkCalls != 1 {
		t.Errorf("bulk calls = %d; want 1", rw.bulkCalls)
	}
	if len(rw.dropped) != 0 {
		t.Errorf("DropTable was called with %v; bulk surface should have absorbed all drops", rw.dropped)
	}
	if len(rw.bulkBatches) != 1 || len(rw.bulkBatches[0]) != 3 {
		t.Errorf("bulk batches = %v; want one batch of 3 tables", rw.bulkBatches)
	}
}
