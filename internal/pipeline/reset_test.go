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

// stubTypeDroppingWriter combines [ir.RowWriter] + [ir.TableDropper]
// + [ir.SchemaTypeDropper]. Records each invocation so the reset path
// tests can assert ordering: tables drop first, then types.
type stubTypeDroppingWriter struct {
	stubDroppingWriter
	typeCalls   int
	typeSchemas []*ir.Schema
	typeErr     error
	callOrder   []string // "drop-table:<name>" / "drop-types"
	recordOrder bool
}

func (s *stubTypeDroppingWriter) DropTable(ctx context.Context, table *ir.Table) error {
	if s.recordOrder {
		s.callOrder = append(s.callOrder, "drop-table:"+table.Name)
	}
	return s.stubDroppingWriter.DropTable(ctx, table)
}

func (s *stubTypeDroppingWriter) DropSchemaTypes(_ context.Context, schema *ir.Schema) error {
	s.typeCalls++
	s.typeSchemas = append(s.typeSchemas, schema)
	if s.recordOrder {
		s.callOrder = append(s.callOrder, "drop-types")
	}
	return s.typeErr
}

// TestResetTargetData_DropsSchemaTypesAfterTables verifies that when
// the row writer implements [ir.SchemaTypeDropper] (PG case), the
// reset path drops schema-defined types AFTER the table drops have
// completed. This is Bug 18: orphan enum types from a partial
// cold-start were left behind by previous resets, causing the next
// CREATE TYPE to fail with "type X already exists".
func TestResetTargetData_DropsSchemaTypesAfterTables(t *testing.T) {
	captureSlog(t)
	schema := &ir.Schema{
		Tables: []*ir.Table{
			{Name: "users", Columns: []*ir.Column{
				{Name: "role", Type: ir.Enum{Values: []string{"admin", "user"}}},
			}},
			{Name: "orders"},
		},
	}
	rw := &stubTypeDroppingWriter{recordOrder: true}
	store := newFakeStateStore()

	if err := resetTargetData(context.Background(), schema, rw, store, "m1"); err != nil {
		t.Fatalf("resetTargetData: %v", err)
	}
	if rw.typeCalls != 1 {
		t.Errorf("DropSchemaTypes calls = %d; want 1", rw.typeCalls)
	}
	if len(rw.typeSchemas) != 1 || rw.typeSchemas[0] != schema {
		t.Errorf("DropSchemaTypes schema = %v; want %v", rw.typeSchemas, schema)
	}
	// Order: every table drop must precede the type drop.
	want := []string{"drop-table:users", "drop-table:orders", "drop-types"}
	if len(rw.callOrder) != len(want) {
		t.Fatalf("call order = %v; want %v", rw.callOrder, want)
	}
	for i, got := range rw.callOrder {
		if got != want[i] {
			t.Errorf("call[%d] = %q; want %q", i, got, want[i])
		}
	}
}

// TestResetTargetData_TypeDropErrorPropagates verifies that errors
// from DropSchemaTypes surface with the reset-action prefix and are
// wrapped so callers can errors.Is against the underlying cause.
func TestResetTargetData_TypeDropErrorPropagates(t *testing.T) {
	captureSlog(t)
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	rw := &stubTypeDroppingWriter{typeErr: errors.New("permission denied on pg_type")}
	store := newFakeStateStore()

	err := resetTargetData(context.Background(), schema, rw, store, "m1")
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	if !strings.Contains(err.Error(), "drop schema types") {
		t.Errorf("err = %v; want 'drop schema types' wording", err)
	}
	if !errors.Is(err, rw.typeErr) {
		t.Errorf("type-drop error not wrapped: %v", err)
	}
}

// TestResetTargetData_NoTypeDropperIsNoOp verifies the reset path is
// happy when the row writer doesn't implement [ir.SchemaTypeDropper]
// (MySQL case): no error, no panic, table drops still run.
func TestResetTargetData_NoTypeDropperIsNoOp(t *testing.T) {
	captureSlog(t)
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	rw := &stubDroppingWriter{} // no SchemaTypeDropper
	store := newFakeStateStore()

	if err := resetTargetData(context.Background(), schema, rw, store, "m1"); err != nil {
		t.Fatalf("resetTargetData: %v", err)
	}
	if len(rw.dropped) != 1 || rw.dropped[0] != "users" {
		t.Errorf("dropped = %v; want [users]", rw.dropped)
	}
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
