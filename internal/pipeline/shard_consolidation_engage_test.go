// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// supportingApplier is a minimal stub [ir.ChangeApplier] that ALSO
// implements [ir.ShardConsolidationLeaseStore] +
// [ir.ShardConsolidationProber] + [ir.ShardColumnSetter]. Used to
// drive engageShardCoordination without spinning up a real engine.
type supportingApplier struct {
	*fakeLeaseStore
	*fakeProber
	shardCol struct {
		name  string
		value any
	}
}

func newSupportingApplier() *supportingApplier {
	return &supportingApplier{
		fakeLeaseStore: newFakeLeaseStore(testClockNow),
		fakeProber:     &fakeProber{},
	}
}

func (a *supportingApplier) SetShardColumn(name string, value any) {
	a.shardCol.name = name
	a.shardCol.value = value
}

// Satisfy ir.ChangeApplier (unused — panicking on Apply).
func (*supportingApplier) EnsureControlTable(context.Context) error { return nil }

func (*supportingApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (*supportingApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) {
	return nil, nil
}

func (*supportingApplier) Apply(context.Context, string, <-chan ir.Change) error { return nil }
func (*supportingApplier) RequestStop(context.Context, string) error             { return nil }
func (*supportingApplier) ClearStopRequested(context.Context, string) error      { return nil }

// Bug 85 pin: supportingApplier also implements
// ShardConsolidationLeaseLister + ShardConsolidationLeaseDeleter +
// PositionOrderer so the GC wire-up at engagement time can be
// exercised. The fakeLeaseStore already carries the rows; this
// surface just lets the engagement type-assertions hit.
func (a *supportingApplier) ListLeases(context.Context) ([]ir.ShardConsolidationLeaseRow, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]ir.ShardConsolidationLeaseRow, 0, len(a.rows))
	for _, r := range a.rows {
		out = append(out, r)
	}
	return out, nil
}

func (a *supportingApplier) DeleteLease(_ context.Context, tableName string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.rows, tableName)
	return nil
}

// (Bug 85.b: supportingApplier does NOT implement PositionAtOrAfter
// any more — the orderer is type-asserted on s.Source in production,
// not on the applier. Stubbing it here previously masked the v0.76.0
// production gap that v0.77.0 then re-hid. The orderer interface is
// satisfied at the engine level by stubNamedEngine.)

// nonSupportingApplier implements ir.ChangeApplier but NOT
// ShardConsolidationLeaseStore — exercises the loud-refusal path.
type nonSupportingApplier struct{}

func (nonSupportingApplier) EnsureControlTable(context.Context) error { return nil }
func (nonSupportingApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (nonSupportingApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) {
	return nil, nil
}
func (nonSupportingApplier) Apply(context.Context, string, <-chan ir.Change) error { return nil }
func (nonSupportingApplier) RequestStop(context.Context, string) error             { return nil }
func (nonSupportingApplier) ClearStopRequested(context.Context, string) error      { return nil }

// stubNamedEngine names itself for the refusal message. OpenSchemaWriter
// returns a fakeShapeApplier-backed SchemaWriter so the engagement's
// shape-writer probe succeeds on the supporting-applier path.
type stubNamedEngine struct {
	name string
}

func (e stubNamedEngine) Name() string                  { return e.name }
func (e stubNamedEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }
func (e stubNamedEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return nil, errors.New("not implemented")
}

func (e stubNamedEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return &fakeSchemaWriter{}, nil
}

func (e stubNamedEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return nil, errors.New("not implemented")
}

func (e stubNamedEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, errors.New("not implemented")
}

func (e stubNamedEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("not implemented")
}

func (e stubNamedEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("not implemented")
}

func (e stubNamedEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("not implemented")
}

// Bug 85.b pin: stubNamedEngine implements ir.PositionOrderer so the
// engagement's `s.Source.(ir.PositionOrderer)` assertion succeeds in
// tests. v0.77.0's first fix wrongly type-asserted the orderer on
// the applier; the corrected fix asserts on s.Source (the engine,
// where PositionAtOrAfter actually lives).
func (e stubNamedEngine) PositionAtOrAfter(_, _ ir.Position) (bool, error) {
	return false, nil
}

// fakeSchemaWriter satisfies ir.SchemaWriter + ir.ShapeDeltaApplier
// for engagement tests. The schema-writer methods are no-ops; the
// shape-applier methods delegate to an embedded fakeShapeApplier.
type fakeSchemaWriter struct {
	fakeShapeApplier
}

func (*fakeSchemaWriter) CreateTablesWithoutConstraints(context.Context, *ir.Schema) error {
	return nil
}
func (*fakeSchemaWriter) CreateIndexes(context.Context, *ir.Schema) error     { return nil }
func (*fakeSchemaWriter) CreateConstraints(context.Context, *ir.Schema) error { return nil }
func (*fakeSchemaWriter) SyncIdentitySequences(context.Context, *ir.Schema) error {
	return nil
}
func (*fakeSchemaWriter) CreateViews(context.Context, *ir.Schema) error { return nil }
func (*fakeSchemaWriter) Close() error                                  { return nil }

func TestEngage_NoopWhenNotShapeA(t *testing.T) {
	t.Parallel()
	s := &Streamer{
		StreamID:          "stream-a",
		CoordinateLiveDDL: true,
		Target:            stubNamedEngine{name: "stub"},
	}
	if err := s.engageShardCoordination(context.Background(), newSupportingApplier()); err != nil {
		t.Fatalf("engageShardCoordination: %v", err)
	}
	if s.ShardConsolidationLeaseManager() != nil {
		t.Error("expected lease manager nil when Shape A not engaged")
	}
}

func TestEngage_NoopWhenOptOut(t *testing.T) {
	t.Parallel()
	s := &Streamer{
		StreamID:          "stream-a",
		InjectShardColumn: ShardColumnSpec{Name: "source_shard_id", Value: "us-east-1"},
		CoordinateLiveDDL: false,
		Target:            stubNamedEngine{name: "stub"},
	}
	if err := s.engageShardCoordination(context.Background(), newSupportingApplier()); err != nil {
		t.Fatalf("engageShardCoordination: %v", err)
	}
	if s.ShardConsolidationLeaseManager() != nil {
		t.Error("expected lease manager nil when --no-coordinate-live-ddl is set")
	}
}

func TestEngage_RefusesNonSupportingEngine(t *testing.T) {
	t.Parallel()
	s := &Streamer{
		StreamID:          "stream-a",
		Target:            stubNamedEngine{name: "fictitious"},
		InjectShardColumn: ShardColumnSpec{Name: "source_shard_id", Value: "us-east-1"},
		CoordinateLiveDDL: true,
	}
	err := s.engageShardCoordination(context.Background(), nonSupportingApplier{})
	if err == nil {
		t.Fatal("expected refusal when target engine doesn't implement the lease store; got nil")
	}
	if !strings.Contains(err.Error(), "fictitious") {
		t.Errorf("refusal message should name the engine; got: %v", err)
	}
	if !strings.Contains(err.Error(), "--no-coordinate-live-ddl") {
		t.Errorf("refusal message should name the opt-out flag; got: %v", err)
	}
}

func TestEngage_ConstructsManagerWhenSupported(t *testing.T) {
	t.Parallel()
	s := &Streamer{
		StreamID:          "stream-a",
		InjectShardColumn: ShardColumnSpec{Name: "source_shard_id", Value: "us-east-1"},
		CoordinateLiveDDL: true,
		Target:            stubNamedEngine{name: "stub"},
	}
	if err := s.engageShardCoordination(context.Background(), newSupportingApplier()); err != nil {
		t.Fatalf("engageShardCoordination: %v", err)
	}
	mgr := s.ShardConsolidationLeaseManager()
	if mgr == nil {
		t.Fatal("expected lease manager")
	}
	if mgr.streamID != "stream-a" {
		t.Errorf("manager.streamID = %q, want %q", mgr.streamID, "stream-a")
	}
	if s.ShardConsolidationBoundaryRouter() == nil {
		t.Error("expected boundary router to be constructed when engagement succeeds")
	}
	s.closeShardCoordination()
	if s.ShardConsolidationLeaseManager() != nil || s.ShardConsolidationBoundaryRouter() != nil {
		t.Error("closeShardCoordination should clear lease manager + boundary router")
	}
}

func TestEngage_DefaultsZeroLeaseConfig(t *testing.T) {
	t.Parallel()
	s := &Streamer{
		StreamID:          "stream-a",
		InjectShardColumn: ShardColumnSpec{Name: "source_shard_id", Value: "us-east-1"},
		CoordinateLiveDDL: true,
		Target:            stubNamedEngine{name: "stub"},
	}
	if err := s.engageShardCoordination(context.Background(), newSupportingApplier()); err != nil {
		t.Fatalf("engageShardCoordination: %v", err)
	}
	mgr := s.ShardConsolidationLeaseManager()
	if mgr == nil {
		t.Fatal("expected manager")
	}
	if mgr.cfg.LeaseDuration != DefaultLeaseDuration {
		t.Errorf("LeaseDuration = %v, want default %v", mgr.cfg.LeaseDuration, DefaultLeaseDuration)
	}
	if mgr.cfg.RenewDeadline != DefaultRenewDeadline {
		t.Errorf("RenewDeadline = %v, want default %v", mgr.cfg.RenewDeadline, DefaultRenewDeadline)
	}
	if mgr.cfg.RetryPeriod != DefaultRetryPeriod {
		t.Errorf("RetryPeriod = %v, want default %v", mgr.cfg.RetryPeriod, DefaultRetryPeriod)
	}
}

// TestEngage_WiresGCDepsWhenAllSurfacesPresent pins Bug 85 (v0.76.0
// shipped #21's GC sweep but missed the WithGC wire-up in
// engageShardCoordination). When the applier implements lister +
// deleter AND the source engine implements PositionOrderer (Bug 85.b
// — v0.77.0's first fix wrongly checked the applier for the orderer),
// engagement must populate the LeaseManager's gcDeps so the heartbeat
// loop's GC-trigger guard sees non-nil deps.
func TestEngage_WiresGCDepsWhenAllSurfacesPresent(t *testing.T) {
	t.Parallel()
	s := &Streamer{
		StreamID:          "stream-a",
		InjectShardColumn: ShardColumnSpec{Name: "source_shard_id", Value: "us-east-1"},
		CoordinateLiveDDL: true,
		Source:            stubNamedEngine{name: "src-stub"},
		Target:            stubNamedEngine{name: "stub"},
	}
	if err := s.engageShardCoordination(context.Background(), newSupportingApplier()); err != nil {
		t.Fatalf("engageShardCoordination: %v", err)
	}
	mgr := s.ShardConsolidationLeaseManager()
	if mgr == nil {
		t.Fatal("expected lease manager")
	}
	if mgr.gcDeps == nil {
		t.Fatal("Bug 85.b: gcDeps must be non-nil when applier supports lister+deleter AND s.Source supports orderer; v0.77.0's first fix wrongly checked the applier for the orderer")
	}
	if mgr.gcDeps.Lister == nil {
		t.Error("gcDeps.Lister nil")
	}
	if mgr.gcDeps.Deleter == nil {
		t.Error("gcDeps.Deleter nil")
	}
	if mgr.gcDeps.PosReader == nil {
		t.Error("gcDeps.PosReader nil")
	}
	if mgr.gcDeps.Orderer == nil {
		t.Error("gcDeps.Orderer nil")
	}
}

// TestEngage_NoGCWhenSourceLacksOrderer is the Bug 85.b regression
// guard: if s.Source doesn't implement PositionOrderer (e.g. a future
// engine that doesn't ship one), engagement should inherit the no-GC
// default rather than crashing or wiring a nil orderer.
func TestEngage_NoGCWhenSourceLacksOrderer(t *testing.T) {
	t.Parallel()
	s := &Streamer{
		StreamID:          "stream-a",
		InjectShardColumn: ShardColumnSpec{Name: "source_shard_id", Value: "us-east-1"},
		CoordinateLiveDDL: true,
		Source:            engineWithoutOrderer{name: "no-orderer-src"},
		Target:            stubNamedEngine{name: "stub"},
	}
	if err := s.engageShardCoordination(context.Background(), newSupportingApplier()); err != nil {
		t.Fatalf("engageShardCoordination: %v", err)
	}
	mgr := s.ShardConsolidationLeaseManager()
	if mgr == nil {
		t.Fatal("expected lease manager")
	}
	if mgr.gcDeps != nil {
		t.Error("gcDeps should be nil when s.Source lacks PositionOrderer (no-GC default)")
	}
}

// engineWithoutOrderer satisfies ir.Engine but NOT ir.PositionOrderer
// (it doesn't define PositionAtOrAfter). Used by the Bug 85.b no-GC-
// default pin.
type engineWithoutOrderer struct {
	name string
}

func (e engineWithoutOrderer) Name() string                  { return e.name }
func (e engineWithoutOrderer) Capabilities() ir.Capabilities { return ir.Capabilities{} }
func (e engineWithoutOrderer) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return nil, errors.New("not implemented")
}

func (e engineWithoutOrderer) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return &fakeSchemaWriter{}, nil
}

func (e engineWithoutOrderer) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return nil, errors.New("not implemented")
}

func (e engineWithoutOrderer) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, errors.New("not implemented")
}

func (e engineWithoutOrderer) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("not implemented")
}

func (e engineWithoutOrderer) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("not implemented")
}

func (e engineWithoutOrderer) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("not implemented")
}

// TestEngage_InheritsNoGCDefaultWhenSurfacesMissing — the no-GC
// default when the applier implements the lease store + prober (so
// engagement succeeds) but NOT the deleter/orderer surfaces.
// supportingApplier-without-deleter is enough to exercise this since
// adding lease store but not the deleter is realistic for older engines.
type leaseStoreOnlyApplier struct {
	*fakeLeaseStore
	*fakeProber
}

func (*leaseStoreOnlyApplier) EnsureControlTable(context.Context) error { return nil }
func (*leaseStoreOnlyApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (*leaseStoreOnlyApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) {
	return nil, nil
}

func (*leaseStoreOnlyApplier) Apply(context.Context, string, <-chan ir.Change) error { return nil }
func (*leaseStoreOnlyApplier) RequestStop(context.Context, string) error             { return nil }
func (*leaseStoreOnlyApplier) ClearStopRequested(context.Context, string) error      { return nil }

func TestEngage_InheritsNoGCDefaultWhenSurfacesMissing(t *testing.T) {
	t.Parallel()
	s := &Streamer{
		StreamID:          "stream-a",
		InjectShardColumn: ShardColumnSpec{Name: "source_shard_id", Value: "us-east-1"},
		CoordinateLiveDDL: true,
		Target:            stubNamedEngine{name: "stub"},
	}
	applier := &leaseStoreOnlyApplier{
		fakeLeaseStore: newFakeLeaseStore(testClockNow),
		fakeProber:     &fakeProber{},
	}
	if err := s.engageShardCoordination(context.Background(), applier); err != nil {
		t.Fatalf("engageShardCoordination: %v", err)
	}
	mgr := s.ShardConsolidationLeaseManager()
	if mgr == nil {
		t.Fatal("expected lease manager")
	}
	if mgr.gcDeps != nil {
		t.Error("gcDeps should be nil when applier doesn't implement deleter/orderer (no-GC default)")
	}
}
