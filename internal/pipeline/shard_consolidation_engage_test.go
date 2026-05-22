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

// supportingApplier is a minimal stub [ir.ChangeApplier] that also
// implements [ir.ShardConsolidationLeaseStore] and
// [ir.ShardColumnSetter]. It is used to drive engageShardCoordination
// without spinning up a real engine.
type supportingApplier struct {
	*fakeLeaseStore
	shardCol struct {
		name  string
		value any
	}
}

func newSupportingApplier() *supportingApplier {
	return &supportingApplier{fakeLeaseStore: newFakeLeaseStore(testClockNow)}
}

func (a *supportingApplier) SetShardColumn(name string, value any) {
	a.shardCol.name = name
	a.shardCol.value = value
}

// Satisfy ir.ChangeApplier (the unused methods panic — the tests in
// this file never exercise apply / status / read).
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

// stubEngine declares its name so the refusal message includes it.
type stubNamedEngine struct {
	name string
}

func (e stubNamedEngine) Name() string                  { return e.name }
func (e stubNamedEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }
func (e stubNamedEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return nil, errors.New("not implemented")
}

func (e stubNamedEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, errors.New("not implemented")
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

func TestEngage_NoopWhenNotShapeA(t *testing.T) {
	t.Parallel()
	s := &Streamer{
		StreamID:          "stream-a",
		CoordinateLiveDDL: true,
		// InjectShardColumn left zero ⇒ not engaged.
	}
	if err := s.engageShardCoordination(newSupportingApplier()); err != nil {
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
		CoordinateLiveDDL: false, // operator passed --no-coordinate-live-ddl
	}
	if err := s.engageShardCoordination(newSupportingApplier()); err != nil {
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
	err := s.engageShardCoordination(nonSupportingApplier{})
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
	}
	if err := s.engageShardCoordination(newSupportingApplier()); err != nil {
		t.Fatalf("engageShardCoordination: %v", err)
	}
	mgr := s.ShardConsolidationLeaseManager()
	if mgr == nil {
		t.Fatal("expected lease manager to be constructed when engagement conditions are met")
	}
	if mgr.streamID != "stream-a" {
		t.Errorf("manager.streamID = %q, want %q", mgr.streamID, "stream-a")
	}
}

func TestEngage_DefaultsZeroLeaseConfig(t *testing.T) {
	t.Parallel()
	s := &Streamer{
		StreamID:          "stream-a",
		InjectShardColumn: ShardColumnSpec{Name: "source_shard_id", Value: "us-east-1"},
		CoordinateLiveDDL: true,
		// ShardCoordinationLease left zero ⇒ defaults kick in.
	}
	if err := s.engageShardCoordination(newSupportingApplier()); err != nil {
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
