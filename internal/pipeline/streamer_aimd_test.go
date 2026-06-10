// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// fakeAIMDApplier is a minimal [ir.ChangeApplier] that ALSO implements
// the optional [ir.BatchSizeProviderSetter] / [ir.BatchObserverSetter]
// surfaces. Used to pin the streamer's ADR-0052 controller-attach path
// without standing up a real engine.
type fakeAIMDApplier struct {
	provider ir.BatchSizeProvider
	observer ir.BatchObserver
}

func (a *fakeAIMDApplier) EnsureControlTable(context.Context) error { return nil }

func (a *fakeAIMDApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (a *fakeAIMDApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) {
	return nil, nil
}

func (a *fakeAIMDApplier) Apply(context.Context, string, <-chan ir.Change) error { return nil }

func (a *fakeAIMDApplier) RequestStop(context.Context, string) error        { return nil }
func (a *fakeAIMDApplier) ClearStopRequested(context.Context, string) error { return nil }

func (a *fakeAIMDApplier) SetBatchSizeProvider(p ir.BatchSizeProvider) { a.provider = p }
func (a *fakeAIMDApplier) SetBatchObserver(o ir.BatchObserver)         { a.observer = o }

// fakeNoSetterApplier is a minimal applier that does NOT implement the
// AIMD optional surfaces — used to pin the "engine without setters
// skips controller wiring" path.
type fakeNoSetterApplier struct{}

func (fakeNoSetterApplier) EnsureControlTable(context.Context) error { return nil }
func (fakeNoSetterApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (fakeNoSetterApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) { return nil, nil }
func (fakeNoSetterApplier) Apply(context.Context, string, <-chan ir.Change) error  { return nil }
func (fakeNoSetterApplier) RequestStop(context.Context, string) error              { return nil }
func (fakeNoSetterApplier) ClearStopRequested(context.Context, string) error       { return nil }

// namedEngine is the minimum stub engine the streamer needs to pull
// a Name() out of for the AIMD controller's engine-default
// resolution. Distinct from broker_test.go's stubTargetEngine to
// avoid the package-level redeclaration.
type namedEngine struct{ name string }

func (e *namedEngine) Name() string                  { return e.name }
func (e *namedEngine) Capabilities() ir.Capabilities { return ir.Capabilities{} }
func (*namedEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return nil, nil
}

func (*namedEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, nil
}

func (*namedEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return nil, nil
}

func (*namedEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, nil
}

func (*namedEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) { return nil, nil }
func (*namedEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, nil
}

func (*namedEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, nil
}

func TestStreamer_MaybeAttachAIMDController_WiresControllerOnDefaultOptIn(t *testing.T) {
	a := &fakeAIMDApplier{}
	s := &Streamer{
		Target:         &namedEngine{name: "mysql"},
		ApplyBatchSize: 100,
		AutoTune:       true,
	}
	ctrl := s.maybeAttachAIMDController(context.Background(), a, "test-stream")
	if ctrl == nil {
		t.Fatal("expected controller; got nil")
	}
	if a.provider == nil || a.observer == nil {
		t.Fatalf("applier setters not invoked: provider=%v observer=%v", a.provider, a.observer)
	}
	if got := a.provider.NextBatchSize(); got != 100 {
		t.Fatalf("provider initial NextBatchSize = %d; want 100", got)
	}
}

func TestStreamer_MaybeAttachAIMDController_RespectsNoAutoTune(t *testing.T) {
	a := &fakeAIMDApplier{}
	s := &Streamer{
		Target:         &namedEngine{name: "mysql"},
		ApplyBatchSize: 100,
		AutoTune:       false, // operator passed --no-auto-tune
	}
	ctrl := s.maybeAttachAIMDController(context.Background(), a, "test-stream")
	if ctrl != nil {
		t.Fatalf("expected nil controller with AutoTune=false; got %v", ctrl)
	}
	if a.provider != nil || a.observer != nil {
		t.Fatalf("applier setters should not be invoked when AutoTune is off")
	}
}

func TestStreamer_MaybeAttachAIMDController_SkipsBatchSizeBelowTwo(t *testing.T) {
	a := &fakeAIMDApplier{}
	s := &Streamer{
		Target:         &namedEngine{name: "mysql"},
		ApplyBatchSize: 1,
		AutoTune:       true,
	}
	ctrl := s.maybeAttachAIMDController(context.Background(), a, "test-stream")
	if ctrl != nil {
		t.Fatalf("ApplyBatchSize=1 should skip controller; got %v", ctrl)
	}
}

func TestStreamer_MaybeAttachAIMDController_SkipsEngineWithoutSetters(t *testing.T) {
	a := fakeNoSetterApplier{}
	s := &Streamer{
		Target:         &namedEngine{name: "test-engine"},
		ApplyBatchSize: 100,
		AutoTune:       true,
	}
	ctrl := s.maybeAttachAIMDController(context.Background(), a, "test-stream")
	if ctrl != nil {
		t.Fatalf("engine without setters should skip controller; got %v", ctrl)
	}
}

func TestStreamer_ResolveAIMDTargetLatency_EngineDefaults(t *testing.T) {
	cases := []struct {
		engine string
		caps   ir.Capabilities
		want   time.Duration
	}{
		// Both Vitess-backed flavors declare TransactionKiller (vtgate
		// ~20s tx-killer) → conservative 5s target per ADR-0052 DP-2.
		{"planetscale", ir.Capabilities{TransactionKiller: true}, 5 * time.Second},
		{"vitess", ir.Capabilities{TransactionKiller: true}, 5 * time.Second},
		{"mysql", capsMySQL, 10 * time.Second},
		{"postgres", capsSlotPG, 10 * time.Second},
		// Zero caps (unset target — a test stub) falls back to the
		// cross-engine default.
		{"zero-caps", ir.Capabilities{}, 10 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.engine, func(t *testing.T) {
			if got := resolveAIMDTargetLatency(tc.caps); got != tc.want {
				t.Fatalf("resolveAIMDTargetLatency(%s) = %v; want %v", tc.engine, got, tc.want)
			}
		})
	}
}

func TestStreamer_MaybeAttachAIMDController_OperatorOverrideTargetLatency(t *testing.T) {
	a := &fakeAIMDApplier{}
	s := &Streamer{
		Target:                 &namedEngine{name: "postgres"},
		ApplyBatchSize:         100,
		AutoTune:               true,
		ApplyTuneTargetLatency: 3 * time.Second, // operator override
	}
	ctrl := s.maybeAttachAIMDController(context.Background(), a, "test-stream")
	if ctrl == nil {
		t.Fatal("expected controller; got nil")
	}
	// The operator override should be reflected in the controller's
	// Snapshot output (Snapshot returns CurrentSize which starts at
	// the cap; we can verify the controller's target by checking that
	// when we feed a 4s latency batch, MD does NOT fire (4s < 3s
	// would be the relevant boundary if defaults applied — 4s > 3s
	// override = MD).
	for i := 0; i < 3; i++ {
		ctrl.ObserveBatch(context.Background(), 4*time.Second, 10, nil)
	}
	if got := a.provider.NextBatchSize(); got != 50 {
		t.Fatalf("with 3s target and 4s observed p95, expected MD to 50; got %d", got)
	}
}
