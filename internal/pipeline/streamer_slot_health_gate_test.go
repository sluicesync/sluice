// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// slotHealthGateEngine is a minimal ir.Engine stub for the
// attachSlotHealthProbe capability gate (RDS validation F4). It records
// OpenSchemaReader calls — the first thing the probe does past the gate
// — so the tests can assert whether the gate admitted or short-circuited
// the source WITHOUT needing a live reader. Every other surface panics
// (the stubEngine discipline: an unexpected call is a test bug).
type slotHealthGateEngine struct {
	caps  ir.Capabilities
	opens int32
}

func (e *slotHealthGateEngine) Name() string                  { return "slot-health-gate-stub" }
func (e *slotHealthGateEngine) Capabilities() ir.Capabilities { return e.caps }

func (e *slotHealthGateEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	atomic.AddInt32(&e.opens, 1)
	return nil, errors.New("slot-health-gate-stub: no reader")
}

func (e *slotHealthGateEngine) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	panic("unexpected OpenSchemaWriter")
}

func (e *slotHealthGateEngine) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	panic("unexpected OpenRowReader")
}

func (e *slotHealthGateEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	panic("unexpected OpenRowWriter")
}

func (e *slotHealthGateEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	panic("unexpected OpenCDCReader")
}

func (e *slotHealthGateEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	panic("unexpected OpenChangeApplier")
}

func (e *slotHealthGateEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	panic("unexpected OpenSnapshotStream")
}

// TestAttachSlotHealthProbe_GateExcludesSlotlessCDC pins the F4 gate:
// a source whose declared CDC mechanism does not create a replication
// slot (postgres-trigger's CDCTriggers, MySQL's CDCBinlog) must be
// short-circuited BEFORE the source is even opened — the trigger
// engine's delegated SchemaReader satisfies ir.SlotHealthReporter, so
// interface presence alone would attach the probe and log the
// misleading "slot-health probe attached ... slot=sluice_slot" INFO on
// a slot-LESS engine.
func TestAttachSlotHealthProbe_GateExcludesSlotlessCDC(t *testing.T) {
	for name, caps := range map[string]ir.Capabilities{
		"postgres-trigger": capsTriggerPG,
		"mysql-binlog":     capsMySQL,
		"zero-caps":        {},
	} {
		eng := &slotHealthGateEngine{caps: caps}
		s := &Streamer{Source: eng, SourceDSN: "stub://source"}
		att := s.attachSlotHealthProbe(context.Background(), "stream-f4")
		att.Close() // noop attachment must be safely closable
		if got := atomic.LoadInt32(&eng.opens); got != 0 {
			t.Errorf("%s: expected the capability gate to short-circuit BEFORE opening the source; got %d OpenSchemaReader call(s)", name, got)
		}
	}
}

// TestAttachSlotHealthProbe_SlotCDCPassesGate is the positive control:
// a CDCLogicalReplication source proceeds past the gate (the stub's
// OpenSchemaReader is reached — its error then degrades the probe to
// the documented non-fatal noop).
func TestAttachSlotHealthProbe_SlotCDCPassesGate(t *testing.T) {
	eng := &slotHealthGateEngine{caps: capsSlotPG}
	s := &Streamer{Source: eng, SourceDSN: "stub://source"}
	att := s.attachSlotHealthProbe(context.Background(), "stream-f4")
	att.Close()
	if got := atomic.LoadInt32(&eng.opens); got != 1 {
		t.Errorf("expected exactly one OpenSchemaReader call for a slot-CDC source; got %d", got)
	}
}
