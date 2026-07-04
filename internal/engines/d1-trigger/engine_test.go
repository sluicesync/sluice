// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package d1trigger

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// TestEngine_Registered confirms the package's init() side-effect landed
// the engine under "d1-trigger". The pipeline orchestrator resolves
// engines by name via engines.Get; a missing registration would be a
// silent breakage where the operator's --source-driver=d1-trigger
// surfaces as "unknown engine" rather than the intended path.
func TestEngine_Registered(t *testing.T) {
	e, ok := engines.Get(EngineName)
	if !ok {
		t.Fatalf("engine %q not registered", EngineName)
	}
	if got := e.Name(); got != EngineName {
		t.Errorf("e.Name() = %q; want %q", got, EngineName)
	}
}

// TestEngine_Capabilities pins the declared surface to its ADR-0136
// values: the composed `d1` cold-start shape (SQLite over HTTP — flat
// namespace, no extension types, batched-insert bulk load) with CDC
// flipped to trigger-based. The struct is fully comparable, so one
// equality catches ANY drift — including a future ir.Capabilities field
// this engine silently inherits a nonzero value for.
func TestEngine_Capabilities(t *testing.T) {
	want := ir.Capabilities{
		BulkLoad:                 ir.BulkLoadBatchedInsert,
		CDC:                      ir.CDCTriggers,
		SchemaScope:              ir.SchemaScopeFlat,
		SupportedTypes:           ir.NewTypeSet(), // no extension types
		SupportsCheckConstraint:  true,
		SupportsGeneratedColumns: true,
		SupportsPartitioning:     false,
		EnumSupport:              ir.EnumNone,
		JSONSupport:              ir.JSONNone,
		UnsignedIntegers:         false,
		DDLDialect:               ir.DDLDialectANSI,
	}
	if got := (Engine{}).Capabilities(); got != want {
		t.Errorf("Capabilities() drifted:\n got %+v\nwant %+v", got, want)
	}
}

// TestEngine_WriteSurfacesNotImplemented pins the CDC-SOURCE-only
// contract: the write / target / change-apply Open* methods must return
// the package's ErrNotImplemented sentinel (errors.Is-checkable) rather
// than silently succeeding with a broken surface.
func TestEngine_WriteSurfacesNotImplemented(t *testing.T) {
	e := Engine{}
	ctx := context.Background()
	const dsn = "d1://acct/db"
	if _, err := e.OpenSchemaWriter(ctx, dsn); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("OpenSchemaWriter err = %v; want ErrNotImplemented", err)
	}
	if _, err := e.OpenRowWriter(ctx, dsn); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("OpenRowWriter err = %v; want ErrNotImplemented", err)
	}
	if _, err := e.OpenChangeApplier(ctx, dsn); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("OpenChangeApplier err = %v; want ErrNotImplemented", err)
	}
}

// TestEngine_NoSlotManager asserts the engine does NOT expose
// [ir.SlotManagerOpener]. The trigger engine has no replication slots,
// so the optional surface must cleanly miss on type-assertion (the
// CLI's `sluice slot list` then reports a polished "engine does not
// support replication-slot management" rather than silently degrading).
// This is the runtime counterpart of capabilities_assert.go's
// intentionally NARROW pin — see its comment before widening anything.
func TestEngine_NoSlotManager(t *testing.T) {
	var e ir.Engine = Engine{}
	if _, ok := e.(ir.SlotManagerOpener); ok {
		t.Errorf("Engine satisfies ir.SlotManagerOpener; want NOT (no slots to manage)")
	}
}

// TestEngine_NoCDCReaderWithSlot is the symmetric pin for
// [ir.CDCReaderWithSlotOpener]: with no slot to bind to, `--slot-name`
// must route to the default OpenCDCReader instead of dialing a
// non-existent slot.
func TestEngine_NoCDCReaderWithSlot(t *testing.T) {
	var e ir.Engine = Engine{}
	if _, ok := e.(ir.CDCReaderWithSlotOpener); ok {
		t.Errorf("Engine satisfies ir.CDCReaderWithSlotOpener; want NOT (no slot to bind to)")
	}
}
