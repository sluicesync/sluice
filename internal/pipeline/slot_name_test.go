// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// slotAwareEngine is a stub that implements both ir.Engine and the
// optional CDCReaderWithSlotOpener / SnapshotStreamWithSlotOpener
// surfaces. Used to assert the orchestrator routes to the slot-
// aware methods when a slot name is set. Unlike stubEngine, this
// type's OpenCDCReader / OpenSnapshotStream return nil cleanly
// rather than panicking — the dispatch tests need to exercise both
// the slot-aware and default paths.
type slotAwareEngine struct {
	stubEngine
	lastCDCSlotName      string
	lastSnapshotSlotName string
	cdcCallCount         int
	snapshotCallCount    int
	defaultCDCCalls      int
	defaultSnapshotCalls int
}

func (e *slotAwareEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	e.defaultCDCCalls++
	return nil, nil //nolint:nilnil // stub
}

func (e *slotAwareEngine) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	e.defaultSnapshotCalls++
	return nil, nil //nolint:nilnil // stub
}

func (e *slotAwareEngine) OpenCDCReaderWithSlot(_ context.Context, _, slotName string) (ir.CDCReader, error) {
	e.cdcCallCount++
	e.lastCDCSlotName = slotName
	return nil, nil //nolint:nilnil // stub
}

func (e *slotAwareEngine) OpenSnapshotStreamWithSlot(_ context.Context, _, slotName string) (*ir.SnapshotStream, error) {
	e.snapshotCallCount++
	e.lastSnapshotSlotName = slotName
	return nil, nil //nolint:nilnil // stub
}

// TestResolveSlotName pins the sluice-prefix convention: every
// sluice-created replication slot starts with `sluice_` so cleanup
// queries can find them all. The empty case passes through unchanged
// (signals "use the engine's default name", which is itself
// `sluice_slot`); already-prefixed names are idempotent.
func TestResolveSlotName(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", ""},                                    // empty → engine default
		{"shard_a", "sluice_shard_a"},               // bare name gets prefix
		{"sluice_shard_a", "sluice_shard_a"},        // already prefixed → idempotent
		{"sluice_slot", "sluice_slot"},              // default name unchanged
		{"my-custom-name", "sluice_my-custom-name"}, // hyphens fine
		// Edge case: a name that contains 'sluice_' but doesn't
		// start with it gets the prefix anyway. Operator-typo
		// case; the strict prefix check makes the convention
		// unambiguous.
		{"app_sluice_a", "sluice_app_sluice_a"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.in, func(t *testing.T) {
			if got := resolveSlotName(c.in); got != c.want {
				t.Errorf("resolveSlotName(%q) = %q; want %q", c.in, got, c.want)
			}
		})
	}
}

// TestOpenCDCReaderWithOptionalSlot covers the dispatch matrix:
// (slotName empty/non-empty) × (engine implements interface or
// not). The shape is the same for the snapshot variant — one
// extra test covers it.
func TestOpenCDCReaderWithOptionalSlot(t *testing.T) {
	t.Run("empty slotName uses default OpenCDCReader", func(t *testing.T) {
		e := &slotAwareEngine{}
		_, _ = openCDCReaderWithOptionalSlot(context.Background(), e, "dsn", "")
		if e.cdcCallCount != 0 {
			t.Errorf("OpenCDCReaderWithSlot called with empty slot; want default path used")
		}
		if e.defaultCDCCalls != 1 {
			t.Errorf("default OpenCDCReader call count = %d; want 1", e.defaultCDCCalls)
		}
	})

	t.Run("non-empty slotName routes to slot-aware method", func(t *testing.T) {
		e := &slotAwareEngine{}
		_, _ = openCDCReaderWithOptionalSlot(context.Background(), e, "dsn", "my_custom_slot")
		if e.cdcCallCount != 1 {
			t.Errorf("OpenCDCReaderWithSlot call count = %d; want 1", e.cdcCallCount)
		}
		if e.lastCDCSlotName != "my_custom_slot" {
			t.Errorf("slotName forwarded = %q; want my_custom_slot", e.lastCDCSlotName)
		}
		if e.defaultCDCCalls != 0 {
			t.Errorf("default OpenCDCReader was called; should have been skipped")
		}
	})

	t.Run("non-implementing engine falls back to default", func(t *testing.T) {
		// nonSlotAwareEngine has the default Open methods but
		// doesn't implement CDCReaderWithSlotOpener. The dispatch
		// should silently fall back to OpenCDCReader.
		e := &nonSlotAwareEngine{}
		_, _ = openCDCReaderWithOptionalSlot(context.Background(), e, "dsn", "ignored")
		if e.defaultCDCCalls != 1 {
			t.Errorf("expected fallback to OpenCDCReader; got %d calls", e.defaultCDCCalls)
		}
	})
}

// nonSlotAwareEngine is a stub that has the basic Engine methods
// but doesn't implement either slot-aware optional surface — used
// to verify the fallback path silently degrades.
type nonSlotAwareEngine struct {
	stubEngine
	defaultCDCCalls int
}

func (e *nonSlotAwareEngine) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	e.defaultCDCCalls++
	return nil, nil //nolint:nilnil // stub
}

// TestOpenSnapshotStreamWithOptionalSlot mirrors the CDC version's
// shape; one positive test for the slot-aware path is enough.
func TestOpenSnapshotStreamWithOptionalSlot(t *testing.T) {
	e := &slotAwareEngine{}
	_, _ = openSnapshotStreamWithOptionalSlot(context.Background(), e, "dsn", "snap_custom_slot")
	if e.snapshotCallCount != 1 {
		t.Errorf("OpenSnapshotStreamWithSlot call count = %d; want 1", e.snapshotCallCount)
	}
	if e.lastSnapshotSlotName != "snap_custom_slot" {
		t.Errorf("slotName forwarded = %q; want snap_custom_slot", e.lastSnapshotSlotName)
	}
}
