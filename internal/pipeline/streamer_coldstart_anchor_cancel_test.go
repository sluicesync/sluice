// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the cold-start CDC-anchor durability rule: the anchor
// write (GitHub issue #15) must survive the caller's cancellation, and a
// stop that lands in the handoff window must therefore CLOSE the snapshot
// stream (keeping the durable slot as the resume anchor) rather than
// ABANDON it (dropping the slot and stranding a completed copy).
//
// Before the fix this path was inverted: WritePosition rode the caller's
// ctx, so an operator stop between "bulk copy committed" and "CDC
// started" made the anchor write fail with context.Canceled, which is an
// Abandon site — the just-created Postgres slot was dropped and the copy
// became unresumable. Ground-truthed against real Postgres (4 of 6
// tight-poll cancels left the source with no replication slot at all).

package pipeline

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// anchorRecordingApplier is a [ir.ChangeApplier] that records the
// liveness of the ctx its WritePosition was called with, and fails the
// write exactly the way a cancelled ctx would.
type anchorRecordingApplier struct {
	ir.ChangeApplier // nil: no other method is exercised by coldStartBeginCDC

	called     bool
	ctxErrSeen error
}

func (a *anchorRecordingApplier) WritePosition(ctx context.Context, _ string, _ ir.Position) error {
	a.called = true
	a.ctxErrSeen = ctx.Err()
	// Model the real applier: the write fails iff its ctx is already dead.
	return ctx.Err()
}

// cancelledHandoffCDCReader stands in for the engine reader whose
// StreamChanges is the next thing the cancelled handoff reaches.
type cancelledHandoffCDCReader struct{}

func (cancelledHandoffCDCReader) StreamChanges(ctx context.Context, _ ir.Position) (<-chan ir.Change, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ch := make(chan ir.Change)
	close(ch)
	return ch, nil
}

// TestColdStartBeginCDC_CancelledStopKeepsTheSlot is the pin: with the
// caller's ctx already cancelled, coldStartBeginCDC must still land the
// anchor write on a LIVE ctx and must tear the stream down via Close,
// never Abandon.
func TestColdStartBeginCDC_CancelledStopKeepsTheSlot(t *testing.T) {
	var abandoned, closed bool
	stream := recordingSnapshotStream(&abandoned, &closed)
	stream.Position = ir.Position{Engine: "postgres", Token: "0/1A2B3C4"}
	stream.Changes = cancelledHandoffCDCReader{}

	applier := &anchorRecordingApplier{}
	s := &Streamer{Source: stubEngine{}, Target: stubEngine{}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the operator's Ctrl-C, landing in the handoff window

	_, _, err := s.coldStartBeginCDC(ctx, stream, applier, "stream-1", nil)
	if err == nil {
		t.Fatal("expected the cancelled handoff to return an error; got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want a context.Canceled from the CDC start", err)
	}

	// The operator-visible invariant first, then the mechanism that
	// delivers it — so a re-break of either is reported for what it is.
	if abandoned {
		t.Error("a cancelled handoff ABANDONED the snapshot stream — that drops the just-created " +
			"replication slot and strands the completed copy (no cdc-state row ⇒ warm resume " +
			"impossible, cold start refuses on the populated target)")
	}
	if !closed {
		t.Error("a cancelled handoff must still Close the snapshot stream")
	}
	if !applier.called {
		t.Fatal("the cold-start anchor was never written")
	}
	if applier.ctxErrSeen != nil {
		t.Errorf("the anchor write rode the cancelled caller ctx (ctx.Err()=%v); "+
			"it must run on an uncancellable ctx or a stop silently discards the resume anchor",
			applier.ctxErrSeen)
	}
}

// TestColdStartBeginCDC_GenuineAnchorFailureStillAbandons guards the
// other side of the rule: when the anchor write fails for a REAL reason
// (not the caller's cancellation), no durable resume point exists, so
// Bug 177's Abandon must still fire and drop the slot.
func TestColdStartBeginCDC_GenuineAnchorFailureStillAbandons(t *testing.T) {
	var abandoned, closed bool
	stream := recordingSnapshotStream(&abandoned, &closed)
	stream.Position = ir.Position{Engine: "postgres", Token: "0/1A2B3C4"}
	stream.Changes = cancelledHandoffCDCReader{}

	wantErr := errors.New("target unreachable")
	applier := &failingAnchorApplier{err: wantErr}
	s := &Streamer{Source: stubEngine{}, Target: stubEngine{}}

	_, _, err := s.coldStartBeginCDC(context.Background(), stream, applier, "stream-1", nil)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v; want the underlying anchor-write failure", err)
	}
	if !abandoned {
		t.Fatal("a genuine anchor-write failure must Abandon (Bug 177: no anchor ⇒ the slot is orphaned WAL)")
	}
	if closed {
		t.Fatal("Abandon must not fall through to CloseFn when AbandonFn is set")
	}
}

// failingAnchorApplier fails WritePosition unconditionally, modelling a
// target-side failure rather than a cancellation.
type failingAnchorApplier struct {
	ir.ChangeApplier
	err error
}

func (a *failingAnchorApplier) WritePosition(context.Context, string, ir.Position) error {
	return a.err
}
