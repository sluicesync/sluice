// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"errors"
	"testing"
)

// TestSnapshotStream_AbandonPrefersAbandonFn pins the Abandon dispatch:
// an engine-supplied AbandonFn is invoked (and CloseFn is NOT — the
// engine's abandon closure owns the full teardown including whatever
// Close would have done), so the durable-artifact discard can't be
// silently skipped (Bug 177).
func TestSnapshotStream_AbandonPrefersAbandonFn(t *testing.T) {
	var abandoned, closed bool
	s := &SnapshotStream{
		AbandonFn: func() error { abandoned = true; return nil },
		CloseFn:   func() error { closed = true; return nil },
	}
	if err := s.Abandon(); err != nil {
		t.Fatalf("Abandon: %v", err)
	}
	if !abandoned {
		t.Fatal("AbandonFn was not invoked")
	}
	if closed {
		t.Fatal("CloseFn was invoked alongside AbandonFn; the abandon closure owns the full teardown")
	}
}

// TestSnapshotStream_AbandonFallsBackToClose pins the compatibility
// contract for engines whose opens create no durable artifact (MySQL
// binlog, VStream): with no AbandonFn, Abandon ≡ Close — byte-identical
// pre-Bug-177 behaviour.
func TestSnapshotStream_AbandonFallsBackToClose(t *testing.T) {
	var closed bool
	s := &SnapshotStream{CloseFn: func() error { closed = true; return nil }}
	if err := s.Abandon(); err != nil {
		t.Fatalf("Abandon: %v", err)
	}
	if !closed {
		t.Fatal("Abandon did not fall back to CloseFn when AbandonFn is nil")
	}
}

// TestSnapshotStream_AbandonNilSafe mirrors Close's nil-safety: a nil
// stream and a zero-value stream both no-op.
func TestSnapshotStream_AbandonNilSafe(t *testing.T) {
	var s *SnapshotStream
	if err := s.Abandon(); err != nil {
		t.Fatalf("nil stream Abandon: %v", err)
	}
	if err := (&SnapshotStream{}).Abandon(); err != nil {
		t.Fatalf("zero-value Abandon: %v", err)
	}
}

// TestSnapshotStream_AbandonPropagatesError pins that a failed
// durable-artifact discard is LOUD — the engine error reaches the
// caller rather than vanishing (the loud-failure tenet: a silently
// failed slot drop reproduces Bug 177).
func TestSnapshotStream_AbandonPropagatesError(t *testing.T) {
	want := errors.New("drop slot: boom")
	s := &SnapshotStream{AbandonFn: func() error { return want }}
	if err := s.Abandon(); !errors.Is(err, want) {
		t.Fatalf("Abandon error = %v; want %v", err, want)
	}
}
