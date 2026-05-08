// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"testing"
)

// TestStreamStopRegistry_RegisterNotifyDeregister exercises the full
// lifecycle: register a channel, notify it, observe the close, deregister
// (no-op since already-closed via notify). Pins the in-process
// stop-signal contract that BackupStream.Run + RequestStreamStop rely
// on for Bug 37's same-process fix.
func TestStreamStopRegistry_RegisterNotifyDeregister(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	ch, deregister := registerStreamStopChan(store)
	defer deregister()

	// Channel should not be closed yet.
	select {
	case <-ch:
		t.Fatal("channel closed before notifyStreamStop; want open")
	default:
	}

	// Notify; channel should close.
	if !notifyStreamStop(store) {
		t.Error("notifyStreamStop = false; want true (registered store)")
	}
	select {
	case <-ch:
		// expected
	default:
		t.Error("channel still open after notifyStreamStop; want closed")
	}

	// Re-notify after notify is still true: the registration is still
	// present (only deregister removes it), and the close-once
	// pattern means the channel stays in the closed state. The
	// "true" return therefore reflects "we had a stream to signal" —
	// a registration-state question, not a channel-state question.
	if !notifyStreamStop(store) {
		t.Error("re-notifyStreamStop = false; want true (still registered)")
	}
}

// TestStreamStopRegistry_NotifyUnknownStore returns false when no
// stream is registered for the given store — the cross-process case
// where the operator's `sluice backup stream stop` runs in a different
// process and has nothing to signal in-process. The file write remains
// the cross-machine rendezvous.
func TestStreamStopRegistry_NotifyUnknownStore(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	if notifyStreamStop(store) {
		t.Error("notifyStreamStop = true on unregistered store; want false")
	}
}

// TestStreamStopRegistry_DeregisterClosesChannelIfStillOpen exercises
// the natural-exit path: the stream's Run returns without anyone
// calling RequestStreamStop, the deferred deregister closes the channel
// so any goroutine still holding a reference exits its select cleanly.
func TestStreamStopRegistry_DeregisterClosesChannelIfStillOpen(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	ch, deregister := registerStreamStopChan(store)
	deregister()

	// Channel should be closed.
	select {
	case <-ch:
		// expected
	default:
		t.Error("channel still open after deregister; want closed")
	}

	// notifyStreamStop after deregister should return false (no entry).
	if notifyStreamStop(store) {
		t.Error("notifyStreamStop = true after deregister; want false")
	}
}

// TestStreamStopRegistry_DeregisterIdempotent calls deregister twice;
// the second call is a no-op (sync.Once-guarded).
func TestStreamStopRegistry_DeregisterIdempotent(t *testing.T) {
	dir := t.TempDir()
	store, _ := NewLocalStore(dir)

	_, deregister := registerStreamStopChan(store)
	deregister()
	// Second call must not panic from double-close.
	deregister()
}
