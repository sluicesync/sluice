// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// In-process stop-signal registry for [BackupStream]. Closes the
// load-bearing same-process gap in Bug 37: when both
// [BackupStream.Run] and [RequestStreamStop] execute in the same Go
// process (CLI single-binary subcommand setups, integration tests,
// the planned `sync from-backup` consumer holding a stream
// inline), the file-poll path is a slow, racy serialisation of an
// in-memory transition. The registry lets RequestStreamStop notify
// the running stream via an in-process channel — instantaneous, no
// file I/O, no select-loop starvation.
//
// Cross-process semantics are unchanged: an operator on machine B
// running `sluice backup stream stop --target=<url>` against a stream
// running on machine A still goes through the file (the channel is
// process-local; there's nothing to register on machine B). The
// registry is opportunistic; the file remains the rendezvous of
// record.
//
// Registry shape: a map of [ir.BackupStore] → chan struct{}, keyed
// by store identity. `LocalStore` and `BlobStore` are pointer types,
// so distinct store instances pointing at the same destination get
// distinct keys — that's intentional: the channel signals a SPECIFIC
// running stream, not "any stream against this destination", so the
// pointer-identity match is correct for the same-process case.

import (
	"sync"

	"github.com/orware/sluice/internal/ir"
)

// streamStopRegistry tracks running [BackupStream] instances by their
// [ir.BackupStore]. Each entry's channel is closed exactly once when
// either the operator's RequestStreamStop fires or the stream's Run
// returns and unregisters itself. Closing-instead-of-sending lets
// multiple captureWindow iterations safely consume the signal via the
// channel-closed-on-receive idiom.
var streamStopRegistry = struct {
	mu sync.Mutex
	// chans is keyed by the dynamic-type pointer of the BackupStore.
	// Concrete store types (LocalStore, BlobStore) are pointer types
	// already, so the interface value's underlying pointer is the
	// natural identity. Map keys are interface-typed; equality
	// works because both sides of the comparison have pointer
	// dynamic types.
	chans map[ir.BackupStore]chan struct{}
}{chans: map[ir.BackupStore]chan struct{}{}}

// registerStreamStopChan creates and registers an in-process stop
// channel for store. Returns the channel + a deregister closure the
// caller must defer. The channel is closed by [notifyStreamStop] when
// an in-process RequestStreamStop fires, OR by the deregister closure
// when the stream exits naturally — whichever happens first. Idempotent
// close is guarded internally so both paths are safe.
//
// The returned deregister closure is the only safe way to remove the
// entry; calling it more than once is a no-op (sync.Once-guarded).
func registerStreamStopChan(store ir.BackupStore) (stopCh chan struct{}, deregister func()) {
	ch := make(chan struct{})
	streamStopRegistry.mu.Lock()
	streamStopRegistry.chans[store] = ch
	streamStopRegistry.mu.Unlock()

	var once sync.Once
	dereg := func() {
		once.Do(func() {
			streamStopRegistry.mu.Lock()
			// Only delete if the registered channel still matches —
			// an unrelated subsequent registration on the same store
			// pointer (test re-use) shouldn't be clobbered by this
			// deregister.
			if streamStopRegistry.chans[store] == ch {
				delete(streamStopRegistry.chans, store)
			}
			streamStopRegistry.mu.Unlock()
			// Close-channel-if-not-already idiom: a select with a
			// default branch detects the closed state cheaply.
			select {
			case <-ch:
				// already closed
			default:
				close(ch)
			}
		})
	}
	return ch, dereg
}

// notifyStreamStop closes the registered stop channel for store, if
// any. Called by [RequestStreamStop] alongside the file write so a
// same-process running stream observes the stop without polling
// [BackupStore.Get]. Returns true when an entry was found and signalled
// — useful for tests asserting the in-process path fired.
//
// Safe to call when no stream is registered (cross-process case): the
// function is a no-op and the file write remains the cross-machine
// rendezvous.
func notifyStreamStop(store ir.BackupStore) bool {
	streamStopRegistry.mu.Lock()
	ch, ok := streamStopRegistry.chans[store]
	streamStopRegistry.mu.Unlock()
	if !ok {
		return false
	}
	// Close-once via a select/default — multiple notifyStreamStop
	// callers (re-issued stop) tolerated.
	select {
	case <-ch:
		// already closed
	default:
		close(ch)
	}
	return true
}
