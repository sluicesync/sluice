// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// In-process stop-signal registry for [SyncFromBackup]. Mirrors the
// [streamStopRegistry] pattern from Phase 4 / v0.19.1 (Bug 37) at the
// broker side: when both [SyncFromBackup.Run] and
// [RequestSyncFromBackupStop] execute in the same Go process (CLI
// single-binary subcommand setups, integration tests), the file-poll
// path is a slow, racy serialisation of an in-memory transition. The
// registry lets RequestSyncFromBackupStop notify the running broker
// via an in-process channel — instantaneous, no file I/O, no
// select-loop starvation.
//
// A separate registry (rather than reusing [streamStopRegistry]) is
// load-bearing: a stream and a broker can run against the SAME store
// in the same process (e.g. an integration test exercising the
// stream→chain→broker fan-out). Sharing the registry would make a
// `RequestStreamStop` close the broker's channel and vice versa —
// wrong semantics, since the two write to different state files
// (`stream_state.json` vs `broker_state.json`) and have independent
// lifecycles.
//
// Cross-process semantics are unchanged: an operator on machine B
// running `sluice sync from-backup stop --target=<url>` against a
// broker running on machine A still goes through the file (the
// channel is process-local; there's nothing to register on
// machine B). The registry is opportunistic; the file remains the
// rendezvous of record.

import (
	"sync"

	"github.com/orware/sluice/internal/ir"
)

// brokerStopRegistry tracks running [SyncFromBackup] instances by
// their [ir.BackupStore]. Each entry's channel is closed exactly once
// when either the operator's RequestSyncFromBackupStop fires or the
// broker's Run returns and unregisters itself. Closing-instead-of-
// sending lets multiple poll iterations safely consume the signal via
// the channel-closed-on-receive idiom.
var brokerStopRegistry = struct {
	mu sync.Mutex
	// chans is keyed by the dynamic-type pointer of the BackupStore.
	// Concrete store types (LocalStore, BlobStore) are pointer types
	// already, so the interface value's underlying pointer is the
	// natural identity. Map keys are interface-typed; equality works
	// because both sides of the comparison have pointer dynamic types.
	chans map[ir.BackupStore]chan struct{}
}{chans: map[ir.BackupStore]chan struct{}{}}

// registerBrokerStopChan creates and registers an in-process stop
// channel for store. Returns the channel + a deregister closure the
// caller must defer. The channel is closed by [notifyBrokerStop] when
// an in-process RequestSyncFromBackupStop fires, OR by the deregister
// closure when the broker exits naturally — whichever happens first.
// Idempotent close is guarded internally so both paths are safe.
//
// The returned deregister closure is the only safe way to remove the
// entry; calling it more than once is a no-op (sync.Once-guarded).
func registerBrokerStopChan(store ir.BackupStore) (stopCh chan struct{}, deregister func()) {
	ch := make(chan struct{})
	brokerStopRegistry.mu.Lock()
	brokerStopRegistry.chans[store] = ch
	brokerStopRegistry.mu.Unlock()

	var once sync.Once
	dereg := func() {
		once.Do(func() {
			brokerStopRegistry.mu.Lock()
			// Only delete if the registered channel still matches —
			// an unrelated subsequent registration on the same store
			// pointer (test re-use) shouldn't be clobbered by this
			// deregister.
			if brokerStopRegistry.chans[store] == ch {
				delete(brokerStopRegistry.chans, store)
			}
			brokerStopRegistry.mu.Unlock()
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

// notifyBrokerStop closes the registered stop channel for store, if
// any. Called by [RequestSyncFromBackupStop] alongside the file write
// so a same-process running broker observes the stop without polling
// the file. Returns true when an entry was found and signalled —
// useful for tests asserting the in-process path fired.
//
// Safe to call when no broker is registered (cross-process case): the
// function is a no-op and the file write remains the cross-machine
// rendezvous.
func notifyBrokerStop(store ir.BackupStore) bool {
	brokerStopRegistry.mu.Lock()
	ch, ok := brokerStopRegistry.chans[store]
	brokerStopRegistry.mu.Unlock()
	if !ok {
		return false
	}
	// Close-once via a select/default — multiple notifyBrokerStop
	// callers (re-issued stop) tolerated.
	select {
	case <-ch:
		// already closed
	default:
		close(ch)
	}
	return true
}
