// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Broker-state liveness file for [SyncFromBackup]. Phase 4.5 mirrors
// Phase 4's `manifests/stream_state.json` shape at the consumer side:
// `manifests/broker_state.json` records the running broker's pid /
// host / heartbeat / stop request so an operator running
// `sluice sync from-backup stop` against the destination can request a
// graceful drain without process access. Both files coexist when a
// stream + broker run against the same destination — one is producer-
// side liveness, the other consumer-side; neither gates the other's
// concurrent-writer check.
//
// Why a parallel file rather than an extension of `stream_state.json`:
// streams write their own state to `stream_state.json`; a broker reads
// the chain (read-only consumer) and writes its own state separately.
// Mixing the two would couple the producer's concurrent-writer check
// to the consumer's liveness, defeating the fan-out value-prop of
// Phase 4.5 (1 stream → N brokers).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// DefaultBrokerStateFilename is the path within the chain store the
// broker liveness file is written to. Lives under
// [lineage.IncrementalManifestPrefix] alongside `stream_state.json` and the
// chain manifests so a single `List(manifests/)` enumerates all of
// them; the chain-walker filters by entry shape so neither state file
// is mistaken for a chain entry.
const DefaultBrokerStateFilename = lineage.IncrementalManifestPrefix + "broker_state.json"

// brokerState is the on-disk shape of `broker_state.json`. Mirrors
// [streamState]'s field layout for operator pattern-matching:
// monitoring tooling that watches a backup destination only has to
// learn one shape.
type brokerState struct {
	// PID is the OS process id of the broker that wrote this file.
	PID int `json:"pid"`

	// Host is the hostname the broker ran on.
	Host string `json:"host"`

	// StreamID is the operator-supplied identifier the broker uses to
	// key its row in `sluice_cdc_state`. Recorded here so a monitoring
	// glance at the file shows which target stream this broker drives
	// without having to query the target.
	StreamID string `json:"stream_id"`

	// StartedAt is the wall-clock timestamp the broker began.
	StartedAt time.Time `json:"started_at"`

	// LastApplyAt is the wall-clock timestamp of the most recent
	// successful incremental-apply commit, or — when the broker tick
	// found no new manifests — the most recent heartbeat poll.
	// Operators monitoring this field flag a broker as stuck when the
	// timestamp goes stale relative to the source's stream activity.
	LastApplyAt time.Time `json:"last_apply_at"`

	// StopRequestedAt, when non-nil, signals an operator has asked
	// the broker to exit cleanly. `sluice sync from-backup stop`
	// writes this; the running broker polls between ticks and exits.
	StopRequestedAt *time.Time `json:"stop_requested_at,omitempty"`
}

// readBrokerState loads the state file at path from store. Returns
// (nil, nil) when the file doesn't exist (the cold-start case);
// (nil, err) when the file exists but can't be decoded.
func readBrokerState(ctx context.Context, store irbackup.Store, path string) (*brokerState, error) {
	exists, err := store.Exists(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("broker state: exists %q: %w", path, err)
	}
	if !exists {
		return nil, nil
	}
	rc, err := store.Get(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("broker state: get %q: %w", path, err)
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("broker state: read %q: %w", path, err)
	}
	var s brokerState
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, fmt.Errorf("broker state: decode %q: %w", path, err)
	}
	return &s, nil
}

// writeBrokerState serialises s as JSON (indented for human
// readability) and writes it to store at path. Overwrites any existing
// content.
//
// Mirrors [writeStreamState]'s contract: reserved for paths that
// legitimately own the entire file content (initial setup, takeover).
// Heartbeat writes use [writeBrokerStateMergeHeartbeat] to preserve
// any concurrent stop_requested_at the operator wrote in the race
// window between the broker's read-poll and write-heartbeat.
func writeBrokerState(ctx context.Context, store irbackup.Store, path string, s *brokerState) error {
	body, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("broker state: marshal: %w", err)
	}
	return store.Put(ctx, path, bytes.NewReader(body))
}

// writeBrokerStateMergeHeartbeat is the heartbeat-write helper that
// mirrors [writeStreamStateMergeHeartbeat]'s clobber-fix shape: read
// the current file, copy any concurrent StopRequestedAt forward into
// the supplied state, then write. Returns true when a concurrent stop
// was observed during the merge — callers use it to short-circuit
// further heartbeat work and trigger a clean exit.
//
// Same TOCTOU caveat as the stream side: a stop write that lands
// between this function's read and its write is still lost. The
// in-process channel registry ([brokerStopRegistry]) closes the
// same-process case entirely; the remaining cross-process window is
// bounded by one state-file flush.
func writeBrokerStateMergeHeartbeat(ctx context.Context, store irbackup.Store, path string, s *brokerState) (stopObserved bool, err error) {
	prior, err := readBrokerState(ctx, store, path)
	if err != nil {
		return false, err
	}
	if prior != nil && prior.StopRequestedAt != nil && s.StopRequestedAt == nil {
		s.StopRequestedAt = prior.StopRequestedAt
		stopObserved = true
	}
	if err := writeBrokerState(ctx, store, path, s); err != nil {
		return stopObserved, err
	}
	return stopObserved, nil
}

// readBrokerStopRequested returns the StopRequestedAt timestamp from
// the state file, or nil if the file doesn't exist or doesn't carry a
// stop request. Errors from the store surface to the caller; missing
// file is (nil, nil).
func readBrokerStopRequested(ctx context.Context, store irbackup.Store, path string) (*time.Time, error) {
	s, err := readBrokerState(ctx, store, path)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, nil
	}
	return s.StopRequestedAt, nil
}

// RequestSyncFromBackupStop sets `stop_requested_at` on the broker
// state file at store's [DefaultBrokerStateFilename] path so the
// running broker observes the request on its next tick poll and exits
// cleanly. Mirrors [RequestStreamStop]: idempotent (re-issuing stop
// preserves the original timestamp), tolerant of cross-machine setups
// (the destination IS the rendezvous), backed by an in-process channel
// for same-process zero-latency observation.
//
// Returns (nil, error) when the state file is absent — there's no
// running broker to stop. The caller's error message names the
// "no broker is running" case explicitly.
func RequestSyncFromBackupStop(ctx context.Context, store irbackup.Store, now time.Time) (*brokerState, error) {
	return requestBrokerStopAt(ctx, store, DefaultBrokerStateFilename, now)
}

// requestBrokerStopAt is [RequestSyncFromBackupStop] generalised to a
// caller-supplied path. Tests pin a deterministic path; production
// callers route through RequestSyncFromBackupStop.
func requestBrokerStopAt(ctx context.Context, store irbackup.Store, path string, now time.Time) (*brokerState, error) {
	prior, err := readBrokerState(ctx, store, path)
	if err != nil {
		return nil, fmt.Errorf("broker stop: read state file: %w", err)
	}
	if prior == nil {
		return nil, fmt.Errorf("broker stop: no broker_state.json at %q; either no broker is running against this destination, or it hasn't completed its first tick yet",
			path)
	}
	if prior.StopRequestedAt == nil {
		t := now.UTC()
		prior.StopRequestedAt = &t
	}
	if err := writeBrokerState(ctx, store, path, prior); err != nil {
		return nil, fmt.Errorf("broker stop: write state file: %w", err)
	}
	// In-process notification (mirrors v0.19.1's stream stop fix):
	// when the running broker is in the same Go process as
	// RequestSyncFromBackupStop (CLI single-binary, integration test),
	// close the process-local stop channel so the running broker
	// exits immediately without waiting for a file-poll tick. No-op
	// when cross-process — the channel is registered per-process, so
	// a stop command issued on machine B against a broker on machine
	// A finds no registered channel and the file remains the
	// cross-machine rendezvous. See [notifyBrokerStop] /
	// [registerBrokerStopChan] in broker_stop_registry.go.
	if notifyBrokerStop(store) {
		slog.DebugContext(
			ctx, "broker stop: in-process channel signalled",
			slog.String("path", path),
		)
	}
	return prior, nil
}
