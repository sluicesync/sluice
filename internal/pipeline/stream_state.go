// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Stream-state liveness file for [BackupStream]. The file lives at
// `manifests/stream_state.json` (path constant: [DefaultStreamStateFilename])
// and serves two purposes:
//
//   - Concurrent-writer protection. On startup, [BackupStream.Run]
//     reads any existing state file and refuses to start if a recent
//     `last_rollover_at` ({"<", "now - 2 × rollover-window"}) is
//     paired with a (pid, host) that doesn't match the current
//     process. Operator overrides via [BackupStream.Force].
//   - Cross-machine stop signaling. `sluice backup stream stop` writes
//     `stop_requested_at` to the file; the running stream polls
//     between rollovers and exits cleanly when the field is set.
//
// The state file is informational-only; the chain itself is the source
// of truth for restore + verify. Losing the state file (operator
// deletes it, object-store eventual-consistency lag) doesn't break
// the chain — only the concurrent-writer / cross-machine-stop
// signalling falls back to ctx-cancel and process signals.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// streamState is the on-disk shape of `stream_state.json`. Lives at
// [DefaultStreamStateFilename] (relative to the store's root). Field
// renames are forward-compatible-only; older sluice ignores unknown
// fields.
type streamState struct {
	// PID is the OS process id of the stream that wrote this file.
	// Used by the concurrent-writer check to identify ownership; not
	// load-bearing for restore. Zero is treated as "unknown" (legacy /
	// future format).
	PID int `json:"pid"`

	// Host is the hostname the stream ran on. Pairs with PID for the
	// concurrent-writer check. Empty is treated as "unknown."
	Host string `json:"host"`

	// StartedAt is the wall-clock timestamp the stream began. Used by
	// monitoring tooling; the concurrent-writer check uses
	// [LastRolloverAt] instead because that's the freshness signal.
	StartedAt time.Time `json:"started_at"`

	// LastRolloverAt is the wall-clock timestamp the most recent
	// rollover (or empty-rollover heartbeat tick) committed. The
	// concurrent-writer check refuses to start a second stream when
	// this is fresher than `now - 2 × rollover-window`.
	LastRolloverAt time.Time `json:"last_rollover_at"`

	// StopRequestedAt, when non-nil, signals the operator has asked
	// the stream to exit cleanly. `sluice backup stream stop` writes
	// this; the running stream polls between rollovers and exits.
	StopRequestedAt *time.Time `json:"stop_requested_at,omitempty"`
}

// readStreamState loads the state file at path from store. Returns
// (nil, nil) when the file doesn't exist (the common cold-start case);
// (nil, err) when the file exists but can't be decoded.
func readStreamState(ctx context.Context, store ir.BackupStore, path string) (*streamState, error) {
	exists, err := store.Exists(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("stream state: exists %q: %w", path, err)
	}
	if !exists {
		return nil, nil
	}
	rc, err := store.Get(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("stream state: get %q: %w", path, err)
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("stream state: read %q: %w", path, err)
	}
	var s streamState
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, fmt.Errorf("stream state: decode %q: %w", path, err)
	}
	return &s, nil
}

// writeStreamState serialises s as JSON (indented for human
// readability) and writes it to store at path. Overwrites any existing
// content — the file is single-writer by design (concurrent-writer
// protection enforces this at the orchestrator level).
func writeStreamState(ctx context.Context, store ir.BackupStore, path string, s *streamState) error {
	body, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("stream state: marshal: %w", err)
	}
	return store.Put(ctx, path, bytes.NewReader(body))
}

// readStreamStopRequested returns the StopRequestedAt timestamp from
// the state file, or nil if the file doesn't exist or doesn't carry a
// stop request. Errors from the store surface to the caller; missing
// file is (nil, nil).
func readStreamStopRequested(ctx context.Context, store ir.BackupStore, path string) (*time.Time, error) {
	s, err := readStreamState(ctx, store, path)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, nil
	}
	return s.StopRequestedAt, nil
}

// preflightStreamState runs the concurrent-writer check at
// [BackupStream.Run] startup.
//
//   - No state file → fresh start; nothing to do.
//   - State file exists, (pid, host) matches → previous run on this
//     host crashed without cleanup; we own this destination, take
//     over.
//   - State file exists, (pid, host) differs, last_rollover_at is
//     stale (`< now - 2*window`) → previous run on a different host
//     is no longer ticking; we take over with a WARN.
//   - State file exists, (pid, host) differs, last_rollover_at is
//     fresh → operator-actionable refusal naming the conflict + the
//     `--force` override.
//   - [BackupStream.Force] true → bypass the check; log a WARN.
//
// The pid/host pair lives in this function so tests can inject a stub
// (which is also why preflightStreamState is a method on
// [BackupStream] — Force is a field).
func (b *BackupStream) preflightStreamState(ctx context.Context, path string, window time.Duration, pid int, host string, now time.Time) error {
	prior, err := readStreamState(ctx, b.Store, path)
	if err != nil {
		return fmt.Errorf("stream: read existing stream_state: %w", err)
	}
	if prior == nil {
		return nil
	}
	freshThreshold := now.Add(-streamStateFreshness * window)
	stale := prior.LastRolloverAt.Before(freshThreshold)
	sameProcess := prior.PID == pid && prior.Host == host

	if b.Force {
		// Operator-confirmed override. Log a WARN naming what we'd
		// otherwise refuse so the operator knows what they bypassed.
		conflict := "stale prior state"
		if !stale {
			conflict = "fresh prior state from another writer"
		}
		// Use slog from the package's existing import surface; since
		// this function lives in stream_state.go and slog isn't
		// imported here, switch to fmt.Errorf for the WARN content
		// and let the caller log. Simpler: emit via a local helper.
		warnConcurrentWriterOverride(ctx, prior, conflict)
		return nil
	}

	if sameProcess {
		// Same process re-running (or the OS recycled the pid). Treat
		// as legitimate restart; the state file gets re-written.
		return nil
	}
	if stale {
		// Different writer, but its rollover cadence has stalled past
		// the freshness window. The previous stream crashed without
		// cleanup; take over with a soft signal.
		warnConcurrentWriterTakeover(ctx, prior)
		return nil
	}
	// Different writer, fresh state — refuse loudly.
	return fmt.Errorf(
		"stream: a stream is already running against this destination (pid=%d host=%q last_rollover_at=%s); to take over, stop it via `sluice backup stream stop` or pass --force",
		prior.PID, prior.Host, prior.LastRolloverAt.UTC().Format(time.RFC3339),
	)
}

// warnConcurrentWriterOverride and warnConcurrentWriterTakeover live as
// thin wrappers around slog in stream.go; declared here as forward
// declarations to keep stream_state.go's import set lean (no slog
// import in this file). Implementation lives in stream_logging.go.

// RequestStreamStop sets `stop_requested_at` on the state file at
// store's [DefaultStreamStateFilename] path so the running stream
// observes the request on its next rollover-tick poll and exits
// cleanly. Returns the prior state file (for log lines naming the
// running pid/host) and any error.
//
// Operator surface: `sluice backup stream stop --target=<url>`. Works
// cross-machine because the destination IS the rendezvous point —
// machine A's stream + machine B's stop command don't need to know
// about each other directly; both agree on the backup destination.
//
// Idempotent: calling RequestStreamStop on a destination whose state
// file already carries a stop_requested_at value preserves the
// existing timestamp (re-issuing stop doesn't reset the clock for
// whatever drain-completion tooling watches the field).
//
// Returns (nil, error) when the state file is absent — there's no
// running stream to stop. The caller's error message names the
// "no stream is running" case explicitly.
func RequestStreamStop(ctx context.Context, store ir.BackupStore, now time.Time) (*streamState, error) {
	return requestStreamStopAt(ctx, store, DefaultStreamStateFilename, now)
}

// requestStreamStopAt is [RequestStreamStop] generalised to a caller-
// supplied path. Tests pin a deterministic path; production callers
// route through RequestStreamStop.
func requestStreamStopAt(ctx context.Context, store ir.BackupStore, path string, now time.Time) (*streamState, error) {
	prior, err := readStreamState(ctx, store, path)
	if err != nil {
		return nil, fmt.Errorf("stream stop: read state file: %w", err)
	}
	if prior == nil {
		return nil, fmt.Errorf("stream stop: no stream_state.json at %q; either no stream is running against this destination, or it hasn't written its first rollover yet",
			path)
	}
	if prior.StopRequestedAt == nil {
		t := now.UTC()
		prior.StopRequestedAt = &t
	}
	if err := writeStreamState(ctx, store, path, prior); err != nil {
		return nil, fmt.Errorf("stream stop: write state file: %w", err)
	}
	return prior, nil
}
