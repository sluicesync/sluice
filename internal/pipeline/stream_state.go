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
//     process — unless the file records a handoff, see below.
//     Operator overrides via [BackupStream.Force].
//   - Cross-machine stop signaling. `sluice backup stream stop` writes
//     `stop_requested_at` to the file; the running stream polls
//     between rollovers and exits cleanly when the field is set.
//   - Handoff. On a clean exit the stream stamps `stopped_at`, so the
//     next stream against the destination takes over immediately
//     instead of waiting out the staleness window ([priorHandedOff]).
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
	"log/slog"
	"time"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
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

	// StoppedAt, when non-nil, records that the owner named by
	// (PID, Host) finished its last rollover and RETURNED — the
	// positive "this destination is free" signal. Written by
	// [BackupStream.recordCleanExit] on every clean [BackupStream.Run]
	// return (stop-requested, ctx-cancel drain, source exhaustion,
	// rollover budget reached). Without it a completed handoff is
	// indistinguishable from a crashed owner and the next stream has
	// to wait out the staleness window — see [priorHandedOff].
	StoppedAt *time.Time `json:"stopped_at,omitempty"`
}

// readStreamState loads the state file at path from store. Returns
// (nil, nil) when the file doesn't exist (the common cold-start case);
// (nil, err) when the file exists but can't be decoded.
func readStreamState(ctx context.Context, store irbackup.Store, path string) (*streamState, error) {
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
// content.
//
// **Multiple writers, by design.** The file is shared between the
// running stream (writes liveness fields: PID, Host, StartedAt,
// LastRolloverAt) and `sluice backup stream stop` (writes
// StopRequestedAt). Treating the file as last-writer-wins led to the
// Bug 37 clobber: the stream's heartbeat write at a rollover boundary
// landed AFTER an operator's RequestStreamStop, overwriting the
// operator's stop_requested_at with the stream's in-memory state (no
// stop). For heartbeat writes use [writeStreamStateMergeHeartbeat]
// instead; it does a read-modify-write that preserves any concurrent
// StopRequestedAt the operator wrote in the race window.
//
// The plain writeStreamState is reserved for paths that legitimately
// own the entire file content (initial Run setup, takeover with
// --force).
func writeStreamState(ctx context.Context, store irbackup.Store, path string, s *streamState) error {
	body, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("stream state: marshal: %w", err)
	}
	return store.Put(ctx, path, bytes.NewReader(body))
}

// writeStreamStateMergeHeartbeat is the heartbeat-write helper that
// fixes Bug 37's clobber race. The stream's per-rollover heartbeat
// write needs to advance LastRolloverAt without obliterating any
// StopRequestedAt the operator's `sluice backup stream stop` wrote in
// the read-window-followed-by-write-window race.
//
// Read-modify-write semantics: read the current file (if any), copy
// StopRequestedAt forward into the supplied state (so the heartbeat
// write doesn't clear it), then write. Returns true when a concurrent
// stop was observed during the merge — callers can use this to
// short-circuit further heartbeat / state work and trigger a clean
// exit without the next outer-loop poll having to discover the stop.
//
// Limitation: this is a TOCTOU pattern over a non-atomic store. If
// `RequestStreamStop` writes between this function's read and write,
// the stop is still lost. The window is much smaller than the original
// (full-rollover-cadence) clobber window — milliseconds vs seconds —
// and the in-process channel notification path (registered via
// [registerStreamStopChan] in stream.go) closes the same-process case
// entirely. For cross-process operators on shared filesystems the
// remaining tiny TOCTOU window is bounded by the read/write latency
// of a single state-file flush; the next rollover-boundary heartbeat
// gets another chance.
func writeStreamStateMergeHeartbeat(ctx context.Context, store irbackup.Store, path string, s *streamState) (stopObserved bool, err error) {
	prior, err := readStreamState(ctx, store, path)
	if err != nil {
		return false, err
	}
	if prior != nil && prior.StopRequestedAt != nil && s.StopRequestedAt == nil {
		s.StopRequestedAt = prior.StopRequestedAt
		stopObserved = true
	}
	if err := writeStreamState(ctx, store, path, s); err != nil {
		return stopObserved, err
	}
	return stopObserved, nil
}

// readStreamStopRequested returns the StopRequestedAt timestamp from
// the state file, or nil if the file doesn't exist or doesn't carry a
// stop request. Errors from the store surface to the caller; missing
// file is (nil, nil).
func readStreamStopRequested(ctx context.Context, store irbackup.Store, path string) (*time.Time, error) {
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
//   - State file exists, (pid, host) differs, but the file records a
//     HANDOFF ([priorHandedOff]) → the prior owner is gone by its own
//     account; take over immediately at INFO, no staleness wait.
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
		warnConcurrentWriterOverride(ctx, prior, conflict)
		return nil
	}

	if sameProcess {
		// Same process re-running (or the OS recycled the pid). Treat
		// as legitimate restart; the state file gets re-written.
		return nil
	}
	if handoff, evidence := priorHandedOff(prior); handoff {
		// The prior owner recorded that it is gone. Not a competitor —
		// take over now, and at INFO: a supervisor/k8s restart loop is
		// the DOCUMENTED shape here (ADR-0087), so it must not be
		// dressed up as an anomaly.
		logConcurrentWriterHandoff(ctx, prior, evidence)
		return nil
	}
	if stale {
		// Different writer, but its rollover cadence has stalled past
		// the freshness window. The previous stream crashed without
		// cleanup; take over with a soft signal.
		warnConcurrentWriterTakeover(ctx, prior)
		return nil
	}
	// Different writer, fresh state — refuse loudly. The remedy must
	// name an action the operator has NOT already taken: when a stop is
	// already recorded (and the owner has rolled over since, so this is
	// a still-draining stream rather than a handoff), telling them to
	// run `backup stream stop` again is a no-op instruction against a
	// signal that is already in the file we just read.
	remedy := "to take over, stop it via `sluice backup stream stop` or pass --force"
	if prior.StopRequestedAt != nil {
		remedy = fmt.Sprintf(
			"a stop was already requested at %s and the stream has committed a rollover since, so it is still draining; wait for it to exit (it exits on its next rollover-tick) or pass --force",
			prior.StopRequestedAt.UTC().Format(time.RFC3339),
		)
	}
	return fmt.Errorf(
		"stream: a stream is already running against this destination (pid=%d host=%q last_rollover_at=%s); %s",
		prior.PID, prior.Host, prior.LastRolloverAt.UTC().Format(time.RFC3339), remedy,
	)
}

// priorHandedOff reports whether prior records a HANDOFF — the owner
// declaring itself gone — rather than a live competitor, and returns
// the evidence for the log line. Two shapes qualify:
//
//   - StoppedAt set. The owner's [BackupStream.recordCleanExit] ran, so
//     it committed its last rollover and returned. Unambiguous.
//   - StopRequestedAt strictly after LastRolloverAt. The operator ran
//     `sluice backup stream stop` and the owner has not written a
//     heartbeat since. Covers the crashed-mid-drain owner and state
//     files written by a sluice that predates StoppedAt.
//
// **The residual, named: the drain window.** The second shape also
// holds for the seconds between `stop` being written and the owner
// actually exiting — `backup stream stop` writes the signal and
// returns without waiting. Taking over there overlaps two writers.
// It is accepted deliberately, and it is narrow: the operator has
// explicitly declared this owner unwanted, the owner eager-exits on
// its next stop-poll tick, and the alternative — the pre-fix
// behaviour — is a hard rc=1 on the documented supervisor-restart
// path with a remedy that cannot help. The unambiguous StoppedAt
// shape is what a current-version clean stop actually produces; the
// second shape is the compatibility/crash fallback.
//
// The crashed-mid-rollover owner needs no special handling here: a
// takeover runs [recoverRotationState] and re-resolves the open
// segment during setup regardless of WHICH branch admitted it, so this
// function only changes WHEN we take over, never WHAT we do next.
func priorHandedOff(prior *streamState) (handoff bool, evidence string) {
	if prior.StoppedAt != nil {
		return true, "prior stream recorded a clean exit"
	}
	if prior.StopRequestedAt != nil && prior.StopRequestedAt.After(prior.LastRolloverAt) {
		return true, "prior stream was asked to stop and wrote no rollover after the request"
	}
	return false, ""
}

// recordCleanExit stamps `stopped_at` on the state file so the NEXT
// `backup stream` against this destination can tell a completed
// handoff from a crashed owner (see [priorHandedOff]). Called from
// [BackupStream.Run]'s deferred clean-exit hook; best-effort, because
// the chain — not this file — is the source of truth, and a failure to
// stamp only costs the next stream a staleness wait.
//
// Two details are load-bearing:
//
//   - The write runs on a cancel-immune, [stopDrainTimeout]-bounded
//     context: the common clean exit is a SIGTERM drain, so the
//     caller's ctx is already cancelled by the time we get here.
//   - It refuses to write when the file no longer names (pid, host).
//     A --force takeover (or a handoff takeover racing our drain)
//     means someone else owns this destination now, and stamping our
//     exit over THEIR liveness record would hand the destination away
//     while they are actively writing it.
func (b *BackupStream) recordCleanExit(ctx context.Context, path string, pid int, host string, now func() time.Time) {
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopDrainTimeout)
	defer cancel()
	prior, err := readStreamState(writeCtx, b.Store, path)
	if err != nil {
		warnCleanExitNotRecorded(writeCtx, path, err.Error())
		return
	}
	if prior == nil {
		warnCleanExitNotRecorded(writeCtx, path, "state file is gone")
		return
	}
	if prior.PID != pid || prior.Host != host {
		warnCleanExitNotRecorded(writeCtx, path, "destination was taken over by another writer while we drained")
		return
	}
	t := now().UTC()
	prior.StoppedAt = &t
	if err := writeStreamState(writeCtx, b.Store, path, prior); err != nil {
		warnCleanExitNotRecorded(writeCtx, path, err.Error())
	}
}

// warnConcurrentWriterOverride / warnConcurrentWriterTakeover /
// logConcurrentWriterHandoff / warnCleanExitNotRecorded are thin slog
// wrappers implemented in stream_logging.go.

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
func RequestStreamStop(ctx context.Context, store irbackup.Store, now time.Time) (*streamState, error) {
	return requestStreamStopAt(ctx, store, DefaultStreamStateFilename, now)
}

// requestStreamStopAt is [RequestStreamStop] generalised to a caller-
// supplied path. Tests pin a deterministic path; production callers
// route through RequestStreamStop.
func requestStreamStopAt(ctx context.Context, store irbackup.Store, path string, now time.Time) (*streamState, error) {
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
	// In-process notification (Bug 37 fix; v0.19.1): when the running
	// stream is in the same process as RequestStreamStop (CLI
	// single-binary, integration test, `sync from-backup` consumer
	// holding a stream inline), close the process-local stop channel
	// so the running captureWindow exits immediately without waiting
	// for a file-poll tick. No-op when cross-process (channel is
	// registered per-process, so a stop command issued on machine B
	// against a stream on machine A finds no registered channel and
	// the file remains the cross-machine rendezvous). See
	// [notifyStreamStop] / [registerStreamStopChan] in
	// stream_stop_registry.go.
	if notifyStreamStop(store) {
		slog.DebugContext(
			ctx, "stream stop: in-process channel signalled",
			slog.String("path", path),
		)
	}
	return prior, nil
}
