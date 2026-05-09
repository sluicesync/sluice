// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Continuous-incremental long-running stream orchestrator. Phase 4 of
// the logical-backup feature (`docs/dev/design-logical-backups-phase-4.md`):
// a single long-running process that produces rolling incrementals at a
// configured cadence, no per-incremental cron orchestration.
//
// Shape:
//
//   - Construct [BackupStream] with engine + DSN + parent reference +
//     rollover policy.
//   - Call [BackupStream.Run] with a context.
//   - The orchestrator drives a `for { rollover() }` loop; each
//     rollover is a bounded window (time / change-count / byte ceilings,
//     first-fired wins) that produces one new manifest under
//     `manifests/incr-<unix-millis>-<seq>.json`.
//   - Window extends to next TxCommit so the chain doesn't end mid-tx
//     (mirrors Phase 3.1's incremental orchestrator).
//   - Empty rollovers are skipped by default; opt-in via
//     [BackupStream.IncludeEmptyRollovers].
//
// The CDC pump is opened ONCE at the start of [BackupStream.Run] and
// reused across rollovers — every rollover commits its window's
// EndPosition as the next rollover's StartPosition without re-opening
// the slot. This is the load-bearing efficiency win over a tight
// `for { incremental.Run() }` loop, which would re-open the slot every
// iteration.
//
// Stop semantics:
//
//   - ctx cancellation (SIGINT / SIGTERM via kongContext): finish the
//     current in-flight rollover (commit manifest + chunks + state),
//     then exit cleanly. Bounded by [stopDrainTimeout].
//   - Cross-machine stop request via `manifests/stream_state.json`'s
//     `stop_requested_at` field: same drain path. Polled between
//     rollovers so the operator's stop is observed within the
//     rollover-tick interval (≤ rollover-window).
//
// Concurrent-writer protection:
//
//   - On startup, [BackupStream.Run] reads any existing
//     `manifests/stream_state.json` and refuses to start if
//     `last_rollover_at` is recent (`< 2 × rollover-window`) AND the
//     (pid, host) doesn't match the current process. Operator overrides
//     via [BackupStream.Force].
//   - Every successful rollover updates `last_rollover_at` so a
//     subsequent run knows the stream is alive.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"

	"github.com/orware/sluice/internal/crypto"
	"github.com/orware/sluice/internal/ir"
)

// DefaultRolloverWindow is the wall-clock cadence each rollover commits
// at when [BackupStream.RolloverWindow] is left zero. 5 minutes mirrors
// [DefaultIncrementalWindow] so operators tuning one knob don't have to
// re-tune the other.
const DefaultRolloverWindow = 5 * time.Minute

// DefaultRolloverMaxChanges is the change-count ceiling each rollover
// commits at when [BackupStream.RolloverMaxChanges] is left zero.
// 100,000 mirrors [DefaultIncrementalChunkChanges] / Phase 2's
// per-chunk row count — the change-chunk format is row-shaped enough
// that the same bound makes sense.
const DefaultRolloverMaxChanges = 100_000

// DefaultRolloverMaxBytes is the buffered-bytes ceiling each rollover
// commits at when [BackupStream.RolloverMaxBytes] is left zero.
// 64 MiB mirrors the existing `--max-buffer-bytes` shape from Phase 2's
// backup writer.
const DefaultRolloverMaxBytes int64 = 64 << 20

// DefaultStreamStateFilename is the path within the store the stream-
// state liveness file is written to. Lives under [incrementalManifestPrefix]
// so a single `List(manifests/)` call enumerates both manifests AND
// the state file (callers that only want manifests filter on
// `incr-` prefix).
const DefaultStreamStateFilename = incrementalManifestPrefix + "stream_state.json"

// DefaultRolloverHookTimeout bounds how long the post-rollover hook is
// allowed to run before the stream gives up and warns. 30 s matches
// [stopDrainTimeout]'s envelope for "should be done by now" assertions.
const DefaultRolloverHookTimeout = 30 * time.Second

// streamStateFreshness is the multiplier applied to RolloverWindow when
// deciding whether an existing `stream_state.json` represents a live
// concurrent writer. A `last_rollover_at` newer than `now - 2*window`
// indicates a stream that's still ticking; older means the previous
// stream crashed and didn't clean up. Tuneable only via tests.
const streamStateFreshness = 2

// streamStopPollInterval is the cadence the in-rollover stop-signal
// poll runs at. Decoupled from RolloverWindow so an operator's
// `sluice backup stream stop` is observed within ~1 s, regardless of
// the (typically minutes-long) rollover-window setting. Mirrors the
// streamer's [stopSignalPollInterval] in spirit, but uses a tighter
// 1 s cadence because a backup stream's "exit-on-stop" budget is
// usually integration-test-tight (10 s end-to-end).
const streamStopPollInterval = 1 * time.Second

// BackupStream runs a continuous-incremental long-running stream
// against Source / SourceDSN. Construct the value, then call Run. Run
// blocks until ctx is cancelled or a stop request is observed via
// `stream_state.json`.
//
// BackupStream does not retain state between Run calls. Concurrent
// calls on the same value are not supported.
type BackupStream struct {
	// Source is the engine the source DSN belongs to. Must declare
	// CDC support (Capabilities().CDC != ir.CDCNone). Required.
	Source ir.Engine

	// SourceDSN is the source-engine-native connection string.
	// Required.
	SourceDSN string

	// Store is the [ir.BackupStore] the parent manifest lives in and
	// every rolled manifest + chunks are written to. Required.
	Store ir.BackupStore

	// ParentRef identifies the parent backup the stream chains off.
	// Either a [ir.Manifest.BackupID] (e.g. "abc123def4567890") or the
	// empty string to chain off the most recent manifest in Store.
	// Required for clean chains.
	ParentRef string

	// SlotName, on engines with a slot concept (Postgres), overrides
	// the engine's default replication-slot name. Engines without
	// slots (MySQL: binlog stream is the slot) ignore the field.
	SlotName string

	// RolloverWindow bounds the wall-clock duration each rollover
	// streams CDC events for before committing a manifest. Zero falls
	// back to [DefaultRolloverWindow]. First of RolloverWindow,
	// RolloverMaxChanges, or RolloverMaxBytes to fire closes the
	// rollover.
	RolloverWindow time.Duration

	// RolloverMaxChanges bounds the total number of [ir.Change] events
	// a single rollover captures. Zero disables the cap (window /
	// bytes only). The cap is approximate — a TxBegin/Commit pair
	// straddling the boundary is allowed to complete so the chain
	// doesn't end mid-transaction.
	RolloverMaxChanges int

	// RolloverMaxBytes bounds the buffered chunk bytes a single
	// rollover may accumulate before committing. Zero falls back to
	// [DefaultRolloverMaxBytes]. Mirrors the existing
	// `--max-buffer-bytes` shape; the bound is checked at chunk-flush
	// boundaries, so actual buffered bytes may transiently exceed the
	// ceiling by up to one chunk's worth.
	RolloverMaxBytes int64

	// ChunkChanges is the per-chunk change-event count. Zero falls
	// back to [DefaultIncrementalChunkChanges]. The writer rolls over
	// to a new chunk file whenever the current one hits this count.
	ChunkChanges int

	// IncludeEmptyRollovers, when true, commits a manifest for a
	// rollover that captured zero changes. Default false (skip empty
	// rollovers — `stream_state.json` covers liveness without
	// polluting the chain).
	IncludeEmptyRollovers bool

	// Force, when true, bypasses the concurrent-writer check at
	// startup. Operator-confirmed: "I'm taking over this destination
	// from a previous stream that may still be running." Logs a WARN
	// when the bypass is exercised.
	Force bool

	// RolloverHook is an optional shell command invoked after each
	// rollover commits successfully. The hook receives env vars
	// SLUICE_ROLLOVER_MANIFEST_PATH, SLUICE_ROLLOVER_PARENT_BACKUP_ID,
	// SLUICE_ROLLOVER_BACKUP_ID, SLUICE_ROLLOVER_CHANGES,
	// SLUICE_ROLLOVER_BYTES, SLUICE_ROLLOVER_ELAPSED_MS. Hook errors
	// are WARN-logged but do NOT fail the stream. Bounded by
	// [DefaultRolloverHookTimeout].
	RolloverHook string

	// SluiceVersion is the build identifier of the running binary,
	// recorded on every manifest. Optional.
	SluiceVersion string

	// Encryption, when non-nil, encrypts every change chunk written
	// during the stream's lifetime. See [BackupEncryption]. Aligns
	// against the parent's chain encryption at startup; mismatched
	// shapes (encrypt mid-chain or vice versa) are refused there.
	Encryption *BackupEncryption

	// Now, when set, overrides the wall-clock-time source for
	// [ir.Manifest.CreatedAt] and `stream_state.json` timestamps. Used
	// by tests to pin timestamps; in production callers leave it nil
	// and the default uses time.Now.
	Now func() time.Time

	// clockNow is the time source used internally for window-closure
	// timing. Defaults to time.Now; tests can override for
	// deterministic window expiry.
	clockNow func() time.Time

	// streamStatePath overrides the path of the liveness file. Tests
	// that exercise the file shape pin a deterministic path; in
	// production callers leave it empty and the default uses
	// [DefaultStreamStateFilename].
	streamStatePath string

	// pidHostFn returns the (pid, host) pair recorded on the liveness
	// file. Defaults to (os.Getpid, os.Hostname); tests inject a stub
	// to simulate cross-host conflicts.
	pidHostFn func() (int, string)
}

// Run executes the long-running stream. Blocks until ctx is cancelled
// or a stop request is observed via `stream_state.json`. Returns nil
// on a clean exit (graceful drain of the in-flight rollover); a
// wrapped error on any unrecoverable failure.
//
// On every successful rollover: Store gains exactly one new manifest
// under `manifests/incr-<unix-millis>-<seq>.json` and one or more
// change-chunk files under `chunks/_changes/<run-namespace>/`.
// `stream_state.json` is updated with the rollover's commit timestamp.
func (b *BackupStream) Run(ctx context.Context) error {
	if err := b.validate(); err != nil {
		return err
	}

	clockNow := b.clockNow
	if clockNow == nil {
		clockNow = time.Now
	}
	now := time.Now
	if b.Now != nil {
		now = b.Now
	}

	rolloverWindow := b.RolloverWindow
	if rolloverWindow <= 0 {
		rolloverWindow = DefaultRolloverWindow
	}
	maxChanges := b.RolloverMaxChanges
	if maxChanges <= 0 {
		maxChanges = DefaultRolloverMaxChanges
	}
	maxBytes := b.RolloverMaxBytes
	if maxBytes <= 0 {
		maxBytes = DefaultRolloverMaxBytes
	}
	chunkSize := b.ChunkChanges
	if chunkSize <= 0 {
		chunkSize = DefaultIncrementalChunkChanges
	}
	statePath := b.streamStatePath
	if statePath == "" {
		statePath = DefaultStreamStateFilename
	}
	pidHost := b.pidHostFn
	if pidHost == nil {
		pidHost = defaultPidHost
	}
	pid, host := pidHost()

	// 1. Concurrent-writer check.
	if err := b.preflightStreamState(ctx, statePath, rolloverWindow, pid, host, now()); err != nil {
		return err
	}

	// 2. Resolve parent manifest.
	parent, parentPath, err := b.resolveParent(ctx)
	if err != nil {
		return fmt.Errorf("stream: resolve parent: %w", err)
	}
	startPos := parent.EndPosition
	if startPos.Engine == "" && startPos.Token == "" {
		slog.WarnContext(ctx, "stream: parent manifest has no EndPosition; chain will start from CDC's current position",
			slog.String("parent_path", parentPath),
		)
	}

	// 2.5. Phase 6.1: align with chain encryption. Refuses early if
	// the parent's chain encryption shape doesn't match the operator's
	// supplied envelope (or vice versa).
	chainCEK, err := b.alignEncryption(parent)
	if err != nil {
		return fmt.Errorf("stream: encryption: %w", err)
	}

	// 3. Open CDC pump once for the lifetime of the stream.
	cdc, err := openCDCReaderWithSlot(ctx, b.Source, b.SourceDSN, b.SlotName)
	if err != nil {
		return wrapWithHint(PhaseConnect, fmt.Errorf("stream: open cdc reader: %w", err))
	}
	defer closeIf(cdc)

	changesCh, err := cdc.StreamChanges(ctx, startPos)
	if err != nil {
		if errors.Is(err, ir.ErrPositionInvalid) {
			return fmt.Errorf("stream: source has pruned past parent's terminal position; take a fresh full backup or shorten the chain interval. Underlying: %w", err)
		}
		return wrapWithHint(PhaseCDC, fmt.Errorf("stream: start cdc stream: %w", err))
	}

	// 4. Initial state file write.
	state := &streamState{
		PID:            pid,
		Host:           host,
		StartedAt:      now().UTC(),
		LastRolloverAt: now().UTC(),
	}
	if err := writeStreamState(ctx, b.Store, statePath, state); err != nil {
		return fmt.Errorf("stream: write initial state: %w", err)
	}

	// Register the in-process stop channel so a same-process
	// RequestStreamStop can signal us instantaneously without going
	// through the file-poll path (Bug 37 fix). Cross-process operators
	// still go through the file (notifyStreamStop is a no-op for them).
	stopCh, deregisterStopCh := registerStreamStopChan(b.Store)
	defer deregisterStopCh()

	slog.InfoContext(ctx, "stream: started",
		slog.String("source_engine", b.Source.Name()),
		slog.String("parent_backup_id", parent.BackupID),
		slog.Duration("rollover_window", rolloverWindow),
		slog.Int("rollover_max_changes", maxChanges),
		slog.Int64("rollover_max_bytes", maxBytes),
	)

	// 5. Drive the rollover loop. The loop runs until ctx is cancelled
	//    or a stop request is observed via state file. Each iteration
	//    is a bounded window producing zero or one manifest. Errors
	//    abort the loop loudly.
	currentParent := parent
	rolloverSeq := 0
	for {
		// Stop-request check (cross-machine stop). A ctx cancel here
		// short-circuits the same way: the captureWindow loop sees
		// ctx.Done and returns; we drop into the cleanup block below.
		if stopReq, sErr := readStreamStopRequested(ctx, b.Store, statePath); sErr != nil {
			slog.WarnContext(ctx, "stream: failed to read stream_state for stop check; will retry on next rollover",
				slog.String("err", sErr.Error()),
			)
		} else if stopReq != nil {
			slog.InfoContext(ctx, "stream: stop requested via stream_state.json; exiting after current rollover",
				slog.Time("requested_at", *stopReq),
			)
			return nil
		}

		// Run one rollover.
		started := clockNow()
		roll, rErr := b.runRollover(ctx, cdc, changesCh, currentParent, startPos, rolloverWindow, maxChanges, maxBytes, chunkSize, statePath, now, clockNow, stopCh, chainCEK)
		elapsed := clockNow().Sub(started)
		if rErr != nil {
			// ctx-cancel during a rollover surfaces here. Per the
			// design doc's SIGTERM contract, a graceful drain commits
			// the in-flight rollover (chunks already flushed inside
			// captureWindow); finalise the manifest here so every
			// change observed before cancel makes it into the chain.
			// Uses a fresh stopDrainTimeout-bounded ctx for the
			// manifest write so a store call against the just-
			// cancelled parent doesn't short-circuit the commit.
			if errors.Is(rErr, context.Canceled) || errors.Is(rErr, context.DeadlineExceeded) {
				if roll.Manifest != nil && len(roll.Manifest.ChangeChunks) > 0 {
					commitCtx, commitCancel := context.WithTimeout(context.WithoutCancel(ctx), stopDrainTimeout)
					manifestPath := buildIncrementalManifestPath(roll.Manifest)
					if err := writeManifestAt(commitCtx, b.Store, manifestPath, roll.Manifest); err != nil {
						slog.WarnContext(ctx, "stream: drain-commit of in-flight manifest failed",
							slog.String("err", err.Error()),
						)
					} else {
						slog.InfoContext(ctx, "stream rollover committed (drain on ctx-cancel)",
							slog.String("manifest_path", manifestPath),
							slog.String("backup_id", roll.Manifest.BackupID),
							slog.Int64("changes", roll.TotalChanges),
							slog.Int64("bytes", roll.TotalBytes),
							slog.Duration("elapsed", elapsed),
						)
					}
					commitCancel()
				} else {
					slog.InfoContext(ctx, "stream: context cancelled during rollover; in-flight rollover not committed",
						slog.Duration("elapsed", elapsed),
					)
				}
				return nil
			}
			return wrapWithHint(PhaseCDC, fmt.Errorf("stream: rollover %d: %w", rolloverSeq, rErr))
		}

		if roll.Manifest == nil {
			// Empty rollover that we skipped per IncludeEmptyRollovers.
			slog.InfoContext(ctx, "stream: rollover empty; skipping manifest write",
				slog.Int("seq", rolloverSeq),
				slog.Duration("elapsed", elapsed),
			)
			if roll.StopRequested {
				// Stop signal observed during the empty window: exit
				// cleanly without overwriting the state file (which now
				// carries stop_requested_at written by the operator's
				// `sluice backup stream stop`; clobbering it would
				// confuse any drain-completion tooling watching the
				// field).
				slog.InfoContext(ctx, "stream: stop requested via stream_state.json during rollover; exiting",
					slog.Int("rollovers", rolloverSeq),
				)
				return nil
			}
			// Update state file's last_rollover_at as a heartbeat even
			// when no manifest was committed — operators monitoring the
			// state file's freshness should see the stream is alive.
			//
			// Bug 37 fix (v0.19.1): use writeStreamStateMergeHeartbeat
			// (read-modify-write) so a concurrent stop_requested_at the
			// operator wrote in the race window between our captureWindow
			// stop-poll and this heartbeat survives intact. If the merge
			// observed a stop, exit immediately rather than starting the
			// next rollover.
			state.LastRolloverAt = now().UTC()
			stopObserved, err := writeStreamStateMergeHeartbeat(ctx, b.Store, statePath, state)
			if err != nil {
				slog.WarnContext(ctx, "stream: failed to update state file after empty rollover",
					slog.String("err", err.Error()),
				)
			}
			if stopObserved {
				slog.InfoContext(ctx, "stream: heartbeat merge observed concurrent stop_requested_at; exiting",
					slog.Int("rollovers", rolloverSeq),
				)
				return nil
			}
			rolloverSeq++
			// Source-channel-closed and skip-empty: the source has gone
			// away (test fakes that emit-then-close, or a long-running
			// engine whose pump terminated). Without this exit, the loop
			// would spin forever producing skip-empty rollovers. In
			// production the same path triggers when the slot becomes
			// invalid mid-stream — we want a loud exit, not a busy spin.
			if roll.SourceClosed {
				slog.InfoContext(ctx, "stream: cdc channel closed; exiting after final empty rollover",
					slog.Int("rollovers", rolloverSeq),
				)
				return nil
			}
			continue
		}

		manifestPath := buildIncrementalManifestPath(roll.Manifest)
		if err := writeManifestAt(ctx, b.Store, manifestPath, roll.Manifest); err != nil {
			return fmt.Errorf("stream: write rollover manifest: %w", err)
		}

		if roll.StopRequested {
			// Stop observed during the window: commit the in-flight
			// manifest (already done above) but skip the
			// state-file last_rollover_at heartbeat write — the
			// state file now carries the operator's
			// stop_requested_at, and writing our heartbeat here would
			// clobber it. Exit cleanly.
			slog.InfoContext(ctx, "stream rollover committed; stop requested via stream_state.json — exiting",
				slog.String("manifest_path", manifestPath),
				slog.String("backup_id", roll.Manifest.BackupID),
				slog.Int64("changes", roll.TotalChanges),
				slog.Int64("bytes", roll.TotalBytes),
				slog.Duration("elapsed", elapsed),
			)
			if b.RolloverHook != "" {
				runRolloverHook(ctx, b.RolloverHook, roll.Manifest, manifestPath, roll.TotalChanges, roll.TotalBytes, elapsed)
			}
			return nil
		}

		// Advance state file's last_rollover_at to mark liveness.
		//
		// Bug 37 fix (v0.19.1): use writeStreamStateMergeHeartbeat
		// (read-modify-write) so a concurrent stop_requested_at survives.
		// If the merge observed a stop, exit immediately rather than
		// starting the next rollover.
		state.LastRolloverAt = now().UTC()
		stopObserved, hbErr := writeStreamStateMergeHeartbeat(ctx, b.Store, statePath, state)
		if hbErr != nil {
			slog.WarnContext(ctx, "stream: failed to update state file after rollover commit",
				slog.String("err", hbErr.Error()),
			)
		}
		if stopObserved {
			slog.InfoContext(ctx, "stream: heartbeat merge observed concurrent stop_requested_at; exiting after committed rollover",
				slog.Int("rollovers", rolloverSeq),
			)
			return nil
		}

		slog.InfoContext(ctx, "stream rollover committed",
			slog.String("manifest_path", manifestPath),
			slog.String("backup_id", roll.Manifest.BackupID),
			slog.String("parent_backup_id", roll.Manifest.ParentBackupID),
			slog.Int64("changes", roll.TotalChanges),
			slog.Int64("bytes", roll.TotalBytes),
			slog.Duration("elapsed", elapsed),
		)

		// Run the rollover hook (best-effort; failures don't fail the stream).
		if b.RolloverHook != "" {
			runRolloverHook(ctx, b.RolloverHook, roll.Manifest, manifestPath, roll.TotalChanges, roll.TotalBytes, elapsed)
		}

		// Advance the chain: next rollover's parent is this rollover's
		// manifest; next rollover's StartPosition is this rollover's
		// EndPosition.
		currentParent = roll.Manifest
		startPos = roll.Manifest.EndPosition
		rolloverSeq++

		// Source-channel-closed: pump terminated (test fakes that
		// emit-then-close, or a real engine whose stream ended). Exit
		// cleanly rather than spinning on an empty channel.
		if roll.SourceClosed {
			slog.InfoContext(ctx, "stream: cdc channel closed after rollover commit; exiting",
				slog.Int("rollovers", rolloverSeq),
			)
			return nil
		}
	}
}

// validate sanity-checks required fields. Mirrors
// [IncrementalBackup.validate] — the CDC-capability gate matters here
// too.
func (b *BackupStream) validate() error {
	switch {
	case b.Source == nil:
		return errors.New("stream: Source engine is nil")
	case b.SourceDSN == "":
		return errors.New("stream: SourceDSN is empty")
	case b.Store == nil:
		return errors.New("stream: Store is nil")
	}
	if b.Source.Capabilities().CDC == ir.CDCNone {
		return fmt.Errorf("stream: source engine %q does not declare CDC support", b.Source.Name())
	}
	return nil
}

// resolveParent finds the parent manifest in the store. Mirrors
// [IncrementalBackup.resolveParent] but doesn't return the path
// because the stream doesn't need it post-resolve.
func (b *BackupStream) resolveParent(ctx context.Context) (*ir.Manifest, string, error) {
	manifests, err := listAllManifests(ctx, b.Store)
	if err != nil {
		return nil, "", err
	}
	if len(manifests) == 0 {
		return nil, "", errors.New("no parent manifest found in store; take a `sluice backup full` first")
	}
	if b.ParentRef != "" {
		for _, m := range manifests {
			id := m.manifest.BackupID
			if id == "" {
				id = ir.ComputeBackupID(m.manifest)
			}
			if id == b.ParentRef {
				return m.manifest, m.path, nil
			}
		}
		return nil, "", fmt.Errorf("parent backup %q not found in store; available: %s",
			b.ParentRef, manifestSummary(manifests))
	}
	// Pick the most recent manifest. Mirrors IncrementalBackup; for a
	// stream re-launching after a crash this naturally picks up the
	// latest committed rollover as the next parent.
	mostRecent := manifests[0]
	for _, m := range manifests[1:] {
		if m.manifest.CreatedAt.After(mostRecent.manifest.CreatedAt) {
			mostRecent = m
		}
	}
	return mostRecent.manifest, mostRecent.path, nil
}

// rolloverOutcome bundles the multi-value result of a single rollover
// run so [runRollover]'s signature stays under gocritic's 5-result
// ceiling. The Manifest field is nil when an empty rollover was
// captured AND IncludeEmptyRollovers is false ("skip the manifest
// write").
type rolloverOutcome struct {
	Manifest      *ir.Manifest
	TotalChanges  int64
	TotalBytes    int64
	SourceClosed  bool
	StopRequested bool
}

// runRollover executes one bounded rollover window. Returns the
// outcome (manifest, counts, sourceClosed, stopRequested) plus any
// fatal error.
//
// When IncludeEmptyRollovers is false and the window captured zero
// changes, the returned outcome's Manifest is nil to signal "skip the
// manifest write."
func (b *BackupStream) runRollover(
	ctx context.Context,
	cdc ir.CDCReader,
	changesCh <-chan ir.Change,
	parent *ir.Manifest,
	startPos ir.Position,
	window time.Duration,
	maxChanges int,
	maxBytes int64,
	chunkSize int,
	statePath string,
	now func() time.Time,
	clockNow func() time.Time,
	stopCh <-chan struct{},
	chainCEK []byte,
) (rolloverOutcome, error) {
	beforeSchema := parent.Schema
	beforeHash, hashErr := ir.ComputeSchemaHash(beforeSchema)
	if hashErr != nil {
		return rolloverOutcome{}, fmt.Errorf("hash source schema: %w", hashErr)
	}

	manifest := &ir.Manifest{
		FormatVersion:  ir.BackupFormatVersion,
		SluiceVersion:  b.SluiceVersion,
		CreatedAt:      now().UTC(),
		SourceEngine:   b.Source.Name(),
		Schema:         beforeSchema,
		PartialState:   ir.BackupStateInProgress,
		Kind:           ir.BackupKindIncremental,
		ParentBackupID: parent.BackupID,
		StartPosition:  startPos,
		SchemaHash:     beforeHash,
	}
	if manifest.ParentBackupID == "" {
		manifest.ParentBackupID = ir.ComputeBackupID(parent)
	}

	deadline := clockNow().Add(window)
	captured, captureErr := b.captureWindow(ctx, cdc, changesCh, manifest, chunkSize, deadline, maxChanges, maxBytes, statePath, clockNow, stopCh, chainCEK)
	out := rolloverOutcome{
		TotalChanges:  captured.TotalChanges,
		TotalBytes:    captured.TotalBytes,
		SourceClosed:  captured.SourceClosed,
		StopRequested: captured.StopRequested,
	}

	// Empty rollover handling: when no changes captured AND the
	// operator hasn't asked to include them, return outcome with
	// Manifest=nil so the caller skips the manifest write. Applies
	// even on ctx-cancel: nothing observed → nothing to commit.
	if captured.TotalChanges == 0 && !b.IncludeEmptyRollovers {
		return out, captureErr
	}

	manifest.EndPosition = captured.EndPos
	if (manifest.EndPosition.Engine == "" && manifest.EndPosition.Token == "") && captured.TotalChanges == 0 {
		// Empty rollover written-anyway path: the chain advances no
		// position. Use the StartPosition as the EndPosition so the
		// next rollover still has a valid resume cursor.
		manifest.EndPosition = startPos
	}

	// Bug 38 fix (v0.20.1): refresh source schema at the rollover
	// boundary and diff against the parent's schema. Previously the
	// stream baked `parent.Schema` into every rollover's manifest
	// without ever re-reading — meaning a `ALTER TABLE ADD COLUMN`
	// on the source was never reflected in subsequent manifests, and
	// a downstream broker hit "column does not exist" the moment it
	// tried to apply a row referencing the new column. Mirrors
	// [IncrementalBackup.Run]'s post-window schema-diff path; same
	// SchemaDelta IR field the broker's ApplyChain consumes (Phase
	// 3.2). Refresh cost is one cheap query per engine
	// (information_schema.columns / pg_class+pg_attribute), bounded
	// by table count.
	//
	// Drain-commit carve-out (v0.20.1 regression fix): on ctx-cancel
	// we still want to commit whatever chunks were flushed inside
	// captureWindow's `case <-ctx.Done()` branch. Calling
	// OpenSchemaReader on the cancelled ctx fails immediately, which
	// pre-fix left out.Manifest=nil and the outer drain-commit path
	// silently dropped the in-flight rollover (the LAST window's
	// changes — e.g. user24 in TestBackupStream_*RolloverByMaxChanges).
	// On ctx-cancel we skip the schema-refresh and commit with the
	// parent's schema; the next stream run reads schema fresh on its
	// first rollover and any source DDL that happened during the
	// cancelled window is captured there as a SchemaDelta against the
	// drain-commit's terminal manifest.
	if !errors.Is(captureErr, context.Canceled) && !errors.Is(captureErr, context.DeadlineExceeded) {
		if err := b.refreshSchemaAndAttachDelta(ctx, manifest, beforeSchema); err != nil {
			return out, err
		}
	}

	manifest.BackupID = ir.ComputeBackupID(manifest)
	manifest.PartialState = ir.BackupStateComplete
	out.Manifest = manifest
	return out, captureErr
}

// refreshSchemaAndAttachDelta re-reads the source schema, diffs it
// against the parent's schema, and attaches any [ir.SchemaDeltaEntry]
// entries to manifest. When the diff is empty the manifest's recorded
// Schema stays as the parent's; when entries are produced, the
// manifest's Schema is replaced with the post-window source shape +
// SchemaHash recomputed (mirrors [IncrementalBackup.Run]'s post-
// window diff path).
//
// Errors here surface as fatal — a stream that can't refresh schema
// loses the ability to keep the broker in sync with source DDL, so
// loud-failure beats silent stale-manifest. Bug 38 fix (v0.20.1).
func (b *BackupStream) refreshSchemaAndAttachDelta(
	ctx context.Context,
	manifest *ir.Manifest,
	beforeSchema *ir.Schema,
) error {
	sr, err := b.Source.OpenSchemaReader(ctx, b.SourceDSN)
	if err != nil {
		return fmt.Errorf("rollover: open schema reader: %w", err)
	}
	defer closeIf(sr)
	afterSchema, err := sr.ReadSchema(ctx)
	if err != nil {
		return fmt.Errorf("rollover: read source schema: %w", err)
	}
	delta := diffSchemas(beforeSchema, afterSchema)
	if len(delta) == 0 {
		return nil
	}
	manifest.SchemaDelta = delta
	manifest.Schema = afterSchema
	afterHash, err := ir.ComputeSchemaHash(afterSchema)
	if err != nil {
		return fmt.Errorf("rollover: hash post-window schema: %w", err)
	}
	manifest.SchemaHash = afterHash
	slog.InfoContext(ctx, "stream: schema delta detected at rollover",
		slog.Int("delta_count", len(delta)),
	)
	return nil
}

// captureOutcome bundles the multi-value result of one
// [BackupStream.captureWindow] call. Lives alongside [rolloverOutcome]
// so each layer of the rollover machinery returns one struct + one
// error rather than a long positional argument list (gocritic's 5-result
// ceiling).
type captureOutcome struct {
	EndPos        ir.Position
	TotalChanges  int64
	TotalBytes    int64
	SourceClosed  bool
	StopRequested bool
}

// captureWindow drains changes from changesCh into chunks staged on
// manifest, closing on the first of: deadline reached, totalChanges ≥
// maxChanges, totalBytes ≥ maxBytes, an in-process stop signal, or a
// cross-machine stop request observed via `stream_state.json`. Mirrors
// [IncrementalBackup.captureWindow] but adds a byte-bound, an in-loop
// stop poll (decoupled from rollover cadence), and tracks totalBytes
// across chunks for the rollover-hook env contract.
//
// Window-end straddle behaviour: an open transaction (TxBegin observed
// without TxCommit) extends the window by up to one transaction so the
// chain doesn't end mid-tx — same as Phase 3.1.
//
// Two stop-signal paths (Bug 37 fix; v0.19.1):
//
//  1. **In-process channel** (`stopCh`). Closed by [notifyStreamStop]
//     when [RequestStreamStop] runs in the same Go process. Detected
//     via the new select case immediately; bypasses file I/O entirely
//     so it can't be starved or clobbered. The integration tests
//     (`pipeline.RequestStreamStop` in the same process as the running
//     stream) take this path; CLI single-binary subcommand setups also
//     register against the same registry.
//
//  2. **File poll** ([streamStopPollInterval], 1 s by default). Reads
//     `stream_state.json`'s `stop_requested_at` field. The cross-
//     machine rendezvous: an operator on machine B running
//     `sluice backup stream stop --target=<url>` against a stream on
//     machine A only has the file path. The poll cadence is decoupled
//     from rollover-window so observation is bounded by ~1 s
//     regardless of the (typically minutes-long) rollover-window
//     setting.
//
// On first observation via either path, the in-flight rollover flushes
// (commits chunks staged so far — may be a partial mid-transaction
// chunk) and returns IMMEDIATELY with [captureOutcome.StopRequested]=
// true so the outer loop can finalise the manifest and exit cleanly.
// Eager exit (rather than wait-for-next-tx-boundary) is load-bearing:
// on a quiet source the next tx boundary may never arrive within the
// operator's drain budget. The chain may end mid-tx in the stop case;
// this is the correct trade — operator issued stop, exit promptly.
//
// Returns the captured outcome and any fatal error.
func (b *BackupStream) captureWindow(
	ctx context.Context,
	cdc ir.CDCReader,
	changesCh <-chan ir.Change,
	manifest *ir.Manifest,
	chunkSize int,
	deadline time.Time,
	maxChanges int,
	maxBytes int64,
	statePath string,
	clockNow func() time.Time,
	stopCh <-chan struct{},
	chainCEK []byte,
) (captureOutcome, error) {
	var (
		writer        *changeChunkWriter
		buf           *bytes.Buffer
		chunkIdx      int
		inTransaction bool
		out           captureOutcome
		curWrappedCEK []byte
	)

	runNamespace := changeChunkRunNamespace(manifest)

	flushTo := func(putCtx context.Context) error {
		if writer == nil {
			return nil
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("close chunk: %w", err)
		}
		path := changeChunkPath(runNamespace, chunkIdx)
		hash := writer.Hash()
		nb := int64(buf.Len())
		if err := b.Store.Put(putCtx, path, buf); err != nil {
			return fmt.Errorf("store put %q: %w", path, err)
		}
		ci := &ir.ChunkInfo{
			File:     path,
			RowCount: writer.ChangeCount(),
			SHA256:   hash,
		}
		if b.Encryption != nil {
			ci.Encryption = &ir.ChunkEncryption{
				Algorithm:  crypto.AlgorithmAESGCM,
				NonceLen:   crypto.NonceLen,
				AuthTagLen: crypto.AuthTagLen,
				WrappedCEK: curWrappedCEK,
			}
		}
		manifest.ChangeChunks = append(manifest.ChangeChunks, ci)
		out.TotalBytes += nb
		writer = nil
		buf = nil
		curWrappedCEK = nil
		chunkIdx++
		return nil
	}
	flush := func() error { return flushTo(ctx) }

	openWriter := func() error {
		buf = &bytes.Buffer{}
		cek, wrapped, err := b.resolveChunkCEK(chainCEK)
		if err != nil {
			return fmt.Errorf("resolve chunk cek: %w", err)
		}
		curWrappedCEK = wrapped
		w, err := newChangeChunkWriter(buf, cek)
		if err != nil {
			return fmt.Errorf("open chunk: %w", err)
		}
		writer = w
		return nil
	}

	timer := time.NewTimer(deadline.Sub(clockNow()))
	defer timer.Stop()

	// Stop-poll ticker: decoupled from rollover-window cadence so an
	// operator's `sluice backup stream stop` is observed promptly
	// regardless of how long the current window has left to run.
	// Reads `stream_state.json` directly; on first observation, sets
	// out.StopRequested so the outer loop knows to exit cleanly after
	// this rollover commits.
	stopPoll := time.NewTicker(streamStopPollInterval)
	defer stopPoll.Stop()

	deadlinePassed := false
	for {
		select {
		case <-ctx.Done():
			// Graceful drain: commit whatever's been captured to the
			// in-flight chunk so the rollover's manifest (written by
			// the caller) covers every change observed before cancel.
			// Mirrors the design doc's SIGTERM "commit current
			// in-flight rollover" contract. Uses a fresh
			// stopDrainTimeout-bounded ctx for the chunk write so a
			// store call against the just-cancelled parent ctx doesn't
			// short-circuit the flush.
			if !inTransaction && writer != nil {
				flushCtx, flushCancel := context.WithTimeout(context.WithoutCancel(ctx), stopDrainTimeout)
				if fErr := flushTo(flushCtx); fErr != nil {
					slog.WarnContext(ctx, "stream: drain-flush failed; in-flight chunk dropped",
						slog.String("err", fErr.Error()),
					)
				}
				flushCancel()
			}
			return out, ctx.Err()
		case <-stopCh:
			// In-process stop signal (Bug 37 fix; v0.19.1). Closed by
			// [notifyStreamStop] when [RequestStreamStop] runs in the
			// same Go process. No file I/O, no select-loop starvation,
			// no clobber-race window — same-process operators get
			// instantaneous observation. Cross-process operators take
			// the file-poll path below; this case is just a no-op for
			// them (their stopCh is never closed by a remote process).
			slog.DebugContext(ctx, "stream: in-process stop signal observed; eager exit",
				slog.Int64("changes_so_far", out.TotalChanges),
			)
			out.StopRequested = true
			if err := flush(); err != nil {
				return out, err
			}
			return out, nil
		case <-timer.C:
			deadlinePassed = true
			if !inTransaction {
				if err := flush(); err != nil {
					return out, err
				}
				return out, nil
			}
		case <-stopPoll.C:
			// Cross-machine stop poll. Operator-issued
			// `sluice backup stream stop` expects prompt exit, not
			// "wait for the source to send another tx" — on a quiet
			// source a tx-boundary may never arrive within the
			// operator's drain budget. Eager-exit on first observation:
			// flush whatever's buffered (could be a partial
			// mid-transaction chunk) and return so the outer loop can
			// commit the in-flight manifest and exit. Surfaces as
			// [captureOutcome.StopRequested]=true.
			if out.StopRequested {
				continue
			}
			req, sErr := readStreamStopRequested(ctx, b.Store, statePath)
			if sErr != nil {
				slog.WarnContext(ctx, "stream: stop-poll read failed; will retry on next tick",
					slog.String("err", sErr.Error()),
				)
				continue
			}
			if req == nil {
				continue
			}
			out.StopRequested = true
			if err := flush(); err != nil {
				return out, err
			}
			return out, nil
		case change, ok := <-changesCh:
			if !ok {
				out.SourceClosed = true
				if errReader, ok := cdc.(interface{ Err() error }); ok {
					if e := errReader.Err(); e != nil {
						return out, fmt.Errorf("cdc reader: %w", e)
					}
				}
				if err := flush(); err != nil {
					return out, err
				}
				return out, nil
			}
			switch change.(type) {
			case ir.TxBegin:
				inTransaction = true
			case ir.TxCommit:
				inTransaction = false
			}
			if writer == nil {
				if err := openWriter(); err != nil {
					return out, err
				}
			}
			if err := writer.WriteChange(change); err != nil {
				return out, err
			}
			out.TotalChanges++
			pos := change.Pos()
			if pos.Engine != "" || pos.Token != "" {
				out.EndPos = pos
			}
			// Roll the chunk on per-chunk-row cap.
			if writer.ChangeCount() >= int64(chunkSize) {
				if err := flush(); err != nil {
					return out, err
				}
			}
			// Approximate max-changes cap: close at next tx boundary.
			if maxChanges > 0 && out.TotalChanges >= int64(maxChanges) && !inTransaction {
				if err := flush(); err != nil {
					return out, err
				}
				return out, nil
			}
			// Approximate max-bytes cap: close at next tx boundary
			// once the running total + the in-flight chunk's buffered
			// bytes crosses the ceiling. Checked at chunk-flush
			// boundaries so transient over-shoot is bounded by one
			// chunk's compressed size.
			if maxBytes > 0 && !inTransaction {
				inflightBytes := int64(0)
				if buf != nil {
					inflightBytes = int64(buf.Len())
				}
				if out.TotalBytes+inflightBytes >= maxBytes {
					if err := flush(); err != nil {
						return out, err
					}
					return out, nil
				}
			}
			if (deadlinePassed || out.StopRequested) && !inTransaction {
				if err := flush(); err != nil {
					return out, err
				}
				return out, nil
			}
		}
	}
}

// openCDCReaderWithSlot is the [BackupStream]/[IncrementalBackup]-
// shared helper for opening the engine's CDC reader, honouring an
// optional slot-name override via [ir.CDCReaderWithSlotOpener]. Engines
// without slot concepts log and fall through to the default opener.
func openCDCReaderWithSlot(ctx context.Context, source ir.Engine, dsn, slotName string) (ir.CDCReader, error) {
	if slotName != "" {
		if opener, ok := source.(ir.CDCReaderWithSlotOpener); ok {
			return opener.OpenCDCReaderWithSlot(ctx, dsn, slotName)
		}
		slog.InfoContext(ctx, "stream: --slot-name supplied but engine has no slot concept; ignoring",
			slog.String("engine", source.Name()),
			slog.String("slot_name", slotName),
		)
	}
	return source.OpenCDCReader(ctx, dsn)
}

// runRolloverHook invokes the operator-supplied post-rollover hook
// command with env vars naming the just-committed rollover. Hook
// failures (non-zero exit, timeout, exec error) WARN-log but do NOT
// fail the stream — the rollover already committed, the hook is a
// best-effort notify.
//
// The hook is wrapped in a 30 s timeout (DefaultRolloverHookTimeout)
// derived from the parent ctx. A long-running hook delays the next
// rollover-tick by up to that timeout.
func runRolloverHook(ctx context.Context, hookCmd string, manifest *ir.Manifest, manifestPath string, changes, bytesWritten int64, elapsed time.Duration) {
	hookCtx, cancel := context.WithTimeout(ctx, DefaultRolloverHookTimeout)
	defer cancel()

	// Use sh -c on Unix-y systems; cmd /C on Windows. The exec.Command
	// stdlib helper handles dispatch via the current OS's shell.
	cmd := newShellCommand(hookCtx, hookCmd)
	cmd.Env = append(cmd.Env,
		fmt.Sprintf("SLUICE_ROLLOVER_MANIFEST_PATH=%s", manifestPath),
		fmt.Sprintf("SLUICE_ROLLOVER_PARENT_BACKUP_ID=%s", manifest.ParentBackupID),
		fmt.Sprintf("SLUICE_ROLLOVER_BACKUP_ID=%s", manifest.BackupID),
		fmt.Sprintf("SLUICE_ROLLOVER_CHANGES=%d", changes),
		fmt.Sprintf("SLUICE_ROLLOVER_BYTES=%d", bytesWritten),
		fmt.Sprintf("SLUICE_ROLLOVER_ELAPSED_MS=%d", elapsed.Milliseconds()),
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		slog.WarnContext(ctx, "stream: rollover hook failed (rollover already committed)",
			slog.String("hook", hookCmd),
			slog.String("err", err.Error()),
			slog.String("output", strings.TrimSpace(string(out))),
		)
		return
	}
	slog.DebugContext(ctx, "stream: rollover hook ok",
		slog.String("hook", hookCmd),
		slog.String("output", strings.TrimSpace(string(out))),
	)
}

// newShellCommand wraps [exec.CommandContext] with the OS-appropriate
// shell so a single-string hook command from the operator works
// identically on Windows and Unix. On Windows uses `cmd /C`; on Unix
// uses `sh -c`. The hook's environment starts as a copy of the
// process's env so PATH and the operator's exported vars are visible.
//
// Pulled into a helper so tests can inject a deterministic shell
// without OS detection.
func newShellCommand(ctx context.Context, cmdStr string) *exec.Cmd {
	shell, flag := defaultShell()
	cmd := exec.CommandContext(ctx, shell, flag, cmdStr)
	// Pre-populate Env from the process; callers append extra
	// SLUICE_ROLLOVER_* vars after.
	cmd.Env = append(cmd.Env, processEnv()...)
	return cmd
}

// alignEncryption mirrors [IncrementalBackup.alignEncryption]: validates
// that this stream's encryption configuration is consistent with the
// chain root's recorded shape, and returns the per-chain CEK (if any).
func (b *BackupStream) alignEncryption(parent *ir.Manifest) ([]byte, error) {
	parentEnc := parent.ChainEncryption
	switch {
	case parentEnc == nil && b.Encryption == nil:
		return nil, nil
	case parentEnc == nil && b.Encryption != nil:
		return nil, errors.New("stream: parent chain is plaintext but --encrypt was supplied; cannot extend a plaintext chain with encrypted incrementals")
	case parentEnc != nil && b.Encryption == nil:
		return nil, fmt.Errorf("stream: parent chain is encrypted (algorithm=%q kek_mode=%q kek_ref=%q) but no --encrypt + key was supplied",
			parentEnc.Algorithm, parentEnc.KEKMode, parentEnc.KEKRef)
	}
	if b.Encryption.Envelope == nil {
		return nil, errors.New("stream: encryption envelope is nil")
	}
	if parentEnc.KEKMode != "" && b.Encryption.Envelope.Mode() != parentEnc.KEKMode {
		return nil, fmt.Errorf("stream: envelope mode %q does not match chain's recorded kek_mode %q",
			b.Encryption.Envelope.Mode(), parentEnc.KEKMode)
	}
	mode := parentEnc.Mode
	if mode == "" {
		mode = crypto.EncryptModePerChain
	}
	if mode == crypto.EncryptModePerChain {
		if len(parentEnc.WrappedCEK) == 0 {
			return nil, errors.New("stream: parent's chain encryption is per-chain but WrappedCEK is empty")
		}
		cek, err := b.Encryption.Envelope.UnwrapCEK(parentEnc.WrappedCEK)
		if err != nil {
			return nil, fmt.Errorf("stream: unwrap parent chain cek: %w", err)
		}
		return cek, nil
	}
	return nil, nil
}

// resolveChunkCEK mirrors [Backup.resolveChunkCEK].
func (b *BackupStream) resolveChunkCEK(chainCEK []byte) (cek, wrapped []byte, err error) {
	if b.Encryption == nil {
		return nil, nil, nil
	}
	mode := b.Encryption.Mode
	if mode == "" {
		mode = crypto.EncryptModePerChain
	}
	if mode == crypto.EncryptModePerChain {
		return chainCEK, nil, nil
	}
	cek, err = crypto.GenerateCEK()
	if err != nil {
		return nil, nil, fmt.Errorf("generate chunk cek: %w", err)
	}
	wrapped, err = b.Encryption.Envelope.WrapCEK(cek)
	if err != nil {
		return nil, nil, fmt.Errorf("wrap chunk cek: %w", err)
	}
	return cek, wrapped, nil
}
