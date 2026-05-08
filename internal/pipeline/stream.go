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
		manifest, totalChanges, totalBytes, sourceClosed, rErr := b.runRollover(ctx, cdc, changesCh, currentParent, startPos, rolloverWindow, maxChanges, maxBytes, chunkSize, now, clockNow)
		elapsed := clockNow().Sub(started)
		if rErr != nil {
			// ctx-cancel during a rollover surfaces here. Treat it as a
			// clean exit: the rollover's in-flight chunks may have been
			// written, but the manifest never finalised; on restart the
			// stream picks up at the previous rollover's EndPosition.
			if errors.Is(rErr, context.Canceled) || errors.Is(rErr, context.DeadlineExceeded) {
				slog.InfoContext(ctx, "stream: context cancelled during rollover; in-flight rollover not committed",
					slog.Duration("elapsed", elapsed),
				)
				return nil
			}
			return wrapWithHint(PhaseCDC, fmt.Errorf("stream: rollover %d: %w", rolloverSeq, rErr))
		}

		if manifest == nil {
			// Empty rollover that we skipped per IncludeEmptyRollovers.
			slog.InfoContext(ctx, "stream: rollover empty; skipping manifest write",
				slog.Int("seq", rolloverSeq),
				slog.Duration("elapsed", elapsed),
			)
			// Update state file's last_rollover_at as a heartbeat even
			// when no manifest was committed — operators monitoring the
			// state file's freshness should see the stream is alive.
			state.LastRolloverAt = now().UTC()
			if err := writeStreamState(ctx, b.Store, statePath, state); err != nil {
				slog.WarnContext(ctx, "stream: failed to update state file after empty rollover",
					slog.String("err", err.Error()),
				)
			}
			rolloverSeq++
			// Source-channel-closed and skip-empty: the source has gone
			// away (test fakes that emit-then-close, or a long-running
			// engine whose pump terminated). Without this exit, the loop
			// would spin forever producing skip-empty rollovers. In
			// production the same path triggers when the slot becomes
			// invalid mid-stream — we want a loud exit, not a busy spin.
			if sourceClosed {
				slog.InfoContext(ctx, "stream: cdc channel closed; exiting after final empty rollover",
					slog.Int("rollovers", rolloverSeq),
				)
				return nil
			}
			continue
		}

		manifestPath := buildIncrementalManifestPath(manifest)
		if err := writeManifestAt(ctx, b.Store, manifestPath, manifest); err != nil {
			return fmt.Errorf("stream: write rollover manifest: %w", err)
		}

		// Advance state file's last_rollover_at to mark liveness.
		state.LastRolloverAt = now().UTC()
		if err := writeStreamState(ctx, b.Store, statePath, state); err != nil {
			slog.WarnContext(ctx, "stream: failed to update state file after rollover commit",
				slog.String("err", err.Error()),
			)
		}

		slog.InfoContext(ctx, "stream rollover committed",
			slog.String("manifest_path", manifestPath),
			slog.String("backup_id", manifest.BackupID),
			slog.String("parent_backup_id", manifest.ParentBackupID),
			slog.Int64("changes", totalChanges),
			slog.Int64("bytes", totalBytes),
			slog.Duration("elapsed", elapsed),
		)

		// Run the rollover hook (best-effort; failures don't fail the stream).
		if b.RolloverHook != "" {
			runRolloverHook(ctx, b.RolloverHook, manifest, manifestPath, totalChanges, totalBytes, elapsed)
		}

		// Advance the chain: next rollover's parent is this rollover's
		// manifest; next rollover's StartPosition is this rollover's
		// EndPosition.
		currentParent = manifest
		startPos = manifest.EndPosition
		rolloverSeq++

		// Source-channel-closed: pump terminated (test fakes that
		// emit-then-close, or a real engine whose stream ended). Exit
		// cleanly rather than spinning on an empty channel.
		if sourceClosed {
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

// runRollover executes one bounded rollover window. Returns the
// committed manifest (with chunks staged on the store), the change
// count, the bytes written across all chunks in the window, and any
// fatal error.
//
// When IncludeEmptyRollovers is false and the window captured zero
// changes, returns (nil, 0, 0, nil) to signal "skip the manifest write."
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
	now func() time.Time,
	clockNow func() time.Time,
) (rolloverManifest *ir.Manifest, totalChanges, totalBytes int64, sourceClosed bool, err error) {
	beforeSchema := parent.Schema
	beforeHash, hashErr := ir.ComputeSchemaHash(beforeSchema)
	if hashErr != nil {
		return nil, 0, 0, false, fmt.Errorf("hash source schema: %w", hashErr)
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
	endPos, capturedChanges, capturedBytes, srcClosed, captureErr := b.captureWindow(ctx, cdc, changesCh, manifest, chunkSize, deadline, maxChanges, maxBytes, clockNow)
	totalChanges = capturedChanges
	totalBytes = capturedBytes
	sourceClosed = srcClosed
	if captureErr != nil {
		return nil, totalChanges, totalBytes, sourceClosed, captureErr
	}

	// Empty rollover handling: when no changes captured AND the
	// operator hasn't asked to include them, return (nil, ...) so the
	// caller skips the manifest write.
	if totalChanges == 0 && !b.IncludeEmptyRollovers {
		return nil, 0, 0, sourceClosed, nil
	}

	manifest.EndPosition = endPos
	if (manifest.EndPosition.Engine == "" && manifest.EndPosition.Token == "") && totalChanges == 0 {
		// Empty rollover written-anyway path: the chain advances no
		// position. Use the StartPosition as the EndPosition so the
		// next rollover still has a valid resume cursor.
		manifest.EndPosition = startPos
	}

	// Phase 4 v1: schema-delta capture is intentionally skipped on
	// stream rollovers. Each rollover is short (default 5 m); reading
	// the source schema twice per window adds latency proportional to
	// table count for negligible benefit (DDL events on a continuous
	// source are rare). Operators who need DDL capture can take a
	// one-shot incremental at the boundary, or future Phase 4.x can
	// promote schema diffing to streams. The schema recorded on each
	// rollover's manifest is the parent's schema — a chain restore
	// walks the parents in order so the schema state at the chain's
	// terminal manifest is correct.

	manifest.BackupID = ir.ComputeBackupID(manifest)
	manifest.PartialState = ir.BackupStateComplete
	return manifest, totalChanges, totalBytes, sourceClosed, nil
}

// captureWindow drains changes from changesCh into chunks staged on
// manifest, closing on the first of: deadline reached, totalChanges ≥
// maxChanges, totalBytes ≥ maxBytes. Mirrors
// [IncrementalBackup.captureWindow] but adds a byte-bound and tracks
// totalBytes across chunks for the rollover-hook env contract.
//
// Window-end straddle behaviour: an open transaction (TxBegin observed
// without TxCommit) extends the window by up to one transaction so the
// chain doesn't end mid-tx — same as Phase 3.1.
//
// Returns the position of the last applied change (= EndPosition),
// total change count, total bytes-written across chunks, and any fatal
// error.
func (b *BackupStream) captureWindow(
	ctx context.Context,
	cdc ir.CDCReader,
	changesCh <-chan ir.Change,
	manifest *ir.Manifest,
	chunkSize int,
	deadline time.Time,
	maxChanges int,
	maxBytes int64,
	clockNow func() time.Time,
) (endPos ir.Position, totalChanges, totalBytes int64, sourceClosed bool, err error) {
	var (
		writer        *changeChunkWriter
		buf           *bytes.Buffer
		chunkIdx      int
		inTransaction bool
	)

	runNamespace := changeChunkRunNamespace(manifest)

	flush := func() error {
		if writer == nil {
			return nil
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("close chunk: %w", err)
		}
		path := changeChunkPath(runNamespace, chunkIdx)
		hash := writer.Hash()
		nb := int64(buf.Len())
		if err := b.Store.Put(ctx, path, buf); err != nil {
			return fmt.Errorf("store put %q: %w", path, err)
		}
		manifest.ChangeChunks = append(manifest.ChangeChunks, &ir.ChunkInfo{
			File:     path,
			RowCount: writer.ChangeCount(),
			SHA256:   hash,
		})
		totalBytes += nb
		writer = nil
		buf = nil
		chunkIdx++
		return nil
	}

	openWriter := func() error {
		buf = &bytes.Buffer{}
		w, err := newChangeChunkWriter(buf)
		if err != nil {
			return fmt.Errorf("open chunk: %w", err)
		}
		writer = w
		return nil
	}

	timer := time.NewTimer(deadline.Sub(clockNow()))
	defer timer.Stop()

	deadlinePassed := false
	for {
		select {
		case <-ctx.Done():
			return endPos, totalChanges, totalBytes, sourceClosed, ctx.Err()
		case <-timer.C:
			deadlinePassed = true
			if !inTransaction {
				if err := flush(); err != nil {
					return endPos, totalChanges, totalBytes, sourceClosed, err
				}
				return endPos, totalChanges, totalBytes, sourceClosed, nil
			}
		case change, ok := <-changesCh:
			if !ok {
				sourceClosed = true
				if errReader, ok := cdc.(interface{ Err() error }); ok {
					if e := errReader.Err(); e != nil {
						return endPos, totalChanges, totalBytes, sourceClosed, fmt.Errorf("cdc reader: %w", e)
					}
				}
				if err := flush(); err != nil {
					return endPos, totalChanges, totalBytes, sourceClosed, err
				}
				return endPos, totalChanges, totalBytes, sourceClosed, nil
			}
			switch change.(type) {
			case ir.TxBegin:
				inTransaction = true
			case ir.TxCommit:
				inTransaction = false
			}
			if writer == nil {
				if err := openWriter(); err != nil {
					return endPos, totalChanges, totalBytes, sourceClosed, err
				}
			}
			if err := writer.WriteChange(change); err != nil {
				return endPos, totalChanges, totalBytes, sourceClosed, err
			}
			totalChanges++
			pos := change.Pos()
			if pos.Engine != "" || pos.Token != "" {
				endPos = pos
			}
			// Roll the chunk on per-chunk-row cap.
			if writer.ChangeCount() >= int64(chunkSize) {
				if err := flush(); err != nil {
					return endPos, totalChanges, totalBytes, sourceClosed, err
				}
			}
			// Approximate max-changes cap: close at next tx boundary.
			if maxChanges > 0 && totalChanges >= int64(maxChanges) && !inTransaction {
				if err := flush(); err != nil {
					return endPos, totalChanges, totalBytes, sourceClosed, err
				}
				return endPos, totalChanges, totalBytes, sourceClosed, nil
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
				if totalBytes+inflightBytes >= maxBytes {
					if err := flush(); err != nil {
						return endPos, totalChanges, totalBytes, sourceClosed, err
					}
					return endPos, totalChanges, totalBytes, sourceClosed, nil
				}
			}
			if deadlinePassed && !inTransaction {
				if err := flush(); err != nil {
					return endPos, totalChanges, totalBytes, sourceClosed, err
				}
				return endPos, totalChanges, totalBytes, sourceClosed, nil
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
