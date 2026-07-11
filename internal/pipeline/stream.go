// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Continuous-incremental long-running stream orchestrator. Phase 4 of
// the logical-backup feature (`docs/dev/design/logical-backups-phase-4.md`):
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

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
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
// state liveness file is written to. Lives under [lineage.IncrementalManifestPrefix]
// so a single `List(manifests/)` call enumerates both manifests AND
// the state file (callers that only want manifests filter on
// `incr-` prefix).
const DefaultStreamStateFilename = lineage.IncrementalManifestPrefix + "stream_state.json"

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

	// Store is the [irbackup.Store] the parent manifest lives in and
	// every rolled manifest + chunks are written to. Required.
	Store irbackup.Store

	// ParentRef identifies the parent backup the stream chains off.
	// Either a [irbackup.Manifest.BackupID] (e.g. "abc123def4567890") or the
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
	// during the stream's lifetime. See [lineage.BackupEncryption]. Aligns
	// against the parent's chain encryption at startup; mismatched
	// shapes (encrypt mid-chain or vice versa) are refused there.
	Encryption *lineage.BackupEncryption

	// RetryAttempts caps the number of consecutive retriable rollover
	// failures the stream will absorb before giving up and returning
	// the underlying error. GitHub issue #22: pre-v0.48.0 the
	// `backup stream run` loop treated any rollover error as terminal,
	// so a source-side TCP reset that the sync-stream path retries
	// through (ADR-0038, v0.46.0) took the backup-stream process down.
	// v0.48.0 mirrors the sync-stream retry policy on the rollover
	// loop: classify via [ir.RetriableError], reopen the CDC pump
	// from the last committed parent's EndPosition, retry.
	//
	// Zero or one means "no retry" (preserve pre-v0.48.0 fail-on-first
	// behaviour); higher values enable bounded retry. Default when
	// nil/zero on the [BackupStream] receiver is supplied by the CLI's
	// flag default (`--retry-attempts`, default 8).
	//
	// Consecutive-failure counter resets when a rollover commits
	// successfully between failures — so a stream surviving for hours
	// doesn't carry retry debt forward.
	RetryAttempts int

	// RetryBackoffBase is the base interval for the exponential
	// backoff between retriable rollover failures. Doubles on each
	// consecutive failure, capped at [RetryBackoffCap]. Zero means
	// use the default (100ms). Only consulted when [RetryAttempts] > 1.
	RetryBackoffBase time.Duration

	// RetryBackoffCap is the upper bound on each per-attempt backoff
	// interval. Zero means use the default (30s). Only consulted when
	// [RetryAttempts] > 1.
	RetryBackoffCap time.Duration

	// RetainRotateAt, when > 0, is the in-process rotation threshold
	// (ADR-0046 §6): once the open segment's age (now - the open
	// segment's full CreatedAt) reaches this duration, the rollover-
	// loop goroutine drives the rotation FSM
	// (STREAMING→DRAIN→SNAPSHOT→BULKCOPY→COMMIT→STREAMING) over the
	// SAME CDC handle, appending a fresh segment to the lineage and
	// continuing. Rotation is ALWAYS in-process — there is no operator
	// wrapper / clean-exit model (the Phase-1 --exit-after-* knobs are
	// removed in v0.67.0).
	//
	// Zero disables rotation (an unbounded single segment — the
	// pre-rotation behaviour).
	RetainRotateAt time.Duration

	// RetainRotateAtChainLength, when > 0, rotates after this many
	// incrementals have been committed to the open segment. Either
	// threshold firing wins (length checked first — no I/O).
	//
	// Zero disables the length threshold.
	RetainRotateAtChainLength int

	// Codec is the operator's --compression choice for chunks written
	// by this stream. Consulted only to pin a never-catalogued open
	// segment's codec; the lineage's recorded codec wins once set.
	// Each rotation-opened segment is written with this codec. Empty
	// resolves to gzip (pre-ADR default).
	Codec blobcodec.Codec

	// Now, when set, overrides the wall-clock-time source for
	// [irbackup.Manifest.CreatedAt] and `stream_state.json` timestamps. Used
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

	// segStore is the OPEN segment's store view (b.Store narrowed to
	// the open segment's Dir; a no-op wrap for the common one-segment
	// shape). Manifest + chunk writes go here. Repointed by the
	// rotation FSM's COMMIT to the freshly-opened segment. Set by Run.
	segStore irbackup.Store

	// segCodec is the codec of the open segment, threaded into the
	// change-chunk writer. Set by Run; repointed by rotation.
	segCodec blobcodec.Codec

	// skipThrough, when non-nil, is the per-segment-boundary dedup
	// floor. ADR-0067: it is set to P_N (the prior segment's
	// EndPosition), NOT the new segment's anchor S. The rollover loop
	// drops every pump event whose position precedes-or-equals P_N
	// (events already in the prior segment's tail) and clears this once
	// it sees the first event strictly after P_N -- so the new segment
	// KEEPS the (P_N, S] overlap the new full's snapshot also captured,
	// making the lineage born-contiguous and compactable. That overlap
	// re-applies idempotently on restore. Requires the source engine to
	// implement [ir.PositionMonotonicChecker] (PG + MySQL do); without
	// it the floor degrades to "no skip" and restore's idempotent replay
	// still absorbs the (now slightly larger) overlap.
	skipThrough *ir.Position
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
//
// Setup (validate → preflights → open the CDC pump → initial state) lives
// in [BackupStream.newRolloverLoop]; this function owns the pump's close
// defer and drives the rollover loop, delegating each committed / empty
// rollover to commitRollover / handleEmptyRollover and keeping the
// transient-retry and in-process rotation-FSM logic inline.
func (b *BackupStream) Run(ctx context.Context) error {
	init, err := b.newRolloverLoop(ctx)
	if err != nil {
		return err
	}
	// ADR-0154 Phase 1: `backup stream` does not yet sign its rollover
	// manifests — refuse to extend a signed chain. See [refuseSignedChain].
	if err := b.refuseSignedChain(ctx); err != nil {
		return err
	}
	cdc := init.cdc
	defer func() { migcore.CloseIf(cdc) }()
	defer init.deregisterStopCh()

	changesCh := init.changesCh
	chainCEK := init.chainCEK
	stopCh := init.stopCh
	rolloverWindow := init.rolloverWindow
	maxChanges := init.maxChanges
	maxBytes := init.maxBytes
	chunkSize := init.chunkSize
	statePath := init.statePath
	retryAttempts := init.retryAttempts
	retryBase := init.retryBase
	retryCap := init.retryCap
	now := init.now
	clockNow := init.clockNow

	// 5. Drive the rollover loop. The loop runs until ctx is cancelled
	//    or a stop request is observed via state file. Each iteration
	//    is a bounded window producing zero or one manifest. Errors
	//    abort the loop loudly.
	currentParent := init.parent
	startPos := init.startPos
	rolloverSeq := 0
	retryConsecutive := 0
	for {
		// Stop-request check (cross-machine stop). A ctx cancel here
		// short-circuits the same way: the captureWindow loop sees
		// ctx.Done and returns; we drop into the cleanup block below.
		if stopReq, sErr := readStreamStopRequested(ctx, b.Store, statePath); sErr != nil {
			slog.WarnContext(
				ctx, "stream: failed to read stream_state for stop check; will retry on next rollover",
				slog.String("err", sErr.Error()),
			)
		} else if stopReq != nil {
			slog.InfoContext(
				ctx, "stream: stop requested via stream_state.json; exiting after current rollover",
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
					if err := lineage.WriteManifestAt(commitCtx, b.segStore, manifestPath, roll.Manifest); err != nil {
						slog.WarnContext(
							ctx, "stream: drain-commit of in-flight manifest failed",
							slog.String("err", err.Error()),
						)
					} else {
						// ADR-0046: append to the open segment in
						// lineage.json on the drain-commit path too.
						lineage.UpdateLineageForManifestBestEffort(commitCtx, b.Store, roll.Manifest, manifestPath, b.segCodec)
						slog.InfoContext(
							ctx, "stream rollover committed (drain on ctx-cancel)",
							slog.String("manifest_path", manifestPath),
							slog.String("backup_id", roll.Manifest.BackupID),
							slog.Int64("changes", roll.TotalChanges),
							slog.Int64("bytes", roll.TotalBytes),
							slog.Duration("elapsed", elapsed),
						)
					}
					commitCancel()
				} else {
					slog.InfoContext(
						ctx, "stream: context cancelled during rollover; in-flight rollover not committed",
						slog.Duration("elapsed", elapsed),
					)
				}
				return nil
			}
			// GitHub #22: classify the rollover error; if it satisfies
			// [ir.RetriableError] (a transient CDC-pump shape like
			// `vstream: recv: rpc error: code = Unavailable …
			// connection reset by peer`, classified by the engine's
			// classifyReaderError in v0.46.0), reopen the pump from the
			// last committed parent's EndPosition and retry. Bounded by
			// [RetryAttempts] consecutive failures; resets on a
			// successful rollover.
			var re ir.RetriableError
			if retryAttempts > 1 && errors.As(rErr, &re) && re.Retriable() {
				// Loose end 2b sibling: an established-then-idle VStream
				// Phase-2 progress timeout (ir.LivenessProgressTimeoutError)
				// is NOT a consecutive failure — the pump re-established and
				// proved a serving tablet, then the source was quiet. Don't
				// advance the give-up budget for it, so a genuinely idle
				// backup source doesn't die after retryAttempts idle
				// reconnects (same rationale + invariant as the sync-stream
				// loop in streamer_retry.go). A Phase-1 / open error carries
				// no marker and still counts, so a stream that can never
				// establish still fails loudly after the budget.
				if !isIdleProgressTimeout(rErr) {
					retryConsecutive++
				}
				if retryConsecutive >= retryAttempts {
					return migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("stream: rollover %d: retry budget exhausted after %d consecutive failures: %w",
						rolloverSeq, retryConsecutive, rErr))
				}
				backoff := computeRetryBackoff(retryConsecutive, retryBase, retryCap, re.RetryHint())
				slog.InfoContext(
					ctx, "stream: transient cdc error; reopening pump and retrying",
					slog.Int("rollover_seq", rolloverSeq),
					slog.Int("attempt", retryConsecutive),
					slog.Int("max_attempts", retryAttempts),
					slog.Duration("backoff", backoff),
					slog.String("err", rErr.Error()),
				)
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return ctx.Err()
				}
				migcore.CloseIf(cdc)
				cdc, err = openCDCReaderWithSlot(ctx, b.Source, b.SourceDSN, b.SlotName)
				if err != nil {
					return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("stream: reopen cdc reader after transient: %w", err))
				}
				// The fresh pump needs chain-consumer ack mode re-armed
				// (it's per-reader state, set before StreamChanges).
				holdChainAck(cdc)
				resumeFrom := currentParent.EndPosition
				changesCh, err = cdc.StreamChanges(ctx, resumeFrom)
				if err != nil {
					if errors.Is(err, ir.ErrPositionInvalid) {
						return fmt.Errorf("stream: after transient retry, source has pruned past parent's terminal position; take a fresh full backup or shorten the chain interval. Underlying: %w", err)
					}
					return migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("stream: restart cdc stream after transient: %w", err))
				}
				continue
			}
			return migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("stream: rollover %d: %w", rolloverSeq, rErr))
		}
		// Successful rollover (whether empty or with a manifest):
		// reset the consecutive-failure counter so a long-lived stream
		// doesn't carry retry debt forward across hours of clean
		// rollovers (GitHub #22 mirrors sync-stream's progress-reset
		// semantics).
		retryConsecutive = 0

		if roll.Manifest == nil {
			if b.handleEmptyRollover(ctx, roll, init.state, statePath, now, elapsed, &rolloverSeq) {
				return nil
			}
			continue
		}

		rc, err := b.commitRollover(ctx, roll, init.state, statePath, now, elapsed, cdc, rolloverSeq)
		if err != nil {
			return err
		}
		if rc.exit {
			return nil
		}
		currentParent = rc.newParent
		startPos = rc.newStartPos
		rolloverSeq++

		// ADR-0046 §2/§6: in-process rotation. After each successful
		// rollover, check the rotation thresholds against the OPEN
		// segment. When one trips, drive the rotation FSM
		// (DRAIN→SNAPSHOT→BULKCOPY→COMMIT) over the SAME cdc handle:
		// it caps the open segment, opens a fresh segment whose full's
		// snapshot anchor S is hard-asserted S>=P_N, and continues
		// streaming on the same handle from S. Rotation is in-process;
		// there is no clean-exit / operator-wrapper model.
		if reason := b.shouldRotate(ctx, rolloverSeq, now()); reason != "" {
			res, rotErr := b.performRotation(ctx, rotateInputs{
				reason:        reason,
				lastCommitted: currentParent,
				changesCh:     changesCh,
				now:           now,
				clockNow:      clockNow,
			})
			if rotErr != nil {
				// S>=P_N hard-fail or any FSM step failure: loud abort,
				// STAY on the open segment (never silently gap). The
				// stream continues streaming the still-open prior
				// segment from its persisted position.
				if errors.Is(rotErr, errRotationAbortStayOpen) {
					slog.ErrorContext(
						ctx, "stream: rotation aborted; staying on the open segment (no gap introduced)",
						slog.String("rotation_reason", reason),
						slog.String("err", rotErr.Error()),
					)
				} else {
					return migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("stream: rotation (%s): %w", reason, rotErr))
				}
			} else {
				// Rotation committed: the new segment is authoritative.
				// Re-point the rollover loop at the new open segment;
				// the SAME cdc handle keeps streaming (no slot re-open).
				b.segStore = res.newSegStore
				b.segCodec = res.newSegCodec
				changesCh = res.changesCh
				currentParent = res.newFull
				// ADR-0067: the new segment's incrementals begin at P_N
				// (the prior segment's EndPosition), KEEPING the (P_N, S]
				// overlap so the lineage is born-contiguous and
				// compactable. startPos = P_N makes the first
				// incremental's manifest honest about its coverage; the
				// skipThrough floor at P_N drops only events already in
				// the prior segment's tail (<= P_N), never the overlap.
				// The (P_N, S] events the new full also captured re-apply
				// idempotently on restore (ADR-0010 / the initial
				// snapshot->CDC handoff dedup, per segment boundary).
				startPos = res.priorEnd
				b.skipThrough = &res.priorEnd
				rolloverSeq = 0
				slog.InfoContext(
					ctx, "stream: rotation committed; continuing on new segment",
					slog.String("rotation_reason", reason),
					slog.String("new_segment_dir", res.newSegDir),
					slog.String("anchor_token", res.resumePos.Token),
				)
			}
		}

		// Source-channel-closed: pump terminated (test fakes that
		// emit-then-close, or a real engine whose stream ended). Exit
		// cleanly rather than spinning on an empty channel.
		if roll.SourceClosed {
			slog.InfoContext(
				ctx, "stream: cdc channel closed after rollover commit; exiting",
				slog.Int("rollovers", rolloverSeq),
			)
			return nil
		}
	}
}

// rolloverInit is the fully-initialised state newRolloverLoop hands to
// [BackupStream.Run]'s rollover loop: the open CDC pump + change channel,
// the resolved parent + start position, the chain CEK, the liveness state
// file, the in-process stop channel + its deregister hook, and the
// resolved rollover-window / retry knobs. cdc is returned OPEN — Run owns
// its close (and the transient-retry path may swap it) via a defer.
type rolloverInit struct {
	cdc              ir.CDCReader
	changesCh        <-chan ir.Change
	parent           *irbackup.Manifest
	startPos         ir.Position
	chainCEK         []byte
	state            *streamState
	stopCh           <-chan struct{}
	deregisterStopCh func()

	rolloverWindow time.Duration
	maxChanges     int
	maxBytes       int64
	chunkSize      int
	statePath      string
	retryAttempts  int
	retryBase      time.Duration
	retryCap       time.Duration
	now            func() time.Time
	clockNow       func() time.Time
}

// newRolloverLoop runs [BackupStream.Run]'s setup: validate, resolve the
// window/retry defaults, run the concurrent-writer + crash-recovery
// preflights, resolve the open segment + parent (incl. the ADR-0087
// rotation-boundary resume heal), align chain encryption, open the CDC
// pump for the stream's lifetime, and write the initial liveness state.
// On any error after the pump opens it closes the pump before returning
// (Run's close-defer is not yet armed); on success the caller arms the
// defer and drives the returned state.
func (b *BackupStream) newRolloverLoop(ctx context.Context) (*rolloverInit, error) {
	if err := b.validate(); err != nil {
		return nil, err
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

	// 1. Concurrent-writer check (stream_state.json lives at the
	//    lineage root — it's a stream-level liveness file, not
	//    per-segment).
	if err := b.preflightStreamState(ctx, statePath, rolloverWindow, pid, host, now()); err != nil {
		return nil, err
	}

	// 1.4. Crash recovery: reconcile any rotation_state.json left by a
	//    crash mid-FSM BEFORE resolving the open segment, so the open
	//    segment is the post-recovery truth (≤COMMIT → prior segment;
	//    >COMMIT → the new segment the atomic write made authoritative).
	if err := recoverRotationState(ctx, b.Store); err != nil {
		return nil, fmt.Errorf("stream: rotation recovery: %w", err)
	}

	// 1.5. Resolve the OPEN segment: rollovers append to it under its
	//    Dir + recorded codec (ADR-0046). For a never-rotated backup
	//    that's the root (Dir == "") — byte-identical to the pre-ADR
	//    single chain. Rotation repoints b.segStore / b.segCodec.
	segStore, segCodec, err := lineage.OpenSegmentStore(ctx, b.Store, b.Codec)
	if err != nil {
		return nil, fmt.Errorf("stream: resolve open segment: %w", err)
	}
	b.segStore = segStore
	b.segCodec = segCodec

	// 1.6. Heal an open-segment catalog that a crash/cancel left out of
	//    sync with disk (an incremental durable on disk but missing from
	//    lineage.json's Incrementals list, because its best-effort catalog
	//    append never landed). Must run BEFORE resolveParent: otherwise the
	//    resumed stream re-stitches off the on-disk tail while the catalog
	//    keeps the head gap, and restore later refuses the segment as
	//    mis-stitched. Best-effort + idempotent (retries next resume).
	if rerr := lineage.ReconcileOpenSegmentCatalog(ctx, b.Store, b.segStore); rerr != nil {
		slog.WarnContext(
			ctx, "stream: open-segment catalog reconcile failed; continuing "+
				"(a crash-orphaned incremental may keep restore refusing this segment until repaired)",
			slog.String("err", rerr.Error()),
		)
	}

	// 2. Resolve parent manifest (within the open segment).
	parent, parentPath, err := b.resolveParent(ctx)
	if err != nil {
		return nil, fmt.Errorf("stream: resolve parent: %w", err)
	}
	startPos := parent.EndPosition
	if startPos.Engine == "" && startPos.Token == "" {
		slog.WarnContext(
			ctx, "stream: parent manifest has no EndPosition; chain will start from CDC's current position",
			slog.String("parent_path", parentPath),
		)
	}

	// 2.1. ADR-0087 rotation-boundary resume heal (Bug 139). When this
	//    resume lands on a rotation-born OPEN segment that never committed
	//    an incremental in its creating session (source idle at the prior
	//    stream stop, or a crash/end at the rotation boundary), the parent
	//    is the segment's full and startPos == the full's anchor S. Resuming
	//    from S would make the first incremental start at S, leaving the
	//    segment permanently stamp-less and the (P_N, S] window forever
	//    uncovered by any incremental — an un-compactable boundary. Instead
	//    resume from the prior segment's EndPosition (P_N), exactly
	//    reconstructing the creating session's post-COMMIT state (currentParent
	//    = the segment's full, startPos = P_N): the first incremental then
	//    starts at P_N, lineage.UpdateLineageForManifest stamps IncrementalCoverageStart
	//    = P_N, and the lineage becomes born-contiguous and compactable. The
	//    (P_N, S] overlap re-applies idempotently on restore (ADR-0010 / the
	//    snapshot->CDC handoff dedup). No skipThrough is needed: a fresh
	//    StreamChanges(P_N) resumes strictly-after P_N like any normal resume.
	if healed, priorEnd, ok := rotationBoundaryResumeStart(ctx, b.Store, startPos); ok {
		slog.InfoContext(
			ctx, "stream: resuming a rotation-born segment from the prior segment's EndPosition (P_N) "+
				"to re-establish ADR-0067 overlap coverage — the creating session stopped/crashed before "+
				"committing this segment's first incremental, so the (P_N, S] window is replayed now and "+
				"the segment's IncrementalCoverageStart is stamped on the first commit (Bug 139)",
			slog.String("parent_path", parentPath),
			slog.String("prior_end_pN", priorEnd.Token),
			slog.String("full_anchor_S", startPos.Token),
		)
		startPos = healed
	}

	// 2.5. Phase 6.1: align with chain encryption. Refuses early if
	// the parent's chain encryption shape doesn't match the operator's
	// supplied envelope (or vice versa).
	chainCEK, err := b.alignEncryption(ctx, parent)
	if err != nil {
		return nil, fmt.Errorf("stream: encryption: %w", err)
	}

	// 2.5. Chain-resume preflight (see [migcore.PreflightChainResume]).
	if err := migcore.PreflightChainResume(ctx, b.Source, b.SourceDSN, startPos); err != nil {
		return nil, migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("stream: chain preflight: %w", err))
	}

	// 3. Open CDC pump for the lifetime of the stream. Returned OPEN;
	// Run owns the close defer (and the transient-retry path may swap the
	// handle) so the pump lives across rollovers. On an error AFTER this
	// open, newRolloverLoop closes it here since Run's defer is not armed
	// yet.
	cdc, err := openCDCReaderWithSlot(ctx, b.Source, b.SourceDSN, b.SlotName)
	if err != nil {
		return nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("stream: open cdc reader: %w", err))
	}

	// Chain-consumer ack mode (see [chainAckController]): the stream
	// has no applier tracker, so without the hold the keepalive acks
	// streamed-but-not-yet-committed positions; each rollover commit
	// below releases its window via releaseChainAckTo, bounding source
	// WAL retention to ~one rollover window.
	holdChainAck(cdc)

	changesCh, err := cdc.StreamChanges(ctx, startPos)
	if err != nil {
		migcore.CloseIf(cdc)
		if errors.Is(err, ir.ErrPositionInvalid) {
			return nil, fmt.Errorf("stream: source has pruned past parent's terminal position; take a fresh full backup or shorten the chain interval. Underlying: %w", err)
		}
		return nil, migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("stream: start cdc stream: %w", err))
	}

	// Retry policy for transient CDC-pump errors (GitHub #22). Mirrors
	// the sync-stream's runWithRetry shape: classify, bounded
	// consecutive-failure counter that resets on a successful
	// rollover, exponential backoff between attempts.
	retryAttempts := b.RetryAttempts
	if retryAttempts < 1 {
		retryAttempts = 1
	}
	retryBase := b.RetryBackoffBase
	if retryBase <= 0 {
		retryBase = 100 * time.Millisecond
	}
	retryCap := b.RetryBackoffCap
	if retryCap <= 0 {
		retryCap = 30 * time.Second
	}

	// 4. Initial state file write.
	state := &streamState{
		PID:            pid,
		Host:           host,
		StartedAt:      now().UTC(),
		LastRolloverAt: now().UTC(),
	}
	if err := writeStreamState(ctx, b.Store, statePath, state); err != nil {
		migcore.CloseIf(cdc)
		return nil, fmt.Errorf("stream: write initial state: %w", err)
	}

	// Register the in-process stop channel so a same-process
	// RequestStreamStop can signal us instantaneously without going
	// through the file-poll path (Bug 37 fix). Cross-process operators
	// still go through the file (notifyStreamStop is a no-op for them).
	// The deregister is returned (not deferred here) so Run holds it for
	// the stream's lifetime — deferring it here would close the channel
	// the moment this setup returns and make the loop see a phantom stop.
	stopCh, deregisterStopCh := registerStreamStopChan(b.Store)

	slog.InfoContext(
		ctx, "stream: started",
		slog.String("source_engine", b.Source.Name()),
		slog.String("parent_backup_id", parent.BackupID),
		slog.Duration("rollover_window", rolloverWindow),
		slog.Int("rollover_max_changes", maxChanges),
		slog.Int64("rollover_max_bytes", maxBytes),
	)

	return &rolloverInit{
		cdc:              cdc,
		changesCh:        changesCh,
		parent:           parent,
		startPos:         startPos,
		chainCEK:         chainCEK,
		state:            state,
		stopCh:           stopCh,
		deregisterStopCh: deregisterStopCh,
		rolloverWindow:   rolloverWindow,
		maxChanges:       maxChanges,
		maxBytes:         maxBytes,
		chunkSize:        chunkSize,
		statePath:        statePath,
		retryAttempts:    retryAttempts,
		retryBase:        retryBase,
		retryCap:         retryCap,
		now:              now,
		clockNow:         clockNow,
	}, nil
}

// rolloverCommit is [BackupStream.commitRollover]'s result. exit=true
// means the caller returns nil (a stop was observed); on the normal
// advance path advanced=true carries the next parent + start position.
type rolloverCommit struct {
	exit        bool
	advanced    bool
	newParent   *irbackup.Manifest
	newStartPos ir.Position
}

// handleEmptyRollover processes a skipped empty rollover: heartbeat the
// liveness file (unless a stop was observed) and decide whether to exit.
// Returns true when Run should exit cleanly (stop requested / observed, or
// the CDC channel closed); false to continue the loop. *rolloverSeq is
// advanced on the continue path.
func (b *BackupStream) handleEmptyRollover(ctx context.Context, roll rolloverOutcome, state *streamState, statePath string, now func() time.Time, elapsed time.Duration, rolloverSeq *int) bool {
	slog.InfoContext(
		ctx, "stream: rollover empty; skipping manifest write",
		slog.Int("seq", *rolloverSeq),
		slog.Duration("elapsed", elapsed),
	)
	if roll.StopRequested {
		slog.InfoContext(
			ctx, "stream: stop requested via stream_state.json during rollover; exiting",
			slog.Int("rollovers", *rolloverSeq),
		)
		return true
	}
	// Bug 37 fix (v0.19.1): read-modify-write heartbeat so a concurrent
	// stop_requested_at the operator wrote in the race window survives.
	state.LastRolloverAt = now().UTC()
	stopObserved, err := writeStreamStateMergeHeartbeat(ctx, b.Store, statePath, state)
	if err != nil {
		slog.WarnContext(
			ctx, "stream: failed to update state file after empty rollover",
			slog.String("err", err.Error()),
		)
	}
	if stopObserved {
		slog.InfoContext(
			ctx, "stream: heartbeat merge observed concurrent stop_requested_at; exiting",
			slog.Int("rollovers", *rolloverSeq),
		)
		return true
	}
	*rolloverSeq++
	// Source-channel-closed and skip-empty: the source has gone away, so
	// exit rather than spin producing skip-empty rollovers forever. In
	// production the same path fires when the slot goes invalid mid-stream.
	if roll.SourceClosed {
		slog.InfoContext(
			ctx, "stream: cdc channel closed; exiting after final empty rollover",
			slog.Int("rollovers", *rolloverSeq),
		)
		return true
	}
	return false
}

// commitRollover writes a rollover's manifest, appends it to the open
// segment, releases the chain-ack window, and heartbeats the liveness
// file. It returns exit=true when a stop was observed (Run returns nil),
// or advanced=true with the next parent + start position on the normal
// path. The rollover hook fires (best-effort) after a durable commit.
func (b *BackupStream) commitRollover(ctx context.Context, roll rolloverOutcome, state *streamState, statePath string, now func() time.Time, elapsed time.Duration, cdc ir.CDCReader, rolloverSeq int) (rolloverCommit, error) {
	manifestPath := buildIncrementalManifestPath(roll.Manifest)
	if err := lineage.WriteManifestAt(ctx, b.segStore, manifestPath, roll.Manifest); err != nil {
		return rolloverCommit{}, fmt.Errorf("stream: write rollover manifest: %w", err)
	}
	// ADR-0046: append this rollover to the open segment in lineage.json
	// (best-effort for the non-rotation path).
	lineage.UpdateLineageForManifestBestEffort(ctx, b.Store, roll.Manifest, manifestPath, b.segCodec)
	// The rollover is durable — let the slot release its window's WAL (this
	// is what bounds source WAL retention to ~one rollover window).
	releaseChainAckTo(ctx, cdc, roll.Manifest.EndPosition)

	if roll.StopRequested {
		slog.InfoContext(
			ctx, "stream rollover committed; stop requested via stream_state.json — exiting",
			slog.String("manifest_path", manifestPath),
			slog.String("backup_id", roll.Manifest.BackupID),
			slog.Int64("changes", roll.TotalChanges),
			slog.Int64("bytes", roll.TotalBytes),
			slog.Duration("elapsed", elapsed),
		)
		if b.RolloverHook != "" {
			runRolloverHook(ctx, b.RolloverHook, roll.Manifest, manifestPath, roll.TotalChanges, roll.TotalBytes, elapsed)
		}
		return rolloverCommit{exit: true}, nil
	}

	// Bug 37 fix (v0.19.1): read-modify-write heartbeat so a concurrent
	// stop_requested_at survives; exit immediately if the merge saw a stop.
	state.LastRolloverAt = now().UTC()
	stopObserved, hbErr := writeStreamStateMergeHeartbeat(ctx, b.Store, statePath, state)
	if hbErr != nil {
		slog.WarnContext(
			ctx, "stream: failed to update state file after rollover commit",
			slog.String("err", hbErr.Error()),
		)
	}
	if stopObserved {
		slog.InfoContext(
			ctx, "stream: heartbeat merge observed concurrent stop_requested_at; exiting after committed rollover",
			slog.Int("rollovers", rolloverSeq),
		)
		return rolloverCommit{exit: true}, nil
	}

	slog.InfoContext(
		ctx, "stream rollover committed",
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
	return rolloverCommit{advanced: true, newParent: roll.Manifest, newStartPos: roll.Manifest.EndPosition}, nil
}

// refuseSignedChain refuses to extend a signed (ADR-0154) chain from
// `backup stream`. Rollover-manifest signing is a Phase 1 follow-up (the
// rotation/CDC path needs its own -race-gated re-sign at every rollover +
// segment-cap); extending a signed chain with unsigned rollovers would
// produce links that refuse at restore, so refuse loudly up front rather
// than emit an un-restorable tail. The signal is the chain's
// lineage.json.sig (not a bare v6 FormatVersion stamp).
func (b *BackupStream) refuseSignedChain(ctx context.Context) error {
	signed, err := lineage.ChainIsSigned(ctx, b.Store)
	if err != nil {
		return fmt.Errorf("backup stream: probe signed chain: %w", err)
	}
	if signed {
		return errors.New("backup stream: cannot extend a signed (ADR-0154) chain — `backup stream` manifest signing is a Phase 1 follow-up; use `backup incremental --sign` to extend a signed chain, or start a fresh unsigned chain")
	}
	return nil
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

// rotationBoundaryResumeStart implements the ADR-0087 Bug-139 resume
// heal. It returns (P_N, P_N, true) — telling a resuming chain extender
// (`backup stream` or one-shot `backup incremental`) to resume from the
// prior segment's EndPosition instead of the open segment's full anchor
// S — when ALL of these hold:
//
//   - lineage.json exists with >= 2 segments (the open segment is
//     rotation-born: a prior segment exists);
//   - the open (last) segment has ZERO recorded incrementals (its
//     creating session never committed a rollover into it);
//   - the resolved parent is that segment's full, i.e. startPos (==
//     parent.EndPosition) equals the segment's StartPosition (S);
//   - the prior segment's EndPosition (P_N) is non-empty.
//
// Returning P_N reconstructs the creating session's post-COMMIT state
// (currentParent = the segment's full @ S, startPos = P_N): the first
// incremental then begins at P_N, [lineage.UpdateLineageForManifest] stamps
// IncrementalCoverageStart = P_N, and the lineage becomes
// born-contiguous and compactable. The source still retains everything
// after P_N because the slot's ack ceiling was only ever released
// through committed incremental ends <= P_N (releaseChainAckTo), so the
// (P_N, S] overlap is replayable; it re-applies idempotently on restore
// (ADR-0010). Any negative case (segment 0, the open segment already has
// incrementals, parent isn't the full, or an empty prior end) returns
// ok=false and the caller keeps today's behaviour. Best-effort: a
// transient catalog read error returns ok=false (no heal) rather than
// failing the chain extension.
func rotationBoundaryResumeStart(ctx context.Context, store irbackup.Store, startPos ir.Position) (resumeStart, priorEnd ir.Position, ok bool) {
	cat, found, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil || !found || len(cat.Segments) < 2 {
		return ir.Position{}, ir.Position{}, false
	}
	open := &cat.Segments[len(cat.Segments)-1]
	if len(open.Incrementals) != 0 {
		return ir.Position{}, ir.Position{}, false
	}
	// Resolved parent must be the segment's full (startPos == its anchor
	// S). parent.EndPosition is startPos here; comparing startPos to the
	// segment's StartPosition is the same test and survives a parent that
	// happens to carry no BackupID.
	if startPos != open.StartPosition {
		return ir.Position{}, ir.Position{}, false
	}
	prior := &cat.Segments[len(cat.Segments)-2]
	if prior.EndPosition.Engine == "" && prior.EndPosition.Token == "" {
		return ir.Position{}, ir.Position{}, false
	}
	return prior.EndPosition, prior.EndPosition, true
}

// resolveParent finds the parent manifest in the store. Mirrors
// [IncrementalBackup.resolveParent] but doesn't return the path
// because the stream doesn't need it post-resolve.
func (b *BackupStream) resolveParent(ctx context.Context) (*irbackup.Manifest, string, error) {
	// A stream chains off a manifest in the OPEN segment (b.segStore
	// is already narrowed to its Dir).
	manifests, err := lineage.ListAllManifestsViaWalk(ctx, b.segStore)
	if err != nil {
		return nil, "", err
	}
	if len(manifests) == 0 {
		return nil, "", errors.New("no parent manifest found in store; take a `sluice backup full` first")
	}
	if b.ParentRef != "" {
		for _, m := range manifests {
			id := m.Manifest.BackupID
			if id == "" {
				id = irbackup.ComputeBackupID(m.Manifest)
			}
			if id == b.ParentRef {
				// Task #42 (ADR-0085): an in-progress parent (crashed
				// full or window) cannot anchor a chain extension.
				if err := refuseInProgressParent(m.Manifest, m.Path); err != nil {
					return nil, "", err
				}
				return m.Manifest, m.Path, nil
			}
		}
		return nil, "", fmt.Errorf("parent backup %q not found in store; available: %s",
			b.ParentRef, manifestSummary(manifests))
	}
	// Resume off the chain TAIL. Mirrors IncrementalBackup; for a
	// stream re-launching after a crash this picks up the latest
	// committed rollover as the next parent. The tail is defined by
	// the open segment's append order (chain order) — NOT max
	// CreatedAt: CreatedAt is wall-clock with platform-dependent
	// resolution, not unique nor strictly monotonic with chain order,
	// so a millisecond tie on the two trailing rollovers would resume
	// off the second-to-last link and branch the lineage (ADR-0046
	// crash-matrix `pre-commit-write` flake, v0.67.0).
	tail := chainTailManifest(ctx, b.Store, manifests)
	if err := refuseInProgressParent(tail.Manifest, tail.Path); err != nil {
		return nil, "", err
	}
	return tail.Manifest, tail.Path, nil
}

// rolloverOutcome bundles the multi-value result of a single rollover
// run so [runRollover]'s signature stays under gocritic's 5-result
// ceiling. The Manifest field is nil when an empty rollover was
// captured AND IncludeEmptyRollovers is false ("skip the manifest
// write").
type rolloverOutcome struct {
	Manifest      *irbackup.Manifest
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
	parent *irbackup.Manifest,
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
	beforeHash, hashErr := irbackup.ComputeSchemaHash(beforeSchema)
	if hashErr != nil {
		return rolloverOutcome{}, fmt.Errorf("hash source schema: %w", hashErr)
	}

	manifest := &irbackup.Manifest{
		// Bug 116 closure: same proportional version-stamp rule as
		// other manifest constructors. Streaming incrementals inherit
		// the parent's effective schema; if it carries security
		// metadata, the streamed manifest is stamped FormatVersion=2.
		FormatVersion:  irbackup.FormatVersionFor(beforeSchema),
		SluiceVersion:  b.SluiceVersion,
		CreatedAt:      now().UTC(),
		SourceEngine:   b.Source.Name(),
		Schema:         beforeSchema,
		PartialState:   irbackup.BackupStateInProgress,
		Kind:           irbackup.BackupKindIncremental,
		ParentBackupID: parent.BackupID,
		StartPosition:  startPos,
		SchemaHash:     beforeHash,
		// Bug 184: record whether this engine's CDC positions commit AFTER
		// their rows (VStream), so restore knows a schema anchor at
		// EndPosition cannot prove the window's data was applied.
		CDCPositionCommitsAfterRows: b.Source.Capabilities().CDCPositionCommitsAfterRows,
	}
	if manifest.ParentBackupID == "" {
		manifest.ParentBackupID = irbackup.ComputeBackupID(parent)
	}
	// ADR-0152: encrypted rollovers write freshly-bound chunks, so the
	// manifest is stamped the chunk-binding version — regardless of the
	// (possibly older) chain root's own stamp; each link's chunks are
	// gated by its OWN recorded version. Must precede captureWindow so
	// [irbackup.ChunkAAD] gates on.
	if b.Encryption != nil {
		manifest.FormatVersion = max(manifest.FormatVersion, irbackup.FormatVersionEncryptedChunkBinding)
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
	if err := assertDataWindowEndPositionInvariant(manifest); err != nil {
		return out, err
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

	// Stamp the CDC-position fold version FIRST (item 57) so ComputeBackupID
	// folds CDCPositionCommitsAfterRows into the id for a VStream segment.
	irbackup.StampCDCPositionBinding(manifest)
	manifest.BackupID = irbackup.ComputeBackupID(manifest)
	manifest.PartialState = irbackup.BackupStateComplete
	out.Manifest = manifest
	return out, captureErr
}

// refreshSchemaAndAttachDelta re-reads the source schema, diffs it
// against the parent's schema, and attaches any [irbackup.SchemaDeltaEntry]
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
	manifest *irbackup.Manifest,
	beforeSchema *ir.Schema,
) error {
	sr, err := b.Source.OpenSchemaReader(ctx, b.SourceDSN)
	if err != nil {
		return fmt.Errorf("rollover: open schema reader: %w", err)
	}
	defer migcore.CloseIf(sr)
	afterSchema, err := sr.ReadSchema(ctx)
	if err != nil {
		return fmt.Errorf("rollover: read source schema: %w", err)
	}
	delta := migcore.DiffSchemas(beforeSchema, afterSchema)
	if len(delta) == 0 {
		// item 51: no-DDL rollovers still refresh the standalone-
		// sequence positions, then re-stamp the hash over the swapped
		// schema so chain-restore's recorded-vs-recomputed check
		// (ADR-0152) holds even when sequence OPTIONS changed inside
		// the window (position-only drift is hash-invisible — see the
		// incremental path's twin swap).
		manifest.Schema = schemaWithRefreshedSequences(manifest.Schema, afterSchema)
		refreshedHash, err := irbackup.ComputeSchemaHash(manifest.Schema)
		if err != nil {
			return fmt.Errorf("rollover: hash refreshed schema: %w", err)
		}
		manifest.SchemaHash = refreshedHash
		return nil
	}
	manifest.SchemaDelta = delta
	manifest.Schema = afterSchema
	afterHash, err := irbackup.ComputeSchemaHash(afterSchema)
	if err != nil {
		return fmt.Errorf("rollover: hash post-window schema: %w", err)
	}
	manifest.SchemaHash = afterHash
	slog.InfoContext(
		ctx, "stream: schema delta detected at rollover",
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
	manifest *irbackup.Manifest,
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
		inTransaction bool
		out           captureOutcome
	)

	// The chunk-writer state (the open writer, its buffer, the running
	// chunk index, the wrapped per-chunk CEK) lives on cb so flushTo /
	// open / processChange share it as they roll chunks. flush closes over
	// the window ctx + out for the boundary select cases below.
	cb := &changeChunkBuffer{
		b:            b,
		manifest:     manifest,
		runNamespace: changeChunkRunNamespace(manifest),
		chainCEK:     chainCEK,
	}
	flush := func() error { return cb.flushTo(ctx, &out) }

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
			if !inTransaction && cb.writer != nil {
				flushCtx, flushCancel := context.WithTimeout(context.WithoutCancel(ctx), stopDrainTimeout)
				if fErr := cb.flushTo(flushCtx, &out); fErr != nil {
					slog.WarnContext(
						ctx, "stream: drain-flush failed; in-flight chunk dropped",
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
			slog.DebugContext(
				ctx, "stream: in-process stop signal observed; eager exit",
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
				slog.WarnContext(
					ctx, "stream: stop-poll read failed; will retry on next tick",
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
			terminate, err := cb.processChange(ctx, change, &out, &inTransaction, deadlinePassed, chunkSize, maxChanges, maxBytes)
			if err != nil {
				return out, err
			}
			if terminate {
				return out, nil
			}
		}
	}
}

// changeChunkBuffer owns the in-flight change-chunk writer for one
// [BackupStream.captureWindow] run: the open writer, its backing buffer,
// the running chunk index, and the wrapped per-chunk CEK. Bundling this
// mutable state (rather than leaving it as captureWindow closures) lets
// flushTo / open / processChange share it while keeping captureWindow's
// select loop under the complexity ceiling. Single-goroutine: every
// method runs on captureWindow's own goroutine.
type changeChunkBuffer struct {
	b            *BackupStream
	manifest     *irbackup.Manifest
	runNamespace string
	chainCEK     []byte

	writer        *blobcodec.ChangeChunkWriter
	buf           *bytes.Buffer
	chunkIdx      int
	curWrappedCEK []byte
}

// flushTo closes the open chunk writer, stores its buffer under the
// per-run change-chunk path, appends the [irbackup.ChunkInfo] (with the
// per-chunk encryption header when the stream encrypts) to the manifest,
// adds the stored bytes to out.TotalBytes, and resets the buffer state
// for the next chunk. A no-op when no chunk is open. putCtx is separate
// from the window ctx so the ctx-cancel drain can flush under a fresh
// stopDrainTimeout-bounded context.
func (cb *changeChunkBuffer) flushTo(putCtx context.Context, out *captureOutcome) error {
	if cb.writer == nil {
		return nil
	}
	if err := cb.writer.Close(); err != nil {
		return fmt.Errorf("close chunk: %w", err)
	}
	path := changeChunkPath(cb.runNamespace, cb.chunkIdx)
	hash := cb.writer.Hash()
	nb := int64(cb.buf.Len())
	if err := cb.b.segStore.Put(putCtx, path, cb.buf); err != nil {
		return fmt.Errorf("store put %q: %w", path, err)
	}
	ci := &irbackup.ChunkInfo{
		File:     path,
		RowCount: cb.writer.ChangeCount(),
		SHA256:   hash,
	}
	if cb.b.Encryption != nil {
		ci.Encryption = &irbackup.ChunkEncryption{
			Algorithm:  crypto.AlgorithmAESGCM,
			NonceLen:   crypto.NonceLen,
			AuthTagLen: crypto.AuthTagLen,
			WrappedCEK: cb.curWrappedCEK,
		}
	}
	cb.manifest.ChangeChunks = append(cb.manifest.ChangeChunks, ci)
	out.TotalBytes += nb
	cb.writer = nil
	cb.buf = nil
	cb.curWrappedCEK = nil
	cb.chunkIdx++
	return nil
}

// open starts a fresh change-chunk writer over a new buffer, resolving
// the per-chunk CEK (per-chunk-encryption mode) via the stream's
// envelope. Called lazily by processChange when the first change of a
// chunk arrives.
func (cb *changeChunkBuffer) open() error {
	cb.buf = &bytes.Buffer{}
	cek, wrapped, err := cb.b.resolveChunkCEK(cb.chainCEK)
	if err != nil {
		return fmt.Errorf("resolve chunk cek: %w", err)
	}
	cb.curWrappedCEK = wrapped
	// ADR-0152: bind the chunk to the path + list ordinal flushTo
	// will record it at (chunkIdx only advances at flush, so open and
	// flush agree; the ordinal guards change-REPLAY order).
	path := changeChunkPath(cb.runNamespace, cb.chunkIdx)
	w, err := blobcodec.NewChangeChunkWriter(cb.buf, cek, cb.b.segCodec, irbackup.ChangeChunkAADForWrite(cb.manifest, path, cb.chunkIdx, cek))
	if err != nil {
		return fmt.Errorf("open chunk: %w", err)
	}
	cb.writer = w
	return nil
}

// processChange applies one received change to the in-flight chunk and
// evaluates the window-closing caps. It returns terminate=true when the
// window should commit and return (a cap fired at a tx boundary, or an
// error occurred with err non-nil); terminate=false means the select
// loop should keep draining. inTransaction is updated in place on tx
// boundaries so the caller's other select cases observe it.
//
// The ADR-0067 per-segment dedup floor is applied first: while
// b.skipThrough is set, events at or before P_N (the prior segment's
// EndPosition) are dropped (terminate=false), and the floor is cleared on
// the first event strictly after P_N — KEEPING the (P_N, S] overlap so
// the lineage is born-contiguous and compactable.
func (cb *changeChunkBuffer) processChange(ctx context.Context, change ir.Change, out *captureOutcome, inTransaction *bool, deadlinePassed bool, chunkSize, maxChanges int, maxBytes int64) (terminate bool, err error) {
	if cb.b.skipThrough != nil {
		cp := change.Pos()
		if cp.Engine == "" && cp.Token == "" {
			// Position-less tx boundary while still skipping -- drop it
			// (it belongs to the skipped prefix).
			return false, nil
		}
		if chk, ok := cb.b.Source.(ir.PositionMonotonicChecker); ok {
			le, cerr := chk.PrecedesOrEqual(cp, *cb.b.skipThrough)
			if cerr == nil && le {
				return false, nil // event <= P_N: already in the prior segment
			}
		}
		// First event strictly after P_N (or no comparator): stop
		// skipping; this event starts the new segment.
		cb.b.skipThrough = nil
	}
	switch change.(type) {
	case ir.TxBegin:
		*inTransaction = true
	case ir.TxCommit:
		*inTransaction = false
	}
	if cb.writer == nil {
		if err := cb.open(); err != nil {
			return true, err
		}
	}
	if err := cb.writer.WriteChange(change); err != nil {
		return true, err
	}
	out.TotalChanges++
	pos := change.Pos()
	if pos.Engine != "" || pos.Token != "" {
		out.EndPos = pos
	}
	// Roll the chunk on per-chunk-row cap.
	if cb.writer.ChangeCount() >= int64(chunkSize) {
		if err := cb.flushTo(ctx, out); err != nil {
			return true, err
		}
	}
	// Approximate max-changes cap: close at next tx boundary.
	if maxChanges > 0 && out.TotalChanges >= int64(maxChanges) && !*inTransaction {
		if err := cb.flushTo(ctx, out); err != nil {
			return true, err
		}
		return true, nil
	}
	// Approximate max-bytes cap: close at next tx boundary once the
	// running total + the in-flight chunk's buffered bytes crosses the
	// ceiling. Checked at chunk-flush boundaries so transient over-shoot
	// is bounded by one chunk's compressed size.
	if maxBytes > 0 && !*inTransaction {
		inflightBytes := int64(0)
		if cb.buf != nil {
			inflightBytes = int64(cb.buf.Len())
		}
		if out.TotalBytes+inflightBytes >= maxBytes {
			if err := cb.flushTo(ctx, out); err != nil {
				return true, err
			}
			return true, nil
		}
	}
	if (deadlinePassed || out.StopRequested) && !*inTransaction {
		if err := cb.flushTo(ctx, out); err != nil {
			return true, err
		}
		return true, nil
	}
	return false, nil
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
		slog.InfoContext(
			ctx, "stream: --slot-name supplied but engine has no slot concept; ignoring",
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
func runRolloverHook(ctx context.Context, hookCmd string, manifest *irbackup.Manifest, manifestPath string, changes, bytesWritten int64, elapsed time.Duration) {
	hookCtx, cancel := context.WithTimeout(ctx, DefaultRolloverHookTimeout)
	defer cancel()

	// Use sh -c on Unix-y systems; cmd /C on Windows. The exec.Command
	// stdlib helper handles dispatch via the current OS's shell.
	cmd := newShellCommand(hookCtx, hookCmd)
	cmd.Env = append(
		cmd.Env,
		fmt.Sprintf("SLUICE_ROLLOVER_MANIFEST_PATH=%s", manifestPath),
		fmt.Sprintf("SLUICE_ROLLOVER_PARENT_BACKUP_ID=%s", manifest.ParentBackupID),
		fmt.Sprintf("SLUICE_ROLLOVER_BACKUP_ID=%s", manifest.BackupID),
		fmt.Sprintf("SLUICE_ROLLOVER_CHANGES=%d", changes),
		fmt.Sprintf("SLUICE_ROLLOVER_BYTES=%d", bytesWritten),
		fmt.Sprintf("SLUICE_ROLLOVER_ELAPSED_MS=%d", elapsed.Milliseconds()),
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		slog.WarnContext(
			ctx, "stream: rollover hook failed (rollover already committed)",
			slog.String("hook", hookCmd),
			slog.String("err", err.Error()),
			slog.String("output", strings.TrimSpace(string(out))),
		)
		return
	}
	slog.DebugContext(
		ctx, "stream: rollover hook ok",
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
//
// Bug 43 (v0.22.1): the chain root's recorded Argon2id params are
// passed to [lineage.BackupEncryption.rebindForChain] so the envelope's KEK
// derives against the chain's salt rather than the freshly-minted
// salt the CLI started the run with. Without this rebind, the unwrap
// of the parent's WrappedCEK fails with `aes-gcm open: cipher:
// message authentication failed`.
func (b *BackupStream) alignEncryption(ctx context.Context, parent *irbackup.Manifest) ([]byte, error) {
	rootManifest, parentEnc, err := lineage.ChainRootEncryption(ctx, b.segStore, parent)
	if err != nil {
		// Audit N-6: a failed root-manifest read must NOT be conflated
		// with "parent chain is plaintext" — that branch decides whether
		// this segment's chunks are written encrypted or plaintext.
		return nil, fmt.Errorf("stream: cannot determine parent chain encryption state (refusing to assume plaintext): %w", err)
	}
	switch {
	case parentEnc == nil && b.Encryption == nil:
		return nil, nil
	case parentEnc == nil && b.Encryption != nil:
		return nil, errors.New("stream: parent chain is plaintext but --encrypt was supplied; cannot extend a plaintext chain with encrypted incrementals")
	case parentEnc != nil && b.Encryption == nil:
		return nil, fmt.Errorf("stream: parent chain is encrypted (algorithm=%q kek_mode=%q kek_ref=%q) but no --encrypt + key was supplied",
			parentEnc.Algorithm, parentEnc.KEKMode, parentEnc.KEKRef)
	}
	if err := b.Encryption.RebindForChain(parentEnc.Argon2id); err != nil {
		return nil, fmt.Errorf("stream: rebuild envelope for chain: %w", err)
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
	// Bug 179: the chain's encryption mode is authoritative for EVERY
	// segment. --encrypt-mode sets the mode only for a fresh full; a stream
	// extending an existing chain must use the chain's mode. Refuse an
	// explicit conflicting --encrypt-mode LOUDLY here — otherwise the
	// rollover builds and verifies but is un-restorable, because the restore
	// resolves a single chain mode/CEK from the root full while the sibling
	// resolveChunkCEK would write this segment's chunks under the operator's
	// (mismatched) mode. Inherit it when omitted so resolveChunkCEK agrees
	// with the chain.
	if b.Encryption.Mode != "" && b.Encryption.Mode != mode {
		return nil, fmt.Errorf("stream: --encrypt-mode=%q conflicts with the chain's encryption mode %q; "+
			"an encrypted chain uses one mode for every segment (omit --encrypt-mode to inherit it, or start a fresh full backup)",
			b.Encryption.Mode, mode)
	}
	b.Encryption.Mode = mode
	if mode == crypto.EncryptModePerChain {
		if len(parentEnc.WrappedCEK) == 0 {
			return nil, errors.New("stream: parent's chain encryption is per-chain but WrappedCEK is empty")
		}
		// ADR-0152 chokepoint: the chain-root manifest OWNS the wrap —
		// its recorded FormatVersion decides bound-vs-legacy, and the
		// Azure key-version retarget (audit N-9) rides along.
		cek, err := lineage.UnwrapChainCEK(b.Encryption.Envelope, parentEnc.WrappedCEK, rootManifest)
		if err != nil {
			return nil, fmt.Errorf("stream: unwrap parent chain cek: %w", err)
		}
		return cek, nil
	}
	// Per-chunk mode: retarget the envelope's key version before the
	// probe below / the per-chunk wraps this run performs.
	lineage.RebindEnvelopeKEK(b.Encryption.Envelope, rootManifest)
	// Per-chunk mode: probe the operator's envelope against one of the
	// parent's existing chunk WrappedCEKs so a rotated passphrase
	// surfaces loudly at stream-extend start. Mirrors the Bug 117
	// ingestion-path closure in [IncrementalBackup.alignEncryption];
	// stream and incremental share the same risk shape on per-chunk
	// rotation.
	if probe := firstPerChunkProbe(parent); probe != nil {
		if err := lineage.ProbeChunkDecrypt(b.Encryption.Envelope, probe); err != nil {
			return nil, fmt.Errorf("stream: %w", err)
		}
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
