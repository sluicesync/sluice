// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// # The persisted unforwarded-schema-change refusal
//
// The CDC readers' unforwarded-schema-change door (GC-2) compares each
// table's constraints, policies, row level security and defaults against a
// baseline the reader takes when it STARTS, and ends the stream with
// [ir.ErrUnforwardedSchemaChange] on a difference. GC-32 carried that
// baseline across retries inside one Run. A new PROCESS had no baseline to
// carry — systemd's Restart=on-failure, a Kubernetes pod restart, a
// supervisor — so its fresh reader baselined the catalog that already held
// the refused change and accepted it silently, forever.
//
// So the refusal outlives the process: a run that ends with it records it
// (the `sync` control-table row via [ir.UnforwardedRefusalStore]; `backup
// stream`'s stream_state.json), and the next start refuses again BEFORE any
// change stream opens, until the operator passes
// --accept-unforwarded-schema-change — which is meant to follow applying the
// change to the target (or, for a backup chain, taking a new full backup).

// unforwardedRefusalAckFlag is the acknowledgement flag, named once so the
// refusal text and the CLI cannot drift apart.
const unforwardedRefusalAckFlag = "--accept-unforwarded-schema-change"

// unforwardedRefusalMaxLen caps the recorded refusal. Larger than
// [lastErrorMaxLen] because the refusal's payload is its tail — the list of
// schema objects that changed — and 1 KiB routinely cut that off behind the
// fixed-length remedy prose.
const unforwardedRefusalMaxLen = 4096

// unforwardedRefusalWriteTimeout bounds the record write. The write runs on
// a cancel-immune context: the refusal usually ends the run by cancelling
// it, and a record that is skipped because the context was already done is
// exactly the silent re-baseline this file exists to prevent.
const unforwardedRefusalWriteTimeout = 30 * time.Second

// storableUnforwardedRefusal makes msg storable in a text column (see
// [storableDiagnostic]) and caps it at [unforwardedRefusalMaxLen] bytes on a
// rune boundary, marking a cut with an ellipsis.
func storableUnforwardedRefusal(msg string) string {
	msg = storableDiagnostic(msg)
	if len(msg) <= unforwardedRefusalMaxLen {
		return msg
	}
	const ellipsis = "…"
	return cutAtRuneBoundary(msg, unforwardedRefusalMaxLen-len(ellipsis)) + ellipsis
}

// recordedUnforwardedRefusalError is the startup door's refusal: a previous
// run recorded an unforwarded-schema-change refusal and this start did not
// acknowledge it. It wraps [ir.ErrUnforwardedSchemaChange] (so the fleet and
// every caller classify it exactly as the reader's own refusal) and is
// terminal — a retry reads the same record.
//
// It is a distinct type so the end-of-run recorder can tell it apart from a
// FRESH refusal: re-recording it would nest the recorded message inside
// itself on every restart.
type recordedUnforwardedRefusalError struct {
	// where names the record, for the operator who wants to inspect it.
	where string
	// recorded is the refusal as the previous run stored it.
	recorded string
	// remedy is the surface-specific step (1).
	remedy string
}

func (e *recordedUnforwardedRefusalError) Error() string {
	return fmt.Sprintf("pipeline: %s: a previous run stopped on this refusal and it is still recorded (%s); "+
		"a restart without %s refuses again, because a restarted reader baselines the source catalog that already "+
		"carries the change and would accept it silently. Remedy: (1) %s; (2) restart with %s, which clears the record "+
		"and takes a fresh baseline — passing it WITHOUT step 1 accepts the difference permanently. Recorded refusal: %s",
		ir.ErrUnforwardedSchemaChange, e.where, unforwardedRefusalAckFlag, e.remedy, unforwardedRefusalAckFlag, e.recorded)
}

func (e *recordedUnforwardedRefusalError) Unwrap() error            { return ir.ErrUnforwardedSchemaChange }
func (e *recordedUnforwardedRefusalError) Terminal() bool           { return true }
func (e *recordedUnforwardedRefusalError) Retriable() bool          { return false }
func (e *recordedUnforwardedRefusalError) RetryHint() time.Duration { return 0 }

var (
	_ ir.TerminalError  = (*recordedUnforwardedRefusalError)(nil)
	_ ir.RetriableError = (*recordedUnforwardedRefusalError)(nil)
)

// freshUnforwardedRefusal reports whether err is an unforwarded-schema-change
// refusal raised by THIS run's reader — the one that has to be recorded. The
// startup door's own replay of a recorded refusal is excluded.
func freshUnforwardedRefusal(err error) bool {
	if !errors.Is(err, ir.ErrUnforwardedSchemaChange) {
		return false
	}
	var replay *recordedUnforwardedRefusalError
	return !errors.As(err, &replay)
}

// syncUnforwardedRefusalRemedy is step (1) of the `sync` remedy.
const syncUnforwardedRefusalRemedy = "apply the same change to the target yourself"

// phaseRefuseRecordedUnforwardedChange is the startup door: it runs on every
// attempt that reaches the change-stream dispatch — before any CDC reader
// opens — and refuses when the stream's control-table row carries a
// recorded refusal the operator has not acknowledged. With
// [Streamer.AcceptUnforwardedSchemaChange] it clears the record, logs a WARN
// naming what was accepted, and continues; the reader then takes a fresh
// baseline, which is the operator's deliberate choice.
//
// It does not reason about which dispatch branch follows — warm resume,
// multi-database warm resume, the interrupted-COPY resume, the
// --restart-from-scratch / --reset-target-data recoveries — because none of
// them is guaranteed to carry the change: the classes the door covers are
// exactly the ones sluice's own schema apply and CDC do not forward.
//
// An applier without the store is a test stub: every real applier
// implements it (pinned in both engines' capabilities_assert.go), and the
// engines without an applier refuse to be a sync target at all.
func (s *Streamer) phaseRefuseRecordedUnforwardedChange(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	store, ok := applier.(ir.UnforwardedRefusalStore)
	if !ok {
		return nil
	}
	recorded, found, err := store.ReadUnforwardedRefusal(ctx, streamID)
	if err != nil {
		return connectHint(fmt.Errorf("pipeline: read the recorded unforwarded-schema-change refusal: %w", err))
	}
	if !found {
		return nil
	}
	if !s.AcceptUnforwardedSchemaChange {
		return &recordedUnforwardedRefusalError{
			where:    fmt.Sprintf("stream %q, sluice_cdc_state.unforwarded_refusal on the target", streamID),
			recorded: recorded,
			remedy:   syncUnforwardedRefusalRemedy,
		}
	}
	if err := store.ClearUnforwardedRefusal(ctx, streamID); err != nil {
		return connectHint(fmt.Errorf("pipeline: %s: clear the recorded unforwarded-schema-change refusal: %w", unforwardedRefusalAckFlag, err))
	}
	// One-shot within the process too: a supervisor restarting this same
	// Streamer after a NEW refusal must not find it pre-accepted.
	s.AcceptUnforwardedSchemaChange = false
	slog.WarnContext(
		ctx, "accepted a recorded unforwarded-schema-change refusal ("+unforwardedRefusalAckFlag+"); "+
			"the record is cleared and the reader takes a fresh baseline, so the accepted difference will not be reported again",
		slog.String("stream_id", streamID),
		slog.String("accepted_refusal", recorded),
	)
	return nil
}

// recordUnforwardedRefusal persists a fresh unforwarded-schema-change
// refusal that ended this attempt, so the next start refuses again (see
// [Streamer.phaseRefuseRecordedUnforwardedChange]). runErr is returned to
// the operator unchanged by the caller either way; a record that fails is
// logged at ERROR, because it means the next restart will NOT refuse.
func (s *Streamer) recordUnforwardedRefusal(ctx context.Context, applier ir.ChangeApplier, streamID string, runErr error) {
	if !freshUnforwardedRefusal(runErr) {
		return
	}
	store, ok := applier.(ir.UnforwardedRefusalStore)
	if !ok {
		logUnforwardedRefusalNotRecorded(ctx, slog.String("stream_id", streamID), "the target applier cannot persist it", runErr)
		return
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unforwardedRefusalWriteTimeout)
	defer cancel()
	if err := store.RecordUnforwardedRefusal(writeCtx, streamID, storableUnforwardedRefusal(runErr.Error())); err != nil {
		logUnforwardedRefusalNotRecorded(ctx, slog.String("stream_id", streamID), err.Error(), runErr)
		return
	}
	slog.InfoContext(
		ctx, "recorded the unforwarded-schema-change refusal on the target; every restart refuses until "+unforwardedRefusalAckFlag,
		slog.String("stream_id", streamID),
	)
}

// logUnforwardedRefusalNotRecorded is the loud half of a failed record.
func logUnforwardedRefusalNotRecorded(ctx context.Context, which slog.Attr, why string, runErr error) {
	slog.ErrorContext(
		ctx, "could NOT record the unforwarded-schema-change refusal: a restart of this stream will NOT refuse again — "+
			"it will take a fresh baseline and silently accept the change. Do not restart until the change is applied "+
			"to the target (for `backup stream`, until a new full backup is taken)",
		which,
		slog.String("why", why),
		slog.String("refusal", runErr.Error()),
	)
}

// backupUnforwardedRefusalRemedy is step (1) of the `backup stream` remedy:
// the chain cannot be patched, so the change has to reach it through a new
// full backup.
const backupUnforwardedRefusalRemedy = "take a new full backup, so the chain carries the change"

// refuseRecordedUnforwardedChange is `backup stream run`'s startup door, the
// sibling of [Streamer.phaseRefuseRecordedUnforwardedChange]: it runs in
// setup before the CDC pump opens and replays a refusal a previous run
// recorded in the stream-state file. With
// [BackupStream.AcceptUnforwardedSchemaChange] it lets the run proceed; the
// record is then dropped by the initial state write, which rewrites the whole
// file, so it survives a setup that fails before the stream actually starts.
func (b *BackupStream) refuseRecordedUnforwardedChange(ctx context.Context, statePath string) error {
	prior, err := readStreamState(ctx, b.Store, statePath)
	if err != nil {
		return fmt.Errorf("stream: read the recorded unforwarded-schema-change refusal: %w", err)
	}
	if prior == nil || prior.UnforwardedRefusal == "" {
		return nil
	}
	if !b.AcceptUnforwardedSchemaChange {
		return &recordedUnforwardedRefusalError{
			where:    "the backup destination's " + statePath,
			recorded: prior.UnforwardedRefusal,
			remedy:   backupUnforwardedRefusalRemedy,
		}
	}
	slog.WarnContext(
		ctx, "accepting a recorded unforwarded-schema-change refusal ("+unforwardedRefusalAckFlag+"); "+
			"the record is cleared when the stream starts and the reader takes a fresh baseline, so the chain "+
			"will not carry the accepted difference and nothing will report it again",
		slog.String("state_path", statePath),
		slog.String("accepted_refusal", prior.UnforwardedRefusal),
	)
	return nil
}

// recordUnforwardedRefusal persists a fresh unforwarded-schema-change refusal
// that ended [BackupStream.Run] into the stream-state file (read-modify-write,
// so the liveness fields and any stop request survive). Runs on a
// cancel-immune context for the reason [unforwardedRefusalWriteTimeout]
// gives; a failure is logged at ERROR because the next run will NOT refuse.
func (b *BackupStream) recordUnforwardedRefusal(ctx context.Context, statePath string, runErr error) {
	if !freshUnforwardedRefusal(runErr) {
		return
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unforwardedRefusalWriteTimeout)
	defer cancel()
	prior, err := readStreamState(writeCtx, b.Store, statePath)
	if err != nil {
		logUnforwardedRefusalNotRecorded(ctx, slog.String("state_path", statePath), err.Error(), runErr)
		return
	}
	if prior == nil {
		prior = &streamState{}
	}
	prior.UnforwardedRefusal = storableUnforwardedRefusal(runErr.Error())
	if err := writeStreamState(writeCtx, b.Store, statePath, prior); err != nil {
		logUnforwardedRefusalNotRecorded(ctx, slog.String("state_path", statePath), err.Error(), runErr)
		return
	}
	slog.InfoContext(
		ctx, "recorded the unforwarded-schema-change refusal in the stream state; every restart refuses until "+unforwardedRefusalAckFlag,
		slog.String("state_path", statePath),
	)
}
