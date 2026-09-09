// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Resuming a cold start that was STOPPED after its copy committed but
// before its CDC anchor was written (audit 2026-09-09 A0909-STOP-1).
//
// # The state this exists for
//
// v0.148.0 made a stop in that window KEEP the source's replication
// slot instead of dropping it, on the reasoning that the slot's
// consistent point is the anchor a resume would need. v0.148.1 had to
// correct the release notes to admit the kept slot bought nothing: the
// anchor is written at the very END of the cold start
// ([Streamer.coldStartBeginCDC]), so a stop before it left no persisted
// position at all. A re-run found none, cold-started, and died on
// "replication slot … already exists". The operator's only exit was to
// drop the slot and re-copy everything — on the v0.148.2 regression
// cycle, 2.5M rows of it.
//
// This file is the resume that was missing. It skips the copy, finishes
// the post-copy phases, takes the CDC anchor from the recorded
// consistent point, and enters CDC through the ordinary warm-resume
// path.
//
// # Scope, stated rather than implied
//
// PostgreSQL sources on the RECORDING cold-start path, and nothing
// else:
//
//   - The source must implement [ir.SnapshotAnchorVerifier]. Only
//     postgres does. MySQL / MariaDB / Vitess / PlanetScale, SQLite,
//     D1 and the trigger engines keep today's behaviour exactly —
//     their cold start after a stop still refuses, because nothing can
//     prove where their source stands.
//   - The recorded anchor exists only if the cold start ran through the
//     ADR-0079 fast parallel path, which is the only path that builds a
//     recording context. A serial PG cold start (--schema-already-applied,
//     an interrupted-COPY resume, the A0 client-copy fallback) records
//     no anchor and is not resumable here.
//   - The multi-namespace cold start ([Streamer.coldStartMultiDatabase])
//     is deliberately NOT reached: it copies serially, records nothing,
//     and its stop path still calls bare abandonStream(). Widening that
//     door is filed separately and must not be done from here.
//   - --reset-target-data and --restart-from-scratch are handled by
//     earlier branches of the dispatch switch and never reach this
//     gate: an operator who asked for a fresh copy gets one.
//
// # Why the phase cannot be the proof (a correction to the filing,
// MEASURED)
//
// The audit entry proposed gating on the recorded PHASE — "identity_sync,
// indexes, constraints, views or complete proves the copy finished".
// On this path the phase says neither of the two things that would make
// that work, and both were measured on real PostgreSQL rather than
// reasoned about:
//
//  1. It can be AHEAD of the copy. The fast cold start runs the copy and
//     the index build OVERLAPPED (ADR-0077) under one errgroup, and an
//     index-axis failure is recorded as phase `indexes` by
//     markFailedLocked while the copy pool may still have been mid-table.
//  2. On an operator STOP it does not move at all. markFailed's write
//     runs on the caller's context, which is exactly the context the
//     stop cancelled, so the write fails and is WARNed. Measured: a stop
//     injected at the index build leaves the header at `bulk_copy`, the
//     last phase whose mark was written while the context was alive —
//     which is the phase the filing's gate would have refused, in the
//     precise window it exists to cover.
//
// So the phase is not the evidence. The PER-TABLE progress rows are:
// every in-scope table must carry a recorded `complete`. Those are
// written by the copy pool at each table's own completion (terminal
// states are never throttled), they are durable before the stop, and
// they answer the question the gate is actually asking. The phase is
// kept only as a coarse floor — a run that never reached the copy
// cannot be resumed however its rows read.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// coldStartResumedMarker is the grep-stable marker on the line a
// resumed cold start emits. An operator who stopped a sync and re-ran
// it needs one token to search their logs for to see that the copy was
// SKIPPED rather than silently redone — and, if they later doubt the
// target, one token that leads to what this run did and did not do.
const coldStartResumedMarker = "COLD-START-RESUMED"

// coldStartAnchorMovedMarker is the grep-stable marker on the refusal
// that fires when the recorded anchor is no longer where the source
// stands. It is a REFUSAL rather than a fall-through to a fresh cold
// start because the two positions it names are the operator's evidence
// that something else consumed their slot.
const coldStartAnchorMovedMarker = "COLD-START-ANCHOR-MOVED"

// resumeStoppedColdStart is the A0909-STOP-1 handoff resume.
//
// handled=false means this stream is not resumable and the caller must
// proceed exactly as it would have without this function — every
// no-evidence path returns it, so the fall-through behaviour is
// today's, including today's refusals. handled=true with a non-nil
// error is a deliberate refusal.
//
// It runs BEFORE the cold start, so a decline costs one header read
// (and, past the cheap gates, one source schema read the cold start
// then repeats).
func (s *Streamer) resumeStoppedColdStart(
	ctx, streamCtx context.Context,
	lsnTracker any,
	applier ir.ChangeApplier,
	streamID string,
) (changes <-chan ir.Change, stop func(), handled bool, err error) {
	// Gate 1a: only a source that can PROVE where it stands.
	verifier, ok := s.Source.(ir.SnapshotAnchorVerifier)
	if !ok {
		return nil, nil, false, nil
	}
	// Gate 1b: only a target that recorded something to stand on.
	store, err := openMigrationStateStore(ctx, s.Target, s.TargetDSN, s.TargetSchema)
	if err != nil {
		slog.WarnContext(ctx, "pipeline: could not open the target's progress store to check whether this "+
			"stopped cold start can be resumed; continuing as if it cannot",
			slog.String("stream_id", streamID),
			slog.String("error", err.Error()))
		return nil, nil, false, nil
	}
	if store == nil {
		return nil, nil, false, nil
	}
	defer migcore.CloseIf(store)

	migrationID := syncMigrationID(streamID)
	state, found, readErr := readRecordedColdStart(ctx, store, migrationID)
	if readErr != nil {
		slog.WarnContext(ctx, "pipeline: could not read the recorded cold-start progress for this stream; "+
			"continuing as if it cannot be resumed",
			slog.String("stream_id", streamID),
			slog.String("error", readErr.Error()))
		return nil, nil, false, nil
	}
	// Gates 3a + 4: a header exists, its phase is past the copy, and it
	// carries an anchor. A row written by a binary older than the anchor
	// column has none, and that is not evidence of anything.
	if why := gradeRecordedColdStartHeader(state, found); why != "" {
		slog.DebugContext(ctx, "pipeline: this stream has no resumable stopped cold start",
			slog.String("stream_id", streamID),
			slog.String("reason", why))
		return nil, nil, false, nil
	}

	// The schema, exactly as the interrupted run shaped it: the same two
	// calls the cold start makes, so the indexes/constraints/views this
	// resume creates are the ones that run would have created.
	// resumingCopy=true skips the slot-headroom preflight — this stream's
	// slot already exists and no new one is consumed.
	schema, _, err := s.coldStartReadSourceSchema(ctx, true)
	if err != nil {
		slog.WarnContext(ctx, "pipeline: could not read the source schema while checking for a resumable "+
			"stopped cold start; continuing as if it cannot be resumed",
			slog.String("stream_id", streamID),
			slog.String("error", err.Error()))
		return nil, nil, false, nil
	}
	if schema == nil {
		return nil, nil, false, nil
	}
	// coldStartPrepareSchema also captures the SLM-1 reader schema seed
	// (the RAW source IR) into the Streamer. That is the right seed for
	// this path and not an accident of reuse: the target was shaped by
	// the cold start and the stream starts at that copy's anchor with no
	// deltas applied, so the reader's prior shape is the cold start's,
	// not a warm resume's target witness. warmResume consumes it below.
	schema, err = s.coldStartPrepareSchema(schema)
	if err != nil {
		return nil, nil, false, nil
	}
	if s.SkipForeignKeys {
		logSkipForeignKeys(ctx, applySkipForeignKeys(schema))
	}

	// Gate 3b, the load-bearing one: every in-scope table recorded
	// complete. See the file comment for why the phase alone is not it.
	if missing, ok := everyTableCopied(schema, state); !ok {
		slog.InfoContext(ctx, "pipeline: this stream's recorded cold start did NOT finish its bulk copy, so "+
			"it cannot be resumed; the run proceeds exactly as it would have without a recorded copy",
			slog.String("stream_id", streamID),
			slog.String("phase", string(state.Phase)),
			slog.String("first_unfinished_table", missing))
		return nil, nil, false, nil
	}

	// Gate 5: the independent witness. Everything above is the target's
	// account of what a previous process did; this is the SOURCE's
	// account of where it stands now, and it is the only one that can
	// tell "nothing has consumed the slot" from "something has".
	anchor, err := verifier.VerifySnapshotAnchor(ctx, s.SourceDSN, s.SlotName, state.SnapshotAnchor)
	switch {
	case err == nil:
	case errors.Is(err, ir.ErrSnapshotAnchorAbsent):
		// The slot is gone, so there is nothing to resume FROM. Proceed
		// as today: the cold start refuses on the populated target,
		// loudly, which is the honest answer.
		slog.InfoContext(ctx, "pipeline: this stream's recorded cold start finished its copy, but the "+
			"replication slot it anchored on no longer exists, so there is nothing to resume from",
			slog.String("stream_id", streamID),
			slog.String("error", err.Error()))
		return nil, nil, false, nil
	default:
		return nil, nil, true, fmt.Errorf(
			"pipeline: %s: this stream's recorded cold start finished its bulk copy, but its snapshot anchor "+
				"could not be proven untouched, so resuming from it could silently skip changes. sluice has "+
				"changed NOTHING. Either investigate what moved the anchor and re-run once it is quiescent, or "+
				"drop the replication slot with `sluice slot drop` and re-run `sluice sync start` with "+ // remedy-partial: the operator's own invocation carries their DSNs
				"--reset-target-data to copy again: %w",
			coldStartAnchorMovedMarker, err,
		)
	}

	// Committed to the resume from here on: every failure below is a
	// real failure of work this run OWNS, and is returned rather than
	// degraded into a silent fresh cold start.
	slog.InfoContext(ctx, "pipeline: "+coldStartResumedMarker+": a previous cold start for this stream was "+
		"STOPPED after its bulk copy had committed every in-scope table but before the CDC anchor was "+
		"recorded. Its replication slot is still exactly where that snapshot was taken, so the copy is being "+
		"RESUMED rather than redone: the bulk copy is SKIPPED, the remaining post-copy phases are finished "+
		"from the recorded phase, and CDC starts from the recorded snapshot anchor — every source change "+
		"since that snapshot is still on the slot and will be delivered",
		slog.String("stream_id", streamID),
		slog.String("recorded_phase", string(state.Phase)),
		slog.Int("tables_already_copied", len(schema.Tables)),
		slog.String("slot", resolveSlotNameForDisplay(s.SlotName)))

	sw, err := s.openColdStartSchemaWriter(ctx)
	if err != nil {
		return nil, nil, true, err
	}
	defer migcore.CloseIf(sw)

	// A recording context for the REMAINING phases, so a stop during
	// them records where it got to and the next attempt resumes from
	// there. Deliberately NOT beginRecordedColdStart: that clears the
	// recorded state to start a fresh run, and this run is the
	// continuation of the recorded one.
	rc := newSyncRecordingContext(ctx, store, streamID)
	if err := runColdStartResumePhases(ctx, rc, &state, schema, sw); err != nil {
		return nil, nil, true, err
	}

	// The CDC anchor, on an UNCANCELLABLE ctx for exactly the reason
	// coldStartBeginCDC's is (see [coldStartAnchorWriteTimeout]): a stop
	// landing on this write must not be the reason the durability record
	// is missing — that is the wedge this whole file exists to undo.
	if pw, ok := applier.(ir.PositionWriter); ok {
		anchorCtx, anchorCancel := context.WithTimeout(context.WithoutCancel(ctx), coldStartAnchorWriteTimeout)
		writeErr := pw.WritePosition(anchorCtx, streamID, anchor)
		anchorCancel()
		if writeErr != nil {
			return nil, nil, true, migcore.WrapWithHint(migcore.PhaseCDC,
				fmt.Errorf("pipeline: persist resumed cold-start CDC anchor position: %w", writeErr))
		}
	} else {
		slog.WarnContext(ctx, "applier does not implement ir.PositionWriter; the resumed cold start's CDC "+
			"anchor cannot be persisted — a stop before the first applied batch will be unresumable again",
			slog.String("stream_id", streamID))
	}
	markComplete(ctx, rc, state)

	// Enter CDC through the ORDINARY warm-resume path. Nothing about
	// this stream is special any more: the target holds the copy, the
	// anchor row holds the position, and the slot holds the WAL from
	// that position onward — which is precisely the state a cold start
	// that ran to completion leaves behind.
	changes, stop, err = s.warmResume(streamCtx, anchor, lsnTracker)
	return changes, stop, true, err
}

// readRecordedColdStart reads the recorded header, healing the one
// cross-version shape that would otherwise error: a control table
// created by a binary older than the snapshot_anchor column, whose
// header SELECT then names a column that does not exist. A first read
// failure is retried once behind EnsureControlTable (which adds it).
//
// The read is tried FIRST so the common case — a target that has never
// run a sync cold start, where the table legitimately does not exist —
// costs one tolerated SELECT and issues no DDL at all.
func readRecordedColdStart(ctx context.Context, store ir.MigrationStateStore, migrationID string) (ir.MigrationState, bool, error) {
	state, found, err := store.Read(ctx, migrationID)
	if err == nil {
		return state, found, nil
	}
	if ensureErr := store.EnsureControlTable(ctx); ensureErr != nil {
		return ir.MigrationState{}, false, errors.Join(err, ensureErr)
	}
	return store.Read(ctx, migrationID)
}

// gradeRecordedColdStartHeader applies the header-only half of the
// resume gate and returns "" when it passes, or the reason it did not.
// Pure, so the ladder is table-testable without a store.
func gradeRecordedColdStartHeader(state ir.MigrationState, found bool) string {
	if !found {
		return "no cold start has been recorded for this stream"
	}
	if !coldStartPhaseAllowsResume(state.Phase) {
		return "the recorded phase is " + string(state.Phase) + ", which is before the bulk copy"
	}
	if state.SnapshotAnchor == "" {
		// The honest reading of an empty column: a run by a binary that
		// predates it, or one whose anchor write failed. Either way this
		// is an absence of evidence, never an empty anchor.
		return "the recorded run carries no snapshot anchor (it was written by an older sluice, or its anchor could not be recorded)"
	}
	return ""
}

// coldStartPhaseAllowsResume is the coarse floor, not the proof: it
// admits every phase from which a run could have a finished copy behind
// it, and refuses the ones that precede the copy entirely.
//
// `bulk_copy` is ADMITTED, which is the whole point of the measurement
// in the file comment: on an operator stop the failure mark cannot be
// written (its context is the cancelled one), so the header keeps the
// last phase marked while the context was alive — `bulk_copy` for the
// index-build window this feature exists for. Refusing it would make
// the gate unreachable in its own motivating case.
//
// Admitting it is safe because it decides nothing on its own:
// [everyTableCopied] is what proves the copy finished, and a genuine
// mid-copy stop leaves at least one table without a `complete` row.
// What this predicate rules out is a run that never got as far as
// copying — `pending`, `tables`, the reserved `failed` literal, and the
// empty value a half-written row could carry — where a "finished copy"
// reading could only come from stale or corrupt evidence.
func coldStartPhaseAllowsResume(phase ir.MigrationPhase) bool {
	switch phase {
	case ir.MigrationPhaseBulkCopy,
		ir.MigrationPhaseIdentitySync,
		ir.MigrationPhaseIndexes,
		ir.MigrationPhaseConstraints,
		ir.MigrationPhaseViews,
		ir.MigrationPhaseComplete:
		return true
	default:
		return false
	}
}

// everyTableCopied reports whether every in-scope table carries a
// recorded `complete`, and names the first that does not.
//
// The name is returned rather than a count because the operator-facing
// line is more useful for it, and because a gate that says only "no"
// invites the reader to assume it looked at more than it did: this
// looks at exactly the tables the CURRENT run has in scope, so a table
// added to the filter since the interrupted run correctly reads as
// not-copied.
func everyTableCopied(schema *ir.Schema, state ir.MigrationState) (firstMissing string, ok bool) {
	if schema == nil || len(schema.Tables) == 0 {
		return "", false
	}
	for _, table := range schema.Tables {
		if state.TableProgress[table.Name].State != ir.TableProgressComplete {
			return table.Name, false
		}
	}
	return "", true
}

// runColdStartResumePhases finishes the post-copy ladder from the
// recorded phase onward, using migrate's own recorded-phase functions
// so a change to how a phase is marked, retried or attributed reaches
// this path too.
//
// The ladder is NOT simply "everything after the recorded phase",
// because the recorded phase says which step was IN FLIGHT, and on the
// PG fast path the phases do not run in the enum's order — the index
// build is overlapped with the copy and identity-sync follows BOTH. So:
//
//   - bulk_copy or indexes: the overlapped copy+index phase was in
//     flight, so the indexes may be partial and identity-sync has not
//     run. Build the indexes (CreateIndexes is idempotent, Bug 131),
//     then continue. These two are ONE case, not two: on an operator
//     stop the header keeps `bulk_copy` because the failure mark's own
//     write is cancelled with everything else (see the file comment),
//     so `bulk_copy`-with-a-finished-copy and `indexes` describe the
//     same interrupted phase.
//   - identity_sync: the index phase completed (it precedes
//     identity-sync in every branch); re-run identity-sync, which is
//     idempotent, then continue.
//   - constraints / views: re-run that phase and the ones after it.
//   - complete: every phase finished; only the anchor was missing.
//
// verifyBuiltIndexes runs on EVERY branch, including complete. It is
// the SLUICE-E-INDEX-MISSING net migrate runs after its index phase,
// and it is the one check here that does not derive its answer from the
// recorded state: it asks the TARGET whether the indexes are actually
// there. A resume that skipped a copy on the strength of a progress row
// should not also take that row's word for the index build.
func runColdStartResumePhases(
	ctx context.Context,
	rc resumeContext,
	state *ir.MigrationState,
	schema *ir.Schema,
	sw ir.SchemaWriter,
) error {
	from := state.Phase
	inIndexWindow := from == ir.MigrationPhaseBulkCopy || from == ir.MigrationPhaseIndexes
	if inIndexWindow {
		if err := runIndexesPhase(ctx, rc, state, schema, sw, false); err != nil {
			return err
		}
	}
	if err := verifyBuiltIndexes(ctx, sw, schema); err != nil {
		err = fmt.Errorf("pipeline: verify indexes: %w", err)
		return migcore.WrapWithHint(migcore.PhaseIndexes, markFailed(ctx, rc, *state, ir.MigrationPhaseIndexes, err))
	}
	if inIndexWindow || from == ir.MigrationPhaseIdentitySync {
		if err := runIdentitySyncPhase(ctx, rc, state, schema, sw); err != nil {
			return err
		}
	}
	if from != ir.MigrationPhaseViews && from != ir.MigrationPhaseComplete {
		if err := runConstraintsPhase(ctx, rc, state, schema, sw); err != nil {
			return err
		}
	}
	if from != ir.MigrationPhaseComplete {
		if err := runRecordedViewsPhase(ctx, rc, state, schema, sw); err != nil {
			return err
		}
	}
	return nil
}

// resolveSlotNameForDisplay renders the RESOLVED slot name for an
// operator-facing line — the object that exists on the server, never
// the raw --slot-name suffix. Same call abandonUnlessStopped makes, for
// the same reason.
func resolveSlotNameForDisplay(operatorSupplied string) string {
	if slot := ResolveSlotName(operatorSupplied); slot != "" {
		return slot
	}
	return defaultSlotNameForAdvice
}
