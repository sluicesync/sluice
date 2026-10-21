// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The VStream copy-completion door (VSTREAM-COPY-COMPLETION-UNPROVEN, Bug 327).
//
// # The harm
//
// Until v0.163.3 a PlanetScale/Vitess cold start persisted checkpoints
// during its copy (ADR-0072 Phase B). Bug 325 froze the checkpoint at the
// first one, written a few thousand rows in, and that first checkpoint
// often carried no copy cursor: a bare per-shard GTID list, byte for byte
// the shape of a finished copy's CDC anchor. v0.163.3 refuses a persisted
// position that carries a cursor (VSTREAM-COPY-RESUME-WITHDRAWN), but a
// cursor-less one reaches the warm resume, which copies nothing: the rest of
// the copy is never made and the stream reports healthy. The v0.163.3
// regression cycle measured 40,000–49,000 of 300,000 rows missing at exit 0.
//
// # The evidence, and why it is these two
//
// The position cannot tell the two apart, so the door asks the target for a
// positive marker that this stream's copy FINISHED. Two exist, both written
// only once a copy has finished and neither by a copy still running:
//
//   - the established-table record ([ir.EstablishedTablesStore]): since
//     v0.163.0 the cold start writes it in the CDC anchor's own transaction
//     ([writeColdStartAnchor]), after the copy. A mid-copy checkpoint never
//     wrote it, so a v0.163.0/v0.163.1 stream stopped mid-copy has none.
//   - the `sync-<stream-id>` header in sluice_migrate_state
//     ([syncMigrationID]): since v0.149.0 every cold-start lane records it
//     and flips it to phase complete immediately after the bulk copy returns
//     clean ([markRecordedColdStartComplete]); [beginRecordedColdStart]
//     clears it at the start of the next cold start and nothing clears it
//     while the stream runs. A stream stopped mid-copy holds a header in an
//     earlier phase.
//
// A stream whose last cold start finished on v0.99.5–v0.148.x carries
// neither, and is refused. That false refusal was accepted by the operator
// (2026-10-21) as the price of refusing the silent loss; --assert-copy-finished
// is its escape.
//
// # Fail closed
//
// The door runs exactly where persisted state is legacy or missing, so no
// read it makes is allowed to pass by failing: a record or header that
// cannot be read refuses (restartable, through [controlReadFailure]); a
// target that keeps neither surface proves nothing and refuses (the flag is
// the escape); a header is evidence only at phase complete, never merely by
// existing.
//
// # Where it runs, and who reaches it
//
// On both warm-resume branches of [Streamer.phaseOpenChangeStream], first,
// before the INCLUDE-TABLE-SCOPE-DRIFT door and the reader open, so a
// refusal has applied no change and changed no target table. Every caller
// that warm-resumes a VStream position from the target's control row goes
// through there: `sync start`, every `sync run` leg and supervisor restart
// (Streamer.Run), and the in-process retry ladder (runOnce again). The
// door is skipped, deliberately, for --position-from-manifest: that position
// is a backup chain's terminal manifest, never a sync cold start's copy
// checkpoint, and the copy under it is the restore's. `sync from-backup run`
// (the broker) has its own position format and never opens a VStream reader.
// A cold start, --restart-from-scratch and --reset-target-data never reach a
// warm-resume branch.

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// vstreamCopyUnprovenMarker names the refusal of a VStream warm resume whose
// copy nothing on the target proves finished.
const vstreamCopyUnprovenMarker = "VSTREAM-COPY-COMPLETION-UNPROVEN"

// copyFinishedAssertedMarker names the WARN of a resume the operator let
// through with --assert-copy-finished.
const copyFinishedAssertedMarker = "COPY-FINISHED-ASSERTED"

// copyCompletion is what the target says about a stream's copy: proven, or
// unproven with the reason (the evidence it looked for and did not find).
type copyCompletion struct {
	proven bool
	why    string
}

// vstreamCopyDoorApplies reports whether the copy-completion door judges this
// run: a source that copies through VStream (the only source whose cold start
// ever persisted a position during its copy), with the position read from the
// target's control row rather than a backup chain.
func (s *Streamer) vstreamCopyDoorApplies() bool {
	return s.Source.Capabilities().CDC == ir.CDCVStream && s.PositionFromManifestStore == nil
}

// phaseRefuseUnprovenVStreamCopy is the door. It returns assertRecord=true
// when it passed on the operator's --assert-copy-finished alone: the caller
// then records the stream's tables once the scope door has passed
// ([Streamer.recordAssertedCopy]), so later resumes pass on the record.
func (s *Streamer) phaseRefuseUnprovenVStreamCopy(ctx context.Context, applier ir.ChangeApplier, streamID string, persisted ir.Position) (assertRecord bool, err error) {
	if !s.vstreamCopyDoorApplies() {
		return false, nil
	}
	c, err := s.vstreamCopyCompletion(ctx, applier, streamID)
	if err != nil {
		return false, err
	}
	if c.proven {
		return false, nil
	}
	if s.AssertCopyFinished {
		slog.WarnContext(ctx, copyFinishedAssertedMarker+": nothing on the target proves this PlanetScale/Vitess stream's "+
			"cold-start copy finished ("+c.why+"); resuming because --assert-copy-finished says it did. If it did not, "+
			"the rows the copy never reached stay missing on the target; re-copy with --restart-from-scratch",
			slog.String("stream_id", streamID),
			slog.String("position_token", truncateDryRunToken(persisted.Token, 60)))
		return true, nil
	}
	slog.ErrorContext(ctx, "resume refused: "+vstreamCopyUnprovenMarker,
		slog.String("stream_id", streamID),
		slog.String("reason", c.why),
		slog.String("position_token", truncateDryRunToken(persisted.Token, 60)))
	return false, vstreamCopyUnprovenError(streamID, c.why)
}

// vstreamCopyCompletion reads the two markers of a finished copy, the
// established-table record first (it is on the applier already open). A
// read that fails is an error, never "unproven" and never "proven".
func (s *Streamer) vstreamCopyCompletion(ctx context.Context, applier ir.ChangeApplier, streamID string) (copyCompletion, error) {
	var missing []string
	if store, ok := applier.(ir.EstablishedTablesStore); ok {
		rec, err := store.ReadEstablishedRecord(ctx, streamID)
		if err != nil {
			return copyCompletion{}, recordReadFailure(err)
		}
		// A record in progress is still a record: the scope door refuses it
		// COLD-START-INCOMPLETE next, which names the right remedy.
		if rec.Recorded {
			return copyCompletion{proven: true}, nil
		}
		missing = append(missing, "the stream has no established-table record")
	} else {
		missing = append(missing, "the target keeps no established-table record")
	}

	mstore, err := openMigrationStateStore(ctx, s.Target, s.TargetDSN, s.TargetSchema)
	if err != nil {
		return copyCompletion{}, controlReadFailure(fmt.Errorf("pipeline: %s: %w", vstreamCopyUnprovenMarker, err))
	}
	if mstore == nil {
		missing = append(missing, "the target keeps no cold-start progress record")
		return copyCompletion{why: strings.Join(missing, ", and ")}, nil
	}
	defer migcore.CloseIf(mstore)
	id := syncMigrationID(streamID)
	state, found, err := readRecordedColdStart(ctx, mstore, id)
	if err != nil {
		return copyCompletion{}, controlReadFailure(fmt.Errorf("pipeline: %s: read the cold-start progress record %q: %w",
			vstreamCopyUnprovenMarker, id, err))
	}
	switch {
	case !found:
		missing = append(missing, fmt.Sprintf("no cold-start progress record (sluice_migrate_state %q)", id))
	case state.Phase != ir.MigrationPhaseComplete:
		missing = append(missing, fmt.Sprintf("its cold-start progress record (sluice_migrate_state %q) reads phase %q, not %q",
			id, state.Phase, ir.MigrationPhaseComplete))
	default:
		return copyCompletion{proven: true}, nil
	}
	return copyCompletion{why: strings.Join(missing, ", and ")}, nil
}

// vstreamCopyUnprovenError carries [ir.ErrIncludeTableScopeDrift], as
// VSTREAM-COPY-RESUME-WITHDRAWN does: a restart reads the same control rows
// back and refuses again, so the supervisor must not retry it
// ([refusalsARestartRepeats]).
func vstreamCopyUnprovenError(streamID, why string) error {
	return fmt.Errorf("pipeline: resume refused: %w: %s: sluice cannot tell whether PlanetScale/Vitess stream %q's "+
		"cold-start copy finished (%s). A cold start stopped during its copy by sluice v0.163.1 or earlier can have saved "+
		"a position that looks exactly like a finished copy's, and resuming from it would never copy the rest of the "+
		"tables, while the stream reported healthy. A copy that finished on v0.149.0 or later leaves a record proving it; "+
		"this stream has none, so it was cold-started by a release before v0.149.0, or stopped during its copy on "+
		"v0.163.1 or earlier. No change has been applied by this run and no target table was changed. Re-copy the stream: "+
		"the same `sluice sync start` command with --restart-from-scratch, which clears the in-scope target tables before "+
		"copying them. If you know the copy finished (the stream has been streaming changes for a while, and the target's "+
		"row counts match the source's), re-run the same command once with --assert-copy-finished: it records the "+
		"stream's current tables as copied, so later resumes pass without it",
		ir.ErrIncludeTableScopeDrift, vstreamCopyUnprovenMarker, streamID, why)
}

// recordAssertedCopy records the stream's current tables as established
// after a resume the operator let through with --assert-copy-finished: the
// set a cold start of this scope would record (live-added tables
// included), with each table's engine id where the source has one, through
// the same write a cold start makes ([recordEstablishedAfterCopy]). It runs
// once the scope door has passed, so a table that door refuses is never
// recorded. A failure refuses: passing without the record would ask for the
// flag again on every resume while saying nothing, and a later resume
// without the flag would refuse anyway.
func (s *Streamer) recordAssertedCopy(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	store, ok := applier.(ir.EstablishedTablesStore)
	if !ok {
		slog.WarnContext(ctx, copyFinishedAssertedMarker+": this target keeps no established-table record, so the "+
			"acknowledgement cannot be recorded; every resume of this stream needs --assert-copy-finished",
			slog.String("stream_id", streamID))
		return nil
	}
	liveAdded, err := s.uniqueKeyDoorLiveAdded(ctx, applier, streamID)
	if err != nil {
		return controlReadFailure(fmt.Errorf("pipeline: %s: %w", copyFinishedAssertedMarker, err))
	}
	scope, err := s.readStreamScope(ctx, func(table string) bool {
		return scopeAdmits(table, s.Filter, func(t string) bool { return liveAdded[t] })
	})
	if err != nil {
		return controlReadFailure(fmt.Errorf("pipeline: %s: %w", copyFinishedAssertedMarker, err))
	}
	keys := make([]string, 0, len(scope.tables))
	names := make([]string, 0, len(scope.tables))
	for _, t := range scope.tables {
		keys = append(keys, t.key())
		names = append(names, t.display())
	}
	sort.Strings(names)
	if err := recordEstablishedAfterCopy(ctx, store, streamID, keys, s.establishedIDs(ctx, keys), s.sourceTableKeys); err != nil {
		return controlReadFailure(fmt.Errorf("pipeline: resume refused: %s: --assert-copy-finished could not record this "+
			"stream's tables as copied (no change has been applied by this run); fix what the error names and re-run the "+
			"same command: %w", copyFinishedAssertedMarker, err))
	}
	slog.WarnContext(ctx, copyFinishedAssertedMarker+": recorded every table in the stream's current scope as copied, on "+
		"the operator's --assert-copy-finished; later resumes pass without the flag",
		slog.String("stream_id", streamID), slog.String("tables", strings.Join(names, ", ")))
	return nil
}
