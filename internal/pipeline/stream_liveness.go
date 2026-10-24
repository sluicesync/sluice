// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Stream liveness (DRAINED-ADD-TABLE-RUNNING-STREAM-UNCHECKED): what a
// `sync start` records on its control row so a drained `schema add-table` can
// tell a stream that stopped from one that may be running
// ([ir.StreamLivenessStore]).
//
// The rule the add applies is deliberately blunt: it passes only on a
// recorded stop. The guard is one sentence, and every writer below exists to
// keep it true: a recorded stop means the stream stopped. Three writers keep
// the record:
//
//   - Every position write an applying stream commits stamps it running, in
//     the same statement as the position (the engines' write-position
//     builders). A stream that advances its position therefore cannot read
//     as stopped, whatever happens to the other writes.
//   - Every attempt stamps it running as it starts, through the attempt's
//     own applier ([Streamer.stampAttemptStart]), before it opens its change
//     stream; a stamp that fails over a recorded stop fails the attempt:
//     a previous run's stopped marker never survives a running attempt, and
//     — with the add writing its pending entry before its own second read —
//     this closes the race between an add and a stream starting under it:
//     the add records its pending entry and then reads the record
//     ([AddTable.recheckStreamStoppedAfterPending]), the stream stamps and
//     then reads the pending entries ([Streamer.judgePendingAdds]), and
//     whichever goes second sees the other.
//   - The stream records the stop when it ends, and only a Run that owns the
//     record does ([runLiveness]): a graceful drain records it before it
//     clears the stop request ([Streamer.recordDrainedStop]); a Run whose
//     attempt reached its apply phase records it on its way out, through an
//     applier opened for the purpose ([Streamer.stopStreamLiveness]) —
//     Ctrl-C and a failure included; and a Run that never streamed only
//     undoes its own stamp, by compare-and-set, when that stamp replaced a
//     stop. A start that is refused or fails before streaming, while another
//     process runs the stream, writes nothing. The one streaming exit that
//     records nothing is a `sync stop` that did not drain
//     ([Streamer.settleUndrainedStop], STOP-DRAIN-HARD-CANCELLED): the
//     cut-off apply's last commit is in doubt, so the record keeps saying
//     running and the add refuses until a later start and stop.
//
// A stream that crashed or was killed records no stop, and nothing tells its
// record from a running stream's: a running stream that is idle writes no
// positions, so its stamp ages exactly like a dead one's. The add refuses
// both, and the remedy for the crashed one is to start the stream once, let
// it reach streaming, and stop it (`sync stop --wait`), which records the
// stop. A start refused before it streams does not record it.
//
// The add makes one more check after its copy, before it hands the table to
// the stream ([AddTable.refuseStreamRanDuringAdd]): the record must still say
// stopped and the stream's position must not have moved. A stream that ran
// during the add — even one that applied and then stopped cleanly — moved
// its position, and the add refuses.
//
// A stamp that fails fails the attempt only over a recorded stop (or an
// unreadable record); otherwise liveness is a fact for other commands, not a
// precondition of streaming, and it WARNs ([StreamLivenessUnrecordedMarker]);
// a drained add-table then reads no stop and refuses.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// StreamLivenessUnrecordedMarker is the grep-stable marker of the WARN a
// stream logs when it cannot record its liveness.
const StreamLivenessUnrecordedMarker = "STREAM-LIVENESS-UNRECORDED"

// streamLivenessStampTimeout bounds every liveness write: an attempt's start
// stamp and a stopping Run's stop stamp. Neither may hang the stream on a
// target that has gone away.
const streamLivenessStampTimeout = 10 * time.Second

// runLiveness is one Run's liveness record-keeping, discharged once, by a
// graceful drain or by Run on its way out. Touched only from Run's goroutine
// (attempts run there, one at a time).
//
// Ownership (the fix-aimed review of c421369a, HIGH): a Run records a stop
// only for a stream it actually ran. A second `sync start` of a stream id
// that another process is running — one refused at a door, one that cannot
// reach its source, one a fleet spec duplicates — must never write the
// stopped marker over the running stream's record, or a drained add-table
// would pass against it. So:
//
//   - armed: an attempt of this Run reached its apply phase, with its change
//     stream open ([Streamer.armStreamLiveness]). Only then does the exit
//     record the stop unconditionally. (Two Runs that both reach apply for
//     one stream id is SYNC-START-CONCURRENT-RUN-UNCHECKED, filed.)
//   - restorable: never armed, but every running stamp this Run made
//     replaced the one before it, and the first replaced a stopped marker —
//     so the row held a stop until this Run's own stamp. Its exit then puts
//     the marker back by compare-and-set on its last stamp's exact value
//     ([ir.StreamLivenessStore.RestoreStreamStopped]): any stamp or
//     position write another run made since leaves the row alone. This is
//     what keeps a start that defers or is refused while a drained add
//     copies from leaving a running stamp behind.
//   - otherwise (a refused start over a running or crashed stream's stamp)
//     the exit writes nothing, and the record keeps saying running.
type runLiveness struct {
	ctx        context.Context // Run's
	streamID   string
	armed      bool
	stopped    bool
	lastStamp  ir.StreamStamp
	restorable bool
	// forgone: this Run must not record a stop at all — its stop did not
	// drain ([Streamer.settleUndrainedStop]).
	forgone bool
}

// noteAttemptStamp folds one attempt's running stamp into the Run's
// ownership (see [runLiveness]).
func (r *runLiveness) noteAttemptStamp(streamID string, stamp ir.StreamStamp) {
	if r == nil || !stamp.Stamped {
		return
	}
	if !r.lastStamp.Stamped {
		r.restorable = stamp.ReplacedStop()
	} else {
		r.restorable = r.restorable && stamp.Replaced(r.lastStamp)
	}
	r.streamID, r.lastStamp = streamID, stamp
}

// forgoStreamStopRecord makes this Run's exit record no stop: the record
// keeps the running stamp the last attempt left, which a drained add-table
// refuses. Touched only from Run's goroutine, like the rest of runLiveness.
func (s *Streamer) forgoStreamStopRecord() {
	if s.liveness != nil {
		s.liveness.forgone = true
	}
}

// armStreamLiveness records that this Run's attempt reached its apply phase,
// change stream open, so its exit records the stop. Skipped on a dry run,
// which writes nothing, and outside Run.
func (s *Streamer) armStreamLiveness(streamID string) {
	r := s.liveness
	if r == nil || r.armed || s.DryRun {
		return
	}
	r.armed, r.streamID = true, streamID
}

// stopStreamLiveness records the stop through an applier opened for it, once
// per Run, if this Run owns the record ([runLiveness]); a drained stop
// already recorded it ([Streamer.recordDrainedStop]). It runs on a context
// detached from cancellation — a Ctrl-C is the commonest way a Run ends.
func (s *Streamer) stopStreamLiveness() {
	r := s.liveness
	if r == nil || r.stopped || r.forgone || (!r.armed && !r.restorable) {
		return
	}
	r.stopped = true
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.ctx), streamLivenessStampTimeout)
	defer cancel()
	open := s.livenessOpen
	if open == nil {
		open = s.openLivenessStore
	}
	store, release, err := open(ctx)
	if err != nil {
		slog.WarnContext(ctx, StreamLivenessUnrecordedMarker+": this stream could not record that it stopped; `sluice "+
			"schema add-table` without --no-drain refuses against it until a later start and stop records it",
			slog.String("stream_id", r.streamID), slog.String("error", err.Error()))
		return
	}
	defer release()
	if store == nil {
		return
	}
	if r.armed {
		recordStreamStopped(ctx, store, r.streamID)
		return
	}
	if _, err := store.RestoreStreamStopped(ctx, r.streamID, r.lastStamp); err != nil {
		slog.WarnContext(ctx, StreamLivenessUnrecordedMarker+": this start stamped the stream running over its recorded "+
			"stop and could not put the stop back; `sluice schema add-table` without --no-drain refuses against it until a "+
			"later start and stop records it",
			slog.String("stream_id", r.streamID), slog.String("error", err.Error()))
	}
}

// stampAttemptStart stamps the stream running through the attempt's own
// applier, at the start of every attempt (see the file comment), and folds
// the stamp into the Run's ownership. It may add the column to an older
// control table.
//
// A stamp that fails is fail-closed over a recorded stop (the fix-aimed
// review of c421369a, MEDIUM): when the record still reads the stopped
// marker, or cannot be read at all, the attempt fails rather than streaming
// under a stop it never overwrote — its position writes could then advance
// with the record still saying stopped. Otherwise (the record already says
// running, or the column is absent and a role cannot add it) it WARNs and the
// attempt goes on, and a drained add-table refuses against the stream.
func (s *Streamer) stampAttemptStart(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	store, ok := applier.(ir.StreamLivenessStore)
	if !ok {
		return nil
	}
	stampCtx, cancel := context.WithTimeout(ctx, streamLivenessStampTimeout)
	defer cancel()
	stamp, err := store.StampStreamAlive(stampCtx, streamID)
	if err == nil {
		s.liveness.noteAttemptStamp(streamID, stamp)
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	readCtx, cancelRead := context.WithTimeout(ctx, streamLivenessStampTimeout)
	defer cancelRead()
	l, rerr := store.ReadStreamLiveness(readCtx, streamID)
	switch {
	case rerr != nil:
		return fmt.Errorf("pipeline: %s: could not record that stream %q is running (%w), nor read its record (%v); "+
			"not streaming under a record that may still say stopped", StreamLivenessUnrecordedMarker, streamID, err, rerr)
	case l.Stopped:
		return fmt.Errorf("pipeline: %s: could not record that stream %q is running (%w) over its recorded stop; not "+
			"streaming under a record that says stopped, or a drained `sluice schema add-table` could pass against "+
			"this running stream", StreamLivenessUnrecordedMarker, streamID, err)
	}
	slog.WarnContext(ctx, StreamLivenessUnrecordedMarker+": this attempt could not record that the stream is running; "+
		"it keeps streaming, and `sluice schema add-table` without --no-drain refuses against it (no stop is recorded "+
		"either way) until the record can be written",
		slog.String("stream_id", streamID), slog.String("error", err.Error()))
	return nil
}

// recordDrainedStop records the stop for a graceful drain, through the
// attempt's applier, before its stop request clears (see the call site): `sync
// stop --wait` returns on the clear, and an add run right after it must find
// the stop. Run's exit then records nothing more.
func (s *Streamer) recordDrainedStop(ctx context.Context, applier ir.ChangeApplier, streamID string) {
	store, ok := applier.(ir.StreamLivenessStore)
	if !ok {
		s.stopStreamLiveness()
		return
	}
	stopCtx, cancel := context.WithTimeout(ctx, streamLivenessStampTimeout)
	defer cancel()
	if recordStreamStopped(stopCtx, store, streamID) && s.liveness != nil {
		s.liveness.stopped = true
	}
}

// openLivenessStore opens the applier a stopping Run records its stop
// through: a pre-supplied applier when it has the surface (tests; the caller
// owns it), otherwise a dedicated applier opened for the purpose (one
// connection, briefly, outside the apply-concurrency budget). A nil store
// with a nil error means there is none (the applier lacks the surface, or a
// unit test drives Run without a target).
func (s *Streamer) openLivenessStore(ctx context.Context) (store ir.StreamLivenessStore, release func(), err error) {
	if s.Applier != nil {
		store, _ = s.Applier.(ir.StreamLivenessStore)
		return store, func() {}, nil
	}
	if s.Target == nil {
		return nil, func() {}, nil
	}
	applier, err := s.Target.OpenChangeApplier(ctx, s.TargetDSN)
	if err != nil {
		return nil, func() {}, fmt.Errorf("open the target to record the stream's stop: %w", err)
	}
	store, ok := applier.(ir.StreamLivenessStore)
	if !ok {
		migcore.CloseIf(applier)
		return nil, func() {}, nil
	}
	return store, func() { migcore.CloseIf(applier) }, nil
}

// recordStreamStopped stamps the stopped marker through store, reporting
// whether it landed and WARNing when it did not: a drained add-table then
// refuses until a later start and stop records it.
func recordStreamStopped(ctx context.Context, store ir.StreamLivenessStore, streamID string) bool {
	if err := store.RecordStreamStopped(ctx, streamID); err != nil {
		slog.WarnContext(ctx, StreamLivenessUnrecordedMarker+": this stream could not record that it stopped; `sluice "+
			"schema add-table` without --no-drain refuses against it until a later start and stop records it",
			slog.String("stream_id", streamID), slog.String("error", err.Error()))
		return false
	}
	return true
}

// DrainedAddTableStreamNotStoppedMarker is the grep-stable marker of a drained
// add-table's refusal of a stream whose record does not show a stop.
const DrainedAddTableStreamNotStoppedMarker = "DRAINED-ADD-TABLE-STREAM-NOT-STOPPED"

// errStreamNotStopped is the cause every drained-add liveness refusal wraps.
var errStreamNotStopped = errors.New(DrainedAddTableStreamNotStoppedMarker)

// judgeStreamStopped decides whether a drained add-table may run against the
// stream, from its liveness record. It passes only on a recorded stop; it
// refuses everything else — a running stamp of any age (a running stream,
// idle or not, and a crashed one look the same), a row no release that
// records liveness has stamped, an unreadable record, and a missing control
// row (every caller has already seen the stream's row, so a row that is gone
// now is not a stop).
func judgeStreamStopped(streamID string, l ir.StreamLiveness, readErr error) error {
	stopIt := fmt.Sprintf("`sluice sync stop --wait --stream-id %s` (same --target-driver/--target)", streamID)
	restartIt := fmt.Sprintf("start it once (`sluice sync start` with the same --stream-id, or the `sluice sync run` "+
		"fleet whose leg it is), let it reach streaming, and stop it with %s; that records the stop (a start "+
		"that is refused or fails before it streams does not)", stopIt)
	why := "A drained add must not run while the stream applies changes: it copies the table and records it after the " +
		"copy, and a running stream drops every change to the table until then, so the target would keep rows the " +
		"source deleted or changed during the add"
	switch {
	case readErr != nil:
		return fmt.Errorf("pipeline: add-table: %w: could not read whether stream %q stopped (%w). An unreadable "+
			"record is not evidence that it stopped; once the target answers, make sure it is stopped with %s, then run "+
			"the add again. %s", errStreamNotStopped, streamID, readErr, stopIt, why)
	case l.Stopped:
		return nil
	case !l.Found:
		return fmt.Errorf("pipeline: add-table: %w: stream %q's control row is gone, so nothing shows that it stopped "+
			"(it existed when the add began); run the add again once the stream's row is back, with the stream stopped by "+
			"%s. %s", errStreamNotStopped, streamID, stopIt, why)
	case !l.Recorded:
		return fmt.Errorf("pipeline: add-table: %w: stream %q has no liveness record on its control row — it last ran "+
			"under a release that does not record one, or it could not write it (%s) — so nothing shows that it "+
			"stopped. If it is running, stop it with %s; if not, %s. Then run the add again. %s",
			errStreamNotStopped, streamID, StreamLivenessUnrecordedMarker, stopIt, restartIt, why)
	}
	return fmt.Errorf("pipeline: add-table: %w: stream %q has not recorded that it stopped: it last recorded that it "+
		"was running %s ago (by the target's clock). Either it is running — an idle stream records nothing new — or "+
		"it stopped without recording it (a crash, a kill). If it is running, stop it with %s; if it is not, %s "+
		"(`sync stop` alone against a stream that is not running only leaves its stop request set until the next "+
		"start). Then run the add again. %s",
		errStreamNotStopped, streamID, l.Age().Round(time.Second), stopIt, restartIt, why)
}
