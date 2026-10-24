// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Stream liveness (DRAINED-ADD-TABLE-RUNNING-STREAM-UNCHECKED): what a
// `sync start` records on its control row so a drained `schema add-table` can
// tell a stream that stopped from one that may be running
// ([ir.StreamLivenessStore]).
//
// The rule the add applies is deliberately blunt: it passes only on a
// recorded stop (or on no row at all). Three writers keep the record:
//
//   - Every position write an applying stream commits stamps it running, in
//     the same statement as the position (the engines' write-position
//     builders). A stream that advances its position therefore cannot read
//     as stopped, whatever happens to the other writes.
//   - Every attempt stamps it running as it starts, through the attempt's
//     own applier ([stampAttemptStart]), before it opens its change stream:
//     a previous run's stopped marker never survives a running attempt, and
//     — with the add writing its pending entry before its own second read —
//     this closes the race between an add and a stream starting under it:
//     the add records its pending entry and then reads the record
//     ([AddTable.recheckStreamStoppedAfterPending]), the stream stamps and
//     then reads the pending entries ([Streamer.judgePendingAdds]), and
//     whichever goes second sees the other.
//   - The stream records the stop when it ends: a graceful drain records it
//     before it clears the stop request ([Streamer.recordDrainedStop]), and
//     every Run that reached the target records it on its way out, through
//     an applier opened for the purpose ([Streamer.stopStreamLiveness]) —
//     Ctrl-C, a refusal and a failure included. The one exit that records
//     nothing is a `sync stop` whose drain timed out and was hard-cancelled
//     ([Streamer.settleHardCancelledStop], STOP-DRAIN-HARD-CANCELLED): the
//     cut-off apply's last commit is in doubt, so the record keeps saying
//     running and the add refuses until a later start and stop.
//
// A stream that crashed or was killed records no stop, and nothing tells its
// record from a running stream's: a running stream that is idle writes no
// positions, so its stamp ages exactly like a dead one's. The add refuses
// both, and the remedy for the crashed one is to start the stream once and
// stop it (`sync stop --wait`), which records the stop — the start's own exit
// records it even when a later check refuses that start.
//
// The add makes one more check after its copy, before it hands the table to
// the stream ([AddTable.refuseStreamRanDuringAdd]): the record must still say
// stopped and the stream's position must not have moved. A stream that ran
// during the add — even one that applied and then stopped cleanly — moved
// its position, and the add refuses.
//
// A stamp that fails never fails the stream — liveness is a fact for other
// commands, not a precondition of streaming. It WARNs
// ([StreamLivenessUnrecordedMarker]); a drained add-table then reads no stop
// and refuses.

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

// runLiveness is one Run's liveness record-keeping: armed by the first
// attempt that reaches the target ([Streamer.armStreamLiveness]) and
// discharged once, by a graceful drain or by Run on its way out. Touched only
// from Run's goroutine (attempts run there, one at a time).
type runLiveness struct {
	ctx      context.Context // Run's
	streamID string
	armed    bool
	stopped  bool
	// forgone: this Run must not record a stop at all — its stop's drain
	// was hard-cancelled ([Streamer.settleHardCancelledStop]).
	forgone bool
}

// forgoStreamStopRecord makes this Run's exit record no stop: the record
// keeps the running stamp the last attempt left, which a drained add-table
// refuses. Touched only from Run's goroutine, like the rest of runLiveness.
func (s *Streamer) forgoStreamStopRecord() {
	if s.liveness != nil {
		s.liveness.forgone = true
	}
}

// armStreamLiveness records that this Run reached the target, so its exit
// records the stop. Skipped on a dry run, which writes nothing, and outside
// Run.
func (s *Streamer) armStreamLiveness(streamID string) {
	r := s.liveness
	if r == nil || r.armed || s.DryRun {
		return
	}
	r.armed, r.streamID = true, streamID
}

// stop records the stop through an applier opened for it, once per Run; a
// drained stop already recorded it ([Streamer.recordDrainedStop]). It runs on
// a context detached from cancellation — a Ctrl-C is the commonest way a Run
// ends.
func (s *Streamer) stopStreamLiveness() {
	r := s.liveness
	if r == nil || !r.armed || r.stopped || r.forgone {
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
	if store != nil {
		recordStreamStopped(ctx, store, r.streamID)
	}
}

// stampAttemptStart stamps the stream running through the attempt's own
// applier, at the start of every attempt (see the file comment). It may add
// the column to an older control table; a failure WARNs and the attempt goes
// on.
func stampAttemptStart(ctx context.Context, applier ir.ChangeApplier, streamID string) {
	store, ok := applier.(ir.StreamLivenessStore)
	if !ok {
		return
	}
	stampCtx, cancel := context.WithTimeout(ctx, streamLivenessStampTimeout)
	defer cancel()
	if _, err := store.StampStreamAlive(stampCtx, streamID); err != nil && ctx.Err() == nil {
		slog.WarnContext(ctx, StreamLivenessUnrecordedMarker+": this attempt could not record that the stream is running; "+
			"it keeps streaming, and `sluice schema add-table` without --no-drain refuses against it (no stop is recorded "+
			"either way) until the record can be written",
			slog.String("stream_id", streamID), slog.String("error", err.Error()))
	}
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
// stream, from its liveness record. It passes only on a recorded stop or on no
// control row; it refuses everything else — a running stamp of any age (a
// running stream, idle or not, and a crashed one look the same), a row no
// release that records liveness has stamped, and an unreadable record.
func judgeStreamStopped(streamID string, l ir.StreamLiveness, readErr error) error {
	stopIt := fmt.Sprintf("`sluice sync stop --wait --stream-id %s` (same --target-driver/--target)", streamID)
	restartIt := fmt.Sprintf("start it once (`sluice sync start` with the same --stream-id, or the `sluice sync run` "+
		"fleet whose leg it is) and stop it with %s; that records the stop, and a start that a later check refuses "+
		"records it too, as it exits", stopIt)
	why := "A drained add must not run while the stream applies changes: it copies the table and records it after the " +
		"copy, and a running stream drops every change to the table until then, so the target would keep rows the " +
		"source deleted or changed during the add"
	switch {
	case readErr != nil:
		return fmt.Errorf("pipeline: add-table: %w: could not read whether stream %q stopped (%w). An unreadable "+
			"record is not evidence that it stopped; once the target answers, make sure it is stopped with %s, then run "+
			"the add again. %s", errStreamNotStopped, streamID, readErr, stopIt, why)
	case !l.Found, l.Stopped:
		return nil
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
