// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// FoldTicket is ADR-0190 amendment D's fold: the checkpoint a mark fence
// would have written in its own synchronous transaction, handed instead to
// the lane batch that carries the fenced transaction's first marked change,
// which writes it in its OWN target transaction — the data, then the marks
// (its own upserts and the closed transactions' deletes), then this
// position, last (data before control, GC-41 (c)).
//
// Pos is the ANCHOR: the highest checkpoint boundary at or below the marked
// change's predecessor once the fence's drain made every such change
// durable — the fenced transaction's START (on a marker stream the last
// recorded boundary, a TxCommit or a GC-41 (j) keepalive boundary, which is
// an empty TxBegin/TxCommit pair the Postgres reader emits between source
// transactions; on a marker-less one the change before). So the
// position the fold persists vouches only for data that was durable before
// the fold began, and the fenced transaction's marks become durable in the
// same commit that moves the position to its start: no crash can land
// between "the marks name T" and "the position sits at T's start", which is
// amendment B's invariant made atomic.
//
// RowsApplied and ClosedTxs are exactly what [Orchestrator.writeCheckpoint]
// would have handed [LaneApplier.WriteCheckpoint] for the same boundary; the
// coordinator CLAIMS the boundary when it issues the ticket (see
// [Orchestrator.claimAnchor]), so neither is ever written twice.
type FoldTicket struct {
	// Tx is the fenced transaction (its [ir.ApplyID] TxID).
	Tx string
	// Pos is the anchor position the fold persists.
	Pos ir.Position
	// RowsApplied is the rows_applied increment the fold's position write
	// adds, as [LaneApplier.WriteCheckpoint]'s rowsApplied.
	RowsApplied int64
	// ClosedTxs are the transactions the anchor passes, whose apply marks the
	// fold deletes, as [LaneApplier.WriteCheckpoint]'s closedTxs.
	ClosedTxs []string
}

// BarrierCheckpoint is ADR-0190 amendment E's fold: the pre-apply checkpoint
// a lane barrier would have persisted in its own synchronous transaction,
// handed instead to [LaneApplier.ApplyBarrierChange], which writes it in the
// barrier's own target transaction — after the data and the marks, last.
//
// Its fields are exactly what [LaneApplier.WriteCheckpoint] would have been
// handed for the same boundary. Pos is the barrier's source transaction's
// START (the highest recorded boundary below the barrier, durable on every
// lane after the barrier's drain), never the barrier's own position: that is
// mid-transaction on a marker stream, and metadata-anchored (0/0) for a
// SchemaSnapshot (Bug 158). RowsApplied is the route-time DML count up to Pos
// — the barrier's own row is counted by a LATER boundary, never by this one.
// ClosedTxs are the transactions Pos passes, whose apply marks the barrier's
// transaction deletes; the barrier's own transaction is never among them.
type BarrierCheckpoint struct {
	Pos         ir.Position
	RowsApplied int64
	ClosedTxs   []string
}

// BarrierFoldNotTransactionalMarker is the grep-stable token of the refusal
// an engine raises when [LaneApplier.ApplyBarrierChange] is handed a
// [BarrierCheckpoint] for a change kind its FoldsBarrierCheckpoint declines
// (a MySQL-family Truncate: the server commits implicitly around the DDL, so
// a position written after it would autocommit on its own, apart from the
// marks it must travel with). The coordinator consults FoldsBarrierCheckpoint
// before it builds one, so this is a coordinator-defect tripwire: the run
// fails loudly rather than writing a position outside a transaction.
const BarrierFoldNotTransactionalMarker = "BARRIER-FOLD-NOT-TRANSACTIONAL"

// BarrierFoldNotTransactional is the [BarrierFoldNotTransactionalMarker]
// refusal for c, raised by an engine before it writes anything.
func BarrierFoldNotTransactional(engine string, c ir.Change) error {
	return fmt.Errorf("laneapply: %s: %s was handed a barrier checkpoint to fold for a %T, which it applies outside "+
		"one transaction — the coordinator must write that checkpoint itself (ADR-0190 amendment E)",
		BarrierFoldNotTransactionalMarker, engine, c)
}

// FoldTicketDuplicateMarker is the grep-stable token of the refusal a lane
// raises when one batch carries two fold tickets. The coordinator issues
// ticket T+1 only after a drain that required ticket T's batch to commit, so
// two tickets in one batch is a coordinator defect; the run fails loudly
// rather than folding one position and dropping the other.
const FoldTicketDuplicateMarker = "FOLD-TICKET-DUPLICATE"

// foldOf returns the one fold ticket buf carries (nil when none), or a
// [FoldTicketDuplicateMarker] error when it carries more than one.
func foldOf(buf []LaneChange) (*FoldTicket, error) {
	var fold *FoldTicket
	for _, e := range buf {
		if e.fold == nil {
			continue
		}
		if fold != nil {
			return nil, fmt.Errorf("laneapply: %s: one lane batch carries the fold tickets of %q and %q — the coordinator "+
				"must not issue a second ticket before the first one's batch commits (ADR-0190 amendment D)",
				FoldTicketDuplicateMarker, fold.Tx, e.fold.Tx)
		}
		fold = e.fold
	}
	return fold, nil
}

// commitOutcomeUnknownError marks a lane error raised by the COMMIT step of
// a fold batch (see [CommitOutcomeUnknown]).
type commitOutcomeUnknownError struct{ err error }

func (e *commitOutcomeUnknownError) Error() string {
	return "commit outcome unknown: " + e.err.Error()
}

func (e *commitOutcomeUnknownError) Unwrap() error { return e.err }

// CommitOutcomeUnknown wraps err, raised by the COMMIT of a lane batch that
// carried a [FoldTicket], so the orchestrator does not retry the batch in
// place. Such an error — the Bug-56 commit watchdog firing, the connection
// dying mid-COMMIT, a deferred constraint — leaves the commit's outcome
// unknown to the lane: had it actually committed, an in-place retry would
// add the ticket's RowsApplied to rows_applied a second time, and would
// decide from an apply-mark tracker whose bookkeeping never recorded the
// durable plan. So the run fails instead, and the streamer's ADR-0038
// re-entry re-reads the persisted position and the marks — the truth, once
// the outcome has settled. A COMMIT the watchdog abandoned can still land
// after that read; every commit path shares that, and ADR-0190 §D.2 records
// what is known of it (a review built only loud outcomes; unverified beyond
// that). Errors raised BEFORE the COMMIT (a statement's serialization abort,
// deadlock, tx-killer) leave nothing durable and keep the ordinary in-lane
// retry. nil stays nil.
func CommitOutcomeUnknown(err error) error {
	if err == nil {
		return nil
	}
	return &commitOutcomeUnknownError{err: err}
}

// IsCommitOutcomeUnknown reports whether err was raised by a fold batch's
// COMMIT ([CommitOutcomeUnknown]).
func IsCommitOutcomeUnknown(err error) bool {
	var e *commitOutcomeUnknownError
	return errors.As(err, &e)
}

// foldCommitUnknownFatal is the run error for a fold batch whose COMMIT
// outcome is unknown: the engine's classification of the cause (so the
// streamer's retry loop still sees a transient as transient and re-enters),
// named for what the lane could not tell.
func (o *Orchestrator) foldCommitUnknownFatal(fold *FoldTicket, rawErr error) error {
	var e *commitOutcomeUnknownError
	cause := rawErr
	if errors.As(rawErr, &e) {
		cause = e.err
	}
	return fmt.Errorf("laneapply: the COMMIT of the lane batch folding transaction %q's checkpoint failed with an unknown "+
		"outcome; not retrying it in place (an in-place retry of a commit that landed would count its rows twice) — the "+
		"run stops and re-reads the persisted position and apply marks (ADR-0190 amendment D): %w",
		fold.Tx, o.la.ClassifyError(cause))
}

// claimAnchor is amendment D's step 5: with every change before the fenced
// transaction's first marked change durable (the fence's drain), read the
// anchor and, when it is not yet persisted, CLAIM it for a fold — compute the
// ticket exactly as [Orchestrator.writeCheckpoint] would compute its write,
// and advance the persisted-checkpoint bookkeeping as a successful write
// does. It returns nil when the transaction is already anchored:
//
//   - no boundary was recorded in this run (5a): the fenced transaction is the
//     first the run delivers, so the run's start position — already persisted
//     — is its start;
//   - the anchor is at or below the last persisted boundary (5b): a barrier of
//     the same transaction, or an idle or count checkpoint, already wrote it.
//
// Why the claim is safe before the fold commits: until it does, the frontier
// cannot pass the marked change (its seq is the fold's), every boundary
// above the anchor lies above that seq, and every boundary at or below the
// anchor is refused by writeCheckpoint's seq guard — so every coordinator
// checkpoint in between (the count cadence, the idle tick) returns in memory
// without touching the engine. Persisted positions therefore stay totally
// ordered by seq across the two writers (the coordinator's checkpoint and
// the lane's fold). If the fold never commits, the lane's error ends the run
// and the next run rebuilds all of this from the durable position.
func (o *Orchestrator) claimAnchor() *FoldTicket {
	ck, ok := o.nextCheckpoint()
	if !ok {
		return nil
	}
	o.checkpointWritten(ck)
	return &FoldTicket{Pos: ck.pos, RowsApplied: ck.rowsApplied, ClosedTxs: ck.closedTxs}
}

// foldAnchored reports whether the fenced transaction is anchored on the
// coordinator's side: it needed no fold, or the fold's batch committed (the
// frontier passed the fold's seq, which advances only after that commit).
func (o *Orchestrator) foldAnchored() bool {
	return o.foldSeq == 0 || o.frontier.FrontierSeq() >= o.foldSeq
}
