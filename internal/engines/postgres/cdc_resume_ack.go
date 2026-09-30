// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

// GC-41 MEDIUM-1: a warm resume must not start behind the slot's
// confirmed_flush_lsn.
//
// The walsender begins decoding at max(requested LSN, confirmed_flush_lsn)
// and skips every transaction that committed before that point. So when a
// slot has been acknowledged PAST the position the target persisted, a
// resume from that position silently skips the gap — the target never
// receives those changes and nothing reports it. Three ways to get there:
//
//   - a stop on sluice v0.156.6 or earlier of a Postgres → MySQL-family sync
//     whose reader acked the streamed LSN over changes still in an apply
//     batch (GC-41 (a)); the FIRST resume on a fixed binary would still
//     have lost them;
//   - the target restored from a backup, or failed over to a replica, that
//     holds an older control row than the slot has been acked to;
//   - the slot dropped and recreated after the stream's row was written
//     (a `--restart-from-scratch` whose copy was interrupted leaves the old
//     row next to a slot created at "now").
//
// The independent evidence is the source server's own
// pg_replication_slots.confirmed_flush_lsn, compared with the position the
// target persisted — neither is derived from the other in this process.
//
// # Why a stream this binary wrote never trips it
//
// The door is sound only if a correctly-running sluice never leaves
// confirmed_flush_lsn past the persisted position. The ack side: the slot is
// released only to positions read back from the target, through a ratchet
// (streamer_slot_ack.go), so confirmed <= the highest position ever
// persisted. The position side needs the persisted position never to move
// BACKWARD, and the reader does stamp two positions below an end the stream
// may already have persisted and released: a SchemaSnapshot carries its
// RelationMessage's WAL start, which pgoutput sends as 0/0 (Bug 158's
// shape), and a pgoutput v2 in-progress chunk carries the previous
// transaction's commit LSN. Neither is ever PERSISTED, because both are
// mid-transaction positions and every apply path writes the position only at
// a source-transaction boundary for a source that brackets its transactions,
// which this reader always does (TxBegin/TxCommit):
//
//   - serial batch: BatchConfig.CheckpointOnlyAtTxBoundary (change_applier_batch.go);
//   - per-change: positions deferred to the TxCommit (persistSourceTxCommit);
//   - concurrent lanes: the frontier checkpoint at a durable boundary, and
//     the barrier (applyBarrierNoPosition) writes none.
//
// In-progress chunks are not requested at all (START_REPLICATION passes no
// `streaming` option). So the comparison is between two transaction-boundary
// values, and has no legitimate false positive. Pinned on a real server, with
// a DDL transaction whose Relation anchor sits below the slot and a stop
// inside it, on all three paths, by
// TestStreamer_PostgresToPostgres_WarmResumeRefusesASlotAckedPastTheTarget.
//
// Chosen over flooring the stamped positions (which does make them monotone
// and was mutation-proven to absorb a persisted SchemaSnapshot anchor) because
// the SchemaSnapshot's position IS its ADR-0049 schema-history anchor:
// raising PG anchors off 0/0 changes history resolution (a resume below a
// table's only anchor becomes ErrPositionInvalid) and the incremental-backup
// EndPosition/anchor invariant, which is not a change to make in a
// slot-ack fix. Chosen over persisting a high-water column because that is a
// control-table change on every target engine for a property the apply
// paths already hold. If a future path persists a mid-transaction position
// (Bug 158 did), this door refuses LOUDLY — with the acknowledgement below
// as the escape — rather than losing anything.

// SlotAckedPastTargetPositionMarker is the grep-stable marker of the
// warm-resume refusal. The refusal wraps [ir.ErrSlotAckedPastTargetPosition],
// whose text is this marker, so the fleet supervisor can see that a restart
// would refuse again.
const SlotAckedPastTargetPositionMarker = "SLOT-ACKED-PAST-TARGET-POSITION"

// AcceptSlotAckedPastPosition records the operator's one-shot
// acknowledgement of a SLOT-ACKED-PAST-TARGET-POSITION refusal, given as the
// exact confirmed_flush_lsn the refusal printed. It is bound to that value:
// a slot that has since moved again refuses anew, because the operator
// verified the target against a gap that is no longer the one in front of
// the stream. After the next durable position write the persisted position
// passes the slot and the acknowledgement is no longer consulted. Must be
// called before StreamChanges.
//
// Why an override exists at all: the only other remedy is a full re-copy,
// and there are cases where the operator can know the gap is empty — every
// change in it was to a table outside this stream's filter, or the target
// was verified against the source — and one legitimate upgrade shape the
// check cannot tell from loss: a stream last written by an older release
// that persisted a mid-transaction position (a SchemaSnapshot anchor, which
// pgoutput stamps 0/0 — Bug 158's shape) below its acknowledged slot.
func (r *CDCReader) AcceptSlotAckedPastPosition(confirmedFlush string) {
	r.ackedPastAccepted = confirmedFlush
}

// checkSlotNotAckedPast refuses a warm resume whose slot has been
// acknowledged past resume, the persisted position's LSN. confirmedFlush is
// the slot's confirmed_flush_lsn as text; "" (the server reports none)
// proves nothing either way and passes.
func (r *CDCReader) checkSlotNotAckedPast(ctx context.Context, confirmedFlush string, resume pglogrepl.LSN) error {
	if confirmedFlush == "" {
		return nil
	}
	flush, err := pglogrepl.ParseLSN(confirmedFlush)
	if err != nil {
		return fmt.Errorf("postgres: parse slot %q confirmed_flush_lsn %q: %w", r.slotName, confirmedFlush, err)
	}
	if flush <= resume {
		return nil
	}
	if accepted, perr := pglogrepl.ParseLSN(r.ackedPastAccepted); perr == nil && accepted == flush {
		slog.WarnContext(
			ctx, "postgres: cdc: "+SlotAckedPastTargetPositionMarker+" acknowledged by the operator; resuming — every change "+
				"committed between the two positions is skipped and was verified absent or already held",
			slog.String("slot", r.slotName),
			slog.String("persisted_position", resume.String()),
			slog.String("confirmed_flush_lsn", flush.String()),
		)
		return nil
	}
	return &terminalPGError{err: fmt.Errorf(
		"postgres: %w: replication slot %q has confirmed_flush_lsn %s, past the position this stream resumes from (%s, "+
			"read from the target's sluice_cdc_state). PostgreSQL starts decoding at the later of the two, so every change "+
			"committed between %s and %s would be skipped, and the target does not hold them. Causes: a stop of a Postgres → "+
			"MySQL-family sync on sluice v0.156.6 or earlier (the slot was acknowledged past changes still in an apply "+
			"batch); a target restored or failed over to an older state; or the slot dropped and recreated after this "+
			"stream's position was written (e.g. an interrupted --restart-from-scratch). Remedy: re-copy with "+
			"`sync start --restart-from-scratch` (or re-copy the affected tables). If you have verified the target already "+
			"holds every change up to %s, start once with --accept-slot-acked-past-position=%s",
		ir.ErrSlotAckedPastTargetPosition, r.slotName, flush, resume, resume, flush, flush, flush,
	)}
}
