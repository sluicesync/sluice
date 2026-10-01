// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

// # Keepalive boundaries (GC-41 (j))
//
// The slot is acknowledged only to a position the consumer has durably
// persisted ([CDCReader.ackLSN]), and the consumer persists a position only
// at a source-transaction boundary — a TxCommit. So a stream whose decoded
// output carries no transactions never moves its slot, however much WAL
// the server writes. On Postgres 15 and later that is the idle stream's
// normal state: pgoutput skips a transaction that touches no published
// table (an empty transaction since PG 15, and on every version one in
// another database), so the walsender reads past it and decodes nothing.
// Measured on PG 15, PG 16 → PG and PG 16 → MySQL (2026-10-01): with the
// user tables idle and ~40 KB/s of WAL elsewhere on the server,
// confirmed_flush_lsn and restart_lsn stayed at the last user-table commit
// and the slot retained 5.69 MB after 135 s. The ADR-0061 source heartbeat
// did not help — its table is outside the publication, so its transactions
// are skipped too. PG 14 decodes them as empty BEGIN/COMMIT pairs, which is
// the only reason the heartbeat ever worked there.
//
// The walsender still says how far it has read: a primary keepalive carries
// ServerWALEnd, its sentPtr. While no transaction is open on this stream,
// that position is a boundary every bit as good as a commit — the pump
// emits a boundary-only transaction there (TxBegin then TxCommit, no rows)
// and the consumer persists it like any other, so the existing ceiling
// releases the slot to it. This is what PostgreSQL's own apply worker does:
// LogicalRepApplyLoop takes a keepalive's end LSN as received, and
// send_feedback reports it as flushed when nothing is pending.
//
// # Why persisting ServerWALEnd loses nothing
//
// A logical walsender outputs a transaction when it decodes the transaction's
// commit record, and advances sentPtr past a record only after processing
// it. The connection is ordered. So every transaction whose commit record
// lies below a keepalive's ServerWALEnd has already been delivered on this
// connection ahead of that keepalive — or was skipped by pgoutput (empty,
// other database, unpublished), and needs nothing applied. A resume from
// that position re-delivers every transaction whose commit record starts at
// or after it, which is everything not yet delivered. A transaction still in
// progress at the source when the keepalive was sent commits later, above
// the position, and is decoded whole from the slot's restart_lsn, which the
// server holds back for it independently of confirmed_flush_lsn.
//
// The ack itself is unchanged: still min(streamed, ceiling), the ceiling
// still raised only from the consumer's durable read-back. A keepalive
// boundary moves streamedLSN, which only ever caps the ack; the slot moves
// once the target holds the boundary. TestSlotAckInputsRoster still
// enumerates every ack input, and there is no new one.
//
// The premise "a keepalive never passes the commit of a transaction
// delivered after it" is a fact about the walsender, so it gets a runtime
// check: [keepaliveBoundary.commit] compares every delivered commit against
// the highest keepalive boundary emitted, and on a violation the pump logs
// [KeepaliveBoundaryPassedCommitMarker] at ERROR and stands no further
// keepalive in for the rest of the connection. It does not stop the stream:
// the transaction that exposed the violation is being delivered now, and a
// stop would leave the persisted position above it — a refusal there would
// cause exactly the loss the check exists to report.
//
// # Shape of the boundary
//
// Both events carry positionAt(ServerWALEnd): for a transaction with no
// rows, the pre- and post-transaction points coincide. The TxBegin opens no
// ADR-0190 identity (there is no source transaction to name, and minting one
// at an LSN a real commit could also start at would let the two collide), so
// no mark is ever written for it. CommitTime is zero: no source transaction
// committed there, and a zero time is what the sync-lag tracker and the
// --apply-delay hold both read as "no timestamp" — an idle stream stays
// UNKNOWN on sync lag (PROG-LAG-1) instead of reporting a fabricated zero,
// and the boundary is not held. It does refresh the control row's
// updated_at (sluice_seconds_since_last_apply), truthfully: the applier did
// write, and the target holds everything up to the boundary.
//
// Boundaries are throttled to one per [keepaliveBoundaryInterval] since the
// last boundary of either kind, so a busy server costs one tiny position
// write per interval and a stream whose own commits keep coming costs none.

// KeepaliveBoundaryPassedCommitMarker is the grep-stable marker of the
// ERROR the pump logs when a delivered transaction's commit lies below a
// keepalive boundary it already emitted — the walsender premise above, seen
// broken.
const KeepaliveBoundaryPassedCommitMarker = "KEEPALIVE-BOUNDARY-PASSED-COMMIT"

// keepaliveBoundaryInterval is the least time between two boundaries the
// pump emits, counting a real commit's. The slot is acknowledged on the
// [keepaliveInterval] cadence anyway, so a boundary more often than that
// would buy nothing.
const keepaliveBoundaryInterval = keepaliveInterval

// keepaliveBoundary is the pump's record of where the stream's last
// transaction boundary is, and whether one is open — what decides whether a
// keepalive may stand in for a commit. Pump-goroutine-only; one per
// connection, so a reconnect starts from its own start position.
type keepaliveBoundary struct {
	// open is true from a BeginMessage to its CommitMessage.
	open bool
	// last is the highest TxCommit position the pump has emitted, real or
	// keepalive; a new boundary must lie strictly above it.
	last pglogrepl.LSN
	// lastAt is when last was emitted.
	lastAt time.Time
	// stoodIn is the highest keepalive boundary emitted, which a later
	// commit must not lie below.
	stoodIn pglogrepl.LSN
	// disabled is set once the walsender premise is seen broken.
	disabled bool
}

// newKeepaliveBoundary starts the record at the connection's start position,
// which is a boundary the consumer already holds.
func newKeepaliveBoundary(start pglogrepl.LSN, now time.Time) *keepaliveBoundary {
	return &keepaliveBoundary{last: start, lastAt: now}
}

// begin records a BeginMessage.
func (b *keepaliveBoundary) begin() { b.open = true }

// commit records a CommitMessage whose commit record starts at commitLSN and
// whose TxCommit carries endLSN. It reports whether the commit lies below a
// keepalive boundary already emitted (the premise broken); from then on no
// keepalive is stood in.
func (b *keepaliveBoundary) commit(commitLSN, endLSN pglogrepl.LSN, now time.Time) (passed bool) {
	b.open = false
	if commitLSN < b.stoodIn {
		passed = true
		b.disabled = true
	}
	if endLSN > b.last {
		b.last = endLSN
	}
	b.lastAt = now
	return passed
}

// due reports whether a keepalive reporting walEnd may stand in for a commit:
// no transaction open (inStream covers a pgoutput streamed chunk, which
// sluice never requests), walEnd strictly past the last boundary, and the
// interval elapsed.
func (b *keepaliveBoundary) due(walEnd pglogrepl.LSN, inStream bool, now time.Time) bool {
	return !b.disabled && !b.open && !inStream &&
		walEnd > b.last && now.Sub(b.lastAt) >= keepaliveBoundaryInterval
}

// stand records a keepalive boundary emitted at walEnd.
func (b *keepaliveBoundary) stand(walEnd pglogrepl.LSN, now time.Time) {
	b.last = walEnd
	b.stoodIn = walEnd
	b.lastAt = now
}

// standKeepaliveIn emits a boundary-only transaction at walEnd, a primary
// keepalive's ServerWALEnd, when kb says one is due, and moves the pump's
// streamed LSN to it. See the file comment.
func (r *CDCReader) standKeepaliveIn(
	ctx context.Context,
	walEnd pglogrepl.LSN,
	inStream bool,
	kb *keepaliveBoundary,
	streamedLSN *pglogrepl.LSN,
	out chan<- ir.Change,
) error {
	now := time.Now()
	if !kb.due(walEnd, inStream, now) {
		return nil
	}
	// positionAt, not openTransaction: a boundary names no source
	// transaction, so it opens no ADR-0190 identity.
	pos, err := r.positionAt(walEnd)
	if err != nil {
		return err
	}
	if err := r.send(ctx, out, ir.TxBegin{Position: pos}); err != nil {
		return err
	}
	end, err := r.closeTransaction(walEnd)
	if err != nil {
		return err
	}
	if err := r.send(ctx, out, ir.TxCommit{Position: end}); err != nil {
		return err
	}
	*streamedLSN = walEnd
	kb.stand(walEnd, now)
	slog.DebugContext(
		ctx, "postgres: cdc: keepalive boundary",
		slog.String("slot", r.slotName),
		slog.String("lsn", walEnd.String()),
	)
	return nil
}

// noteCommit records a delivered commit on kb, logging
// [KeepaliveBoundaryPassedCommitMarker] if it exposes the walsender premise
// as broken.
func (r *CDCReader) noteCommit(ctx context.Context, kb *keepaliveBoundary, commitLSN, endLSN pglogrepl.LSN) {
	stoodIn := kb.stoodIn
	if !kb.commit(commitLSN, endLSN, time.Now()) {
		return
	}
	slog.ErrorContext(
		ctx, KeepaliveBoundaryPassedCommitMarker+": postgres: cdc: a transaction was delivered whose commit lies below "+
			"a keepalive boundary this stream already emitted. The boundary may already be persisted on the target and "+
			"acknowledged on the slot; this run still applies the transaction, but had the stream stopped between the "+
			"two, a resume would have skipped it. Keepalive boundaries are off for the rest of this connection. Verify "+
			"the target against the source for changes committed near the logged LSNs, and report this (GC-41 (j))",
		slog.String("slot", r.slotName),
		slog.String("commit_lsn", commitLSN.String()),
		slog.String("keepalive_boundary", stoodIn.String()),
	)
}
