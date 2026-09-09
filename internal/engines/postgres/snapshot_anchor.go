// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The source half of the stopped-cold-start handoff resume
// (A0909-STOP-1): proving that this server still stands EXACTLY where a
// previous run's snapshot anchor says it does.
//
// # The measured premise this rests on
//
// A logical replication slot created with EXPORT_SNAPSHOT reports a
// consistent_point, and until something CONSUMES the slot its
// pg_replication_slots.confirmed_flush_lsn is that same value.
// Measured on real postgres:16 (2026-09-09):
// CREATE_REPLICATION_SLOT … LOGICAL pgoutput returned consistent_point
// 0/1946620; confirmed_flush_lsn for that slot read 0/1946620 —
// identical — with restart_lsn earlier at 0/19465E8. Two further
// committed rows moved NEITHER value, and peeking the slot then
// returned exactly those two post-slot rows and none of the three
// pre-slot ones. So an unconsumed slot's confirmed_flush_lsn IS the
// snapshot's consistent point, and resuming CDC from it is both
// lossless (nothing after the snapshot has been discarded) and
// duplicate-free (nothing before it is re-delivered).
//
// That is a fact about PostgreSQL, not about sluice, so it gets a
// runtime check rather than a comment: the equality itself IS the gate
// below, and TestSnapshotAnchor_UnconsumedSlotHoldsTheConsistentPoint
// re-measures the premise against a real server on every integration
// run.
//
// # What this file must never do
//
// Create, drop, advance or consume anything. It reads catalog state and
// answers a question; every mutation stays with the callers that own
// the slot's lifecycle.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// VerifySnapshotAnchor implements [ir.SnapshotAnchorVerifier].
//
// It returns the resumable position for recordedAnchor only when all of
// the following hold on the live server:
//
//   - the anchor decodes as one of THIS engine's position tokens and
//     names the slot the caller resolved (a token naming a different
//     slot is not evidence about this one);
//   - that slot still exists;
//   - nothing is attached to it (active = false) — an attached consumer
//     may be advancing it as we look, so "equal right now" would prove
//     nothing a moment later;
//   - its confirmed_flush_lsn is EXACTLY the anchor's LSN, compared by
//     the server in pg_lsn ordering rather than as text.
//
// Any other outcome is an error naming both the recorded and the
// observed value, because the caller's only sound response is to refuse
// and hand an operator those two numbers.
func (e Engine) VerifySnapshotAnchor(ctx context.Context, dsn, slotName, recordedAnchor string) (ir.Position, error) {
	if recordedAnchor == "" {
		return ir.Position{}, errors.New("postgres: verify snapshot anchor: no anchor was recorded")
	}
	recorded := ir.Position{Engine: engineNamePostgres, Token: recordedAnchor}
	decoded, ok, err := decodePGPos(recorded)
	if err != nil {
		return ir.Position{}, fmt.Errorf("postgres: verify snapshot anchor: %w", err)
	}
	if !ok {
		return ir.Position{}, errors.New("postgres: verify snapshot anchor: the recorded anchor is the empty position")
	}
	// The caller's resolved slot name, with the same empty-means-default
	// convention openSnapshotStreamShared applies. A recorded anchor for
	// a DIFFERENT slot says nothing about the one this run would use —
	// and silently preferring the token's own name would resume from a
	// slot the operator did not ask for.
	want := slotName
	if want == "" {
		want = defaultSlot
	}
	if decoded.Slot != want {
		return ir.Position{}, fmt.Errorf(
			"postgres: verify snapshot anchor: the recorded anchor names replication slot %q but this run is "+
				"configured for slot %q; the recorded copy cannot be resumed against a different slot",
			decoded.Slot, want,
		)
	}

	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return ir.Position{}, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return ir.Position{}, err
	}
	defer func() { _ = db.Close() }()

	// One statement, and the LSN comparison is the SERVER's: pg_lsn
	// ordering is not text ordering ("0/9" vs "0/10"), so comparing the
	// two strings in Go would be a correctness bug wearing the clothes
	// of an optimisation. An unparseable recorded LSN errors here rather
	// than silently comparing unequal — decodePGPos already rejected
	// that shape, so this is the belt to its braces.
	const q = `
		SELECT active,
		       COALESCE(confirmed_flush_lsn::text, ''),
		       confirmed_flush_lsn IS NOT DISTINCT FROM $2::pg_lsn
		FROM   pg_replication_slots
		WHERE  slot_name = $1`
	var (
		active    bool
		confirmed string
		atAnchor  bool
	)
	switch err := db.QueryRowContext(ctx, q, want, decoded.LSN).Scan(&active, &confirmed, &atAnchor); {
	case errors.Is(err, sql.ErrNoRows):
		return ir.Position{}, fmt.Errorf(
			"postgres: verify snapshot anchor: replication slot %q does not exist on this source: %w",
			want, ir.ErrSnapshotAnchorAbsent,
		)
	case err != nil:
		return ir.Position{}, fmt.Errorf("postgres: verify snapshot anchor: read slot %q: %w", want, err)
	}
	if active {
		return ir.Position{}, fmt.Errorf(
			"postgres: verify snapshot anchor: replication slot %q is ACTIVE — a consumer is attached to it and "+
				"may be advancing it, so its position cannot be proven to still be the recorded snapshot anchor "+
				"(recorded %s, slot currently at %s). Stop the other consumer, or drop the slot and start over",
			want, decoded.LSN, displayLSN(confirmed),
		)
	}
	if !atAnchor {
		return ir.Position{}, fmt.Errorf(
			"postgres: verify snapshot anchor: replication slot %q has MOVED since the snapshot was taken: the "+
				"recorded anchor is %s but the slot's confirmed_flush_lsn is %s. Something consumed this slot, so "+
				"the changes between those two positions are no longer available and resuming from the anchor "+
				"would skip them",
			want, decoded.LSN, displayLSN(confirmed),
		)
	}
	return recorded, nil
}

// displayLSN renders a possibly-NULL confirmed_flush_lsn for an
// operator-facing message. A logical slot normally has one from
// creation; NULL is worth naming as such rather than printing as an
// empty gap in a sentence about two positions.
func displayLSN(lsn string) string {
	if lsn == "" {
		return "unset (NULL)"
	}
	return lsn
}
