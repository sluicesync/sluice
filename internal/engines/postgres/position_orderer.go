// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

// PositionAtOrAfter implements [ir.PositionOrderer] for the Postgres
// engine. It reports whether event position p is at or after anchor —
// i.e. whether the schema snapshotted at anchor (ADR-0049) is the one
// in effect for an event observed at p.
//
// Postgres LSNs are a TOTAL order on a single timeline: p is at or
// after anchor iff p.LSN >= anchor.LSN. Unlike the MySQL GTID case
// this is a genuine total order, but the interface stays a partial
// "is-at-or-after" predicate for engine-neutrality (ADR-0049 rejects a
// -1/0/1 comparator).
//
// Positions are comparable only within the same replication slot: a
// slot binds the WAL timeline the LSN is meaningful on, and two LSNs
// from different slots are not orderable. A slot mismatch is therefore
// unorderable → false (the schema-history store's loud floor then
// routes to ADR-0022 cold-start), never a guess.
//
// err is returned ONLY for a malformed/unparseable position (a real
// bug) — never for an ordinary "false" answer.
func (Engine) PositionAtOrAfter(p, anchor ir.Position) (bool, error) {
	pp, ok, err := decodePGPos(p)
	if err != nil {
		return false, fmt.Errorf("postgres: position-orderer: decode p: %w", err)
	}
	if !ok {
		return false, errors.New("postgres: position-orderer: p is the empty/from-now sentinel, not an orderable position")
	}
	ap, ok, err := decodePGPos(anchor)
	if err != nil {
		return false, fmt.Errorf("postgres: position-orderer: decode anchor: %w", err)
	}
	if !ok {
		return false, errors.New("postgres: position-orderer: anchor is the empty/from-now sentinel, not an orderable position")
	}

	// Different slots → different WAL timelines; LSNs are not
	// comparable. Unorderable, not a silent guess.
	if pp.Slot != ap.Slot {
		return false, nil
	}

	// decodePGPos already validated both LSNs parse, but parse again
	// to compare the numeric value (the canonical "X/XXXXXXXX" form is
	// not byte-lexically ordered across the slash, so a string compare
	// would be wrong).
	pLSN, err := pglogrepl.ParseLSN(pp.LSN)
	if err != nil {
		return false, fmt.Errorf("postgres: position-orderer: parse p lsn %q: %w", pp.LSN, err)
	}
	aLSN, err := pglogrepl.ParseLSN(ap.LSN)
	if err != nil {
		return false, fmt.Errorf("postgres: position-orderer: parse anchor lsn %q: %w", ap.LSN, err)
	}
	return pLSN >= aLSN, nil
}

// AddedTableCopyPosition implements [ir.AddedTableReplayWindowSource] for
// Postgres (Bug 336). The add puts the table in the publication and then
// creates a temporary slot whose exported snapshot it copies from, so the
// restarted stream decodes, and replays onto the copy, every change to the
// table committed between the ALTER PUBLICATION and that snapshot's
// consistent point (156 ms measured with autocommit writes, up to the commit
// of any transaction open across the add).
//
// A row change's position is its transaction's commit LSN (the BeginMessage
// FinalLSN the reader stamps on every row). A transaction the snapshot holds
// committed before the consistent point C, so its commit LSN is < C (or <= C,
// whichever end of the commit record FinalLSN names); one it does not hold
// commits at or after C. The bound is therefore C+1: a change is in the
// window unless its LSN is >= C+1, so every change the copy holds is in it,
// and the one boundary case either reading leaves ambiguous (a commit LSN of
// exactly C) is in it too, where the idempotent form is right whatever the
// target holds. Pinned by TestAddedTableCopyPosition_BoundsTheSnapshot.
//
// The position names the STREAM's slot, system and timeline (resumeFrom):
// the snapshot rode a temporary slot, and [Engine.PositionAtOrAfter] orders
// positions of one slot only, so a bound in the temporary slot's name would
// never be passed and the window would never close.
func (Engine) AddedTableCopyPosition(snapshot, resumeFrom ir.Position) (ir.Position, error) {
	snap, ok, err := decodePGPos(snapshot)
	if err != nil {
		return ir.Position{}, fmt.Errorf("postgres: added-table copy position: decode the snapshot position: %w", err)
	}
	if !ok {
		return ir.Position{}, errors.New("postgres: added-table copy position: the snapshot reported no position")
	}
	resume, ok, err := decodePGPos(resumeFrom)
	if err != nil {
		return ir.Position{}, fmt.Errorf("postgres: added-table copy position: decode the stream's position: %w", err)
	}
	if !ok {
		return ir.Position{}, errors.New("postgres: added-table copy position: the stream has no position to resume from")
	}
	if snap.SystemID != "" && resume.SystemID != "" && snap.SystemID != resume.SystemID {
		return ir.Position{}, fmt.Errorf("postgres: added-table copy position: the snapshot was taken on system %s and the stream reads system %s",
			snap.SystemID, resume.SystemID)
	}
	lsn, err := pglogrepl.ParseLSN(snap.LSN)
	if err != nil {
		return ir.Position{}, fmt.Errorf("postgres: added-table copy position: parse snapshot lsn %q: %w", snap.LSN, err)
	}
	resume.LSN = (lsn + 1).String()
	return encodePGPos(resume)
}
