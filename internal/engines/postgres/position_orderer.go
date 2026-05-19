// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"

	"github.com/jackc/pglogrepl"

	"github.com/orware/sluice/internal/ir"
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
		return false, fmt.Errorf("postgres: position-orderer: p is the empty/from-now sentinel, not an orderable position")
	}
	ap, ok, err := decodePGPos(anchor)
	if err != nil {
		return false, fmt.Errorf("postgres: position-orderer: decode anchor: %w", err)
	}
	if !ok {
		return false, fmt.Errorf("postgres: position-orderer: anchor is the empty/from-now sentinel, not an orderable position")
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
