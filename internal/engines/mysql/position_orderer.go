// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// PositionAtOrAfter implements [ir.PositionOrderer] for the MySQL
// engine. It reports whether event position p is at or after anchor —
// i.e. whether the schema snapshotted at anchor (ADR-0049) is the one
// in effect for an event observed at p.
//
// The causal test reuses the engine's existing GTID-subset primitive
// (the same predicate verifyGTIDSetReachable evaluates server-side via
// GTID_SUBSET): a GTID set A is "at or before" a GTID set P iff every
// transaction in A is also in P, i.e. P ⊇ A. go-mysql's
// MysqlGTIDSet.Contain(o) reports exactly this superset relation
// (receiver ⊇ argument). This is a PARTIAL order — two disjoint GTID
// sets are neither at-or-after the other, which is the correct answer
// and the reason ADR-0049 rejects a -1/0/1 total-order comparator
// (the Bug-74-class trap).
//
// file/pos mode has no cross-instance ordering (binlog filenames are
// instance-local; ADR mirrors verifySourceInstanceIdentity's reasoning):
// positions are comparable only when they share a ServerUUID, in which
// case (file, pos) is a total order within that lineage. A ServerUUID
// mismatch is an unorderable pair → false (the schema-history store's
// loud floor then routes to ADR-0022 cold-start), never a guess.
//
// err is returned ONLY for a malformed/unparseable position (a real
// bug) — never for an ordinary "false" answer.
func (e Engine) PositionAtOrAfter(p, anchor ir.Position) (bool, error) {
	pp, ok, err := decodeBinlogPos(p)
	if err != nil {
		return false, fmt.Errorf("mysql: position-orderer: decode p: %w", err)
	}
	if !ok {
		return false, errors.New("mysql: position-orderer: p is the empty/from-now sentinel, not an orderable position")
	}
	ap, ok, err := decodeBinlogPos(anchor)
	if err != nil {
		return false, fmt.Errorf("mysql: position-orderer: decode anchor: %w", err)
	}
	if !ok {
		return false, errors.New("mysql: position-orderer: anchor is the empty/from-now sentinel, not an orderable position")
	}

	if pp.Mode != ap.Mode {
		// A stream's position mode is fixed for the reader's lifetime
		// (see positionMode docs); a p/anchor mode mismatch means the
		// two positions came from incompatible streams. Unorderable —
		// loud, not a silent false.
		return false, fmt.Errorf("mysql: position-orderer: mode mismatch (p=%q anchor=%q); positions are not comparable",
			pp.Mode, ap.Mode)
	}

	switch pp.Mode {
	case positionModeGTID:
		return gtidAtOrAfter(pp.GTIDSet, ap.GTIDSet)
	case positionModeFilePos:
		// Cross-instance binlog filenames are not comparable (the
		// node-replace class verifySourceInstanceIdentity guards). When
		// both positions carry a ServerUUID and they differ, the pair
		// is unorderable → false (loud floor handles the consequence).
		// An empty ServerUUID on either side (positions written before
		// the field existed) degrades to the within-lineage comparison
		// rather than refusing — same posture as
		// verifySourceInstanceIdentity's transitional handling.
		if pp.ServerUUID != "" && ap.ServerUUID != "" && pp.ServerUUID != ap.ServerUUID {
			return false, nil
		}
		if pp.File != ap.File {
			// Binlog filenames sort lexically in rotation order
			// (mysql-bin.000001 < mysql-bin.000002 < …); the fixed
			// zero-padded width makes string comparison correct.
			return pp.File > ap.File, nil
		}
		return pp.Pos >= ap.Pos, nil
	default:
		return false, fmt.Errorf("mysql: position-orderer: unsupported position mode %q", pp.Mode)
	}
}

// gtidAtOrAfter reports whether GTID set p is at or after anchor —
// i.e. p ⊇ anchor (every transaction the anchor covers is already in
// p). Empty strings are valid: the empty set is a subset of every
// set, so an empty anchor is at-or-before everything, and an empty p
// is at-or-after only an empty anchor.
//
// Parsing uses go-mysql's offline MySQL-flavor GTID parser — no DB
// round-trip, unlike verifyGTIDSetReachable's server-side
// GTID_SUBSET (the predicate is identical; this is the offline twin
// for ordering two persisted positions against each other).
func gtidAtOrAfter(p, anchor string) (bool, error) {
	pSet, err := gomysql.ParseMysqlGTIDSet(p)
	if err != nil {
		return false, fmt.Errorf("mysql: position-orderer: parse p gtid set %q: %w", p, err)
	}
	aSet, err := gomysql.ParseMysqlGTIDSet(anchor)
	if err != nil {
		return false, fmt.Errorf("mysql: position-orderer: parse anchor gtid set %q: %w", anchor, err)
	}
	// Contain(o) reports whether the receiver is a superset of o.
	// p at-or-after anchor ⟺ p ⊇ anchor.
	return pSet.Contain(aSet), nil
}
