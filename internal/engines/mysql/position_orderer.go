// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"
	"strings"

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
// VStream/PlanetScale positions take a separate path. A VStream
// position is a JSON ARRAY of per-shard shardGtid (see
// cdc_vstream_position.go), not the single binlogPos object the binlog
// path uses — both ride the same MySQL-family engine tag ("mysql" or
// "planetscale"), so the token SHAPE is the discriminator. VStream
// positions are ordered by per-shard GTID superset (the same partial
// order, applied per shard); the COPY-cursor TablePKs are ignored for
// ordering (a resume cursor doesn't change which schema version is in
// effect — it's snapshot bookkeeping). A shape mismatch between p and
// anchor (one VStream, one vanilla binlogPos) is a loud "not
// comparable" error, never a silent false — they came from
// incompatible streams.
//
// err is returned ONLY for a malformed/unparseable position (a real
// bug) — never for an ordinary "false" answer.
func (e Engine) PositionAtOrAfter(p, anchor ir.Position) (bool, error) {
	pIsVStream := isVStreamToken(p)
	aIsVStream := isVStreamToken(anchor)
	if pIsVStream || aIsVStream {
		if pIsVStream != aIsVStream {
			// One side is a VStream array, the other a vanilla binlogPos
			// object: the positions came from incompatible streams.
			// Unorderable — loud, not a silent false (mirrors the
			// mode-mismatch branch below).
			return false, fmt.Errorf(
				"mysql: position-orderer: position-shape mismatch (p vstream=%v anchor vstream=%v); positions are not comparable",
				pIsVStream, aIsVStream,
			)
		}
		return vstreamPositionAtOrAfter(p, anchor)
	}

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

// isVStreamToken reports whether p's token is a VStream position (a
// JSON array of shardGtid) rather than a vanilla binlogPos object. The
// two share the same MySQL-family engine tag, so the leading non-space
// byte of the token is the discriminator: '[' for the array, '{' for
// the object. The from-now sentinel (empty token) and non-mysql-family
// engines are NOT VStream tokens — they fall through to the binlog
// path, which surfaces the appropriate sentinel/loud-engine error.
func isVStreamToken(p ir.Position) bool {
	if !isMySQLFamilyEngine(p.Engine) {
		return false
	}
	t := strings.TrimSpace(p.Token)
	return t != "" && t[0] == '['
}

// vstreamPositionAtOrAfter orders two VStream positions by per-shard
// GTID superset. p is at-or-after anchor iff, for EVERY shard present
// in anchor, p carries the same (keyspace, shard) and p's GTID set for
// that shard ⊇ anchor's (the same partial-order predicate the binlog
// path uses, applied per shard). A shard present in anchor but absent
// in p ⇒ not-at-or-after (false, no error) — the partial order's
// disjoint case, never a silent guess.
//
// TablePKs (the COPY-resume cursor) is IGNORED: it is snapshot
// bookkeeping, not causal position, so a position carrying a cursor
// orders identically to the same position without one. This is what
// lets a mid-COPY warm-resume order against the post-snapshot
// schema-history anchors without the cursor perturbing the result.
//
// The per-shard gtid is a canonical Vitess "MySQL56/<sets>" string;
// stripGTIDFlavor removes the flavor prefix before handing the bare set
// to go-mysql's parser (ParseMysqlGTIDSet rejects the prefixed form).
func vstreamPositionAtOrAfter(p, anchor ir.Position) (bool, error) {
	pShards, ok, err := decodeVStreamPos(p)
	if err != nil {
		return false, fmt.Errorf("mysql: position-orderer: decode vstream p: %w", err)
	}
	if !ok {
		return false, errors.New("mysql: position-orderer: vstream p is the empty/from-now sentinel, not an orderable position")
	}
	aShards, ok, err := decodeVStreamPos(anchor)
	if err != nil {
		return false, fmt.Errorf("mysql: position-orderer: decode vstream anchor: %w", err)
	}
	if !ok {
		return false, errors.New("mysql: position-orderer: vstream anchor is the empty/from-now sentinel, not an orderable position")
	}

	// Index p's shards by (keyspace, shard) so the anchor scan is O(n).
	pByShard := make(map[string]string, len(pShards))
	for _, s := range pShards {
		pByShard[vstreamShardKey(s.Keyspace, s.Shard)] = s.Gtid
	}

	// p is at-or-after anchor iff it dominates EVERY anchor shard. A
	// sharded keyspace yields one entry per shard; the superset must
	// hold for all of them (partial order over the per-shard product).
	for _, a := range aShards {
		pGtid, present := pByShard[vstreamShardKey(a.Keyspace, a.Shard)]
		if !present {
			// anchor covers a shard p doesn't carry — unorderable in the
			// at-or-after direction (the disjoint case). Not an error.
			return false, nil
		}
		after, err := gtidAtOrAfter(stripGTIDFlavor(pGtid), stripGTIDFlavor(a.Gtid))
		if err != nil {
			return false, err
		}
		if !after {
			return false, nil
		}
	}
	return true, nil
}

// vstreamShardKey is the composite map key identifying one shard within
// a (possibly multi-keyspace) VStream position.
func vstreamShardKey(keyspace, shard string) string {
	return keyspace + "/" + shard
}

// stripGTIDFlavor removes the leading "MySQL56/" flavor prefix Vitess
// stamps on its GTID strings ("MySQL56/<uuid>:1-N"). go-mysql's
// ParseMysqlGTIDSet expects the bare set without the flavor prefix (the
// prefix makes it read the flavor as part of the first UUID and fail
// with "invalid UUID length"). The match is case-insensitive on the
// prefix and a no-op for the sentinels ("", "current") and for any
// already-bare set, so it's safe to apply unconditionally.
func stripGTIDFlavor(gtid string) string {
	const prefix = "MySQL56/"
	if len(gtid) >= len(prefix) && strings.EqualFold(gtid[:len(prefix)], prefix) {
		return gtid[len(prefix):]
	}
	return gtid
}
