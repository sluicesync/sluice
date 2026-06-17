// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"
)

// Snapshot→CDC position stitch for the auto-shard-by-table COPY
// (ADR-0095). The keyspace-wide single-stream COPY captured ONE global
// VGTID at the global COPY_COMPLETED (a single consistent snapshot
// point). The auto-shard path copies one table at a time, so each table
// t_i finishes its COPY at a DIFFERENT per-table VGTID P_i. The CDC tail
// must resume from a position that loses no change after any table's
// snapshot and re-delivers only overlap the idempotent apply path
// absorbs.
//
// The correct resume position is the per-shard GTID-set INTERSECTION
// (the set-theoretic MINIMUM) of the captured per-table snapshots:
//
//	P_start = ⋂_i P_i   (per shard)
//
// Gapless: P_start ⊆ every P_i, so CDC replays the full window
// (P_start, P_i] and everything after for EVERY table — no committed
// change after any table's snapshot can be skipped (the silent-loss
// class). Overlap-safe: the re-delivered window (P_start, P_i] is the
// same at-least-once seam every cold-start→CDC handoff already relies on
// (ADR-0010 idempotent upsert + the Bug-125 idempotent-copy writer
// already on this path). The FORBIDDEN direction is the union/maximum
// ⋃_i P_i, which would skip (P_i, max] for every lagging table — this
// helper never constructs it.

// stitchSnapshotMin computes the CDC-resume position from the per-table
// snapshot positions captured by the auto-shard COPY loop. Each element
// of perTable is one table's snapshot VGTID (a []shardGtid). The result
// is the per-shard set-minimum: for each shard, the captured per-table
// GTID set that is a subset of every other captured set for that shard.
//
// Because the per-table snapshots come from ONE causally-ordered VStream
// session against one vtgate, on each shard one captured set is already a
// subset of all the others (the earliest-completing table's). go-mysql
// exposes the superset predicate (MysqlGTIDSet.Contain) but no
// interval-intersection primitive, and hand-rolling GTID interval
// arithmetic is exactly the subtle-codec class the Bug-74 "pin the class"
// lesson warns against — so the stitch SELECTS the set-min among the
// observed candidates rather than synthesising a new set. If no captured
// candidate is a subset of all the others for some shard (genuinely
// disjoint per-shard sets — which a single monotonic session should never
// produce), it refuses LOUDLY rather than guess a position that might
// gap.
//
// A single-element perTable (one table, or every table sharing one
// snapshot) returns that element unchanged — the degenerate min. An empty
// perTable is a programming error (the caller copied at least one table)
// and refused loudly.
func stitchSnapshotMin(perTable [][]shardGtid) ([]shardGtid, error) {
	switch len(perTable) {
	case 0:
		return nil, errors.New("mysql/vstream: snapshot stitch: no per-table snapshot positions to stitch")
	case 1:
		return perTable[0], nil
	}

	// Group the candidate GTID strings per (keyspace, shard). Every
	// per-table snapshot must name the same shard layout (one session,
	// one keyspace); a shard missing from any table is a contract
	// violation we surface loudly rather than silently dropping.
	type shardKey struct{ keyspace, shard string }
	candidates := make(map[shardKey][]string)
	order := make([]shardKey, 0)
	seen := make(map[shardKey]bool)
	for ti, snap := range perTable {
		if len(snap) == 0 {
			return nil, fmt.Errorf("mysql/vstream: snapshot stitch: per-table snapshot %d has no shards", ti)
		}
		for _, sg := range snap {
			k := shardKey{keyspace: sg.Keyspace, shard: sg.Shard}
			if !seen[k] {
				seen[k] = true
				order = append(order, k)
			}
			candidates[k] = append(candidates[k], sg.Gtid)
		}
	}

	out := make([]shardGtid, 0, len(order))
	for _, k := range order {
		gtids := candidates[k]
		if len(gtids) != len(perTable) {
			return nil, fmt.Errorf(
				"mysql/vstream: snapshot stitch: shard %s/%s present in %d of %d per-table snapshots; "+
					"every table's snapshot must name the same shard layout",
				k.keyspace, k.shard, len(gtids), len(perTable),
			)
		}
		minGtid, err := selectGTIDSetMin(gtids)
		if err != nil {
			return nil, fmt.Errorf("mysql/vstream: snapshot stitch: shard %s/%s: %w", k.keyspace, k.shard, err)
		}
		out = append(out, shardGtid{Keyspace: k.keyspace, Shard: k.shard, Gtid: minGtid})
	}
	return out, nil
}

// selectGTIDSetMin returns the GTID string from gtids whose set is a
// subset of EVERY other set in gtids (the set-minimum). It uses the same
// containment primitive the PositionOrderer uses (gtidAtOrAfter →
// MysqlGTIDSet.Contain, receiver ⊇ argument), so the ordering here is
// byte-consistent with the engine's persisted-position ordering.
//
// An empty/from-beginning sentinel ("" or "current") in the candidate
// set is itself the safe minimum: "" is the start-of-binlog position,
// which is a subset of any real GTID set — a table that emitted no real
// VGTID before its COPY_COMPLETED resumes the whole keyspace from the
// beginning, the most conservative (no-gap) choice.
//
// Refuses loudly when no candidate is a subset of all the others
// (genuinely disjoint sets).
func selectGTIDSetMin(gtids []string) (string, error) {
	// The empty/from-beginning sentinel dominates as the minimum.
	for _, g := range gtids {
		if g == "" {
			return "", nil
		}
	}

	for i, cand := range gtids {
		isMin := true
		for j, other := range gtids {
			if i == j {
				continue
			}
			// cand is a candidate minimum iff every other set is at-or-after
			// cand, i.e. other ⊇ cand. gtidAtOrAfter(other, cand) reports
			// exactly other-contains-cand.
			ok, err := gtidAtOrAfter(stripGTIDFlavor(other), stripGTIDFlavor(cand))
			if err != nil {
				return "", fmt.Errorf("compare gtid sets %q vs %q: %w", other, cand, err)
			}
			if !ok {
				isMin = false
				break
			}
		}
		if isMin {
			return cand, nil
		}
	}
	return "", fmt.Errorf(
		"no per-table GTID set is a subset of all the others (disjoint snapshots): %v; "+
			"a single VStream session should never produce this — refusing to guess a CDC-resume position that might gap",
		gtids,
	)
}
