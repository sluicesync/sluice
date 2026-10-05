// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// ProbeReplayKey implements [ir.ReplayKeyProber]: it reports whether the
// target table exists and, if so, whether a replayed row of the recorded
// table collides on a key instead of landing a second copy.
//
// The predicate is "SOME unique key is fully supplied", not "the key the
// upsert picker would choose", because that is how this engine's write
// paths collide: ON DUPLICATE KEY UPDATE (the change applier, the
// idempotent writer) and LOAD DATA LOCAL (the restore bulk load, which
// downgrades the duplicate to a warning and skips the row) both fire on a
// conflict against ANY unique index. Measured in
// TestRowWriter_ProbeReplayKey_ShapeMatrix: an AUTO_INCREMENT surrogate
// PRIMARY KEY beside a NOT NULL UNIQUE (id) converges, an AUTO_INCREMENT
// or DEFAULT (UUID_TO_BIN(UUID())) surrogate alone duplicates.
//
// A key qualifies when it is the PRIMARY KEY or a UNIQUE index whose every
// key part is
//
//   - a plain column (a functional key part — COLUMN_NAME NULL in
//     STATISTICS — never qualifies, conservatively: ODKU would collide on
//     it, but whether its expression reads only supplied columns is not
//     visible here);
//   - NOT NULL on the target (ODKU does not collide on a NULL: a
//     nullable-UNIQUE-only table still duplicates — measured);
//   - not a generated column on the target (the applier drops it from
//     the INSERT; refused conservatively, like a functional part);
//   - SUPPLIED: a non-generated column of the recorded table, matched
//     case-insensitively, which is how MySQL resolves a column name.
//
// On a vtgate keyspace with more than one shard, no key qualifies unless
// every column of the table's PRIMARY VINDEX is supplied too: keys are
// enforced per shard, and a re-sent row whose routing column the rows do not
// carry lands on whatever shard its fresh value hashes to
// ([RowWriter.replayRoutesBySuppliedVindex]).
//
// Why not the applier's batching probe ([ChangeApplier.tableIsKeyless]),
// which counts any UNIQUE: that is ADR-0089 batch sizing and is
// deliberately loose about nullability and supply.
func (w *RowWriter) ProbeReplayKey(ctx context.Context, table *ir.Table) (exists, keyed bool, err error) {
	if table == nil {
		return false, false, errors.New("mysql: ProbeReplayKey: table is nil")
	}
	supplied := irbackup.ReplaySuppliedColumns(table)
	if len(supplied) == 0 {
		return false, false, fmt.Errorf("mysql: ProbeReplayKey: recorded table %q carries no insertable columns; "+
			"cannot judge whether its rows supply a key", table.Name)
	}
	name := table.Name
	const existsQ = `SELECT COUNT(*) FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = COALESCE(NULLIF(?, ''), DATABASE()) AND TABLE_NAME = ?`
	var n int
	if err := w.db.QueryRowContext(ctx, existsQ, w.schema, name).Scan(&n); err != nil {
		return false, false, fmt.Errorf("mysql: probe replay key for %q: existence: %w", name, err)
	}
	if n == 0 {
		return false, false, nil
	}
	cols, err := w.loadKeyColumnShape(ctx, name)
	if err != nil {
		return true, false, err
	}
	keys, err := w.loadUniqueKeys(ctx, name)
	if err != nil {
		return true, false, err
	}
	carried := make(map[string]bool, len(supplied))
	for _, c := range supplied {
		carried[strings.ToLower(c)] = true
	}
	routed, err := w.replayRoutesBySuppliedVindex(ctx, name, carried)
	if err != nil {
		return true, false, err
	}
	if !routed {
		return true, false, nil
	}
	for _, key := range keys {
		if replayKeyCollides(key, cols, carried) {
			return true, true, nil
		}
	}
	return true, false, nil
}

// replayRoutesBySuppliedVindex reports whether a replayed row of table is
// routed to the SAME shard as the row it re-sends, which every key collision
// on a sharded keyspace depends on: a vtgate-fronted keyspace with more than
// one shard enforces a PRIMARY KEY or UNIQUE index per shard, so two rows
// collide only when the primary vindex — the one vtgate routes an INSERT by —
// sends them to one shard. That holds exactly when every primary-vindex
// column is SUPPLIED by the rows (carried, lower-cased). A primary vindex on
// a column the rows do not carry — a vtgate-sequence-backed or defaulted
// surrogate — gives every re-sent row a fresh value and so a fresh shard,
// and a supplied UNIQUE elsewhere collides with nothing on the other one:
// measured on vttestserver (2 shards, sid from a sequence, UNIQUE(email)
// supplied), 7 of 16 re-sends landed a duplicate email at no error
// (TestVStream_ProbeReplayKey_UnsuppliedPrimaryVindex).
//
// Not a vtgate target, or one shard: every key is enforced keyspace-wide
// (vtgate ignores vindexes on an unsharded keyspace), so true. Fail closed
// otherwise: a shard enumeration or vschema read that errors returns the
// error, and the doors and retry gates treat a probe that cannot answer as
// never licensing a re-write. A table the sharded keyspace's vschema does
// not know (errNoVSchemaEntry) is not refused here: vtgate cannot route a
// row to it at all, so every write to it fails — loudly — before anything
// can land twice.
func (w *RowWriter) replayRoutesBySuppliedVindex(ctx context.Context, table string, carried map[string]bool) (bool, error) {
	if w.vtgateCfg == nil || w.schema == "" {
		return true, nil
	}
	w.replayShardsMu.Lock()
	shards := w.replayShards
	w.replayShardsMu.Unlock()
	if shards == 0 {
		shardMap, err := discoverAllShardsForKeyspace(ctx, w.vtgateCfg, w.schema)
		if err != nil {
			return false, fmt.Errorf("mysql: probe replay key for %q: enumerate the shards of keyspace %q: %w", table, w.schema, err)
		}
		shards = len(shardMap[w.schema])
		if shards > 0 {
			w.replayShardsMu.Lock()
			w.replayShards = shards
			w.replayShardsMu.Unlock()
		}
	}
	if shards <= 1 {
		return true, nil
	}
	primary, err := readPrimaryVindexColumns(ctx, w.db, w.schema, table)
	if errors.Is(err, errNoVSchemaEntry) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("mysql: probe replay key for %q: read its primary vindex on the sharded keyspace %q: %w",
			table, w.schema, err)
	}
	return primaryVindexSupplied(primary, carried), nil
}

// primaryVindexSupplied is the DECISION of [RowWriter.replayRoutesBySuppliedVindex],
// split from the reads so it is graded without a cluster: every primary
// vindex column (matched case-insensitively, as MySQL resolves a column
// name) is carried. A sharded table with NO primary vindex is not routable
// by vtgate, so nothing can land twice through it; it reports true.
func primaryVindexSupplied(primary []string, carried map[string]bool) bool {
	for _, c := range primary {
		if !carried[strings.ToLower(c)] {
			return false
		}
	}
	return true
}

// keyColumnShape is what [RowWriter.ProbeReplayKey] needs to know about a
// target column: whether it can hold NULL and whether it is generated.
type keyColumnShape struct {
	nullable  bool
	generated bool
}

// replayKeyCollides reports whether key (its key parts' column names, ""
// for a functional part) collides on every replayed row: see
// [RowWriter.ProbeReplayKey] for the four conditions. cols is keyed by
// lower-cased column name; carried holds the lower-cased supplied names.
func replayKeyCollides(key []string, cols map[string]keyColumnShape, carried map[string]bool) bool {
	if len(key) == 0 {
		return false
	}
	for _, part := range key {
		if part == "" {
			return false
		}
		lc := strings.ToLower(part)
		shape, ok := cols[lc]
		if !ok || shape.nullable || shape.generated || !carried[lc] {
			return false
		}
	}
	return true
}

// loadKeyColumnShape reads each target column's nullability and whether it
// is generated, keyed by lower-cased name.
func (w *RowWriter) loadKeyColumnShape(ctx context.Context, name string) (map[string]keyColumnShape, error) {
	const q = `SELECT COLUMN_NAME, IS_NULLABLE, COALESCE(GENERATION_EXPRESSION, '') FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = COALESCE(NULLIF(?, ''), DATABASE()) AND TABLE_NAME = ?`
	rows, err := w.db.QueryContext(ctx, q, w.schema, name)
	if err != nil {
		return nil, fmt.Errorf("mysql: probe replay key for %q: columns: %w", name, err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]keyColumnShape{}
	for rows.Next() {
		var col, nullable, genExpr string
		if err := rows.Scan(&col, &nullable, &genExpr); err != nil {
			return nil, fmt.Errorf("mysql: probe replay key for %q: columns: %w", name, err)
		}
		out[strings.ToLower(col)] = keyColumnShape{nullable: nullable != "NO", generated: genExpr != ""}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: probe replay key for %q: columns: %w", name, err)
	}
	return out, nil
}

// loadUniqueKeys reads every unique index of the target table (the PRIMARY
// KEY included) as its key parts' column names in SEQ_IN_INDEX order, ""
// standing for a functional key part.
func (w *RowWriter) loadUniqueKeys(ctx context.Context, name string) ([][]string, error) {
	const q = `SELECT INDEX_NAME, COLUMN_NAME FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = COALESCE(NULLIF(?, ''), DATABASE()) AND TABLE_NAME = ? AND NON_UNIQUE = 0
		ORDER BY INDEX_NAME, SEQ_IN_INDEX`
	rows, err := w.db.QueryContext(ctx, q, w.schema, name)
	if err != nil {
		return nil, fmt.Errorf("mysql: probe replay key for %q: unique indexes: %w", name, err)
	}
	defer func() { _ = rows.Close() }()
	var keys [][]string
	index := map[string]int{}
	for rows.Next() {
		var idxName string
		var col sql.NullString
		if err := rows.Scan(&idxName, &col); err != nil {
			return nil, fmt.Errorf("mysql: probe replay key for %q: unique indexes: %w", name, err)
		}
		i, ok := index[idxName]
		if !ok {
			i = len(keys)
			index[idxName] = i
			keys = append(keys, nil)
		}
		keys[i] = append(keys[i], col.String) // "" for a functional part
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: probe replay key for %q: unique indexes: %w", name, err)
	}
	return keys, nil
}
