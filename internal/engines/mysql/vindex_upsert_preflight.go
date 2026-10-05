// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// ShardKeyUpsertMismatch implements the pipeline's optional
// ShardKeyUpsertProber (migcore) for a SHARDED Vitess/PlanetScale keyspace:
// it reports the first in-scope table that routes on a vindex column outside
// its primary key, or "" when every table is safe (GC-41 (e) part (a)).
//
// # What it is defending against
//
// sluice's idempotent write is `INSERT … AS new ON DUPLICATE KEY UPDATE
// <every non-key column> = new.<col>` — the CDC applier's insert and batched
// update, and the cold-start copy's idempotent writer. vtgate plans that
// statement on the shard the NEW row's vindex value routes to, and refuses it
// outright when the update list assigns any vindex column (VT12001 "DML cannot
// update vindex column", vitess planbuilder/operators/insert.go) — unless the
// assignment is spelled literally `col = VALUES(col)`, which it accepts and
// which silently leaves a row on each of two shards when the value moved
// (TestUpsertSpelling_VitessFamilyNeverUsesValuesFunc keeps sluice off it).
// So on a table whose vindex column is not in the primary key, every
// available spelling either fails on every write or duplicates a primary
// key, and leaving the column out of the list is the duplicating one: there is
// no before-image in an upsert to prove the value did not move. The same
// shape as PlanetScale Neki's shard key outside the conflict key, refused
// under the same code for the same reason.
//
// A vindex column inside the primary key is safe: the update list never names
// a key column, and a conflicting row has the same key — so the same vindex
// value, so the same shard. That is the shape this passes.
//
// # Every vindex, not only the primary one
//
// vtgate's insert check walks every column vindex of the table, so a
// secondary (lookup) vindex on a non-key column fails the upsert exactly as a
// primary one does. The per-change UPDATE would get through for a lookup
// vindex, but the table could not take an insert, so it is refused whole.
//
// # Postures
//
//   - not a vtgate flavor, no bound keyspace, nothing in scope: nothing to do.
//   - shard discovery fails, or the keyspace has one shard: proceed. vtgate
//     ignores vindexes on an unsharded keyspace, and a transient discovery
//     failure must not break a working target.
//   - a table's vschema or key cannot be read: WARN and proceed with the
//     others. This is the deliberate difference from the Neki prober, whose
//     probe failure refuses: on vtgate the statement itself is the backstop —
//     a row write that assigns a vindex column is refused at plan time
//     (SHARDED-TARGET-VINDEX-UPDATE), and the trim in
//     [dropUnchangedKeyColumns] never touches a non-key column — so
//     proceeding costs a later, louder failure and never a silent one, while
//     refusing would stop every sharded target whose role may not read the
//     vschema.
//   - a table absent from the keyspace's vschema: skipped quietly. vtgate
//     cannot route to it at all, and the create-step door
//     (SLUICE-E-SCHEMA-TARGET-KEYSPACE-SHARDED) is what speaks to it.
//
// Reached wherever the pipeline runs PreflightShardKeyUpsert: the sync cold
// start, its stopped-copy resume, the multi-namespace fan-out, and migrate's
// target preflight (which a sharded keyspace never reaches: migrate refuses it
// earlier, at its state-store open). A warm CDC restart runs no target
// preflight; there the statement-time refusal above is what an operator sees.
//
// A table whose every column is in the primary key and whose FIRST key column
// is a vindex column passes here and is still refused at its first insert:
// with no non-key column to assign, the upsert's no-op assignment names the
// first key column ([onDuplicateKeyUpdateClause]). Measured on vttestserver;
// filed as the remainder of GC-41 (e) rather than refused here, because it is
// a rendering choice a fix should change, not a schema the operator must.
func (w *RowWriter) ShardKeyUpsertMismatch(ctx context.Context, tables []*ir.Table) (string, error) {
	if w.vtgateCfg == nil || w.schema == "" || len(tables) == 0 {
		return "", nil
	}
	shardMap, err := discoverAllShardsForKeyspace(ctx, w.vtgateCfg, w.schema)
	if err != nil {
		slog.WarnContext(ctx,
			"mysql: vindex/upsert preflight could not enumerate shards; proceeding — a table that routes on a "+
				"vindex outside its primary key will be refused at its first row write (SHARDED-TARGET-VINDEX-UPDATE) "+
				"instead of here",
			slog.String("keyspace", w.schema), slog.String("err", err.Error()))
		return "", nil
	}
	if len(shardMap[w.schema]) <= 1 {
		return "", nil
	}
	for _, t := range tables {
		if t == nil {
			continue
		}
		vindexCols, err := readVindexColumns(ctx, w.db, w.schema, t.Name)
		if err == nil && len(vindexCols) > 0 {
			var pk []string
			if pk, err = loadPrimaryKeyDB(ctx, w.db, w.schema, t.Name); err == nil {
				if detail := vindexUpsertMismatch(w.schema, t.Name, pk, vindexCols); detail != "" {
					return detail, nil
				}
				continue
			}
		}
		if err != nil && !errors.Is(err, errNoVSchemaEntry) {
			slog.WarnContext(ctx,
				"mysql: vindex/upsert preflight could not read a table's vindexes or primary key; proceeding — if "+
					"it routes on a vindex outside its primary key its first row write is refused "+
					"(SHARDED-TARGET-VINDEX-UPDATE) instead of here",
				slog.String("keyspace", w.schema), slog.String("table", t.Name), slog.String("err", err.Error()))
		}
	}
	return "", nil
}

// vindexUpsertMismatch is the DECISION, split from the reads so every shape
// is graded without a cluster: the detail naming the vindex columns outside
// pk, or "" when there are none. A table with no primary key fails with every
// vindex column, because sluice's upsert on a keyless target assigns the
// whole row.
func vindexUpsertMismatch(keyspace, table string, pk, vindexCols []string) string {
	var outside []string
	for _, vc := range vindexCols {
		in := false
		for _, k := range pk {
			if strings.EqualFold(k, vc) {
				in = true
				break
			}
		}
		if !in {
			outside = append(outside, vc)
		}
	}
	if len(outside) == 0 {
		return ""
	}
	key := "no primary key"
	if len(pk) > 0 {
		key = "primary key (" + strings.Join(pk, ", ") + ")"
	}
	return fmt.Sprintf(
		"table %s.%s on the sharded keyspace routes on vindex column(s) (%s) outside its %s; "+
			"vtgate refuses an ON DUPLICATE KEY UPDATE that assigns a vindex column (VT12001), and leaving the "+
			"column out of the update list is the spelling that duplicates a moved row on two shards",
		keyspace, table, strings.Join(outside, ", "), key,
	)
}

// errNoVSchemaEntry marks a table the keyspace's vschema does not know.
var errNoVSchemaEntry = errors.New("mysql: table has no vschema entry")

// readVindexColumns returns every column of every column vindex vtgate holds
// for keyspace.table, via `SHOW VSCHEMA VINDEXES ON`. The result has one row
// per column vindex (primary first) and a Columns field that lists a
// multi-column vindex's columns comma-separated; the order and the primary
// one do not matter here — every vindex column counts (see
// [RowWriter.ShardKeyUpsertMismatch]). Measured on vttestserver (mysql80): an
// unknown table is Error 1146 VT05005, mapped to errNoVSchemaEntry.
func readVindexColumns(ctx context.Context, db *sql.DB, keyspace, table string) ([]string, error) {
	vindexes, err := readColumnVindexes(ctx, db, keyspace, table)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, v := range vindexes {
		out = append(out, v...)
	}
	return out, nil
}

// readPrimaryVindexColumns returns the columns of keyspace.table's PRIMARY
// vindex — the one vtgate routes an INSERT by — as the first row of
// `SHOW VSCHEMA VINDEXES ON`. That vtgate lists the primary first is a fact
// about Vitess, measured on vttestserver (a secondary vindex added
// afterwards does not displace it) by
// TestVStream_ProbeReplayKey_UnsuppliedPrimaryVindex. A table with no
// column vindex returns nil.
func readPrimaryVindexColumns(ctx context.Context, db *sql.DB, keyspace, table string) ([]string, error) {
	vindexes, err := readColumnVindexes(ctx, db, keyspace, table)
	if err != nil || len(vindexes) == 0 {
		return nil, err
	}
	return vindexes[0], nil
}

// readColumnVindexes returns each column vindex of keyspace.table as its
// columns, in the order `SHOW VSCHEMA VINDEXES ON` lists them.
func readColumnVindexes(ctx context.Context, db *sql.DB, keyspace, table string) ([][]string, error) {
	// A SHOW statement takes no bind parameters; both names are quoted
	// identifiers, so nothing an operator named can escape them.
	rows, err := db.QueryContext(ctx, "SHOW VSCHEMA VINDEXES ON "+quoteIdent(keyspace)+"."+quoteIdent(table))
	if err != nil {
		var me *mysqldriver.MySQLError
		if errors.As(err, &me) && me.Number == 1146 {
			return nil, errNoVSchemaEntry
		}
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	colsAt := -1
	for i, n := range names {
		if strings.EqualFold(n, "Columns") {
			colsAt = i
		}
	}
	if colsAt < 0 {
		return nil, fmt.Errorf("mysql: SHOW VSCHEMA VINDEXES returned no Columns field (got %v)", names)
	}
	var out [][]string
	for rows.Next() {
		vals := make([]sql.NullString, len(names))
		ptrs := make([]any, len(names))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		var cols []string
		for _, c := range strings.Split(vals[colsAt].String, ",") {
			if c = strings.TrimSpace(c); c != "" {
				cols = append(cols, c)
			}
		}
		if len(cols) > 0 {
			out = append(out, cols)
		}
	}
	return out, rows.Err()
}
