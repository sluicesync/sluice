// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"log/slog"
	"sort"

	"sluicesync.dev/sluice/internal/ir"
)

// # Owed schema boundaries (GC-43 (r) F1)
//
// An in-scope DDL does not emit its schema boundary. The generic-DDL arm
// clears the decode cache and records the DDL's position; the
// [ir.SchemaSnapshot] that carries the change — to schema history, to the
// ADR-0091 schema-change forward, through the SL-2 TIMESTAMP⇄DATETIME
// refusal — is emitted lazily, by maybeSnapshotSchemaB1, ahead of the
// table's NEXT row. That state lives only in this process. A restart builds
// a fresh reader with nothing pending, so the first post-restart row decodes
// against the live post-DDL shape and no boundary is ever emitted for it.
//
// So a persisted position past the DDL, followed by a restart before the
// table's next row, loses the boundary: the target keeps the pre-DDL shape
// and the values that follow land in it. Measured with
// `ALTER TABLE t MODIFY ts DATETIME(6)`: microsecond values rounded into the
// target's DATETIME(0), at exit 0.
//
// The rotation boundary (cdc_rotation_boundary.go) is a persisted position
// with no row in it, so it could reach exactly that state on an idle stream.
// Before it emits, it SETTLES what is owed: for every owed table it runs
// what the table's next row would run — tableFor's rebuild, then
// maybeSnapshotSchemaB1 — inside the boundary transaction, ahead of its
// commit. The outputs (a version, no delta, a nullability forward, or the SL-2
// refusal stopping the stream) are the ones the lazy path would produce, read
// against the same live catalog, only earlier. If any owed table cannot be
// rebuilt now (a transient source error), nothing is emitted and the
// boundary is skipped, which is the pre-GC-43 (r) behaviour.
//
// # GTID mode: a settled ADD COLUMN can refuse at the next stop (loud)
//
// A forward is not finished when its snapshot is emitted. A forwarded ADD
// COLUMN backfills the target's existing rows, and the pipeline counts that
// backfill durable only once the target has persisted a position STRICTLY
// past the boundary snapshot's (schema_forward_backfill_ledger.go: a stop
// before then ends the run ADD-COLUMN-BACKFILL-INCOMPLETE). In file/pos mode
// the rotation boundary is such a position — (N+1, 4) lies past the DDL's own
// (N, end). In GTID mode it is not: with no transaction committed since the
// DDL, the boundary carries the DDL anchor's own executed set. So on an idle
// GTID source a stop after a settled ADD COLUMN, before any later transaction
// (any database, or the --source-heartbeat-interval row), refuses with that
// marker although the backfill landed (measured on mysql:8.0 GTID and
// mariadb:11.4). That is accepted, because the alternative is worse: SKIPPING
// the boundary there was tried and measured, and it does not keep the forward
// — a restart re-reads the DDL, and the pipeline's forward intercept, which
// has no pre-state after a warm resume, takes the table's first boundary as
// its baseline and forwards nothing (GC-44). Settling applies the forward
// before the restart can lose it; the refusal is loud and is acknowledged
// with --accept-unforwarded-schema-change.
//
// # What is owed
//
// Every table the reader knows when the DDL clears its cache: the decode
// cache itself, the seeded and retained shapes (priorSig), and the emitted
// versions (snapshotSig, forwardNullSig). That is the set the lazy path
// would emit a boundary for on its next row. A table the reader has never
// heard of — created by an in-scope CREATE TABLE after this process started
// and not yet written — is not in it; filed with the commit-path sibling
// under GC-44.
//
// A table whose rebuild has no columns is gone (dropped or renamed away). The
// lazy path never reaches it — it has no next row — so it settles with
// nothing emitted, and leaves no empty shape in the cache.
//
// # The wider class (GC-44, OPEN)
//
// Settling at the rotation closes the one path GC-43 (r) added. It does not
// close the class, and the reason is on the pipeline side: after a warm
// resume the forward intercept has no pre-state for any table, so the first
// boundary it sees per table becomes its baseline and is not forwarded —
// whether that boundary is a DDL replayed from before the stop (the persisted
// position was BEFORE it) or a DDL run after the restart. Measured on
// mysql:8.0 → Postgres 16: both lose an fsp widen, rounding the next row's
// microseconds at exit 0. Any TxCommit persisting past a pending DDL (an
// out-of-scope commit, another table's) is one way in, not the root. See
// GC-44 in docs/dev/audit-backlog.md.

// oweSchemaBoundaries marks every table the reader knows as owing its
// post-DDL boundary. Called by the generic-DDL arm before it clears the cache.
func (r *CDCReader) oweSchemaBoundaries() {
	if r.owedSchemaBoundary == nil {
		r.owedSchemaBoundary = map[string]struct{}{}
	}
	owe := func(qn string) {
		if schema, _ := splitQualified(qn); r.databaseInScope(schema) {
			r.owedSchemaBoundary[qn] = struct{}{}
		}
	}
	for qn := range r.schemaCache {
		owe(qn)
	}
	for qn := range r.priorSig {
		owe(qn)
	}
	for qn := range r.snapshotSig {
		owe(qn)
	}
	for qn := range r.forwardNullSig {
		owe(qn)
	}
}

// owedSchemaBoundaryTables rebuilds every owed table. ok is false — and
// nothing should be emitted — when any rebuild fails.
func (r *CDCReader) owedSchemaBoundaryTables(ctx context.Context) (tables map[string]*tableSchema, ok bool) {
	if len(r.owedSchemaBoundary) == 0 {
		return nil, true
	}
	tables = make(map[string]*tableSchema, len(r.owedSchemaBoundary))
	for qn := range r.owedSchemaBoundary {
		tbl, err := r.tableFor(ctx, qn)
		if err != nil {
			slog.DebugContext(ctx, "mysql: cdc: could not rebuild a table owing its post-DDL schema boundary; "+
				"not persisting a position at this rotation",
				slog.String("table", qn), slog.String("err", err.Error()))
			return nil, false
		}
		if len(tbl.Columns) == 0 {
			// Gone: nothing the lazy path would ever emit.
			delete(r.schemaCache, qn)
			delete(r.owedSchemaBoundary, qn)
			continue
		}
		tables[qn] = tbl
	}
	return tables, true
}

// settleSchemaBoundaries emits each rebuilt owed table's boundary exactly as
// its next row would (maybeSnapshotSchemaB1), in name order.
func (r *CDCReader) settleSchemaBoundaries(ctx context.Context, tables map[string]*tableSchema, out chan<- ir.Change) error {
	names := make([]string, 0, len(tables))
	for qn := range tables {
		names = append(names, qn)
	}
	sort.Strings(names)
	for _, qn := range names {
		if err := r.maybeSnapshotSchemaB1(ctx, qn, tables[qn], out); err != nil {
			return err
		}
	}
	return nil
}
