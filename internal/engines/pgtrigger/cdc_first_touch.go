// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

// The first-touch schema boundary (KEY-COLLATION-TRIGGER-CDC-NO-HISTORY).
//
// # The harm
//
// The target appliers' key-collation guard (KEY-COLLATION-MERGE) arms from
// the source's shape of a table: the applier's active schema version, which
// a SchemaSnapshot on the change stream sets. The pgoutput reader emits one
// at every table's first Relation of a stream, so a native Postgres → MySQL
// sync refuses an insert of 'A' beside 'a' on a UNIQUE key the source keeps
// apart (case-sensitive) and MySQL's `utf8mb4_0900_ai_ci` does not. This
// reader emitted none, so the same insert from a postgres-trigger source was
// an ON DUPLICATE KEY UPDATE onto row 'a' — the target holding one row where
// the source holds two — at exit 0, in steady state, with no DDL anywhere.
// Measured on postgres:16 → MySQL 8 for live `sync start` (cold start and
// warm resume) and for a chain restore of a `backup incremental` and a
// `backup stream` window (TestKeyCollationTriggerCDC_*).
//
// # The fix
//
// The first change of each table in a reader session is preceded by an
// [ir.SchemaSnapshot] carrying the table's shape: the schema reader's read of
// it — the same reader that recorded the backup manifest's schema and the
// cold start's seed, so a version and either of those agree field for field
// — with each column's collation stamped as the pgoutput reader stamps a
// Relation's ([postgres.ColumnCollationsAsOf], [postgres.StampColumnCollation]).
// The capturing transaction's xid stands in for the decoded one, so a
// nondeterministic collation the catalog says was set AFTER the rows were
// captured is stamped as not established (named, Unknown determinism), which
// both target guards compare as bytes: the KEY-COLLATION-PG-LOOSENED-REPLAY
// rule, on this lane.
//
// # The anchor
//
// The snapshot is anchored one id BELOW the change it precedes — the position
// "applied through id-1". Every change before it in this session carried an
// id at or below that, so the anchor never goes backwards, and an applier
// that persists a boundary's position outside a source transaction (this
// stream has none: it is marker-less) persists a point from which a resume
// re-reads the change the snapshot governs. Anchored AT the change's id, a
// stop between the snapshot's commit and the row's would resume past the row:
// silent loss. Ordered by [Engine.PositionAtOrAfter], which this boundary is
// why the engine has: a warm resume primes the applier's schema-history cache
// by ordering the retained versions against the persisted position, and with
// versions retained and no orderer that prime refuses every restart.
//
// # What it reaches, stated
//
// Every consumer of this reader's stream: live `sync start` (the cold start's
// CDC handoff opens this reader too, through openCDCReader), `backup
// incremental` and `backup stream` (the snapshot is drained onto the
// manifest's SchemaHistory, from where chain restore replays it ahead of the
// window's rows), and `sync from-backup`, which already armed from the
// segment's recorded schema (KEY-COLLATION-BROKER-UNARMED) and now also sees
// versions that agree with it. A table read here that is no longer in the
// catalog under the name its changes carry refuses rather than apply them
// unguarded: only DDL can do that, and DDL halts this stream anyway.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/ir"
)

// EmitsSchemaBoundaries marks this reader for the pipeline's
// schemaBoundaryEmitter surface: it takes no schema seed, but its stream
// carries the first-touch boundaries above, so a warm resume must still load
// the retained history and target catalog the unforwarded-stream check
// judges them against. Without that load every warm resume judged with no
// prior, and a narrowing --type-override refused on every restart
// (TRIGGER-WARM-RESUME-NO-PRIOR). The method set is the whole contract; the
// pipeline type-asserts it, so its spelling is pinned at compile time in
// pipeline/optional_dispatch_pin_test.go and end to end by
// pipeline.TestKeyCollationTriggerCDC_WarmResumeShapes.
func (r *CDCReader) EmitsSchemaBoundaries() {}

// CDCReaderEmitsSchemaBoundaries is the engine-level twin of
// [CDCReader.EmitsSchemaBoundaries], for decisions the pipeline makes before
// the reader is open — the Shape A coordination door, which refuses live
// cross-shard coordination for this engine because its boundaries would be
// forwarded to the consolidated target (TRIGGER-SHAPE-A-FORWARD). Pinned in
// pipeline/optional_dispatch_pin_test.go.
func (Engine) CDCReaderEmitsSchemaBoundaries() {}

// tableShapeFunc reads the source's shape of schema.table for changes the
// transaction txid (an xid8, as the change log records it) captured.
type tableShapeFunc func(ctx context.Context, schema, table string, txid int64) (*ir.Table, error)

// firstTouch is one reader session's record of the tables it has emitted a
// first-touch boundary for. Owned by the pump goroutine; no locking.
type firstTouch struct {
	shape   tableShapeFunc
	touched map[string]bool
}

// precede returns events with an [ir.SchemaSnapshot] ahead of the first
// change of each table this session has not touched yet. txids parallels
// events. A failed shape read is returned and nothing of the batch is: the
// alternative is applying the table's changes with the guard unarmed.
func (f *firstTouch) precede(ctx context.Context, events []ir.Change, txids []int64) ([]ir.Change, error) {
	if len(events) != len(txids) {
		return nil, fmt.Errorf("first-touch schema boundary: %d changes but %d txids (a decode bug)", len(events), len(txids))
	}
	var out []ir.Change
	for i, ev := range events {
		schema, table, ok := changeTable(ev)
		key := schema + "." + table
		if !ok || f.touched[key] {
			if out != nil {
				out = append(out, ev)
			}
			continue
		}
		anchor, err := anchorBefore(ev.Pos())
		if err != nil {
			return nil, err
		}
		if f.shape == nil {
			return nil, fmt.Errorf("first-touch schema boundary of %s.%s: the reader has no shape reader (built without openCDCReader)", schema, table)
		}
		tbl, err := f.shape(ctx, schema, table, txids[i])
		if err != nil {
			return nil, fmt.Errorf("first-touch schema boundary of %s.%s: %w", schema, table, err)
		}
		if out == nil {
			out = append(make([]ir.Change, 0, len(events)+1), events[:i]...)
		}
		out = append(out, ir.SchemaSnapshot{Position: anchor, Schema: schema, Table: table, IR: tbl}, ev)
		if f.touched == nil {
			f.touched = map[string]bool{}
		}
		f.touched[key] = true
	}
	if out == nil {
		return events, nil
	}
	return out, nil
}

// changeTable is the table a change names, for the changes this reader
// emits.
func changeTable(c ir.Change) (schema, table string, ok bool) {
	switch v := c.(type) {
	case ir.Insert:
		return v.Schema, v.Table, true
	case ir.Update:
		return v.Schema, v.Table, true
	case ir.Delete:
		return v.Schema, v.Table, true
	case ir.Truncate:
		return v.Schema, v.Table, true
	}
	return "", "", false
}

// anchorBefore is the position one change-log id below p: the boundary's
// anchor (see the file comment).
func anchorBefore(p ir.Position) (ir.Position, error) {
	pos, ok, err := decodePos(p)
	if err != nil {
		return ir.Position{}, err
	}
	if !ok || pos.LastID < 1 {
		return ir.Position{}, fmt.Errorf("first-touch schema boundary: change position %+v names no change-log id", p)
	}
	return encodePos(pgTriggerPos{LastID: pos.LastID - 1})
}

// readTableShape is the production [tableShapeFunc]: the schema reader's shape
// of the table, with each column's collation stamped as of the capturing
// transaction.
//
// The cost, measured (postgres:16, one container): a schema read costs about
// 60 ms on a near-empty schema and about 400 ms beside 500 other tables,
// views and sequences, because the reader's view and sequence passes walk the
// whole schema whatever the table scope. Read per table, a stream over 100 of
// those tables would spend about 40 s of pump time on first touches; read as
// below, the same 100 first touches took 1.36 s in all (13.6 ms each, the
// schema read included; measured with a throwaway probe, not a CI test).
// So the shapes are read ONCE per
// schema per session, scoped to the tables this install captures there
// ([capturedTableNames]) that this stream reads (the pipeline's scope
// predicate, when one is set), and each table's collations — the
// per-row-transaction half — are still read at its own first touch. Setup
// does NOT vet everything the schema reader refuses (an extension-function
// DEFAULT, an extension type: the first cut's comment claimed it did, and the
// re-review found one out-of-scope table failing every first touch), so a
// failed per-schema read falls back to reading each table on its own
// (FIRST-TOUCH-BATCH-SCOPE). A table missing from the cached read is read on
// its own.
//
// The per-schema read is as of the session's first touch of the schema, not
// of each table, and that is sound only where a later DDL that matters to the
// shape leaves a marker. On the event-trigger tier the capture fires on ALTER
// TABLE, CREATE TABLE, CREATE INDEX and a dropped table (setup.go): an added
// column, key, unique index or collation halts the stream at its marker
// before any change after it. It does NOT fire on DROP INDEX, ALTER TYPE …
// ADD/RENAME VALUE or ALTER DOMAIN, so those leave the cached shape stale for
// the session. A stale shape cannot disarm the key-collation guard: both
// target guards take their keys from the TARGET's catalog and only the column
// collations from the shape. That is not the same as saying a source DROP
// INDEX is safe, and it is NOT: the target keeps the dropped key, so two rows
// the source now allows under it (even byte-identical ones) meet on the
// target, where an upsert through that key overwrites the first row at exit 0
// — a pre-existing hole on every CDC source (DROP INDEX is invisible to all
// of them), not caused by this boundary, and filed separately. An enum label
// added on the source reaches the target exactly as it did before this
// boundary existed (the boundary never carried it); a domain CHECK holds no
// data. On the polled-fingerprint tier no DDL leaves a marker, so there the
// shape is read per table ([CDCReader.shapePerTable],
// FIRST-TOUCH-POLLED-TIER-STALE: the per-schema read described a table without
// a UNIQUE key added after the schema's first touch — which, measured, did not
// unarm the guard, for the reason above, but left the recorded shape wrong);
// a change made after a table's own first touch is still not seen until the
// next session, as for collations.
func (r *CDCReader) readTableShape(ctx context.Context, schema, table string, txid int64) (*ir.Table, error) {
	tbl, err := r.cachedShape(ctx, schema, table)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		return nil, fmt.Errorf("the source catalog has no table %s.%s, which these captured changes name — a DDL "+
			"(DROP, RENAME, SET SCHEMA) ran after they were captured, and the stream refuses to apply them with nothing to "+
			"guard their keys by; drain the stream and run the recovery the observed-DDL refusal names", schema, table)
	}
	colls, err := postgres.ColumnCollationsAsOf(ctx, r.db, schema, table, xidOf(txid))
	if err != nil {
		return nil, err
	}
	for _, c := range tbl.Columns {
		if c == nil {
			continue
		}
		if cc, ok := colls[c.Name]; ok {
			c.Type = postgres.StampColumnCollation(c.Type, cc)
		}
	}
	return tbl, nil
}

// cachedShape returns the schema reader's shape of schema.table from the
// session's per-schema read, reading it first if this is the schema's first
// touch, and reading the table alone if the cached read lacks it. nil when the
// catalog has no such table.
func (r *CDCReader) cachedShape(ctx context.Context, schema, table string) (*ir.Table, error) {
	tables, ok := r.shapes[schema]
	if !ok && r.shapePerTable {
		// The polled-fingerprint tier (FIRST-TOUCH-POLLED-TIER-STALE): DDL
		// leaves no marker there, so a key added to this table after the
		// schema's first touch would be judged from the older read. Pay the
		// per-table cost instead.
		tables, ok = map[string]*ir.Table{}, true
	}
	if !ok {
		names, err := capturedTableNames(ctx, r.db, r.schema, schema)
		if err != nil {
			return nil, fmt.Errorf("list the captured tables of %s: %w", schema, err)
		}
		inScope := func(name string) bool {
			if name == table {
				return true
			}
			return names[name] && (r.scopeAllowed == nil || r.scopeAllowed(schema, name))
		}
		if tables, err = r.readShapes(ctx, schema, inScope); err != nil {
			// FIRST-TOUCH-BATCH-SCOPE: setup does not vet everything the
			// schema reader refuses (an extension-function DEFAULT, an
			// extension type), so one captured table this stream does not
			// read must not fail the shapes of the tables it does. Read
			// each table on its own for the rest of the session; the
			// touched table's own read below still fails loudly if IT is
			// the one the reader refuses.
			slog.InfoContext(ctx, "pgtrigger: first-touch schema boundary: the per-schema shape read failed; reading each table on its own",
				slog.String("schema", schema), slog.Any("err", err))
			tables = map[string]*ir.Table{}
		}
		if r.shapes == nil {
			r.shapes = map[string]map[string]*ir.Table{}
		}
		r.shapes[schema] = tables
	}
	if tbl := tables[table]; tbl != nil {
		delete(tables, table) // handed out once: each table is touched once per session
		return tbl, nil
	}
	one, err := r.readShapes(ctx, schema, func(name string) bool { return name == table })
	if err != nil {
		return nil, err
	}
	return one[table], nil
}

// readShapes reads the tables of schema the scope admits, keyed by name.
func (r *CDCReader) readShapes(ctx context.Context, schema string, scope func(string) bool) (map[string]*ir.Table, error) {
	sr, err := r.openShapeReader(ctx)
	if err != nil {
		return nil, err
	}
	sr.SetSchema(schema)
	sr.SetTableScope(scope)
	s, err := sr.ReadSchema(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]*ir.Table{}
	for _, t := range s.Tables {
		if t != nil {
			out[t.Name] = t
		}
	}
	return out, nil
}

// capturedTableNames lists the tables of tableSchema that carry this install's
// capture trigger (the install's functions live in installSchema), the scope
// of [CDCReader.cachedShape]'s per-schema read.
func capturedTableNames(ctx context.Context, db *sql.DB, installSchema, tableSchema string) (map[string]bool, error) {
	const q = `
SELECT c.relname
  FROM pg_catalog.pg_trigger tg
  JOIN pg_catalog.pg_proc p       ON p.oid = tg.tgfoid
  JOIN pg_catalog.pg_namespace fn ON fn.oid = p.pronamespace
  JOIN pg_catalog.pg_class c      ON c.oid = tg.tgrelid
  JOIN pg_catalog.pg_namespace tn ON tn.oid = c.relnamespace
 WHERE fn.nspname = $1
   AND tn.nspname = $2
   AND tg.tgname = $3
   AND NOT tg.tgisinternal`
	rows, err := db.QueryContext(ctx, q, installSchema, tableSchema, CaptureTriggerRow)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	names := map[string]bool{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names[n] = true
	}
	return names, rows.Err()
}

// openShapeReader opens the schema reader [CDCReader.readTableShape] reads
// through, once per session; Close releases it after the pump has stopped.
func (r *CDCReader) openShapeReader(ctx context.Context) (*postgres.SchemaReader, error) {
	if r.shapeReader != nil {
		return r.shapeReader, nil
	}
	sr, err := postgres.Engine{}.WithConnectionLabel(r.appID).OpenSchemaReader(ctx, r.dsn)
	if err != nil {
		return nil, fmt.Errorf("open the schema reader: %w", err)
	}
	pgsr, ok := sr.(*postgres.SchemaReader)
	if !ok {
		if c, isCloser := sr.(interface{ Close() error }); isCloser {
			_ = c.Close()
		}
		return nil, errors.New("open the schema reader: the composed postgres engine did not return a *postgres.SchemaReader")
	}
	r.shapeReader = pgsr
	return pgsr, nil
}

// xidOf is the 32-bit xid of an xid8 (the epoch is the high 32 bits), the
// form pg_attribute.xmin is compared in; 0 (nothing compared) for none.
func xidOf(txid int64) uint32 {
	if txid <= 0 {
		return 0
	}
	return uint32(txid & 0xFFFFFFFF) //nolint:gosec // masked to 32 bits: the low word of an xid8 IS the xid
}
