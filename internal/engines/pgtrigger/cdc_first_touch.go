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
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/ir"
)

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

// readTableShape is the production [tableShapeFunc]: the schema reader's read
// of the one table, with each column's collation stamped as of the capturing
// transaction.
func (r *CDCReader) readTableShape(ctx context.Context, schema, table string, txid int64) (*ir.Table, error) {
	sr, err := r.openShapeReader(ctx)
	if err != nil {
		return nil, err
	}
	sr.SetSchema(schema)
	sr.SetTableScope(func(name string) bool { return name == table })
	s, err := sr.ReadSchema(ctx)
	if err != nil {
		return nil, err
	}
	var tbl *ir.Table
	for _, t := range s.Tables {
		if t != nil && t.Name == table {
			tbl = t
			break
		}
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
