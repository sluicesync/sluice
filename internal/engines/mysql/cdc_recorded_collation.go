// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
)

// # A row's collation is the one its event recorded
// (KEY-COLLATION-REVERTED-WINDOW-RESTORE, KEY-COLLATION-LOOSENED-WINDOW)
//
// A reader behind the binlog head reads each table's definition from the
// catalog, which answers as of NOW. A key column whose collation changed
// between the reader's position and the head reads as if every row had been
// written under the current collation, and the shape the stream reports —
// its schema boundaries and the schema history a backup records — says so.
// Two measured harms (MySQL 8.0 and 8.4, MINIMAL and FULL, a `backup
// incremental` or a warm-resumed `sync start` read after the change):
//
//   - changed and changed BACK (`_ci` → `_bin`, 'A' beside 'a', 'A' moved
//     away, back to `_ci`): the `_bin` shape was never reported at all, and
//     'A' replayed onto row 1's 'a' — target row 1 held 'A', or was gone
//     with the key as the primary key, at exit 0;
//   - LOOSENED (`_bin` → `_ci` after 'A' beside 'a' moved away): the table's
//     first boundary after a resume carried the catalog's `_ci` (GC-44 F12),
//     the target key was loosened before 'A' arrived, and 'A' merged.
//
// The TABLE_MAP before every rows event records each character column's
// collation ID — under the default binlog_row_metadata=MINIMAL as well as
// FULL. So the shape a rows event is decoded and reported under carries the
// RECORDED collation (and its character set) wherever the two differ
// ([CDCReader.stampRecordedCollations]): the event's own statement of what
// it was written under, at its position. The boundaries the stream already
// emits — the first touch of a table, the first row after a DDL — then carry
// the shape in force at that row, so a sync forwards (or, under
// `--schema-changes=refuse`, refuses) the change before the rows written
// under it, and a backup's schema history records it for chain restore to
// guard those rows under. Decoding is unaffected: character values are
// decoded by the TABLE_MAP's character set already ([binlogColumnCharsets]).
//
// Not covered: a collation ID the source's collation table does not name
// (the shape keeps the catalog's; the DEFERRED-KEY-COLLATION hold refuses it
// where it can see it), a source that records no collation (MariaDB's
// default binlog_row_metadata=NO_LOG — its in-place column changes ahead of
// the reader refuse in [retypedColumnUnverified]), and VStream, whose events
// carry no collation (F17's named VStream residual).
//
// UNVERIFIED PREMISE: a collation change on the source is a DDL, so the row
// after it is a table's first row after a DDL, where the boundary is
// emitted. A shape stamped differently from the last one reported without an
// intervening DDL or first touch is not emitted as a boundary of its own.

// recordedShapeAtEvent is [CDCReader.shapeAtEvent] with the event's recorded
// collations stamped on ([CDCReader.stampRecordedCollations]).
func (r *CDCReader) recordedShapeAtEvent(ctx context.Context, qn string, catalog *tableSchema, ev *replication.RowsEvent) (*tableSchema, error) {
	shape, err := r.shapeAtEvent(ctx, qn, catalog, ev)
	if err != nil {
		return nil, err
	}
	return r.stampRecordedCollations(ctx, shape, ev.Table), nil
}

// stampRecordedCollations returns tbl with each character column's
// collation, and character set, replaced by the ones its rows event
// recorded where they differ from tbl's; tbl itself when none differ.
func (r *CDCReader) stampRecordedCollations(ctx context.Context, tbl *tableSchema, tm *replication.TableMapEvent) *tableSchema {
	if tm == nil || len(tm.ColumnType) != len(tbl.Columns) {
		return tbl
	}
	ids := tm.CollationMap()
	if len(ids) == 0 {
		return tbl
	}
	nameOf, charsetOf := r.collationIDNames(ctx), r.collationCharsets(ctx)
	var stamped *tableSchema
	for i, col := range tbl.Columns {
		id, ok := ids[i]
		if !ok || id == binaryCollationID || characterCollation(col.Type) == "" {
			continue
		}
		name := strings.ToLower(nameOf(id))
		if name == "" || strings.EqualFold(name, characterCollation(col.Type)) {
			continue
		}
		typ, ok := withCollation(col.Type, canonicalCharsetName(charsetOf(id)), name)
		if !ok {
			continue
		}
		if stamped == nil {
			cp := *tbl
			cp.Columns = append([]*ir.Column(nil), tbl.Columns...)
			stamped = &cp
		}
		c := *col
		c.Type = typ
		stamped.Columns[i] = &c
	}
	if stamped == nil {
		return tbl
	}
	return stamped
}

// withCollation is t with its character set and collation set; ok is false
// for a type that has none.
func withCollation(t ir.Type, charset, collation string) (ir.Type, bool) {
	switch v := t.(type) {
	case ir.Varchar:
		if charset != "" {
			v.Charset = charset
		}
		v.Collation = collation
		return v, true
	case ir.Char:
		if charset != "" {
			v.Charset = charset
		}
		v.Collation = collation
		return v, true
	case ir.Text:
		if charset != "" {
			v.Charset = charset
		}
		v.Collation = collation
		return v, true
	}
	return t, false
}
