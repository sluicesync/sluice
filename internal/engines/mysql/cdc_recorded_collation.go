// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
)

// # A collation the stream will never record (KEY-COLLATION-REVERTED-WINDOW-RESTORE)
//
// A reader behind the binlog head reads each table's definition from the
// catalog, which answers as of NOW. A column whose collation was changed and
// changed BACK between the reader's position and the head reads as if it had
// never changed: `MODIFY k … COLLATE utf8mb4_bin`, a row 'A' beside the
// existing 'a', the row moved out of the way, `MODIFY k … COLLATE
// utf8mb4_0900_ai_ci`. The look-ahead (cdc_ddl_lookahead.go) proves the
// column LIST, and the decode is byte-exact, so nothing refused; the shape the
// rows were decoded under — and the schema history the stream recorded — said
// `_ci` throughout. A restore armed its key-collation guard from that shape,
// equal to the target's, and replayed 'A' onto row 1's 'a': target row 1 held
// 'A' where the source holds 'a', at exit 0 (measured on MySQL 8.0, MINIMAL
// and FULL, `backup incremental` started after the change; a warm-resumed
// sync reads the binlog the same way).
//
// The TABLE_MAP before every rows event records each character column's
// collation ID — under the default binlog_row_metadata=MINIMAL as well as
// FULL. So the reader compares it, per character column, with two
// independent statements of what the stream knows:
//
//   - the shape it decodes the event with (the catalog's, or the event's own
//     names under FULL): equal means the stream's record is right;
//   - the collation the column's previous rows were recorded under in this
//     stream, or — for its first — the basis the pipeline handed over (the
//     schema the copy or the parent backup recorded, or the retained history
//     at a resume; [CDCReader.SetDecodedShapeBaseline]): equal means the rows
//     predate a change the catalog already shows, which the delta and the
//     boundary carry (a forward `_ci` → `_bin` replayed behind the head).
//
// A collation equal to NEITHER was in force only between two changes the
// stream cannot see, so no boundary will ever describe the shape these rows
// were written under; the reader refuses before decoding any of them
// (DEFERRED-SCHEMA-CHANGE). It refuses for any character column, not only a
// key: the reader does not know the table's unique keys, and a collation that
// existed only inside a stretch of binlog sluice cannot describe is a schema
// the stream would otherwise report wrongly.
//
// Not covered: a collation ID the source's collation table does not name
// (no comparison is possible; the DEFERRED-KEY-COLLATION hold refuses that
// case for the columns it sees), a source that records no collation
// (MariaDB's default binlog_row_metadata=NO_LOG — its in-place column changes
// ahead of the reader already refuse in [retypedColumnUnverified]), and a
// column with no basis (no baseline was handed over, or the table is not in
// it).

// holdDecodedShape holds a rows event's shape before any row of it is
// decoded: its recorded collations ([CDCReader.holdRecordedCollations])
// against the basis, then its order and types
// ([CDCReader.holdDecodedOrder]), which makes it the next basis — so the
// collation hold must run first.
func (r *CDCReader) holdDecodedShape(ctx context.Context, qn string, tbl *tableSchema, tm *replication.TableMapEvent) error {
	if err := r.holdRecordedCollations(ctx, qn, tbl, tm); err != nil {
		return err
	}
	return r.holdDecodedOrder(qn, tbl)
}

// holdRecordedCollations refuses a rows event of qn whose TABLE_MAP records,
// for a character column of tbl, a collation that neither tbl nor the column's
// last recorded (or baseline) collation declares; otherwise it records each
// column's collation for the next event.
func (r *CDCReader) holdRecordedCollations(ctx context.Context, qn string, tbl *tableSchema, tm *replication.TableMapEvent) error {
	if tm == nil || len(tm.ColumnType) != len(tbl.Columns) {
		return nil
	}
	ids := tm.CollationMap()
	if len(ids) == 0 {
		return nil
	}
	nameOf := r.collationIDNames(ctx)
	last := r.recordedCollations[qn]
	basis := r.decodedShapes[qn]
	for i, col := range tbl.Columns {
		declared := characterCollation(col.Type)
		id, ok := ids[i]
		if declared == "" || !ok || id == binaryCollationID {
			continue
		}
		recorded := strings.ToLower(nameOf(id))
		if recorded == "" {
			continue
		}
		if !strings.EqualFold(recorded, declared) {
			known := last[col.Name]
			if known == "" {
				known = characterCollation(basis.types[col.Name])
			}
			switch {
			case known == "" && r.decodedShapeBaselineSet:
				// Armed, and nothing says what the column was before: the
				// deferred-shape hold's no-basis posture (a warm resume of a
				// table with no retained history), loud rather than unchecked.
				return unrecordedCollationRefusal(qn, col.Name, recorded, declared, "")
			case known != "" && !strings.EqualFold(recorded, known):
				return unrecordedCollationRefusal(qn, col.Name, recorded, declared, known)
			}
		}
		if last == nil {
			if r.recordedCollations == nil {
				r.recordedCollations = map[string]map[string]string{}
			}
			last = map[string]string{}
			r.recordedCollations[qn] = last
		}
		last[col.Name] = recorded
	}
	return nil
}

// unrecordedCollationRefusal is the DEFERRED-SCHEMA-CHANGE refusal of
// [CDCReader.holdRecordedCollations]. known is "" when the stream holds no
// shape of the table from before this event.
func unrecordedCollationRefusal(qn, col, recorded, declared, known string) error {
	before := "the last collation this stream knew it by is " + known
	if known == "" {
		before = "this stream holds no shape of the table from before this event to show what it was"
	}
	return fmt.Errorf("mysql: cdc: %s: %w: column %q was written under collation %s at this binlog event, but the "+
		"table's definition as this stream reads it declares %s, and %s. The collation was changed, and changed "+
		"again, in a stretch of binlog whose schema the catalog no longer shows, so no schema boundary will record the shape these rows were written under, and on a key "+
		"column a target comparing under %s can merge rows the source keeps apart. Refusing before any row of it is "+
		"decoded. Re-copy the stream: `sluice sync start --reset-target-data --yes` with the stream's other flags "+
		"unchanged for a sync, or a fresh `backup full` for a backup chain",
		qn, ir.ErrDeferredSchemaChange, col, recorded, declared, before, declared)
}
