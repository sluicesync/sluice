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
// A collation ID the reader cannot name refuses, retryably
// ([CDCReader.refuseUnnamedCollations], COLLATION-NAMES-UNAVAILABLE). Not
// covered: a source that records no collation (MariaDB's
// default binlog_row_metadata=NO_LOG — its in-place column changes ahead of
// the reader refuse in [retypedColumnUnverified]), and VStream, whose events
// carry no collation (F17's named VStream residual).
//
// The boundary placement rests on a premise, now CHECKED rather than
// assumed: a collation change on the source is a DDL the binlog carries, so
// the row after it is the table's first row after a DDL, where the boundary
// is emitted. A collation change with no logged DDL (sql_log_bin=0) refuses
// instead of decoding rows under a shape no boundary announces
// ([CDCReader.holdUnannouncedCollation]). ENUM and SET columns are not
// stamped: their values travel as label indexes, so a collation change does
// not alter which values the row image carries, and their labels are
// compared separately (recordedLabelsAgree).

// recordedShapeAtEvent is [CDCReader.shapeAtEvent] with the event's recorded
// collations stamped on ([CDCReader.stampRecordedCollations]).
func (r *CDCReader) recordedShapeAtEvent(ctx context.Context, qn string, catalog *tableSchema, ev *replication.RowsEvent) (*tableSchema, error) {
	shape, err := r.shapeAtEvent(ctx, qn, catalog, ev)
	if err != nil {
		return nil, err
	}
	if err := r.refuseUnnamedCollations(ctx, qn, shape, ev.Table); err != nil {
		return nil, err
	}
	shape = r.stampRecordedCollations(ctx, shape, ev.Table)
	if err := r.holdUnannouncedCollation(qn, shape); err != nil {
		return nil, err
	}
	return shape, nil
}

// holdUnannouncedCollation turns this file's premise into a check: a
// character column's collation may change from the shape this stream last
// decoded the table with only where a boundary will carry the change — a
// logged DDL that can reach THIS table ([ddlGenerations]; one that cannot be
// bounded to named tables counts for every table) was read since that
// shape, or this is its first row of an armed stream. Anywhere else the
// stream would decode rows under a collation no boundary announces: a DDL
// run with sql_log_bin=0, which reaches the catalog and the TABLE_MAP but
// writes no QUERY event. It refuses DEFERRED-SCHEMA-CHANGE before any row of
// it is decoded (review of d6ecb939, finding 2). Scoped like the reader's
// other refusals: an excluded table's rows reach no target. A column the
// basis declares no collation for, or a table with no basis, is not
// compared. A basis still SEEDED from the baseline owes a boundary instead
// of refusing (third review of a0878c3c; see the loop).
func (r *CDCReader) holdUnannouncedCollation(qn string, shape *tableSchema) error {
	if r.scopeAllowed != nil && !r.tableInScope(qn) {
		return nil
	}
	// Not the owed-boundary set: every logged DDL marks every table the
	// reader knows as owing one (the cache clear is blanket), so an ALTER of
	// another table would silence the check for this one — measured, the
	// sql_log_bin=0 change after an unrelated ALTER passed.
	if r.firstTouch.pending(qn) {
		return nil
	}
	basis, ok := r.decodedShapes[qn]
	if !ok {
		return nil
	}
	// A logged DDL that can have changed THIS table was read since the basis
	// was taken: the change may be its, and the post-DDL boundary carries it
	// (a table the reader had not cached when the DDL arrived owes no
	// boundary by name, yet gets one at its first rebuild). Scoped per table
	// so a routine ALTER of another table does not silence the check; a DDL
	// that cannot be attributed to named tables counts for every table.
	if r.ddlGen.at(qn) != r.basisDDLs[qn] {
		return nil
	}
	for _, c := range shape.Columns {
		now, was := characterCollation(c.Type), characterCollation(basis.types[c.Name])
		if now == "" || was == "" || strings.EqualFold(now, was) {
			continue
		}
		// A type still seeded from the baseline is a catalog read, not a
		// shape this stream observed: a parent manifest's schema is read at
		// window close and can already show a LOGGED change the stream has
		// yet to reach, so refusing on it refused rows written before that
		// DDL, on every restart (third review of a0878c3c). Neither reading
		// can be told apart here, so the row's recorded shape becomes a
		// boundary at the stream's start — what a first touch does — and
		// the history carries it either way: a later logged DDL is its own
		// boundary, and an unlogged change reverted before the table's first
		// row leaves a version recording the collation the rows were
		// written under, so chain restore guards them under it (measured:
		// KEY-COLLATION-MERGE, nothing merged —
		// TestChainRestore_UnloggedRevertedKeyCollationBeforeTheWindowsFirstRow;
		// skipping the seeded basis instead restored them merged). Where no
		// boundary can be emitted (a shape that is never a boundary, or no
		// start anchor), it refuses as before.
		if basis.seeded[c.Name] && !shape.synthesized && r.seededBoundaries.owe(qn) {
			continue
		}
		return fmt.Errorf("mysql: cdc: %s: %w: %s: column %q is decoded under collation %s at this binlog event, "+
			"and the last shape this stream decoded the table with declared %s, but no schema change of the table "+
			"was logged in between — a DDL run with sql_log_bin=0 changes the catalog and the row images without "+
			"writing the statement to the binlog — so no schema boundary will carry the change ahead of these rows, "+
			"and on a key a target comparing under %s can merge rows the source keeps apart. Refusing before any row "+
			"of it is decoded. Re-copy the stream: `sluice sync start --reset-target-data --yes` with the stream's "+
			"other flags unchanged for a sync, or a fresh `backup full` for a backup chain",
			qn, ir.ErrDeferredSchemaChange, unannouncedCollationMarker, c.Name, now, was, was)
	}
	return nil
}

// noteBasisAt records the DDL count at which qn's basis was set.
func (r *CDCReader) noteBasisAt(qn string) {
	if r.basisDDLs == nil {
		r.basisDDLs = map[string]int64{}
	}
	r.basisDDLs[qn] = r.ddlGen.at(qn)
}

// ddlGenerations counts logged DDLs per table they can reach
// ([classifyDDLReach]): per qualified table, per database (DROP DATABASE),
// and for every table at once when a statement cannot be bounded to named
// tables. A neutral statement (an index, a table option) reaches none.
type ddlGenerations struct {
	all    int64
	tables map[string]int64
	dbs    map[string]int64
}

// note counts one logged DDL.
func (g *ddlGenerations) note(reach ddlReach) {
	switch {
	case reach.neutral:
		return
	case reach.all:
		g.all++
		return
	}
	if g.tables == nil {
		g.tables, g.dbs = map[string]int64{}, map[string]int64{}
	}
	for _, t := range reach.tables {
		g.tables[strings.ToLower(qualifiedName(t.schema, t.table))]++
	}
	for _, db := range reach.databases {
		g.dbs[strings.ToLower(db)]++
	}
}

// at is qn's generation: it changes whenever a DDL that can reach qn is
// noted.
func (g *ddlGenerations) at(qn string) int64 {
	schema, _ := splitQualified(qn)
	return g.all + g.tables[strings.ToLower(qn)] + g.dbs[strings.ToLower(schema)]
}

// unannouncedCollationMarker names [CDCReader.holdUnannouncedCollation]'s
// refusal.
const unannouncedCollationMarker = "COLLATION-CHANGE-UNANNOUNCED"

// refuseUnnamedCollations refuses, retryably, a rows event that records a
// collation ID for a character column the reader cannot name — the source's
// collation-name table could not be read, or was read and lacks the ID.
// Without a name nothing is stamped, and the reverted and loosened windows
// would merge again (review of d6ecb939, finding 3). Every character column,
// not only the primary key: the reader does not know the table's UNIQUE
// keys, and a refusal that a restart retries costs nothing on a column that
// was never at risk (third review of a0878c3c, finding 2). The read is
// retried every [serverCollationsRetry]. A reader with no database handle
// and no loaded table (a test seam) is exempt.
func (r *CDCReader) refuseUnnamedCollations(ctx context.Context, qn string, shape *tableSchema, tm *replication.TableMapEvent) error {
	if tm == nil || len(tm.ColumnType) != len(shape.Columns) {
		return nil
	}
	ids := tm.CollationMap()
	if len(ids) == 0 {
		return nil
	}
	// The name table loads lazily, on the first use; a check that ran before
	// that first load mistook "not loaded yet" for "unreadable" and refused
	// every stream's first row (measured in the MySQL integration suite).
	if r.db != nil {
		r.collationIDNames(ctx)
	}
	names := r.serverCollationIDNames
	if names == nil && r.db == nil {
		return nil
	}
	for i, col := range shape.Columns {
		id, recorded := ids[i]
		if !recorded || id == binaryCollationID || characterCollation(col.Type) == "" {
			continue
		}
		why := "could not be read to name it"
		if names != nil {
			if names[id] != "" {
				continue
			}
			why = "was read and does not name it"
		}
		return fmt.Errorf("mysql: cdc: %s: %s: character column %q was written under collation ID %d, and the source's "+
			"collation table (information_schema.COLLATIONS) %s, so the collation the row was written under cannot "+
			"be carried to the target. Refusing; starting the stream again retries the read",
			qn, collationNamesUnavailableMarker, col.Name, id, why)
	}
	return nil
}

// collationNamesUnavailableMarker names [CDCReader.refuseUnnamedCollations]'s
// refusal.
const collationNamesUnavailableMarker = "COLLATION-NAMES-UNAVAILABLE"

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
		// An unnamed ID never reaches here from a rows event:
		// refuseUnnamedCollations refused it first.
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

// seededCollationBoundaries are the boundaries owed by tables whose first
// observed row recorded a collation the seeded baseline does not declare
// ([CDCReader.holdUnannouncedCollation]). The zero value owes none and has
// no anchor, so it refuses instead.
type seededCollationBoundaries struct {
	anchor   ir.Position
	anchored bool
	due      map[string]struct{}
}

// start records the stream's start position, when one resolved, and clears
// the owed set. Called once per StreamChanges.
func (s *seededCollationBoundaries) start(anchor ir.Position, ok bool) {
	s.anchor, s.anchored, s.due = anchor, ok, map[string]struct{}{}
}

// owe marks qn's boundary owed; false when no anchor resolved.
func (s *seededCollationBoundaries) owe(qn string) bool {
	if !s.anchored {
		return false
	}
	s.due[qn] = struct{}{}
	return true
}

// take reports whether qn owes the boundary, and clears it.
func (s *seededCollationBoundaries) take(qn string) bool {
	if _, ok := s.due[qn]; !ok {
		return false
	}
	delete(s.due, qn)
	return true
}
