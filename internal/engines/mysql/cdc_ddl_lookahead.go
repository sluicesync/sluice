// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # The catalog answers as of NOW; a row event was recorded THEN (Bug 301)
//
// The binlog reader decodes a row event against a column list it reads from
// information_schema when its cache is cold. Its cache is cold at the start
// of every stream (a backup window, a warm resume, the CDC phase after a cold
// copy) and again after every in-scope DDL clears it. The catalog read
// reflects every DDL the server has COMMITTED — including any the reader has
// not reached yet. A reader that is behind the binlog head (every backup
// incremental is, by the length of its window; a resumed sync is, by its
// downtime; a live sync is, by its lag) then decodes events recorded under
// one shape against a later one.
//
// The TABLE_MAP guard (cdc_table_map_guard.go) catches the reshape that
// moves the column count or a column's type family. The one it cannot see
// is the dangerous one: a reorder of two same-type columns, a rename swap, a
// drop-and-add of the same type. Those decoded cleanly and landed every
// value under the wrong column name, at exit 0 — measured on mysql:8.4 under
// `backup incremental`, a warm-resumed `sync start`, and a LIVE `sync start`
// a few hundred rows behind (Bug 301).
//
// # The proof
//
// When the source records column names (binlog_row_metadata=FULL) the event
// itself says what it was written under, and cdc_recorded_shape.go decodes
// by it. Without names, sluice proves the catalog shape instead:
//
//  1. tableFor captures the server's binlog coordinate immediately AFTER the
//     catalog read. Every DDL that read could have seen is committed, and
//     therefore lies at or before that coordinate in the binlog.
//  2. Before any row of the table is decoded, the reader classifies every
//     QUERY event between its own position and that coordinate
//     (SHOW BINLOG EVENTS) with [classifyDDLReach]. None that reshapes the
//     table means the catalog shape IS the shape at the reader's position.
//     One that does means the catalog is ahead, and the reader refuses —
//     before the first row is decoded, so nothing wrong is emitted.
//
// The expected value is independent of the thing being checked: the
// catalog says what the table is now, the binlog's own DDL record says what
// happened in between, and neither is derived from the other.
//
// # Cost
//
// The scan reads the binlog between the reader and the head, once: a
// scanned-through marker means a later proof scans only what lies past the
// last one. A reader at the head scans nothing. Under FULL a rows event
// carries its own names and needs no scan, except for a table holding a
// generated column or a type the binlog does not describe
// (cdc_recorded_shape.go); the rotation boundary's settle
// (cdc_owed_schema_boundary.go) has no rows event to read and proves its
// shapes by the scan whatever the metadata.
//
// # Premise
//
// UNVERIFIED PREMISE: every DDL is written to the binlog. A session with
// sql_log_bin=0 can reshape a table invisibly to every binlog consumer —
// sluice included, and not only here — and this proof cannot see it.
//
// UNVERIFIED PREMISE: anything visible in the catalog lies at or before the
// position SHOW BINARY LOG STATUS reports right after the read. The
// reasoning: both servers write a transaction's binlog events in the
// commit's flush stage, BEFORE the storage engine commits and the data
// dictionary change becomes visible, and the status position is the end of
// what has been flushed. No test makes a server violate it; if one did, a
// DDL just past the reported position could be in the catalog and outside
// the scan.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	sqldriver "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// binlogCoord is a (file, offset) position in the source's binlog, comparable
// across rotations. The zero value is "unknown".
type binlogCoord struct {
	file string
	pos  uint32
}

// isZero reports whether the coordinate is unknown.
func (c binlogCoord) isZero() bool { return c.file == "" }

// orderedWith reports whether c and o can be ordered: the same file, or two
// files of one binlog basename with numeric suffixes. Anything else (a
// basename change, a name with no sequence) has no order the proof can
// trust, and it fails closed on that.
func (c binlogCoord) orderedWith(o binlogCoord) bool {
	if c.file == o.file {
		return true
	}
	cBase, _, cOK := splitBinlogName(c.file)
	oBase, _, oOK := splitBinlogName(o.file)
	return cOK && oOK && cBase == oBase
}

// after reports whether c lies strictly after o.
func (c binlogCoord) after(o binlogCoord) bool {
	if c.file != o.file {
		return binlogFileAfter(c.file, o.file)
	}
	return c.pos > o.pos
}

func (c binlogCoord) String() string { return fmt.Sprintf("%s:%d", c.file, c.pos) }

// binlogEventRow is one row of SHOW BINLOG EVENTS.
type binlogEventRow struct {
	pos, end uint32
	typ      string
	info     string
}

// aheadDDL is a schema-changing QUERY event found by the look-ahead.
type aheadDDL struct {
	at    binlogCoord
	stmt  string
	reach ddlReach
}

// ddlLookahead is the reader's record of the binlog it has classified ahead
// of its own position. Pump goroutine only.
type ddlLookahead struct {
	// through is the coordinate classification has reached: every event
	// starting before it, at or after the reader's position when it was
	// scanned, is in ddls (if it can reshape a table).
	through binlogCoord
	// ddls are the non-neutral DDL events found, in binlog order.
	ddls []aheadDDL

	// listEvents and listFiles are test seams; nil means the live
	// SHOW BINLOG EVENTS / SHOW BINARY LOGS queries.
	listEvents func(ctx context.Context, file string, from uint32, limit int) ([]binlogEventRow, error)
	listFiles  func(ctx context.Context) ([]string, error)
	// shortReadWait is a test seam for the wait between short re-reads;
	// zero means lookaheadShortReadWait.
	shortReadWait time.Duration
}

// lookaheadPage is the SHOW BINLOG EVENTS page size. Each page is a direct
// seek to its FROM offset, so the scan is linear in the range it covers.
const lookaheadPage = 5000

// lookaheadShortReads and lookaheadShortReadWait bound how long a scan waits
// for the binlog to serve up to a position the server already reported.
const (
	lookaheadShortReads    = 100 // about ten seconds
	lookaheadShortReadWait = 100 * time.Millisecond
)

// scan classifies every event in [from, to) not already classified, then
// moves the marker to to. from is the reader's position: anything before it
// has been crossed and is forgotten.
func (l *ddlLookahead) scan(ctx context.Context, db *sql.DB, from, to binlogCoord) error {
	kept := l.ddls[:0]
	for _, d := range l.ddls {
		if !from.after(d.at) {
			kept = append(kept, d)
		}
	}
	l.ddls = kept
	start := from
	if l.through.after(from) {
		start = l.through
	}
	if !to.after(start) {
		return nil
	}
	listEvents, listFiles := l.listEvents, l.listFiles
	if listEvents == nil {
		listEvents = func(ctx context.Context, file string, from uint32, limit int) ([]binlogEventRow, error) {
			return showBinlogEvents(ctx, db, file, from, limit)
		}
	}
	if listFiles == nil {
		listFiles = func(ctx context.Context) ([]string, error) { return showBinaryLogs(ctx, db) }
	}
	var files []string
	shortReads := 0
	at := start
	for {
		rows, err := listEvents(ctx, at.file, at.pos, lookaheadPage)
		if err != nil {
			return errLookaheadPrivilege(fmt.Errorf("mysql: cdc: look-ahead SHOW BINLOG EVENTS IN %q FROM %d: %w", at.file, at.pos, err))
		}
		next := at.pos
		for _, ev := range rows {
			if at.file == to.file && ev.pos >= to.pos {
				l.through = to
				return nil
			}
			if d, ok := classifyBinlogEvent(ev); ok {
				l.ddls = append(l.ddls, aheadDDL{at: binlogCoord{at.file, ev.pos}, stmt: d.stmt, reach: d.reach})
			}
			// MariaDB 11.4+ writes End_log_pos 0 inside a transaction (see
			// readerCoord); an event's own start is then the furthest
			// boundary known, and paging from it re-reads one event.
			next = max(next, ev.end, ev.pos)
		}
		if len(rows) == lookaheadPage {
			if next <= at.pos {
				return fmt.Errorf("mysql: cdc: look-ahead made no progress in %s at %d", at.file, at.pos)
			}
			at.pos = next
			continue
		}
		// End of this file.
		if at.file == to.file {
			if next < to.pos {
				// The file ends before the coordinate the catalog read was
				// stamped with: SHOW BINARY LOG STATUS can run ahead of what
				// the binlog serves (sync_binlog, group commit). Never mark
				// unread binlog as classified — a later proof would skip a
				// DDL in it. Re-read from here a few times, then refuse.
				shortReads++
				if shortReads > lookaheadShortReads {
					return sluicecode.Wrap(sluicecode.CodeCDCBinlogLookaheadShort,
						"retry; if it recurs, check that the source flushes its binlog (sync_binlog, "+
							"binlog_group_commit_sync_delay) and serves SHOW BINLOG EVENTS up to the position it reports",
						fmt.Errorf("mysql: cdc: look-ahead could read %s only to offset %d, short of the "+
							"position the schema read was stamped with (%s); refusing to prove a shape over binlog "+
							"it could not read", at.file, next, to))
				}
				l.through, at.pos = binlogCoord{at.file, next}, next
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(l.shortReadDelay()):
				}
				continue
			}
			l.through = to
			return nil
		}
		if files == nil {
			if files, err = listFiles(ctx); err != nil {
				return errLookaheadPrivilege(fmt.Errorf("mysql: cdc: look-ahead SHOW BINARY LOGS: %w", err))
			}
		}
		nextFile := ""
		for _, f := range files {
			if binlogFileAfter(f, at.file) && (nextFile == "" || binlogFileAfter(nextFile, f)) {
				nextFile = f
			}
		}
		if nextFile == "" || binlogFileAfter(nextFile, to.file) {
			return fmt.Errorf("mysql: cdc: look-ahead found no binlog after %s on the way to %s", at.file, to)
		}
		at = binlogCoord{file: nextFile, pos: 4}
	}
}

// reaching returns the DDLs in [from, to) that can reshape schema.table, in
// binlog order. from is the reader's position (the start of the next event
// it will read); to is a server coordinate, the start of the next event the
// server will write — so a DDL the catalog read could see lies before it.
func (l *ddlLookahead) reaching(from, to binlogCoord, schema, table string) []aheadDDL {
	var out []aheadDDL
	for _, d := range l.ddls {
		if from.after(d.at) {
			continue
		}
		if !to.after(d.at) {
			break
		}
		if d.reach.reaches(schema, table) {
			out = append(out, d)
		}
	}
	return out
}

// nonDDLEventTypes are the SHOW BINLOG EVENTS Event_type values that cannot
// carry a schema change: transaction framing, GTID bookkeeping, row images
// and their table maps, rotation. The list is an ALLOWLIST and fails
// closed: an event type not on it — a MariaDB `Query_compressed` (DDL
// written under log_bin_compress=ON, measured on mariadb:11.4 hiding a
// reorder from a first cut that read only `Query`), or a type a newer
// server adds — is classified by its Info when it is a query, and reaches
// every table otherwise.
var nonDDLEventTypes = map[string]bool{
	"Format_desc": true, "Previous_gtids": true, "Gtid": true, "Anonymous_Gtid": true,
	"Gtid_tagged_log": true, "Gtid_list": true, "Binlog_checkpoint": true, "Start_encryption": true,
	"Table_map": true, "Rows_query": true, "Annotate_rows": true, "Transaction_payload": true,
	"Write_rows": true, "Update_rows": true, "Delete_rows": true, "Partial_update_rows": true,
	"Write_rows_v1": true, "Update_rows_v1": true, "Delete_rows_v1": true,
	"Write_rows_compressed_v1": true, "Update_rows_compressed_v1": true, "Delete_rows_compressed_v1": true,
	"Write_rows_compressed": true, "Update_rows_compressed": true, "Delete_rows_compressed": true,
	"Xid": true, "XA_prepare": true, "Intvar": true, "Rand": true, "User var": true,
	"Rotate": true, "Stop": true, "Heartbeat": true, "Heartbeat_log": true, "Heartbeat_log_v2": true,
	// Group Replication membership and certification bookkeeping.
	"View_change": true, "Transaction_context": true,
}

// classifyBinlogEvent classifies one SHOW BINLOG EVENTS row. ok is true for
// an event that can reshape a table.
func classifyBinlogEvent(ev binlogEventRow) (classifiedQuery, bool) {
	if nonDDLEventTypes[ev.typ] {
		return classifiedQuery{}, false
	}
	if strings.HasPrefix(ev.typ, "Query") {
		// `Query` and MariaDB's `Query_compressed`: SHOW BINLOG EVENTS shows
		// the statement text for both.
		return classifyQueryInfo(ev.info)
	}
	return classifiedQuery{stmt: ev.typ + " event", reach: ddlReach{all: true}}, true
}

// classifiedQuery is a QUERY event's statement and what it can reshape.
type classifiedQuery struct {
	stmt  string
	reach ddlReach
}

// classifyQueryInfo classifies a SHOW BINLOG EVENTS Info for a Query event.
// ok is false for statements that reshape nothing: transaction control the
// reader handles in its own arms (BEGIN, COMMIT, XA, SAVEPOINT) and anything
// [classifyDDLReach] finds neutral.
func classifyQueryInfo(info string) (classifiedQuery, bool) {
	db, stmt, ok := splitUseClause(info)
	if !ok {
		// A `use` prefix we cannot read: the statement may name any table.
		return classifiedQuery{stmt: info, reach: ddlReach{all: true}}, true
	}
	if isTransactionControl(stmt) {
		return classifiedQuery{}, false
	}
	reach := classifyDDLReach(stmt, db)
	if reach.neutral {
		return classifiedQuery{}, false
	}
	return classifiedQuery{stmt: stmt, reach: reach}, true
}

// isTransactionControl reports whether a QUERY event statement is one the
// reader's own dispatch treats as a transaction boundary rather than DDL.
func isTransactionControl(stmt string) bool {
	s := strings.TrimSpace(stripLeadingSQLComments(stmt))
	if s == "BEGIN" || s == "COMMIT" {
		return true
	}
	if _, isXA := xaStatementVerb(s); isXA {
		return true
	}
	u := strings.ToUpper(s)
	for _, p := range []string{"SAVEPOINT ", "RELEASE SAVEPOINT ", "ROLLBACK TO ", "ROLLBACK"} {
		if strings.HasPrefix(u, p) {
			return true
		}
	}
	return false
}

// splitUseClause splits the "use `db`; " prefix SHOW BINLOG EVENTS puts on a
// Query event run with a default database. ok is false when a `use` prefix
// is present but cannot be read.
func splitUseClause(info string) (db, stmt string, ok bool) {
	const prefix = "use `"
	if !strings.HasPrefix(info, prefix) {
		return "", info, true
	}
	var b strings.Builder
	for i := len(prefix); i < len(info); i++ {
		if info[i] != '`' {
			b.WriteByte(info[i])
			continue
		}
		if i+1 < len(info) && info[i+1] == '`' {
			b.WriteByte('`')
			i++
			continue
		}
		rest := info[i+1:]
		if !strings.HasPrefix(rest, "; ") {
			return "", "", false
		}
		return b.String(), rest[2:], true
	}
	return "", "", false
}

// showBinlogEvents reads one page of SHOW BINLOG EVENTS.
func showBinlogEvents(ctx context.Context, db *sql.DB, file string, from uint32, limit int) ([]binlogEventRow, error) {
	// SHOW BINLOG EVENTS takes no placeholders; the file name is quoted as
	// a string literal (it comes from the server's own binlog index).
	q := fmt.Sprintf("SHOW BINLOG EVENTS IN '%s' FROM %d LIMIT %d", strings.ReplaceAll(file, "'", "''"), from, limit)
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []binlogEventRow
	for rows.Next() {
		var (
			logName, typ string
			pos, end     uint32
			serverID     uint64
			info         sql.NullString
		)
		if err := rows.Scan(&logName, &pos, &typ, &serverID, &end, &info); err != nil {
			return nil, err
		}
		out = append(out, binlogEventRow{pos: pos, end: end, typ: typ, info: info.String})
	}
	return out, rows.Err()
}

// showBinaryLogs lists the source's binlog files.
func showBinaryLogs(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []string
	for rows.Next() {
		dest := make([]any, len(cols))
		holders := make([]any, len(cols))
		for i := range dest {
			holders[i] = &dest[i]
		}
		if err := rows.Scan(holders...); err != nil {
			return nil, err
		}
		if name, ok := scanString(dest[0]); ok {
			out = append(out, name)
		}
	}
	return out, rows.Err()
}

// noteEventOffset records the end offset an event carries, for readerCoord.
func (r *CDCReader) noteEventOffset(hdr *replication.EventHeader) {
	if hdr != nil && hdr.LogPos != 0 {
		r.lastLogPos = hdr.LogPos
	}
}

// readerCoord is the reader's own binlog coordinate while it dispatches the
// event hdr describes: the event's end offset, the start of the next event.
//
// MariaDB 11.4+ writes end_log_pos = 0 into the events INSIDE a transaction
// (binlog_legacy_event_pos=OFF, its default — measured on mariadb:11.4), so
// a rows event there carries no offset at all. The last offset an event did
// carry is then the coordinate: it lies at or before this event, and nothing
// between them is a DDL (a DDL is its own group and carries its offset), so
// the proof range it opens is never short of one the reader has not crossed.
func (r *CDCReader) readerCoord(hdr *replication.EventHeader) binlogCoord {
	pos := r.lastLogPos
	if hdr != nil && hdr.LogPos != 0 {
		pos = hdr.LogPos
	}
	return binlogCoord{file: r.currentFile, pos: pos}
}

// catalogCoord is the server's binlog coordinate, read immediately after a
// catalog read. The test seam r.catalogPosition overrides it.
func (r *CDCReader) catalogCoord(ctx context.Context) (binlogCoord, error) {
	if r.catalogPosition != nil {
		return r.catalogPosition(ctx)
	}
	file, pos, err := masterStatus(ctx, r.db)
	if err != nil {
		return binlogCoord{}, err
	}
	return binlogCoord{file: file, pos: pos}, nil
}

// proveCatalogShape proves that tbl — a shape read from the catalog at
// tbl.catalogAt while the reader stood at tbl.loadedAt — is the shape of the
// table at the reader's position, or refuses. Once proven, a shape stays
// proven until the cache drops it (the next in-scope DDL).
func (r *CDCReader) proveCatalogShape(ctx context.Context, tbl *tableSchema) error {
	d, ahead, err := r.catalogShapeAhead(ctx, tbl)
	if err != nil {
		return err
	}
	if ahead {
		return errCDCCatalogAheadOfPosition(tbl, d, r.flavor)
	}
	return nil
}

// catalogShapeAhead reports the first DDL that makes tbl's catalog shape
// ahead of the position it was loaded at, marking tbl proven when there is
// none.
func (r *CDCReader) catalogShapeAhead(ctx context.Context, tbl *tableSchema) (aheadDDL, bool, error) {
	if tbl.proven || (tbl.catalogAt.isZero() && tbl.loadedAt.isZero()) {
		tbl.proven = true
		return aheadDDL{}, false, nil
	}
	if tbl.loadedAt.isZero() || tbl.catalogAt.isZero() {
		return aheadDDL{}, false, errors.New("mysql: cdc: internal: a catalog shape was loaded with no reader position to prove it against")
	}
	if !tbl.catalogAt.orderedWith(tbl.loadedAt) {
		return aheadDDL{}, false, fmt.Errorf("mysql: cdc: %s.%s: the stream's binlog position (%s) and the position the "+
			"schema read was stamped with (%s) are in binlog files that cannot be ordered; refusing to prove the table's shape",
			tbl.Schema, tbl.Name, tbl.loadedAt, tbl.catalogAt)
	}
	if !tbl.catalogAt.after(tbl.loadedAt) {
		tbl.proven = true
		return aheadDDL{}, false, nil
	}
	if err := r.ddlAhead.scan(ctx, r.db, tbl.loadedAt, tbl.catalogAt); err != nil {
		return aheadDDL{}, false, err
	}
	var recheck retypedAhead
	for _, d := range r.ddlAhead.reaching(tbl.loadedAt, tbl.catalogAt, tbl.Schema, tbl.Name) {
		if !d.reach.inPlace {
			return d, true, nil
		}
		recheck.add(d)
	}
	tbl.proven, tbl.retypedAhead = true, recheck
	return aheadDDL{}, false, nil
}

// retypedAhead is what in-place DDLs between a catalog shape's load and its
// read changed: some columns' types, never a name or a position. The column
// list therefore holds at the reader's position, but those columns' catalog
// types may not — so every row event is checked against them
// ([CDCReader.recheckRetypedColumns]) before it is decoded.
type retypedAhead struct {
	first   *aheadDDL
	columns map[string]bool
	all     bool
}

func (a *retypedAhead) add(d aheadDDL) {
	if a.first == nil {
		a.first = &d
	}
	if d.reach.retypedAll {
		a.all = true
	}
	for _, c := range d.reach.retyped {
		if a.columns == nil {
			a.columns = map[string]bool{}
		}
		a.columns[strings.ToLower(c)] = true
	}
}

func (a retypedAhead) covers(column string) bool {
	return a.all || a.columns[strings.ToLower(column)]
}

// recheckRetypedColumns refuses a row event when a column an in-place DDL
// ahead of the reader retyped is not recorded as a type the catalog type
// decodes the same way. ENUM and SET refuse unless the event records their
// labels: a label list reordered in place keeps the type code, and only the
// labels say what an index meant.
func (r *CDCReader) recheckRetypedColumns(tbl *tableSchema, tm *replication.TableMapEvent) error {
	if tbl.retypedAhead.first == nil {
		return nil
	}
	if tm == nil || len(tm.ColumnType) != len(tbl.Columns) {
		return errCDCCatalogAheadOfPosition(tbl, *tbl.retypedAhead.first, r.flavor)
	}
	for j, col := range tbl.Columns {
		if !tbl.retypedAhead.covers(col.Name) {
			continue
		}
		if why := retypedColumnUnverified(tbl, j, tm); why != "" {
			return errCDCSchemaReplayMismatch(tbl, fmt.Sprintf(
				"column %q was redefined by a schema change logged later, at %s (%q), and %s",
				col.Name, tbl.retypedAhead.first.at, tbl.retypedAhead.first.stmt, why,
			))
		}
	}
	return nil
}

// retypedColumnUnverified says why recorded column j of tm cannot be shown to
// decode the way tbl's current definition does, or "" when it can. A column
// an in-place DDL redefined is decoded by the current definition only on
// positive evidence: a classifiable type on both sides, and every property
// the decoder reads from the definition rather than the row image recorded
// in the event — so a source that records none of it (MariaDB's default
// binlog_row_metadata=NO_LOG) refuses rather than passes.
func retypedColumnUnverified(tbl *tableSchema, j int, tm *replication.TableMapEvent) string {
	declared := dataTypeAt(tbl, j)
	want, wantOK := binlogTypeFamilyForDataType(declared)
	got, gotOK := binlogTypeFamilyForCode(tm.ColumnType[j], tm.ColumnMeta[j])
	switch {
	case !wantOK || !gotOK:
		return fmt.Sprintf("its current type (%s) or the type the event recorded is one sluice cannot compare", declared)
	case !recordedColumnAccepts(tbl, j, tm, j):
		return fmt.Sprintf("the type this event recorded for it (%s family) does not decode the way the current definition (%s, %s family) does", got, declared, want)
	}
	switch want {
	case famEnum, famSet:
		if tm.EnumStrValueMap()[j] == nil && tm.SetStrValueMap()[j] == nil {
			return "the event does not record its labels, so an index cannot be shown to name the same label"
		}
	case famString:
		if _, ok := tm.CollationMap()[j]; !ok {
			return "the event does not record the character set it was written in"
		}
	case famInt, famDecimal, famFloat:
		// This refusal is what lets the decoder take an unrecorded
		// signedness from the shape for every OTHER column
		// (cdc_unrecorded_signedness.go): a column redefined ahead of the
		// reader never reaches it without the event's own record.
		if _, ok := tm.UnsignedMap()[j]; !ok {
			return "the event does not record whether it was signed"
		}
	}
	return ""
}

// errCDCCatalogAheadOfPosition is the loud half of Bug 301 on a source whose
// row events carry no column names. It reuses the replay-mismatch code: the
// cause and the remedy are the same family, and this is the refusal the old
// guard would have issued for the cases it could see.
func errCDCCatalogAheadOfPosition(tbl *tableSchema, d aheadDDL, flavor Flavor) error {
	stmt := strings.Join(strings.Fields(d.stmt), " ")
	if len(stmt) > 200 {
		stmt = stmt[:200] + "…"
	}
	return sluicecode.Wrap(
		sluicecode.CodeCDCSchemaReplayMismatch,
		"set binlog_row_metadata=FULL on the source ("+rowMetadataFullStatement(flavor)+") so row events record "+
			"the column names they were written under, then re-snapshot: `sync start --restart-from-scratch` for a sync, "+
			"a fresh `backup full` for a backup chain. Events already written without names cannot be decoded faithfully",
		fmt.Errorf(
			"mysql: cdc: %s.%s: the stream is reading the binlog at %s, but the table's current definition already "+
				"includes a schema change logged later, at %s (%q). The row events between the two were recorded under "+
				"the earlier shape, and the binlog does not record their column names (binlog_row_metadata is not FULL), "+
				"so decoding them against the current definition could put values under the wrong columns. Refusing "+
				"before any row of this table is decoded. This happens whenever the stream is behind the binlog head "+
				"when a schema change lands: a backup window, a resumed sync, or a live sync that is lagging",
			tbl.Schema, tbl.Name, tbl.loadedAt, d.at, stmt,
		),
	)
}

// rowMetadataFullStatement is how an operator sets binlog_row_metadata=FULL
// durably on a source of flavor. MySQL 8 persists it with SET PERSIST;
// MariaDB has no SET PERSIST (the statement is a syntax error there), so
// it takes SET GLOBAL for the running server plus the option-file line
// that keeps it across a restart.
func rowMetadataFullStatement(flavor Flavor) string {
	if flavor == FlavorMariaDB {
		return rowMetadataFullMariaDB
	}
	return rowMetadataFullMySQL
}

// The two spellings of rowMetadataFullStatement. Every refusal that names
// the setting knows its reader's flavor; the operator docs, which do not,
// state both (docs/operator/error-codes.md).
const (
	rowMetadataFullMySQL   = "SET PERSIST binlog_row_metadata = 'FULL'"
	rowMetadataFullMariaDB = "MariaDB has no SET PERSIST: SET GLOBAL binlog_row_metadata = 'FULL' for the running " +
		"server, plus binlog_row_metadata = FULL under [mariadbd] in its option file so a restart keeps it"
)

// dataTypeAt is tbl's catalog data_type for column j, or "unknown".
func dataTypeAt(tbl *tableSchema, j int) string {
	if j < len(tbl.DataTypes) {
		return tbl.DataTypes[j]
	}
	return "unknown"
}

// errLookaheadPrivilege names the grant the look-ahead's SHOW BINLOG EVENTS
// needs when the server refuses it (MySQL error 1227). The CDC account
// already needs it to stream at all, so this fires only on an account set up
// by hand without it.
func errLookaheadPrivilege(err error) error {
	var me *sqldriver.MySQLError
	if !errors.As(err, &me) || me.Number != 1227 {
		return err
	}
	return sluicecode.Wrap(
		sluicecode.CodeCDCReplicationPermission,
		"grant the stream's account the binlog privileges: GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO "+
			"<account> (MySQL); on MariaDB 10.5+ the same grants, which include BINLOG MONITOR",
		fmt.Errorf("mysql: cdc: reading the binlog ahead of the stream (SHOW BINLOG EVENTS) was refused: %w", err),
	)
}

// shortReadDelay is the wait between re-reads of a binlog that ended short.
func (l *ddlLookahead) shortReadDelay() time.Duration {
	if l.shortReadWait > 0 {
		return l.shortReadWait
	}
	return lookaheadShortReadWait
}
