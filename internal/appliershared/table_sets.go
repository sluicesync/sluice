// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// The table-set columns of sluice_cdc_state behind the
// INCLUDE-TABLE-SCOPE-DRIFT door (Bug 321). Each holds a set of entries in
// the codec below, and each is added on first write, never at startup.
const (
	// PendingAddedTablesColumn records the tables whose `sluice schema
	// add-table` has started and not completed (see
	// [ir.PendingAddedTablesStore]). NULL means "nothing pending".
	PendingAddedTablesColumn = "pending_added_tables"
	// EstablishedTablesColumn records the tables the stream established —
	// copied by its cold start or by a completed add-table, and applied
	// continuously since (see [ir.EstablishedTablesStore]). NULL means "not
	// recorded" (a stream cold-started before the column existed), which is
	// NOT the empty set.
	EstablishedTablesColumn = "established_tables"
	// DroppedTablesColumn records the tables that left the stream's scope
	// after it established them (see [ir.EstablishedRecord.Dropped]). NULL
	// reads as the empty set.
	DroppedTablesColumn = "dropped_tables"
)

// ColdStartInProgressEntry is the established-set value a cold start writes at
// its point of no return ([ir.EstablishedRecord.ColdStartInProgress]). It
// cannot collide with a real entry: a bare table name contains no NUL, and a
// multi-database entry has a non-empty namespace before its NUL.
const ColdStartInProgressEntry = "\x00cold-start-in-progress"

// EncodeTableSet renders a table set for a table-set column: one
// strconv.QuoteToASCII'd entry per line, sorted, duplicates dropped. An empty
// set renders as "".
//
// The value round-trips through a store, so it is a codec, and a table name
// can hold any byte its engine accepts: a newline, a quote, a comma (which is
// why MySQL's live_added_tables comma list was not reused), or — in a
// Postgres database whose encoding is SQL_ASCII — bytes that are not UTF-8,
// which encoding/json would silently replace with U+FFFD. QuoteToASCII
// escapes every such byte and its output is plain ASCII, so the column's
// character set can mangle nothing and [DecodeTableSet] returns the exact
// bytes (TestTableSetCodecRoundTrip). NUL-separated entries (multi-database
// keys, pending entries, the in-progress sentinel) ride it like any other.
func EncodeTableSet(tables []string) string {
	set := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		set[t] = struct{}{}
	}
	names := make([]string, 0, len(set))
	for t := range set {
		names = append(names, t)
	}
	sort.Strings(names)
	lines := make([]string, len(names))
	for i, t := range names {
		lines[i] = strconv.QuoteToASCII(t)
	}
	return strings.Join(lines, "\n")
}

// DecodeTableSet parses a value [EncodeTableSet] wrote. A line that is not a
// quoted string is an error, never skipped: a record that cannot be read must
// refuse the resume rather than read as some other set.
func DecodeTableSet(stored string) ([]string, error) {
	if stored == "" {
		return nil, nil
	}
	lines := strings.Split(stored, "\n")
	out := make([]string, 0, len(lines))
	for i, line := range lines {
		name, err := strconv.Unquote(line)
		if err != nil {
			return nil, fmt.Errorf("table-set line %d (%q) is not a quoted table name: %w", i+1, line, err)
		}
		out = append(out, name)
	}
	return out, nil
}

// EncodePendingEntry renders one pending add-table entry:
// `<table>\x00<started ms>\x00<heartbeat ms>\x00<token>`. A NUL cannot occur
// in a table name on either engine, so the split is unambiguous.
func EncodePendingEntry(p ir.PendingAddedTable) string {
	return p.Table + "\x00" + strconv.FormatInt(p.StartedAt.UnixMilli(), 10) + "\x00" +
		strconv.FormatInt(p.HeartbeatAt.UnixMilli(), 10) + "\x00" + p.Token
}

// DecodePendingEntry parses an entry [EncodePendingEntry] wrote. A malformed
// entry is an error, never a skipped one.
func DecodePendingEntry(entry string) (ir.PendingAddedTable, error) {
	parts := strings.Split(entry, "\x00")
	if len(parts) != 4 || parts[3] == "" {
		return ir.PendingAddedTable{}, fmt.Errorf("pending add-table entry %q is not <table>\\x00<started>\\x00<heartbeat>\\x00<token>", entry)
	}
	started, err1 := strconv.ParseInt(parts[1], 10, 64)
	heartbeat, err2 := strconv.ParseInt(parts[2], 10, 64)
	if err1 != nil || err2 != nil {
		return ir.PendingAddedTable{}, fmt.Errorf("pending add-table entry %q carries a malformed timestamp", entry)
	}
	return ir.PendingAddedTable{
		Table: parts[0], Token: parts[3],
		StartedAt: time.UnixMilli(started).UTC(), HeartbeatAt: time.UnixMilli(heartbeat).UTC(),
	}, nil
}

// TableSetQueries are the engine-built statements the helpers below run
// against one table-set column; every one binds the stream id as its last
// parameter.
type TableSetQueries struct {
	// Column names the column in error messages.
	Column string
	// Read is `SELECT <column>, <server now in Unix ms> FROM … WHERE
	// stream_id = <p1>`. The clock is read in the same statement as the
	// row, so it is always the clock of the server holding the control row
	// (P1: vtgate routes a table-less SELECT to whichever tablet it picks,
	// which on a sharded keyspace need not be the row's).
	Read string
	// ReadForUpdate is Read with a row lock (`… FOR UPDATE`), run inside the
	// read-modify-write transaction so two concurrent writers on one stream
	// cannot each write back a set missing the other's entry. Its clock is
	// what a pending edit stamps, so a stamp is the row's own server's time.
	ReadForUpdate string
	// Update is `UPDATE … SET <column> = <p1> WHERE stream_id = <p2>`.
	Update string
	// IsMissingColumn classifies the dialect's unknown-column error.
	IsMissingColumn func(error) bool
}

// Recoveries named by an undecodable value's error ([ir.UndecodableTableSetError]).
const (
	recoveryRecord  = "re-copy the stream with `sluice sync start --restart-from-scratch`, whose cold start overwrites the record without reading it"
	recoveryPending = "clear the stream's pending add-table records with `UPDATE sluice_cdc_state SET pending_added_tables = NULL WHERE stream_id = '<stream>'` " +
		"on the target, then bring each table they named in again with `sluice schema add-table` (empty its target table first) or re-copy the stream"
)

func recoveryFor(column string) string {
	switch column {
	case PendingAddedTablesColumn:
		return recoveryPending
	case "live_added_tables":
		return "clear it with `UPDATE sluice_cdc_state SET live_added_tables = NULL WHERE stream_id = '<stream>'` on the " +
			"target, then name each added table in --include-table or bring it in again with `sluice schema add-table`"
	}
	return recoveryRecord
}

func undecodable(column, raw string, err error) error {
	return &ir.UndecodableTableSetError{Column: column, RawBytes: len(raw), Recovery: recoveryFor(column), Err: err}
}

// readRow reads one table-set column and the server's now. No row, no
// control table and no column read as (NULL, zero, nil).
func readRow(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, q TableSetQueries, streamID string) (sql.NullString, time.Time, error) {
	var (
		stored sql.NullString
		nowMs  int64
	)
	err := db.QueryRowContext(ctx, q.Read, streamID).Scan(&stored, &nowMs)
	switch {
	case errors.Is(err, sql.ErrNoRows), cfg.IsMissingTable(err), q.IsMissingColumn(err):
		return sql.NullString{}, time.Time{}, nil
	case err != nil:
		return sql.NullString{}, time.Time{}, fmt.Errorf("%s: read %s: %w", cfg.EngineName, q.Column, err)
	}
	return stored, time.UnixMilli(nowMs).UTC(), nil
}

// ReadTableSet returns streamID's set and whether one is recorded. No row,
// no control table, no column and NULL all read as not recorded; any other
// failure is an error, and a value that does not decode is an
// [ir.UndecodableTableSetError] naming the recovery.
func ReadTableSet(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, q TableSetQueries, streamID string) (tables []string, recorded bool, err error) {
	stored, _, err := readRow(ctx, db, cfg, q, streamID)
	if err != nil || !stored.Valid {
		return nil, false, err
	}
	tables, err = DecodeTableSet(stored.String)
	if err != nil {
		return nil, false, fmt.Errorf("%s: read %s of stream %q: %w", cfg.EngineName, q.Column, streamID, undecodable(q.Column, stored.String, err))
	}
	return tables, true, nil
}

// ReadPendingEntries reads a pending_added_tables set, with the server's now
// (read in the same statement) to classify it against. It does not fail on
// a value that does not decode: each line or entry that does not is
// returned as a malformed entry ([ir.PendingAddedTable.Malformed]) — never
// skipped — so `sync health`, `sync status` and the door can report it and
// name its recovery instead of failing outright.
func ReadPendingEntries(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, q TableSetQueries, streamID string) ([]ir.PendingAddedTable, time.Time, error) {
	stored, now, err := readRow(ctx, db, cfg, q, streamID)
	if err != nil || !stored.Valid || stored.String == "" {
		return nil, now, err
	}
	return DecodePendingSet(stored.String), now, nil
}

// DecodePendingSet decodes a stored pending set entry by entry, returning a
// malformed entry for each line that is not a quoted string (Table "") and
// for each entry that does not decode (Table is the name before its first
// NUL).
func DecodePendingSet(stored string) []ir.PendingAddedTable {
	var out []ir.PendingAddedTable
	for _, line := range strings.Split(stored, "\n") {
		entry, err := strconv.Unquote(line)
		if err != nil {
			out = append(out, ir.PendingAddedTable{Malformed: line})
			continue
		}
		p, err := DecodePendingEntry(entry)
		if err != nil {
			out = append(out, ir.PendingAddedTable{Table: pendingEntryTable(entry), Malformed: entry})
			continue
		}
		out = append(out, p)
	}
	return out
}

// pendingEntryTable is the table name of a pending entry, well-formed or
// not: the text before its first NUL.
func pendingEntryTable(entry string) string {
	if i := strings.IndexByte(entry, 0); i >= 0 {
		return entry[:i]
	}
	return entry
}

// ReadEstablishedRecord reads the established and dropped sets. The record
// is Recorded when the established column holds a value; the in-progress
// sentinel reads as ColdStartInProgress, the all-uncopied sentinel as
// DroppedAllUncopied; a NULL dropped set reads as empty. An undecodable
// value is an [ir.UndecodableTableSetError] naming its column.
func ReadEstablishedRecord(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, established, dropped TableSetQueries, streamID string) (ir.EstablishedRecord, error) {
	est, recorded, err := ReadTableSet(ctx, db, cfg, established, streamID)
	if err != nil || !recorded {
		return ir.EstablishedRecord{}, err
	}
	drop, _, err := ReadTableSet(ctx, db, cfg, dropped, streamID)
	if err != nil {
		return ir.EstablishedRecord{Recorded: true}, err
	}
	rec := ir.EstablishedRecord{Recorded: true}
	for _, e := range est {
		if e == ColdStartInProgressEntry {
			rec.ColdStartInProgress = true
		} else {
			rec.Established = append(rec.Established, e)
		}
	}
	if rec.ColdStartInProgress {
		rec.Established = nil
	}
	for _, d := range drop {
		if d == ir.DroppedAllUncopiedEntry {
			rec.DroppedAllUncopied = true
		} else {
			rec.Dropped = append(rec.Dropped, d)
		}
	}
	return rec, nil
}

// TableSetEdit computes the next value of a set from the current one.
type TableSetEdit struct {
	// Replace: the next value does not depend on the current one, so the
	// current value is never decoded — a value that does not decode is
	// overwritten (logged at ERROR with its length) rather than blocking the
	// write. This is what lets a cold start recover a corrupted record (L1).
	Replace bool
	// Apply computes next from current. recorded is false when the column
	// is NULL; now is the clock of the server holding the row. A nil next
	// with write true stores NULL; write false leaves the row untouched.
	Apply func(current map[string]struct{}, recorded bool, now time.Time) (next map[string]struct{}, write bool)
}

// UpdateTableSet applies edit to streamID's set under a row lock and writes
// the result back, skipping the write when nothing changes.
//
// requireRow selects the missing-row behaviour: true makes a missing row an
// error wrapping cfg.ErrStreamNotFound (a writer that must not silently
// record nothing); false makes a missing row, table or column nothing to do.
func UpdateTableSet(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, q TableSetQueries, streamID string, requireRow bool, edit TableSetEdit) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s: update %s: begin: %w", cfg.EngineName, q.Column, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	var (
		stored sql.NullString
		nowMs  int64
	)
	switch serr := tx.QueryRowContext(ctx, q.ReadForUpdate, streamID).Scan(&stored, &nowMs); {
	case errors.Is(serr, sql.ErrNoRows):
		if requireRow {
			return fmt.Errorf("%s: update %s: %w: %q", cfg.EngineName, q.Column, cfg.ErrStreamNotFound, streamID)
		}
		return tx.Rollback()
	case !requireRow && (cfg.IsMissingTable(serr) || q.IsMissingColumn(serr)):
		return tx.Rollback()
	case serr != nil:
		return fmt.Errorf("%s: update %s: %w", cfg.EngineName, q.Column, serr)
	}
	current, derr := DecodeTableSet(stored.String)
	switch {
	case derr != nil && edit.Replace:
		slog.ErrorContext(ctx, "overwriting a table-set value that does not decode; its contents are lost",
			slog.String("column", q.Column), slog.String("stream_id", streamID), slog.Int("discarded_bytes", len(stored.String)),
			slog.String("error", derr.Error()))
		current = nil
	case derr != nil:
		return fmt.Errorf("%s: update %s of stream %q: %w", cfg.EngineName, q.Column, streamID, undecodable(q.Column, stored.String, derr))
	}
	next, write := edit.Apply(toEntrySet(current), stored.Valid, time.UnixMilli(nowMs).UTC())
	if !write {
		return tx.Rollback()
	}
	value := encodeNext(next)
	if (value == nil && !stored.Valid) || (value != nil && stored.Valid && value.(string) == stored.String) {
		return tx.Rollback() // nothing changes
	}
	if _, err := tx.ExecContext(ctx, q.Update, value, streamID); err != nil {
		return fmt.Errorf("%s: update %s: %w", cfg.EngineName, q.Column, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s: update %s: commit: %w", cfg.EngineName, q.Column, err)
	}
	return nil
}

func toEntrySet(entries []string) map[string]struct{} {
	set := make(map[string]struct{}, len(entries)+1)
	for _, e := range entries {
		set[e] = struct{}{}
	}
	return set
}

// encodeNext is the column value for next: nil (NULL) or its encoding.
func encodeNext(next map[string]struct{}) any {
	if next == nil {
		return nil
	}
	names := make([]string, 0, len(next))
	for t := range next {
		names = append(names, t)
	}
	return EncodeTableSet(names)
}

// PendingAdd is the [TableSetEdit] that writes table's pending entry under
// token, started and alive at the server's now, replacing any earlier entry
// for the table, malformed or not (a re-run add takes the entry over; the
// add it superseded then finds a token that is not its own).
func PendingAdd(table, token string) TableSetEdit {
	return TableSetEdit{Apply: func(set map[string]struct{}, _ bool, now time.Time) (map[string]struct{}, bool) {
		removePendingEntries(set, []string{table})
		set[EncodePendingEntry(ir.PendingAddedTable{Table: table, Token: token, StartedAt: now, HeartbeatAt: now})] = struct{}{}
		return set, true
	}}
}

// PendingTouch is the [TableSetEdit] that refreshes the heartbeat of table's
// entry to now (alive) or the epoch (stopped), keeping its start — only when
// the entry still carries token. *superseded reports otherwise; nothing is
// written then.
func PendingTouch(table, token string, alive bool, superseded *bool) TableSetEdit {
	return TableSetEdit{Apply: func(set map[string]struct{}, _ bool, now time.Time) (map[string]struct{}, bool) {
		entry, p, ok := ownPendingEntry(set, table, token)
		if !ok {
			*superseded = true
			return nil, false
		}
		delete(set, entry)
		if alive {
			p.HeartbeatAt = now
		} else {
			p.HeartbeatAt = time.Unix(0, 0).UTC()
		}
		set[EncodePendingEntry(p)] = struct{}{}
		return set, true
	}}
}

// PendingClear is the [TableSetEdit] that removes tables' entries from a
// pending set whatever their tokens, malformed ones included, storing NULL
// when it comes out empty.
func PendingClear(tables []string) TableSetEdit {
	return TableSetEdit{Apply: func(set map[string]struct{}, _ bool, _ time.Time) (map[string]struct{}, bool) {
		removePendingEntries(set, tables)
		if len(set) == 0 {
			return nil, true
		}
		return set, true
	}}
}

// PendingAbandon is the [TableSetEdit] that rewrites table's entry as
// abandoned whatever its token: [ir.PendingAddAbandonedToken], heartbeat the
// epoch, start kept (the epoch for a malformed entry, whose start is
// unreadable). No entry is nothing to write.
func PendingAbandon(table string) TableSetEdit {
	return TableSetEdit{Apply: func(set map[string]struct{}, _ bool, _ time.Time) (map[string]struct{}, bool) {
		for entry := range set {
			if pendingEntryTable(entry) != table {
				continue
			}
			p, err := DecodePendingEntry(entry)
			if err != nil {
				p = ir.PendingAddedTable{Table: table, StartedAt: time.Unix(0, 0).UTC()}
			}
			delete(set, entry)
			p.Token, p.HeartbeatAt = ir.PendingAddAbandonedToken, time.Unix(0, 0).UTC()
			set[EncodePendingEntry(p)] = struct{}{}
			return set, true
		}
		return nil, false
	}}
}

// ownPendingEntry finds table's entry when it carries token.
func ownPendingEntry(set map[string]struct{}, table, token string) (string, ir.PendingAddedTable, bool) {
	for entry := range set {
		if p, err := DecodePendingEntry(entry); err == nil && p.Table == table && p.Token == token {
			return entry, p, true
		}
	}
	return "", ir.PendingAddedTable{}, false
}

func removePendingEntries(set map[string]struct{}, tables []string) {
	drop := make(map[string]bool, len(tables))
	for _, t := range tables {
		drop[t] = true
	}
	for entry := range set {
		if drop[pendingEntryTable(entry)] {
			delete(set, entry)
		}
	}
}

// BeginColdStart is the [TableSetEdit] that writes the in-progress sentinel
// over the established set. A replace: it overwrites a corrupted record.
func BeginColdStart() TableSetEdit {
	return TableSetEdit{Replace: true, Apply: func(map[string]struct{}, bool, time.Time) (map[string]struct{}, bool) {
		return map[string]struct{}{ColdStartInProgressEntry: {}}, true
	}}
}

// EstablishedReplace is the [TableSetEdit] that records exactly tables (a
// cold start's copy, a dropped set). The empty set is stored as "", not
// NULL: "established nothing" is a recorded fact. A replace: it overwrites a
// corrupted value.
func EstablishedReplace(tables []string) TableSetEdit {
	return TableSetEdit{Replace: true, Apply: func(map[string]struct{}, bool, time.Time) (map[string]struct{}, bool) {
		return toEntrySet(tables), true
	}}
}

// PendingAddQueries are the statements [CompletePendingAdd] runs: all three
// table-set columns of one row, read under a lock and written in one UPDATE.
type PendingAddQueries struct {
	// ReadForUpdate is `SELECT pending_added_tables, established_tables,
	// dropped_tables … WHERE stream_id = <p1> FOR UPDATE`.
	ReadForUpdate string
	// Update is `UPDATE … SET pending_added_tables = <p1>, established_tables
	// = <p2>, dropped_tables = <p3> WHERE stream_id = <p4>`.
	Update string
}

// CompletePendingAdd is [ir.PendingAddedTablesStore.CompletePendingAddedTable]
// over one transaction: the entry's token is checked, the entry removed, the
// table recorded as established and taken out of the dropped set, all
// together or not at all. The established half is skipped for a stream with
// no record (one table is not a record of what it established), during a
// cold start in progress (the cold start will record its own copy), and on a
// multi-database stream (add-table names a bare table; the record keys
// `<namespace>\x00<table>` — a bare entry would be junk no scope matches).
// The caller has ensured all three columns exist.
func CompletePendingAdd(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, q PendingAddQueries, streamID, table, token string) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s: complete pending add-table: begin: %w", cfg.EngineName, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	var pendingCol, establishedCol, droppedCol sql.NullString
	switch serr := tx.QueryRowContext(ctx, q.ReadForUpdate, streamID).Scan(&pendingCol, &establishedCol, &droppedCol); {
	case errors.Is(serr, sql.ErrNoRows):
		return fmt.Errorf("%s: complete pending add-table: %w (stream %q has no row)", cfg.EngineName, ir.ErrPendingAddSuperseded, streamID)
	case serr != nil:
		return fmt.Errorf("%s: complete pending add-table: %w", cfg.EngineName, serr)
	}
	pending, err := DecodeTableSet(pendingCol.String)
	if err != nil {
		return fmt.Errorf("%s: complete pending add-table: %w", cfg.EngineName, undecodable(PendingAddedTablesColumn, pendingCol.String, err))
	}
	pset := toEntrySet(pending)
	entry, _, ok := ownPendingEntry(pset, table, token)
	if !ok {
		return fmt.Errorf("%s: complete pending add-table %q: %w", cfg.EngineName, table, ir.ErrPendingAddSuperseded)
	}
	delete(pset, entry)
	var pendingNext any
	if len(pset) > 0 {
		pendingNext = encodeNext(pset)
	}

	establishedNext, droppedNext := nullableString(establishedCol), nullableString(droppedCol)
	if establishedCol.Valid {
		est, err := DecodeTableSet(establishedCol.String)
		if err != nil {
			return fmt.Errorf("%s: complete pending add-table: %w", cfg.EngineName, undecodable(EstablishedTablesColumn, establishedCol.String, err))
		}
		if !inProgressOrMultiDatabase(est) {
			eset := toEntrySet(est)
			eset[table] = struct{}{}
			establishedNext = encodeNext(eset)
			if droppedCol.Valid {
				drop, err := DecodeTableSet(droppedCol.String)
				if err != nil {
					return fmt.Errorf("%s: complete pending add-table: %w", cfg.EngineName, undecodable(DroppedTablesColumn, droppedCol.String, err))
				}
				dset := toEntrySet(drop)
				delete(dset, table)
				droppedNext = encodeNext(dset)
			}
		}
	}
	if _, err := tx.ExecContext(ctx, q.Update, pendingNext, establishedNext, droppedNext, streamID); err != nil {
		return fmt.Errorf("%s: complete pending add-table: %w", cfg.EngineName, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s: complete pending add-table: commit: %w", cfg.EngineName, err)
	}
	return nil
}

// NarrowRecord computes a resume's narrowing of a record: established keeps
// the entries in inScope, the rest (leaving) join dropped, and dropped loses
// the entries in gone. Pure, so [NarrowEstablished] applies it to the values
// it read under its row lock — an entry a concurrent completion added since
// the caller's own read is judged too (kept if in scope, dropped otherwise),
// never overwritten away.
func NarrowRecord(established, dropped, inScope, gone []string) (nextEstablished, nextDropped, leaving []string) {
	in, isGone := toEntrySet(inScope), toEntrySet(gone)
	nextEstablished = []string{}
	drop := toEntrySet(dropped)
	for _, e := range established {
		if _, ok := in[e]; ok {
			nextEstablished = append(nextEstablished, e)
		} else {
			leaving = append(leaving, e)
			drop[e] = struct{}{}
		}
	}
	nextDropped = []string{}
	for e := range drop {
		if _, ok := isGone[e]; !ok {
			nextDropped = append(nextDropped, e)
		}
	}
	sort.Strings(nextEstablished)
	sort.Strings(nextDropped)
	sort.Strings(leaving)
	return nextEstablished, nextDropped, leaving
}

// RecordQueries are the statements [NarrowEstablished] runs: both record
// columns of one row, read under a lock and written in one UPDATE.
type RecordQueries struct {
	// ReadForUpdate is `SELECT established_tables, dropped_tables … WHERE
	// stream_id = <p1> FOR UPDATE`.
	ReadForUpdate string
	// Update is `UPDATE … SET established_tables = <p1>, dropped_tables =
	// <p2> WHERE stream_id = <p3>`.
	Update string
}

// NarrowEstablished is [ir.EstablishedTablesStore.NarrowEstablishedRecord]:
// [NarrowRecord] over the values read under a row lock, written back in the
// same transaction. No row, no record, or a cold start in progress: nothing
// is written. The caller has ensured both columns exist.
func NarrowEstablished(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, q RecordQueries, streamID string, inScope, gone []string) (leaving []string, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: narrow established-table record: begin: %w", cfg.EngineName, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	var establishedCol, droppedCol sql.NullString
	switch serr := tx.QueryRowContext(ctx, q.ReadForUpdate, streamID).Scan(&establishedCol, &droppedCol); {
	case errors.Is(serr, sql.ErrNoRows):
		return nil, tx.Rollback()
	case serr != nil:
		return nil, fmt.Errorf("%s: narrow established-table record: %w", cfg.EngineName, serr)
	}
	if !establishedCol.Valid {
		return nil, tx.Rollback()
	}
	est, err := DecodeTableSet(establishedCol.String)
	if err != nil {
		return nil, fmt.Errorf("%s: narrow established-table record: %w", cfg.EngineName, undecodable(EstablishedTablesColumn, establishedCol.String, err))
	}
	if _, inProgress := toEntrySet(est)[ColdStartInProgressEntry]; inProgress {
		return nil, tx.Rollback()
	}
	drop, err := DecodeTableSet(droppedCol.String)
	if err != nil {
		return nil, fmt.Errorf("%s: narrow established-table record: %w", cfg.EngineName, undecodable(DroppedTablesColumn, droppedCol.String, err))
	}
	nextEst, nextDrop, leaving := NarrowRecord(est, drop, inScope, gone)
	if EncodeTableSet(nextEst) == EncodeTableSet(est) && EncodeTableSet(nextDrop) == EncodeTableSet(drop) {
		return nil, tx.Rollback()
	}
	if _, err := tx.ExecContext(ctx, q.Update, EncodeTableSet(nextEst), EncodeTableSet(nextDrop), streamID); err != nil {
		return nil, fmt.Errorf("%s: narrow established-table record: %w", cfg.EngineName, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("%s: narrow established-table record: commit: %w", cfg.EngineName, err)
	}
	return leaving, nil
}

func nullableString(s sql.NullString) any {
	if !s.Valid {
		return nil
	}
	return s.String
}

func inProgressOrMultiDatabase(entries []string) bool {
	for _, e := range entries {
		if strings.IndexByte(e, 0) >= 0 {
			return true
		}
	}
	return false
}
