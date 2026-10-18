// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// The table-set columns of sluice_cdc_state behind the
// INCLUDE-TABLE-SCOPE-DRIFT door (Bug 321). Both hold a set of table names in
// the codec below, and both are added on first write, never at startup.
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

// EncodePendingEntry renders one pending add-table entry for the
// pending_added_tables set: `<table>\x00<started ms>\x00<heartbeat ms>`. A
// NUL cannot occur in a table name on either engine, so the split is
// unambiguous; the set codec carries the NULs like any other byte.
func EncodePendingEntry(table string, started, heartbeat time.Time) string {
	return table + "\x00" + strconv.FormatInt(started.UnixMilli(), 10) + "\x00" + strconv.FormatInt(heartbeat.UnixMilli(), 10)
}

// DecodePendingEntry parses an entry [EncodePendingEntry] wrote. A malformed
// entry is an error, never a skipped one.
func DecodePendingEntry(entry string) (ir.PendingAddedTable, error) {
	parts := strings.Split(entry, "\x00")
	if len(parts) != 3 {
		return ir.PendingAddedTable{}, fmt.Errorf("pending add-table entry %q is not <table>\\x00<started>\\x00<heartbeat>", entry)
	}
	started, err1 := strconv.ParseInt(parts[1], 10, 64)
	heartbeat, err2 := strconv.ParseInt(parts[2], 10, 64)
	if err1 != nil || err2 != nil {
		return ir.PendingAddedTable{}, fmt.Errorf("pending add-table entry %q carries a malformed timestamp", entry)
	}
	return ir.PendingAddedTable{Table: parts[0], StartedAt: time.UnixMilli(started).UTC(), HeartbeatAt: time.UnixMilli(heartbeat).UTC()}, nil
}

// pendingEntryTable is the table an encoded pending entry names; the entry
// itself when it does not decode (so an edit by table name still matches it).
func pendingEntryTable(entry string) string {
	if p, err := DecodePendingEntry(entry); err == nil {
		return p.Table
	}
	return entry
}

// EncodeTableSet renders a table set for a table-set column: one
// strconv.QuoteToASCII'd name per line, sorted, duplicates dropped. An empty
// set renders as "".
//
// The value round-trips through a store, so it is a codec, and a table name
// can hold any byte its engine accepts: a newline, a quote, a comma (which is
// why MySQL's live_added_tables comma list was not reused), or — in a
// Postgres database whose encoding is SQL_ASCII — bytes that are not UTF-8,
// which encoding/json would silently replace with U+FFFD. QuoteToASCII
// escapes every such byte and its output is plain ASCII, so the column's
// character set can mangle nothing and [DecodeTableSet] returns the exact
// bytes (TestTableSetCodecRoundTrip). A multi-database stream's entries carry
// their namespace before a NUL byte, which the codec carries like any other.
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

// TableSetQueries are the engine-built statements the helpers below run
// against one table-set column; every one binds the stream id as its last
// parameter.
type TableSetQueries struct {
	// Column names the column in error messages.
	Column string
	// Read is `SELECT <column> … WHERE stream_id = <p1>`.
	Read string
	// ReadForUpdate is Read with a row lock (`… FOR UPDATE`), run inside the
	// read-modify-write transaction so two concurrent writers on one stream
	// cannot each write back a set missing the other's table.
	ReadForUpdate string
	// Update is `UPDATE … SET <column> = <p1> WHERE stream_id = <p2>`.
	Update string
	// IsMissingColumn classifies the dialect's unknown-column error.
	IsMissingColumn func(error) bool
}

// ReadTableSet returns streamID's set and whether one is recorded. No row,
// no control table, no column and NULL all read as not recorded; any other
// failure, including a value that does not decode, is an error.
func ReadTableSet(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, q TableSetQueries, streamID string) (tables []string, recorded bool, err error) {
	var stored sql.NullString
	err = db.QueryRowContext(ctx, q.Read, streamID).Scan(&stored)
	switch {
	case errors.Is(err, sql.ErrNoRows), cfg.IsMissingTable(err), q.IsMissingColumn(err):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("%s: read %s: %w", cfg.EngineName, q.Column, err)
	}
	if !stored.Valid {
		return nil, false, nil
	}
	tables, err = DecodeTableSet(stored.String)
	if err != nil {
		return nil, false, fmt.Errorf("%s: read %s of stream %q: %w", cfg.EngineName, q.Column, streamID, err)
	}
	return tables, true, nil
}

// ReadPendingEntries reads and decodes a pending_added_tables set.
func ReadPendingEntries(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, q TableSetQueries, streamID string) ([]ir.PendingAddedTable, error) {
	entries, _, err := ReadTableSet(ctx, db, cfg, q, streamID)
	if err != nil {
		return nil, err
	}
	out := make([]ir.PendingAddedTable, 0, len(entries))
	for _, e := range entries {
		p, err := DecodePendingEntry(e)
		if err != nil {
			return nil, fmt.Errorf("%s: read %s of stream %q: %w", cfg.EngineName, q.Column, streamID, err)
		}
		out = append(out, p)
	}
	return out, nil
}

// ReadEstablishedRecord reads the established and dropped sets. The record
// is Recorded when the established column holds a value; a NULL dropped set
// reads as empty.
func ReadEstablishedRecord(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, established, dropped TableSetQueries, streamID string) (ir.EstablishedRecord, error) {
	est, recorded, err := ReadTableSet(ctx, db, cfg, established, streamID)
	if err != nil || !recorded {
		return ir.EstablishedRecord{}, err
	}
	drop, _, err := ReadTableSet(ctx, db, cfg, dropped, streamID)
	if err != nil {
		return ir.EstablishedRecord{}, err
	}
	return ir.EstablishedRecord{Recorded: true, Established: est, Dropped: drop}, nil
}

// TableSetEdit computes the next value of a set from the current one.
// recorded is false when the column is NULL. A nil next with write true
// stores NULL; write false leaves the row untouched.
type TableSetEdit func(current map[string]struct{}, recorded bool) (next map[string]struct{}, write bool)

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
	var stored sql.NullString
	switch serr := tx.QueryRowContext(ctx, q.ReadForUpdate, streamID).Scan(&stored); {
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
	current, err := DecodeTableSet(stored.String)
	if err != nil {
		return fmt.Errorf("%s: update %s of stream %q: %w", cfg.EngineName, q.Column, streamID, err)
	}
	set := make(map[string]struct{}, len(current)+1)
	for _, t := range current {
		set[t] = struct{}{}
	}
	next, write := edit(set, stored.Valid)
	if !write {
		return tx.Rollback()
	}
	var value any
	if next != nil {
		names := make([]string, 0, len(next))
		for t := range next {
			names = append(names, t)
		}
		value = EncodeTableSet(names)
	}
	if (value == nil && !stored.Valid) || (value != nil && stored.Valid && value.(string) == EncodeTableSet(current)) {
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

// PendingAdd is the [TableSetEdit] that writes table's pending entry, started
// and alive at now, replacing any earlier entry for it.
func PendingAdd(table string, now time.Time) TableSetEdit {
	return func(set map[string]struct{}, _ bool) (map[string]struct{}, bool) {
		removePendingEntries(set, []string{table})
		set[EncodePendingEntry(table, now, now)] = struct{}{}
		return set, true
	}
}

// PendingHeartbeat is the [TableSetEdit] that refreshes table's heartbeat to
// now, keeping its start; no entry, no write.
func PendingHeartbeat(table string, now time.Time) TableSetEdit {
	return func(set map[string]struct{}, _ bool) (map[string]struct{}, bool) {
		for entry := range set {
			p, err := DecodePendingEntry(entry)
			if err != nil || p.Table != table {
				continue
			}
			delete(set, entry)
			set[EncodePendingEntry(table, p.StartedAt, now)] = struct{}{}
			return set, true
		}
		return nil, false
	}
}

// PendingClear is the [TableSetEdit] that removes tables' entries from a
// pending set, storing NULL when it comes out empty.
func PendingClear(tables []string) TableSetEdit {
	return func(set map[string]struct{}, _ bool) (map[string]struct{}, bool) {
		removePendingEntries(set, tables)
		if len(set) == 0 {
			return nil, true
		}
		return set, true
	}
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

// SetNull is the [TableSetEdit] that stores NULL (the record reset a cold
// start makes before it copies).
func SetNull() TableSetEdit {
	return func(map[string]struct{}, bool) (map[string]struct{}, bool) { return nil, true }
}

// SetRemove is the [TableSetEdit] that removes table from a set (a completed
// add-table leaving the dropped set); an unrecorded set stays unrecorded.
func SetRemove(table string) TableSetEdit {
	return func(set map[string]struct{}, recorded bool) (map[string]struct{}, bool) {
		if !recorded {
			return nil, false
		}
		delete(set, table)
		return set, true
	}
}

// EstablishedReplace is the [TableSetEdit] that records exactly tables (a
// cold start's copy, a narrowed resume). The empty set is stored as "", not
// NULL: "established nothing" is a recorded fact.
func EstablishedReplace(tables []string) TableSetEdit {
	return func(map[string]struct{}, bool) (map[string]struct{}, bool) {
		next := make(map[string]struct{}, len(tables))
		for _, t := range tables {
			next[t] = struct{}{}
		}
		return next, true
	}
}

// EstablishedAdd is the [TableSetEdit] a completed add-table applies. It
// leaves an unrecorded (NULL) set unrecorded: one table is not a record of
// what the stream established, and writing it would turn every other table
// the stream does stream into "never copied".
//
// It also leaves a multi-database stream's record alone (F3): add-table names
// a bare table, the record keys `<namespace>\x00<table>`, and a bare entry
// would be junk no scope ever matches. Such a stream re-records with a
// --restart-from-scratch, which its refusal names instead of add-table.
func EstablishedAdd(table string) TableSetEdit {
	return func(set map[string]struct{}, recorded bool) (map[string]struct{}, bool) {
		if !recorded {
			return nil, false
		}
		for k := range set {
			if strings.IndexByte(k, 0) >= 0 {
				return nil, false
			}
		}
		set[table] = struct{}{}
		return set, true
	}
}
