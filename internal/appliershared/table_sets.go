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
)

// The two table-set columns of sluice_cdc_state behind the
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
)

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

// PendingAdd is the [TableSetEdit] that adds table to a pending set.
func PendingAdd(table string) TableSetEdit {
	return func(set map[string]struct{}, _ bool) (map[string]struct{}, bool) {
		set[table] = struct{}{}
		return set, true
	}
}

// PendingClear is the [TableSetEdit] that removes tables from a pending set,
// storing NULL when it comes out empty.
func PendingClear(tables []string) TableSetEdit {
	return func(set map[string]struct{}, _ bool) (map[string]struct{}, bool) {
		for _, t := range tables {
			delete(set, t)
		}
		if len(set) == 0 {
			return nil, true
		}
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
func EstablishedAdd(table string) TableSetEdit {
	return func(set map[string]struct{}, recorded bool) (map[string]struct{}, bool) {
		if !recorded {
			return nil, false
		}
		set[table] = struct{}{}
		return set, true
	}
}
