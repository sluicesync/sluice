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

// PendingAddedTablesColumn is the sluice_cdc_state column that records the
// tables whose `sluice schema add-table` has started and not completed (see
// [ir.PendingAddedTablesStore]). NULL means "nothing pending".
const PendingAddedTablesColumn = "pending_added_tables"

// EncodePendingAddedTables renders a pending set for the column: one
// strconv.QuoteToASCII'd name per line, sorted, duplicates dropped. An empty
// set renders as "" — the caller stores NULL for it.
//
// The value round-trips through a store, so it is a codec, and a table name
// can hold any byte its engine accepts: a newline, a quote, a comma (which is
// why MySQL's live_added_tables comma list was not reused), or — in a
// Postgres database whose encoding is SQL_ASCII — bytes that are not UTF-8,
// which encoding/json would silently replace with U+FFFD. QuoteToASCII
// escapes every such byte and its output is plain ASCII, so the column's
// character set can mangle nothing and [DecodePendingAddedTables] returns the
// exact bytes (TestPendingAddedTablesCodecRoundTrip).
func EncodePendingAddedTables(tables []string) string {
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

// DecodePendingAddedTables parses a value [EncodePendingAddedTables] wrote.
// A line that is not a quoted string is an error, never skipped: a record
// that cannot be read must refuse the resume rather than read as "nothing
// pending".
func DecodePendingAddedTables(stored string) ([]string, error) {
	if stored == "" {
		return nil, nil
	}
	lines := strings.Split(stored, "\n")
	out := make([]string, 0, len(lines))
	for i, line := range lines {
		name, err := strconv.Unquote(line)
		if err != nil {
			return nil, fmt.Errorf("pending_added_tables line %d (%q) is not a quoted table name: %w", i+1, line, err)
		}
		out = append(out, name)
	}
	return out, nil
}

// PendingAddedTablesQueries are the engine-built statements the helpers
// below run; every one binds the stream id as its last parameter.
type PendingAddedTablesQueries struct {
	// Read is `SELECT pending_added_tables … WHERE stream_id = <p1>`.
	Read string
	// ReadForUpdate is Read with a row lock (`… FOR UPDATE`), run inside the
	// read-modify-write transaction so two concurrent add-tables on one
	// stream cannot each write back a set missing the other's table.
	ReadForUpdate string
	// Update is `UPDATE … SET pending_added_tables = <p1> WHERE stream_id = <p2>`.
	Update string
	// IsMissingColumn classifies the dialect's unknown-column error.
	IsMissingColumn func(error) bool
}

// ReadPendingAddedTables returns streamID's pending set. No row, no control
// table and no column all read as empty (see [ir.PendingAddedTablesStore]);
// any other failure, including a value that does not decode, is an error.
func ReadPendingAddedTables(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, q PendingAddedTablesQueries, streamID string) ([]string, error) {
	var stored sql.NullString
	err := db.QueryRowContext(ctx, q.Read, streamID).Scan(&stored)
	switch {
	case errors.Is(err, sql.ErrNoRows), cfg.IsMissingTable(err), q.IsMissingColumn(err):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("%s: read pending added tables: %w", cfg.EngineName, err)
	}
	if !stored.Valid {
		return nil, nil
	}
	tables, err := DecodePendingAddedTables(stored.String)
	if err != nil {
		return nil, fmt.Errorf("%s: read pending added tables of stream %q: %w", cfg.EngineName, streamID, err)
	}
	return tables, nil
}

// UpdatePendingAddedTables applies edit to streamID's pending set under a
// row lock and writes the result back (NULL when it comes out empty).
//
// requireRow selects the missing-row behaviour: Record passes true, so a
// missing row is an error wrapping cfg.ErrStreamNotFound; Clear passes false,
// so a missing row (or table, or column) is nothing to clear.
func UpdatePendingAddedTables(
	ctx context.Context, db *sql.DB, cfg *ControlTableConfig, q PendingAddedTablesQueries,
	streamID string, requireRow bool, edit func(map[string]struct{}),
) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s: update pending added tables: begin: %w", cfg.EngineName, err)
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
			return fmt.Errorf("%s: record pending added table: %w: %q", cfg.EngineName, cfg.ErrStreamNotFound, streamID)
		}
		return tx.Rollback()
	case !requireRow && (cfg.IsMissingTable(serr) || q.IsMissingColumn(serr)):
		return tx.Rollback()
	case serr != nil:
		return fmt.Errorf("%s: update pending added tables: %w", cfg.EngineName, serr)
	}
	current, err := DecodePendingAddedTables(stored.String)
	if err != nil {
		return fmt.Errorf("%s: update pending added tables of stream %q: %w", cfg.EngineName, streamID, err)
	}
	set := make(map[string]struct{}, len(current)+1)
	for _, t := range current {
		set[t] = struct{}{}
	}
	edit(set)
	names := make([]string, 0, len(set))
	for t := range set {
		names = append(names, t)
	}
	encoded := EncodePendingAddedTables(names)
	if encoded == EncodePendingAddedTables(current) && stored.Valid == (len(names) > 0) {
		return tx.Rollback() // nothing to change (a Clear of a table never recorded)
	}
	var value any
	if len(names) > 0 {
		value = encoded
	}
	if _, err := tx.ExecContext(ctx, q.Update, value, streamID); err != nil {
		return fmt.Errorf("%s: update pending added tables: %w", cfg.EngineName, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s: update pending added tables: commit: %w", cfg.EngineName, err)
	}
	return nil
}
