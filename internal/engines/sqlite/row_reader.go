// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
)

// RowReader streams rows from SQLite tables for the bulk-copy phase. It
// implements [ir.RowReader]. Each cell is decoded by its ACTUAL storage
// class (see value_decode.go); a value whose class cannot be faithfully
// represented in the column's resolved IR type aborts the read with a
// loud error naming the table, column, rowid, and offending class —
// surfaced via [Err] after the channel closes, the same sticky-error
// contract the other engines' readers use (Bug 68).
type RowReader struct {
	db   *sql.DB
	path string

	// dateEnc is the per-source temporal value encoding (ADR-0129),
	// resolved from the `sqlite_date_encoding` DSN param at OpenRowReader.
	// dateEncodingInherit (the zero value) defers to the process-global
	// default at decode time via [resolveDateEncoding].
	dateEnc dateEncoding

	// tempPath is the materialized dump DB this reader owns, removed on Close
	// (ADR-0130). Empty when the source was a real binary `.db`.
	tempPath string

	mu  sync.Mutex
	err error // sticky error from the most recent ReadRows call
}

// rowChanBuffer bounds the reader's output channel so decode can overlap
// the downstream write while preserving back-pressure. Mirrors the
// same-named constant in the other engine readers.
const rowChanBuffer = 64

// Close releases the underlying connection pool and, for a materialized `.sql`
// dump, removes the temp DB after the pool is closed (the file handle must be
// released first, which matters on Windows). A `.db` source removes nothing.
// Safe to call multiple times.
func (r *RowReader) Close() error {
	if r.db == nil {
		return nil
	}
	err := r.db.Close()
	if r.tempPath != "" {
		// Clear the path first so a repeated Close is a no-op (the contract),
		// not a remove of an already-gone file.
		path := r.tempPath
		r.tempPath = ""
		if rmErr := os.Remove(path); rmErr != nil && err == nil {
			err = rmErr
		}
	}
	return err
}

// Err returns the error, if any, that terminated the most recently
// returned channel. Only valid after the channel has been fully drained.
func (r *RowReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *RowReader) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

// ReadRows streams the rows of table over the returned channel. The
// channel closes when the table is fully read, when ctx is cancelled, or
// when a value fails the storage-class fidelity check (in which case
// [Err] returns the cause). Callers must drain the channel or cancel ctx.
func (r *RowReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	if table == nil {
		return nil, errors.New("sqlite: ReadRows: table is nil")
	}
	if len(table.Columns) == 0 {
		return nil, fmt.Errorf("sqlite: ReadRows: table %q has no columns", table.Name)
	}

	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()

	hasRowid := r.tableHasRowid(ctx, table.Name)
	query := buildSelect(table, hasRowid)
	// rowserrcheck/sqlclosecheck can't follow rows into the streaming
	// goroutine; both rows.Err() and rows.Close() are handled inside
	// stream() (Close via defer, Err checked once iteration ends).
	rows, err := r.db.QueryContext(ctx, query) //nolint:rowserrcheck,sqlclosecheck
	if err != nil {
		return nil, fmt.Errorf("sqlite: ReadRows: query %q failed: %w", table.Name, err)
	}

	out := make(chan ir.Row, rowChanBuffer)
	go r.stream(ctx, rows, table, hasRowid, out)
	return out, nil
}

// stream scans rows and pushes decoded IR Rows onto out. It owns rows
// (closes it) and out (closes it before returning).
func (r *RowReader) stream(ctx context.Context, rows *sql.Rows, table *ir.Table, hasRowid bool, out chan<- ir.Row) {
	defer close(out)
	defer func() { _ = rows.Close() }()

	cols := table.Columns
	// scanBuf layout: [rowid?] + one slot per column, all scanned as *any
	// so modernc hands back each value's native storage-class Go type.
	width := len(cols)
	rowidIdx := -1
	if hasRowid {
		rowidIdx = 0
		width++
	}
	scanBuf := make([]any, width)
	scanPtrs := make([]any, width)
	for i := range scanBuf {
		scanPtrs[i] = &scanBuf[i]
	}

	// Resolve the per-source temporal encoding once (inherit → the
	// process-global default); decodeCell receives a concrete encoding.
	enc := resolveDateEncoding(r.dateEnc)

	ordinal := int64(0)
	for rows.Next() {
		ordinal++
		if err := rows.Scan(scanPtrs...); err != nil {
			r.setErr(fmt.Errorf("sqlite: table %q: scan: %w", table.Name, err))
			return
		}

		rowID := ordinal
		if rowidIdx >= 0 {
			if id, ok := scanBuf[rowidIdx].(int64); ok {
				rowID = id
			}
		}

		row := make(ir.Row, len(cols))
		for i, col := range cols {
			raw := scanBuf[i]
			if rowidIdx >= 0 {
				raw = scanBuf[i+1]
			}
			v, err := decodeCell(raw, col.Type, enc)
			if err != nil {
				r.setErr(fmt.Errorf("sqlite: table %q column %q rowid %d: %w",
					table.Name, col.Name, rowID, err))
				return
			}
			row[col.Name] = v
		}

		select {
		case out <- row:
		case <-ctx.Done():
			r.setErr(ctx.Err())
			return
		}
	}

	if err := rows.Err(); err != nil {
		r.setErr(fmt.Errorf("sqlite: table %q: rows iteration: %w", table.Name, err))
	}
}

// tableHasRowid reports whether the table exposes a `rowid` (true for
// ordinary tables, false for WITHOUT ROWID tables). A cheap probe —
// `SELECT rowid ... LIMIT 1` errors on a WITHOUT ROWID table — lets the
// reader include the rowid in fidelity-refusal messages when available
// and fall back to an ordinal otherwise.
func (r *RowReader) tableHasRowid(ctx context.Context, table string) bool {
	var discard any
	err := r.db.QueryRowContext(ctx, "SELECT rowid FROM "+quoteIdent(table)+" LIMIT 1").Scan(&discard)
	// nil → the column exists and a row was read; ErrNoRows → it exists but
	// the table is empty. A "no such column: rowid" error (WITHOUT ROWID
	// table) returns false.
	return err == nil || errors.Is(err, sql.ErrNoRows)
}

// buildSelect produces a SELECT over every column of table in declaration
// order, optionally prefixed with rowid. Identifiers are double-quoted
// with embedded quotes doubled. Temporal columns are wrapped by
// [selectColumnExpr] so the driver hands back their RAW storage class.
func buildSelect(table *ir.Table, hasRowid bool) string {
	parts := make([]string, 0, len(table.Columns)+1)
	if hasRowid {
		parts = append(parts, "rowid")
	}
	for _, c := range table.Columns {
		parts = append(parts, selectColumnExpr(c))
	}
	return "SELECT " + strings.Join(parts, ", ") + " FROM " + quoteIdent(table.Name)
}

// selectColumnExpr renders the SELECT expression for one column. A column
// whose resolved IR type is temporal (ir.Date / ir.Timestamp / ir.Time) is
// wrapped in coalesce(col, col) — a NAMED WART:
//
// modernc.org/sqlite UNCONDITIONALLY parses TEXT stored in a column DECLARED
// exactly DATE / DATETIME / TIMESTAMP into a Go time.Time on the
// interface{} scan path (rows.go, no DSN off-switch in v1.53.0). That would
// pre-empt sluice's explicit --sqlite-date-encoding decode using the
// driver's own layout set — making the driver, not value_decode.go, the
// authority on temporal values (and diverging the production path from the
// unit pins). coalesce(col, col) returns the value and storage class
// UNCHANGED while making the result an EXPRESSION with no declared type, so
// sqlite3_column_decltype is empty, the driver returns the raw storage class,
// and value_decode.go applies the operator's encoding (ADR-0129). NULL stays
// NULL (coalesce(NULL,NULL)=NULL). It is exercised by
// TestRealDriver_TemporalEncodings.
func selectColumnExpr(c *ir.Column) string {
	switch c.Type.(type) {
	case ir.Date, ir.Timestamp, ir.Time:
		q := quoteIdent(c.Name)
		return "coalesce(" + q + ", " + q + ") AS " + q
	default:
		return quoteIdent(c.Name)
	}
}

// quoteIdent double-quotes a SQLite identifier, escaping internal double
// quotes by doubling.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
