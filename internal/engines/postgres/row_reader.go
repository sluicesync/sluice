package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/orware/sluice/internal/ir"
)

// RowReader streams rows from PostgreSQL tables for the bulk-copy
// phase. It implements [ir.RowReader].
//
// The reader holds an open *sql.DB; callers should call Close when
// done to release the connection pool. Errors during streaming are
// stored on the reader and accessible via Err after the row channel
// has closed (mirrors database/sql.Rows.Err).
type RowReader struct {
	db     *sql.DB
	schema string

	mu  sync.Mutex
	err error // sticky error from the most recent ReadRows call
}

// Close releases the underlying connection pool.
func (r *RowReader) Close() error {
	if r.db == nil {
		return nil
	}
	return r.db.Close()
}

// Err returns the error, if any, that terminated the most recently
// returned channel. It is only valid to call after the channel has
// been fully drained.
func (r *RowReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// ReadRows streams the rows of table over the returned channel. The
// channel closes when the table is fully read, when ctx is cancelled,
// or when an error occurs (in which case [Err] returns the cause).
//
// Callers must either fully drain the channel or cancel ctx — leaving
// a partially-read channel without cancellation will leak the
// streaming goroutine.
func (r *RowReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	if table == nil {
		return nil, errors.New("postgres: ReadRows: table is nil")
	}
	if len(table.Columns) == 0 {
		return nil, fmt.Errorf("postgres: ReadRows: table %q has no columns", table.Name)
	}

	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()

	query := buildSelect(r.schema, table)
	// rowserrcheck and sqlclosecheck can't follow rows into the
	// goroutine; both rows.Err() and rows.Close() are handled inside
	// stream() (Close via defer, Err checked once iteration ends).
	rows, err := r.db.QueryContext(ctx, query) //nolint:rowserrcheck,sqlclosecheck
	if err != nil {
		return nil, fmt.Errorf("postgres: ReadRows: query failed: %w", err)
	}

	out := make(chan ir.Row)
	go r.stream(ctx, rows, table, out)
	return out, nil
}

// stream is the goroutine that scans rows from the database and pushes
// them onto out as IR Rows. It owns rows and is responsible for
// closing it; out is owned by stream and is closed before stream exits.
func (r *RowReader) stream(ctx context.Context, rows *sql.Rows, table *ir.Table, out chan<- ir.Row) {
	defer close(out)
	defer func() { _ = rows.Close() }()

	cols := table.Columns
	scanBuf := make([]any, len(cols))
	scanPtrs := make([]any, len(cols))
	for i := range scanBuf {
		scanPtrs[i] = &scanBuf[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			r.setErr(fmt.Errorf("postgres: scan: %w", err))
			return
		}

		row := make(ir.Row, len(cols))
		for i, col := range cols {
			v, err := decodeValue(scanBuf[i], col.Type)
			if err != nil {
				r.setErr(fmt.Errorf("postgres: column %q: %w", col.Name, err))
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
		r.setErr(fmt.Errorf("postgres: rows iteration: %w", err))
	}
}

func (r *RowReader) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

// buildSelect produces a SELECT statement that fetches all columns of
// table in the column order declared on the IR. The table is
// schema-qualified (Postgres has namespaced schemas, unlike MySQL).
// Identifiers are double-quoted with internal quotes escaped.
func buildSelect(schema string, table *ir.Table) string {
	cols := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		cols[i] = quoteIdent(c.Name)
	}
	tableRef := quoteIdent(schema) + "." + quoteIdent(table.Name)
	return fmt.Sprintf(
		"SELECT %s FROM %s",
		strings.Join(cols, ", "),
		tableRef,
	)
}

// quoteIdent double-quotes a Postgres identifier. Internal double
// quotes are escaped by doubling — the canonical form for identifier
// quoting in standard SQL.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
