package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/orware/sluice/internal/ir"
)

// querier is the slice of database/sql RowReader needs from its query
// source. Both *sql.DB (the simple-mode path, owns its own pool) and
// *sql.Conn (the snapshot-mode path, holds a single pinned connection
// running a long REPEATABLE-READ transaction) satisfy it.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// RowReader streams rows from MySQL tables for the bulk-copy phase.
// It implements [ir.RowReader].
//
// Errors that occur during streaming are stored on the reader and
// accessible via Err after the row channel has closed (mirrors
// database/sql.Rows.Err).
type RowReader struct {
	q      querier
	schema string

	// closer owns the underlying connection resources for this reader.
	// In simple mode it's the *sql.DB; in snapshot mode it's nil and
	// the SnapshotStream owns the lifecycle. Close is a no-op when nil.
	closer io.Closer

	mu  sync.Mutex
	err error // sticky error from the most recent ReadRows call
}

// Close releases the underlying connection resources. Safe to call
// multiple times. In snapshot mode (closer==nil) this is a no-op —
// the SnapshotStream's Close is the operative cleanup.
func (r *RowReader) Close() error {
	if r.closer == nil {
		return nil
	}
	return r.closer.Close()
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
// a partially-read channel without cancellation will leak the streaming
// goroutine.
func (r *RowReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	if table == nil {
		return nil, fmt.Errorf("mysql: ReadRows: table is nil")
	}
	if len(table.Columns) == 0 {
		return nil, fmt.Errorf("mysql: ReadRows: table %q has no columns", table.Name)
	}

	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()

	query := buildSelect(table)
	// rowserrcheck and sqlclosecheck can't follow rows into the
	// goroutine; both rows.Err() and rows.Close() are handled inside
	// stream() (Close via defer, Err checked once iteration ends).
	rows, err := r.q.QueryContext(ctx, query) //nolint:rowserrcheck,sqlclosecheck
	if err != nil {
		return nil, fmt.Errorf("mysql: ReadRows: query failed: %w", err)
	}

	out := make(chan ir.Row)
	go r.stream(ctx, rows, table, out)
	return out, nil
}

// stream is the goroutine that scans rows from the database and pushes
// them onto out as IR Rows. It owns rows and is responsible for closing
// it; out is owned by stream and is closed before stream exits.
func (r *RowReader) stream(ctx context.Context, rows *sql.Rows, table *ir.Table, out chan<- ir.Row) {
	defer close(out)
	defer rows.Close()

	cols := table.Columns
	scanBuf := make([]any, len(cols))
	scanPtrs := make([]any, len(cols))
	for i := range scanBuf {
		scanPtrs[i] = &scanBuf[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			r.setErr(fmt.Errorf("mysql: scan: %w", err))
			return
		}

		row := make(ir.Row, len(cols))
		for i, col := range cols {
			v, err := decodeValue(scanBuf[i], col.Type)
			if err != nil {
				r.setErr(fmt.Errorf("mysql: column %q: %w", col.Name, err))
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
		r.setErr(fmt.Errorf("mysql: rows iteration: %w", err))
	}
}

func (r *RowReader) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

// buildSelect produces a SELECT statement that fetches all columns of
// table in the column order declared on the IR. MySQL identifiers are
// quoted with backticks; any backticks within identifiers (rare) are
// escaped by doubling.
func buildSelect(table *ir.Table) string {
	cols := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		cols[i] = quoteIdent(c.Name)
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s",
		strings.Join(cols, ", "),
		quoteIdent(table.Name),
	)
}

// quoteIdent backtick-quotes a MySQL identifier. Backticks within the
// identifier are escaped by doubling — the only escape MySQL recognises
// inside a backtick-quoted identifier.
func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
