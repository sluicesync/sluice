package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// defaultMaxRowsPerBatch caps how many rows go into a single INSERT
// statement. The cap is conservative to stay well under MySQL's
// default max_allowed_packet (16 MiB pre-8.0, 64 MiB on 8.0+) for
// realistic row sizes; tables with very wide rows can override.
//
// PlanetScale's pscale-cli dumper batches by ~1 MB of statement
// body rather than row count; using row count here is simpler and
// works fine for the typical migration shape. We can switch to
// byte-driven batching when the BulkLoadLoadDataInfile strategy
// lands and we have a real performance baseline to tune against.
const defaultMaxRowsPerBatch = 500

// RowWriter performs bulk inserts into MySQL tables. It implements
// [ir.RowWriter].
//
// The writer chooses a backend strategy at construction time based on
// the engine's declared [ir.BulkLoadMethod]:
//
//   - BulkLoadBatchedInsert: accumulates rows into multi-row INSERT
//     statements via prepared parameter placeholders. Used by
//     PlanetScale (which doesn't support LOAD DATA INFILE) and as the
//     fallback for vanilla MySQL until the LOAD DATA INFILE path lands.
//   - BulkLoadLoadDataInfile: TODO. Currently falls through to
//     BatchedInsert with no functional difference.
//
// The writer holds an open *sql.DB; callers should call Close when
// finished to release the connection pool.
type RowWriter struct {
	db       *sql.DB
	schema   string
	bulkLoad ir.BulkLoadMethod

	// maxRowsPerBatch caps the number of rows folded into a single
	// INSERT statement. Tests can override it; callers typically
	// leave it as the zero value, in which case defaultMaxRowsPerBatch
	// is used.
	maxRowsPerBatch int
}

// Close releases the underlying connection pool.
func (w *RowWriter) Close() error {
	if w.db == nil {
		return nil
	}
	return w.db.Close()
}

// WriteRows consumes rows from the channel and inserts them into table
// using the strategy chosen at construction time. The method returns
// when the channel is closed (success) or when ctx is cancelled / a
// DB error occurs (failure).
//
// Caller responsibilities:
//   - Provide the channel; the writer drains it.
//   - Cancel ctx if iteration should stop early; without cancellation,
//     a writer with a partially-drained channel will block.
//   - Ensure table accurately describes the column types of the rows.
//     The writer trusts the [ir.Type] on each column to decide value
//     preparation (notably, []string-to-CSV for Set columns).
func (w *RowWriter) WriteRows(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	if table == nil {
		return fmt.Errorf("mysql: WriteRows: table is nil")
	}
	if len(table.Columns) == 0 {
		return fmt.Errorf("mysql: WriteRows: table %q has no columns", table.Name)
	}
	if rows == nil {
		return fmt.Errorf("mysql: WriteRows: rows channel is nil")
	}

	switch w.bulkLoad {
	case ir.BulkLoadBatchedInsert, ir.BulkLoadLoadDataInfile:
		// LoadDataInfile falls through to BatchedInsert until the
		// dedicated path lands. This keeps the surface honest:
		// vanilla MySQL still works (slower than ideal), and the
		// switch-on-capability shape stays in place for the day we
		// add LoadDataInfile.
		return w.writeBatched(ctx, table, rows)
	case ir.BulkLoadNone:
		return fmt.Errorf("mysql: WriteRows: engine declares BulkLoad=None; cannot write rows")
	default:
		return fmt.Errorf("mysql: WriteRows: unknown BulkLoadMethod %v", w.bulkLoad)
	}
}

// writeBatched buffers rows up to maxRowsPerBatch and flushes them as
// a single multi-row INSERT statement using parameter placeholders.
// Letting the driver handle parameter encoding sidesteps the
// per-type escaping problems that custom SQL generation would face.
func (w *RowWriter) writeBatched(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	limit := w.maxRowsPerBatch
	if limit <= 0 {
		limit = defaultMaxRowsPerBatch
	}

	batch := make([]ir.Row, 0, limit)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		query := buildBatchInsert(table, len(batch))
		args := flattenArgs(batch, table)
		if _, err := w.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("mysql: insert into %q (%d rows): %w", table.Name, len(batch), err)
		}
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case row, ok := <-rows:
			if !ok {
				return flush()
			}
			batch = append(batch, row)
			if len(batch) >= limit {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// buildBatchInsert returns the parameterised INSERT statement for the
// given table and row count. Identifiers are backtick-quoted; values
// are placeholders (`?`) for the driver to fill in.
func buildBatchInsert(table *ir.Table, rowCount int) string {
	colNames := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		colNames[i] = quoteIdent(c.Name)
	}

	rowPart := buildRowPlaceholder(len(table.Columns))
	rowParts := make([]string, rowCount)
	for i := range rowParts {
		rowParts[i] = rowPart
	}

	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		quoteIdent(table.Name),
		strings.Join(colNames, ", "),
		strings.Join(rowParts, ", "),
	)
}

// buildRowPlaceholder returns a single row's "(?, ?, ...)" placeholder
// fragment for a row with the given column count. Returns "()" for a
// zero-column row, which is invalid SQL — the caller should validate
// upstream.
func buildRowPlaceholder(numCols int) string {
	if numCols <= 0 {
		return "()"
	}
	if numCols == 1 {
		return "(?)"
	}
	return "(" + strings.Repeat("?, ", numCols-1) + "?)"
}

// flattenArgs walks the batch column-major-by-row and produces the
// flat []any the driver expects. Values are passed through prepareValue
// to handle the IR-Set-to-string conversion and similar adjustments.
func flattenArgs(batch []ir.Row, table *ir.Table) []any {
	args := make([]any, 0, len(batch)*len(table.Columns))
	for _, row := range batch {
		for _, col := range table.Columns {
			args = append(args, prepareValue(row[col.Name], col.Type))
		}
	}
	return args
}

// prepareValue adjusts a Row value to a form the driver accepts.
//
// Most IR-canonical Go values pass through to go-sql-driver/mysql
// unchanged: int64, uint64, float64, string, []byte, bool, time.Time,
// and nil all serialise correctly without intervention. The exceptions:
//
//   - [ir.Set] values are []string in IR but MySQL expects a
//     comma-separated string literal.
//   - [ir.Geometry] values are raw WKB bytes (per docs/value-types.md);
//     MySQL's wire format is `<srid uint32 LE><wkb>`. We prepend the
//     SRID using the column's declared SRID (or 0 when unset).
//   - [ir.JSON] values are []byte in IR (the raw JSON document
//     per docs/value-types.md). go-sql-driver/mysql labels []byte
//     parameters with `_binary` charset on the wire, which Vitess
//     rejects with "Cannot create a JSON value from a string with
//     CHARACTER SET 'binary'" when the destination column is JSON.
//     Convert to a Go string so the driver sends VARCHAR (no
//     charset prefix) and MySQL/Vitess parses it as JSON cleanly.
//     Real-world bug found during PlanetScale-target testing.
func prepareValue(v any, t ir.Type) any {
	if v == nil {
		return nil
	}
	if _, isSet := t.(ir.Set); isSet {
		if ss, ok := v.([]string); ok {
			return strings.Join(ss, ",")
		}
	}
	if _, isJSON := t.(ir.JSON); isJSON {
		if b, ok := v.([]byte); ok {
			return string(b)
		}
	}
	if geom, isGeom := t.(ir.Geometry); isGeom {
		if b, ok := v.([]byte); ok {
			out := make([]byte, 4+len(b))
			// Little-endian uint32 SRID prefix, matching MySQL's
			// on-wire geometry layout.
			srid := uint32(geom.SRID)
			out[0] = byte(srid)
			out[1] = byte(srid >> 8)
			out[2] = byte(srid >> 16)
			out[3] = byte(srid >> 24)
			copy(out[4:], b)
			return out
		}
	}
	return v
}
