package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// defaultMaxRowsPerBatch caps how many rows go into a single INSERT
// statement. Conservative for now; can tune with real-world data.
const defaultMaxRowsPerBatch = 500

// RowWriter performs bulk inserts into PostgreSQL tables. It implements
// [ir.RowWriter].
//
// First-cut strategy is BatchedInsert (multi-row INSERT with $N
// placeholders) regardless of the engine's declared BulkLoad
// capability. The Capability declaration is BulkLoadCopy, which would
// use Postgres's native COPY protocol via pgx for higher throughput;
// that path is a planned follow-up.
type RowWriter struct {
	db     *sql.DB
	schema string

	// maxRowsPerBatch caps the number of rows folded into a single
	// INSERT. Tests can override; callers leave it at zero (which
	// causes defaultMaxRowsPerBatch to be used).
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
// via batched multi-row INSERT statements. See [ir.RowWriter.WriteRows]
// for the contract.
func (w *RowWriter) WriteRows(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	if table == nil {
		return errors.New("postgres: WriteRows: table is nil")
	}
	if len(table.Columns) == 0 {
		return fmt.Errorf("postgres: WriteRows: table %q has no columns", table.Name)
	}
	if rows == nil {
		return errors.New("postgres: WriteRows: rows channel is nil")
	}

	limit := w.maxRowsPerBatch
	if limit <= 0 {
		limit = defaultMaxRowsPerBatch
	}

	batch := make([]ir.Row, 0, limit)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		query := buildBatchInsert(w.schema, table, len(batch))
		args, err := flattenArgs(batch, table)
		if err != nil {
			return fmt.Errorf("postgres: prepare args for %q: %w", table.Name, err)
		}
		if _, err := w.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("postgres: insert into %q (%d rows): %w", table.Name, len(batch), err)
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
// given table and row count. Postgres uses $1, $2, ... placeholders
// (numbered, not positional like MySQL's ?).
//
// The numbering is global across rows: row 1 is $1..$N, row 2 is
// $(N+1)..$(2N), etc.
func buildBatchInsert(schema string, table *ir.Table, rowCount int) string {
	colNames := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		colNames[i] = quoteIdent(c.Name)
	}

	numCols := len(table.Columns)
	rowParts := make([]string, rowCount)
	paramIdx := 0
	for i := range rowParts {
		params := make([]string, numCols)
		for j := range params {
			paramIdx++
			params[j] = fmt.Sprintf("$%d", paramIdx)
		}
		rowParts[i] = "(" + strings.Join(params, ", ") + ")"
	}

	tableRef := quoteIdent(schema) + "." + quoteIdent(table.Name)
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		tableRef,
		strings.Join(colNames, ", "),
		strings.Join(rowParts, ", "),
	)
}

// flattenArgs walks the batch column-major-by-row and produces the
// flat []any the driver expects, with each value passed through
// prepareValue for any IR-canonical → driver-acceptable adjustments.
func flattenArgs(batch []ir.Row, table *ir.Table) ([]any, error) {
	args := make([]any, 0, len(batch)*len(table.Columns))
	for _, row := range batch {
		for _, col := range table.Columns {
			v, err := prepareValue(row[col.Name], col.Type)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", col.Name, err)
			}
			args = append(args, v)
		}
	}
	return args, nil
}

// prepareValue adjusts an IR Row value into a form pgx will accept.
//
// Most IR-canonical Go values pass through to pgx unchanged: bool,
// int64, float64, string, []byte, time.Time, and nil all serialise
// correctly without intervention. The exceptions are:
//
//   - [ir.Array] values, whose canonical Go form is []any; pgx wants
//     a typed slice for native array binding. We convert based on the
//     element type.
//   - Nothing else (Postgres handles the rest natively via pgx).
//
// Returning an error here means the IR value didn't match the
// declared column type — usually a translator bug upstream.
func prepareValue(v any, t ir.Type) (any, error) {
	if v == nil {
		return nil, nil
	}

	if arr, isArr := t.(ir.Array); isArr {
		any, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("expected []any for Array column, got %T", v)
		}
		return convertArray(any, arr.Element)
	}

	return v, nil
}

// convertArray turns []any (the IR canonical form for arrays) into a
// typed Go slice that pgx can serialise as a Postgres array.
//
// We support the element types most common in practice. For others,
// the function returns an error so the upstream caller knows to
// translate first.
func convertArray(v []any, elem ir.Type) (any, error) {
	switch elem.(type) {
	case ir.Boolean:
		out := make([]bool, len(v))
		for i, e := range v {
			b, ok := e.(bool)
			if !ok {
				return nil, fmt.Errorf("array element %d: expected bool, got %T", i, e)
			}
			out[i] = b
		}
		return out, nil
	case ir.Integer:
		out := make([]int64, len(v))
		for i, e := range v {
			n, ok := e.(int64)
			if !ok {
				return nil, fmt.Errorf("array element %d: expected int64, got %T", i, e)
			}
			out[i] = n
		}
		return out, nil
	case ir.Float:
		out := make([]float64, len(v))
		for i, e := range v {
			f, ok := e.(float64)
			if !ok {
				return nil, fmt.Errorf("array element %d: expected float64, got %T", i, e)
			}
			out[i] = f
		}
		return out, nil
	case ir.Char, ir.Varchar, ir.Text, ir.UUID, ir.Inet, ir.Cidr, ir.Macaddr, ir.Decimal, ir.Time:
		out := make([]string, len(v))
		for i, e := range v {
			s, ok := e.(string)
			if !ok {
				return nil, fmt.Errorf("array element %d: expected string, got %T", i, e)
			}
			out[i] = s
		}
		return out, nil
	}
	return nil, fmt.Errorf("postgres: array of element type %T not supported", elem)
}
