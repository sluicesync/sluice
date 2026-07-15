// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
)

// RowReader streams rows from a mydumper dump directory for the bulk-copy
// phase. It implements [ir.RowReader] with the same sticky-error contract
// the other engine readers use (Bug 68): a mid-chunk lex/decode failure
// closes the channel and surfaces via [Err] after the drain, so the
// orchestrator can distinguish "fully read" from "failed mid-table".
//
// It deliberately implements NONE of the batched/counter surfaces: dump
// chunks have no PK addressing, so the orchestrator must route every table
// through the single-reader whole-table copy (the D1RowReader posture —
// see capabilities_assert.go).
type RowReader struct {
	dir *dumpDir

	mu  sync.Mutex
	err error // sticky error from the most recent ReadRows call
}

// rowChanBuffer bounds the reader's output channel so decode can overlap
// the downstream write while preserving back-pressure. Mirrors the
// same-named constant in the other engine readers.
const rowChanBuffer = 64

// Close releases nothing — every chunk file is opened and closed within
// its ReadRows stream — but keeps the reader shape symmetric with the
// other engines (the pipeline closes readers via migcore.CloseIf).
func (r *RowReader) Close() error { return nil }

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

// ReadRows streams the rows of table over the returned channel, reading
// the table's data chunks in chunk-number order and decompressing on the
// fly. The channel closes when the table is fully read, when ctx is
// cancelled, or when a chunk fails validation/decoding (in which case
// [Err] returns the cause).
func (r *RowReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	if table == nil {
		return nil, errors.New("mydumper: ReadRows: table is nil")
	}
	if len(table.Columns) == 0 {
		return nil, fmt.Errorf("mydumper: ReadRows: table %q has no columns", table.Name)
	}
	tf := r.dir.tables[table.Name]
	if tf == nil {
		return nil, fmt.Errorf("mydumper: ReadRows: table %q has no schema file in %q", table.Name, r.dir.path)
	}

	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()

	warnIfSingleFloatColumns(table)

	out := make(chan ir.Row, rowChanBuffer)
	go func() {
		defer close(out)
		for _, chunk := range tf.chunks {
			if err := r.streamChunk(ctx, chunk, table, out); err != nil {
				if !errors.Is(err, context.Canceled) {
					r.setErr(err)
				}
				return
			}
		}
	}()
	return out, nil
}

// warnIfSingleFloatColumns surfaces the FLOAT display-rounding wart, once
// per table read: mydumper's bare SELECT renders single-precision FLOAT
// columns through mysqld's ~6-significant-digit float→text formatter
// (ground-truthed against mydumper v1.0.3: 8388608 dumps as 8.38861e6) —
// the dump-file sibling of the VStream-COPY FLOAT class
// ([ir.LossyFloatCopyReader], ADR-0153). The loss happened AT DUMP TIME,
// inside the file, so this reader cannot re-read exactly; the honest move
// is a loud WARN naming the columns (ADR-0161 §4). DOUBLE columns are
// unaffected (rendered at full double precision).
func warnIfSingleFloatColumns(table *ir.Table) {
	var floats []string
	for _, col := range table.Columns {
		if f, ok := col.Type.(ir.Float); ok && f.Precision == ir.FloatSingle {
			floats = append(floats, col.Name)
		}
	}
	if len(floats) == 0 {
		return
	}
	slog.Warn(
		"mydumper: single-precision FLOAT values in a mydumper dump are display-rounded to ~6 "+
			"significant digits by mysqld AT DUMP TIME (the dump file itself carries the rounded text); "+
			"values may differ from the live column in the 7th+ digit. Migrate from the live database "+
			"if exact FLOAT bits matter.",
		slog.String("table", table.Name),
		slog.Any("float_columns", floats),
	)
}

// streamChunk lexes one data-chunk file and emits its rows.
func (r *RowReader) streamChunk(ctx context.Context, path string, table *ir.Table, out chan<- ir.Row) error {
	return processChunk(ctx, path, table.Name, func(sc *insertScan, columns []string) error {
		targets, err := resolveInsertColumns(table, columns)
		if err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(path), err)
		}
		var vals []literal
		for {
			var done bool
			vals, done, err = sc.nextTuple(vals)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
			if len(vals) != len(targets) {
				return sc.errAt("tuple has %d values; table %s expects %d", len(vals), table.Name, len(targets))
			}
			row := make(ir.Row, len(targets))
			for i, col := range targets {
				v, err := literalToRowValue(vals[i], col)
				if err != nil {
					return fmt.Errorf("mydumper: %s: table %s: %w", filepath.Base(path), table.Name, err)
				}
				row[col.Name] = v
			}
			select {
			case out <- row:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
}

// processChunk opens one data-chunk file (decompressing by suffix),
// streams its statements, and dispatches: SET headers are validated
// (charset / time-zone posture), comments skipped, INSERT/REPLACE handed
// to onInsert (with the header already consumed and the statement's
// explicit column list, nil when bare), and ANY other statement refused
// loudly naming the file — a data chunk holds only extended INSERTs.
func processChunk(ctx context.Context, path, tableName string,
	onInsert func(sc *insertScan, columns []string) error,
) error {
	f, err := openDumpFile(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	base := filepath.Base(path)
	stream := newStatementStream(f, 0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		stmt, err := stream.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("mydumper: %s: %w", base, err)
		}
		switch kw := statementKeyword(stmt); kw {
		case "":
			// comment-only fragment
		case "SET":
			if err := checkSetStatement(stmt); err != nil {
				return fmt.Errorf("mydumper: %s: %w", base, err)
			}
		case "INSERT", "REPLACE":
			sc, stmtTable, columns, err := parseInsertHeader(stmt, base)
			if err != nil {
				return err
			}
			if stmtTable != tableName {
				return fmt.Errorf("mydumper: %s: chunk for table %s contains an INSERT into %s — "+
					"corrupt or mislabelled dump", base, tableName, stmtTable)
			}
			if err := onInsert(sc, columns); err != nil {
				return err
			}
		default:
			return fmt.Errorf("mydumper: %s: unexpected %s statement in a data chunk (only SET headers "+
				"and extended INSERTs are valid)", base, kw)
		}
	}
}

// resolveInsertColumns maps an INSERT statement's column list onto the
// table's IR columns, in statement order. A bare `INSERT INTO t VALUES`
// maps to every table column in declaration order (only legal when the
// table has no generated columns — MySQL itself rejects the bare form
// there, so its presence means a corrupt dump). An explicit list must
// name only known, non-generated columns and must COVER every
// non-generated column: a dump that omitted a column would silently land
// target-side defaults where the source had real values.
func resolveInsertColumns(table *ir.Table, columns []string) ([]*ir.Column, error) {
	byName := make(map[string]*ir.Column, len(table.Columns))
	nonGenerated := 0
	for _, c := range table.Columns {
		byName[c.Name] = c
		if !c.IsGenerated() {
			nonGenerated++
		}
	}

	if columns == nil {
		if nonGenerated != len(table.Columns) {
			return nil, fmt.Errorf("table %s has generated columns but the INSERT carries no column "+
				"list — not a shape MySQL (or mydumper) produces; corrupt dump", table.Name)
		}
		return append([]*ir.Column(nil), table.Columns...), nil
	}

	targets := make([]*ir.Column, len(columns))
	seen := make(map[string]bool, len(columns))
	for i, name := range columns {
		col := byName[name]
		if col == nil {
			return nil, fmt.Errorf("INSERT names column %q which table %s does not declare", name, table.Name)
		}
		if col.IsGenerated() {
			return nil, fmt.Errorf("INSERT supplies a value for generated column %s.%s — not a shape "+
				"mydumper produces; corrupt dump", table.Name, name)
		}
		if seen[name] {
			return nil, fmt.Errorf("INSERT names column %q twice", name)
		}
		seen[name] = true
		targets[i] = col
	}
	if len(seen) != nonGenerated {
		return nil, fmt.Errorf("INSERT into %s lists %d of the table's %d non-generated columns — an "+
			"omitted column would silently take the target default instead of the source value; refusing",
			table.Name, len(seen), nonGenerated)
	}
	return targets, nil
}

// countTableRows re-scans a table's chunks counting tuples (no value
// decode) — the verify count-depth surface for file sources (ADR-0161 §9).
func (d *dumpDir) countTableRows(ctx context.Context, tableName string) (int64, error) {
	tf := d.tables[tableName]
	if tf == nil {
		return 0, fmt.Errorf("mydumper: table %q has no schema file in %q", tableName, d.path)
	}
	var total int64
	for _, chunk := range tf.chunks {
		err := processChunk(ctx, chunk, tableName, func(sc *insertScan, _ []string) error {
			var vals []literal
			for {
				var done bool
				var err error
				vals, done, err = sc.nextTuple(vals)
				if err != nil {
					return err
				}
				if done {
					return nil
				}
				total++
			}
		})
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}
