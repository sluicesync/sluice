// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// ProbeReplayKey implements [ir.ReplayKeyProber]: it reports whether the
// target table exists and, if so, whether a re-applied INSERT into it
// collides on a key instead of appending a second copy of the row.
//
// It reads the live catalog into a minimal [ir.Table] (column nullability,
// the PRIMARY KEY, every UNIQUE index with its key parts) and hands that to
// [effectiveUpsertKeyColumns] — the engine's own upsert-key picker, which
// TestReplayKeyPredicatesAgree already binds to irbackup.TableReplayIdempotent.
// So the target and the recorded schema are judged by one predicate.
//
// Why not the applier's batching probe ([ChangeApplier.tableIsKeyless]),
// which counts any UNIQUE index: that predicate is about ADR-0089 batch
// sizing and is deliberately loose. ON DUPLICATE KEY UPDATE fires on a
// nullable UNIQUE index only for rows whose key parts are all non-NULL, so a
// re-applied row with a NULL in it is appended again. A door against silent
// duplication needs the strict answer: a PRIMARY KEY, or a UNIQUE index whose
// every key part is a NOT NULL column (a functional key part — COLUMN_NAME
// NULL in STATISTICS — never qualifies).
func (w *RowWriter) ProbeReplayKey(ctx context.Context, table *ir.Table) (exists, keyed bool, err error) {
	if table == nil {
		return false, false, errors.New("mysql: ProbeReplayKey: table is nil")
	}
	name := table.Name
	const existsQ = `SELECT COUNT(*) FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = COALESCE(NULLIF(?, ''), DATABASE()) AND TABLE_NAME = ?`
	var n int
	if err := w.db.QueryRowContext(ctx, existsQ, w.schema, name).Scan(&n); err != nil {
		return false, false, fmt.Errorf("mysql: probe replay key for %q: existence: %w", name, err)
	}
	if n == 0 {
		return false, false, nil
	}
	live, err := w.loadKeyShape(ctx, name)
	if err != nil {
		return true, false, err
	}
	_, keyed = effectiveUpsertKeyColumns(live)
	return true, keyed, nil
}

// loadKeyShape reads exactly the parts of a target table the upsert-key
// picker consults: each column's nullability and every UNIQUE index (the
// PRIMARY KEY included) in key-part order.
func (w *RowWriter) loadKeyShape(ctx context.Context, name string) (*ir.Table, error) {
	t := &ir.Table{Name: name}

	const colsQ = `SELECT COLUMN_NAME, IS_NULLABLE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = COALESCE(NULLIF(?, ''), DATABASE()) AND TABLE_NAME = ?`
	cols, err := w.loadNullability(ctx, colsQ, name)
	if err != nil {
		return nil, err
	}
	t.Columns = cols

	const idxQ = `SELECT INDEX_NAME, COLUMN_NAME FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = COALESCE(NULLIF(?, ''), DATABASE()) AND TABLE_NAME = ? AND NON_UNIQUE = 0
		ORDER BY INDEX_NAME, SEQ_IN_INDEX`
	irows, err := w.db.QueryContext(ctx, idxQ, w.schema, name)
	if err != nil {
		return nil, fmt.Errorf("mysql: probe replay key for %q: unique indexes: %w", name, err)
	}
	defer func() { _ = irows.Close() }()
	byName := map[string]*ir.Index{}
	for irows.Next() {
		var idxName string
		var col sql.NullString
		if err := irows.Scan(&idxName, &col); err != nil {
			return nil, fmt.Errorf("mysql: probe replay key for %q: unique indexes: %w", name, err)
		}
		idx, ok := byName[idxName]
		if !ok {
			idx = &ir.Index{Name: idxName, Unique: true}
			byName[idxName] = idx
			if idxName == "PRIMARY" {
				t.PrimaryKey = idx
			} else {
				t.Indexes = append(t.Indexes, idx)
			}
		}
		part := ir.IndexColumn{Column: col.String}
		if !col.Valid {
			// A functional key part: STATISTICS carries the expression, not
			// a column. The picker refuses any part with an Expression.
			part = ir.IndexColumn{Expression: "functional key part"}
		}
		idx.Columns = append(idx.Columns, part)
	}
	if err := irows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: probe replay key for %q: unique indexes: %w", name, err)
	}
	return t, nil
}

// loadNullability reads each column's name and nullability for
// [RowWriter.loadKeyShape].
func (w *RowWriter) loadNullability(ctx context.Context, q, name string) ([]*ir.Column, error) {
	rows, err := w.db.QueryContext(ctx, q, w.schema, name)
	if err != nil {
		return nil, fmt.Errorf("mysql: probe replay key for %q: columns: %w", name, err)
	}
	defer func() { _ = rows.Close() }()
	var out []*ir.Column
	for rows.Next() {
		var col, nullable string
		if err := rows.Scan(&col, &nullable); err != nil {
			return nil, fmt.Errorf("mysql: probe replay key for %q: columns: %w", name, err)
		}
		out = append(out, &ir.Column{Name: col, Nullable: nullable != "NO"})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: probe replay key for %q: columns: %w", name, err)
	}
	return out, nil
}
