// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// floatRepairBatchRows bounds how many per-row UPDATEs a single target
// transaction folds in the FLOAT re-read repair. The repair is a rare
// corrective pass (only after a VStream cold-start COPY of a FLOAT
// column), so this trades a modest replay window on a mid-pass crash for
// far fewer commit round-trips than autocommit-per-row on a cross-region
// PlanetScale target — the same order of magnitude the apply-batch cap
// uses. Each UPDATE is idempotent (SET float WHERE pk), so a re-run of the
// last uncommitted batch lands the same values.
const floatRepairBatchRows = 500

// UpdateFloatColumnsByPK implements [ir.FloatRepairWriter]. It corrects
// the single-precision FLOAT columns a VStream cold-start COPY landed
// display-rounded (roadmap open-bug 2026-07-09): the pipeline re-reads
// those columns EXACTLY from the source over SQL and streams (PK + FLOAT)
// rows here, one `UPDATE <table> SET <float...> WHERE <pk...>` per row.
//
// Each row's PK-column values drive the WHERE (reusing the applier's
// NULL-aware predicate builder); the remaining keys are the FLOAT columns
// SET on the matched row (reusing the applier's SET builder + value
// shaping, so a FLOAT-mapped-to-DOUBLE target column is handled exactly as
// CDC handles it). A row absent on the target (deleted between COPY and
// re-read) is a zero-rows-affected no-op — NOT an error; the subsequent
// CDC replay from the copy anchor is the authority on such rows.
//
// A FLOAT column that is ALSO a PK member is left in the WHERE only (never
// SET): a float PK cannot be repaired by keying on itself, and the caller
// already excludes such columns from the re-read's FLOAT set. If a row
// arrives with only PK keys (no FLOAT column to set), it is skipped.
func (w *RowWriter) UpdateFloatColumnsByPK(ctx context.Context, table *ir.Table, pkColumns []string, rows <-chan ir.Row) error {
	if table == nil {
		return errors.New("mysql: UpdateFloatColumnsByPK: table is nil")
	}
	if len(pkColumns) == 0 {
		return fmt.Errorf("mysql: UpdateFloatColumnsByPK: table %q has no primary key columns", table.Name)
	}
	colTypes := colTypesByName(table.Columns)
	pkSet := make(map[string]struct{}, len(pkColumns))
	for _, c := range pkColumns {
		pkSet[c] = struct{}{}
	}
	tableRef := w.qualifiedRef(table.Name)

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mysql: UpdateFloatColumnsByPK: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	inBatch := 0
	for row := range rows {
		before := make(ir.Row, len(pkColumns))
		for _, pk := range pkColumns {
			before[pk] = row[pk]
		}
		after := make(ir.Row, len(row))
		for k, v := range row {
			if _, isPK := pkSet[k]; !isPK {
				after[k] = v
			}
		}
		if len(after) == 0 {
			// Nothing to set (the FLOAT column was PK-only); the caller
			// filters these, but stay defensive rather than emit an empty
			// SET clause.
			continue
		}

		setSQL, setArgs, err := buildSetClause(after, colTypes)
		if err != nil {
			return fmt.Errorf("mysql: UpdateFloatColumnsByPK: %s: build SET: %w", table.Name, err)
		}
		whereSQL, whereArgs, err := buildWhereClause(before, colTypes)
		if err != nil {
			return fmt.Errorf("mysql: UpdateFloatColumnsByPK: %s: build WHERE: %w", table.Name, err)
		}
		args := make([]any, 0, len(setArgs)+len(whereArgs))
		args = append(args, setArgs...)
		args = append(args, whereArgs...)
		if _, err := tx.ExecContext(ctx, "UPDATE "+tableRef+" SET "+setSQL+" WHERE "+whereSQL, args...); err != nil {
			return fmt.Errorf("mysql: UpdateFloatColumnsByPK: %s: exec: %w", table.Name, err)
		}

		inBatch++
		if inBatch >= floatRepairBatchRows {
			if err := tx.Commit(); err != nil {
				committed = true // the deferred rollback must not run on a committed tx
				return fmt.Errorf("mysql: UpdateFloatColumnsByPK: %s: commit: %w", table.Name, err)
			}
			if tx, err = w.db.BeginTx(ctx, nil); err != nil {
				committed = true
				return fmt.Errorf("mysql: UpdateFloatColumnsByPK: %s: begin next batch: %w", table.Name, err)
			}
			inBatch = 0
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mysql: UpdateFloatColumnsByPK: %s: final commit: %w", table.Name, err)
	}
	committed = true
	return nil
}

// qualifiedRef renders the backtick-quoted target table reference,
// qualifying by the writer's database when one is set (the same
// empty-schema fallback the other RowWriter DDL helpers use).
func (w *RowWriter) qualifiedRef(name string) string {
	if w.schema != "" {
		return quoteIdent(w.schema) + "." + quoteIdent(name)
	}
	return quoteIdent(name)
}

// colTypesByName builds the column-type lookup the applier's SET/WHERE
// builders consume, keyed by unqualified column name.
func colTypesByName(cols []*ir.Column) map[string]*ir.Column {
	m := make(map[string]*ir.Column, len(cols))
	for _, c := range cols {
		m[c.Name] = c
	}
	return m
}
