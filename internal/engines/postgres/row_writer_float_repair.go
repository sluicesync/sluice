// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// Compile-time pin: a signature drift on UpdateFloatColumnsByPK would
// otherwise silently drop this *RowWriter out of the ir.FloatRepairWriter
// optional-interface assertion at streamer_coldstart_float_repair.go, taking
// the WARN-skip branch — postgres cold-start FLOAT repair would become a
// no-op with no compile error (ARCH-F1). This turns that into a build break.
var _ ir.FloatRepairWriter = (*RowWriter)(nil)

// floatRepairBatchRows bounds how many per-row UPDATEs a single target
// transaction folds in the FLOAT re-read repair (roadmap open-bug
// 2026-07-09). See the MySQL sibling for the rationale; each UPDATE is
// idempotent, so a re-run of the last uncommitted batch is harmless.
const floatRepairBatchRows = 500

// UpdateFloatColumnsByPK implements [ir.FloatRepairWriter] for a Postgres
// target. After a VStream cold-start COPY (PlanetScale/Vitess source)
// lands single-precision FLOAT columns display-rounded, the pipeline
// re-reads those columns EXACTLY from the source and streams (PK + FLOAT)
// rows here; each drives one `UPDATE <table> SET <float...> WHERE <pk...>`
// (reusing the applier's SET/WHERE builders + value shaping, so a
// source FLOAT mapped to a target real/double/numeric column is handled
// exactly as CDC handles it).
//
// A row absent on the target (deleted between COPY and re-read) is a
// zero-rows-affected no-op — NOT an error; the CDC replay from the copy
// anchor is the authority on such rows. A FLOAT column that is also a PK
// member is left in the WHERE only (the caller excludes it from the
// re-read's FLOAT set); a row with only PK keys is skipped.
func (w *RowWriter) UpdateFloatColumnsByPK(ctx context.Context, table *ir.Table, pkColumns []string, rows <-chan ir.Row) error {
	if table == nil {
		return errors.New("postgres: UpdateFloatColumnsByPK: table is nil")
	}
	if len(pkColumns) == 0 {
		return fmt.Errorf("postgres: UpdateFloatColumnsByPK: table %q has no primary key columns", table.Name)
	}
	colTypes := colTypesByName(table.Columns)
	pkSet := make(map[string]struct{}, len(pkColumns))
	for _, c := range pkColumns {
		pkSet[c] = struct{}{}
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres: UpdateFloatColumnsByPK: begin: %w", err)
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
			continue
		}

		stmt, args, err := buildUpdateSQL(w.schema, table.Name, before, after, colTypes)
		if err != nil {
			return fmt.Errorf("postgres: UpdateFloatColumnsByPK: %s: build update: %w", table.Name, err)
		}
		if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
			return fmt.Errorf("postgres: UpdateFloatColumnsByPK: %s: exec: %w", table.Name, err)
		}

		inBatch++
		if inBatch >= floatRepairBatchRows {
			if err := tx.Commit(); err != nil {
				committed = true
				return fmt.Errorf("postgres: UpdateFloatColumnsByPK: %s: commit: %w", table.Name, err)
			}
			if tx, err = w.db.BeginTx(ctx, nil); err != nil {
				committed = true
				return fmt.Errorf("postgres: UpdateFloatColumnsByPK: %s: begin next batch: %w", table.Name, err)
			}
			inBatch = 0
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("postgres: UpdateFloatColumnsByPK: %s: final commit: %w", table.Name, err)
	}
	committed = true
	return nil
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
