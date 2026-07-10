// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/engines/internal/floatrepair"
	"sluicesync.dev/sluice/internal/ir"
)

// Compile-time pin: a signature drift on UpdateFloatColumnsByPK would
// otherwise silently drop this *RowWriter out of the ir.FloatRepairWriter
// optional-interface assertion at streamer_coldstart_float_repair.go, taking
// the WARN-skip branch — postgres cold-start FLOAT repair would become a
// no-op with no compile error (ARCH-F1). This turns that into a build break.
var _ ir.FloatRepairWriter = (*RowWriter)(nil)

// floatRepairBatchRows bounds how many rows the FLOAT re-read repair folds
// into ONE batched UPDATE (audit PERF-P1). See the MySQL sibling for the
// rationale; each batched UPDATE is idempotent + autocommit, so a re-run of
// the last batch is harmless. 500 rows × a few (PK+FLOAT) columns stays far
// under PG's 65535 bind-parameter ceiling.
const floatRepairBatchRows = 500

// UpdateFloatColumnsByPK implements [ir.FloatRepairWriter] for a Postgres
// target. After a VStream cold-start COPY (PlanetScale/Vitess source)
// lands single-precision FLOAT columns display-rounded, the pipeline
// re-reads those columns EXACTLY from the source and streams (PK + FLOAT)
// rows here; this batches them into UPDATE-against-a-VALUES-join statements
// (audit PERF-P1) and applies each against the target.
//
// The engine-neutral batching / column-selection / generated-column
// filtering lives in [floatrepair.RepairByPK] (ARCH-F3); this wrapper
// supplies only the Postgres batched-statement builder ([pgFloatBatchExecer]),
// reusing the applier's SET value shaping (so a source FLOAT mapped to a
// target real/double/numeric column is handled exactly as CDC handles it).
// A row absent on the target (deleted between COPY and re-read) is a clean
// VALUES-join miss — NOT an error. A FLOAT column that is also a PK member
// is left in the join only; the caller excludes it from the re-read's FLOAT
// set.
func (w *RowWriter) UpdateFloatColumnsByPK(ctx context.Context, table *ir.Table, pkColumns []string, rows <-chan ir.Row) error {
	return floatrepair.RepairByPK(ctx, table, pkColumns, rows, floatRepairBatchRows, &pgFloatBatchExecer{w: w})
}

// pgFloatBatchExecer is the Postgres [floatrepair.BatchExecer]: it renders
// and executes one batched UPDATE for a batch of repair rows.
type pgFloatBatchExecer struct{ w *RowWriter }

// ExecBatch builds and runs one `UPDATE <t> AS tgt SET c = v.c FROM (VALUES
// …) AS v(pk, c) WHERE tgt.pk = v.pk` statement in autocommit. The batch is
// a single atomic multi-row UPDATE; the repair is idempotent and runs before
// the CDC anchor persists, so no explicit transaction is needed.
func (e *pgFloatBatchExecer) ExecBatch(ctx context.Context, table *ir.Table, pkColumns, setColumns []string, batch []ir.Row) error {
	colTypes := colTypesByName(table.Columns)
	opts := emitOpts{HasPostGIS: e.w.hasPostGIS, TargetSchema: e.w.schema}
	stmt, args, err := buildFloatRepairBatchSQL(e.w.schema, table.Name, pkColumns, setColumns, batch, colTypes, opts)
	if err != nil {
		return fmt.Errorf("postgres: UpdateFloatColumnsByPK: %s: build batch: %w", table.Name, err)
	}
	if _, err := e.w.db.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("postgres: UpdateFloatColumnsByPK: %s: exec batch: %w", table.Name, err)
	}
	return nil
}

// buildFloatRepairBatchSQL renders the batched UPDATE-against-a-VALUES-join
// and its row-major arg list ($1.. numbered row by row). Column order is PK
// columns then SET columns.
//
// # The first-row cast (the load-bearing correctness detail)
//
// Postgres infers each VALUES column's type from the FIRST row's expression,
// and an untyped `$N` parameter in a VALUES defaults to text — which would
// break the `tgt.pk = v.pk` join for a non-text PK and, worse, round-trip a
// numeric/real SET value through text on the way in. So the FIRST row's
// placeholders are cast to each column's target storage type
// (`$N::real`, `$N::bigint`, `$N::numeric(p,s)`, …) via [floatRepairCastType];
// subsequent rows are bare `$N` (they inherit the column type). The cast
// target is the column's STORAGE type, so the value coerces exactly as the
// per-row `SET c = $1` / `WHERE pk = $2` assignment did (a `numeric(p,s)`
// column still rounds to its scale on assignment — identical stored value).
//
// Each value is shaped by the SAME [prepareApplierValue] the CDC applier
// uses. PK columns are non-null (the join key); a NULL FLOAT SET value binds
// through normally (`SET c = v.c` with a NULL v.c sets NULL).
func buildFloatRepairBatchSQL(schema, tableName string, pkColumns, setColumns []string, batch []ir.Row, colTypes map[string]*ir.Column, opts emitOpts) (sqlStmt string, args []any, err error) {
	allCols := make([]string, 0, len(pkColumns)+len(setColumns))
	allCols = append(allCols, pkColumns...)
	allCols = append(allCols, setColumns...)

	// Cast type for each column — applied only to the first row's placeholders.
	casts := make([]string, len(allCols))
	for i, c := range allCols {
		ct, err := floatRepairCastType(colTypes[c], opts)
		if err != nil {
			return "", nil, fmt.Errorf("column %q: %w", c, err)
		}
		casts[i] = ct
	}

	tableRef := quoteIdent(schema) + "." + quoteIdent(tableName)
	var b strings.Builder
	args = make([]any, 0, len(batch)*len(allCols))

	b.WriteString("UPDATE ")
	b.WriteString(tableRef)
	b.WriteString(" AS tgt SET ")
	for i, c := range setColumns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteIdent(c))
		b.WriteString(" = v.")
		b.WriteString(quoteIdent(c))
	}

	b.WriteString(" FROM (VALUES ")
	paramIdx := 1
	for i, row := range batch {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		for j, c := range allCols {
			if j > 0 {
				b.WriteString(", ")
			}
			b.WriteString("$")
			b.WriteString(strconv.Itoa(paramIdx))
			if i == 0 {
				b.WriteString("::")
				b.WriteString(casts[j])
			}
			v, err := prepareApplierValue(row[c], colTypes, c)
			if err != nil {
				return "", nil, fmt.Errorf("column %q: %w", c, err)
			}
			args = append(args, v)
			paramIdx++
		}
		b.WriteString(")")
	}
	b.WriteString(") AS v(")
	for i, c := range allCols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteIdent(c))
	}
	b.WriteString(") WHERE ")
	for i, c := range pkColumns {
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString("tgt.")
		b.WriteString(quoteIdent(c))
		b.WriteString(" = v.")
		b.WriteString(quoteIdent(c))
	}
	return b.String(), args, nil
}

// floatRepairCastType returns the Postgres storage type to cast a VALUES
// column's first-row placeholder to. It reuses the DDL type emitter for
// every family EXCEPT integers: [emitColumnType] appends
// ` GENERATED … AS IDENTITY` for an AUTO_INCREMENT column, which is not a
// valid CAST target, so integers render via the storage-type helpers
// directly. A column whose type has no valid cast spelling (enum /
// extension / geometry as a repair-table PK — practically impossible, since
// the caller trims to PK + single-precision FLOAT) surfaces the emitter's
// loud refusal rather than emitting a silently-wrong cast.
func floatRepairCastType(col *ir.Column, opts emitOpts) (string, error) {
	if col == nil {
		return "", errors.New("floatrepair cast: column type is unknown (not in the target table)")
	}
	if i, ok := col.Type.(ir.Integer); ok {
		return postgresIntName(effectiveWidth(i)), nil
	}
	return emitColumnType(col.Type, opts)
}

// colTypesByName builds the column-type lookup the applier's value shaping
// consumes, keyed by unqualified column name.
func colTypesByName(cols []*ir.Column) map[string]*ir.Column {
	m := make(map[string]*ir.Column, len(cols))
	for _, c := range cols {
		m[c.Name] = c
	}
	return m
}
