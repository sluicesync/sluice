// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/engines/internal/floatrepair"
	"sluicesync.dev/sluice/internal/ir"
)

// Compile-time pin: keep this *RowWriter satisfying ir.FloatRepairWriter so a
// signature drift becomes a build break, not a silently-skipped repair (see
// the postgres sibling; ARCH-F1).
var _ ir.FloatRepairWriter = (*RowWriter)(nil)

// floatRepairBatchRows bounds how many rows the FLOAT re-read repair folds
// into ONE batched UPDATE (audit PERF-P1). The pre-batch path issued one
// `UPDATE … SET float WHERE pk` per row — one round-trip per row, hours-to-
// days on a cross-region PlanetScale target for a large table. Batching
// cuts that to ceil(rows/500) statements. 500 rows × a few (PK+FLOAT)
// columns stays well under the max_allowed_packet / statement-size ceiling,
// and each batched UPDATE is idempotent + autocommit (a single multi-row
// UPDATE is atomic, and the repair re-runs cleanly on a re-cold-start), so
// no per-batch transaction round-trips are needed.
const floatRepairBatchRows = 500

// UpdateFloatColumnsByPK implements [ir.FloatRepairWriter]. It corrects
// the single-precision FLOAT columns a VStream cold-start COPY landed
// display-rounded (roadmap open-bug 2026-07-09): the pipeline re-reads
// those columns EXACTLY from the source over SQL and streams (PK + FLOAT)
// rows here; this batches them into UPDATE-against-a-UNION-join statements
// (audit PERF-P1) and applies each against the target.
//
// The engine-neutral batching / column-selection / generated-column
// filtering lives in [floatrepair.RepairByPK] (ARCH-F3); this wrapper
// supplies only the MySQL batched-statement builder ([mysqlFloatBatchExecer]).
// A row's PK values drive the JOIN; the remaining (FLOAT) keys are SET on the
// matched row, reusing the applier's value shaping so a FLOAT-mapped-to-DOUBLE
// target column is handled exactly as CDC handles it. A row absent on the
// target (deleted between COPY and re-read) is a clean join miss — NOT an
// error. A FLOAT column that is also a PK member is left in the JOIN only
// (never SET); the caller already excludes it from the re-read's FLOAT set.
func (w *RowWriter) UpdateFloatColumnsByPK(ctx context.Context, table *ir.Table, pkColumns []string, rows <-chan ir.Row) error {
	return floatrepair.RepairByPK(ctx, table, pkColumns, rows, floatRepairBatchRows, &mysqlFloatBatchExecer{w: w})
}

// mysqlFloatBatchExecer is the MySQL [floatrepair.BatchExecer]: it renders
// and executes one batched UPDATE for a batch of repair rows.
type mysqlFloatBatchExecer struct{ w *RowWriter }

// ExecBatch builds and runs one `UPDATE <t> AS tgt JOIN (SELECT … UNION ALL
// …) AS v ON tgt.pk = v.pk SET tgt.c = v.c` statement in autocommit. The
// batch is a single atomic multi-row UPDATE; the repair is idempotent and
// runs before the CDC anchor persists, so no explicit transaction is needed.
func (e *mysqlFloatBatchExecer) ExecBatch(ctx context.Context, table *ir.Table, pkColumns, setColumns []string, batch []ir.Row) error {
	colTypes := colTypesByName(table.Columns)
	stmt, args, err := buildFloatRepairBatchSQL(e.w.qualifiedRef(table.Name), pkColumns, setColumns, batch, colTypes)
	if err != nil {
		return fmt.Errorf("mysql: UpdateFloatColumnsByPK: %s: build batch: %w", table.Name, err)
	}
	if _, err := e.w.db.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("mysql: UpdateFloatColumnsByPK: %s: exec batch: %w", table.Name, err)
	}
	return nil
}

// buildFloatRepairBatchSQL renders the batched UPDATE-against-a-UNION-join
// and its row-major arg list. The derived table `v` supplies one row per
// repair row via `SELECT ? [, ?…] UNION ALL SELECT ? [, ?…]` (5.7-compatible;
// NOT the 8.0.19 VALUES ROW() constructor). The FIRST SELECT names the
// columns (`? AS pk`, `? AS c`); subsequent SELECTs are positional. Column
// order is PK columns then SET columns; the args follow the same order,
// row by row.
//
// Each value is shaped by the SAME [prepareApplierValue] the CDC applier
// uses (so the interpolated/bound literal is byte-identical to what a
// per-row UPDATE would have produced). PK columns are non-null (the join
// key), so no `IS NULL` handling is needed; a NULL FLOAT SET value binds
// through normally and sets NULL on the matched row.
func buildFloatRepairBatchSQL(tableRef string, pkColumns, setColumns []string, batch []ir.Row, colTypes map[string]*ir.Column) (sqlStmt string, args []any, err error) {
	allCols := make([]string, 0, len(pkColumns)+len(setColumns))
	allCols = append(allCols, pkColumns...)
	allCols = append(allCols, setColumns...)

	var b strings.Builder
	args = make([]any, 0, len(batch)*len(allCols))

	b.WriteString("UPDATE ")
	b.WriteString(tableRef)
	b.WriteString(" AS tgt JOIN (")
	for i, row := range batch {
		if i == 0 {
			b.WriteString("SELECT ")
		} else {
			b.WriteString(" UNION ALL SELECT ")
		}
		for j, c := range allCols {
			if j > 0 {
				b.WriteString(", ")
			}
			b.WriteString("?")
			if i == 0 {
				// First SELECT names the derived columns for the ON/SET refs.
				b.WriteString(" AS ")
				b.WriteString(quoteIdent(c))
			}
			v, err := prepareApplierValue(row[c], colTypes, c)
			if err != nil {
				return "", nil, fmt.Errorf("column %q: %w", c, err)
			}
			args = append(args, v)
		}
	}
	b.WriteString(") AS v ON ")
	for i, c := range pkColumns {
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString("tgt.")
		b.WriteString(quoteIdent(c))
		b.WriteString(" = v.")
		b.WriteString(quoteIdent(c))
	}
	b.WriteString(" SET ")
	for i, c := range setColumns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("tgt.")
		b.WriteString(quoteIdent(c))
		b.WriteString(" = v.")
		b.WriteString(quoteIdent(c))
	}
	return b.String(), args, nil
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

// colTypesByName builds the column-type lookup the applier's value shaping
// consumes, keyed by unqualified column name.
func colTypesByName(cols []*ir.Column) map[string]*ir.Column {
	m := make(map[string]*ir.Column, len(cols))
	for _, c := range cols {
		m[c.Name] = c
	}
	return m
}
