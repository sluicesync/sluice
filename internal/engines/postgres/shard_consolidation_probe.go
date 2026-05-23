// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// ADR-0054 Shape A Phase 2c — ShardConsolidationProber engine impl
// for Postgres. The pipeline's BoundaryRouter dispatches one of these
// methods per recognized shape on takeover; the engine queries
// information_schema / pg_catalog for the observable effect of the
// recorded DDL and classifies into Applied / NotApplied / Inconsistent
// per ADR-0054 §4.
//
// Implemented on *ChangeApplier so the lease-store + prober live on
// the same type — one type-assertion at engagement time confirms both
// surfaces.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// ProbeAddColumn implements [pipeline.ShardConsolidationProber] for
// Postgres. Returns Applied when ALL named columns exist on the
// target with the expected name; NotApplied when NONE exist;
// Inconsistent on partial state or a column-with-wrong-type case.
//
// v1: column existence + name-only check. A future iteration could
// also verify IR Type matches information_schema.data_type — left
// out of v1 because the IR-type-to-PG-type round-trip (e.g.
// Integer{Width:64} ↔ "bigint") would require either reusing the
// schema reader's type-resolution path or adding an inverse-emit
// helper. The Inconsistent-on-partial path catches the most likely
// silent-divergence shape (one shard's lease holder crashed
// mid-ALTER on multi-column ADD).
func (a *ChangeApplier) ProbeAddColumn(ctx context.Context, table *ir.Table, cols []*ir.Column) (ir.ProbeOutcome, error) {
	if len(cols) == 0 {
		return ir.ProbeOutcomeApplied, nil
	}
	schemaName := a.probeSchemaFor(table)
	present, err := a.countColumnsPresent(ctx, schemaName, table.Name, cols)
	if err != nil {
		return ir.ProbeOutcomeInconsistent, err
	}
	return classifyProbeCount(present, len(cols)), nil
}

// ProbeDropColumn returns Applied when NONE of the named columns
// exist; NotApplied when ALL exist; Inconsistent on partial.
func (a *ChangeApplier) ProbeDropColumn(ctx context.Context, table *ir.Table, cols []*ir.Column) (ir.ProbeOutcome, error) {
	if len(cols) == 0 {
		return ir.ProbeOutcomeApplied, nil
	}
	schemaName := a.probeSchemaFor(table)
	present, err := a.countColumnsPresent(ctx, schemaName, table.Name, cols)
	if err != nil {
		return ir.ProbeOutcomeInconsistent, err
	}
	// Invert: Applied means none present, NotApplied means all
	// present, Inconsistent in between.
	switch present {
	case 0:
		return ir.ProbeOutcomeApplied, nil
	case len(cols):
		return ir.ProbeOutcomeNotApplied, nil
	default:
		return ir.ProbeOutcomeInconsistent, nil
	}
}

// ProbeCreateIndex returns Applied when ALL named indexes exist on
// the target; NotApplied when NONE exist; Inconsistent on partial.
// Index names are looked up under pgIndexName's prefix convention
// (matches DropShapeIndex / CreateShapeIndex).
func (a *ChangeApplier) ProbeCreateIndex(ctx context.Context, table *ir.Table, indexes []*ir.Index) (ir.ProbeOutcome, error) {
	if len(indexes) == 0 {
		return ir.ProbeOutcomeApplied, nil
	}
	schemaName := a.probeSchemaFor(table)
	present, err := a.countIndexesPresent(ctx, schemaName, table.Name, indexes)
	if err != nil {
		return ir.ProbeOutcomeInconsistent, err
	}
	return classifyProbeCount(present, len(indexes)), nil
}

// ProbeDropIndex inverts ProbeCreateIndex: Applied when NONE exist,
// NotApplied when ALL exist, Inconsistent partial.
func (a *ChangeApplier) ProbeDropIndex(ctx context.Context, table *ir.Table, indexes []*ir.Index) (ir.ProbeOutcome, error) {
	if len(indexes) == 0 {
		return ir.ProbeOutcomeApplied, nil
	}
	schemaName := a.probeSchemaFor(table)
	present, err := a.countIndexesPresent(ctx, schemaName, table.Name, indexes)
	if err != nil {
		return ir.ProbeOutcomeInconsistent, err
	}
	switch present {
	case 0:
		return ir.ProbeOutcomeApplied, nil
	case len(indexes):
		return ir.ProbeOutcomeNotApplied, nil
	default:
		return ir.ProbeOutcomeInconsistent, nil
	}
}

// ProbeAlterColumnType implements the v0.76.0 task #20 IR-type-matching
// probe (ADR-0054 closure). The v1 implementation was existence-only —
// a column dropped + re-added with a different type silently passed the
// existence check. v2 verifies that the column's catalog-reported IR
// type matches want.Type via the same per-engine type-translation path
// the schema reader uses (translateType), reusing the type-mapping
// logic rather than duplicating it.
//
// Outcomes per ADR-0054 §4:
//
//   - Column ABSENT → Inconsistent (catastrophic — column should always
//     exist on the target after CREATE TABLE).
//   - Column PRESENT and IR-type matches want.Type → Applied.
//   - Column PRESENT but IR-type differs → Inconsistent + error naming
//     expected vs observed type. The mismatch is the v0.76.0
//     silent-divergence shape (the v1 probe couldn't distinguish a
//     legitimate ALTER COLUMN TYPE from a drop+re-add with the wrong
//     type); v2 closes that gap loudly.
//
// Note on PG NUMERIC: PG's information_schema reports
// numeric_precision / numeric_scale as NULL for bare `numeric`
// (unconstrained), distinguishing it from `numeric(p,s)`. The IR's
// Decimal.Unconstrained encodes the same distinction, and
// translateType maps NULL→Unconstrained=true. So a probe of
// `numeric(10,2)` against want.Type=Decimal{Unconstrained:true}
// correctly returns Inconsistent.
func (a *ChangeApplier) ProbeAlterColumnType(ctx context.Context, table *ir.Table, want *ir.Column) (ir.ProbeOutcome, error) {
	if want == nil {
		return ir.ProbeOutcomeInconsistent, errors.New("postgres: probe alter column type: want is nil")
	}
	schemaName := a.probeSchemaFor(table)
	observed, present, err := a.readColumnIRType(ctx, schemaName, table.Name, want.Name)
	if err != nil {
		return ir.ProbeOutcomeInconsistent, err
	}
	if !present {
		return ir.ProbeOutcomeInconsistent, nil
	}
	if reflect.DeepEqual(observed, want.Type) {
		return ir.ProbeOutcomeApplied, nil
	}
	return ir.ProbeOutcomeInconsistent, fmt.Errorf(
		"postgres: probe alter column type %s.%s.%s: observed IR type %v, want %v",
		schemaName, table.Name, want.Name, observed, want.Type,
	)
}

// readColumnIRType introspects the named column on the target via
// information_schema + pg_attribute and reconstructs the IR Type the
// schema reader would build for that column. Returns (irType, true)
// when the column exists, (nil, false) when absent. The query mirrors
// the projection list in populateColumns — same per-column metadata
// columns + the same translateType dispatch — so probe and read see
// the same IR types for the same target state.
//
// Scoped to v0.76.0 task #20: only the subset of fields the v1 probe
// catalog touches (ALTER COLUMN TYPE leaves Nullable / Default / GenExpr
// unchanged; the existing ProbeAlterColumnNullability owns nullability).
// We pass an empty enumValues / geomInfo since the IR-delta classifier
// in v1 refuses on enum / geometry type alters (multi-shape combo).
func (a *ChangeApplier) readColumnIRType(ctx context.Context, schemaName, tableName, colName string) (ir.Type, bool, error) {
	const q = `
		SELECT
			LOWER(c.data_type),
			c.udt_name,
			c.character_maximum_length,
			c.numeric_precision,
			c.numeric_scale,
			c.datetime_precision,
			c.is_identity,
			COALESCE(coll.collname, ''),
			COALESCE(a.atttypmod, -1),
			COALESCE(pg_catalog.format_type(a.atttypid, a.atttypmod), '')
		FROM   information_schema.columns c
		LEFT JOIN pg_class      cl   ON cl.relname    = c.table_name
		                            AND cl.relnamespace = (
		                                  SELECT oid FROM pg_namespace WHERE nspname = c.table_schema)
		LEFT JOIN pg_attribute  a    ON a.attrelid    = cl.oid
		                            AND a.attname     = c.column_name
		                            AND a.attnum      > 0
		                            AND NOT a.attisdropped
		LEFT JOIN pg_collation  coll ON coll.oid       = a.attcollation
		                            AND coll.oid      <> 0
		                            AND coll.collname <> 'default'
		WHERE  c.table_schema = $1 AND c.table_name = $2 AND c.column_name = $3`

	var (
		dataType, udtName   string
		charMaxLen, numPrec sql.NullInt64
		numScale, dtPrec    sql.NullInt64
		isIdentity          string
		collation           string
		attTypmod           int32
		formatType          string
	)
	switch err := a.db.QueryRowContext(ctx, q, schemaName, tableName, colName).Scan(
		&dataType, &udtName,
		&charMaxLen, &numPrec, &numScale, &dtPrec,
		&isIdentity,
		&collation,
		&attTypmod,
		&formatType,
	); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("postgres: probe read column %q: %w", colName, err)
	}

	meta := columnMeta{
		DataType:        dataType,
		UDTName:         udtName,
		CharMaxLen:      nullInt64ToPtr(charMaxLen),
		NumPrec:         nullInt64ToPtr(numPrec),
		NumScale:        nullInt64ToPtr(numScale),
		DTPrec:          nullInt64ToPtr(dtPrec),
		IsAutoIncrement: isAutoIncrement(isIdentity, sql.NullString{}),
		Collation:       collation,
		AttTypmod:       attTypmod,
		FormatType:      formatType,
	}
	t, err := translateType(meta)
	if err != nil {
		return nil, true, fmt.Errorf("postgres: probe translate type for %s.%s.%s: %w", schemaName, tableName, colName, err)
	}
	return t, true, nil
}

// ProbeAlterColumnNullability returns Applied when the column's
// IS_NULLABLE matches want.Nullable, NotApplied when it doesn't, and
// Inconsistent when the column is absent.
func (a *ChangeApplier) ProbeAlterColumnNullability(ctx context.Context, table *ir.Table, want *ir.Column) (ir.ProbeOutcome, error) {
	if want == nil {
		return ir.ProbeOutcomeInconsistent, errors.New("postgres: probe alter column nullability: want is nil")
	}
	schemaName := a.probeSchemaFor(table)
	const q = `SELECT is_nullable FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2 AND column_name = $3`
	var v string
	switch err := a.db.QueryRowContext(ctx, q, schemaName, table.Name, want.Name).Scan(&v); {
	case errors.Is(err, sql.ErrNoRows):
		return ir.ProbeOutcomeInconsistent, nil
	case err != nil:
		return ir.ProbeOutcomeInconsistent, fmt.Errorf("postgres: probe nullability: %w", err)
	}
	currentNullable := strings.EqualFold(v, "YES")
	if currentNullable == want.Nullable {
		return ir.ProbeOutcomeApplied, nil
	}
	return ir.ProbeOutcomeNotApplied, nil
}

// probeSchemaFor resolves which schema's catalog to probe for the
// given IR table. Mirrors the applier's data-write schema-resolution
// (table.Schema > the applier's schema setting). The control-schema
// distinction doesn't apply here — probes inspect USER data tables,
// not control tables.
func (a *ChangeApplier) probeSchemaFor(table *ir.Table) string {
	if table != nil && table.Schema != "" {
		return table.Schema
	}
	return a.schema
}

// countColumnsPresent returns the number of named columns present in
// the target's information_schema.columns.
func (a *ChangeApplier) countColumnsPresent(ctx context.Context, schemaName, tableName string, cols []*ir.Column) (int, error) {
	names := make([]string, 0, len(cols))
	for _, c := range cols {
		if c != nil {
			names = append(names, c.Name)
		}
	}
	if len(names) == 0 {
		return 0, nil
	}
	const q = `SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2 AND column_name = ANY($3)`
	var n int
	if err := a.db.QueryRowContext(ctx, q, schemaName, tableName, pgTextArray(names)).Scan(&n); err != nil {
		return 0, fmt.Errorf("postgres: probe columns: %w", err)
	}
	return n, nil
}

// countIndexesPresent returns the number of named indexes present in
// the target's pg_indexes. Uses the same pgIndexName prefix
// convention CreateShapeIndex / DropShapeIndex / emitCreateIndex use.
func (a *ChangeApplier) countIndexesPresent(ctx context.Context, schemaName, tableName string, indexes []*ir.Index) (int, error) {
	names := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		if idx == nil || strings.EqualFold(idx.Name, "PRIMARY") {
			continue
		}
		names = append(names, pgIndexName(tableName, idx.Name))
	}
	if len(names) == 0 {
		return 0, nil
	}
	const q = `SELECT COUNT(*) FROM pg_indexes
		WHERE schemaname = $1 AND tablename = $2 AND indexname = ANY($3)`
	var n int
	if err := a.db.QueryRowContext(ctx, q, schemaName, tableName, pgTextArray(names)).Scan(&n); err != nil {
		return 0, fmt.Errorf("postgres: probe indexes: %w", err)
	}
	return n, nil
}

// classifyProbeCount maps (present, want) → ProbeOutcome for the
// "ALL present = Applied / NONE present = NotApplied / partial =
// Inconsistent" pattern. Used by ProbeAddColumn + ProbeCreateIndex.
func classifyProbeCount(present, want int) ir.ProbeOutcome {
	switch present {
	case want:
		return ir.ProbeOutcomeApplied
	case 0:
		return ir.ProbeOutcomeNotApplied
	default:
		return ir.ProbeOutcomeInconsistent
	}
}

// pgTextArray wraps a []string for use as a PG text[] bind value with
// the pgx driver via database/sql. pgx accepts []string directly as a
// text[] when passed through QueryRowContext args; this helper makes
// the intent explicit at the call sites.
func pgTextArray(s []string) any { return s }
