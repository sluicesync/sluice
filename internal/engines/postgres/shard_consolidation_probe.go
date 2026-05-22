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

// ProbeAlterColumnType is a minimal v1 implementation: it verifies
// the column EXISTS. Type-matching (Applied vs NotApplied based on
// catalog's data_type vs want.Type) is deferred — the IR-type-to-PG-
// type inverse mapping isn't trivial to factor without coupling the
// applier to the schema reader. v1 surfaces "column missing" as
// Inconsistent (the column should always exist on the target after
// CREATE TABLE / cold-start), and treats "column exists" as Applied
// so the takeover record-only path lands. A column-type mismatch
// (the silent-divergence hazard) is left to surface via Phase 2e
// integration tests + a future Phase 3 type-matching pass.
func (a *ChangeApplier) ProbeAlterColumnType(ctx context.Context, table *ir.Table, want *ir.Column) (ir.ProbeOutcome, error) {
	if want == nil {
		return ir.ProbeOutcomeInconsistent, errors.New("postgres: probe alter column type: want is nil")
	}
	schemaName := a.probeSchemaFor(table)
	exists, err := a.columnPresent(ctx, schemaName, table.Name, want.Name)
	if err != nil {
		return ir.ProbeOutcomeInconsistent, err
	}
	if !exists {
		return ir.ProbeOutcomeInconsistent, nil
	}
	return ir.ProbeOutcomeApplied, nil
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

// columnPresent returns whether the named column exists on the target.
func (a *ChangeApplier) columnPresent(ctx context.Context, schemaName, tableName, colName string) (bool, error) {
	const q = `SELECT EXISTS(SELECT 1 FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2 AND column_name = $3)`
	var v bool
	if err := a.db.QueryRowContext(ctx, q, schemaName, tableName, colName).Scan(&v); err != nil {
		return false, fmt.Errorf("postgres: probe column %q: %w", colName, err)
	}
	return v, nil
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
