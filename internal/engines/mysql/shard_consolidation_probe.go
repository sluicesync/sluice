// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// ADR-0054 Shape A Phase 2c — ShardConsolidationProber engine impl
// for MySQL. The pipeline's BoundaryRouter dispatches one of these
// methods per recognized shape on takeover; the engine queries
// information_schema for the observable effect of the recorded DDL
// and classifies into Applied / NotApplied / Inconsistent per
// ADR-0054 §4.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// ProbeAddColumn implements [pipeline.ShardConsolidationProber] for
// MySQL. ALL present → Applied; NONE present → NotApplied; partial →
// Inconsistent.
func (a *ChangeApplier) ProbeAddColumn(ctx context.Context, table *ir.Table, cols []*ir.Column) (ir.ProbeOutcome, error) {
	if len(cols) == 0 {
		return ir.ProbeOutcomeApplied, nil
	}
	present, err := a.countColumnsPresent(ctx, table.Name, cols)
	if err != nil {
		return ir.ProbeOutcomeInconsistent, err
	}
	return classifyProbeCount(present, len(cols)), nil
}

// ProbeDropColumn inverts ProbeAddColumn.
func (a *ChangeApplier) ProbeDropColumn(ctx context.Context, table *ir.Table, cols []*ir.Column) (ir.ProbeOutcome, error) {
	if len(cols) == 0 {
		return ir.ProbeOutcomeApplied, nil
	}
	present, err := a.countColumnsPresent(ctx, table.Name, cols)
	if err != nil {
		return ir.ProbeOutcomeInconsistent, err
	}
	switch present {
	case 0:
		return ir.ProbeOutcomeApplied, nil
	case len(cols):
		return ir.ProbeOutcomeNotApplied, nil
	default:
		return ir.ProbeOutcomeInconsistent, nil
	}
}

// ProbeCreateIndex returns Applied when ALL named indexes exist,
// NotApplied when NONE, Inconsistent on partial.
func (a *ChangeApplier) ProbeCreateIndex(ctx context.Context, table *ir.Table, indexes []*ir.Index) (ir.ProbeOutcome, error) {
	if len(indexes) == 0 {
		return ir.ProbeOutcomeApplied, nil
	}
	present, err := a.countIndexesPresent(ctx, table.Name, indexes)
	if err != nil {
		return ir.ProbeOutcomeInconsistent, err
	}
	return classifyProbeCount(present, len(indexes)), nil
}

// ProbeDropIndex inverts ProbeCreateIndex.
func (a *ChangeApplier) ProbeDropIndex(ctx context.Context, table *ir.Table, indexes []*ir.Index) (ir.ProbeOutcome, error) {
	if len(indexes) == 0 {
		return ir.ProbeOutcomeApplied, nil
	}
	present, err := a.countIndexesPresent(ctx, table.Name, indexes)
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

// ProbeAlterColumnType is the v1 minimal: column existence check.
// Type-matching deferred to a future iteration (see PG counterpart
// for the rationale + future direction).
func (a *ChangeApplier) ProbeAlterColumnType(ctx context.Context, table *ir.Table, want *ir.Column) (ir.ProbeOutcome, error) {
	if want == nil {
		return ir.ProbeOutcomeInconsistent, errors.New("mysql: probe alter column type: want is nil")
	}
	exists, err := a.columnPresent(ctx, table.Name, want.Name)
	if err != nil {
		return ir.ProbeOutcomeInconsistent, err
	}
	if !exists {
		return ir.ProbeOutcomeInconsistent, nil
	}
	return ir.ProbeOutcomeApplied, nil
}

// ProbeAlterColumnNullability returns Applied when the column's
// IS_NULLABLE matches want.Nullable, NotApplied when it doesn't,
// Inconsistent on absent column.
func (a *ChangeApplier) ProbeAlterColumnNullability(ctx context.Context, table *ir.Table, want *ir.Column) (ir.ProbeOutcome, error) {
	if want == nil {
		return ir.ProbeOutcomeInconsistent, errors.New("mysql: probe alter column nullability: want is nil")
	}
	const q = `SELECT is_nullable FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`
	var v string
	switch err := a.db.QueryRowContext(ctx, q, table.Name, want.Name).Scan(&v); {
	case errors.Is(err, sql.ErrNoRows):
		return ir.ProbeOutcomeInconsistent, nil
	case err != nil:
		return ir.ProbeOutcomeInconsistent, fmt.Errorf("mysql: probe nullability: %w", err)
	}
	currentNullable := strings.EqualFold(v, "YES")
	if currentNullable == want.Nullable {
		return ir.ProbeOutcomeApplied, nil
	}
	return ir.ProbeOutcomeNotApplied, nil
}

// countColumnsPresent returns the number of named columns present in
// the target's information_schema.columns. MySQL's
// information_schema queries use DATABASE() to scope to the
// connection's current database.
func (a *ChangeApplier) countColumnsPresent(ctx context.Context, tableName string, cols []*ir.Column) (int, error) {
	if len(cols) == 0 {
		return 0, nil
	}
	// MySQL doesn't support `column_name = ANY(?)` with a slice the
	// way PG does; build a parameterised IN list.
	placeholders := make([]string, 0, len(cols))
	args := []any{tableName}
	for _, c := range cols {
		if c == nil {
			continue
		}
		placeholders = append(placeholders, "?")
		args = append(args, c.Name)
	}
	if len(placeholders) == 0 {
		return 0, nil
	}
	q := fmt.Sprintf(`SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ? AND column_name IN (%s)`,
		strings.Join(placeholders, ","))
	var n int
	if err := a.db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("mysql: probe columns: %w", err)
	}
	return n, nil
}

// columnPresent returns whether the named column exists on the target.
func (a *ChangeApplier) columnPresent(ctx context.Context, tableName, colName string) (bool, error) {
	const q = `SELECT EXISTS(SELECT 1 FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?)`
	var v bool
	if err := a.db.QueryRowContext(ctx, q, tableName, colName).Scan(&v); err != nil {
		return false, fmt.Errorf("mysql: probe column %q: %w", colName, err)
	}
	return v, nil
}

// countIndexesPresent returns the number of named indexes present in
// the target's information_schema.statistics. MySQL index names are
// table-scoped (no prefix-with-table convention).
func (a *ChangeApplier) countIndexesPresent(ctx context.Context, tableName string, indexes []*ir.Index) (int, error) {
	placeholders := make([]string, 0, len(indexes))
	args := []any{tableName}
	for _, idx := range indexes {
		if idx == nil || strings.EqualFold(idx.Name, "PRIMARY") {
			continue
		}
		placeholders = append(placeholders, "?")
		args = append(args, idx.Name)
	}
	if len(placeholders) == 0 {
		return 0, nil
	}
	q := fmt.Sprintf(`SELECT COUNT(DISTINCT index_name) FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = ? AND index_name IN (%s)`,
		strings.Join(placeholders, ","))
	var n int
	if err := a.db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("mysql: probe indexes: %w", err)
	}
	return n, nil
}

// classifyProbeCount maps (present, want) → ProbeOutcome.
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
