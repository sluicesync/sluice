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
	"reflect"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
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

// ProbeAlterColumnType implements the v0.76.0 task #20 IR-type-matching
// probe (ADR-0054 closure). v1 was existence-only — a column dropped +
// re-added with a different type silently passed. v2 verifies the
// column's catalog-reported IR type matches want.Type via the same
// translateType helper the schema reader uses.
//
// Outcomes per ADR-0054 §4:
//
//   - Column ABSENT → Inconsistent (catastrophic).
//   - Column PRESENT and IR-type matches want.Type → Applied.
//   - Column PRESENT but IR-type differs → Inconsistent + error naming
//     expected vs observed type.
//
// MySQL note: per the ADR-0054 v0.73.2 charset-normalizer amendment,
// the post-DDL IR doesn't carry per-column charset; v2 builds a meta
// with empty Charset so the IR comparison is type+length only. A
// future iteration could pin charset once the normalizer carries it.
func (a *ChangeApplier) ProbeAlterColumnType(ctx context.Context, table *ir.Table, want *ir.Column) (ir.ProbeOutcome, error) {
	if want == nil {
		return ir.ProbeOutcomeInconsistent, errors.New("mysql: probe alter column type: want is nil")
	}
	observed, present, err := a.readColumnIRType(ctx, table.Name, want.Name)
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
		"mysql: probe alter column type %s.%s: observed IR type %v, want %v",
		table.Name, want.Name, observed, want.Type,
	)
}

// readColumnIRType introspects the named column on the target via
// information_schema.columns and reconstructs the IR Type via the same
// translateType helper the schema reader uses. Returns (irType, true)
// when the column exists, (nil, false) when absent. Scoped to the v0.76.0
// task #20 probe path — only the subset of metadata translateType
// consumes from information_schema.
//
// Charset is intentionally NOT carried (see ProbeAlterColumnType comment
// for the ADR-0054 amendment context).
func (a *ChangeApplier) readColumnIRType(ctx context.Context, tableName, colName string) (ir.Type, bool, error) {
	const q = `
		SELECT
			LOWER(data_type),
			column_type,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			datetime_precision,
			COALESCE(collation_name, ''),
			COALESCE(srs_id, 0),
			COALESCE(extra, '')
		FROM   information_schema.columns
		WHERE  table_schema = DATABASE() AND table_name = ? AND column_name = ?`

	var (
		dataType, columnType string
		charMaxLen, numPrec  sql.NullInt64
		numScale, dtPrec     sql.NullInt64
		collation            string
		srsID                int
		extra                string
	)
	switch err := a.db.QueryRowContext(ctx, q, tableName, colName).Scan(
		&dataType, &columnType,
		&charMaxLen, &numPrec, &numScale, &dtPrec,
		&collation,
		&srsID,
		&extra,
	); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("mysql: probe read column %q: %w", colName, err)
	}

	meta := columnMeta{
		DataType:   dataType,
		ColumnType: strings.ToLower(columnType),
		CharMaxLen: nullInt64ToPtrMySQL(charMaxLen),
		NumPrec:    nullInt64ToPtrMySQL(numPrec),
		NumScale:   nullInt64ToPtrMySQL(numScale),
		DTPrec:     nullInt64ToPtrMySQL(dtPrec),
		// Charset deliberately empty — see ProbeAlterColumnType comment.
		Collation: collation,
		SrsID:     srsID,
		Extra:     strings.ToLower(extra),
	}
	t, err := translateType(meta)
	if err != nil {
		return nil, true, fmt.Errorf("mysql: probe translate type for %s.%s: %w", tableName, colName, err)
	}
	return t, true, nil
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

// ProbeRenameColumn implements [ir.ShardConsolidationProber] for
// MySQL (ADR-0054 v0.78.0 — task #22 RENAME COLUMN sub-task).
// Outcomes mirror the PG implementation: newName present + oldName
// absent + IR type matches → Applied; oldName present + newName
// absent → NotApplied; everything else → Inconsistent. The IR-type
// match closes the drop+re-add-with-wrong-type silent divergence
// the v0.76.0 ProbeAlterColumnType v2 chases on the type-alter
// shape.
func (a *ChangeApplier) ProbeRenameColumn(ctx context.Context, table *ir.Table, oldName, newName string, want *ir.Column) (ir.ProbeOutcome, error) {
	if oldName == "" || newName == "" {
		return ir.ProbeOutcomeInconsistent, errors.New("mysql: probe rename column: oldName and newName must be non-empty")
	}
	if oldName == newName {
		return ir.ProbeOutcomeInconsistent, errors.New("mysql: probe rename column: oldName and newName are identical")
	}
	oldPresent, err := a.singleColumnPresent(ctx, table.Name, oldName)
	if err != nil {
		return ir.ProbeOutcomeInconsistent, err
	}
	newPresent, err := a.singleColumnPresent(ctx, table.Name, newName)
	if err != nil {
		return ir.ProbeOutcomeInconsistent, err
	}
	switch {
	case oldPresent && !newPresent:
		return ir.ProbeOutcomeNotApplied, nil
	case !oldPresent && newPresent:
		if want == nil {
			return ir.ProbeOutcomeApplied, nil
		}
		observed, present, readErr := a.readColumnIRType(ctx, table.Name, newName)
		if readErr != nil {
			return ir.ProbeOutcomeInconsistent, readErr
		}
		if !present {
			return ir.ProbeOutcomeInconsistent, fmt.Errorf(
				"mysql: probe rename column %s.%s: column vanished between presence and type checks",
				table.Name, newName,
			)
		}
		if reflect.DeepEqual(observed, want.Type) {
			return ir.ProbeOutcomeApplied, nil
		}
		return ir.ProbeOutcomeInconsistent, fmt.Errorf(
			"mysql: probe rename column %s.%s: observed IR type %v, want %v",
			table.Name, newName, observed, want.Type,
		)
	case oldPresent && newPresent:
		return ir.ProbeOutcomeInconsistent, fmt.Errorf(
			"mysql: probe rename column %s: both %q and %q exist",
			table.Name, oldName, newName,
		)
	default: // both absent
		return ir.ProbeOutcomeInconsistent, fmt.Errorf(
			"mysql: probe rename column %s: neither %q nor %q exists",
			table.Name, oldName, newName,
		)
	}
}

// singleColumnPresent reports whether the named column exists on
// table in the current database. Thin wrapper over the existing
// information_schema.columns query.
func (a *ChangeApplier) singleColumnPresent(ctx context.Context, tableName, colName string) (bool, error) {
	const q = `SELECT EXISTS(SELECT 1 FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?)`
	var exists bool
	if err := a.db.QueryRowContext(ctx, q, tableName, colName).Scan(&exists); err != nil {
		return false, fmt.Errorf("mysql: probe single column %q on %q: %w", colName, tableName, err)
	}
	return exists, nil
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

// nullInt64ToPtrMySQL converts an sql.NullInt64 (returned by Scan) to
// the *int64 shape columnMeta expects (nil for NULL, &v otherwise). The
// schema reader uses a custom Scanner type (nullableInt64) wired into
// QueryContext; the probe scans directly via sql.NullInt64 + this
// helper so a single column lookup doesn't require the custom scanner
// machinery.
func nullInt64ToPtrMySQL(n sql.NullInt64) *int64 {
	if !n.Valid {
		return nil
	}
	v := n.Int64
	return &v
}
