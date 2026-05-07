// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// ExactRowCount implements [ir.Verifier]. Returns the exact row count
// for the given table via SELECT COUNT(*).
//
// Authoritative count (vs. RowReader's CountRows which uses
// information_schema.tables.table_rows for ETA hints — that's
// approximate and lags actual cardinality on InnoDB tables that
// haven't been ANALYZE-d recently): `sluice verify` needs counts
// that won't silently disagree with what's stored, so we pay the
// full-scan cost.
//
// Returns (0, error) on any operational failure; (0, nil) is reserved
// for "table is empty."
func (r *SchemaReader) ExactRowCount(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("mysql: ExactRowCount: table is nil")
	}
	if r.db == nil {
		return 0, errors.New("mysql: ExactRowCount: reader not opened")
	}
	q := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quoteIdent(table.Name))
	var count int64
	if err := r.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("mysql: ExactRowCount %s: %w", table.Name, err)
	}
	return count, nil
}
