// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// ExactRowCount implements [ir.Verifier]. Returns the exact row count
// for the given table via SELECT COUNT(*).
//
// Authoritative count (vs. [SchemaReader.CountRows] / RowReader's
// CountRows which use pg_class.reltuples for ETA hints): `sluice
// verify` needs counts that won't silently disagree with what's
// actually stored, so we pay the full-scan cost.
//
// Schema-qualified by the schema the reader is bound to (default
// `public`). Returns (0, error) on any operational failure; (0, nil)
// is reserved for "table is empty."
func (r *SchemaReader) ExactRowCount(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("postgres: ExactRowCount: table is nil")
	}
	if r.db == nil {
		return 0, errors.New("postgres: ExactRowCount: reader not opened")
	}
	q := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s`,
		quoteIdent(r.schema), quoteIdent(table.Name))
	var count int64
	if err := r.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("postgres: ExactRowCount %s.%s: %w",
			r.schema, table.Name, err)
	}
	return count, nil
}
