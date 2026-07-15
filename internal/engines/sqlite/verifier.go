// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// ExactRowCount implements [ir.Verifier] for `sluice verify --depth count`:
// an authoritative SELECT COUNT(*) over the file. Added with the flat-file
// staging shim (ADR-0163) so a staged csv/tsv/ndjson source — and, by the
// same type, any sqlite `.db`/dump source or target — supports count-depth
// verification. Sample depth stays unimplemented (no SampleVerifier): the
// documented refusal stands.
func (r *SchemaReader) ExactRowCount(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("sqlite: ExactRowCount: table is nil")
	}
	var n int64
	q := "SELECT COUNT(*) FROM " + quoteIdent(table.Name)
	if err := r.db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0, fmt.Errorf("sqlite: count rows in %q: %w", table.Name, err)
	}
	return n, nil
}
