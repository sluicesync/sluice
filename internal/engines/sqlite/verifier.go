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

// ExactRowCount implements [ir.Verifier] for a live `d1` source: the same
// authoritative COUNT(*) over the HTTP query API, projected as TEXT and parsed
// exactly like every other integer this transport reads. Added with audit
// 2026-09-01 LA-3 — until then `sluice verify --depth count` refused every
// `d1` source, so the lane had no independent row count at all. It is the same
// query the D1 row reader brackets each table read with; here it is the
// operator-invoked form. Sample depth stays unimplemented, as for the file
// engine.
func (r *D1SchemaReader) ExactRowCount(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil {
		return 0, errors.New("d1: ExactRowCount: table is nil")
	}
	n, err := (&D1RowReader{client: r.client}).countRows(ctx, table.Name)
	if err != nil {
		return 0, fmt.Errorf("d1: count rows in %q: %w", table.Name, err)
	}
	return n, nil
}
