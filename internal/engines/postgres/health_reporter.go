// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// SourceCurrentPosition implements [ir.HealthReporter]. Returns the
// source's current WAL position via `pg_current_wal_lsn()`. Used by
// `sluice sync health` to compute lag relative to the target's
// tracked position.
//
// Returns a Position with Engine=postgres, Token=LSN-as-text-string.
// The token is comparable to the StreamStatus.Position.Token values
// surfaced by `ListStreams`.
func (r *SchemaReader) SourceCurrentPosition(ctx context.Context) (ir.Position, error) {
	if r.db == nil {
		return ir.Position{}, errors.New("postgres: SourceCurrentPosition: reader not opened")
	}
	var lsn string
	if err := r.db.QueryRowContext(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&lsn); err != nil {
		return ir.Position{}, fmt.Errorf("postgres: SourceCurrentPosition: %w", err)
	}
	return ir.Position{Engine: dialectName, Token: lsn}, nil
}

// LagBytes implements [ir.BytesLagReporter]. Computes the byte
// distance between two PG LSN positions via `pg_wal_lsn_diff(later,
// earlier)` (PG returns numeric — fits a signed int64 for any LSN
// distance an operator's stream would realistically accumulate).
//
// Returns negative or zero when earlier ≥ later (the target has
// caught up to or passed the source's snapshot position — usually
// transient on quiet streams).
//
// Returns an error when either position's Token is malformed or
// not parseable as a PG LSN.
func (r *SchemaReader) LagBytes(ctx context.Context, earlier, later ir.Position) (int64, error) {
	if r.db == nil {
		return 0, errors.New("postgres: LagBytes: reader not opened")
	}
	if earlier.Token == "" || later.Token == "" {
		return 0, errors.New("postgres: LagBytes: both positions must have non-empty Token")
	}
	var diff int64
	q := `SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::bigint`
	if err := r.db.QueryRowContext(ctx, q, later.Token, earlier.Token).Scan(&diff); err != nil {
		return 0, fmt.Errorf("postgres: LagBytes (later=%q earlier=%q): %w", later.Token, earlier.Token, err)
	}
	return diff, nil
}
