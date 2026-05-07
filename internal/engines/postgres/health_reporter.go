// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
//
// **Bug 32 fix (v0.15.1):** earlier versions passed `Position.Token`
// verbatim into `pg_wal_lsn_diff`. That works when the Token is a
// bare LSN string (the shape returned by `SourceCurrentPosition`),
// but the persisted-state Token from `sluice_cdc_state` is a JSON
// envelope `{"slot":"...","lsn":"X/Y"}` — passing the JSON verbatim
// to `pg_wal_lsn_diff` errors with SQLSTATE 22P02. extractPGLSN now
// transparently handles both shapes; the engine owns its position
// format, the consumer doesn't have to.
func (r *SchemaReader) LagBytes(ctx context.Context, earlier, later ir.Position) (int64, error) {
	if r.db == nil {
		return 0, errors.New("postgres: LagBytes: reader not opened")
	}
	earlierLSN, err := extractPGLSN(earlier)
	if err != nil {
		return 0, fmt.Errorf("postgres: LagBytes earlier-position: %w", err)
	}
	laterLSN, err := extractPGLSN(later)
	if err != nil {
		return 0, fmt.Errorf("postgres: LagBytes later-position: %w", err)
	}
	var diff int64
	q := `SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::bigint`
	if err := r.db.QueryRowContext(ctx, q, laterLSN, earlierLSN).Scan(&diff); err != nil {
		return 0, fmt.Errorf("postgres: LagBytes (later=%q earlier=%q): %w", laterLSN, earlierLSN, err)
	}
	return diff, nil
}

// extractPGLSN normalises an [ir.Position] into a bare LSN string
// suitable for passing to `pg_wal_lsn_diff($1::pg_lsn, ...)`. Two
// position-token shapes flow into here:
//
//  1. **Bare LSN** — what `SourceCurrentPosition` emits via
//     `pg_current_wal_lsn()::text`. Pass through unchanged.
//  2. **JSON envelope** — what the CDC reader emits via [encodePGPos]:
//     `{"slot":"...","lsn":"X/Y"}`. The orchestrator's
//     `sluice_cdc_state` row carries this shape. Parse the JSON and
//     extract the `lsn` field.
//
// Either shape is a valid PG position; the caller doesn't have to
// know which.
func extractPGLSN(p ir.Position) (string, error) {
	if p.Token == "" {
		return "", errors.New("position has empty Token")
	}
	if strings.HasPrefix(strings.TrimSpace(p.Token), "{") {
		decoded, _, err := decodePGPos(p)
		if err != nil {
			return "", fmt.Errorf("decode JSON-envelope position: %w", err)
		}
		return decoded.LSN, nil
	}
	return p.Token, nil
}
