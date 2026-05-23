// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

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

// SlotSpillStats implements [ir.SlotSpillReporter]. Reads the per-slot
// CDC-decode spill counters from PG 14+'s `pg_stat_replication_slots`
// view (severity-B finding F2 of the 2026-05-22 PG-internals research
// run).
//
// Returns ok=false (not an error) in two "no signal" cases the consumer
// distinguishes from "definitely zero":
//
//  1. The view doesn't exist (`SQLSTATE 42P01 undefined_table`) — PG <
//  14. sluice's declared baseline is PG 14+; this is defensive for
//     operators pointing sluice at an older server during evaluation.
//  2. No row exists for the slot in the view yet — the slot has never
//     been used for decoding (a freshly-created slot before any
//     `START_REPLICATION` has emitted changes through it).
//
// Either case surfaces as "spill stats unavailable" in the health
// surface rather than a misleading 0.
//
// Empty slotName returns an error — the caller didn't supply enough
// info to scope the query, and returning ok=false would mask a real bug
// in the wiring layer.
func (r *SchemaReader) SlotSpillStats(ctx context.Context, slotName string) (ir.SpillStats, bool, error) {
	if r.db == nil {
		return ir.SpillStats{}, false, errors.New("postgres: SlotSpillStats: reader not opened")
	}
	if slotName == "" {
		return ir.SpillStats{}, false, errors.New("postgres: SlotSpillStats: slotName is empty")
	}
	var stats ir.SpillStats
	const q = `SELECT spill_txns, spill_bytes FROM pg_stat_replication_slots WHERE slot_name = $1`
	err := r.db.QueryRowContext(ctx, q, slotName).Scan(&stats.SpillTxns, &stats.SpillBytes)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// No row for this slot yet — decode hasn't happened. Surface as
		// "unavailable" rather than 0; a careless reader would mistake
		// 0 for "definitely no spill" when the real signal is "we can't
		// tell yet."
		return ir.SpillStats{}, false, nil
	case isUndefinedTableError(err):
		// PG < 14 — the view doesn't exist. Same "unavailable" surface.
		return ir.SpillStats{}, false, nil
	case err != nil:
		return ir.SpillStats{}, false, fmt.Errorf("postgres: SlotSpillStats: %w", err)
	}
	return stats, true, nil
}

// isUndefinedTableError reports whether err wraps a PG `undefined_table`
// SQLSTATE (42P01). Used to detect the "view doesn't exist on PG < 14"
// case for `pg_stat_replication_slots` without hard-coding a PG version
// check.
func isUndefinedTableError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "42P01"
	}
	return false
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
