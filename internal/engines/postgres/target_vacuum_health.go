// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// Target-side autovacuum / dead-tuple / wraparound probe (the ADR-0107
// item-36 vacuum rule family, roadmap 2026-07-22). The applier satisfies
// [ir.TargetVacuumHealthReporter]; the pipeline's vacuum-health alerter
// sidecar type-asserts it on the opened ChangeApplier and polls at the
// telemetry cadence. ADVISORY ONLY — two cheap catalog reads per minute,
// off the apply hot path, never on the value path.

// vacuumDeadTupleNoiseFloor is the minimum n_dead_tup a table must carry
// to be considered by the worst-ratio scan. Ratio alone is meaningless on
// tiny tables (10 dead of 20 rows reads 0.5 but is a trivial vacuum), and
// Postgres's own autovacuum trigger is threshold+scale-factor shaped for
// the same reason. 1000 dead tuples is far above the default
// autovacuum_vacuum_threshold (50) yet negligible bloat on any table an
// operator would page over — below it the probe reports "healthy" (a real
// observation, so a fired rule re-arms).
const vacuumDeadTupleNoiseFloor = 1000

// TargetVacuumHealth implements [ir.TargetVacuumHealthReporter]: the
// worst-dead-tuple-ratio user table above the noise floor (from
// pg_stat_user_tables — near-real-time in PG 15+'s shared-memory stats,
// flushed at transaction end on older versions) plus the connected
// database's age(datfrozenxid) wraparound headroom.
//
// ok=false only when the probe cannot produce a usable reading (query
// error — e.g. the role cannot read the stats view); zero dead tuples on
// a healthy target is ok=true with a zero-value reading. The sluice
// control tables are included in the scan deliberately: they are user-
// schema tables on the target and their bloat is just as real.
func (a *ChangeApplier) TargetVacuumHealth(ctx context.Context) (ir.VacuumHealth, bool, error) {
	var h ir.VacuumHealth

	// Worst user table by dead-tuple ratio, floor-gated. GREATEST(…, 1)
	// guards the all-dead-rows edge (n_live_tup 0) against divide-by-zero
	// while keeping the ratio exact everywhere else.
	const worstQ = `
		SELECT schemaname || '.' || relname,
		       n_dead_tup,
		       n_live_tup,
		       COALESCE(last_autovacuum, 'epoch'::timestamptz),
		       autovacuum_count
		FROM pg_stat_user_tables
		WHERE n_dead_tup >= $1
		ORDER BY n_dead_tup::float8 / GREATEST((n_dead_tup + n_live_tup)::float8, 1) DESC,
		         n_dead_tup DESC
		LIMIT 1`
	row := a.db.QueryRowContext(ctx, worstQ, vacuumDeadTupleNoiseFloor)
	var lastAV sql.NullTime
	err := row.Scan(&h.WorstTable, &h.DeadTuples, &h.LiveTuples, &lastAV, &h.AutovacuumCount)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// No table above the floor — a healthy reading, not an absence of
		// signal. Leave the zero values in place.
	case err != nil:
		return ir.VacuumHealth{}, false, fmt.Errorf("postgres: vacuum-health probe (pg_stat_user_tables): %w", err)
	default:
		if total := h.DeadTuples + h.LiveTuples; total > 0 {
			h.DeadTupleRatio = float64(h.DeadTuples) / float64(total)
		}
		// COALESCE maps NULL (never autovacuumed since stats reset) to the
		// epoch; normalise that sentinel back to the zero time the IR
		// contract documents.
		if lastAV.Valid && lastAV.Time.Unix() != 0 {
			h.LastAutovacuum = lastAV.Time
		}
	}

	// Wraparound headroom for the connected database. Same query the
	// source-side XID preflight uses (xid_wraparound_preflight.go) — this
	// is its TARGET-side advisory sibling.
	const xidQ = `SELECT age(datfrozenxid), datname FROM pg_database WHERE datname = current_database()`
	if err := a.db.QueryRowContext(ctx, xidQ).Scan(&h.XIDAge, &h.Datname); err != nil {
		return ir.VacuumHealth{}, false, fmt.Errorf("postgres: vacuum-health probe (age(datfrozenxid)): %w", err)
	}
	return h, true, nil
}
