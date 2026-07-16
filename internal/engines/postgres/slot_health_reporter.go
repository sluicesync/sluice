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

// SlotHealth implements [ir.SlotHealthReporter]. Returns a per-probe
// snapshot of the named slot's retention pressure (restart_lsn lag vs.
// `max_slot_wal_keep_size`) and active-liveness flag — severity-A
// finding F13 of the 2026-05-22 Reddit-research run, see ADR-0059.
//
// The single query joins `pg_replication_slots` (slot row), the live
// WAL head via `pg_current_wal_lsn()`, and the `max_slot_wal_keep_size`
// GUC from `pg_settings.setting` (numeric, in MB; -1 sentinel = unlimited
// per the GUC's documented contract). `pg_wal_lsn_diff` does the lag
// arithmetic on the server so we don't shuttle two LSN strings back and
// parse them client-side.
//
// **No row for the slot → ok=false.** Returned cleanly rather than as
// an error: a freshly-cold-started stream may probe this surface before
// the very first START_REPLICATION has populated the slot row. The
// consumer treats ok=false as "no signal this tick" and waits for the
// next probe.
//
// **Empty slotName → error.** The caller's wiring is broken — same
// shape as [SchemaReader.SlotSpillStats].
//
// **Why the GUC reads from pg_settings.setting rather than
// current_setting().** `current_setting('max_slot_wal_keep_size')`
// returns a human-readable string with units ("1024MB", "-1", "0").
// `pg_size_bytes` parses the human-readable form but errors on "-1"
// (it's not a valid size literal). `pg_settings.setting` returns the
// raw numeric value in the column's documented unit (MB); -1 passes
// through cleanly. The conversion to bytes happens in Go.
func (r *SchemaReader) SlotHealth(ctx context.Context, slotName string) (ir.SlotHealth, bool, error) {
	if r.db == nil {
		return ir.SlotHealth{}, false, errors.New("postgres: SlotHealth: reader not opened")
	}
	if slotName == "" {
		return ir.SlotHealth{}, false, errors.New("postgres: SlotHealth: slotName is empty")
	}

	// pg_replication_slots.wal_status was added in PG 13. sluice's
	// declared baseline is PG 14+ (F2 already pins this), so the column
	// is always present; no COALESCE needed.
	//
	// pg_wal_lsn_diff returns numeric — cast to bigint for the int64
	// scan target. The diff fits comfortably: lag values of multiple
	// TB are still well under int64 max.
	const q = `
		SELECT
		    s.active,
		    s.wal_status,
		    pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)::bigint AS lag_bytes,
		    (SELECT setting::bigint FROM pg_settings WHERE name = 'max_slot_wal_keep_size') AS max_keep_mb
		FROM pg_replication_slots s
		WHERE s.slot_name = $1`

	var h ir.SlotHealth
	h.SlotName = slotName
	var maxKeepMB sql.NullInt64
	var lagBytes sql.NullInt64

	err := r.db.QueryRowContext(ctx, q, slotName).Scan(&h.Active, &h.WALStatus, &lagBytes, &maxKeepMB)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// Slot doesn't exist (yet) — surface as "unavailable" rather
		// than an error. Cold-start can probe before the slot row
		// materialises.
		return ir.SlotHealth{}, false, nil
	case err != nil:
		return ir.SlotHealth{}, false, fmt.Errorf("postgres: SlotHealth: %w", err)
	}

	// lag_bytes can be NULL when restart_lsn is itself NULL — a slot
	// that exists but hasn't been used, or a slot Postgres has
	// INVALIDATED (restart_lsn is nulled on invalidation). Treat as
	// zero lag for the threshold path; that's accurate for the unused
	// case (nothing to retain), and the invalidated case is dispatched
	// on wal_status='lost' by the evaluator BEFORE any percentage math
	// (audit MED-D0-9: percent 0 on a lost slot used to read as "clean"
	// and emit a false "condition cleared").
	if lagBytes.Valid {
		h.LagBytes = lagBytes.Int64
	}

	// max_keep_mb is the GUC value as documented:
	//   -1 = unlimited (PG default)
	//    0 = no retention (extreme operator setting)
	//   >0 = MB cap
	// Convert to bytes for the threshold math; preserve the -1 sentinel.
	switch {
	case !maxKeepMB.Valid:
		// Defensive: the subquery should always return a row (every
		// installation has the GUC). If it doesn't, treat as unlimited
		// so we don't spam a percentage WARN against a phantom bound.
		h.MaxKeepSizeBytes = -1
	case maxKeepMB.Int64 < 0:
		h.MaxKeepSizeBytes = -1
	default:
		h.MaxKeepSizeBytes = maxKeepMB.Int64 * 1024 * 1024
	}

	return h, true, nil
}
