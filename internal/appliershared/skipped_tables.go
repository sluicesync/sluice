// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

// # The unknown-target-table skip ledger (audit C-11)
//
// When a CDC stream carries changes for a table the TARGET lacks, every
// engine's applier skips them rather than halting — the source still
// holds every skipped row, so a skipped table is always recoverable
// with `sluice schema add-table` (fresh table snapshot), while a halted
// stream lags every table and, past binlog/slot retention, loses the
// position itself. The skip is never log-only: each one increments a
// durable per-(stream, table) row in `sluice_cdc_skipped_tables` with
// first/last skipped position tokens, and `sync status` / the `sync
// stop` summary / `sync health` surface the record (nonzero trips
// health).
//
// This file owns the engine-neutral halves: the table name, the
// once-per-table WARN (tracker + wording, shared so the two engines
// cannot drift), and the shared list scan behind each engine's
// [ir.SkippedTableLister]. The DDL, the upsert (ON CONFLICT vs ON
// DUPLICATE KEY), and the missing-table classifier stay engine-side,
// per the ADR-0081 tier-c seam.

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// SkippedTablesTableName is the per-target control table that holds the
// durable unknown-target-table skip ledger (audit C-11): one row per
// (stream_id, table_name) with a cumulative skip count and the first /
// last skipped source position tokens. Rows survive restarts and
// accumulate; they are cleared only by [ir.StreamCleaner]-style resets.
const SkippedTablesTableName = "sluice_cdc_skipped_tables"

// SkippedTableRemedy aliases the ir-level remedy string so the
// engines' call sites in this package family read locally; the single
// source of truth is [ir.SkippedTableRemedy] (the CLI and pipeline
// surfaces read it from ir without importing this package).
const SkippedTableRemedy = ir.SkippedTableRemedy

// SkipWarnTracker records which tables have already had their
// once-per-applier-lifetime unknown-target-table WARN, so a busy
// missing table logs one WARN, not one per event. Mutex-guarded because
// the ADR-0104/0105 concurrent apply lanes reach the skip path from W
// goroutines. The zero value is ready to use.
type SkipWarnTracker struct {
	mu     sync.Mutex
	warned map[string]bool
}

// FirstSighting reports whether this is the first skip for the given
// qualified table in this tracker's lifetime, marking it seen.
func (t *SkipWarnTracker) FirstSighting(qualifiedTable string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.warned[qualifiedTable] {
		return false
	}
	if t.warned == nil {
		t.warned = map[string]bool{}
	}
	t.warned[qualifiedTable] = true
	return true
}

// WarnSkippedTable emits the once-per-table skip WARN (the caller gates
// it on [SkipWarnTracker.FirstSighting]). op names the first skipped
// operation ("insert"/"update"/"delete"/"truncate") for diagnosis; the
// hint names the remedy and the durable ledger so the WARN is a
// pointer, not the record.
func WarnSkippedTable(ctx context.Context, engineName, op, qualifiedTable string) {
	slog.WarnContext(
		ctx, engineName+": applier: target lacks this table — skipping its CDC events (counted durably in "+SkippedTablesTableName+"; `sluice sync health` trips while the count is nonzero)",
		slog.String("op", op),
		slog.String("table", qualifiedTable),
		slog.String("hint", SkippedTableRemedy),
	)
}

// ListSkippedTables runs the engine-built SELECT over the canonical
// 7-column projection — stream_id, table_name, skip_count,
// first_position, last_position, first_skipped_at, last_skipped_at —
// and scans it into the pipeline-facing records. Tolerant of the table
// being absent (a never-CDC'd or pre-upgrade target lists no skips
// rather than erroring — the same posture as [ListStreams]); every
// other error, including a row that doesn't scan, is surfaced rather
// than guessed around (the rows are a persisted round-trip surface).
func ListSkippedTables(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, query string) ([]ir.SkippedTableRecord, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		if cfg.IsMissingTable(err) {
			return []ir.SkippedTableRecord{}, nil
		}
		return nil, fmt.Errorf("%s: list skipped tables: %w", cfg.EngineName, err)
	}
	defer func() { _ = rows.Close() }()

	out := []ir.SkippedTableRecord{}
	for rows.Next() {
		var (
			rec          ir.SkippedTableRecord
			firstSkipped time.Time
			lastSkipped  time.Time
		)
		if err := rows.Scan(&rec.StreamID, &rec.Table, &rec.SkipCount, &rec.FirstPosition, &rec.LastPosition, &firstSkipped, &lastSkipped); err != nil {
			return nil, fmt.Errorf("%s: scan skipped table: %w", cfg.EngineName, err)
		}
		rec.FirstSkippedAt = firstSkipped
		rec.LastSkippedAt = lastSkipped
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: list skipped tables: %w", cfg.EngineName, err)
	}
	return out, nil
}
