// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package floatrepair holds the engine-neutral skeleton of the
// VStream-COPY FLOAT display-rounding repair (roadmap open-bug
// 2026-07-09; audit ARCH-F3). A PlanetScale/Vitess cold-start COPY
// lands single-precision FLOAT columns display-rounded; the sync
// cold-start re-reads those columns EXACTLY from the source and UPDATEs
// the target rows by primary key BEFORE CDC begins.
//
// The MySQL and Postgres target writers were ~95% identical per-row
// UPDATE loops; this package hoists the shared control flow — PK
// validation, deterministic SET-column derivation, generated-column
// filtering, and BATCHING — leaving each engine only the dialect-
// specific batched-statement builder (its [BatchExecer]). Mirrors the
// internal/engines/internal/triggercdc shared-package pattern.
//
// # Batching (audit PERF-P1)
//
// The pre-batch implementation issued one `UPDATE … SET floats WHERE pk`
// per row — one network round-trip per row, so a large table on a WAN
// PlanetScale target took hours-to-days. [RepairByPK] instead folds
// batchRows rows into ONE statement (an UPDATE against a VALUES/UNION
// join the engine builds), cutting round-trips from O(rows) to
// O(rows/batchRows). Each batch is a single autocommit statement: a
// multi-row UPDATE is atomic, and the repair is idempotent and runs
// before the CDC anchor persists, so a crash mid-repair just
// re-cold-starts — no explicit BEGIN/COMMIT is needed (dropping those
// per-batch round-trips too).
package floatrepair

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// BatchExecer builds and executes ONE batched UPDATE for a set of repair
// rows against a single engine's target. The skeleton hands it the
// already-filtered join key (pkColumns) and SET columns (setColumns) —
// both in the deterministic, generated-column-filtered order the per-row
// path used — plus the batch of full (PK + FLOAT) rows. The engine reads
// each row's values by column name, shapes them with the SAME value-
// shaping the CDC applier uses, and issues one statement.
type BatchExecer interface {
	ExecBatch(ctx context.Context, table *ir.Table, pkColumns, setColumns []string, batch []ir.Row) error
}

// RepairByPK is the engine-neutral driver for the FLOAT re-read repair.
// It streams rows off the channel, accumulates them into batches of
// batchRows, and flushes each batch through exec.ExecBatch as ONE
// statement.
//
// Column selection matches the per-row implementation byte-for-byte:
//   - The join key is pkColumns, sorted and generated-column-filtered
//     via [appliershared.NonGeneratedRowKeys] — exactly what the per-row
//     WHERE builder produced (a STORED generated column in the predicate
//     risks a silent zero-rows-affected match).
//   - The SET columns are each row's NON-PK keys, likewise sorted +
//     generated-filtered. A PK-only row (nothing to SET — the FLOAT
//     column was itself a PK member, which the caller already excludes)
//     is skipped, never emitted as an empty UPDATE.
//
// Every row in the stream must carry the same repairable-column set (the
// caller trims each to PK + repairable-FLOAT); a mismatch is refused
// loudly rather than silently updating a different column set per batch.
//
// A row absent on the target is a clean join miss (zero rows affected),
// NOT an error — the same semantics as the per-row path; the subsequent
// CDC replay from the copy anchor is the authority on such rows.
func RepairByPK(ctx context.Context, table *ir.Table, pkColumns []string, rows <-chan ir.Row, batchRows int, exec BatchExecer) error {
	if table == nil {
		return errors.New("floatrepair: RepairByPK: table is nil")
	}
	if len(pkColumns) == 0 {
		return fmt.Errorf("floatrepair: RepairByPK: table %q has no primary key columns", table.Name)
	}
	if batchRows < 1 {
		// Defensive: a zero/negative cap would never flush mid-stream. Treat
		// it as one-row-per-statement (the pre-batch behaviour) rather than
		// buffering the whole table.
		batchRows = 1
	}

	colTypes := colTypesByName(table.Columns)
	pkSet := make(map[string]struct{}, len(pkColumns))
	for _, c := range pkColumns {
		pkSet[c] = struct{}{}
	}

	// Effective join key: sorted + generated-filtered, once (pkColumns is
	// constant for the stream). NonGeneratedRowKeys keys on the map's keys
	// only, so a placeholder-valued row suffices.
	pkKeyRow := make(ir.Row, len(pkColumns))
	for _, c := range pkColumns {
		pkKeyRow[c] = struct{}{}
	}
	effPK := appliershared.NonGeneratedRowKeys(pkKeyRow, colTypes)
	if len(effPK) == 0 {
		return fmt.Errorf("floatrepair: RepairByPK: table %q has no non-generated primary key columns to key the repair on", table.Name)
	}

	var (
		batch      []ir.Row
		setColumns []string
	)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := exec.ExecBatch(ctx, table, effPK, setColumns, batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	for row := range rows {
		after := make(ir.Row, len(row))
		for k, v := range row {
			if _, isPK := pkSet[k]; !isPK {
				after[k] = v
			}
		}
		cols := appliershared.NonGeneratedRowKeys(after, colTypes)
		if len(cols) == 0 {
			// PK-only row: nothing to SET — skip, exactly as the per-row path
			// did (the caller filters these, but stay defensive).
			continue
		}
		if setColumns == nil {
			setColumns = cols
		} else if !slices.Equal(setColumns, cols) {
			return fmt.Errorf("floatrepair: RepairByPK: table %q: inconsistent repair column set across rows (%v vs %v); "+
				"every streamed row must carry the same PK+FLOAT shape", table.Name, setColumns, cols)
		}
		batch = append(batch, row)
		if len(batch) >= batchRows {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// colTypesByName builds the column-type lookup keyed by unqualified
// column name — the input NonGeneratedRowKeys (and each engine's value
// shaping) consumes.
func colTypesByName(cols []*ir.Column) map[string]*ir.Column {
	m := make(map[string]*ir.Column, len(cols))
	for _, c := range cols {
		m[c.Name] = c
	}
	return m
}
