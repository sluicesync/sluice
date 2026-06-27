// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/engines/sqlite"
	"sluicesync.dev/sluice/internal/ir"
)

// OpenSnapshotStream opens a trigger-native consistent snapshot + CDC handoff
// (ADR-0135 §4). It pairs the cold-start bulk-copy reader (the delegated
// [sqlite.Engine] RowReader — full storage-class fidelity, within-table chunking,
// the ADR-0129 date/bool policy) with the trigger CDC poller, anchored so the
// union covers every row exactly once with no gap and no double-apply-loss.
//
// # Gap-freedom (why MAX(id) read BEFORE the snapshot is sufficient for SQLite)
//
// The anchor is COALESCE(MAX(id), 0) read on the change-log BEFORE any snapshot
// table is read; CDC then replays id > anchor. The correctness argument relies on
// SQLite's SINGLE-WRITER model: only one write transaction is active at a time,
// so the change-log id (INTEGER PRIMARY KEY AUTOINCREMENT) is allocated in COMMIT
// order and is strictly monotonic. Therefore:
//
//   - Any change with id ≤ anchor committed at or before the anchor read; every
//     snapshot table read happens strictly AFTER the anchor read, so the snapshot
//     sees it → it is bulk-copied. (No gap: nothing ≤ anchor is missing.)
//   - Any change with id > anchor is replayed by CDC. A row that ALSO landed in
//     the snapshot (it committed during the copy) just re-applies idempotently on
//     its PK (ADR-0010). Over-replay is SAFE; a gap is forbidden.
//
// This is the load-bearing SIMPLIFICATION over pgtrigger, whose PG bigserial id
// is NOT commit-ordered (a low id can commit after a higher one, and rolled-back
// txns leave gaps), forcing its contiguous-committed-prefix anchor + xmin
// safety-lag. SQLite needs neither: the single-writer total order makes the naive
// MAX(id)-before-snapshot anchor gap-free.
//
// Lifecycle: Rows and Changes own independent read connections; ReleaseRows
// closes the bulk-copy reader and Close stops the poller + closes both.
func (e Engine) OpenSnapshotStream(ctx context.Context, dsn string) (*ir.SnapshotStream, error) {
	// Refuse loudly when the change-log is absent — the operator forgot
	// `sluice trigger setup`. Fire it here so cold-start aborts before any data
	// moves rather than mid-stream. Use a short-lived read connection for the
	// preflight + anchor read.
	anchorDB, _, err := sqlite.OpenFile(ctx, dsn, true)
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: snapshot: open: %w", err)
	}
	if exists, err := changeLogTableExists(ctx, anchorDB); err != nil {
		_ = anchorDB.Close()
		return nil, fmt.Errorf("sqlite-trigger: snapshot: preflight: %w", err)
	} else if !exists {
		_ = anchorDB.Close()
		return nil, fmt.Errorf(
			"sqlite-trigger: %s does not exist on the source — run `sluice trigger setup --source-driver sqlite-trigger --dsn=... --tables=...` before starting the stream",
			ChangeLogTable,
		)
	}

	// Capture the anchor BEFORE constructing the Rows reader (the happens-before
	// edge the gap-freedom argument above needs).
	anchor, err := readChangeLogMaxID(ctx, anchorDB)
	_ = anchorDB.Close()
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: snapshot: read CDC anchor: %w", err)
	}
	position, err := encodePos(sqliteTriggerPos{LastID: anchor})
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: snapshot: encode position: %w", err)
	}

	// Rows: the delegated cold-start reader (its own read-only pool).
	rowReader, err := e.sq.OpenRowReader(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: snapshot: open row reader: %w", err)
	}

	// Changes: the trigger poller, resuming from the anchor (its own pool).
	cdcReader, err := openCDCReader(ctx, dsn)
	if err != nil {
		_ = closeReader(rowReader)
		return nil, fmt.Errorf("sqlite-trigger: snapshot: open cdc reader: %w", err)
	}

	stream := &ir.SnapshotStream{
		Position: position,
		Rows:     rowReader,
		Changes:  cdcReader,
	}

	// rowsReleased guards ReleaseRows against a double close. Not mutex-guarded:
	// the orchestrator serialises ReleaseRows (after bulk-copy) and Close (on
	// unwind); Close also calls releaseRows as a safety net. Mirrors pgtrigger.
	rowsReleased := false
	releaseRows := func() error {
		if rowsReleased {
			return nil
		}
		rowsReleased = true
		return closeReader(rowReader)
	}
	stream.ReleaseRowsFn = releaseRows
	stream.CloseFn = func() error {
		// Stop the CDC poller first (cancels its pump + closes its pool), then
		// release the bulk-copy reader if not already.
		var firstErr error
		if err := cdcReader.(interface{ Close() error }).Close(); err != nil {
			firstErr = err
		}
		if err := releaseRows(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	return stream, nil
}
