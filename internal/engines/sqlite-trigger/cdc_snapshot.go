// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// OpenSnapshotStream opens a trigger-native consistent snapshot + CDC handoff
// (ADR-0135 §4) for a local SQLite FILE. It delegates to [openSnapshotStream]
// with the local backend; the D1 engine (ADR-0136) calls the same shared
// function with the D1 backend via [OpenD1SnapshotStream].
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
// MAX(id)-before-snapshot anchor gap-free. On D1 the same holds at the primary
// query path (writes serialised per database; ADR-0136 §4).
//
// Lifecycle: Rows and Changes own independent read connections; ReleaseRows
// closes the bulk-copy reader and Close stops the poller + closes both.
func (e Engine) OpenSnapshotStream(ctx context.Context, dsn string) (*ir.SnapshotStream, error) {
	return openSnapshotStream(ctx, localBackend(dsn))
}

// openSnapshotStream is the transport-neutral snapshot→CDC handoff used by both
// the local file engine and the D1 engine (ADR-0136). The backend supplies the
// cold-start row reader, the executor (for the anchor read + the change-log
// preflight), and the CDC reader.
func openSnapshotStream(ctx context.Context, b backend) (*ir.SnapshotStream, error) {
	// Refuse loudly when the change-log is absent — the operator forgot
	// `sluice trigger setup`. Fire it here so cold-start aborts before any data
	// moves rather than mid-stream. Use a short-lived executor for the preflight
	// + anchor read (captured BEFORE the Rows reader — the happens-before edge the
	// gap-freedom argument needs).
	anchorExec, err := b.openExec(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: snapshot: open: %w", err)
	}
	if exists, err := anchorExec.changeLogExists(ctx); err != nil {
		_ = anchorExec.close()
		return nil, fmt.Errorf("sqlite-trigger: snapshot: preflight: %w", err)
	} else if !exists {
		_ = anchorExec.close()
		return nil, changeLogAbsentErr(b.driver)
	}

	anchor, err := anchorExec.maxChangeLogID(ctx)
	_ = anchorExec.close()
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: snapshot: read CDC anchor: %w", err)
	}
	position, err := encodePos(sqliteTriggerPos{LastID: anchor})
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: snapshot: encode position: %w", err)
	}

	// Rows: the delegated cold-start reader (its own read connection).
	rowReader, err := b.coldStart.OpenRowReader(ctx, b.dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite-trigger: snapshot: open row reader: %w", err)
	}

	// Changes: the trigger poller, resuming from the anchor (its own executor).
	cdcReader, err := openCDCReaderBackend(ctx, b)
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
