// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// OpenBackupSnapshot implements [irbackup.SnapshotOpener] for the local
// SQLite-file engine (roadmap item 163): a full backup records the
// change log's MAX(id), read BEFORE the row sweep's reader opens, as its
// EndPosition, so a `backup incremental` chained off the full resumes
// the trigger poller at that anchor. The D1 engine (ADR-0136) reaches
// the same function through [OpenD1BackupSnapshot].
//
// Why "MAX(id) before the sweep" is gap-free here — the same argument
// the sync cold start rests on ([Engine.OpenSnapshotStream]): SQLite
// serialises writers, so the change-log id is allocated in COMMIT
// order. Every change with id ≤ anchor committed before the anchor
// read, and every table the sweep reads is read strictly AFTER it, so
// the sweep sees the change. Every change with id > anchor replays from
// the chain. A change that both committed during the sweep AND landed
// in a table read after it is in the full AND replays — an idempotent
// over-replay (ADR-0010), never a gap. On D1 the same holds at the
// primary query path (writes serialised per database); it is DERIVED
// from the local proof, not measured on a live D1 in this release.
//
// Before this existed the engine had no snapshot opener and no
// position capturer, so the orchestrator fell back to the v0.17.x path
// and recorded NO EndPosition; the first `backup incremental` then
// anchored "from now" and silently omitted every write between the
// full's sweep and its own open — measured at 100 of 100 rows by the
// v0.153.1 regression cycle.
//
// What the options mean here: PersistChainSlot (`--chain-slot`) is
// REFUSED — there is no slot to persist and none is needed, the change
// log is the retention; SlotName, ReaderParallelism and InScopeTables
// are ignored (no slot, a single serial reader — the same shape as the
// sync cold start, whose gap-freedom argument leans on the single-writer
// order — and no scope-sensitive preflight at this door).
func (Engine) OpenBackupSnapshot(ctx context.Context, dsn string, opts irbackup.SnapshotOptions) (*irbackup.Snapshot, error) {
	return openBackupSnapshot(ctx, localBackend(dsn), opts)
}

// OpenD1BackupSnapshot opens the full-backup snapshot against a live
// Cloudflare D1 database (the `d1-trigger` analogue of
// [Engine.OpenBackupSnapshot]): the same anchor-before-sweep logic over
// the D1 backend, with the cold-start rows read by the lossless `d1`
// reader (ADR-0132).
func OpenD1BackupSnapshot(ctx context.Context, dsn string, opts irbackup.SnapshotOptions) (*irbackup.Snapshot, error) {
	b, err := d1Backend(dsn)
	if err != nil {
		return nil, err
	}
	return openBackupSnapshot(ctx, b, opts)
}

// openBackupSnapshot is the transport-neutral full-backup snapshot used by
// both [Engine.OpenBackupSnapshot] (local file) and [OpenD1BackupSnapshot]
// (D1 over HTTP). It anchors the change log, THEN opens the cold-start row
// reader — that ordering is the happens-before edge the gap-freedom
// argument needs — and returns the two on an [irbackup.Snapshot] whose
// CloseFn closes the reader.
func openBackupSnapshot(ctx context.Context, b backend, opts irbackup.SnapshotOptions) (*irbackup.Snapshot, error) {
	if opts.PersistChainSlot {
		return nil, errors.New(b.driver + ": backup snapshot: --chain-slot asks for a standing replication slot, and a " +
			"trigger-CDC source has none — its chain anchor is the change log's id, which needs no slot to retain; " +
			"drop --chain-slot")
	}
	position, err := anchorChangeLog(ctx, b)
	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, b.driver+": backup snapshot: anchored at the change log's MAX(id) before the row sweep; the sweep runs SERIAL on a single reader (trigger-CDC design; no parallel knob on this flavor)",
		slog.String("position_token", position.Token))

	rowReader, err := b.coldStart.OpenRowReader(ctx, b.dsn)
	if err != nil {
		return nil, fmt.Errorf("%s: backup snapshot: open row reader: %w", b.driver, err)
	}
	return &irbackup.Snapshot{
		Position: position,
		Rows:     rowReader,
		CloseFn:  func() error { return closeReader(rowReader) },
	}, nil
}

// anchorChangeLog is the shared anchor read behind both trigger-native
// handoffs — the sync cold start ([openSnapshotStream]) and the full backup
// ([openBackupSnapshot]) — so the two cannot drift on what the anchor IS.
// On a short-lived read executor it refuses loudly when the change log is
// absent (the operator forgot `sluice trigger setup`), reads
// COALESCE(MAX(id), 0), grades the id-allocation state against it
// ([verifyChangeLogWatermark] — CDC door 2 of 2; a cold start that hands
// back an anchor the log can dip below has already lost the changes
// captured while it dipped), and encodes the position. The caller opens its
// row reader strictly AFTER this returns.
func anchorChangeLog(ctx context.Context, b backend) (ir.Position, error) {
	anchorExec, err := b.openExec(ctx, true)
	if err != nil {
		return ir.Position{}, fmt.Errorf("%s: snapshot: open: %w", b.driver, err)
	}
	defer func() { _ = anchorExec.close() }()
	if exists, err := anchorExec.changeLogExists(ctx); err != nil {
		return ir.Position{}, fmt.Errorf("%s: snapshot: preflight: %w", b.driver, err)
	} else if !exists {
		return ir.Position{}, changeLogAbsentErr(b.driver)
	}
	anchor, err := anchorExec.maxChangeLogID(ctx)
	if err != nil {
		return ir.Position{}, fmt.Errorf("%s: snapshot: read CDC anchor: %w", b.driver, err)
	}
	if err := verifyChangeLogWatermark(ctx, anchorExec, b.driver, anchor); err != nil {
		return ir.Position{}, err
	}
	position, err := encodePos(sqliteTriggerPos{LastID: anchor})
	if err != nil {
		return ir.Position{}, fmt.Errorf("%s: snapshot: encode position: %w", b.driver, err)
	}
	return position, nil
}
