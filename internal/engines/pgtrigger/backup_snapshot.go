// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"errors"
	"log/slog"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// OpenBackupSnapshot implements [irbackup.SnapshotOpener] for the
// trigger engine (roadmap item 163): a full backup's row sweep runs on
// a pinned REPEATABLE READ view, and the change log's SETTLED anchor —
// the same contiguous-committed-prefix + settle + clamp the sync cold
// start computes ([openAnchoredSnapshot]) — is captured INSIDE that
// view and recorded as the manifest's EndPosition. A `backup
// incremental` chained off the full resumes the trigger poller at that
// anchor, so the chain is contiguous by the same argument the
// snapshot→CDC handoff already rests on: everything ≤ anchor is in the
// sweep, everything > anchor replays.
//
// Before this existed the engine had no share of the anchor machinery
// at the backup door: the orchestrator fell back to the v0.17.x
// non-snapshot path, and the delegated postgres reader's own capturer
// recorded a pgoutput {slot, lsn} the trigger poller cannot decode —
// so the first `backup incremental` refused with a foreign-position
// error, while the SQLite/D1 twins (no capturer at all) extended "from
// now" and silently omitted every write between the full's sweep and
// the incremental's anchor (measured at 100 of 100 rows by the v0.153.1
// regression cycle).
//
// What the options mean here, stated rather than implied:
//
//   - PersistChainSlot (`--chain-slot`) is REFUSED. It asks for a
//     standing replication slot to retain WAL for the chain; a
//     trigger-CDC source has no slot and needs none — the change log
//     IS the retention, and its prune floor is the consumer registry
//     (see the item-163 residual on the prune floor in
//     docs/operator/cdc-streaming.md).
//   - SlotName is ignored (no slot concept).
//   - ReaderParallelism is ignored: the sweep runs SERIAL on the one
//     pinned connection, exactly as the sync cold start does (the
//     anchor's consistency argument is bound to that single view;
//     SnapshotName stays empty so the orchestrator never fans out).
//   - InScopeTables is ignored: this engine has no scope-sensitive
//     preflight at the snapshot door.
//
// CloseFn commits the read-only snapshot tx, closes the pinned conn and
// the pool. There is no CommitFn: nothing run-scoped outlives the run.
func (e Engine) OpenBackupSnapshot(ctx context.Context, dsn string, opts irbackup.SnapshotOptions) (*irbackup.Snapshot, error) {
	if opts.PersistChainSlot {
		return nil, errors.New("pgtrigger: backup snapshot: --chain-slot asks for a standing replication slot, and a " +
			"trigger-CDC source has none — its chain anchor is the change log's id, which needs no slot to retain; " +
			"drop --chain-slot")
	}
	snap, err := openAnchoredSnapshot(ctx, dsn, e.appID)
	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "pgtrigger: backup snapshot: anchored at the change log's settled id; the row sweep runs SERIAL on the single snapshot connection (trigger-CDC design; no parallel knob on this flavor)",
		slog.String("position_token", snap.position.Token))
	return &irbackup.Snapshot{
		Position: snap.position,
		Rows:     snap.rows(),
		CloseFn:  snap.close,
	}, nil
}
