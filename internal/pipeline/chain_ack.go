// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// slotAckReleaser is the structural seam to a CDC reader whose source
// keeps server-side consumer state that an ack RELEASES (Postgres: the
// logical slot's confirmed_flush_lsn, via [postgres.CDCReader]'s
// ReleaseSlotAckTo). Such a reader never acks past the highest position
// released through this seam (GC-41: unconditionally, floored at the
// stream's start), so every consumer of one owes it a release of each
// position it holds DURABLY — or the slot retains WAL for the life of
// the stream. There are exactly two kinds of consumer, and each releases
// from its own durable evidence:
//
//   - the backup-chain orchestrators ([IncrementalBackup], [BackupStream])
//     release a window's EndPosition after its manifest commits
//     ([releaseChainAckTo]);
//   - the continuous-sync [Streamer] releases the target's persisted
//     control-row position, read back from the target
//     ([Streamer.startSlotAckCeiling]).
//
// Engines without server-side consumer state (MySQL binlog, VStream) and
// the trigger-CDC engines (whose change-log frontier has its own
// registry, [ir.ChangeLogConsumerRegistry]) don't implement it and need
// no equivalent. The roster that holds every CDC call site to this is
// TestSlotAckReleaseRoster_EveryStreamChangesSiteReleases.
type slotAckReleaser interface {
	// ReleaseSlotAckTo ratchets the ack ceiling to pos (monotonic).
	ReleaseSlotAckTo(pos ir.Position) error
}

// releaseChainAckTo raises the ack ceiling to pos after a backup-chain
// window has been durably committed. Failures are logged, not fatal: an
// un-released ack only delays WAL release (retention-side pressure), it
// never loses chain data — the loud path is reserved for the opposite
// direction (acking too far).
func releaseChainAckTo(ctx context.Context, cdc ir.CDCReader, pos ir.Position) {
	releaser, ok := cdc.(slotAckReleaser)
	if !ok {
		return
	}
	if err := releaser.ReleaseSlotAckTo(pos); err != nil {
		slog.WarnContext(
			ctx, "backup: release slot ack failed; source WAL release is delayed until the next chain link",
			slog.String("err", err.Error()),
		)
	}
}
