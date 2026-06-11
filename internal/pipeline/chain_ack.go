// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// chainAckController is the structural seam to the engine CDC reader's
// chain-consumer ack mode (Postgres: [postgres.CDCReader]'s
// HoldSlotAckAtCommitted / ReleaseSlotAckTo). The backup-chain
// orchestrators ([IncrementalBackup], [BackupStream]) have no change
// applier, so the reader has no applied-LSN tracker — and the
// no-tracker keepalive fallback acks the STREAMED position, which can
// run ahead of what the orchestrator durably commits to chunks. An
// ack past the recorded EndPosition releases source WAL the chain has
// not captured: the next link would silently start past it (the
// walsender fast-forwards to confirmed_flush_lsn). Hold-then-release
// pins the ack to durably-committed window ends instead.
//
// Same type-assert/silently-omit shape as [lsnTrackerAttacher]:
// engines without server-side consumer state (MySQL) don't implement
// it and need no equivalent.
type chainAckController interface {
	// HoldSlotAckAtCommitted must be called before StreamChanges.
	HoldSlotAckAtCommitted()
	// ReleaseSlotAckTo ratchets the ack ceiling to pos (monotonic).
	ReleaseSlotAckTo(pos ir.Position) error
}

// preflightChainResume runs the engine's [ir.ChainResumePreflighter]
// (when implemented) against the chain's resume position before any
// CDC stream opens. Shared by [IncrementalBackup.Run] and
// [BackupStream.Run] — the refusal semantics are identical: a slot
// that is missing or has advanced past `from` cannot serve the chain
// gap-free, and starting the stream anyway would silently skip the
// WAL in between. The zero position (a "from now" chain start) skips
// the check; engines without the surface (MySQL) skip it too.
func preflightChainResume(ctx context.Context, source ir.Engine, dsn string, from ir.Position) error {
	pf, ok := source.(ir.ChainResumePreflighter)
	if !ok || (from.Engine == "" && from.Token == "") {
		return nil
	}
	return pf.PreflightChainResume(ctx, dsn, from)
}

// holdChainAck switches cdc into chain-consumer ack mode when the
// engine supports it. Call before StreamChanges.
func holdChainAck(cdc ir.CDCReader) {
	if holder, ok := cdc.(chainAckController); ok {
		holder.HoldSlotAckAtCommitted()
	}
}

// releaseChainAckTo raises the chain-consumer ack ceiling to pos after
// a window has been durably committed. Failures are logged, not
// fatal: an un-released ack only delays WAL release (retention-side
// pressure), it never loses chain data — the loud path is reserved
// for the opposite direction (acking too far).
func releaseChainAckTo(ctx context.Context, cdc ir.CDCReader, pos ir.Position) {
	holder, ok := cdc.(chainAckController)
	if !ok {
		return
	}
	if err := holder.ReleaseSlotAckTo(pos); err != nil {
		slog.WarnContext(
			ctx, "backup: release slot ack failed; source WAL release is delayed until the next chain link",
			slog.String("err", err.Error()),
		)
	}
}
