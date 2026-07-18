// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// warmResume opens a CDC reader on the source and starts streaming
// from the persisted position. No snapshot, no bulk-copy.
//
// lsnTracker is the opaque applied-LSN feedback channel (Bug 15,
// ADR-0020). Attached to the reader before StreamChanges so the
// keepalive path uses applied-LSN from the very first ack — no
// window where the slot could advance past un-applied work just
// because the reader was constructed before the tracker was
// passed through. nil tracker means the engine doesn't support
// LSN feedback (the pre-v0.5.0 shape) or the applier isn't a
// matching engine; the reader falls back to streamed-LSN.
//
// Warm resume reuses the publication scope established at cold
// start; we don't re-read the schema or re-call EnsurePublication
// here. Defence-in-depth lives in the applier's dispatch path
// (skip-with-warning on unknown tables).
//
// stop is a non-nil teardown closure the caller MUST defer. It closes
// the CDC reader, which terminates the engine's binlog/replication
// goroutine deterministically. Cancelling ctx alone only unwinds the
// pump (the channel closes when the pump exits); it does NOT stop the
// go-mysql BinlogSyncer goroutine spawned by StreamChanges. Without an
// explicit Close that goroutine runs to its reconnect-retry budget
// (~30s under a torn-down source) and keeps logging via slog.Default()
// — which, when a later test in the same binary swaps slog.Default()
// via captureSlog, surfaces a cross-test DATA RACE under `-race`. The
// closure is always non-nil (no-op on error paths, which clean up
// inline) so the caller can defer it unconditionally.
func (s *Streamer) warmResume(ctx context.Context, persisted ir.Position, lsnTracker any) (changes <-chan ir.Change, stop func(), err error) {
	stop = func() {}
	slog.InfoContext(
		ctx, "warm resume from persisted position",
		slog.String("position_token", persisted.Token),
	)
	cdc, err := openCDCReaderWithOptionalSlot(ctx, s.Source, s.SourceDSN, s.SlotName)
	if err != nil {
		return nil, stop, migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("pipeline: open cdc reader: %w", err))
	}
	if lsnTracker != nil {
		if attacher, ok := cdc.(lsnTrackerAttacher); ok {
			attacher.AttachLSNTracker(lsnTracker)
		}
	}
	// Roadmap item 18(c): apply operator-supplied --poll-interval to
	// poll-based CDC readers (today: postgres-trigger). Push-based
	// engines (pgoutput, binlog, VStream) don't implement the setter
	// and silently ignore.
	if s.PollInterval > 0 {
		if setter, ok := cdc.(pollIntervalSetter); ok {
			setter.SetPollInterval(s.PollInterval)
		}
	}
	// ADR-0091 F7a: when single-stream forwarding is active, tell the
	// reader to relax its mid-stream schema-change gate so the unambiguous
	// shapes reach the forward intercept as SchemaSnapshots instead of
	// being refused / swallowed at the source-read level. PG (GAP #1: DROP
	// COLUMN / ALTER COLUMN TYPE) and MySQL (GAP #2: ALTER COLUMN
	// NULLABILITY — the nullability-only change that does not move the
	// decode signature) both implement the setter; readers that don't
	// (vstream) silently ignore.
	if setter, ok := cdc.(schemaForwardModeSetter); ok {
		setter.SetSchemaForward(s.singleStreamSchemaForwardActive())
	}
	// ADR-0173 Phase 2: request UN-narrowed before-images for the filtered
	// tables (row-move eval needs every OLD column); refuses if unsupported.
	if err := s.applyFullBeforeImageTables(cdc); err != nil {
		migcore.CloseIf(cdc)
		return nil, stop, err
	}
	// ADR-0174 Piece 2 / audit F-P1: push the --where predicates into the
	// reader's SERVER-SIDE stream filter on warm resume so the resumed stream
	// is reduced at the source (VStream) instead of pulling the whole keyspace
	// and discarding ~99% client-side after every restart. PERFORMANCE only —
	// the client-side row-move classification (interceptWhereFilter → route)
	// still runs on every delivered change and preserves correctness; readers
	// with no server-side stream filter (binlog / pgoutput) don't implement the
	// setter and silently no-op (correct, just unfiltered at the source).
	if len(s.RowFilters) > 0 {
		if setter, ok := cdc.(ir.ServerSideCDCFilterSetter); ok {
			setter.SetServerSideRowFilters(s.RowFilters)
		}
	}
	changes, err = cdc.StreamChanges(ctx, persisted)
	if err != nil {
		migcore.CloseIf(cdc)
		return nil, stop, migcore.WrapWithHint(migcore.PhaseCDC, fmt.Errorf("pipeline: start cdc: %w", err))
	}
	// Hand the caller a closure that closes the CDC reader. The reader's
	// Close cancels its pump AND closes the underlying syncer/slot, so
	// the engine-side streaming goroutine is joined deterministically
	// rather than left to run out its reconnect budget after ctx cancel.
	stop = func() { migcore.CloseIf(cdc) }
	// GitHub issue #19: capture the reader's Err method so runOnce
	// can surface a pump error (transient `read: connection reset`
	// etc.) into the ADR-0038 retry loop after the changes channel
	// closes. Optional-interface probe; pre-v0.46 readers without
	// Err() pass through as nil and runOnce's check no-ops.
	if errer, ok := cdc.(interface{ Err() error }); ok {
		s.sourceErrFn = errer.Err
	}
	// ADR-0094: reshard-reopen surface (VStream flavors) for runOnce.
	if rr, ok := cdc.(ir.ReshardReopener); ok {
		s.sourceReshard = rr
	}
	// ADR-0137 Phase B: capture the change-log-pruner surface (trigger-CDC
	// engines) so the apply-phase auto-prune sidecar can reap the source
	// change-log on a cadence. Non-trigger readers don't implement it → nil.
	if p, ok := cdc.(ir.ChangeLogPruner); ok {
		s.changeLogPruner = p
	}
	return changes, stop, nil
}

// openCDCReaderWithOptionalSlot calls the engine's slot-aware
// OpenCDCReaderWithSlot when slotName is non-empty AND the engine
// implements [ir.CDCReaderWithSlotOpener]. Otherwise falls back to
// the default OpenCDCReader. Engines without slot concepts (MySQL)
// silently ignore an operator-supplied slot name.
//
// The split keeps the streamer's main paths readable — the
// type-assertion dance lives in one place rather than at every
// open-CDC call site.
func openCDCReaderWithOptionalSlot(ctx context.Context, source ir.Engine, dsn, slotName string) (ir.CDCReader, error) {
	if slotName == "" {
		return source.OpenCDCReader(ctx, dsn)
	}
	if opener, ok := source.(ir.CDCReaderWithSlotOpener); ok {
		return opener.OpenCDCReaderWithSlot(ctx, dsn, slotName)
	}
	// Engine doesn't implement the slot-aware surface. Use the
	// default and emit a debug-level note so the operator can spot
	// the silent ignore via --log-level=debug if curious.
	slog.DebugContext(
		ctx, "engine does not implement CDCReaderWithSlotOpener; --slot-name silently ignored",
		slog.String("engine", source.Name()),
	)
	return source.OpenCDCReader(ctx, dsn)
}
