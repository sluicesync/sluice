// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
)

// PositionFromManifestPreflight is the optional engine-side surface
// for the Phase 3.3.C pre-flight checks fired before
// `sluice sync start --position-from-manifest` opens CDC. PG
// implements it on the engine's [SchemaReader] (parallel to
// [HealthReporter] / [BackupPositionCapturer]); engines without
// operator-attention surfaces simply omit the method.
//
// The contract: implementations inspect the source's slot/WAL state
// against the supplied chainTerminal position and the slotName the
// CDC reader will use, and return a [PreflightReport] capturing soft
// warnings + an optional refusal. The streamer surfaces refusals as
// run-aborting errors; warnings turn into refusals when
// `--strict-preflight` is set.
//
// Lives in the ir package (not pipeline) so engine packages can
// reference it without forming an import cycle through pipeline's
// integration tests.
type PositionFromManifestPreflight interface {
	PreflightPositionFromManifest(
		ctx context.Context,
		chainTerminal ir.Position,
		slotName string,
	) (PreflightReport, error)
}

// PreflightReport bundles the result of a Phase 3.3.C pre-flight
// against the source. Warnings are operator-actionable advisories
// that don't block the run by default; Refusal is a fatal condition
// the operator must address before the run can proceed (slot lost,
// slot missing, WAL gap exceeds keep-size).
type PreflightReport struct {
	// Warnings is the slice of soft-warning messages emitted by the
	// preflight. Each is a single-sentence operator-facing string;
	// the streamer logs them via slog.WarnContext and (when
	// StrictPreflight is true) escalates to a refusal.
	Warnings []string

	// Refusal is non-empty when the preflight encountered a fatal
	// condition. The streamer surfaces it as a wrapped run error.
	// Empty means "no refusal" — warnings only.
	Refusal string
}

// BackupPositionCapturer is the optional engine surface for capturing
// the source's current CDC position from a one-shot query. Used by
// the full-backup orchestrator as a v0.18.0 fallback when the engine
// does NOT implement [BackupSnapshotOpener] — the snapshot-anchored
// path is the preferred shape because it closes the during-backup
// write-window gap that this fallback path leaves open.
//
// The captured position is the source-side cursor at the moment of
// capture. With the v0.17.x fallback shape, the full backup calls
// this at the end of the per-table row sweep so the recorded
// EndPosition reflects "the source has produced everything up to
// here at the moment the backup completes." Writes that landed on
// already-read tables during the backup window are read by neither
// the row sweep (no shared snapshot) nor the first incremental's
// `--since=<full>.EndPosition` window (those LSNs are before the
// captured EndPosition) — the v0.17.2 release notes called this out
// as a known caveat with the workaround "pair backups with
// continuous `sluice sync start`."
//
// In v0.18.0 the gap is closed via [BackupSnapshotOpener]: engines
// that implement it capture EndPosition at snapshot START (the
// source position at which a cross-table consistent read view is
// pinned) and the orchestrator never calls CaptureBackupPosition.
// Engines that DON'T implement BackupSnapshotOpener fall through to
// this surface with a WARN log line so operators know the chain
// rooted in this full will carry the v0.17.x during-backup write-
// window gap.
//
// Engines wire this on their [SchemaReader] (parallel to
// [HealthReporter]) so the full-backup orchestrator can type-assert
// on a value it already opens. The captured position's encoding is
// engine-specific:
//
//   - Postgres: a JSON-envelope `{slot,lsn}` shape using the slot
//     name supplied via slotName (or the engine's default when empty)
//     plus `pg_current_wal_lsn()`. The slot need not exist at capture
//     time — Phase 3.3's `--position-from-manifest` pre-flights the
//     slot state before resuming CDC from the recorded LSN.
//   - MySQL: a binlog-mode position recording `@@global.gtid_executed`
//     when GTID mode is on, or a `(file, pos)` pair otherwise.
//
// Engines without CDC support don't implement this surface; the
// orchestrator type-asserts and falls back to "no EndPosition recorded"
// (matches the v0.16.x shape; first incremental against such a manifest
// surfaces a clear "parent has no EndPosition; chain will start from
// CDC's current position" warning).
type BackupPositionCapturer interface {
	// CaptureBackupPosition returns the source's current CDC position.
	// The slotName argument is honoured by engines with a slot concept
	// (Postgres) and ignored by others (MySQL); empty falls back to the
	// engine's default. The returned position is suitable for storage
	// in [Manifest.EndPosition] and as a [Position] argument to the
	// engine's CDC reader.
	CaptureBackupPosition(ctx context.Context, slotName string) (ir.Position, error)
}
