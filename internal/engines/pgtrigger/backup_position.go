// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/engines/postgres"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// SchemaReader is the trigger engine's schema reader: the composed
// [postgres.SchemaReader] — the schema surface is byte-equivalent to
// vanilla PG, and EMBEDDING (not delegation, unlike the Engine) keeps
// every optional surface that reader carries reachable through the
// same type assertions: the migrate preflight probers, the
// health/heartbeat/lag reporters, the extension probe. Only the two
// surfaces whose answer is about the CDC POSITION are re-pointed at the
// change log, because on this engine a position is a change-log id and
// not a WAL LSN:
//
//   - [SchemaReader.CaptureBackupPosition] — the full-backup fallback
//     door (roadmap item 163).
//   - [SchemaReader.PreflightPositionFromManifest] — the `sync start
//     --position-from-manifest` preflight, which on the composed reader
//     inspects a replication slot this engine never has.
//
// Before item 163 [Engine.OpenSchemaReader] handed back the composed
// reader as-is, so a trigger full recorded a pgoutput {slot, lsn} under
// the "postgres" tag — a position the trigger poller refuses as foreign
// at the first `backup incremental`.
type SchemaReader struct {
	*postgres.SchemaReader

	// dsn is the trigger-native DSN (the `schema` parameter stripped, as
	// [parseDSNCompat] renders it) for the pool the capturer opens; schema
	// is the change log's namespace; appID the connection label.
	dsn    string
	schema string
	appID  string
}

// CaptureBackupPosition implements [irbackup.PositionCapturer]. On this
// engine it is reached by exactly one caller — the full-backup
// orchestrator's POST-SWEEP fallback, taken only when
// [Engine.OpenBackupSnapshot] refused — and on that path it always
// reports [irbackup.ErrPositionUnavailable]: the manifest records no
// EndPosition, the orchestrator says so at WARN, and `backup
// incremental` / `backup stream` refuse to root a chain on the full
// (POSITIONLESS-FULL-ROOT) rather than start "from now".
//
// Why unavailable rather than "the change log's current MAX(id)": the
// fallback sweep shared no snapshot with the change log, so any anchor
// read NOW sits above every row a source write landed during the sweep
// on a table already read — those rows are in neither the full nor an
// incremental resuming at the anchor. That is the v0.17.x during-backup
// gap the snapshot-anchored path exists to close, and recording a
// position that claims to cover it would be the silent half. Nor is
// there a legitimate configuration that reaches this door: the
// snapshot open has no `wal_level` dependency (unlike vanilla PG's) and
// refuses only on a fault — a missing change log, a tampered id
// sequence, a wedged pre-snapshot transaction, a lost connection — each
// of which the operator fixes and re-runs. The one distinction the
// message draws is the missing change log (setup was never run), whose
// remedy differs.
//
// Pinned by TestCaptureBackupPosition_IsUnavailableOnTheFallbackDoor;
// the roster gate that requires this surface to exist on every CDC
// engine's reader is docsync's TestEveryCDCEngineRecordsABackupPosition.
func (r *SchemaReader) CaptureBackupPosition(ctx context.Context, _ string) (ir.Position, error) {
	db, err := postgres.OpenPgxDB(r.dsn, r.appID)
	if err != nil {
		return ir.Position{}, fmt.Errorf("pgtrigger: CaptureBackupPosition: open: %w", err)
	}
	defer func() { _ = db.Close() }()
	exists, err := changeLogTableExists(ctx, db, r.schema)
	if err != nil {
		return ir.Position{}, fmt.Errorf("pgtrigger: CaptureBackupPosition: check change-log: %w", err)
	}
	if !exists {
		return ir.Position{}, fmt.Errorf("pgtrigger: CaptureBackupPosition: %s.%s does not exist on the source, so there is "+
			"no change-log anchor to record — run `sluice trigger setup --dsn=... --tables=...` before the full, or take "+
			"a standalone full with --source-driver postgres: %w", r.schema, ChangeLogTable, irbackup.ErrPositionUnavailable)
	}
	return ir.Position{}, fmt.Errorf("pgtrigger: CaptureBackupPosition: this door is reached only after the row sweep ran "+
		"WITHOUT the snapshot-anchored path (its open refused — see the WARN naming the cause), so no anchor read now can "+
		"cover the writes that landed during the sweep; fix the cause and re-run `backup full`: %w",
		irbackup.ErrPositionUnavailable)
}

// PreflightPositionFromManifest implements
// [irbackup.PositionFromManifestPreflight] for a trigger-CDC chain
// terminal. The composed reader's preflight inspects the replication
// slot a pgoutput chain resumes on (existence, wal_status,
// wal_keep_size, Patroni); a trigger chain resumes on a change-log id
// and has no slot, so inheriting that preflight would REFUSE every
// `sync start --position-from-manifest` off a trigger chain for a
// missing `sluice_slot`. The source-side doors that do apply — the
// change log's presence, its sequence configuration, the capture
// trigger shape — run at the poller's own open (openCDCReader) with no
// position needed.
//
// What this still grades: the terminal position must be one THIS engine
// can decode. A pgoutput {slot, lsn} from a vanilla-postgres chain
// pointed at this engine is refused here, before the stream opens,
// rather than at the poller's decode.
func (r *SchemaReader) PreflightPositionFromManifest(_ context.Context, chainTerminal ir.Position, _ string) (irbackup.PreflightReport, error) {
	if _, _, err := decodePos(chainTerminal); err != nil {
		refusal := fmt.Sprintf("the chain's terminal position is not a trigger-CDC position (%v); a chain captured "+
			"from a vanilla postgres source cannot be resumed through the postgres-trigger engine", err)
		return irbackup.PreflightReport{Refusal: refusal}, nil
	}
	return irbackup.PreflightReport{}, nil
}
