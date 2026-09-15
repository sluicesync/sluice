// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/engines/sqlite"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// SchemaReader is the local-file trigger engine's schema reader: the
// composed [sqlite.SchemaReader] (EMBEDDED, so every optional surface it
// carries — Close, the table/index/view preflights — is promoted through
// the same type assertions) plus the one CDC-position surface a
// trigger-CDC source owes the full-backup orchestrator,
// [SchemaReader.CaptureBackupPosition]. [D1SchemaReader] is the same
// shape over the `d1` reader for the d1-trigger engine; both answer
// through [unavailableBackupPosition].
type SchemaReader struct {
	*sqlite.SchemaReader

	b backend
}

// D1SchemaReader is the d1-trigger engine's schema reader: the composed
// [sqlite.D1SchemaReader] plus [D1SchemaReader.CaptureBackupPosition].
// See [SchemaReader].
type D1SchemaReader struct {
	*sqlite.D1SchemaReader

	b backend
}

// OpenD1SchemaReader opens the `d1` cold-start schema reader against the
// live D1 database and wraps it in [D1SchemaReader] (the `d1-trigger`
// analogue of [Engine.OpenSchemaReader]). Credentials + reachability are
// verified by the composed open, exactly as before.
func OpenD1SchemaReader(ctx context.Context, dsn string) (ir.SchemaReader, error) {
	b, err := d1Backend(dsn)
	if err != nil {
		return nil, err
	}
	sr, err := b.coldStart.OpenSchemaReader(ctx, dsn)
	if err != nil {
		return nil, err
	}
	d1sr, ok := sr.(*sqlite.D1SchemaReader)
	if !ok {
		// Cannot happen today — the d1 engine returns its own concrete type —
		// and if it ever changes, refusing here is the loud failure we want
		// rather than silently handing back a reader without the capturer.
		_ = closeReader(sr)
		return nil, fmt.Errorf("%s: OpenSchemaReader: the composed d1 engine returned a %T, not a *sqlite.D1SchemaReader; the trigger engine cannot attach its position surface", EngineNameD1, sr)
	}
	return &D1SchemaReader{D1SchemaReader: d1sr, b: b}, nil
}

// CaptureBackupPosition implements [irbackup.PositionCapturer]; see
// [unavailableBackupPosition] for why it never records one.
func (r *SchemaReader) CaptureBackupPosition(ctx context.Context, _ string) (ir.Position, error) {
	return unavailableBackupPosition(ctx, r.b)
}

// CaptureBackupPosition implements [irbackup.PositionCapturer]; see
// [unavailableBackupPosition] for why it never records one.
func (r *D1SchemaReader) CaptureBackupPosition(ctx context.Context, _ string) (ir.Position, error) {
	return unavailableBackupPosition(ctx, r.b)
}

// unavailableBackupPosition is the trigger engines' post-sweep capturer
// (roadmap item 163). It is reached by exactly one caller — the
// full-backup orchestrator's fallback, taken only when
// [openBackupSnapshot] refused — and on that path it always reports
// [irbackup.ErrPositionUnavailable]: the manifest records no
// EndPosition, the orchestrator says so at WARN, and `backup
// incremental` / `backup stream` refuse to root a chain on the full
// (POSITIONLESS-FULL-ROOT) rather than start "from now".
//
// Why unavailable rather than "the change log's current MAX(id)": the
// fallback sweep read its tables AFTER whatever the orchestrator opened
// in place of the snapshot, so an anchor read NOW sits above every write
// that landed during the sweep on a table already read — those rows are
// in neither the full nor an incremental resuming at the anchor. The
// gap-free anchor must be read BEFORE the sweep, which is what
// [openBackupSnapshot] does and this door cannot. Nor is there a
// legitimate configuration that reaches it: the snapshot open refuses
// only on a fault — a missing change log, a tampered id sequence, an
// unreadable source — each of which the operator fixes and re-runs. The
// one distinction the message draws is the missing change log (setup
// was never run), whose remedy differs.
//
// Pinned by TestCaptureBackupPosition_IsUnavailableOnTheFallbackDoor on
// both transports; the roster gate that requires the surface on every
// CDC engine's reader is docsync's TestEveryCDCEngineRecordsABackupPosition.
func unavailableBackupPosition(ctx context.Context, b backend) (ir.Position, error) {
	exec, err := b.openExec(ctx, true)
	if err != nil {
		return ir.Position{}, fmt.Errorf("%s: CaptureBackupPosition: open: %w", b.driver, err)
	}
	defer func() { _ = exec.close() }()
	exists, err := exec.changeLogExists(ctx)
	if err != nil {
		return ir.Position{}, fmt.Errorf("%s: CaptureBackupPosition: check change-log: %w", b.driver, err)
	}
	if !exists {
		return ir.Position{}, fmt.Errorf("%s: CaptureBackupPosition: change-log table %q does not exist on the source, so "+
			"there is no change-log anchor to record — run `sluice trigger setup --dsn=... --tables=... --source-driver=%s` "+
			"before the full, or take a standalone full with the cold-start driver: %w",
			b.driver, ChangeLogTable, b.driver, irbackup.ErrPositionUnavailable)
	}
	return ir.Position{}, fmt.Errorf("%s: CaptureBackupPosition: this door is reached only after the row sweep ran WITHOUT "+
		"the snapshot-anchored path (its open refused — see the WARN naming the cause), so no anchor read now can cover "+
		"the writes that landed during the sweep; fix the cause and re-run `backup full`: %w",
		b.driver, irbackup.ErrPositionUnavailable)
}
