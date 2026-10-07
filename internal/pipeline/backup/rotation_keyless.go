// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The write side of the Bug 297 door: never BUILD a rotated chain that no
// restore can apply whole. Its read side is rotated_keyless_door.go.
//
// Rotation (`backup stream --retain-rotate-at` / `--retain-rotate-at-chain-
// length`, ADR-0046) writes a new segment full; chain restore applies that
// full over the rows the earlier segments restored, which a table with no
// PRIMARY KEY and no NOT NULL UNIQUE index cannot absorb. Two doors, both
// over the SAME scope the rotation full reads ([Backup.readScopedSchema]
// with the stream's filter, which is none — a rotation full backs up every
// table the engine does not exclude by default):
//
//   - at `backup stream` start, when rotation is enabled
//     ([PreflightRotationKeyless]): the operator learns it before the chain
//     exists, and nothing is written;
//   - at each rotation ([Backup.RotationSegment]): a table that appeared, or
//     lost its key, after the stream started. The rotation aborts and the
//     stream STAYS on its open segment (the FSM's ordinary stay-open abort),
//     so the chain it keeps building is still restorable; the next rollover
//     re-judges, and rotation resumes once the table has a key.
//
// Judged on the recorded (source) schema alone, through the F-E1 predicate
// with no target ([migcore.FindReplayKeylessTables], no probe) — there is no
// target at backup time. This is deliberately broader than the restore door,
// which refuses only a later full carrying ROWS for the table: whether the
// table will hold rows at some future rotation is unknowable at start, and
// the cost of guessing wrong is a chain found unrestorable during a recovery.
// An empty keyless table therefore blocks rotation too; the remedy is a key
// or no rotation, since a rotation full has no table filter of its own.

// rotationKeylessHint is the remedy riding the write-side refusal.
const rotationKeylessHint = "give each named table a PRIMARY KEY or a NOT NULL UNIQUE index on the source, or run `backup stream run` " +
	"without --retain-rotate-at / --retain-rotate-at-chain-length (bound the chain with a new `backup full` into a new location instead)"

// readScopedSchema opens the source schema reader and reads the schema the
// backup will write, with b.Filter applied — the one read both [Backup.Run]
// and [PreflightRotationKeyless] use, so the door judges exactly the tables a
// rotation full would carry. The caller closes the returned reader.
func (b *Backup) readScopedSchema(ctx context.Context) (ir.SchemaReader, *ir.Schema, error) {
	sr, err := b.Source.OpenSchemaReader(ctx, b.SourceDSN)
	if err != nil {
		return nil, nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("backup: open source schema reader: %w", err))
	}

	// ADR-0047 tier (b): a PG-source backup may carry uncatalogued
	// extension types verbatim. The restore-target engine is unknown
	// at backup time, so this only enables CAPTURE — the PG-restore-
	// only constraint is enforced later by the recorded lineage marker
	// (lineage.VerbatimExtensionColumnsIn → lineage.Segment) + the loud
	// restore-time engine gate. A non-PG source never enables it.
	migcore.ApplyVerbatimExtensionPassthrough(sr, migcore.VerbatimBackupSourcePG(b.Source))

	// catalog Bug 76: scope per-column type validation to the filtered
	// table set (b.Filter already has engine defaults merged by the caller).
	migcore.ApplyTableScope(sr, b.Filter)

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		migcore.CloseIf(sr)
		return nil, nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("backup: read source schema: %w", err))
	}
	// migcore.ApplyTableFilter errors when the filter excludes everything.
	// For backups that's still a valid intent in some workflows
	// (e.g. "snapshot-time-only"), but matching the migrate shape
	// — surface the error so the operator notices.
	if err := migcore.ApplyTableFilter(ctx, schema, b.Filter); err != nil {
		migcore.CloseIf(sr)
		return nil, nil, err
	}
	return sr, schema, nil
}

// refuseRotationKeyless is the per-rotation door; a no-op for an ordinary
// `backup full`.
func (b *Backup) refuseRotationKeyless(ctx context.Context, schema *ir.Schema) error {
	if !b.RotationSegment {
		return nil
	}
	return errRotationKeyless(ctx, "backup stream: rotation", schema,
		"Refusing to write the next segment full; the stream stays on its open segment, which remains restorable")
}

// PreflightRotationKeyless is the `backup stream` start door: it reads the
// source schema exactly as a rotation-born segment full would (engine-default
// exclusions merged, no operator filter) and refuses when a table in it is
// keyless. Called only when rotation is enabled.
func PreflightRotationKeyless(ctx context.Context, source ir.Engine, sourceDSN string) error {
	b := &Backup{Source: source, SourceDSN: sourceDSN}
	b.Filter, _ = migcore.EffectiveTableFilter(b.Filter, source, sourceDSN)
	sr, schema, err := b.readScopedSchema(ctx)
	if err != nil {
		return fmt.Errorf("backup stream: rotation keyless check: %w", err)
	}
	migcore.CloseIf(sr)
	return errRotationKeyless(ctx, "backup stream", schema, "Nothing has been written")
}

// errRotationKeyless returns the coded refusal when schema carries a keyless
// table, nil otherwise. tail says what happens next.
func errRotationKeyless(ctx context.Context, mode string, schema *ir.Schema, tail string) error {
	if schema == nil {
		return nil
	}
	keyless, err := migcore.FindReplayKeylessTables(ctx, nil, schema.Tables, migcore.ReplayJudgeOptions{})
	if err != nil {
		return fmt.Errorf("%s: keyless check: %w", mode, err)
	}
	if len(keyless) == 0 {
		return nil
	}
	return sluicecode.Wrap(sluicecode.CodeBackupRotatedKeylessTable, rotationKeylessHint, fmt.Errorf(
		"%s: segment rotation (--retain-rotate-at / --retain-rotate-at-chain-length) is enabled, and %d table(s) "+
			"it would back up have no key: %s. Every rotation writes a new segment full, which chain restore applies over "+
			"the rows the earlier segments restored; a keyless table cannot absorb that, so the chain could never be "+
			"restored whole (Bug 297). %s",
		mode, len(keyless), migcore.RenderReplayKeylessTables(keyless), tail,
	))
}
