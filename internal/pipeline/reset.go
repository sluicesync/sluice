// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Destructive recovery: --reset-target-data.
//
// When the operator opts into the flag, the pipeline clears the
// bookkeeping row first (DELETE FROM sluice_*_state WHERE id = ?)
// and then iterates the source-side schema (post-filter) issuing
// DROP TABLE IF EXISTS on each table via the engine's optional
// [ir.TableDropper] surface. Bookkeeping-row clearing happens before
// the drop loop so a mid-flight failure leaves the next retry on a
// clean idempotent path; the drops are themselves idempotent via IF
// EXISTS. See ADR-0023 for the full design.
//
// Engine-neutrality: this file imports only [ir]; engine packages
// expose [ir.TableDropper], [ir.StreamCleaner], the optional
// [ir.SchemaTypeDropper] for engines whose enum/UDTs survive table
// drops (PG; MySQL doesn't need it), and the existing
// [ir.MigrationStateStore.ClearMigration] surface. Engines that
// don't opt in cause [resetTargetData] to surface a clear "engine
// does not support --reset-target-data" error before any work runs.
//
// Schema-readers exclude `sluice_*_state` tables already (see
// ADR-0015), so the drop loop never targets the bookkeeping tables;
// the bookkeeping row is cleared via the engine-specific DELETE
// surface instead.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// resetTargetData clears the migration-state row for migrationID,
// then drops every table in the post-filter schema on the target.
// Used by [Migrator] when --reset-target-data is set. The drop loop
// uses the engine's [ir.TableDropper] surface; an engine that doesn't
// implement it errors out clearly before any work runs.
//
// Idempotent across retries thanks to DROP TABLE IF EXISTS plus the
// store's tolerant-of-missing-row semantics. Each successful drop
// emits an INFO log line so the operator has an audit trail of what
// went away.
func resetTargetData(ctx context.Context, schema *ir.Schema, rw ir.RowWriter, store ir.MigrationStateStore, migrationID string) error {
	dropper, ok := rw.(ir.TableDropper)
	if !ok {
		return errors.New("pipeline: --reset-target-data: target engine's row writer does not support DROP TABLE; drop dest tables manually before re-running")
	}
	if store != nil && migrationID != "" {
		if err := store.ClearMigration(ctx, migrationID); err != nil {
			return fmt.Errorf("pipeline: --reset-target-data: clear migrate-state row: %w", err)
		}
		slog.InfoContext(
			ctx, "reset: cleared migrate-state row",
			slog.String("migration_id", migrationID),
		)
	}
	if err := dropTables(ctx, dropper, schema.Tables); err != nil {
		return err
	}
	if err := dropSchemaTypes(ctx, rw, schema); err != nil {
		return err
	}
	slog.InfoContext(
		ctx, "reset: target data wiped; proceeding with cold-start",
		slog.Int("tables_dropped", len(schema.Tables)),
	)
	return nil
}

// resetTargetDataForStream is the Streamer's analogue of
// [resetTargetData]. The bookkeeping surface is the [ir.StreamCleaner]
// on the [ir.ChangeApplier] (sluice_cdc_state) rather than the
// migration-state store. Same idempotency contract.
func resetTargetDataForStream(ctx context.Context, schema *ir.Schema, rw ir.RowWriter, applier ir.ChangeApplier, streamID string) error {
	dropper, ok := rw.(ir.TableDropper)
	if !ok {
		return errors.New("pipeline: --reset-target-data: target engine's row writer does not support DROP TABLE; drop dest tables manually before re-running")
	}
	cleaner, ok := applier.(ir.StreamCleaner)
	if !ok {
		return errors.New("pipeline: --reset-target-data: target engine's change applier does not support clearing the cdc-state row; drop the row manually before re-running")
	}
	if streamID != "" {
		if err := cleaner.ClearStream(ctx, streamID); err != nil {
			return fmt.Errorf("pipeline: --reset-target-data: clear cdc-state row: %w", err)
		}
		slog.InfoContext(
			ctx, "reset: cleared cdc-state row",
			slog.String("stream_id", streamID),
		)
	}
	if err := dropTables(ctx, dropper, schema.Tables); err != nil {
		return err
	}
	if err := dropSchemaTypes(ctx, rw, schema); err != nil {
		return err
	}
	slog.InfoContext(
		ctx, "reset: target data wiped; proceeding with cold-start",
		slog.Int("tables_dropped", len(schema.Tables)),
	)
	return nil
}

// copyReaderIsIdempotent reports whether a snapshot reader's COPY-phase
// row stream is idempotent — i.e. the cold-start bulk copy routes through
// the engine's UPSERT writer (ON DUPLICATE KEY UPDATE / ON CONFLICT DO
// UPDATE) rather than plain INSERT. This MUST mirror the discriminator
// in [runBulkCopyWithOpts] exactly: a reader is idempotent iff it
// implements [ir.IdempotentCopyReader] and reports true. The VStream /
// PlanetScale snapshot reader does (it re-emits COPY rows during binlog
// catchup, Bug 125); the native MySQL binlog snapshot and the Postgres
// snapshot do NOT — their reads are gap-free and overlap-free, so they
// take the faster plain-INSERT path.
//
// The distinction matters for the restart-from-scratch / auto-resnapshot
// recovery (ADR-0093): a fresh cold-start onto a NON-idempotent reader
// runs plain INSERT, which dup-key-collides (MySQL Error 1062) on a
// target that still holds rows from the prior copy. The idempotent path
// genuinely absorbs the overlap; the plain-INSERT path does not, so the
// caller must clean the target first.
func copyReaderIsIdempotent(rows ir.RowReader) bool {
	icr, ok := rows.(ir.IdempotentCopyReader)
	return ok && icr.CopyNeedsIdempotentWriter()
}

// resetTargetTablesForRestart drops the in-scope target tables (and any
// schema-defined types) ahead of a from-scratch cold-start whose reader
// is NON-idempotent. It reuses the same FK-safe drop machinery as
// [resetTargetDataForStream] (PG CASCADE / MySQL InnoDB referential
// drops) so a re-copy onto a populated, already-FK-constrained target can
// recreate empty tables via CreateTablesWithoutConstraints. Unlike the
// full --reset-target-data path it does NOT clear the cdc-state row —
// the restart-from-scratch / auto-resnapshot dispatch already discards
// the persisted position, and the cold-start re-stamps a fresh one at the
// CDC handoff, so clearing it here would be redundant.
//
// Idempotent across retries via DROP TABLE IF EXISTS. An engine without
// the [ir.TableDropper] surface surfaces a clear, accurate refusal rather
// than silently proceeding into the dup-key trap.
func resetTargetTablesForRestart(ctx context.Context, schema *ir.Schema, rw ir.RowWriter) error {
	dropper, ok := rw.(ir.TableDropper)
	if !ok {
		return errors.New(
			"pipeline: restart-from-scratch onto a non-idempotent source (native MySQL binlog) needs an empty target, " +
				"but this target engine's row writer does not support DROP TABLE — drop the dest tables manually, " +
				"or re-run with --reset-target-data, before retrying",
		)
	}
	if err := dropTables(ctx, dropper, schema.Tables); err != nil {
		return err
	}
	if err := dropSchemaTypes(ctx, rw, schema); err != nil {
		return err
	}
	slog.InfoContext(
		ctx, "restart-from-scratch: non-idempotent source — dropped target tables before the fresh cold-start (plain INSERT would otherwise dup-key on the leftover rows)",
		slog.Int("tables_dropped", len(schema.Tables)),
	)
	return nil
}

// dropSchemaTypes drops user-defined database-level types the source
// IR schema would create on a cold-start (e.g. PG enum types). Probes
// the row writer for the optional [ir.SchemaTypeDropper] surface; a
// no-op when the engine doesn't expose it (MySQL embeds enum values
// inline on the column, so there are no orphan types to clean up).
//
// Must run AFTER the table drops — columns can reference the types,
// so dropping the type first either errors or requires more aggressive
// CASCADE on the type drop itself. Tables-first keeps the dependency
// order natural. Fixes Bug 18 where partial cold-start failures left
// enum types orphaned on the target.
func dropSchemaTypes(ctx context.Context, rw ir.RowWriter, schema *ir.Schema) error {
	typeDropper, ok := rw.(ir.SchemaTypeDropper)
	if !ok {
		return nil
	}
	if err := typeDropper.DropSchemaTypes(ctx, schema); err != nil {
		return fmt.Errorf("pipeline: --reset-target-data: drop schema types: %w", err)
	}
	slog.InfoContext(ctx, "reset: dropped schema-defined types")
	return nil
}

// dropTables removes every named table on the target. Probes the
// dropper for the optional [ir.BulkTableDropper] surface; engines
// that implement it pay one round-trip for the whole batch (notable
// on databases with hundreds of sluice-managed tables). Engines
// without it fall back to per-table DropTable. Either way, an INFO
// audit line is emitted per table so the operator's recovery log
// names exactly what went away.
func dropTables(ctx context.Context, dropper ir.TableDropper, tables []*ir.Table) error {
	if len(tables) == 0 {
		return nil
	}
	if bulk, ok := dropper.(ir.BulkTableDropper); ok {
		// Log per-table INTENT before the bulk DROP so the audit
		// trail is intact even when the SQL fails partway. The
		// statement itself is atomic on PG/MySQL — partial successes
		// don't surface — but a network-level retry could lose the
		// summary line.
		for _, table := range tables {
			slog.InfoContext(
				ctx, "reset: dropping target table",
				slog.String("table", table.Name),
			)
		}
		if err := bulk.DropTables(ctx, tables); err != nil {
			return fmt.Errorf("pipeline: --reset-target-data: bulk drop %d tables: %w", len(tables), err)
		}
		return nil
	}
	for _, table := range tables {
		if err := dropper.DropTable(ctx, table); err != nil {
			return fmt.Errorf("pipeline: --reset-target-data: drop %q: %w", table.Name, err)
		}
		slog.InfoContext(
			ctx, "reset: dropped target table",
			slog.String("table", table.Name),
		)
	}
	return nil
}
