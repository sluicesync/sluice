// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Per-target persistence for resumable simple-mode migrations.
//
// This file mirrors control_table.go's shape — same idempotent CREATE
// TABLE, same ADD COLUMN IF NOT EXISTS migration story for older
// deployments — but holds different state for a different concept.
// `sluice_cdc_state` tracks long-running continuous-sync streams (one
// row per stream); `sluice_migrate_state` tracks one-shot simple-mode
// migrations (one header row per --migration-id, plus one
// `sluice_migrate_table_progress` row per table — ADR-0082). They are
// deliberately kept separate: streams and migrations have different
// lifetimes and different recovery semantics, and conflating them
// would make ad-hoc inspection of either harder.
//
// The CRUD control flow lives ONCE in internal/migratestate (the
// ADR-0081 tier-c precedent): this file owns only the dialect leaves
// — the ensure DDL + column migration, the schema-qualified
// identifier quoting, $n placeholders, and ON CONFLICT upsert syntax
// — and hands the finished statements to the shared skeleton.
//
// The store is wired into the Postgres engine via
// [Engine.OpenMigrationStateStore], which the pipeline.Migrator
// type-asserts at startup. See internal/ir/interfaces.go for the
// MigrationStateStore contract.

package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/migratestate"
)

// migrateStateTableName is the per-target header table; one row per
// --migration-id. Parallel to controlTableName; the two deliberately
// don't share a row model.
const migrateStateTableName = migratestate.HeaderTableName

// migrateProgressTableName is the per-target per-table progress
// table; one row per (migration_id, table_name) — ADR-0082. Excluded
// from user-schema reads in schema_reader.go alongside the other
// sluice_* bookkeeping tables.
const migrateProgressTableName = migratestate.ProgressTableName

// MigrationStateStore is the Postgres implementation of
// [ir.MigrationStateStore]. One value per target connection pool;
// safe to call concurrently from a single Migrator.Run (the
// cross-table pool's per-table checkpoint writers hit distinct
// progress rows).
type MigrationStateStore struct {
	db     *sql.DB
	schema string
	shared *migratestate.Store
}

// newMigrationStateStore builds the store plus its dialect SQL. The
// statement set and the argument-order contract per statement are
// documented on [migratestate.SQL].
func newMigrationStateStore(db *sql.DB, schema string) *MigrationStateStore {
	hdr := quoteIdent(schema) + "." + quoteIdent(migrateStateTableName)
	prog := quoteIdent(schema) + "." + quoteIdent(migrateProgressTableName)
	return &MigrationStateStore{
		db:     db,
		schema: schema,
		shared: &migratestate.Store{
			DB: db,
			Config: migratestate.Config{
				EngineName:     "postgres",
				IsMissingTable: isUndefinedRelationErr,
			},
			SQL: migratestate.SQL{
				ReadHeader: "SELECT phase, table_progress, state_format, started_at, updated_at, last_error FROM " +
					hdr + " WHERE migration_id = $1",
				ReadProgressRows: "SELECT table_name, progress, updated_at FROM " +
					prog + " WHERE migration_id = $1",
				// started_at: set from the column default on first
				// insert, preserved on conflict by simply not being in
				// the SET list — the "set once" semantics resume runs
				// rely on.
				//
				// UTC contract (audit 2026-07-16 M1.3, the task-#44
				// lease-TZ class): started_at/updated_at are naive
				// TIMESTAMP columns and pgx reads a naive timestamp back
				// as UTC digits. CURRENT_TIMESTAMP casts through the
				// SESSION TimeZone, so on a server set to, say,
				// America/Los_Angeles the stored digits were 7h behind
				// what the reader assumed — the backfill concurrent-run
				// heartbeat guard (pipeline.backfill, 5m freshness on
				// UpdatedAt) then missed live runs on TZ-behind servers
				// and over-refused for hours on TZ-ahead ones. Writing
				// timezone('utc', now()) — naive UTC digits — makes the
				// stored value exactly what the read path assumes.
				UpsertHeader: "INSERT INTO " + hdr + " " +
					"(migration_id, phase, table_progress, state_format, started_at, updated_at, last_error) " +
					"VALUES ($1, $2, $3, $4, timezone('utc', now()), timezone('utc', now()), $5) " +
					"ON CONFLICT (migration_id) DO UPDATE SET " +
					"phase = EXCLUDED.phase, " +
					"table_progress = EXCLUDED.table_progress, " +
					"state_format = EXCLUDED.state_format, " +
					"updated_at = timezone('utc', now()), " +
					"last_error = EXCLUDED.last_error",
				UpsertProgressRow: "INSERT INTO " + prog + " " +
					"(migration_id, table_name, progress, updated_at) " +
					"VALUES ($1, $2, $3, timezone('utc', now())) " +
					"ON CONFLICT (migration_id, table_name) DO UPDATE SET " +
					"progress = EXCLUDED.progress, " +
					"updated_at = timezone('utc', now())",
				MarkUpgraded: "UPDATE " + hdr +
					" SET table_progress = $1, state_format = $2 WHERE migration_id = $3",
				DeleteHeader:       "DELETE FROM " + hdr + " WHERE migration_id = $1",
				DeleteProgressRows: "DELETE FROM " + prog + " WHERE migration_id = $1",
			},
		},
	}
}

// Close releases the underlying connection pool.
func (s *MigrationStateStore) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// EnsureControlTable creates the per-target migrate-state tables in
// the configured schema if they don't exist, and adds the ADR-0082
// state_format column to a header table created by a ≤v0.99.x
// binary. Idempotent — safe to call on every start.
//
// state_format DEFAULT 1: an existing row that pre-dates the column
// reads back as FormatLegacyBlob, which is exactly what it is — Read
// detects it and the first write upgrades it to per-table progress
// rows.
func (s *MigrationStateStore) EnsureControlTable(ctx context.Context) error {
	hdr := quoteIdent(s.schema) + "." + quoteIdent(migrateStateTableName)
	prog := quoteIdent(s.schema) + "." + quoteIdent(migrateProgressTableName)
	// Timestamp defaults use timezone('utc', now()) — naive UTC digits —
	// not CURRENT_TIMESTAMP, whose session-TZ cast skews the read-back
	// (M1.3; see the UpsertHeader comment). The upserts always supply
	// both columns explicitly, so the defaults only matter for rows
	// written by hand or by future statements that omit them.
	hdrDDL := `
		CREATE TABLE IF NOT EXISTS ` + hdr + ` (
			migration_id    VARCHAR(255) NOT NULL,
			phase           VARCHAR(32)  NOT NULL,
			table_progress  TEXT         NULL,
			state_format    INT          NOT NULL DEFAULT 1,
			started_at      TIMESTAMP    NOT NULL DEFAULT (timezone('utc', now())),
			updated_at      TIMESTAMP    NOT NULL DEFAULT (timezone('utc', now())),
			last_error      TEXT         NULL,
			PRIMARY KEY (migration_id)
		)`
	if _, err := s.db.ExecContext(ctx, hdrDDL); err != nil {
		return fmt.Errorf("postgres: ensure migrate-state table: %w", err)
	}
	addFormat := "ALTER TABLE " + hdr +
		" ADD COLUMN IF NOT EXISTS state_format INT NOT NULL DEFAULT 1"
	if _, err := s.db.ExecContext(ctx, addFormat); err != nil {
		return fmt.Errorf("postgres: ensure migrate-state table: add state_format: %w", err)
	}
	progDDL := `
		CREATE TABLE IF NOT EXISTS ` + prog + ` (
			migration_id    VARCHAR(255) NOT NULL,
			table_name      VARCHAR(255) NOT NULL,
			progress        TEXT         NOT NULL,
			updated_at      TIMESTAMP    NOT NULL DEFAULT (timezone('utc', now())),
			PRIMARY KEY (migration_id, table_name)
		)`
	if _, err := s.db.ExecContext(ctx, progDDL); err != nil {
		return fmt.Errorf("postgres: ensure migrate-state progress table: %w", err)
	}
	return nil
}

// Read returns the merged header + per-table state for migrationID,
// or ok=false when no row exists. Tolerant of the header table being
// absent (treated as "no row") so dry-run / pre-EnsureControlTable
// inspection paths don't error. A legacy single-blob row is detected
// here and upgraded to per-table progress rows on the first write
// (one transaction; Read itself never writes — see
// internal/migratestate).
func (s *MigrationStateStore) Read(ctx context.Context, migrationID string) (ir.MigrationState, bool, error) {
	return s.shared.Read(ctx, migrationID)
}

// Write upserts the header row plus any per-table entries present in
// state.TableProgress (never deleting absent ones). The hot
// per-checkpoint path is [MigrationStateStore.WriteTableProgress].
func (s *MigrationStateStore) Write(ctx context.Context, state ir.MigrationState) error {
	return s.shared.Write(ctx, state)
}

// WriteTableProgress upserts one table's progress row — the O(1)
// per-checkpoint write (ADR-0082).
func (s *MigrationStateStore) WriteTableProgress(ctx context.Context, migrationID, tableName string, progress ir.TableProgress) error {
	return s.shared.WriteTableProgress(ctx, migrationID, tableName, progress)
}

// ClearMigration deletes the progress rows and header row for
// migrationID. Used by the `--reset-target-data` recovery path
// (ADR-0023). Idempotent and tolerant of missing rows or missing
// tables — the next run with `--reset-target-data` proceeds cleanly
// either way.
func (s *MigrationStateStore) ClearMigration(ctx context.Context, migrationID string) error {
	return s.shared.ClearMigration(ctx, migrationID)
}
