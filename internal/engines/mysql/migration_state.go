// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Per-target persistence for resumable simple-mode migrations.
//
// This file mirrors control_table.go's shape — same idempotent CREATE
// TABLE, same detect-then-ALTER migration story for older deployments
// — but holds different state for a different concept.
// `sluice_cdc_state` tracks long-running continuous-sync streams (one
// row per stream); `sluice_migrate_state` tracks one-shot simple-mode
// migrations (one header row per --migration-id, plus one
// `sluice_migrate_table_progress` row per table — ADR-0082). They are
// deliberately kept separate: streams and migrations have different
// lifetimes and different recovery semantics, and conflating them
// would make ad-hoc inspection of either harder.
//
// MySQL has a flat namespace (no schema-qualification needed beyond
// the connection's default database), so the tables live unqualified.
//
// The CRUD control flow lives ONCE in internal/migratestate (the
// ADR-0081 tier-c precedent): this file owns only the dialect leaves
// — the ensure DDL + detect-then-ALTER column migration, backtick
// quoting, ? placeholders, and the row-alias ON DUPLICATE KEY upsert
// syntax — and hands the finished statements to the shared skeleton.
//
// The store is wired into the MySQL engine via
// [Engine.OpenMigrationStateStore], which the pipeline.Migrator
// type-asserts at startup. See internal/ir/interfaces.go for the
// MigrationStateStore contract.

package mysql

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

// MigrationStateStore is the MySQL implementation of
// [ir.MigrationStateStore]. One value per target connection pool.
type MigrationStateStore struct {
	db     *sql.DB
	shared *migratestate.Store
}

// newMigrationStateStore builds the store plus its dialect SQL. The
// statement set and the argument-order contract per statement are
// documented on [migratestate.SQL]. Upserts use the row-alias form
// (8.0.20+) — same form the data-write path uses for cross-applier
// consistency.
func newMigrationStateStore(db *sql.DB) *MigrationStateStore {
	const hdr = "`" + migrateStateTableName + "`"
	const prog = "`" + migrateProgressTableName + "`"
	return &MigrationStateStore{
		db: db,
		shared: &migratestate.Store{
			DB: db,
			Config: migratestate.Config{
				EngineName:     "mysql",
				IsMissingTable: isMySQLMissingTableErr,
			},
			SQL: migratestate.SQL{
				ReadHeader: "SELECT phase, table_progress, state_format, started_at, updated_at, last_error FROM " +
					hdr + " WHERE migration_id = ?",
				ReadProgressRows: "SELECT table_name, progress, updated_at FROM " +
					prog + " WHERE migration_id = ?",
				// started_at is deliberately excluded from the SET list
				// so its DEFAULT CURRENT_TIMESTAMP on the original
				// INSERT survives subsequent upserts; updated_at
				// refreshes via the column's ON UPDATE clause.
				UpsertHeader: "INSERT INTO " + hdr + " " +
					"(migration_id, phase, table_progress, state_format, last_error) " +
					"VALUES (?, ?, ?, ?, ?) AS new " +
					"ON DUPLICATE KEY UPDATE " +
					"phase = new.phase, " +
					"table_progress = new.table_progress, " +
					"state_format = new.state_format, " +
					"last_error = new.last_error",
				UpsertProgressRow: "INSERT INTO " + prog + " " +
					"(migration_id, table_name, progress) " +
					"VALUES (?, ?, ?) AS new " +
					"ON DUPLICATE KEY UPDATE progress = new.progress",
				MarkUpgraded: "UPDATE " + hdr +
					" SET table_progress = ?, state_format = ? WHERE migration_id = ?",
				DeleteHeader:       "DELETE FROM " + hdr + " WHERE migration_id = ?",
				DeleteProgressRows: "DELETE FROM " + prog + " WHERE migration_id = ?",
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

// EnsureControlTable creates the per-target migrate-state tables if
// they don't exist, and adds the ADR-0082 state_format column to a
// header table created by a ≤v0.99.x binary. Idempotent — safe to
// call on every start.
//
// MySQL doesn't allow CREATE TABLE inside a multi-statement tx; the
// caller runs this from the *sql.DB pool at Migrator startup, not
// inside a per-write tx (the single-row write paths use the same
// pool).
//
// The column migration uses detect-then-ALTER (same shape as
// ensureCrossEngineParityColumn) rather than ADD COLUMN IF NOT
// EXISTS, which would impose an 8.0.29 floor. state_format DEFAULT 1:
// an existing row that pre-dates the column reads back as
// FormatLegacyBlob, which is exactly what it is — Read detects it and
// the first write upgrades it to per-table progress rows.
func (s *MigrationStateStore) EnsureControlTable(ctx context.Context) error {
	// Detect-then-create, NOT bare CREATE TABLE IF NOT EXISTS: Vitess
	// under PlanetScale safe migrations refuses every direct DDL
	// STATEMENT (Error 1105 "direct DDL is disabled") regardless of
	// whether the table exists, so the exists-already path must issue
	// no DDL at all. On a safe-migrations production branch the tables
	// arrive via the expand deploy request (expand-contract stages them
	// on the dev branch); this detect gate is what lets the backfill
	// then open the store there without tripping the DDL block
	// (live-caught 2026-07-15).
	hdrExists, err := s.controlTableExists(ctx, migrateStateTableName)
	if err != nil {
		return err
	}
	if !hdrExists {
		const hdrDDL = `
			CREATE TABLE IF NOT EXISTS ` + "`" + migrateStateTableName + "`" + ` (
				migration_id    VARCHAR(255) NOT NULL,
				phase           VARCHAR(32)  NOT NULL,
				table_progress  TEXT         NULL,
				state_format    INT          NOT NULL DEFAULT 1,
				started_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
				updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
					ON UPDATE CURRENT_TIMESTAMP,
				last_error      TEXT         NULL,
				PRIMARY KEY (migration_id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
		if _, err := s.db.ExecContext(ctx, hdrDDL); err != nil {
			return fmt.Errorf("mysql: ensure migrate-state table: %w", err)
		}
	}
	if err := s.ensureStateFormatColumn(ctx); err != nil {
		return err
	}
	progExists, err := s.controlTableExists(ctx, migrateProgressTableName)
	if err != nil {
		return err
	}
	if !progExists {
		const progDDL = `
			CREATE TABLE IF NOT EXISTS ` + "`" + migrateProgressTableName + "`" + ` (
				migration_id    VARCHAR(255) NOT NULL,
				table_name      VARCHAR(255) NOT NULL,
				progress        TEXT         NOT NULL,
				updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
					ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (migration_id, table_name)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
		if _, err := s.db.ExecContext(ctx, progDDL); err != nil {
			return fmt.Errorf("mysql: ensure migrate-state progress table: %w", err)
		}
	}
	return nil
}

// controlTableExists reports whether a migrate-state control table is
// already present in the connected schema, so EnsureControlTable can
// skip the CREATE statement entirely (see the safe-migrations note
// there).
func (s *MigrationStateStore) controlTableExists(ctx context.Context, table string) (bool, error) {
	const q = `
		SELECT COUNT(*)
		FROM   information_schema.TABLES
		WHERE  TABLE_SCHEMA = DATABASE()
		  AND  TABLE_NAME   = ?`
	var n int
	if err := s.db.QueryRowContext(ctx, q, table).Scan(&n); err != nil {
		return false, fmt.Errorf("mysql: ensure migrate-state table: detect %s: %w", table, err)
	}
	return n > 0, nil
}

// ensureStateFormatColumn adds the ADR-0082 state_format column to a
// header table created by a ≤v0.99.x binary. Detect-then-ALTER keeps
// the migration portable to MySQL 8.0.x versions older than 8.0.29
// (no ADD COLUMN IF NOT EXISTS), mirroring control_table.go's column
// migrations.
func (s *MigrationStateStore) ensureStateFormatColumn(ctx context.Context) error {
	const checkQ = `
		SELECT COUNT(*)
		FROM   information_schema.COLUMNS
		WHERE  TABLE_SCHEMA = DATABASE()
		  AND  TABLE_NAME   = ?
		  AND  COLUMN_NAME  = 'state_format'`
	var n int
	if err := s.db.QueryRowContext(ctx, checkQ, migrateStateTableName).Scan(&n); err != nil {
		return fmt.Errorf("mysql: ensure migrate-state table: detect state_format: %w", err)
	}
	if n > 0 {
		return nil
	}
	const alter = "ALTER TABLE `" + migrateStateTableName + "` ADD COLUMN state_format INT NOT NULL DEFAULT 1"
	if _, err := s.db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("mysql: ensure migrate-state table: add state_format: %w", err)
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
