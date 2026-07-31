// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Crash-recovery record for the ADR-0182 query-timeout raise
//
// The opt-in PlanetScale query-timeout raise (ADR-0182) modifies a
// keyspace-wide setting for the duration of a migration and must restore it
// afterwards — even across a crash. The "leave it as you found it" guarantee
// needs a durable record of "we raised the timeout; the value to restore is
// X", written BEFORE the raise takes effect and read on the next run's start
// so a dangling raise is reverted before anything else.
//
// That record rides a dedicated `ps_query_timeout_raise` column on the same
// sluice_migrate_state HEADER row keyed by migration_id (added additively,
// detect-then-ALTER, exactly like state_format). It is written through its
// OWN statements below — NEVER the generic header upsert — so ordinary phase
// transitions (which never touch the column) cannot clobber a live raise
// record. NULL means "no raise recorded"; a non-NULL JSON envelope carries
// the previous value (which may itself be empty, meaning "the keyspace was at
// its default when we raised it").
//
// MySQL-only by construction: the raise only ever targets a
// PlanetScale/Vitess keyspace, so only the MySQL store implements
// [ir.QueryTimeoutRaiseRecorder]. Postgres (never a PlanetScale target) is
// untouched.

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// Compile-time proof the MySQL store satisfies the pipeline-facing recorder.
var _ ir.QueryTimeoutRaiseRecorder = (*MigrationStateStore)(nil)

// psQueryTimeoutRaiseRecord is the JSON shape of the ps_query_timeout_raise
// column. Its mere presence (a non-NULL column) means a raise is recorded;
// Previous is the value to restore, and an EMPTY Previous is meaningful — it
// records that the keyspace held no explicit option (was at its default) when
// we raised it, which the revert maps back to the documented default. The
// envelope (rather than a bare string) keeps that distinction unambiguous
// against SQL NULL and leaves room to grow.
type psQueryTimeoutRaiseRecord struct {
	Previous string `json:"previous"`
}

// ensurePSQueryTimeoutRaiseColumn adds the ps_query_timeout_raise column to a
// header table created by a binary that predates ADR-0182. Detect-then-ALTER,
// mirroring ensureStateFormatColumn (portable below MySQL 8.0.29, which lacks
// ADD COLUMN IF NOT EXISTS).
func (s *MigrationStateStore) ensurePSQueryTimeoutRaiseColumn(ctx context.Context) error {
	const checkQ = `
		SELECT COUNT(*)
		FROM   information_schema.COLUMNS
		WHERE  TABLE_SCHEMA = DATABASE()
		  AND  TABLE_NAME   = ?
		  AND  COLUMN_NAME  = 'ps_query_timeout_raise'`
	var n int
	if err := s.db.QueryRowContext(ctx, checkQ, migrateStateTableName).Scan(&n); err != nil {
		return fmt.Errorf("mysql: ensure migrate-state table: detect ps_query_timeout_raise: %w", err)
	}
	if n > 0 {
		return nil
	}
	const alter = "ALTER TABLE `" + migrateStateTableName + "` ADD COLUMN ps_query_timeout_raise TEXT NULL"
	if _, err := s.db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("mysql: ensure migrate-state table: add ps_query_timeout_raise: %w", wrapControlTableBootstrapError(err, alter))
	}
	return nil
}

// ReadQueryTimeoutRaise implements [ir.QueryTimeoutRaiseRecorder]. A missing
// table or row (dry-run / pre-Ensure inspection, or simply no raise recorded)
// reads as ok=false — never an error.
func (s *MigrationStateStore) ReadQueryTimeoutRaise(ctx context.Context, migrationID string) (previous string, ok bool, err error) {
	if migrationID == "" {
		return "", false, errors.New("mysql: read query-timeout raise: migrationID is empty")
	}
	const q = "SELECT ps_query_timeout_raise FROM `" + migrateStateTableName + "` WHERE migration_id = ?"
	var raw sql.NullString
	switch err := s.db.QueryRowContext(ctx, q, migrationID).Scan(&raw); {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case isMySQLMissingTableErr(err):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("mysql: read query-timeout raise: %w", err)
	}
	if !raw.Valid {
		return "", false, nil
	}
	var rec psQueryTimeoutRaiseRecord
	if err := json.Unmarshal([]byte(raw.String), &rec); err != nil {
		return "", false, fmt.Errorf("mysql: decode query-timeout raise record for migration_id %q: %w", migrationID, err)
	}
	return rec.Previous, true, nil
}

// RecordQueryTimeoutRaise implements [ir.QueryTimeoutRaiseRecorder]. UPDATE
// (not upsert): the header row is guaranteed to exist by the time a raise is
// recorded (the pipeline's loadOrInitState always writes it first), and an
// UPDATE that touches ONLY this column can never disturb the row's phase or
// progress.
func (s *MigrationStateStore) RecordQueryTimeoutRaise(ctx context.Context, migrationID, previous string) error {
	if migrationID == "" {
		return errors.New("mysql: record query-timeout raise: migrationID is empty")
	}
	encoded, err := json.Marshal(psQueryTimeoutRaiseRecord{Previous: previous})
	if err != nil {
		return fmt.Errorf("mysql: encode query-timeout raise record: %w", err)
	}
	const q = "UPDATE `" + migrateStateTableName + "` SET ps_query_timeout_raise = ? WHERE migration_id = ?"
	res, err := s.db.ExecContext(ctx, q, string(encoded), migrationID)
	if err != nil {
		return fmt.Errorf("mysql: record query-timeout raise: %w", err)
	}
	// A zero-row UPDATE means the header row is missing — the caller broke
	// the loadOrInitState-first contract, and silently doing nothing would
	// let the raise proceed with no crash-recovery record.
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return fmt.Errorf("mysql: record query-timeout raise: no migrate-state header row for migration_id %q", migrationID)
	}
	return nil
}

// ClearQueryTimeoutRaise implements [ir.QueryTimeoutRaiseRecorder]. Idempotent
// and tolerant of a missing table/row.
func (s *MigrationStateStore) ClearQueryTimeoutRaise(ctx context.Context, migrationID string) error {
	if migrationID == "" {
		return errors.New("mysql: clear query-timeout raise: migrationID is empty")
	}
	const q = "UPDATE `" + migrateStateTableName + "` SET ps_query_timeout_raise = NULL WHERE migration_id = ?"
	if _, err := s.db.ExecContext(ctx, q, migrationID); err != nil && !isMySQLMissingTableErr(err) {
		return fmt.Errorf("mysql: clear query-timeout raise: %w", err)
	}
	return nil
}
