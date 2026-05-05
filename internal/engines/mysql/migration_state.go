// Per-target persistence for resumable simple-mode migrations.
//
// This file mirrors control_table.go's shape — same idempotent CREATE
// TABLE, same detect-then-ALTER migration story for older deployments
// — but holds different state for a different concept.
// `sluice_cdc_state` tracks long-running continuous-sync streams (one
// row per stream); `sluice_migrate_state` tracks one-shot simple-mode
// migrations (one row per --migration-id). They are deliberately kept
// separate: streams and migrations have different lifetimes and
// different recovery semantics, and conflating them would make ad-hoc
// inspection of either harder.
//
// MySQL has a flat namespace (no schema-qualification needed beyond
// the connection's default database), so the table lives unqualified.
//
// The store is wired into the MySQL engine via
// [Engine.OpenMigrationStateStore], which the pipeline.Migrator
// type-asserts at startup. See internal/ir/interfaces.go for the
// MigrationStateStore contract.

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// migrateStateTableName is the per-target table that holds simple-
// mode migration state. Parallel to controlTableName; the two
// deliberately don't share a row model.
const migrateStateTableName = "sluice_migrate_state"

// MigrationStateStore is the MySQL implementation of
// [ir.MigrationStateStore]. One value per target connection pool.
type MigrationStateStore struct {
	db *sql.DB
}

// Close releases the underlying connection pool.
func (s *MigrationStateStore) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// EnsureControlTable creates the per-target sluice_migrate_state
// table if it doesn't exist. Idempotent — safe to call on every
// start.
//
// MySQL doesn't allow CREATE TABLE inside a multi-statement tx; the
// caller runs this from the *sql.DB pool at Migrator startup, not
// inside a per-write tx (the single-row write paths use the same
// pool).
//
// No detect-then-ALTER is needed today because the table is shipped
// in its final v1 shape; the pattern is established in control_table
// .go for future column additions.
func (s *MigrationStateStore) EnsureControlTable(ctx context.Context) error {
	const ddl = `
		CREATE TABLE IF NOT EXISTS ` + "`" + migrateStateTableName + "`" + ` (
			migration_id    VARCHAR(255) NOT NULL,
			phase           VARCHAR(32)  NOT NULL,
			table_progress  TEXT         NULL,
			started_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
				ON UPDATE CURRENT_TIMESTAMP,
			last_error      TEXT         NULL,
			PRIMARY KEY (migration_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("mysql: ensure migrate-state table: %w", err)
	}
	return nil
}

// Read returns the row for migrationID, or ok=false when no row
// exists. Tolerant of the table being absent (treated as "no row")
// so dry-run / pre-EnsureControlTable inspection paths don't error.
func (s *MigrationStateStore) Read(ctx context.Context, migrationID string) (ir.MigrationState, bool, error) {
	if migrationID == "" {
		return ir.MigrationState{}, false, errors.New("mysql: migrate-state Read: migrationID is empty")
	}
	const q = "SELECT migration_id, phase, table_progress, started_at, updated_at, last_error " +
		"FROM `" + migrateStateTableName + "` WHERE migration_id = ?"
	row := s.db.QueryRowContext(ctx, q, migrationID)

	var (
		id, phase                string
		tableProgress, lastError sql.NullString
		startedAt, updatedAt     time.Time
	)
	switch err := row.Scan(&id, &phase, &tableProgress, &startedAt, &updatedAt, &lastError); {
	case errors.Is(err, sql.ErrNoRows):
		return ir.MigrationState{}, false, nil
	case isMySQLMissingTableErr(err):
		return ir.MigrationState{}, false, nil
	case err != nil:
		return ir.MigrationState{}, false, fmt.Errorf("mysql: read migrate-state: %w", err)
	}

	progress, err := decodeMigrateTableProgress(tableProgress.String)
	if err != nil {
		return ir.MigrationState{}, false, fmt.Errorf("mysql: decode table_progress: %w", err)
	}
	return ir.MigrationState{
		MigrationID:   id,
		Phase:         ir.MigrationPhase(phase),
		TableProgress: progress,
		StartedAt:     startedAt,
		UpdatedAt:     updatedAt,
		LastError:     lastError.String,
	}, true, nil
}

// Write upserts the migration-state row. updated_at is refreshed to
// CURRENT_TIMESTAMP via the ON UPDATE clause; started_at is set on
// first insert and preserved on subsequent upserts because we don't
// list it in the ON DUPLICATE KEY UPDATE clause.
func (s *MigrationStateStore) Write(ctx context.Context, state ir.MigrationState) error {
	if state.MigrationID == "" {
		return errors.New("mysql: migrate-state Write: MigrationID is empty")
	}
	if state.Phase == "" {
		return errors.New("mysql: migrate-state Write: Phase is empty")
	}
	progressJSON, err := encodeMigrateTableProgress(state.TableProgress)
	if err != nil {
		return fmt.Errorf("mysql: encode table_progress: %w", err)
	}

	// Row-alias UPSERT (8.0.20+) — same form the data-write path
	// uses for cross-applier consistency. started_at is deliberately
	// excluded from the SET list so its DEFAULT CURRENT_TIMESTAMP on
	// the original INSERT survives subsequent upserts.
	const q = "INSERT INTO `" + migrateStateTableName + "` " +
		"(migration_id, phase, table_progress, last_error) " +
		"VALUES (?, ?, ?, ?) AS new " +
		"ON DUPLICATE KEY UPDATE " +
		"phase = new.phase, " +
		"table_progress = new.table_progress, " +
		"last_error = new.last_error"
	args := []any{
		state.MigrationID,
		string(state.Phase),
		nullableMigrateString(progressJSON),
		nullableMigrateString(state.LastError),
	}
	if _, err := s.db.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("mysql: write migrate-state: %w", err)
	}
	return nil
}

// nullableMigrateString returns a sql.NullString that's invalid when
// s is empty. Empty values land as SQL NULL on disk so a fresh row
// with no per-table progress reads back as nil rather than `{}`.
//
// Suffix is `Migrate` rather than the bare name to avoid colliding
// with any future helper of the same shape elsewhere in the engine
// package.
func nullableMigrateString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// encodeMigrateTableProgress serialises the per-table state map to
// JSON. An empty or nil map returns "" — store as SQL NULL — so a
// freshly-inserted state row before any table starts isn't littered
// with `{}` literals. Per-entry encoding is delegated to
// [ir.TableProgress.MarshalJSON]; see internal/ir/migration_state.go
// for the bare-string-vs-object choice.
func encodeMigrateTableProgress(m map[string]ir.TableProgress) (string, error) {
	if len(m) == 0 {
		return "", nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// decodeMigrateTableProgress is the inverse of
// encodeMigrateTableProgress. An empty input returns nil so callers
// can use the zero map shape without a special case. Per-entry
// decoding is delegated to [ir.TableProgress.UnmarshalJSON] which
// accepts both the v0.3.0 bare-string form and the v0.4.0 cursor-
// bearing object form.
func decodeMigrateTableProgress(s string) (map[string]ir.TableProgress, error) {
	if s == "" {
		return nil, nil
	}
	out := map[string]ir.TableProgress{}
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, err
	}
	return out, nil
}
