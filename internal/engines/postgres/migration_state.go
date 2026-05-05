// Per-target persistence for resumable simple-mode migrations.
//
// This file mirrors control_table.go's shape — same idempotent CREATE
// TABLE, same ADD COLUMN IF NOT EXISTS migration story for older
// deployments — but holds different state for a different concept.
// `sluice_cdc_state` tracks long-running continuous-sync streams (one
// row per stream); `sluice_migrate_state` tracks one-shot simple-mode
// migrations (one row per --migration-id). They are deliberately kept
// separate: streams and migrations have different lifetimes and
// different recovery semantics, and conflating them would make ad-hoc
// inspection of either harder.
//
// The store is wired into the Postgres engine via
// [Engine.OpenMigrationStateStore], which the pipeline.Migrator
// type-asserts at startup. See internal/ir/interfaces.go for the
// MigrationStateStore contract.

package postgres

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

// MigrationStateStore is the Postgres implementation of
// [ir.MigrationStateStore]. One value per target connection pool;
// safe to call concurrently from a single Migrator.Run.
type MigrationStateStore struct {
	db     *sql.DB
	schema string
}

// Close releases the underlying connection pool.
func (s *MigrationStateStore) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// EnsureControlTable creates the per-target sluice_migrate_state
// table in the configured schema if it doesn't exist. Idempotent —
// safe to call on every start.
//
// The ADD COLUMN IF NOT EXISTS calls beneath the CREATE TABLE provide
// a forward-compatibility hook for any future column we add: a v0.3
// deployment with an older table shape picks up the new columns on
// the next Migrator.Run. v1 has no such columns yet, but the pattern
// is established here for parity with control_table.go's
// stop_requested_at migration.
func (s *MigrationStateStore) EnsureControlTable(ctx context.Context) error {
	tableRef := quoteIdent(s.schema) + "." + quoteIdent(migrateStateTableName)
	ddl := `
		CREATE TABLE IF NOT EXISTS ` + tableRef + ` (
			migration_id    VARCHAR(255) NOT NULL,
			phase           VARCHAR(32)  NOT NULL,
			table_progress  TEXT         NULL,
			started_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			last_error      TEXT         NULL,
			PRIMARY KEY (migration_id)
		)`
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("postgres: ensure migrate-state table: %w", err)
	}
	return nil
}

// Read returns the row for migrationID, or ok=false when no row
// exists. Tolerant of the table being absent (treated as "no row")
// so dry-run / pre-EnsureControlTable inspection paths don't error.
func (s *MigrationStateStore) Read(ctx context.Context, migrationID string) (ir.MigrationState, bool, error) {
	if migrationID == "" {
		return ir.MigrationState{}, false, errors.New("postgres: migrate-state Read: migrationID is empty")
	}
	tableRef := quoteIdent(s.schema) + "." + quoteIdent(migrateStateTableName)
	q := "SELECT migration_id, phase, table_progress, started_at, updated_at, last_error FROM " +
		tableRef + " WHERE migration_id = $1"
	row := s.db.QueryRowContext(ctx, q, migrationID)

	var (
		id, phase                string
		tableProgress, lastError sql.NullString
		startedAt, updatedAt     time.Time
	)
	switch err := row.Scan(&id, &phase, &tableProgress, &startedAt, &updatedAt, &lastError); {
	case errors.Is(err, sql.ErrNoRows):
		return ir.MigrationState{}, false, nil
	case isUndefinedRelationErr(err):
		return ir.MigrationState{}, false, nil
	case err != nil:
		return ir.MigrationState{}, false, fmt.Errorf("postgres: read migrate-state: %w", err)
	}

	progress, err := decodeTableProgress(tableProgress.String)
	if err != nil {
		return ir.MigrationState{}, false, fmt.Errorf("postgres: decode table_progress: %w", err)
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
// CURRENT_TIMESTAMP on every call; started_at is set on first insert
// and preserved on subsequent upserts via the COALESCE-on-conflict
// trick so resume runs don't overwrite the original start time.
func (s *MigrationStateStore) Write(ctx context.Context, state ir.MigrationState) error {
	if state.MigrationID == "" {
		return errors.New("postgres: migrate-state Write: MigrationID is empty")
	}
	if state.Phase == "" {
		return errors.New("postgres: migrate-state Write: Phase is empty")
	}
	progressJSON, err := encodeTableProgress(state.TableProgress)
	if err != nil {
		return fmt.Errorf("postgres: encode table_progress: %w", err)
	}

	tableRef := quoteIdent(s.schema) + "." + quoteIdent(migrateStateTableName)
	// COALESCE on started_at: keep the existing value when the row
	// already exists; populate from EXCLUDED on first insert. The
	// EXCLUDED-side default is CURRENT_TIMESTAMP via the column
	// default, which fires on insert but not on update — exactly the
	// "set once" semantics we want.
	q := "INSERT INTO " + tableRef + " " +
		"(migration_id, phase, table_progress, started_at, updated_at, last_error) " +
		"VALUES ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $4) " +
		"ON CONFLICT (migration_id) DO UPDATE SET " +
		"phase = EXCLUDED.phase, " +
		"table_progress = EXCLUDED.table_progress, " +
		"updated_at = CURRENT_TIMESTAMP, " +
		"last_error = EXCLUDED.last_error"
	args := []any{
		state.MigrationID,
		string(state.Phase),
		nullableString(progressJSON),
		nullableString(state.LastError),
	}
	if _, err := s.db.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("postgres: write migrate-state: %w", err)
	}
	return nil
}

// nullableString returns a sql.NullString that's invalid when s is
// empty. Centralises the "empty string maps to SQL NULL" convention
// so encodeTableProgress's empty-map = nil JSON stays distinct from
// "no entries yet" on disk.
func nullableString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// encodeTableProgress serialises the per-table state map to JSON.
// An empty or nil map returns "" — store as SQL NULL — so a freshly-
// inserted state row before any table starts isn't littered with
// `{}` literals. The per-entry encoding is delegated to
// [ir.TableProgress.MarshalJSON]; see internal/ir/migration_state.go
// for the bare-string-vs-object choice.
func encodeTableProgress(m map[string]ir.TableProgress) (string, error) {
	if len(m) == 0 {
		return "", nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// decodeTableProgress is the inverse of encodeTableProgress. An empty
// input returns nil so callers can use the zero map shape without a
// special case. Per-entry decoding is delegated to
// [ir.TableProgress.UnmarshalJSON] which accepts both the v0.3.0 bare-
// string form and the v0.4.0 cursor-bearing object form.
func decodeTableProgress(s string) (map[string]ir.TableProgress, error) {
	if s == "" {
		return nil, nil
	}
	out := map[string]ir.TableProgress{}
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, err
	}
	return out, nil
}
