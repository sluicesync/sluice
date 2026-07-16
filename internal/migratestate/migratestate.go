// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package migratestate is the shared persistence skeleton for the
// per-target resumable-migration state store (ADR-0082).
//
// # Why this package exists
//
// Through v0.99.x both engines persisted the entire
// map[table]TableProgress as ONE JSON blob in the single
// sluice_migrate_state row, and every per-table breadcrumb and every
// per-batch resume cursor re-encoded and re-upserted the whole blob.
// At the 10k-table scale the ADR-0076 cross-table pool targets that
// is O(N) work per checkpoint and O(N²) over a migration — roughly
// 0.5–1 MB of JSON rewritten through the target's MVCC/TOAST on one
// hot row, ≥20k times. ADR-0082 splits the store into:
//
//   - a HEADER row (sluice_migrate_state): migration_id, phase,
//     state_format, started_at/updated_at, last_error — the global,
//     rarely-written fields; and
//   - one PROGRESS row per table (sluice_migrate_table_progress):
//     (migration_id, table_name) → that table's TableProgress JSON.
//
// Per-table breadcrumbs and per-batch cursors become single-row
// upserts — O(1) in table count.
//
// # The dialect seam (the ADR-0081 tier-c precedent)
//
// Like appliershared's control-table CRUD, the store logic lives ONCE
// here behind a small dialect seam: each engine builds its finished
// SQL (identifier quoting, placeholder style, upsert syntax, PG's
// schema qualification stay engine-side) plus a [Config] of the
// genuinely divergent leaves, and this package owns the control flow
// — scan loops, missing-table tolerance, the legacy upgrade
// transaction, error shapes. The ensure-table DDL and the per-engine
// column-migration mechanism (ADD COLUMN IF NOT EXISTS vs
// detect-then-ALTER) stay engine-side, exactly as tier (c) found for
// the CDC control table.
//
// # Legacy upgrade and the one-way sentinel
//
// A header row written by a ≤v0.99.x binary has state_format 1 (the
// column's default after the additive migration) and the whole map in
// table_progress. [Store.Read] DETECTS such a row and decodes the blob
// without writing anything — a Read must stay a read, so inspection
// under a read-only target user (and dry-run paths) works on legacy
// rows. The one-time upgrade — explode the blob into per-table
// progress rows inside a single transaction and overwrite the blob
// with [UpgradedBlobSentinel] — runs on the FIRST WRITE path
// ([Store.Write] / [Store.WriteTableProgress]) for that migration_id,
// which was going to mutate the store anyway. The sentinel is
// deliberately NOT valid JSON, so an old binary that later reads the
// row fails loudly at decode instead of silently treating the
// migration as "no progress" and re-copying tables. Crash-safety: the
// transaction either commits whole (format 2, rows authoritative) or
// rolls back (format 1, blob authoritative) — the next Read re-detects
// the legacy row and the next write re-runs the upgrade, and the
// delete-rows-first step inside the transaction makes the re-run
// immune to orphan progress rows from any earlier life of the same
// migration_id.
package migratestate

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// HeaderTableName is the per-target table that holds one header row
// per --migration-id. Same table as the pre-ADR-0082 single-row
// store; the per-table progress moved out of its table_progress
// column into [ProgressTableName]. Each engine aliases this constant
// so its const SQL strings keep a single source of truth.
const HeaderTableName = "sluice_migrate_state"

// ProgressTableName is the per-target table that holds one row per
// (migration_id, table_name) — that table's TableProgress JSON. The
// engines' schema readers exclude it from user-schema reads alongside
// the other sluice_* bookkeeping tables (ADR-0015).
const ProgressTableName = "sluice_migrate_table_progress"

// Header state_format values. The column is additive: a header table
// created by a ≤v0.99.x binary gains it with DEFAULT 1 on the next
// EnsureControlTable, so "column says 1" and "row written by an old
// binary" coincide exactly.
const (
	// FormatLegacyBlob marks a row whose table_progress column holds
	// the whole-map JSON blob (the ≤v0.99.x wire shape). Read-only:
	// new binaries never write this format; Read upgrades it.
	FormatLegacyBlob = 1

	// FormatPerTableRows marks a row whose per-table progress lives in
	// [ProgressTableName]; the header's table_progress column holds
	// [UpgradedBlobSentinel] and is never decoded.
	FormatPerTableRows = 2
)

// UpgradedBlobSentinel replaces the legacy table_progress blob once a
// row is upgraded to (or born at) FormatPerTableRows. It is
// deliberately NOT valid JSON: a ≤v0.99.x binary pointed at an
// upgraded row fails its table_progress decode loudly ("invalid
// character 'u' ...") instead of silently reading "no progress" and
// re-copying every table — loud failure beats silent re-copy, and the
// text itself tells an operator inspecting the row in psql/mysql
// where the progress went. State-row compatibility is one-way
// (forward), the same contract the v0.4.0 → v0.3.0 shape change set.
const UpgradedBlobSentinel = "upgraded to sluice state_format 2: per-table progress lives in " +
	ProgressTableName + "; older sluice binaries cannot read this migration"

// Config is the dialect seam for the shared migrate-state store.
// Everything NOT in this struct — scan loops, missing-table
// tolerance, the upgrade transaction, error shapes — is shared
// control flow that lives in this package.
type Config struct {
	// EngineName prefixes every error this package wraps
	// ("postgres: read migrate-state: …") so operator-facing output
	// is byte-identical to the pre-extraction per-engine store.
	EngineName string

	// IsMissingTable classifies the dialect's "table absent" error
	// (MySQL Error 1146, PG SQLSTATE 42P01). Read and ClearMigration
	// tolerate a missing table — dry-run / pre-EnsureControlTable
	// inspection degrades to "no row" rather than erroring. Must
	// return false on nil.
	IsMissingTable func(error) bool
}

// SQL carries the engine-built statements the shared control flow
// executes. Placeholder ARGUMENT ORDER is part of the contract — each
// field's comment names the exact argument tuple this package passes;
// the engine writes its placeholders to consume them in that order.
type SQL struct {
	// ReadHeader: args (migrationID). Must project exactly
	// (phase, table_progress, state_format, started_at, updated_at,
	// last_error) for the header row.
	ReadHeader string

	// ReadProgressRows: args (migrationID). Must project exactly
	// (table_name, progress, updated_at) for every progress row of
	// the migration.
	ReadProgressRows string

	// UpsertHeader: args (migrationID, phase, blobSentinel,
	// stateFormat, lastError). Inserts or updates the header row,
	// setting started_at only on first insert (the engine's upsert
	// preserves it on conflict) and refreshing updated_at.
	UpsertHeader string

	// UpsertProgressRow: args (migrationID, tableName, progressJSON).
	// Inserts or updates one progress row, refreshing its updated_at.
	UpsertProgressRow string

	// MarkUpgraded: args (blobSentinel, stateFormat, migrationID).
	// Flips a legacy header row to FormatPerTableRows and replaces
	// its blob with the sentinel. Runs inside the upgrade tx.
	MarkUpgraded string

	// DeleteHeader / DeleteProgressRows: args (migrationID).
	DeleteHeader       string
	DeleteProgressRows string
}

// Store is the shared ir.MigrationStateStore CRUD (everything except
// EnsureControlTable and Close, which stay engine-side — the DDL and
// the column-migration mechanism are wholly dialect). One value per
// engine MigrationStateStore; safe for the Migrator's concurrent
// per-table writers because every method is a self-contained
// statement or transaction on the pool.
type Store struct {
	DB     *sql.DB
	Config Config
	SQL    SQL

	// mu guards pendingUpgrades — the legacy rows Read has detected
	// whose one-time per-table-rows upgrade is deferred to the first
	// write path (see the package comment). The mutex is held across
	// the upgrade transaction itself so concurrent per-table writers
	// run it exactly once.
	mu              sync.Mutex
	pendingUpgrades map[string]map[string]ir.TableProgress
}

// Read returns the state for migrationID, or ok=false when no header
// row exists. Tolerant of the header table being absent (treated as
// "no row") so dry-run / pre-EnsureControlTable inspection paths
// don't error.
//
// Read never writes. On a FormatLegacyBlob row it decodes the blob and
// RECORDS the row as needing the one-time per-table-rows upgrade; the
// first write path (Write / WriteTableProgress) for that migration_id
// performs it (see the package comment). This keeps Read working under
// a read-only target user, and keeps a legacy row readable by older
// binaries until this binary actually mutates it. Requires
// EnsureControlTable to have run for legacy-shaped tables — the
// header SELECT references state_format, which the ensure migration
// adds.
func (s *Store) Read(ctx context.Context, migrationID string) (ir.MigrationState, bool, error) {
	if migrationID == "" {
		return ir.MigrationState{}, false, fmt.Errorf("%s: migrate-state Read: migrationID is empty", s.Config.EngineName)
	}
	row := s.DB.QueryRowContext(ctx, s.SQL.ReadHeader, migrationID)

	var (
		phase                    string
		tableProgress, lastError sql.NullString
		format                   int
		startedAt, updatedAt     time.Time
	)
	switch err := row.Scan(&phase, &tableProgress, &format, &startedAt, &updatedAt, &lastError); {
	case errors.Is(err, sql.ErrNoRows):
		s.dropPendingUpgrade(migrationID)
		return ir.MigrationState{}, false, nil
	case s.Config.IsMissingTable(err):
		s.dropPendingUpgrade(migrationID)
		return ir.MigrationState{}, false, nil
	case err != nil:
		return ir.MigrationState{}, false, fmt.Errorf("%s: read migrate-state: %w", s.Config.EngineName, err)
	}

	state := ir.MigrationState{
		MigrationID: migrationID,
		Phase:       ir.MigrationPhase(phase),
		StartedAt:   startedAt,
		UpdatedAt:   updatedAt,
		LastError:   lastError.String,
	}

	if format >= FormatPerTableRows {
		// The row is already at the per-table layout — clear any stale
		// needs-upgrade note (e.g. another process upgraded between two
		// Reads) so a later write can't replay old blob progress.
		s.dropPendingUpgrade(migrationID)
		progress, latest, err := s.readProgressRows(ctx, migrationID)
		if err != nil {
			return ir.MigrationState{}, false, err
		}
		state.TableProgress = progress
		// The header's updated_at only moves on header writes now;
		// surface the most recent activity across header + progress
		// rows so the operator-facing "age" stays meaningful.
		if latest.After(state.UpdatedAt) {
			state.UpdatedAt = latest
		}
		return state, true, nil
	}

	// FormatLegacyBlob: a ≤v0.99.x row. Decode the whole-map blob and
	// note the row for the write-deferred one-time upgrade — Read
	// itself must not write (read-only target users inspect through
	// here).
	progress, err := decodeLegacyTableProgress(tableProgress.String)
	if err != nil {
		return ir.MigrationState{}, false, fmt.Errorf("%s: decode table_progress: %w", s.Config.EngineName, err)
	}
	state.TableProgress = progress
	s.notePendingUpgrade(migrationID, progress)
	return state, true, nil
}

// notePendingUpgrade records that migrationID's header row was seen at
// FormatLegacyBlob with the given decoded blob progress. The first
// write path replays it into per-table rows via upgradeIfPending.
func (s *Store) notePendingUpgrade(migrationID string, progress map[string]ir.TableProgress) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingUpgrades == nil {
		s.pendingUpgrades = map[string]map[string]ir.TableProgress{}
	}
	s.pendingUpgrades[migrationID] = progress
}

// dropPendingUpgrade forgets a needs-upgrade note — the row is gone or
// already at the per-table layout, so replaying the old blob would
// resurrect stale progress.
func (s *Store) dropPendingUpgrade(migrationID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pendingUpgrades, migrationID)
}

// upgradeIfPending runs the one-time legacy upgrade when Read flagged
// migrationID's header as FormatLegacyBlob, and is a no-op otherwise.
// Every write path calls it first, so the write it precedes always
// lands on a per-table-layout row (a progress row upserted under a
// still-legacy header would be invisible to Read). The mutex is held
// across the upgrade transaction so the Migrator's concurrent
// per-table writers run it exactly once; a failure leaves the note in
// place (blob authoritative) for the next write to retry.
func (s *Store) upgradeIfPending(ctx context.Context, migrationID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	progress, ok := s.pendingUpgrades[migrationID]
	if !ok {
		return nil
	}
	if err := s.upgradeLegacyRow(ctx, migrationID, progress); err != nil {
		return fmt.Errorf("%s: upgrade migrate-state row to per-table progress: %w", s.Config.EngineName, err)
	}
	delete(s.pendingUpgrades, migrationID)
	return nil
}

// readProgressRows loads every progress row for migrationID plus the
// most recent per-row updated_at. A missing progress table is NOT
// tolerated here: the caller only gets here after finding a
// FormatPerTableRows header, which EnsureControlTable created
// alongside the progress table — its absence means someone dropped it
// and the recorded progress is gone, which must surface loudly rather
// than read as "every table is fresh".
func (s *Store) readProgressRows(ctx context.Context, migrationID string) (map[string]ir.TableProgress, time.Time, error) {
	rows, err := s.DB.QueryContext(ctx, s.SQL.ReadProgressRows, migrationID)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("%s: read migrate-state progress rows: %w", s.Config.EngineName, err)
	}
	defer func() { _ = rows.Close() }()

	var (
		out    map[string]ir.TableProgress
		latest time.Time
	)
	for rows.Next() {
		var (
			name, progress string
			updated        time.Time
		)
		if err := rows.Scan(&name, &progress, &updated); err != nil {
			return nil, time.Time{}, fmt.Errorf("%s: scan migrate-state progress row: %w", s.Config.EngineName, err)
		}
		var entry ir.TableProgress
		if err := json.Unmarshal([]byte(progress), &entry); err != nil {
			return nil, time.Time{}, fmt.Errorf("%s: decode progress for table %q: %w", s.Config.EngineName, name, err)
		}
		if out == nil {
			out = map[string]ir.TableProgress{}
		}
		out[name] = entry
		if updated.After(latest) {
			latest = updated
		}
	}
	if err := rows.Err(); err != nil {
		return nil, time.Time{}, fmt.Errorf("%s: read migrate-state progress rows: %w", s.Config.EngineName, err)
	}
	return out, latest, nil
}

// upgradeLegacyRow explodes a legacy blob's entries into per-table
// progress rows and flips the header to FormatPerTableRows, all in
// one transaction. Called from upgradeIfPending (under the store
// mutex) on the first write path after Read detected a legacy row.
// Crash-safe by tx atomicity: a crash before commit leaves the row at
// FormatLegacyBlob with the blob intact, and the next Read + write
// pair re-runs the whole upgrade. The delete-first step clears
// any orphan progress rows a previous life of this migration_id left
// behind (e.g. an old binary's ClearMigration deleted only the header
// row it knew about), so stale entries can never shadow the blob's.
//
// Inserts are ordered by table name so any two concurrent upgrades of
// the same row lock progress rows in the same order — no deadlock
// cycle (single-process ownership of a migration_id is the contract,
// but the ordering makes the property hold even without it).
func (s *Store) upgradeLegacyRow(ctx context.Context, migrationID string, progress map[string]ir.TableProgress) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, s.SQL.DeleteProgressRows, migrationID); err != nil {
		return fmt.Errorf("clear orphan progress rows: %w", err)
	}
	for _, name := range sortedTableNames(progress) {
		encoded, err := encodeProgressEntry(progress[name])
		if err != nil {
			return fmt.Errorf("encode progress for table %q: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx, s.SQL.UpsertProgressRow, migrationID, name, encoded); err != nil {
			return fmt.Errorf("write progress row for table %q: %w", name, err)
		}
	}
	if _, err := tx.ExecContext(ctx, s.SQL.MarkUpgraded, UpgradedBlobSentinel, FormatPerTableRows, migrationID); err != nil {
		return fmt.Errorf("mark header upgraded: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// Write upserts the header row and any per-table entries present in
// state.TableProgress. It NEVER deletes progress rows absent from the
// map — entries are only ever added or updated over a migration's
// life, so a header-only Write (nil/empty map, the phase-transition
// shape) leaves previously-persisted per-table progress untouched.
// The hot per-checkpoint path is [Store.WriteTableProgress]; Write
// with a populated map is the rare full-snapshot shape (first write,
// tests).
//
// The header always lands at FormatPerTableRows with the sentinel
// blob, so a fresh migration is born one-way exactly like an upgraded
// one. updated_at refresh and started_at set-once semantics live in
// the engines' upsert SQL.
func (s *Store) Write(ctx context.Context, state ir.MigrationState) error {
	if state.MigrationID == "" {
		return fmt.Errorf("%s: migrate-state Write: MigrationID is empty", s.Config.EngineName)
	}
	if state.Phase == "" {
		return fmt.Errorf("%s: migrate-state Write: Phase is empty", s.Config.EngineName)
	}
	// A legacy row Read flagged must be exploded into per-table rows
	// BEFORE the header flips to FormatPerTableRows below — a
	// header-only Write on a legacy row would otherwise replace the
	// blob with the sentinel and lose the recorded progress.
	if err := s.upgradeIfPending(ctx, state.MigrationID); err != nil {
		return err
	}
	headerArgs := []any{
		state.MigrationID,
		string(state.Phase),
		UpgradedBlobSentinel,
		FormatPerTableRows,
		nullableString(state.LastError),
	}

	if len(state.TableProgress) == 0 {
		// Header-only: the common O(1) shape (initial pending row,
		// phase transitions, failure marks). No transaction needed.
		if _, err := s.DB.ExecContext(ctx, s.SQL.UpsertHeader, headerArgs...); err != nil {
			return fmt.Errorf("%s: write migrate-state: %w", s.Config.EngineName, err)
		}
		return nil
	}

	// Full snapshot: header + every entry, one transaction, inserts
	// ordered by table name (same deadlock-ordering argument as the
	// upgrade path).
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s: write migrate-state: begin: %w", s.Config.EngineName, err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, s.SQL.UpsertHeader, headerArgs...); err != nil {
		return fmt.Errorf("%s: write migrate-state: %w", s.Config.EngineName, err)
	}
	for _, name := range sortedTableNames(state.TableProgress) {
		encoded, err := encodeProgressEntry(state.TableProgress[name])
		if err != nil {
			return fmt.Errorf("%s: encode progress for table %q: %w", s.Config.EngineName, name, err)
		}
		if _, err := tx.ExecContext(ctx, s.SQL.UpsertProgressRow, state.MigrationID, name, encoded); err != nil {
			return fmt.Errorf("%s: write progress row for table %q: %w", s.Config.EngineName, name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s: write migrate-state: commit: %w", s.Config.EngineName, err)
	}
	return nil
}

// WriteTableProgress upserts ONE table's progress row — the O(1)
// per-checkpoint write that replaces re-encoding the whole map
// (ADR-0082). It deliberately does not touch the header row: pool
// workers checkpointing different tables hit different rows instead
// of contending on one hot header.
//
// Contract: the caller has already established the header for this
// migrationID (the pipeline's loadOrInitState always Reads — which
// flags legacy rows for the write-deferred upgrade this method then
// performs — or Writes a fresh header before any per-table write
// happens). A progress row written under a still-legacy header would
// be invisible to Read; upgradeIfPending plus the pipeline ordering
// makes that unreachable.
func (s *Store) WriteTableProgress(ctx context.Context, migrationID, tableName string, progress ir.TableProgress) error {
	if migrationID == "" {
		return fmt.Errorf("%s: migrate-state WriteTableProgress: migrationID is empty", s.Config.EngineName)
	}
	if tableName == "" {
		return fmt.Errorf("%s: migrate-state WriteTableProgress: tableName is empty", s.Config.EngineName)
	}
	if err := s.upgradeIfPending(ctx, migrationID); err != nil {
		return err
	}
	encoded, err := encodeProgressEntry(progress)
	if err != nil {
		return fmt.Errorf("%s: encode progress for table %q: %w", s.Config.EngineName, tableName, err)
	}
	if _, err := s.DB.ExecContext(ctx, s.SQL.UpsertProgressRow, migrationID, tableName, encoded); err != nil {
		return fmt.Errorf("%s: write table progress %q: %w", s.Config.EngineName, tableName, err)
	}
	return nil
}

// ClearMigration deletes the progress rows and the header row for
// migrationID. Idempotent and tolerant of missing rows or missing
// tables. Progress rows go first: a crash between the two deletes
// leaves a header with zero progress rows — the next run's
// "partial migration recorded" refusal (or the reset re-run) handles
// it — whereas the reverse order would leave orphan progress rows a
// future same-id migration could read as its own. Not a transaction:
// PG aborts a tx on the first error, which would defeat the
// per-statement missing-table tolerance.
func (s *Store) ClearMigration(ctx context.Context, migrationID string) error {
	if migrationID == "" {
		return fmt.Errorf("%s: migrate-state ClearMigration: migrationID is empty", s.Config.EngineName)
	}
	if _, err := s.DB.ExecContext(ctx, s.SQL.DeleteProgressRows, migrationID); err != nil && !s.Config.IsMissingTable(err) {
		return fmt.Errorf("%s: clear migrate-state progress rows: %w", s.Config.EngineName, err)
	}
	if _, err := s.DB.ExecContext(ctx, s.SQL.DeleteHeader, migrationID); err != nil && !s.Config.IsMissingTable(err) {
		return fmt.Errorf("%s: clear migrate-state: %w", s.Config.EngineName, err)
	}
	// The row is gone — a needs-upgrade note left behind would make a
	// later same-id write resurrect the cleared blob's progress rows.
	s.dropPendingUpgrade(migrationID)
	return nil
}

// sortedTableNames returns the map's keys in lexical order — the
// stable insert order both multi-row writers use so concurrent
// writers can't lock progress rows in conflicting orders.
func sortedTableNames(m map[string]ir.TableProgress) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// encodeProgressEntry serialises one table's progress entry. The
// per-entry wire shape is owned by [ir.TableProgress.MarshalJSON]
// (bare string for terminal states, object form for cursor-bearing
// ones). Since the cursor-value envelope landed, a legacy blob's bare
// numeric cursors re-encode as envelopes during the upgrade — a
// value-exact re-encoding (the legacy decode parses integers
// exact-int64-first), no longer byte-identical re-keying.
func encodeProgressEntry(p ir.TableProgress) (string, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// decodeLegacyTableProgress decodes the ≤v0.99.x whole-map blob. An
// empty input returns nil (a fresh pending row before any table
// started stores SQL NULL). Per-entry decoding is delegated to
// [ir.TableProgress.UnmarshalJSON], which accepts both the v0.3.0
// bare-string form and the cursor-bearing object form.
func decodeLegacyTableProgress(s string) (map[string]ir.TableProgress, error) {
	if s == "" {
		return nil, nil
	}
	out := map[string]ir.TableProgress{}
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, err
	}
	return out, nil
}

// nullableString maps the package's "empty string is SQL NULL"
// convention for last_error, mirroring the pre-extraction per-engine
// helpers.
func nullableString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}
