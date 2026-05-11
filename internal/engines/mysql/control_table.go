// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// controlTableName is the per-target table that holds CDC stream
// positions. ADR-0007 picks the name; v1 honors it verbatim. A
// configurable prefix lands as part of roadmap §10.
const controlTableName = "sluice_cdc_state"

// ensureControlTable creates the per-target sluice_cdc_state table
// if it doesn't exist. Idempotent — second-and-later calls are no-
// ops courtesy of CREATE TABLE IF NOT EXISTS.
//
// The table lives in the connection's default database (DBName from
// the DSN). MySQL has a flat namespace so no schema-qualification is
// needed; the database is implicit in the connection.
//
// MySQL does not allow CREATE TABLE inside an explicit transaction
// (DDL implicit-commits), so callers run this from the *sql.DB pool
// at applier startup, not inside the per-change tx.
//
// Per-column migrations use detect-then-ALTER (information_schema
// lookup + ALTER) rather than `ADD COLUMN IF NOT EXISTS`; the IF NOT
// EXISTS form for ADD COLUMN landed in MySQL 8.0.29 and sluice
// supports 8.0+ broadly, so the conservative path is the portable
// choice. Existing rows keep their data; new columns start NULL.
//
// Tracked migrations:
//   - stop_requested_at (v0.3.0)
//   - live_added_tables (v0.27.0, ADR-0034 MySQL Phase 2 mid-stream
//     live add-table)
//   - slot_name, source_dsn_fingerprint, target_schema (v0.32.2,
//     cross-engine parity with PG control table) — close OBS-1: a
//     cross-engine PG → MySQL live add-table with `--slot-name <name>`
//     pre-v0.32.2 surfaced MySQL Error 1054 ("Unknown column
//     slot_name") at the per-target write because the column never
//     existed on the MySQL side. PG added the column in v0.24.0 via
//     a PG-target-only ALTER; the MySQL writer's CREATE TABLE never
//     picked it up. Same gap for source_dsn_fingerprint (v0.25.0)
//     and target_schema (v0.25.1, Bug 46). Bringing the schema to
//     parity lets MySQL targets faithfully record what the streamer
//     supplies — no behavior change for MySQL → MySQL flows where
//     the streamer doesn't supply any of these values.
func ensureControlTable(ctx context.Context, db *sql.DB) error {
	const ddl = `
		CREATE TABLE IF NOT EXISTS ` + "`" + controlTableName + "`" + ` (
			stream_id              VARCHAR(255) NOT NULL,
			source_position        TEXT         NOT NULL,
			updated_at             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
				ON UPDATE CURRENT_TIMESTAMP,
			stop_requested_at      TIMESTAMP    NULL,
			slot_name              VARCHAR(255) NULL,
			source_dsn_fingerprint VARCHAR(255) NULL,
			target_schema          VARCHAR(255) NULL,
			PRIMARY KEY (stream_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("mysql: ensure control table: %w", err)
	}
	if err := ensureStopRequestedColumn(ctx, db); err != nil {
		return err
	}
	if err := ensureLiveAddedTablesColumn(ctx, db); err != nil {
		return err
	}
	if err := ensureCrossEngineParityColumn(ctx, db, "slot_name", "VARCHAR(255) NULL"); err != nil {
		return err
	}
	if err := ensureCrossEngineParityColumn(ctx, db, "source_dsn_fingerprint", "VARCHAR(255) NULL"); err != nil {
		return err
	}
	return ensureCrossEngineParityColumn(ctx, db, "target_schema", "VARCHAR(255) NULL")
}

// ensureCrossEngineParityColumn adds a column to an existing control
// table when missing, using the same detect-then-ALTER shape as
// ensureStopRequestedColumn so the migration stays portable to MySQL
// 8.0.x versions older than 8.0.29 that lack ADD COLUMN IF NOT EXISTS.
// Closes OBS-1: pre-v0.32.2 deployments that ran sluice before any
// of slot_name / source_dsn_fingerprint / target_schema existed on
// the MySQL side pick the columns up on the next EnsureControlTable
// call without losing existing rows.
//
// columnDef is the bare type + nullability spec (e.g. "VARCHAR(255)
// NULL"). columnName is interpolated unsafely into the SQL — callers
// supply only internally-defined constants, never operator input.
func ensureCrossEngineParityColumn(ctx context.Context, db *sql.DB, columnName, columnDef string) error {
	const checkQ = `
		SELECT COUNT(*)
		FROM   information_schema.COLUMNS
		WHERE  TABLE_SCHEMA = DATABASE()
		  AND  TABLE_NAME   = ?
		  AND  COLUMN_NAME  = ?`
	var n int
	if err := db.QueryRowContext(ctx, checkQ, controlTableName, columnName).Scan(&n); err != nil {
		return fmt.Errorf("mysql: ensure control table: detect %s: %w", columnName, err)
	}
	if n > 0 {
		return nil
	}
	alter := "ALTER TABLE `" + controlTableName + "` ADD COLUMN `" + columnName + "` " + columnDef
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("mysql: ensure control table: add %s: %w", columnName, err)
	}
	return nil
}

// ensureLiveAddedTablesColumn adds the live_added_tables column to
// an existing control table when missing. ADR-0034 (MySQL Phase 2
// mid-stream live add-table). Same detect-then-ALTER shape as
// ensureStopRequestedColumn — keeps the migration portable to MySQL
// 8.0.x versions older than 8.0.29.
//
// The column is TEXT NULL holding a comma-separated list of
// unqualified source-table names that have been live-added to this
// stream's scope. NULL on legacy rows; the orchestrator's
// add-table --no-drain path UPSERTs the value via
// recordLiveAddedTable.
func ensureLiveAddedTablesColumn(ctx context.Context, db *sql.DB) error {
	const checkQ = `
		SELECT COUNT(*)
		FROM   information_schema.COLUMNS
		WHERE  TABLE_SCHEMA = DATABASE()
		  AND  TABLE_NAME   = ?
		  AND  COLUMN_NAME  = 'live_added_tables'`
	var n int
	if err := db.QueryRowContext(ctx, checkQ, controlTableName).Scan(&n); err != nil {
		return fmt.Errorf("mysql: ensure control table: detect live_added_tables: %w", err)
	}
	if n > 0 {
		return nil
	}
	const alter = "ALTER TABLE `" + controlTableName + "` ADD COLUMN live_added_tables TEXT NULL"
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("mysql: ensure control table: add live_added_tables: %w", err)
	}
	return nil
}

// ensureStopRequestedColumn adds the stop_requested_at column to an
// existing control table when missing. Detect-then-ALTER avoids the
// MySQL 8.0.29 floor that ADD COLUMN IF NOT EXISTS would impose;
// sluice broadly supports 8.0+. The lookup uses DATABASE() so the
// query naturally scopes to the connection's default database.
func ensureStopRequestedColumn(ctx context.Context, db *sql.DB) error {
	const checkQ = `
		SELECT COUNT(*)
		FROM   information_schema.COLUMNS
		WHERE  TABLE_SCHEMA = DATABASE()
		  AND  TABLE_NAME   = ?
		  AND  COLUMN_NAME  = 'stop_requested_at'`
	var n int
	if err := db.QueryRowContext(ctx, checkQ, controlTableName).Scan(&n); err != nil {
		return fmt.Errorf("mysql: ensure control table: detect stop_requested_at: %w", err)
	}
	if n > 0 {
		return nil
	}
	const alter = "ALTER TABLE `" + controlTableName + "` ADD COLUMN stop_requested_at TIMESTAMP NULL"
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("mysql: ensure control table: add stop_requested_at: %w", err)
	}
	return nil
}

// readPosition returns the persisted source_position for streamID,
// or ok=false when no row exists. The Engine field of the returned
// position is set to "mysql" by the caller — only the Token survives
// across runs (the engine reading is implicitly the engine that
// wrote).
//
// Tolerant of the control table being absent: a missing-table error
// is reported as ok=false (same as "no row") so dry-run flows that
// skip EnsureControlTable still work. The same string-match helper
// powers ListStreams's missing-table fallback.
func readPosition(ctx context.Context, db *sql.DB, streamID string) (token string, ok bool, err error) {
	const q = "SELECT source_position FROM `" + controlTableName + "` WHERE stream_id = ?"
	row := db.QueryRowContext(ctx, q, streamID)
	switch err := row.Scan(&token); {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case isMySQLMissingTableErr(err):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("mysql: read position: %w", err)
	}
	return token, true, nil
}

// listStreams returns every row in the per-target control table.
// Tolerant of the table being absent (treated as "no streams") so
// `sluice sync status` works against a target that hasn't been a
// CDC destination yet.
//
// The Position values returned set Engine to the supplied
// engineName for symmetry with ReadPosition's contract.
//
// COALESCE on slot_name / source_dsn_fingerprint / target_schema so
// legacy rows that pre-date those columns (v0.32.2 introduced them on
// the MySQL side; OBS-1) surface as empty strings in the StreamStatus
// — callers branch on empty-string rather than handling
// sql.NullString. The columns are also COALESCE'd in case the
// control table itself was created pre-v0.32.2 and an ALTER landed
// the columns: existing rows would be NULL until a subsequent
// position-write upserts a non-empty value.
//
// The query falls back to the legacy column set (no slot_name etc.)
// when MySQL reports "Unknown column" — the path is reachable only
// during an in-progress upgrade where another connection has run
// EnsureControlTable's ALTER concurrently but this connection's
// query was already planned against the old schema. Defence in
// depth; the fallback returns empty strings for the missing fields.
func listStreams(ctx context.Context, db *sql.DB, engineName string) ([]ir.StreamStatus, error) {
	const q = "SELECT stream_id, source_position, updated_at, " +
		"COALESCE(slot_name, ''), " +
		"COALESCE(source_dsn_fingerprint, ''), " +
		"COALESCE(target_schema, '') " +
		"FROM `" + controlTableName + "`"
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		// MySQL surfaces missing-table as error 1146; the friendly
		// fallback treats that as "no streams". String-match keeps
		// the helper driver-version-tolerant.
		if isMySQLMissingTableErr(err) {
			return []ir.StreamStatus{}, nil
		}
		// Unknown-column fallback: pre-v0.32.2 control tables that
		// haven't had EnsureControlTable's ALTER applied yet. The
		// streamer's startup runs EnsureControlTable so this is rare
		// in practice; the fallback keeps `sluice sync status` working
		// against a target mid-upgrade.
		if isMySQLUnknownColumnErr(err) {
			return listStreamsLegacy(ctx, db, engineName)
		}
		return nil, fmt.Errorf("mysql: list streams: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []ir.StreamStatus{}
	for rows.Next() {
		var (
			streamID     string
			token        string
			updated      time.Time
			slotName     string
			fingerprint  string
			targetSchema string
		)
		if err := rows.Scan(&streamID, &token, &updated, &slotName, &fingerprint, &targetSchema); err != nil {
			return nil, fmt.Errorf("mysql: scan streams: %w", err)
		}
		out = append(out, ir.StreamStatus{
			StreamID:             streamID,
			Position:             ir.Position{Engine: engineName, Token: token},
			UpdatedAt:            updated,
			SlotName:             slotName,
			SourceDSNFingerprint: fingerprint,
			TargetSchema:         targetSchema,
		})
	}
	return out, rows.Err()
}

// listStreamsLegacy is the pre-v0.32.2 SELECT shape, used as a
// fallback when the new query trips an Unknown-column error (e.g.
// in-progress upgrade window before EnsureControlTable's ALTER ran).
// The returned StreamStatus values have empty SlotName /
// SourceDSNFingerprint / TargetSchema — the columns are simply not
// present yet on this control table.
func listStreamsLegacy(ctx context.Context, db *sql.DB, engineName string) ([]ir.StreamStatus, error) {
	const q = "SELECT stream_id, source_position, updated_at FROM `" + controlTableName + "`"
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		if isMySQLMissingTableErr(err) {
			return []ir.StreamStatus{}, nil
		}
		return nil, fmt.Errorf("mysql: list streams (legacy): %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []ir.StreamStatus{}
	for rows.Next() {
		var (
			streamID string
			token    string
			updated  time.Time
		)
		if err := rows.Scan(&streamID, &token, &updated); err != nil {
			return nil, fmt.Errorf("mysql: scan streams (legacy): %w", err)
		}
		out = append(out, ir.StreamStatus{
			StreamID:  streamID,
			Position:  ir.Position{Engine: engineName, Token: token},
			UpdatedAt: updated,
		})
	}
	return out, rows.Err()
}

// isMySQLMissingTableErr returns true when err looks like MySQL's
// "Table 'X' doesn't exist" / error 1146. listStreams uses this to
// degrade gracefully when the control table hasn't been created on
// the target yet.
func isMySQLMissingTableErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "doesn't exist") || strings.Contains(msg, "Error 1146")
}

// writePositionTx upserts the (streamID, token, slotName,
// sourceFingerprint, targetSchema) row inside an open transaction.
// Called from the applier's per-change tx after the data write —
// atomicity guarantees that progress and data move together.
//
// Uses the row-alias UPSERT form (MySQL 8.0.20+) for consistency
// with the data-write Insert path. stop_requested_at is left
// untouched: a position write is the streamer making forward
// progress, which must not clear an in-flight stop request.
//
// slotName / sourceFingerprint / targetSchema follow the
// COALESCE-tolerant shape the PG counterpart uses (v0.24.0, v0.25.0,
// v0.25.1 — bridged to MySQL in v0.32.2 to close OBS-1): a non-empty
// value overwrites the row's existing column; an empty value
// preserves whatever was already there. The NULLIF wrapper around
// each placeholder converts the driver-side empty string back to
// SQL NULL on the INSERT path; the COALESCE on each ON DUPLICATE
// KEY UPDATE entry preserves the row's existing value when the
// incoming write supplies the empty (now NULL) string. Mirrors the
// PG counterpart in
// internal/engines/postgres/control_table.go::writePositionTx.
//
// Engines that don't supply these values (today: MySQL's own CDC
// streamer doesn't have a slot concept; the streamer's SetSlotName
// is structural-optional and the applier no-ops empty input)
// produce NULL columns on the row — identical to the pre-v0.32.2
// shape on the MySQL side.
func writePositionTx(ctx context.Context, tx *sql.Tx, streamID, token, slotName, sourceFingerprint, targetSchema string) error {
	const q = "INSERT INTO `" + controlTableName + "` " +
		"(stream_id, source_position, slot_name, source_dsn_fingerprint, target_schema) " +
		"VALUES (?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, '')) " +
		"AS new ON DUPLICATE KEY UPDATE " +
		"source_position = new.source_position, " +
		"slot_name = COALESCE(new.slot_name, `" + controlTableName + "`.slot_name), " +
		"source_dsn_fingerprint = COALESCE(new.source_dsn_fingerprint, `" + controlTableName + "`.source_dsn_fingerprint), " +
		"target_schema = COALESCE(new.target_schema, `" + controlTableName + "`.target_schema)"
	if _, err := tx.ExecContext(ctx, q, streamID, token, slotName, sourceFingerprint, targetSchema); err != nil {
		return fmt.Errorf("mysql: write position: %w", err)
	}
	return nil
}

// readStopRequested returns true when the named stream's row has a
// non-NULL stop_requested_at column. Tolerant of the table being
// absent (returns false, nil) so polling-loop startup races don't
// surface as errors.
//
// Returns (false, nil) when the row doesn't exist — a stop signal
// that hasn't been recorded is, by definition, not present. The
// Streamer's poll loop calls this every few seconds via the
// receiver method on ChangeApplier; the lint pass can't see that
// cross-package usage, hence the nolint.
//
//nolint:unused // called by pipeline poll loop via ChangeApplier receiver
func readStopRequested(ctx context.Context, db *sql.DB, streamID string) (bool, error) {
	const q = "SELECT stop_requested_at IS NOT NULL FROM `" + controlTableName + "` WHERE stream_id = ?"
	var stopRequested bool
	err := db.QueryRowContext(ctx, q, streamID).Scan(&stopRequested)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case isMySQLMissingTableErr(err):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("mysql: read stop flag: %w", err)
	}
	return stopRequested, nil
}

// requestStop flips the stop flag on the named stream's row. Returns
// errStreamNotFound when no row exists (the operator likely typoed
// the stream ID; the CLI surfaces a friendly message).
//
// Idempotent: repeated calls land the same flag (the timestamp
// updates, but the streamer treats any non-NULL value as "stop
// requested" so the repeat is harmless). updated_at is left alone
// so the "age" column in `sync status` continues to reflect real
// apply activity rather than stop-request bookkeeping.
//
// Implementation note: MySQL's go-sql-driver reports RowsAffected
// using `changed-rows` semantics by default — a UPDATE that touches
// the same row with the same new value reports 0 rows affected
// rather than 1. That makes "rows affected = 0 means missing row"
// unreliable for our idempotency contract. We use a SELECT-then-
// UPDATE pair instead. The two queries don't need to be in a
// transaction: the UPDATE is itself atomic, and a stream row can
// only be inserted by the streamer's writePositionTx, which races
// don't matter for here (a transient missing row → operator retry
// → success).
func requestStop(ctx context.Context, db *sql.DB, streamID string) error {
	const existsQ = "SELECT 1 FROM `" + controlTableName + "` WHERE stream_id = ?"
	var dummy int
	switch err := db.QueryRowContext(ctx, existsQ, streamID).Scan(&dummy); {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("%w: %q", errStreamNotFound, streamID)
	case err != nil:
		return fmt.Errorf("mysql: request stop: existence check: %w", err)
	}
	const updateQ = "UPDATE `" + controlTableName + "` SET stop_requested_at = CURRENT_TIMESTAMP WHERE stream_id = ?"
	if _, err := db.ExecContext(ctx, updateQ, streamID); err != nil {
		return fmt.Errorf("mysql: request stop: %w", err)
	}
	return nil
}

// errStreamNotFound is returned by [requestStop] (and thus
// [ChangeApplier.RequestStop]) when no row matches the requested
// stream_id. The CLI string-matches the wrapped engine error rather
// than importing this sentinel, mirroring the slot-not-found shape.
var errStreamNotFound = errors.New("mysql: stream not found")

// clearStopRequested resets the stop_requested_at flag to NULL for
// the named stream. Called by [pipeline.Streamer] at startup so a
// previous `sluice sync stop` doesn't leave a sticky stop signal
// that immediately exits the next `sluice sync start`. Idempotent
// and tolerant of a missing row or table (returns nil) — the next
// position-write commit will populate the row.
func clearStopRequested(ctx context.Context, db *sql.DB, streamID string) error {
	const q = "UPDATE `" + controlTableName + "` SET stop_requested_at = NULL WHERE stream_id = ?"
	if _, err := db.ExecContext(ctx, q, streamID); err != nil {
		// Tolerant of the table being absent — same shape as
		// readPosition. EnsureControlTable runs first, but a
		// brand-new target may have an in-flight schema-apply.
		if isMySQLMissingTableErr(err) {
			return nil
		}
		return fmt.Errorf("mysql: clear stop signal: %w", err)
	}
	return nil
}

// clearStream deletes the named stream's row from the per-target
// control table. Idempotent and tolerant of a missing row or table —
// re-running `--reset-target-data` after a partial failure proceeds
// cleanly. See [ChangeApplier.ClearStream] for the recovery flow.
func clearStream(ctx context.Context, db *sql.DB, streamID string) error {
	const q = "DELETE FROM `" + controlTableName + "` WHERE stream_id = ?"
	if _, err := db.ExecContext(ctx, q, streamID); err != nil {
		if isMySQLMissingTableErr(err) {
			return nil
		}
		return fmt.Errorf("mysql: clear stream: %w", err)
	}
	return nil
}

// readLiveAddedTables returns the comma-separated list parsed into a
// deduplicated, sorted slice of unqualified table names. Empty slice
// when the column is NULL, the row is missing, the column is missing
// (legacy pre-v0.27.0 control table), or the table itself is missing.
// ADR-0034.
//
// The streamer's poll goroutine calls this on every tick; tolerance
// of legacy/missing surfaces means a streamer running against a
// pre-v0.27.0 control table degrades to "no live-adds" rather than
// erroring on every tick.
func readLiveAddedTables(ctx context.Context, db *sql.DB, streamID string) ([]string, error) {
	const q = "SELECT live_added_tables FROM `" + controlTableName + "` WHERE stream_id = ?"
	var raw sql.NullString
	err := db.QueryRowContext(ctx, q, streamID).Scan(&raw)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case isMySQLMissingTableErr(err) || isMySQLUnknownColumnErr(err):
		// Legacy control table without live_added_tables column, or
		// the table itself doesn't exist yet — both surface as "no
		// live-added tables" rather than errors.
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("mysql: read live_added_tables: %w", err)
	}
	if !raw.Valid {
		return nil, nil
	}
	return parseLiveAddedTables(raw.String), nil
}

// recordLiveAddedTable appends tableName to the per-target row's
// live_added_tables column. Idempotent — duplicates are deduplicated
// before write. The orchestrator's add-table --no-drain path calls
// this once per successful run.
//
// The read-modify-write happens under a single transaction with
// SELECT ... FOR UPDATE so concurrent runs serialise. The cdc-state
// row must already exist (the streamer's first applied change creates
// it); the orchestrator's preflight has already verified this via
// ListStreams, but the function still surfaces a clear error if the
// row vanishes between preflight and write (rare; operator
// concurrently ran sync stop --wait + delete).
func recordLiveAddedTable(ctx context.Context, db *sql.DB, streamID, tableName string) error {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return errors.New("mysql: record live-added table: tableName is empty")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mysql: record live-added table: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const selectQ = "SELECT live_added_tables FROM `" + controlTableName + "` WHERE stream_id = ? FOR UPDATE"
	var raw sql.NullString
	if err := tx.QueryRowContext(ctx, selectQ, streamID).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("mysql: record live-added table: stream %q has no cdc-state row (streamer must be running)", streamID)
		}
		return fmt.Errorf("mysql: record live-added table: select for update: %w", err)
	}

	existing := []string{}
	if raw.Valid {
		existing = parseLiveAddedTables(raw.String)
	}
	merged := mergeLiveAddedTables(existing, tableName)
	joined := strings.Join(merged, ",")

	const updateQ = "UPDATE `" + controlTableName + "` SET live_added_tables = ? WHERE stream_id = ?"
	if _, err := tx.ExecContext(ctx, updateQ, joined, streamID); err != nil {
		return fmt.Errorf("mysql: record live-added table: update: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mysql: record live-added table: commit: %w", err)
	}
	return nil
}

// parseLiveAddedTables splits a comma-separated list, trims
// whitespace, drops empties, deduplicates by exact match, and sorts
// the result. The sort is for deterministic comparison ("did the
// poll observe a new value?") and log readability.
func parseLiveAddedTables(raw string) []string {
	if raw == "" {
		return nil
	}
	seen := map[string]struct{}{}
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		seen[p] = struct{}{}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// mergeLiveAddedTables returns a sorted, deduplicated union of
// existing + [tableName].
func mergeLiveAddedTables(existing []string, tableName string) []string {
	merged := make([]string, 0, len(existing)+1)
	merged = append(merged, existing...)
	merged = append(merged, tableName)
	return parseLiveAddedTables(strings.Join(merged, ","))
}

// isMySQLUnknownColumnErr reports whether err looks like MySQL's
// "Unknown column 'X' in 'field list'" / error 1054 — the surface a
// pre-v0.27.0 control table without live_added_tables presents on
// SELECT live_added_tables ... . readLiveAddedTables uses this to
// degrade gracefully.
func isMySQLUnknownColumnErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Unknown column") || strings.Contains(msg, "Error 1054")
}
