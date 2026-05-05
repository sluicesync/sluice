package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
// stop_requested_at is added via a detect-then-ALTER on existing
// tables. ADD COLUMN IF NOT EXISTS landed in MySQL 8.0.29; sluice
// supports 8.0+ broadly, so the conservative path queries
// information_schema.COLUMNS first and only ALTERs when the column
// is missing. Existing rows keep their data; the new column starts
// NULL.
func ensureControlTable(ctx context.Context, db *sql.DB) error {
	const ddl = `
		CREATE TABLE IF NOT EXISTS ` + "`" + controlTableName + "`" + ` (
			stream_id         VARCHAR(255) NOT NULL,
			source_position   TEXT         NOT NULL,
			updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
				ON UPDATE CURRENT_TIMESTAMP,
			stop_requested_at TIMESTAMP    NULL,
			PRIMARY KEY (stream_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("mysql: ensure control table: %w", err)
	}
	return ensureStopRequestedColumn(ctx, db)
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
func listStreams(ctx context.Context, db *sql.DB, engineName string) ([]ir.StreamStatus, error) {
	const q = "SELECT stream_id, source_position, updated_at FROM `" + controlTableName + "`"
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		// MySQL surfaces missing-table as error 1146; the friendly
		// fallback treats that as "no streams". String-match keeps
		// the helper driver-version-tolerant.
		if isMySQLMissingTableErr(err) {
			return []ir.StreamStatus{}, nil
		}
		return nil, fmt.Errorf("mysql: list streams: %w", err)
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
			return nil, fmt.Errorf("mysql: scan streams: %w", err)
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

// writePositionTx upserts the (streamID, token) row inside an open
// transaction. Called from the applier's per-change tx after the
// data write — atomicity guarantees that progress and data move
// together.
//
// Uses the row-alias UPSERT form (MySQL 8.0.20+) for consistency
// with the data-write Insert path. stop_requested_at is left
// untouched: a position write is the streamer making forward
// progress, which must not clear an in-flight stop request.
func writePositionTx(ctx context.Context, tx *sql.Tx, streamID, token string) error {
	const q = "INSERT INTO `" + controlTableName + "` (stream_id, source_position) VALUES (?, ?) " +
		"AS new ON DUPLICATE KEY UPDATE source_position = new.source_position"
	if _, err := tx.ExecContext(ctx, q, streamID, token); err != nil {
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
