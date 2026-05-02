package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
func ensureControlTable(ctx context.Context, db *sql.DB) error {
	const ddl = `
		CREATE TABLE IF NOT EXISTS ` + "`" + controlTableName + "`" + ` (
			stream_id       VARCHAR(255) NOT NULL,
			source_position TEXT         NOT NULL,
			updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
				ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (stream_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("mysql: ensure control table: %w", err)
	}
	return nil
}

// readPosition returns the persisted source_position for streamID,
// or ok=false when no row exists. The Engine field of the returned
// position is set to "mysql" by the caller — only the Token survives
// across runs (the engine reading is implicitly the engine that
// wrote).
func readPosition(ctx context.Context, db *sql.DB, streamID string) (token string, ok bool, err error) {
	const q = "SELECT source_position FROM `" + controlTableName + "` WHERE stream_id = ?"
	row := db.QueryRowContext(ctx, q, streamID)
	switch err := row.Scan(&token); {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("mysql: read position: %w", err)
	}
	return token, true, nil
}

// writePositionTx upserts the (streamID, token) row inside an open
// transaction. Called from the applier's per-change tx after the
// data write — atomicity guarantees that progress and data move
// together.
//
// Uses the row-alias UPSERT form (MySQL 8.0.20+) for consistency
// with the data-write Insert path.
func writePositionTx(ctx context.Context, tx *sql.Tx, streamID, token string) error {
	const q = "INSERT INTO `" + controlTableName + "` (stream_id, source_position) VALUES (?, ?) " +
		"AS new ON DUPLICATE KEY UPDATE source_position = new.source_position"
	if _, err := tx.ExecContext(ctx, q, streamID, token); err != nil {
		return fmt.Errorf("mysql: write position: %w", err)
	}
	return nil
}
