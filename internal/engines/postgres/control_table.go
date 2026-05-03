package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// controlTableName is the per-target table that holds CDC stream
// positions. ADR-0007 picks the name; v1 honors it verbatim.
const controlTableName = "sluice_cdc_state"

// ensureControlTable creates the per-target sluice_cdc_state table
// in the named schema if it doesn't exist. Idempotent — second-and-
// later calls are no-ops courtesy of CREATE TABLE IF NOT EXISTS.
//
// Postgres has namespaced schemas, so the table lives in the schema
// passed in (taken from the DSN's `schema` query parameter, default
// "public"). The Streamer reads the schema from the engine config
// and threads it through.
func ensureControlTable(ctx context.Context, db *sql.DB, schema string) error {
	tableRef := quoteIdent(schema) + "." + quoteIdent(controlTableName)
	ddl := `
		CREATE TABLE IF NOT EXISTS ` + tableRef + ` (
			stream_id       VARCHAR(255) NOT NULL,
			source_position TEXT         NOT NULL,
			updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (stream_id)
		)`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("postgres: ensure control table: %w", err)
	}
	return nil
}

// readPosition returns the persisted source_position for streamID,
// or ok=false when no row exists. Engine on the returned Position
// is set by the caller — only the Token survives across runs.
//
// Tolerant of the control table being absent: a missing-relation
// error is reported as ok=false (same as "no row") so dry-run
// flows that skip EnsureControlTable still work. Missing-relation
// detection uses the same string-match helper as the schema reader's
// PostGIS lookup.
func readPosition(ctx context.Context, db *sql.DB, schema, streamID string) (token string, ok bool, err error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(controlTableName)
	q := "SELECT source_position FROM " + tableRef + " WHERE stream_id = $1"
	row := db.QueryRowContext(ctx, q, streamID)
	switch err := row.Scan(&token); {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case isUndefinedRelationErr(err):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("postgres: read position: %w", err)
	}
	return token, true, nil
}

// listStreams returns every row in the per-target control table.
// Tolerant of the table being absent (treated as "no streams") so
// `sluice sync status` works against a target that hasn't been a
// CDC destination yet.
//
// The Position values returned set Engine to the
// engine-specific identifier the binlog or pgoutput readers use,
// since the token alone is opaque without that context. The CLI
// is the consumer; it doesn't strictly need Engine populated, but
// keeping the shape consistent with ReadPosition's return matters
// for any future caller that might.
func listStreams(ctx context.Context, db *sql.DB, schema, engineName string) ([]ir.StreamStatus, error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(controlTableName)
	q := "SELECT stream_id, source_position, updated_at FROM " + tableRef
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		// Best-effort tolerance: missing-relation = no streams.
		// The schema reader uses the same string-match approach for
		// PostGIS detection (see schema_reader.go).
		if isUndefinedRelationErr(err) {
			return []ir.StreamStatus{}, nil
		}
		return nil, fmt.Errorf("postgres: list streams: %w", err)
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
			return nil, fmt.Errorf("postgres: scan streams: %w", err)
		}
		out = append(out, ir.StreamStatus{
			StreamID:  streamID,
			Position:  ir.Position{Engine: engineName, Token: token},
			UpdatedAt: updated,
		})
	}
	return out, rows.Err()
}

// writePositionTx upserts the (streamID, token) row inside an open
// transaction. Called from the applier's per-change tx after the
// data write; same atomicity guarantee as the MySQL counterpart.
//
// The updated_at column is refreshed on every upsert via
// CURRENT_TIMESTAMP — diagnostic info for operators inspecting the
// control table by hand.
func writePositionTx(ctx context.Context, tx *sql.Tx, schema, streamID, token string) error {
	tableRef := quoteIdent(schema) + "." + quoteIdent(controlTableName)
	q := "INSERT INTO " + tableRef + " (stream_id, source_position, updated_at) " +
		"VALUES ($1, $2, CURRENT_TIMESTAMP) " +
		"ON CONFLICT (stream_id) DO UPDATE SET " +
		"source_position = EXCLUDED.source_position, " +
		"updated_at = EXCLUDED.updated_at"
	if _, err := tx.ExecContext(ctx, q, streamID, token); err != nil {
		return fmt.Errorf("postgres: write position: %w", err)
	}
	return nil
}
