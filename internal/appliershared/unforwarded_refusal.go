// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// UnforwardedRefusalColumn is the sluice_cdc_state column that persists an
// [ir.ErrUnforwardedSchemaChange] refusal on the stream's row, so a restarted
// `sync` refuses again instead of re-baselining past the change the refusal
// named (see [ir.UnforwardedRefusalStore]). NULL means "no refusal recorded".
const UnforwardedRefusalColumn = "unforwarded_refusal"

// RecordUnforwardedRefusal stores msg on streamID's row. updateQuery is the
// engine-built `UPDATE … SET unforwarded_refusal = <p1> WHERE stream_id = <p2>`;
// existsQuery (a `SELECT 1 … WHERE stream_id = ?`) is required only when
// cfg.RowsAffectedIsChangedRows is true, exactly as for [RequestStop] — MySQL
// reports 0 rows for an UPDATE that rewrites the same value, so a repeated
// record of the same refusal would otherwise read as a missing row.
//
// A missing row is an error wrapping cfg.ErrStreamNotFound, never a silent
// success: the caller logs that the refusal was NOT persisted, because a
// restart would then take a fresh baseline and accept the change.
func RecordUnforwardedRefusal(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, existsQuery, updateQuery, streamID, msg string) error {
	if cfg.RowsAffectedIsChangedRows {
		var dummy int
		switch err := db.QueryRowContext(ctx, existsQuery, streamID).Scan(&dummy); {
		case errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("%s: record unforwarded-schema-change refusal: %w: %q", cfg.EngineName, cfg.ErrStreamNotFound, streamID)
		case err != nil:
			return fmt.Errorf("%s: record unforwarded-schema-change refusal: existence check: %w", cfg.EngineName, err)
		}
		if _, err := db.ExecContext(ctx, updateQuery, msg, streamID); err != nil {
			return fmt.Errorf("%s: record unforwarded-schema-change refusal: %w", cfg.EngineName, err)
		}
		return nil
	}
	res, err := db.ExecContext(ctx, updateQuery, msg, streamID)
	if err != nil {
		return fmt.Errorf("%s: record unforwarded-schema-change refusal: %w", cfg.EngineName, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: record unforwarded-schema-change refusal: rows affected: %w", cfg.EngineName, err)
	}
	if n == 0 {
		return fmt.Errorf("%s: record unforwarded-schema-change refusal: %w: %q", cfg.EngineName, cfg.ErrStreamNotFound, streamID)
	}
	return nil
}

// ReadUnforwardedRefusal returns the refusal recorded on streamID's row, or
// ok=false when there is none. query is the engine-built
// `SELECT unforwarded_refusal … WHERE stream_id = ?`.
//
// Three absences read as "none", and each is provably so rather than assumed:
//   - no row, or no control table: a refusal is recorded only ON a row, and
//     the CDC anchor creates the row before any change stream opens;
//   - a control table without the column (isMissingColumn): the engines'
//     Record adds the column before writing, so a table that lacks it has
//     never held a record. That covers a control table a pre-column binary
//     created and a `--schema-already-applied` run that skips the ensure.
//
// Any other error is returned: the caller must not start a stream it could
// not check.
func ReadUnforwardedRefusal(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, isMissingColumn func(error) bool, query, streamID string) (msg string, ok bool, err error) {
	var stored sql.NullString
	err = db.QueryRowContext(ctx, query, streamID).Scan(&stored)
	switch {
	case errors.Is(err, sql.ErrNoRows), cfg.IsMissingTable(err), isMissingColumn(err):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("%s: read unforwarded-schema-change refusal: %w", cfg.EngineName, err)
	}
	if !stored.Valid {
		return "", false, nil
	}
	return stored.String, true, nil
}
