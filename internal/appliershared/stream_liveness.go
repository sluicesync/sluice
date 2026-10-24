// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// StreamLivenessColumn is the sluice_cdc_state column a running stream stamps
// its liveness into ([ir.StreamLivenessStore]). Only what applies changes
// under the stream id writes it — the stream's attempt-start and stop stamps,
// and every position write an applier commits once it knows the column is
// there (the stream's, and a sync-from-backup broker's), in the same
// statement as the position — never a stop request, a table-set edit, an
// add-table, or anything else that touches the row.
//
// Its value is the control-table server's clock in Unix milliseconds, a plain
// BIGINT, so no column type, session time zone or ON UPDATE clause can move
// or reinterpret it (the v0.163.4 lessons: a MySQL ON UPDATE CURRENT_TIMESTAMP
// column moves on every write to the row, and a Postgres naive TIMESTAMP's
// digits depend on the session zone). NULL means no release that stamps
// liveness has stamped the row; [StreamStoppedMillis] is the stopped marker.
const StreamLivenessColumn = "stream_alive_ms"

// StreamStoppedMillis is the stopped marker: the value a stream stamps when
// it stops. No running stamp can equal it — it is the Unix epoch, and a
// running stamp is the server's clock now.
const StreamStoppedMillis = 0

// StreamLivenessQueries are one engine's statements for the liveness column.
// Each engine renders them with its own quoting, placeholders and clock
// expression (the server's now in Unix milliseconds); the shared helpers
// below own the control flow.
type StreamLivenessQueries struct {
	// Read selects the column and the server's now in milliseconds for one
	// stream_id.
	Read string
	// Stamp sets the column to the server's now, over a stopped marker too.
	Stamp string
	// Stop sets the column to the stopped marker.
	Stop string
	// IsMissingColumn classifies the dialect's "no such column" error.
	IsMissingColumn func(error) bool
}

// ReadStreamLiveness implements [ir.StreamLivenessStore.ReadStreamLiveness].
// A stored value below the stopped marker cannot have been written by a
// stamp, so it is an error rather than a guess.
func ReadStreamLiveness(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, q StreamLivenessQueries, streamID string) (ir.StreamLiveness, error) {
	var (
		stored sql.NullInt64
		nowMs  int64
	)
	err := db.QueryRowContext(ctx, q.Read, streamID).Scan(&stored, &nowMs)
	switch {
	case errors.Is(err, sql.ErrNoRows), cfg.IsMissingTable(err):
		return ir.StreamLiveness{}, nil
	case q.IsMissingColumn(err):
		return ir.StreamLiveness{Found: true}, nil
	case err != nil:
		return ir.StreamLiveness{}, fmt.Errorf("%s: read %s: %w", cfg.EngineName, StreamLivenessColumn, err)
	}
	l := ir.StreamLiveness{Found: true, Recorded: stored.Valid, Now: time.UnixMilli(nowMs).UTC()}
	switch {
	case !stored.Valid:
	case stored.Int64 == StreamStoppedMillis:
		l.Stopped = true
	case stored.Int64 < StreamStoppedMillis:
		return ir.StreamLiveness{}, fmt.Errorf("%s: read %s of stream %q: %d is not a liveness stamp",
			cfg.EngineName, StreamLivenessColumn, streamID, stored.Int64)
	default:
		l.AliveAt = time.UnixMilli(stored.Int64).UTC()
	}
	return l, nil
}

// StampStreamLiveness runs one liveness stamp (q.Stamp or q.Stop) and reports
// whether a row took it. A missing row is not an error —
// an UPDATE that matches nothing — and neither is a missing control table: a
// stream's first cold start creates its row later. A row counts as stamped
// on MySQL's changed-rows RowsAffected too, because every running stamp
// writes a new clock reading (and the stopped marker's report is unused).
func StampStreamLiveness(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, stmt, streamID string) (stamped bool, err error) {
	res, err := db.ExecContext(ctx, stmt, streamID)
	switch {
	case cfg.IsMissingTable(err):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("%s: stamp %s: %w", cfg.EngineName, StreamLivenessColumn, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("%s: stamp %s: %w", cfg.EngineName, StreamLivenessColumn, err)
	}
	return n > 0, nil
}
