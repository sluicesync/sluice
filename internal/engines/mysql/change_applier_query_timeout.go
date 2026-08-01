// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Crash-recovery record for the ADR-0182 query-timeout raise — the sync side
//
// The opt-in PlanetScale query-timeout raise (ADR-0182) modifies a
// keyspace-wide setting for the duration of the cold-start bulk copy and must
// restore it afterwards — even across a crash. The migrate path records that in
// a dedicated column of the sluice_migrate_state HEADER row (see
// migration_state_query_timeout.go). The `sync` cold-start has NO migrate-state
// store, and its own position row (sluice_cdc_state) does not yet exist when the
// raise happens: the raise runs BEFORE the cold-start copy, so ReadPosition
// still reports found=false and sluice_cdc_state.source_position (LONGTEXT NOT
// NULL) can't be seeded with a placeholder without risking the resume path
// misreading it.
//
// So the sync side gets its OWN dedicated table, sluice_cdc_query_timeout_raise,
// keyed by stream_id, created alongside sluice_cdc_state inside the applier's
// [ChangeApplier.EnsureControlTable] (item 111 phase 3). Having no other NOT
// NULL columns lets Record upsert freely without a pre-existing row (unlike the
// migrate path's UPDATE-only recorder, whose header row is guaranteed to exist).
// The JSON envelope [psQueryTimeoutRaiseRecord] is shared with the migrate
// recorder: an EMPTY Previous is meaningful (the keyspace was at its default
// when we raised it), which the revert maps back to the documented default.
//
// MySQL-only by construction: the raise only ever targets a PlanetScale/Vitess
// keyspace, so only the MySQL engine implements [ir.QueryTimeoutRaiseRecorder].
// Postgres (never a PlanetScale target) is untouched.

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// Compile-time proof the MySQL applier satisfies the pipeline-facing recorder,
// the sync-side sibling of the migrate store's assertion.
var _ ir.QueryTimeoutRaiseRecorder = (*ChangeApplier)(nil)

// cdcQueryTimeoutRaiseTableName is the per-target table holding the sync
// cold-start's ADR-0182 crash-recovery record, keyed by stream_id. Dedicated
// (not a column on sluice_cdc_state) because the raise precedes that table's
// first position write — see this file's header.
const cdcQueryTimeoutRaiseTableName = "sluice_cdc_query_timeout_raise"

// cdcQueryTimeoutRaiseTableDDL renders the CREATE — the single source for both
// [ensureCDCQueryTimeoutRaiseTable] and the bootstrap printer
// ([Engine.ControlTableDDL] / `sluice control-tables ddl`, ADR-0165), so a
// safe-migrations branch bootstrapped via `sluice deploy-ddl` gets this table
// too. ps_query_timeout_raise is NOT NULL (its mere presence as a row means "a
// raise is recorded"; the row is DELETEd on revert), carrying the JSON envelope.
func cdcQueryTimeoutRaiseTableDDL(controlKeyspace string) string {
	return `CREATE TABLE IF NOT EXISTS ` + controlTableRef(controlKeyspace, cdcQueryTimeoutRaiseTableName) + ` (
	stream_id              VARCHAR(255) ` + controlIdentifierCollateClause + ` NOT NULL,
	ps_query_timeout_raise TEXT         NOT NULL,
	PRIMARY KEY (stream_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
}

// ensureCDCQueryTimeoutRaiseTable creates sluice_cdc_query_timeout_raise if it
// doesn't exist. Idempotent, ADDITIVE (never touches sluice_cdc_state or any
// existing data), and detect-then-create for the SAME reason as
// [ensureControlTable]: on a PlanetScale safe-migrations branch the
// exists-already path must issue no DDL at all, and a genuinely-needed CREATE
// that gets refused is the coded bootstrap refusal
// (SLUICE-E-PS-DIRECT-DDL-BLOCKED). Called from
// [ChangeApplier.EnsureControlTable] so the table exists before any raise.
func ensureCDCQueryTimeoutRaiseTable(ctx context.Context, db *sql.DB, controlKeyspace string) error {
	exists, err := controlTableExists(ctx, db, controlKeyspace, cdcQueryTimeoutRaiseTableName)
	if err != nil {
		return fmt.Errorf("mysql: ensure cdc query-timeout raise table: %w", err)
	}
	if !exists {
		ddl := cdcQueryTimeoutRaiseTableDDL(controlKeyspace)
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("mysql: ensure cdc query-timeout raise table: %w", wrapControlTableBootstrapError(wrapDDLError(err), ddl))
		}
	} else {
		warnLegacyControlTableCollation(ctx, db, controlKeyspace, cdcQueryTimeoutRaiseTableName, "stream_id")
	}
	return nil
}

// ReadQueryTimeoutRaise implements [ir.QueryTimeoutRaiseRecorder]. The
// migrationID parameter is the STREAM ID here (the opaque record key). A
// missing table or row (pre-Ensure inspection, or simply no raise recorded)
// reads as ok=false — never an error, mirroring the migrate store's tolerance.
func (a *ChangeApplier) ReadQueryTimeoutRaise(ctx context.Context, streamID string) (previous string, ok bool, err error) {
	if streamID == "" {
		return "", false, errors.New("mysql: applier: read query-timeout raise: streamID is empty")
	}
	q := "SELECT ps_query_timeout_raise FROM " + controlTableRef(a.controlKeyspace, cdcQueryTimeoutRaiseTableName) + " WHERE stream_id = ?"
	var raw sql.NullString
	switch err := a.db.QueryRowContext(ctx, q, streamID).Scan(&raw); {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case isMySQLMissingTableErr(err):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("mysql: applier: read query-timeout raise: %w", err)
	}
	if !raw.Valid {
		// The column is NOT NULL, so this is unreachable in practice; treated as
		// "no raise recorded" defensively rather than surfacing a phantom error.
		return "", false, nil
	}
	var rec psQueryTimeoutRaiseRecord
	if err := json.Unmarshal([]byte(raw.String), &rec); err != nil {
		return "", false, fmt.Errorf("mysql: applier: decode query-timeout raise record for stream %q: %w", streamID, err)
	}
	return rec.Previous, true, nil
}

// RecordQueryTimeoutRaise implements [ir.QueryTimeoutRaiseRecorder]. UPSERT (not
// UPDATE): the dedicated table has no other NOT NULL columns, so the row need
// NOT pre-exist — unlike the migrate store's UPDATE-only recorder, whose header
// row is guaranteed present. The row-alias / VALUES() spelling follows the same
// flavor gate the data-write path uses.
func (a *ChangeApplier) RecordQueryTimeoutRaise(ctx context.Context, streamID, previous string) error {
	if streamID == "" {
		return errors.New("mysql: applier: record query-timeout raise: streamID is empty")
	}
	encoded, err := json.Marshal(psQueryTimeoutRaiseRecord{Previous: previous})
	if err != nil {
		return fmt.Errorf("mysql: applier: encode query-timeout raise record: %w", err)
	}
	ref := controlTableRef(a.controlKeyspace, cdcQueryTimeoutRaiseTableName)
	q := "INSERT INTO " + ref + " (stream_id, ps_query_timeout_raise) VALUES (?, ?)" +
		a.upsert.clauseOpen() +
		"ps_query_timeout_raise = " + a.upsert.newRowRef("ps_query_timeout_raise")
	if _, err := a.db.ExecContext(ctx, q, streamID, string(encoded)); err != nil {
		return fmt.Errorf("mysql: applier: record query-timeout raise: %w", err)
	}
	return nil
}

// ClearQueryTimeoutRaise implements [ir.QueryTimeoutRaiseRecorder]. DELETE the
// row (the dedicated table means the row's absence IS "no raise recorded").
// Idempotent and tolerant of a missing table/row.
func (a *ChangeApplier) ClearQueryTimeoutRaise(ctx context.Context, streamID string) error {
	if streamID == "" {
		return errors.New("mysql: applier: clear query-timeout raise: streamID is empty")
	}
	q := "DELETE FROM " + controlTableRef(a.controlKeyspace, cdcQueryTimeoutRaiseTableName) + " WHERE stream_id = ?"
	if _, err := a.db.ExecContext(ctx, q, streamID); err != nil && !isMySQLMissingTableErr(err) {
		return fmt.Errorf("mysql: applier: clear query-timeout raise: %w", err)
	}
	return nil
}
