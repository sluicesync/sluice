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

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// controlTableName is the per-target table that holds CDC stream
// positions. ADR-0007 picks the name; v1 honors it verbatim. A
// configurable prefix lands as part of roadmap §10. Aliased from
// appliershared so both engines share one source of truth.
const controlTableName = appliershared.ControlTableName

// shardConsolidationLeaseTableName is the ADR-0054 per-target control
// table that holds the cross-shard DDL-coordination lease (one row per
// consolidated target table). See ensureShardConsolidationLeaseTable.
const shardConsolidationLeaseTableName = appliershared.ShardConsolidationLeaseTableName

// controlCfg is the ADR-0081 tier-c dialect seam for the shared
// control-table CRUD in internal/appliershared: the engine-constant
// leaves (error prefix, the Error-1146 missing-table classifier, the
// stream-not-found sentinel, the changed-rows RowsAffected wart) the
// shared skeletons need. SQL text is built at each call site —
// quoting and placeholder style stay in this package.
var controlCfg = &appliershared.ControlTableConfig{
	EngineName:        "mysql",
	IsMissingTable:    isMySQLMissingTableErr,
	ErrStreamNotFound: errStreamNotFound,

	// go-sql-driver defaults to changed-rows RowsAffected semantics —
	// an UPDATE that rewrites a row with the same value reports 0 —
	// so RequestStop's missing-row detection uses the shared
	// SELECT-then-UPDATE shape rather than a rows-affected check.
	RowsAffectedIsChangedRows: true,
}

// controlTableRef returns the backtick-quoted reference to a control table,
// keyspace-qualified when controlKeyspace is non-empty (the sidecar-keyspace
// feature: the CDC control tables live in an UNSHARDED keyspace, separate
// from the sharded DATA keyspace, so a sharded PlanetScale/Vitess target —
// which requires a vindex on every table in the sharded keyspace — accepts
// them). Each identifier is quoted in its OWN pair of backticks:
// `ctl`.`sluice_cdc_state`, NEVER `ctl.sluice_cdc_state` (a single, wrong
// identifier). Empty controlKeyspace reproduces the pre-feature bare name
// byte-for-byte, so the default single-keyspace path is unchanged.
func controlTableRef(controlKeyspace, table string) string {
	if controlKeyspace == "" {
		return "`" + table + "`"
	}
	return "`" + controlKeyspace + "`.`" + table + "`"
}

// controlSchemaPredicate returns the information_schema TABLE_SCHEMA right-hand
// side (the value in `TABLE_SCHEMA = …`) plus any bound argument it needs. The
// control-table column-existence probes scope to one database; with a sidecar
// control keyspace they must inspect THAT keyspace's information_schema rows,
// not the connection's default database. Empty controlKeyspace → the bare
// `DATABASE()` form with no extra arg (byte-identical to the single-keyspace
// path); set → a `?` placeholder BOUND to the keyspace name — bound, not
// interpolated, so an operator-supplied name can never reshape the query. The
// predicate is always the first term in these WHERE clauses, so its arg is
// prepended ahead of the table-name arg.
func controlSchemaPredicate(controlKeyspace string) (rhs string, args []any) {
	if controlKeyspace == "" {
		return "DATABASE()", nil
	}
	return "?", []any{controlKeyspace}
}

// validateControlKeyspace refuses a --control-keyspace value that is not a
// plain Vitess/PlanetScale keyspace identifier. A control keyspace flows into
// backtick-quoted identifiers ([controlTableRef]) in every control-table
// statement, so a name containing a backtick (or other non-identifier byte)
// would silently corrupt that quoting; per the loud-failure tenet we reject it
// up front rather than emit a malformed statement. Empty is the "unset"
// sentinel and is accepted (it means "no qualification").
//
// Hyphens ARE allowed: a PlanetScale database's default unsharded keyspace is
// named after the database (e.g. `sluice-ck-dst`), which legally contains
// hyphens — the intended default sidecar. Genuinely illegal input (backtick,
// whitespace, dot, quote, semicolon) is still refused loudly.
func validateControlKeyspace(name string) error {
	if name == "" {
		return nil
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '_',
			r == '-':
			// ok
		default:
			return fmt.Errorf("must be a plain identifier [A-Za-z0-9_-]; got %q", name)
		}
	}
	return nil
}

// ensureControlTable creates the per-target sluice_cdc_state table
// if it doesn't exist. Idempotent — second-and-later calls detect the
// table and issue no DDL at all.
//
// The table lives in the connection's default database (DBName from
// the DSN). MySQL has a flat namespace so no schema-qualification is
// needed; the database is implicit in the connection. The sidecar-
// keyspace feature is the one exception: a non-empty controlKeyspace
// keyspace-qualifies the DDL (`ks`.`sluice_cdc_state`) so the table is
// created in a separate UNSHARDED keyspace on a sharded PlanetScale/
// Vitess target (see [controlTableRef]).
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
//   - source_position TEXT → LONGTEXT widen (roadmap item 65a) — the
//     engine-opaque position token is a GTID/VGTID set that can exceed
//     TEXT's 64 KB (≈1000 server-UUID entries, or a very heavily
//     sharded VGTID); sluice_cdc_schema_history.anchor_position was
//     already LONGTEXT for exactly this reason. See
//     [ensureLongTextPositionColumn] for the widen mechanics and the
//     PlanetScale safe-migrations refusal shape.
//
// Detect-then-create (roadmap item 66, mirroring
// MigrationStateStore.EnsureControlTable's v0.99.248 gate): Vitess
// under PlanetScale safe migrations refuses every direct DDL STATEMENT
// (Error 1105 "direct DDL is disabled") regardless of whether the
// table exists, so the exists-already path must issue no DDL at all —
// that is what lets a sync stream start against a safe-migrations
// branch whose control tables were bootstrapped via `sluice
// deploy-ddl`. When the CREATE is genuinely needed and refused, the
// failure is the coded bootstrap refusal
// (SLUICE-E-PS-DIRECT-DDL-BLOCKED) naming that channel.
func ensureControlTable(ctx context.Context, db *sql.DB, controlKeyspace string) error {
	exists, err := controlTableExists(ctx, db, controlKeyspace, controlTableName)
	if err != nil {
		return fmt.Errorf("mysql: ensure control table: %w", err)
	}
	if !exists {
		ddl := controlTableDDL(controlKeyspace)
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("mysql: ensure control table: %w", wrapControlTableBootstrapError(wrapDDLError(err), ddl))
		}
	}
	if err := ensureStopRequestedColumn(ctx, db, controlKeyspace); err != nil {
		return err
	}
	if err := ensureLiveAddedTablesColumn(ctx, db, controlKeyspace); err != nil {
		return err
	}
	if err := ensureCrossEngineParityColumn(ctx, db, controlKeyspace, "slot_name", "VARCHAR(255) NULL"); err != nil {
		return err
	}
	if err := ensureCrossEngineParityColumn(ctx, db, controlKeyspace, "source_dsn_fingerprint", "VARCHAR(255) NULL"); err != nil {
		return err
	}
	if err := ensureCrossEngineParityColumn(ctx, db, controlKeyspace, "target_schema", "VARCHAR(255) NULL"); err != nil {
		return err
	}
	// rows_applied (ADR-0156 phase 2): the lifetime cumulative
	// row-level-DML-applied counter surfaced in `sync start`'s live
	// panel. Additive on the SAME detect-then-ALTER path as the
	// cross-engine parity columns above; NOT NULL DEFAULT 0 backfills
	// legacy rows to 0 (an honest cumulative starting point — pre-upgrade
	// applies were never tracked).
	if err := ensureCrossEngineParityColumn(ctx, db, controlKeyspace, "rows_applied", "BIGINT NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	// source_position TEXT → LONGTEXT widen (roadmap item 65a): tables
	// created by a pre-widen binary carry the 64 KB TEXT column.
	return ensureLongTextPositionColumn(ctx, db, controlKeyspace, controlTableName, "source_position", "LONGTEXT NOT NULL")
}

// controlTableDDL renders the sluice_cdc_state CREATE statement — the
// single source for both [ensureControlTable] and the bootstrap
// printer ([Engine.ControlTableDDL] / `sluice control-tables ddl`,
// ADR-0165).
func controlTableDDL(controlKeyspace string) string {
	return `CREATE TABLE IF NOT EXISTS ` + controlTableRef(controlKeyspace, controlTableName) + ` (
	stream_id              VARCHAR(255) NOT NULL,
	source_position        LONGTEXT     NOT NULL,
	updated_at             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
		ON UPDATE CURRENT_TIMESTAMP,
	stop_requested_at      TIMESTAMP    NULL,
	slot_name              VARCHAR(255) NULL,
	source_dsn_fingerprint VARCHAR(255) NULL,
	target_schema          VARCHAR(255) NULL,
	rows_applied           BIGINT       NOT NULL DEFAULT 0,
	PRIMARY KEY (stream_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
}

// controlTableExists reports whether a control table is already
// present, scoped to the connection's default database or the sidecar
// control keyspace (see [controlSchemaPredicate]) — the detect half of
// the detect-then-create gate the safe-migrations constraint imposes
// on every control-table ensure.
func controlTableExists(ctx context.Context, db *sql.DB, controlKeyspace, table string) (bool, error) {
	schemaRHS, schemaArgs := controlSchemaPredicate(controlKeyspace)
	q := "SELECT COUNT(*) FROM information_schema.TABLES " +
		"WHERE TABLE_SCHEMA = " + schemaRHS + " AND TABLE_NAME = ?"
	args := append(append([]any{}, schemaArgs...), table)
	var n int
	if err := db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return false, fmt.Errorf("detect %s: %w", table, err)
	}
	return n > 0, nil
}

// shardConsolidationLeaseRow aliases the shared lease-row mirror of
// ir.ShardConsolidationLeaseRow (ADR-0081 tier c); the
// shard_consolidation_lease.go file converts to the pipeline-facing
// shape.
type shardConsolidationLeaseRow = appliershared.ShardLeaseRow

// tryAcquireShardLease retries tryAcquireShardLeaseOnce on an InnoDB
// deadlock (1213). Concurrent shards racing to INSERT the same ABSENT
// lease row deadlock on the gap lock taken by SELECT ... FOR UPDATE; the
// victim transaction is rolled back, so a retry re-runs the acquire — on
// the next pass the row exists and we take the held/takeover path instead
// of the racing INSERT. Without this a contended shard fails spuriously
// (reproduced by TestPhase2e_MySQL_3ShardContention_*; classifyApplierError
// already treats 1213 as retriable on the apply path).
func tryAcquireShardLease(ctx context.Context, db *sql.DB, controlKeyspace, tableName, streamID string, expires time.Time) (acquired bool, current shardConsolidationLeaseRow, err error) {
	const maxAttempts = 8
	backoff := 5 * time.Millisecond
	for attempt := 1; ; attempt++ {
		acquired, current, err = tryAcquireShardLeaseOnce(ctx, db, controlKeyspace, tableName, streamID, expires)
		if err == nil || attempt >= maxAttempts || !isMySQLDeadlock(err) {
			return acquired, current, err
		}
		select {
		case <-ctx.Done():
			return false, current, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 200*time.Millisecond {
			backoff *= 2
		}
	}
}

// tryAcquireShardLeaseOnce is a single acquire attempt. The acquire wins
// iff one of:
//
//   - The row is ABSENT (the INSERT lands cleanly), or
//   - The row's lease_expires_at <= now() AND applied_at IS NULL
//     (EXPIRED takeover-eligible).
//
// MySQL has no INSERT ... ON CONFLICT WHERE form (unlike PG). We use
// a SELECT ... FOR UPDATE inside a single tx to serialise concurrent
// acquires, then INSERT / UPDATE / no-op based on the loaded row's
// state. The row-level lock on the lease row scopes contention to
// the consolidated target table, so other tables proceed in parallel.
func tryAcquireShardLeaseOnce(ctx context.Context, db *sql.DB, controlKeyspace, tableName, streamID string, expires time.Time) (acquired bool, current shardConsolidationLeaseRow, err error) {
	leaseRef := controlTableRef(controlKeyspace, shardConsolidationLeaseTableName)
	tx, beginErr := db.BeginTx(ctx, nil)
	if beginErr != nil {
		return false, shardConsolidationLeaseRow{}, fmt.Errorf("mysql: lease acquire: begin tx: %w", beginErr)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	selectQ := "SELECT target_table_full_name, COALESCE(lease_holder_stream_id, ''), " +
		"lease_expires_at, COALESCE(ddl_text, ''), COALESCE(ddl_checksum, ''), " +
		"applied_schema_version, applied_at, anchor_position, source_engine " +
		"FROM " + leaseRef + " " +
		"WHERE target_table_full_name = ? FOR UPDATE"
	var row shardConsolidationLeaseRow
	scanErr := appliershared.ScanShardLeaseRow(tx.QueryRowContext(ctx, selectQ, tableName), &row)
	switch {
	case errors.Is(scanErr, sql.ErrNoRows):
		// ABSENT: insert fresh row.
		insertQ := "INSERT INTO " + leaseRef + " " +
			"(target_table_full_name, lease_holder_stream_id, lease_expires_at, applied_schema_version) " +
			"VALUES (?, ?, ?, 0)"
		if _, execErr := tx.ExecContext(ctx, insertQ, tableName, streamID, expires); execErr != nil {
			return false, shardConsolidationLeaseRow{}, fmt.Errorf("mysql: lease acquire: insert: %w", execErr)
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return false, shardConsolidationLeaseRow{}, fmt.Errorf("mysql: lease acquire: commit: %w", commitErr)
		}
		committed = true
		return true, shardConsolidationLeaseRow{
			TargetTableFullName: tableName,
			LeaseHolderStreamID: streamID,
			LeaseExpiresAt:      sql.NullTime{Time: expires, Valid: true},
		}, nil
	case scanErr != nil:
		return false, shardConsolidationLeaseRow{}, fmt.Errorf("mysql: lease acquire: select: %w", scanErr)
	}
	// Row exists. Classify state:
	//   APPLIED (applied_at NOT NULL) → contended; return current
	//     (peer should advance via Observe, not re-acquire).
	//   HELD (lease_expires_at > now()) → contended; return current.
	//   EXPIRED (lease_expires_at <= now() AND applied_at IS NULL) →
	//     takeover-eligible; UPDATE holder + expires; preserve
	//     ddl_text so the caller's probe-and-record can read it.
	if row.AppliedAt.Valid {
		// Commit the (read-only) tx and return.
		if commitErr := tx.Commit(); commitErr != nil {
			return false, shardConsolidationLeaseRow{}, fmt.Errorf("mysql: lease acquire: commit (applied): %w", commitErr)
		}
		committed = true
		return false, row, nil
	}
	// We must compare lease_expires_at against the target's wall-clock
	// "now()". MySQL CURRENT_TIMESTAMP is the source of truth here so
	// peer streams agree on expiry.
	var nowTime time.Time
	if err := tx.QueryRowContext(ctx, "SELECT CURRENT_TIMESTAMP").Scan(&nowTime); err != nil {
		return false, shardConsolidationLeaseRow{}, fmt.Errorf("mysql: lease acquire: read now: %w", err)
	}
	if row.LeaseExpiresAt.Valid && row.LeaseExpiresAt.Time.After(nowTime) {
		// HELD by another stream.
		if commitErr := tx.Commit(); commitErr != nil {
			return false, shardConsolidationLeaseRow{}, fmt.Errorf("mysql: lease acquire: commit (held): %w", commitErr)
		}
		committed = true
		return false, row, nil
	}
	// EXPIRED takeover-eligible. UPDATE holder + expires; PRESERVE
	// ddl_text so probe-and-record on the caller side has the prior
	// holder's recorded text.
	updateQ := "UPDATE " + leaseRef + " SET " +
		"lease_holder_stream_id = ?, lease_expires_at = ? " +
		"WHERE target_table_full_name = ?"
	if _, execErr := tx.ExecContext(ctx, updateQ, streamID, expires, tableName); execErr != nil {
		return false, shardConsolidationLeaseRow{}, fmt.Errorf("mysql: lease acquire: takeover update: %w", execErr)
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return false, shardConsolidationLeaseRow{}, fmt.Errorf("mysql: lease acquire: commit (takeover): %w", commitErr)
	}
	committed = true
	// Return the row we acquired, with prior holder's ddl_text
	// preserved in case the caller needs it for probe-and-record.
	row.LeaseHolderStreamID = streamID
	row.LeaseExpiresAt = sql.NullTime{Time: expires, Valid: true}
	return true, row, nil
}

// heartbeatShardLease extends lease_expires_at iff the row is still
// held by streamID.
func heartbeatShardLease(ctx context.Context, db *sql.DB, controlKeyspace, tableName, streamID string, expires time.Time) (extended bool, err error) {
	q := "UPDATE " + controlTableRef(controlKeyspace, shardConsolidationLeaseTableName) + " SET lease_expires_at = ? " +
		"WHERE target_table_full_name = ? " +
		"AND lease_holder_stream_id = ? " +
		"AND applied_at IS NULL"
	return appliershared.GuardedExec(ctx, db, controlCfg, "lease heartbeat", q, expires, tableName, streamID)
}

// recordShardLeaseDDLText UPDATEs ddl_text for the held lease.
func recordShardLeaseDDLText(ctx context.Context, db *sql.DB, controlKeyspace, tableName, streamID, ddlText string) (recorded bool, err error) {
	q := "UPDATE " + controlTableRef(controlKeyspace, shardConsolidationLeaseTableName) + " SET ddl_text = ? " +
		"WHERE target_table_full_name = ? " +
		"AND lease_holder_stream_id = ? " +
		"AND applied_at IS NULL"
	return appliershared.GuardedExec(ctx, db, controlCfg, "lease record ddl", q, ddlText, tableName, streamID)
}

// finalizeShardLeaseApply records applied_at + ddl_text + ddl_checksum
// + applied_schema_version + anchor_position + source_engine atomically,
// gated on continued ownership.
//
// MySQL's changed-rows RowsAffected semantics don't undermine the
// "0 rows means contention" detection here: the UPDATE always changes
// applied_at (NULL → CURRENT_TIMESTAMP).
//
// anchorPos / anchorEngine carry the source-side CDC position the
// boundary was observed at (v0.76.0+). Empty strings store NULL via the
// NULLIF wrapper so legacy callers / unit-test fakes preserve the
// pre-anchor shape.
func finalizeShardLeaseApply(ctx context.Context, db *sql.DB, controlKeyspace, tableName, streamID, ddlText, ddlChecksum string, version int64, anchorPos, anchorEngine string) (finalized bool, err error) {
	q := "UPDATE " + controlTableRef(controlKeyspace, shardConsolidationLeaseTableName) + " SET " +
		"ddl_text = ?, ddl_checksum = ?, applied_schema_version = ?, applied_at = CURRENT_TIMESTAMP, " +
		"anchor_position = NULLIF(?, ''), source_engine = NULLIF(?, '') " +
		"WHERE target_table_full_name = ? " +
		"AND lease_holder_stream_id = ? " +
		"AND applied_at IS NULL"
	return appliershared.GuardedExec(ctx, db, controlCfg, "lease finalize", q,
		ddlText, ddlChecksum, version, anchorPos, anchorEngine, tableName, streamID)
}

// listShardLeases returns every row in the per-target lease table.
// Tolerant of the table being absent. ADR-0054 §6 operator-visibility
// surface used by `sluice sync status`, plus the v0.76.0 lease GC
// sweep's enumeration source.
func listShardLeases(ctx context.Context, db *sql.DB, controlKeyspace string) ([]shardConsolidationLeaseRow, error) {
	q := "SELECT target_table_full_name, COALESCE(lease_holder_stream_id, ''), " +
		"lease_expires_at, COALESCE(ddl_text, ''), COALESCE(ddl_checksum, ''), " +
		"applied_schema_version, applied_at, anchor_position, source_engine " +
		"FROM " + controlTableRef(controlKeyspace, shardConsolidationLeaseTableName)
	return appliershared.ListShardLeases(ctx, db, controlCfg, q)
}

// deleteShardLease removes the row keyed by tableName. Tolerant of the
// row being absent (DELETE on a missing PK is a no-op) and of the table
// itself being absent (returns nil so a GC sweep against a pre-Ensure
// target is a no-op). v0.76.0 lease GC sweep (task #21).
func deleteShardLease(ctx context.Context, db *sql.DB, controlKeyspace, tableName string) error {
	q := "DELETE FROM " + controlTableRef(controlKeyspace, shardConsolidationLeaseTableName) + " WHERE target_table_full_name = ?"
	return appliershared.TolerantExec(ctx, db, controlCfg, "lease delete", q, tableName)
}

// selectShardLease loads the row for tableName. Returns ok=false when
// no row exists OR when the table itself is missing (pre-Ensure
// inspection path).
func selectShardLease(ctx context.Context, db *sql.DB, controlKeyspace, tableName string) (row shardConsolidationLeaseRow, ok bool, err error) {
	q := "SELECT target_table_full_name, COALESCE(lease_holder_stream_id, ''), " +
		"lease_expires_at, COALESCE(ddl_text, ''), COALESCE(ddl_checksum, ''), " +
		"applied_schema_version, applied_at, anchor_position, source_engine " +
		"FROM " + controlTableRef(controlKeyspace, shardConsolidationLeaseTableName) + " " +
		"WHERE target_table_full_name = ?"
	return appliershared.SelectShardLease(ctx, db, controlCfg, q, tableName)
}

// ensureShardConsolidationLeaseTable creates the per-target
// sluice_shard_consolidation_lease control table (ADR-0054 §1) if it
// doesn't exist. Idempotent — second-and-later calls are no-ops
// courtesy of CREATE TABLE IF NOT EXISTS. ADDITIVE: never touches
// sluice_cdc_state, sluice_cdc_schema_history, or any existing data.
//
// MySQL CREATE TABLE inside an explicit transaction implicit-commits,
// so callers run this from the *sql.DB pool at applier startup,
// alongside ensureControlTable / ensureSchemaHistoryTable.
//
// See ADR-0054 §1 for the row schema and state machine, §2 for the
// timing defaults that govern lease_expires_at extension cadence.
// Detect-then-create for the same reason as [ensureControlTable]: on a
// safe-migrations branch the exists-already path must issue no DDL at
// all, and a genuinely-needed CREATE that gets refused is the coded
// bootstrap refusal.
func ensureShardConsolidationLeaseTable(ctx context.Context, db *sql.DB, controlKeyspace string) error {
	exists, err := controlTableExists(ctx, db, controlKeyspace, shardConsolidationLeaseTableName)
	if err != nil {
		return fmt.Errorf("mysql: ensure shard consolidation lease table: %w", err)
	}
	if !exists {
		ddl := shardConsolidationLeaseTableDDL(controlKeyspace)
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("mysql: ensure shard consolidation lease table: %w", wrapControlTableBootstrapError(wrapDDLError(err), ddl))
		}
	}
	// Migration path for v0.75.0 deployments whose
	// sluice_shard_consolidation_lease table pre-dates the v0.76.0 anchor
	// columns. Same detect-then-ALTER shape as ensureCrossEngineParityColumn
	// — keeps the migration portable to MySQL 8.0.x versions older than
	// 8.0.29 that lack ADD COLUMN IF NOT EXISTS. Task #21 (lease GC sweep)
	// reads anchor_position / source_engine; legacy rows have NULL and are
	// defensively retained by the sweeper.
	for _, col := range []struct{ name, def string }{
		{"anchor_position", "LONGTEXT NULL"},
		{"source_engine", "TEXT NULL"},
	} {
		if err := ensureShardLeaseColumn(ctx, db, controlKeyspace, col.name, col.def); err != nil {
			return err
		}
	}
	// anchor_position TEXT → LONGTEXT widen (roadmap item 65a): tables
	// created (or column-migrated) by a pre-widen binary carry the 64 KB
	// TEXT column, but the anchor is the same engine-opaque position
	// token sluice_cdc_state.source_position holds — a >64 KB GTID/VGTID
	// set must fit both. No-op when the ADD above just created it as
	// LONGTEXT.
	return ensureLongTextPositionColumn(ctx, db, controlKeyspace, shardConsolidationLeaseTableName, "anchor_position", "LONGTEXT NULL")
}

// shardConsolidationLeaseTableDDL renders the ADR-0054 lease-table
// CREATE statement — single-sourced between
// [ensureShardConsolidationLeaseTable] and the bootstrap printer
// ([Engine.ControlTableDDL], ADR-0165).
func shardConsolidationLeaseTableDDL(controlKeyspace string) string {
	return `CREATE TABLE IF NOT EXISTS ` + controlTableRef(controlKeyspace, shardConsolidationLeaseTableName) + ` (
	target_table_full_name        VARCHAR(512) NOT NULL,
	lease_holder_stream_id        VARCHAR(64)  NULL,
	lease_expires_at              TIMESTAMP    NULL,
	ddl_text                      TEXT         NULL,
	ddl_checksum                  VARCHAR(64)  NULL,
	applied_schema_version        BIGINT       NOT NULL DEFAULT 0,
	applied_at                    TIMESTAMP    NULL,
	created_at                    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
	anchor_position               LONGTEXT     NULL,
	source_engine                 TEXT         NULL,
	PRIMARY KEY (target_table_full_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
}

// ensureShardLeaseColumn adds a column to the lease control table when
// missing. Same shape as ensureCrossEngineParityColumn but scoped to the
// lease table — keeps the additive migration portable to MySQL 8.0.x
// versions older than 8.0.29 (no ADD COLUMN IF NOT EXISTS).
func ensureShardLeaseColumn(ctx context.Context, db *sql.DB, controlKeyspace, columnName, columnDef string) error {
	schemaRHS, schemaArgs := controlSchemaPredicate(controlKeyspace)
	checkQ := "SELECT COUNT(*) FROM information_schema.COLUMNS " +
		"WHERE TABLE_SCHEMA = " + schemaRHS + " AND TABLE_NAME = ? AND COLUMN_NAME = ?"
	args := append(append([]any{}, schemaArgs...), shardConsolidationLeaseTableName, columnName)
	var n int
	if err := db.QueryRowContext(ctx, checkQ, args...).Scan(&n); err != nil {
		return fmt.Errorf("mysql: ensure shard consolidation lease table: detect %s: %w", columnName, err)
	}
	if n > 0 {
		return nil
	}
	alter := "ALTER TABLE " + controlTableRef(controlKeyspace, shardConsolidationLeaseTableName) + " ADD COLUMN `" + columnName + "` " + columnDef
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("mysql: ensure shard consolidation lease table: add %s: %w", columnName, wrapControlTableBootstrapError(err, alter))
	}
	return nil
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
func ensureCrossEngineParityColumn(ctx context.Context, db *sql.DB, controlKeyspace, columnName, columnDef string) error {
	schemaRHS, schemaArgs := controlSchemaPredicate(controlKeyspace)
	checkQ := "SELECT COUNT(*) FROM information_schema.COLUMNS " +
		"WHERE TABLE_SCHEMA = " + schemaRHS + " AND TABLE_NAME = ? AND COLUMN_NAME = ?"
	args := append(append([]any{}, schemaArgs...), controlTableName, columnName)
	var n int
	if err := db.QueryRowContext(ctx, checkQ, args...).Scan(&n); err != nil {
		return fmt.Errorf("mysql: ensure control table: detect %s: %w", columnName, err)
	}
	if n > 0 {
		return nil
	}
	alter := "ALTER TABLE " + controlTableRef(controlKeyspace, controlTableName) + " ADD COLUMN `" + columnName + "` " + columnDef
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("mysql: ensure control table: add %s: %w", columnName, wrapControlTableBootstrapError(err, alter))
	}
	return nil
}

// ensureLongTextPositionColumn widens a position-token column from
// TEXT (64 KB) to LONGTEXT when — and ONLY when — the live column is
// still `text` (roadmap item 65a). The engine-opaque position token is
// a GTID/VGTID set with no upper bound sluice controls;
// sluice_cdc_schema_history.anchor_position has been LONGTEXT from the
// start for the same reason, and a >64 KB write into a TEXT column
// fails loudly mid-stream (sluice pins STRICT_TRANS_TABLES — never a
// silent truncation, but an avoidable stream abort).
//
// DETECT-FIRST is load-bearing, not an optimization: on a PlanetScale
// safe-migrations production branch every direct DDL statement is
// refused (Error 1105 "direct DDL is disabled") regardless of whether
// it would change anything, so the already-LONGTEXT path must issue no
// DDL at all — the same lesson as MigrationStateStore.EnsureControlTable's
// detect-then-create gate (live-caught 2026-07-15). When the column IS
// still TEXT and the ALTER itself trips the safe-migrations block, the
// error is surfaced loudly with the exact statement to ship via a
// deploy request — never a silent skip (see
// [wrapPositionWidenDDLError]).
//
// A missing column (ErrNoRows) is tolerated as a no-op: the fresh
// CREATEs declare LONGTEXT directly, and the add-column migrations own
// the column-absent case.
//
// columnName / columnDef are internally-defined constants, never
// operator input (same contract as ensureCrossEngineParityColumn).
func ensureLongTextPositionColumn(ctx context.Context, db *sql.DB, controlKeyspace, tableName, columnName, columnDef string) error {
	schemaRHS, schemaArgs := controlSchemaPredicate(controlKeyspace)
	checkQ := "SELECT DATA_TYPE FROM information_schema.COLUMNS " +
		"WHERE TABLE_SCHEMA = " + schemaRHS + " AND TABLE_NAME = ? AND COLUMN_NAME = ?"
	args := append(append([]any{}, schemaArgs...), tableName, columnName)
	var dataType string
	switch err := db.QueryRowContext(ctx, checkQ, args...).Scan(&dataType); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("mysql: ensure %s: detect %s type: %w", tableName, columnName, err)
	}
	if !strings.EqualFold(dataType, "text") {
		// Already LONGTEXT (or some future shape) — no DDL.
		return nil
	}
	alter := "ALTER TABLE " + controlTableRef(controlKeyspace, tableName) + " MODIFY COLUMN `" + columnName + "` " + columnDef
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return wrapPositionWidenDDLError(err, tableName, columnName, alter)
	}
	return nil
}

// wrapPositionWidenDDLError shapes the failure of the item-65a widen
// ALTER. The PlanetScale safe-migrations refusal (Error 1105 "direct
// DDL is disabled") gets a dedicated remedy-bearing message carrying
// the exact statement to ship via a deploy request, coded
// SLUICE-E-PS-DIRECT-DDL-BLOCKED like every other control-table DDL
// site (roadmap item 66 — `sluice deploy-ddl` is the channel that
// ships it); every other error keeps the plain wrap. Loud by design:
// silently skipping the widen would re-arm the >64 KB position-write
// failure this migration exists to remove.
func wrapPositionWidenDDLError(err error, tableName, columnName, alter string) error {
	if isDirectDDLDisabledErr(err) {
		return sluicecode.Wrap(
			sluicecode.CodePSDirectDDLBlocked,
			"apply the widen via a PlanetScale deploy request (`sluice deploy-ddl --ddl '"+alter+"'`), then re-run",
			fmt.Errorf("%w: %w | "+
				"sluice needs to widen %s.%s from TEXT to LONGTEXT (a GTID/VGTID position "+
				"set can exceed TEXT's 64 KB), but the target branch has Safe Migrations "+
				"enabled, which refuses direct DDL. Apply the widen via a PlanetScale "+
				"deploy request — the exact statement is: %s — then re-run sluice",
				ErrSafeMigrationsBlocked, err, tableName, columnName, alter),
		)
	}
	return fmt.Errorf("mysql: ensure %s: widen %s to LONGTEXT: %w", tableName, columnName, err)
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
func ensureLiveAddedTablesColumn(ctx context.Context, db *sql.DB, controlKeyspace string) error {
	schemaRHS, schemaArgs := controlSchemaPredicate(controlKeyspace)
	checkQ := "SELECT COUNT(*) FROM information_schema.COLUMNS " +
		"WHERE TABLE_SCHEMA = " + schemaRHS + " AND TABLE_NAME = ? AND COLUMN_NAME = 'live_added_tables'"
	args := append(append([]any{}, schemaArgs...), controlTableName)
	var n int
	if err := db.QueryRowContext(ctx, checkQ, args...).Scan(&n); err != nil {
		return fmt.Errorf("mysql: ensure control table: detect live_added_tables: %w", err)
	}
	if n > 0 {
		return nil
	}
	alter := "ALTER TABLE " + controlTableRef(controlKeyspace, controlTableName) + " ADD COLUMN live_added_tables TEXT NULL"
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("mysql: ensure control table: add live_added_tables: %w", wrapControlTableBootstrapError(err, alter))
	}
	return nil
}

// ensureStopRequestedColumn adds the stop_requested_at column to an
// existing control table when missing. Detect-then-ALTER avoids the
// MySQL 8.0.29 floor that ADD COLUMN IF NOT EXISTS would impose;
// sluice broadly supports 8.0+. The lookup scopes to DATABASE() (the
// connection's default database) or, under the sidecar-keyspace
// feature, to the explicit control keyspace (see
// [controlSchemaPredicate]).
func ensureStopRequestedColumn(ctx context.Context, db *sql.DB, controlKeyspace string) error {
	schemaRHS, schemaArgs := controlSchemaPredicate(controlKeyspace)
	checkQ := "SELECT COUNT(*) FROM information_schema.COLUMNS " +
		"WHERE TABLE_SCHEMA = " + schemaRHS + " AND TABLE_NAME = ? AND COLUMN_NAME = 'stop_requested_at'"
	args := append(append([]any{}, schemaArgs...), controlTableName)
	var n int
	if err := db.QueryRowContext(ctx, checkQ, args...).Scan(&n); err != nil {
		return fmt.Errorf("mysql: ensure control table: detect stop_requested_at: %w", err)
	}
	if n > 0 {
		return nil
	}
	alter := "ALTER TABLE " + controlTableRef(controlKeyspace, controlTableName) + " ADD COLUMN stop_requested_at TIMESTAMP NULL"
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("mysql: ensure control table: add stop_requested_at: %w", wrapControlTableBootstrapError(err, alter))
	}
	return nil
}

// readPosition returns the persisted source_position for streamID,
// or ok=false when no row exists. The Engine field of the returned
// position is set to "mysql" by the caller — only the Token survives
// across runs (the engine reading is implicitly the engine that
// wrote). Missing-table tolerance and error shape live in the shared
// skeleton; the same string-match classifier powers ListStreams's
// missing-table fallback.
func readPosition(ctx context.Context, db *sql.DB, controlKeyspace, streamID string) (token string, ok bool, err error) {
	q := "SELECT source_position FROM " + controlTableRef(controlKeyspace, controlTableName) + " WHERE stream_id = ?"
	return appliershared.ReadPosition(ctx, db, controlCfg, q, streamID)
}

// listStreams returns every row in the per-target control table via
// the shared skeleton (missing table → "no streams"; COALESCE on
// slot_name / source_dsn_fingerprint / target_schema so legacy rows
// that pre-date those columns — v0.32.2 introduced them on the MySQL
// side; OBS-1 — surface as empty strings).
//
// The fallback hook retries with the legacy column set (no slot_name
// etc.) when MySQL reports "Unknown column" — the path is reachable
// only during an in-progress upgrade where another connection has run
// EnsureControlTable's ALTER concurrently but this connection's
// query was already planned against the old schema. Defence in
// depth; the fallback returns empty strings for the missing fields.
func listStreams(ctx context.Context, db *sql.DB, controlKeyspace, engineName string) ([]ir.StreamStatus, error) {
	q := "SELECT stream_id, source_position, updated_at, " +
		"COALESCE(slot_name, ''), " +
		"COALESCE(source_dsn_fingerprint, ''), " +
		"COALESCE(target_schema, ''), " +
		"COALESCE(rows_applied, 0) " +
		"FROM " + controlTableRef(controlKeyspace, controlTableName)
	cfg := *controlCfg
	cfg.ListStreamsFallback = func(ctx context.Context, queryErr error) ([]ir.StreamStatus, bool, error) {
		if !isMySQLUnknownColumnErr(queryErr) {
			return nil, false, nil
		}
		out, err := listStreamsLegacy(ctx, db, controlKeyspace, engineName)
		return out, true, err
	}
	return appliershared.ListStreams(ctx, db, &cfg, q, engineName)
}

// listStreamsLegacy is the pre-v0.32.2 SELECT shape, used as a
// fallback when the new query trips an Unknown-column error (e.g.
// in-progress upgrade window before EnsureControlTable's ALTER ran).
// The returned StreamStatus values have empty SlotName /
// SourceDSNFingerprint / TargetSchema — the columns are simply not
// present yet on this control table.
func listStreamsLegacy(ctx context.Context, db *sql.DB, controlKeyspace, engineName string) ([]ir.StreamStatus, error) {
	q := "SELECT stream_id, source_position, updated_at FROM " + controlTableRef(controlKeyspace, controlTableName)
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
// Deliberately NOT routed through the appliershared tier-c skeletons:
// the upsert SQL is the ADR-0007/ADR-0010 resume contract and wholly
// dialect (row-alias ON DUPLICATE KEY here vs ON CONFLICT on PG), so
// each engine byte-owns it.
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
//
// rowsApplied is the number of row-level DML changes (Insert/Update/
// Delete) this position write makes durable; it is ADDED to the row's
// cumulative rows_applied in the SAME UPSERT (COALESCE(existing, 0) +
// delta) so the counter advances atomically with the position (ADR-0156
// phase 2). 0 for a no-data position write (broker cold-start,
// schema-delta-only incrementals, a Truncate/SchemaSnapshot serial
// apply).
func writePositionTx(ctx context.Context, tx *sql.Tx, controlKeyspace, streamID, token, slotName, sourceFingerprint, targetSchema string, rowsApplied int64, upsert upsertSpelling) error {
	// Sidecar-keyspace feature: when controlKeyspace is set, this UPSERT
	// still rides the SAME per-change *sql.Tx as the data write (ADR-0007 /
	// ADR-0049 #4a atomicity) — it is deliberately NOT decoupled onto a
	// separate connection. On a sharded target that makes the position write
	// and the data write span two keyspaces, so vtgate can only best-effort
	// the cross-keyspace commit (no 2PC). That is acceptable here: the
	// sharded-target apply is already cross-shard / non-atomic, and keyed
	// idempotent apply makes a torn resume safe (a position that committed
	// without its data, or vice versa, replays cleanly on restart). Empty
	// controlKeyspace is unchanged single-keyspace, genuinely atomic behaviour.
	if _, err := tx.ExecContext(ctx, writePositionUpsertSQL(controlKeyspace, upsert), streamID, token, slotName, sourceFingerprint, targetSchema, rowsApplied); err != nil {
		return fmt.Errorf("mysql: write position: %w", err)
	}
	return nil
}

// writePositionUpsertSQL builds the ADR-0007 position-write UPSERT, keyspace-
// qualified per controlKeyspace (empty = bare, byte-identical to the historical
// statement) and rendered in the caller's [upsertSpelling] (roadmap item 73:
// VALUES() on mariadb targets, the row alias everywhere else). Extracted as a
// pure function so the byte-exactness of this atomicity-critical statement is
// unit-pinnable for the bare and sidecar-qualified shapes × both spellings
// without a live target. The `ref.slot_name` COALESCE sources use the SAME
// fully-qualified reference as the table, so a qualified name yields a valid
// three-part `ks`.`table`.column reference and the bare name yields the
// historical `table`.column form.
func writePositionUpsertSQL(controlKeyspace string, upsert upsertSpelling) string {
	ref := controlTableRef(controlKeyspace, controlTableName)
	return "INSERT INTO " + ref + " " +
		"(stream_id, source_position, slot_name, source_dsn_fingerprint, target_schema, rows_applied) " +
		"VALUES (?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?)" +
		upsert.clauseOpen() +
		"source_position = " + upsert.newRowRef("source_position") + ", " +
		"slot_name = COALESCE(" + upsert.newRowRef("slot_name") + ", " + ref + ".slot_name), " +
		"source_dsn_fingerprint = COALESCE(" + upsert.newRowRef("source_dsn_fingerprint") + ", " + ref + ".source_dsn_fingerprint), " +
		"target_schema = COALESCE(" + upsert.newRowRef("target_schema") + ", " + ref + ".target_schema), " +
		// rows_applied ACCUMULATES: add this write's delta to the existing
		// count (COALESCE guards a legacy NULL row, though NOT NULL DEFAULT 0
		// means the column is never NULL post-migration).
		"rows_applied = COALESCE(" + ref + ".rows_applied, 0) + " + upsert.newRowRef("rows_applied")
}

// readStopRequested returns true when the named stream's row has a
// non-NULL stop_requested_at column, via the shared skeleton (missing
// table / missing row → false, nil — a stop signal that hasn't been
// recorded is, by definition, not present). The Streamer's poll loop
// calls this every few seconds via the receiver method on
// ChangeApplier; the lint pass can't see that cross-package usage,
// hence the nolint.
//
//nolint:unused // called by pipeline poll loop via ChangeApplier receiver
func readStopRequested(ctx context.Context, db *sql.DB, controlKeyspace, streamID string) (bool, error) {
	q := "SELECT stop_requested_at IS NOT NULL FROM " + controlTableRef(controlKeyspace, controlTableName) + " WHERE stream_id = ?"
	return appliershared.ReadStopRequested(ctx, db, controlCfg, q, streamID)
}

// requestStop flips the stop flag on the named stream's row. Returns
// errStreamNotFound when no row exists (the operator likely typoed
// the stream ID; the CLI surfaces a friendly message). Idempotent;
// updated_at is left alone so the "age" column in `sync status`
// continues to reflect real apply activity rather than stop-request
// bookkeeping.
//
// The existence probe + unconditional UPDATE pair (rather than a
// rows-affected check) is the shared skeleton's changed-rows branch —
// see ControlTableConfig.RowsAffectedIsChangedRows on controlCfg.
func requestStop(ctx context.Context, db *sql.DB, controlKeyspace, streamID string) error {
	ref := controlTableRef(controlKeyspace, controlTableName)
	existsQ := "SELECT 1 FROM " + ref + " WHERE stream_id = ?"
	updateQ := "UPDATE " + ref + " SET stop_requested_at = CURRENT_TIMESTAMP WHERE stream_id = ?"
	return appliershared.RequestStop(ctx, db, controlCfg, existsQ, updateQ, streamID)
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
// position-write commit will populate the row. (EnsureControlTable
// runs first, but a brand-new target may have an in-flight
// schema-apply.)
func clearStopRequested(ctx context.Context, db *sql.DB, controlKeyspace, streamID string) error {
	q := "UPDATE " + controlTableRef(controlKeyspace, controlTableName) + " SET stop_requested_at = NULL WHERE stream_id = ?"
	return appliershared.TolerantExec(ctx, db, controlCfg, "clear stop signal", q, streamID)
}

// clearStream deletes the named stream's row from the per-target
// control table. Idempotent and tolerant of a missing row or table —
// re-running `--reset-target-data` after a partial failure proceeds
// cleanly. See [ChangeApplier.ClearStream] for the recovery flow.
func clearStream(ctx context.Context, db *sql.DB, controlKeyspace, streamID string) error {
	q := "DELETE FROM " + controlTableRef(controlKeyspace, controlTableName) + " WHERE stream_id = ?"
	return appliershared.TolerantExec(ctx, db, controlCfg, "clear stream", q, streamID)
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
func readLiveAddedTables(ctx context.Context, db *sql.DB, controlKeyspace, streamID string) ([]string, error) {
	q := "SELECT live_added_tables FROM " + controlTableRef(controlKeyspace, controlTableName) + " WHERE stream_id = ?"
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
func recordLiveAddedTable(ctx context.Context, db *sql.DB, controlKeyspace, streamID, tableName string) error {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return errors.New("mysql: record live-added table: tableName is empty")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mysql: record live-added table: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	ref := controlTableRef(controlKeyspace, controlTableName)
	selectQ := "SELECT live_added_tables FROM " + ref + " WHERE stream_id = ? FOR UPDATE"
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

	updateQ := "UPDATE " + ref + " SET live_added_tables = ? WHERE stream_id = ?"
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
