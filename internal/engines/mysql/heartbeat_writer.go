// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// EnsureHeartbeatTable / WriteHeartbeat / PruneHeartbeat implement
// [ir.HeartbeatWriter] for MySQL — severity-A finding F17 of the
// 2026-05-22 Reddit-research run, see ADR-0061.
//
// MySQL has a flat namespace (no schema-vs-database distinction in the
// shape PG carries), so the table lives in the connection's default
// database. The table carries:
//
//   - id (BIGINT AUTO_INCREMENT PRIMARY KEY) — surrogate PK;
//   - ts (TIMESTAMP DEFAULT CURRENT_TIMESTAMP) — server-side timestamp;
//   - stream_id (VARCHAR(255) NOT NULL) — labels the row with the
//     originating stream so an operator inspecting the table by hand
//     can see which sluice instance produced it.
//
// **Permission detection.** When the connecting user lacks CREATE
// privilege on the database, MySQL returns error 1142 (ER_TABLEACCESS_DENIED_ERROR);
// the lack-of-CREATE form is error 1044 (ER_DBACCESS_DENIED_ERROR). The
// EnsureHeartbeatTable path wraps both classes in
// [ir.ErrHeartbeatPermission] so the pipeline wiring can `errors.Is`
// check and degrade to WARN-once-skip without classifying the whole
// stream as failed.
//
// Why we don't put the heartbeat table in the same database as the
// streamed tables and trust the connection: the SchemaReader's DSN
// determines the connection's default database, which is also the
// source-of-truth for "what schema we're reading," so the heartbeat
// table lives alongside the data. Operators wanting per-database
// granularity can override `--heartbeat-table-name` if they need to
// disambiguate.

// MySQL error numbers we care about for the heartbeat surface. These
// are stable across all server versions sluice targets (8.0+).
const (
	mysqlErrTableAccessDenied = 1142 // ER_TABLEACCESS_DENIED_ERROR
	mysqlErrDBAccessDenied    = 1044 // ER_DBACCESS_DENIED_ERROR
)

// EnsureHeartbeatTable implements [ir.HeartbeatWriter]. Creates the
// heartbeat table in the connection's default database if it doesn't
// already exist; idempotent on second-and-later calls.
//
// Returns [ir.ErrHeartbeatPermission]-wrapped on the
// insufficient-privilege classes so the pipeline wiring can `errors.Is`
// check and degrade gracefully.
//
// MySQL's CREATE TABLE inside an explicit transaction implicit-commits,
// so this runs against the *sql.DB pool directly — same as
// ensureControlTable.
func (r *SchemaReader) EnsureHeartbeatTable(ctx context.Context, tableName string) error {
	if r.db == nil {
		return errors.New("mysql: EnsureHeartbeatTable: reader not opened")
	}
	if tableName == "" {
		return errors.New("mysql: EnsureHeartbeatTable: tableName is empty")
	}
	if !validHeartbeatTableName(tableName) {
		return fmt.Errorf("mysql: EnsureHeartbeatTable: tableName %q contains invalid characters; allow [A-Za-z0-9_]", tableName)
	}
	// Detect first (GC-40 (a)): MySQL checks the CREATE privilege before
	// IF NOT EXISTS, so a source user holding only INSERT/DELETE on a
	// heartbeat table an admin pre-created would otherwise take the
	// permission-degrade path below and write no heartbeats at all.
	exists, err := controlTableExists(ctx, r.db, "", tableName)
	if err != nil {
		return fmt.Errorf("mysql: ensure heartbeat table %q: %w", tableName, err)
	}
	if exists {
		return checkHeartbeatTableShape(ctx, r.db, tableName)
	}
	ddl := "CREATE TABLE IF NOT EXISTS `" + tableName + "` (" +
		"id        BIGINT       NOT NULL AUTO_INCREMENT, " +
		"ts        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
		"stream_id VARCHAR(255) NOT NULL, " +
		"PRIMARY KEY (id)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
	if _, err := r.db.ExecContext(ctx, ddl); err != nil {
		if isMySQLPermissionDenied(err) {
			return errors.Join(ir.ErrHeartbeatPermission, err)
		}
		return fmt.Errorf("mysql: ensure heartbeat table %q: %w", tableName, err)
	}
	return nil
}

// heartbeatTableShape is the column set EnsureHeartbeatTable creates, by
// information_schema DATA_TYPE. Unchanged since the writer shipped
// (v0.82.0), so a table any release created matches it.
var heartbeatTableShape = map[string]string{"id": "bigint", "ts": "timestamp", "stream_id": "varchar"}

// heartbeatTableShapeText renders [heartbeatTableShape] for the refusal.
const heartbeatTableShapeText = "(id BIGINT AUTO_INCREMENT PRIMARY KEY, ts TIMESTAMP, stream_id VARCHAR(255)) and no other column"

// checkHeartbeatTableShape refuses a table already present under the
// heartbeat's name unless it has exactly the shape EnsureHeartbeatTable
// creates. The writer INSERTs into the table and its prune DELETEs by ts, so
// a name that collides with a user table — a typo in
// --source-heartbeat-table-name — would write into and delete from the
// SOURCE's data. Exactly the three columns, with their types: any other
// column is evidence the table holds something besides heartbeats. Only a
// base table qualifies: a view reads back no columns here and is refused (a
// DELETE through an updatable view deletes from the table under it).
func checkHeartbeatTableShape(ctx context.Context, db *sql.DB, tableName string) error {
	rows, err := db.QueryContext(ctx,
		"SELECT c.COLUMN_NAME, c.DATA_TYPE FROM information_schema.COLUMNS c "+
			"JOIN information_schema.TABLES t ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME "+
			"WHERE c.TABLE_SCHEMA = DATABASE() AND c.TABLE_NAME = ? AND t.TABLE_TYPE = 'BASE TABLE' "+
			"ORDER BY c.ORDINAL_POSITION", tableName)
	if err != nil {
		return fmt.Errorf("mysql: ensure heartbeat table %q: read its columns: %w", tableName, err)
	}
	defer func() { _ = rows.Close() }()
	var found []string
	matched := 0
	for rows.Next() {
		var name, dataType string
		if err := rows.Scan(&name, &dataType); err != nil {
			return fmt.Errorf("mysql: ensure heartbeat table %q: read its columns: %w", tableName, err)
		}
		found = append(found, name+" "+dataType)
		if want, ok := heartbeatTableShape[strings.ToLower(name)]; ok && strings.EqualFold(dataType, want) {
			matched++
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("mysql: ensure heartbeat table %q: read its columns: %w", tableName, err)
	}
	if matched == len(heartbeatTableShape) && len(found) == len(heartbeatTableShape) {
		return nil
	}
	return refuseForeignHeartbeatTable(tableName, found)
}

// refuseForeignHeartbeatTable renders the [ir.ErrHeartbeatTableNotSluices]
// refusal.
func refuseForeignHeartbeatTable(tableName string, found []string) error {
	return fmt.Errorf(
		"mysql: %w: table `%s` already exists in the source database and is not sluice's heartbeat table "+
			"(its columns: %s; sluice's heartbeat table has exactly %s). The heartbeat would INSERT rows into it and "+
			"DELETE its rows by ts, so sluice refuses to touch it. Pick a --source-heartbeat-table-name that does not "+
			"exist (sluice creates it), or turn the heartbeat off (drop --source-heartbeat-interval, or pass --no-source-heartbeat)",
		ir.ErrHeartbeatTableNotSluices, tableName, describeFoundColumns(found), heartbeatTableShapeText,
	)
}

// describeFoundColumns renders the columns the shape check read; none means
// the name is not a base table (a view, say).
func describeFoundColumns(found []string) string {
	if len(found) == 0 {
		return "none, it is not a base table"
	}
	return strings.Join(found, ", ")
}

// WriteHeartbeat implements [ir.HeartbeatWriter]. INSERTs one row with
// the server-side current timestamp and the supplied streamID.
func (r *SchemaReader) WriteHeartbeat(ctx context.Context, tableName, streamID string) error {
	if r.db == nil {
		return errors.New("mysql: WriteHeartbeat: reader not opened")
	}
	if tableName == "" {
		return errors.New("mysql: WriteHeartbeat: tableName is empty")
	}
	if !validHeartbeatTableName(tableName) {
		return fmt.Errorf("mysql: WriteHeartbeat: tableName %q contains invalid characters", tableName)
	}
	q := "INSERT INTO `" + tableName + "` (stream_id) VALUES (?)"
	if _, err := r.db.ExecContext(ctx, q, streamID); err != nil {
		if isMySQLPermissionDenied(err) {
			return errors.Join(ir.ErrHeartbeatPermission, err)
		}
		return fmt.Errorf("mysql: write heartbeat row: %w", err)
	}
	return nil
}

// PruneHeartbeat implements [ir.HeartbeatWriter]. DELETEs rows whose
// ts column is older than (NOW() - olderThan seconds). The comparison
// happens server-side so the prune doesn't trust the writer's clock.
//
// olderThan <= 0 is a no-op so the pipeline wiring can gate the prune
// cadence without paying a round-trip on every tick.
func (r *SchemaReader) PruneHeartbeat(ctx context.Context, tableName string, olderThan time.Duration) (int64, error) {
	if r.db == nil {
		return 0, errors.New("mysql: PruneHeartbeat: reader not opened")
	}
	if tableName == "" {
		return 0, errors.New("mysql: PruneHeartbeat: tableName is empty")
	}
	if !validHeartbeatTableName(tableName) {
		return 0, fmt.Errorf("mysql: PruneHeartbeat: tableName %q contains invalid characters", tableName)
	}
	if olderThan <= 0 {
		return 0, nil
	}
	// MySQL's DATE_SUB(NOW(), INTERVAL ? SECOND) accepts a parameter.
	q := "DELETE FROM `" + tableName + "` WHERE ts < DATE_SUB(NOW(), INTERVAL ? SECOND)"
	seconds := int64(olderThan / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	res, err := r.db.ExecContext(ctx, q, seconds)
	if err != nil {
		if isMySQLPermissionDenied(err) {
			return 0, errors.Join(ir.ErrHeartbeatPermission, err)
		}
		return 0, fmt.Errorf("mysql: prune heartbeat table: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("mysql: prune heartbeat table: rows affected: %w", err)
	}
	return n, nil
}

// isMySQLPermissionDenied reports whether err is a MySQL
// insufficient-privilege error (1142 ER_TABLEACCESS_DENIED_ERROR or
// 1044 ER_DBACCESS_DENIED_ERROR). The go-sql-driver surfaces these via
// *gomysql.MySQLError; the error numbers are stable across MySQL
// versions sluice targets.
func isMySQLPermissionDenied(err error) bool {
	if err == nil {
		return false
	}
	var mysqlErr *gomysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case mysqlErrTableAccessDenied, mysqlErrDBAccessDenied:
			return true
		}
	}
	return false
}

// validHeartbeatTableName guards against operator-supplied table names
// that would inject syntax through MySQL's backtick-quoted identifier.
// Backticks themselves can't appear in the name; we restrict to a
// conservative ASCII alnum + underscore set so the engine never has to
// escape the value.
func validHeartbeatTableName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_':
		default:
			return false
		}
	}
	// Defensive: reject anything starting with a digit (matches MySQL's
	// unquoted identifier rule; we still quote, but the rule keeps the
	// surface predictable for operators inspecting the table).
	first := name[0]
	return first < '0' || first > '9'
}
