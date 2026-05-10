// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// # Bug-6 fix: shape applier values for JSON columns
//
// Before this fix, the applier's INSERT / UPDATE-SET / WHERE
// builders appended row values straight to the args slice and
// always emitted bare `?` placeholders. Both omissions hit JSON
// columns, producing two distinct production failures with one
// root cause — the applier didn't model the JSON column's wire
// shape on either side of the equation:
//
//   - Loud (PG → MySQL CDC, Vitess/PlanetScale targets only): the
//     new image is bound as []byte, which go-sql-driver/mysql
//     labels with `_binary` charset on the wire. Vitess rejects the
//     INSERT with "Cannot create a JSON value from a string with
//     CHARACTER SET 'binary'" and sluice exits. Vanilla MySQL is
//     more permissive and accepts the same bytes, which is why the
//     loud path was invisible to in-house testing for a long time.
//   - Silent (MySQL → MySQL CDC, vanilla MySQL included): the
//     applier emits `WHERE data = ?` against a JSON-typed column.
//     MySQL's equality operator does not implicitly cast the
//     parameter to JSON, so the predicate matches zero rows
//     regardless of whether the parameter is byte-identical to the
//     stored document. The applier explicitly tolerates "update
//     misses" for resume idempotency, so it silently advances the
//     position. The destination row stays stale forever.
//
// The fix is two-part:
//
//  1. Every applier-bound value is routed through prepareValue with
//     the column's declared IR type, so JSON []byte arrives as
//     string (the `_binary` charset prefix is then absent on the
//     wire). This kills the Vitess-specific loud path. The IR type,
//     not the value bytes, is the discriminator — a heuristic over
//     byte shape would be wrong for binary columns whose contents
//     happen to start with `{`.
//  2. WHERE predicates against JSON columns wrap the placeholder in
//     CAST(? AS JSON) so MySQL's equality operator does a JSON-vs-
//     JSON comparison instead of a JSON-vs-string-literal one. This
//     kills the silent path on vanilla MySQL too.
//
// To support both, the applier caches the destination column-type
// map per table and consults it on every Insert/Update/Delete.
// Cache miss is one round-trip; hit is a map lookup.
//
// As defence in depth, dispatch also emits a debug-level log when
// Update or Delete reports zero rows affected, so the previously
// silent divergence has at least one observable footprint in the
// log stream.

// ChangeApplier applies [ir.Change] events to a MySQL target, one
// source change per target transaction. It implements
// [ir.ChangeApplier].
//
// # Identity-key behaviour (read this before pointing it at a real
// # table)
//
// The applier upserts rows on Insert using the table's PRIMARY KEY
// as the conflict target — that's what makes resume after a partial
// apply safe (a re-applied Insert turns into a no-op UPDATE rather
// than a duplicate-key error). Two situations to be aware of:
//
//   - **Tables without any PK fall back to plain INSERT.** Both PG's
//     ON CONFLICT and MySQL's ON DUPLICATE KEY UPDATE require a key
//     to collide with; without one, the syntax is unusable. Plain
//     INSERT means a re-applied Insert produces a duplicate row.
//     Resume idempotency on no-PK tables is therefore best-effort,
//     and continuous-sync on such tables is not recommended. Add a
//     PRIMARY KEY to the source table before running sluice in
//     continuous-sync mode.
//
//   - **Tables with a UNIQUE KEY but no PRIMARY KEY** are a known
//     trouble spot in MySQL replication generally — sluice doesn't
//     special-case the unique-key as a conflict target either. The
//     applier behaves as if there's no PK (plain INSERT path). If
//     you need upsert semantics here, declare the unique column as
//     the PRIMARY KEY on the source table.
//
// # Lifecycle
//
// One applier per target connection pool. Apply is single-goroutine:
// it consumes the change channel sequentially to preserve source
// ordering. Concurrent calls on the same applier are not supported.
type ChangeApplier struct {
	db     *sql.DB
	schema string

	// pkCache maps "schema.table" → ordered list of PK column names.
	// Populated lazily via a single information_schema query the
	// first time a change for the table arrives. An empty slice
	// (length 0) means "table exists but has no PK" — in that case
	// Insert falls back to plain INSERT (see the package comment).
	pkCache map[string][]string

	// colTypeCache maps "schema.table" → column-name → *ir.Column. It
	// is the input to prepareValue for every value the applier
	// binds: see the file-header comment for the JSON-column bug
	// this exists to fix. The map carries the full Column descriptor
	// (not just the IR type) so prepareValue can consult fields like
	// [ir.Column.SourceColumnType] when disambiguating value shapes
	// — see Bug 47 / convertArrayLikeToJSON in row_writer.go.
	// Populated lazily on the first sight of a table via a single
	// information_schema query — same shape as pkCache. Cache miss
	// is one round-trip; hit is a map lookup.
	colTypeCache map[string]map[string]*ir.Column

	// maxBufferBytes is the soft byte-size cap on the in-flight
	// batch's buffered change values during ApplyBatch. Implements
	// [ir.MaxBufferBytesSetter] via [SetMaxBufferBytes]. Zero or
	// negative means "no byte cap"; the row-count cap remains the
	// only flush trigger. See ADR-0028.
	maxBufferBytes int64
}

// SetMaxBufferBytes implements [ir.MaxBufferBytesSetter]. The
// streamer calls this after [Engine.OpenChangeApplier] returns when
// --max-buffer-bytes is set, before ApplyBatch runs. Zero or negative
// means "no byte cap"; the row-count cap remains the only flush
// trigger.
func (a *ChangeApplier) SetMaxBufferBytes(bytes int64) {
	a.maxBufferBytes = bytes
}

// Close releases the underlying connection pool.
func (a *ChangeApplier) Close() error {
	if a.db == nil {
		return nil
	}
	return a.db.Close()
}

// EnsureControlTable creates the per-target sluice_cdc_state table
// if it doesn't exist. Idempotent. Must run before Apply on any
// fresh target; the Streamer drives this at startup.
func (a *ChangeApplier) EnsureControlTable(ctx context.Context) error {
	return ensureControlTable(ctx, a.db)
}

// ReadPosition returns the last persisted source position for
// streamID, or ok=false when no row exists. The returned Position
// always has Engine = "mysql"; only the Token survives across
// runs (the engine reading is implicitly the engine that wrote).
func (a *ChangeApplier) ReadPosition(ctx context.Context, streamID string) (ir.Position, bool, error) {
	token, ok, err := readPosition(ctx, a.db, streamID)
	if err != nil {
		return ir.Position{}, false, err
	}
	if !ok {
		return ir.Position{}, false, nil
	}
	// Mirror PG: returned Position.Engine is hard-coded to "mysql".
	// Broker-driven rows carry their engine sentinel inside the JSON
	// envelope (`_engine` field, see pipeline.isBrokerToken). Bug 39
	// (v0.20.1) is the load-bearing rationale for that envelope.
	return ir.Position{Engine: engineNameMySQL, Token: token}, true, nil
}

// ListStreams returns all rows in the per-target control table.
// Used by `sluice sync status` for operational visibility. Tolerant
// of the table being absent — operators querying status against a
// fresh target should see "no streams" rather than an error.
func (a *ChangeApplier) ListStreams(ctx context.Context) ([]ir.StreamStatus, error) {
	return listStreams(ctx, a.db, engineNameMySQL)
}

// RequestStop flips the stop flag on the named stream's row. The
// running [pipeline.Streamer] polls this column every few seconds
// and exits cleanly once it observes a non-NULL value. Idempotent —
// repeated calls land the same flag.
//
// Returns an error wrapping [errStreamNotFound] when no row exists
// for streamID; the CLI's `sync stop` branches on it to surface a
// friendly "no stream X on target" message.
func (a *ChangeApplier) RequestStop(ctx context.Context, streamID string) error {
	if streamID == "" {
		return errors.New("mysql: applier: RequestStop: streamID is empty")
	}
	return requestStop(ctx, a.db, streamID)
}

// ReadStopRequested returns true when the named stream's row has a
// non-NULL stop_requested_at column. The pipeline's Streamer poll
// goroutine consults this method via a structural interface (the
// internal pipeline.stopFlagReader). Exported because Go's method-
// set rules require an exported method to satisfy an interface from
// another package — even when that interface is itself unexported.
func (a *ChangeApplier) ReadStopRequested(ctx context.Context, streamID string) (bool, error) {
	return readStopRequested(ctx, a.db, streamID)
}

// ClearStopRequested resets stop_requested_at to NULL for the named
// stream. The Streamer calls this at startup so a previous
// `sluice sync stop` doesn't leave a sticky signal that immediately
// exits the next `sluice sync start` (Bug 11 in v0.3.2 testing).
// Idempotent and tolerant of a missing row.
func (a *ChangeApplier) ClearStopRequested(ctx context.Context, streamID string) error {
	if streamID == "" {
		return errors.New("mysql: applier: ClearStopRequested: streamID is empty")
	}
	return clearStopRequested(ctx, a.db, streamID)
}

// ClearStream deletes the named stream's row from the per-target
// sluice_cdc_state table. Used by the `--reset-target-data` recovery
// path (ADR-0023). Implements [ir.StreamCleaner]. Idempotent and
// tolerant of a missing row or table.
func (a *ChangeApplier) ClearStream(ctx context.Context, streamID string) error {
	if streamID == "" {
		return errors.New("mysql: applier: ClearStream: streamID is empty")
	}
	return clearStream(ctx, a.db, streamID)
}

// ReadLiveAddedTables returns the comma-parsed live_added_tables
// column for streamID — the set of tables that have been live-added to
// this stream's scope by `sluice schema add-table --no-drain`
// (ADR-0034 MySQL Phase 2). The pipeline streamer's poll goroutine
// calls this on its tick cadence to keep its in-memory dispatch
// filter in sync.
//
// Empty slice covers all "no live-adds" surfaces: NULL column, missing
// row, missing column (legacy pre-v0.27.0 control table), missing
// table. The streamer treats every shape as "no live-adds; preserve
// the operator's original filter."
func (a *ChangeApplier) ReadLiveAddedTables(ctx context.Context, streamID string) ([]string, error) {
	if streamID == "" {
		return nil, errors.New("mysql: applier: ReadLiveAddedTables: streamID is empty")
	}
	return readLiveAddedTables(ctx, a.db, streamID)
}

// RecordLiveAddedTable appends tableName to the per-target row's
// live_added_tables column for streamID. ADR-0034. Called by the
// add-table --no-drain orchestrator on a successful live-add. The
// streamer's poll goroutine picks the change up on its next tick;
// from that point onwards, binlog events on the new table reach the
// applier.
//
// Idempotent: re-running with the same tableName does not double-
// record. Concurrent runs against different tables serialise via
// SELECT ... FOR UPDATE.
//
// Errors when the cdc-state row doesn't exist for streamID — the
// orchestrator's preflight has already verified this via ListStreams,
// but a clean error here surfaces the rare race where the row was
// deleted between preflight and write.
func (a *ChangeApplier) RecordLiveAddedTable(ctx context.Context, streamID, tableName string) error {
	if streamID == "" {
		return errors.New("mysql: applier: RecordLiveAddedTable: streamID is empty")
	}
	return recordLiveAddedTable(ctx, a.db, streamID, tableName)
}

// WritePosition implements [ir.PositionWriter]: upserts the position
// row for streamID in `sluice_cdc_state` without any accompanying
// data write. Used by Phase 4.5's broker for cold-start initial-
// position writes and schema-delta-only incrementals (no change
// chunks → no Apply path to ride along with).
//
// Wraps the same writePositionTx helper the Apply path uses, so the
// row shape and idempotency contract are identical.
func (a *ChangeApplier) WritePosition(ctx context.Context, streamID string, pos ir.Position) error {
	if streamID == "" {
		return errors.New("mysql: applier: WritePosition: streamID is empty")
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mysql: applier: WritePosition: begin tx: %w", err)
	}
	if err := writePositionTx(ctx, tx, streamID, pos.Token); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mysql: applier: WritePosition: commit: %w", err)
	}
	return nil
}

// Apply consumes changes from the channel and applies each to the
// target in its own transaction. The position write happens inside
// the same transaction as the data write (per ADR-0007), so a
// crash between them rolls back both — progress and data can never
// diverge.
//
// Returns when the channel closes (clean shutdown), when ctx is
// cancelled, or when a target write fails.
func (a *ChangeApplier) Apply(ctx context.Context, streamID string, changes <-chan ir.Change) error {
	if streamID == "" {
		return errors.New("mysql: applier: streamID is empty (Streamer is responsible for resolving it)")
	}
	for {
		select {
		case c, ok := <-changes:
			if !ok {
				return nil
			}
			// Source-tx boundary events are no-ops on the per-change
			// path (ADR-0027): each row event already commits its
			// own target transaction, so a TxBegin / TxCommit
			// signal carries no extra information here. The
			// boundary semantics are only useful to the batched
			// applier, which observes them to align target tx
			// boundaries to source tx boundaries.
			switch c.(type) {
			case ir.TxBegin, ir.TxCommit:
				continue
			}
			if err := a.applyOne(ctx, streamID, c); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// applyOne dispatches a single change to its SQL form, runs the
// data write, and writes the position update — all in the same
// transaction.
func (a *ChangeApplier) applyOne(ctx context.Context, streamID string, c ir.Change) error {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mysql: applier: begin tx: %w", err)
	}
	if err := a.dispatch(ctx, tx, c); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := writePositionTx(ctx, tx, streamID, c.Pos().Token); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mysql: applier: commit: %w", err)
	}
	return nil
}

// dispatch routes a single change to its SQL form on the open tx.
func (a *ChangeApplier) dispatch(ctx context.Context, tx *sql.Tx, c ir.Change) error {
	switch v := c.(type) {
	case ir.Insert:
		schema := applierSchema(a.schema, v.Schema)
		pk, err := a.pkFor(ctx, tx, v.Schema, v.Table)
		if err != nil {
			return fmt.Errorf("mysql: applier: pk lookup for %s.%s: %w", v.Schema, v.Table, err)
		}
		colTypes, err := a.colTypesFor(ctx, tx, v.Schema, v.Table)
		if err != nil {
			return fmt.Errorf("mysql: applier: column types for %s.%s: %w", v.Schema, v.Table, err)
		}
		stmt, args := buildInsertSQL(schema, v.Table, v.Row, pk, colTypes)
		if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
			return fmt.Errorf("mysql: applier: insert into %s.%s: %w", v.Schema, v.Table, err)
		}
		return nil

	case ir.Update:
		colTypes, err := a.colTypesFor(ctx, tx, v.Schema, v.Table)
		if err != nil {
			return fmt.Errorf("mysql: applier: column types for %s.%s: %w", v.Schema, v.Table, err)
		}
		stmt, args := buildUpdateSQL(applierSchema(a.schema, v.Schema), v.Table, v.Before, v.After, colTypes)
		// Update misses are tolerated (zero rows affected). On resume
		// we may replay an Update whose target row was already
		// updated — that's expected, not an error. Silent zero-rows-
		// affected can also signal Bug-6-style WHERE-predicate
		// breakage on JSON columns; we surface it at debug level so
		// the divergence has at least one observable footprint.
		res, err := tx.ExecContext(ctx, stmt, args...)
		if err != nil {
			return fmt.Errorf("mysql: applier: update %s.%s: %w", v.Schema, v.Table, err)
		}
		logZeroRowsAffected(ctx, "update", v.Schema, v.Table, res)
		return nil

	case ir.Delete:
		colTypes, err := a.colTypesFor(ctx, tx, v.Schema, v.Table)
		if err != nil {
			return fmt.Errorf("mysql: applier: column types for %s.%s: %w", v.Schema, v.Table, err)
		}
		stmt, args := buildDeleteSQL(applierSchema(a.schema, v.Schema), v.Table, v.Before, colTypes)
		// Delete misses are tolerated for the same reason as Update.
		res, err := tx.ExecContext(ctx, stmt, args...)
		if err != nil {
			return fmt.Errorf("mysql: applier: delete from %s.%s: %w", v.Schema, v.Table, err)
		}
		logZeroRowsAffected(ctx, "delete", v.Schema, v.Table, res)
		return nil

	case ir.Truncate:
		stmt := buildTruncateSQL(applierSchema(a.schema, v.Schema), v.Table)
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("mysql: applier: truncate %s.%s: %w", v.Schema, v.Table, err)
		}
		return nil
	}
	return fmt.Errorf("mysql: applier: unknown change type %T", c)
}

// logZeroRowsAffected emits a debug-level log line when a target Exec
// reports zero rows affected. Resume idempotency depends on tolerating
// these (the comment in dispatch explains why), but a silent zero-
// rows-affected can also be the signature of a WHERE-predicate bug
// against a target row that exists but doesn't match — the silent
// failure mode of Bug 6. Logging it at debug level keeps the
// resume-idempotency contract intact while making the divergence
// visible to anyone investigating after the fact.
func logZeroRowsAffected(ctx context.Context, op, schema, table string, res sql.Result) {
	if res == nil {
		return
	}
	n, err := res.RowsAffected()
	if err != nil {
		// RowsAffected is documented to return an error only when the
		// driver doesn't support it. go-sql-driver/mysql does, so we
		// shouldn't reach this branch — but we'd rather skip the log
		// than escalate a non-fatal driver quirk to a fatal error.
		return
	}
	if n == 0 {
		slog.DebugContext(ctx, "mysql: applier: zero rows affected",
			slog.String("op", op),
			slog.String("schema", schema),
			slog.String("table", table),
		)
	}
}

// pkFor returns the cached PK column list for the named table,
// loading it on the first sight of the table. An empty slice means
// "no PK" — Insert falls back to plain INSERT in that case.
func (a *ChangeApplier) pkFor(ctx context.Context, tx *sql.Tx, schema, table string) ([]string, error) {
	qn := qualifiedName(schema, table)
	if cached, ok := a.pkCache[qn]; ok {
		return cached, nil
	}
	pk, err := loadPrimaryKey(ctx, tx, applierSchema(a.schema, schema), table)
	if err != nil {
		return nil, err
	}
	a.pkCache[qn] = pk
	return pk, nil
}

// colTypesFor returns the cached column-name → IR type map for the
// named table, loading it on the first sight of the table. The map
// is consulted for every value the applier binds so prepareValue can
// shape JSON / Set / Geometry values for the driver — see the file-
// header comment for the JSON-column bug that makes this routing
// load-bearing.
//
// The reused machinery (loadTableSchema + translateType) is the same
// path the CDC reader takes to refresh its decoder cache after DDL,
// so any new IR type the schema reader learns is automatically
// available to the applier without further plumbing.
func (a *ChangeApplier) colTypesFor(ctx context.Context, _ *sql.Tx, schema, table string) (map[string]*ir.Column, error) {
	qn := qualifiedName(schema, table)
	if cached, ok := a.colTypeCache[qn]; ok {
		return cached, nil
	}
	// loadTableSchema queries information_schema directly; we use the
	// applier's *sql.DB rather than the open tx because the lookup is
	// effectively read-only metadata that is stable across the tx
	// boundary, and loadTableSchema's signature already takes a *sql.DB.
	// The pkFor helper uses the tx for symmetry with the data write,
	// but column-type metadata changes only on DDL, which sluice does
	// not interleave with row events on the applier side.
	tbl, err := loadTableSchema(ctx, a.db, applierSchema(a.schema, schema), table)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*ir.Column, len(tbl.Columns))
	for _, col := range tbl.Columns {
		out[col.Name] = col
	}
	a.colTypeCache[qn] = out
	return out, nil
}

// applierSchema picks the schema name to use in SQL. The applier's
// configured schema (a.schema, derived from the target DSN) is
// authoritative — it is the destination database the operator
// pointed sluice at. The change's source-side schema is metadata
// only; using it would route writes to a same-named schema on the
// target, which is wrong whenever source and target schema names
// differ (e.g. source_db → target_db on the same MySQL instance,
// or any cross-engine pair). v.Schema is honoured only as a
// fallback when the applier wasn't configured with one — which
// shouldn't happen in practice but keeps the function total.
func applierSchema(defaultSchema, changeSchema string) string {
	if defaultSchema != "" {
		return defaultSchema
	}
	return changeSchema
}

// loadPrimaryKey reads the PK columns for the named table from
// information_schema. Returns an empty slice (not nil) for tables
// with no PK; nil indicates a query error.
func loadPrimaryKey(ctx context.Context, tx *sql.Tx, schema, table string) ([]string, error) {
	const q = `
		SELECT column_name
		FROM   information_schema.statistics
		WHERE  table_schema = ?
		  AND  table_name   = ?
		  AND  index_name   = 'PRIMARY'
		ORDER  BY seq_in_index`

	rows, err := tx.QueryContext(ctx, q, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pk := make([]string, 0, 4)
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		pk = append(pk, col)
	}
	return pk, rows.Err()
}

// buildInsertSQL builds an INSERT statement. With a non-empty PK,
// uses the row-alias UPSERT form (8.0.20+):
//
//	INSERT INTO `s`.`t` (`a`, `b`) VALUES (?, ?) AS new
//	ON DUPLICATE KEY UPDATE `a` = new.`a`, `b` = new.`b`
//
// With an empty PK list (tables without a PRIMARY KEY), falls back
// to a plain INSERT — see the ChangeApplier package doc for the
// resume-idempotency caveat.
//
// colTypes maps column names to their full IR descriptors and is the
// input to prepareValue. A missing entry (empty map, or column not
// present) is tolerated and the raw value is bound — the same
// pre-Bug-6 shape — so that callers without a populated cache
// (currently only unit tests pre-dating this fix) still produce
// valid SQL.
func buildInsertSQL(schema, table string, row ir.Row, pk []string, colTypes map[string]*ir.Column) (sqlStmt string, args []any) {
	cols := sortedKeys(row)
	args = make([]any, 0, len(cols))
	colSQL := make([]string, len(cols))
	for i, c := range cols {
		colSQL[i] = quoteIdent(c)
		args = append(args, prepareApplierValue(row[c], colTypes, c))
	}

	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	placeholders := strings.Repeat("?, ", len(cols))
	placeholders = strings.TrimSuffix(placeholders, ", ")

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableRef)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(colSQL, ", "))
	sb.WriteString(") VALUES (")
	sb.WriteString(placeholders)
	sb.WriteByte(')')

	if len(pk) > 0 {
		// Row-alias UPSERT: every non-PK column gets reassigned to
		// the new row's value. PK columns are excluded from the
		// SET list because updating them on conflict would be a
		// no-op at best (PK columns equal by definition during the
		// conflict) and silently incorrect if the new and existing
		// rows have differing PK shapes.
		pkSet := make(map[string]struct{}, len(pk))
		for _, p := range pk {
			pkSet[p] = struct{}{}
		}
		nonPK := make([]string, 0, len(cols))
		for _, c := range cols {
			if _, isPK := pkSet[c]; !isPK {
				nonPK = append(nonPK, c)
			}
		}
		if len(nonPK) > 0 {
			sb.WriteString(" AS new ON DUPLICATE KEY UPDATE ")
			parts := make([]string, len(nonPK))
			for i, c := range nonPK {
				parts[i] = fmt.Sprintf("%s = new.%s", quoteIdent(c), quoteIdent(c))
			}
			sb.WriteString(strings.Join(parts, ", "))
		} else {
			// Every column is a PK column — the row IS its own key.
			// On conflict there's nothing to update; emit
			// ON DUPLICATE KEY UPDATE with a no-op assignment so
			// the conflict is absorbed silently.
			sb.WriteString(" AS new ON DUPLICATE KEY UPDATE ")
			sb.WriteString(quoteIdent(pk[0]))
			sb.WriteString(" = new.")
			sb.WriteString(quoteIdent(pk[0]))
		}
	}
	return sb.String(), args
}

// buildUpdateSQL builds an UPDATE statement. SET uses every column
// in After (including ones whose value didn't change — unchanged-
// column detection is a v1.5 optimization). WHERE uses every column
// in Before with NULL-aware predicate building.
func buildUpdateSQL(schema, table string, before, after ir.Row, colTypes map[string]*ir.Column) (sqlStmt string, args []any) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	setSQL, setArgs := buildSetClause(after, colTypes)
	whereSQL, whereArgs := buildWhereClause(before, colTypes)

	args = make([]any, 0, len(setArgs)+len(whereArgs))
	args = append(args, setArgs...)
	args = append(args, whereArgs...)
	return "UPDATE " + tableRef + " SET " + setSQL + " WHERE " + whereSQL, args
}

// buildDeleteSQL builds a DELETE statement using the Before image
// as the WHERE predicate.
func buildDeleteSQL(schema, table string, before ir.Row, colTypes map[string]*ir.Column) (sqlStmt string, args []any) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	whereSQL, whereArgs := buildWhereClause(before, colTypes)
	return "DELETE FROM " + tableRef + " WHERE " + whereSQL, whereArgs
}

// buildTruncateSQL builds a TRUNCATE TABLE statement.
func buildTruncateSQL(schema, table string) string {
	return "TRUNCATE TABLE " + quoteIdent(schema) + "." + quoteIdent(table)
}

// buildSetClause renders "col1 = ?, col2 = ?" for an UPDATE SET.
// NULL values bind through database/sql normally; no special form
// is needed in SET (unlike WHERE).
func buildSetClause(row ir.Row, colTypes map[string]*ir.Column) (clause string, args []any) {
	cols := sortedKeys(row)
	parts := make([]string, len(cols))
	args = make([]any, 0, len(cols))
	for i, c := range cols {
		parts[i] = quoteIdent(c) + " = ?"
		args = append(args, prepareApplierValue(row[c], colTypes, c))
	}
	return strings.Join(parts, ", "), args
}

// buildWhereClause renders an AND-joined predicate with NULL-aware
// handling: nil row values produce "col IS NULL" (no parameter) so
// SQL's NULL semantics don't make the predicate unsatisfiable.
//
// JSON columns get a CAST(? AS JSON) on the right-hand side. The
// equality operator on a JSON-typed column compared to a plain
// string literal never matches in MySQL — the server doesn't
// implicitly cast the parameter to JSON, so `WHERE j = ?` returns
// zero rows even when the bound string is byte-equal to the stored
// document. CAST(? AS JSON) parses the parameter as JSON and the
// resulting JSON-vs-JSON comparison ignores formatting differences
// (whitespace, key order) the way operators expect. This is the
// SQL-side half of the Bug 6 silent-failure fix; the value-shaping
// half (prepareValue routing) is the other.
func buildWhereClause(row ir.Row, colTypes map[string]*ir.Column) (clause string, args []any) {
	cols := sortedKeys(row)
	parts := make([]string, 0, len(cols))
	args = make([]any, 0, len(cols))
	for _, c := range cols {
		v := row[c]
		if v == nil {
			parts = append(parts, quoteIdent(c)+" IS NULL")
			continue
		}
		parts = append(parts, quoteIdent(c)+" = "+placeholderFor(colTypes, c))
		args = append(args, prepareApplierValue(v, colTypes, c))
	}
	return strings.Join(parts, " AND "), args
}

// placeholderFor returns the right-hand-side placeholder fragment
// for a column. JSON columns become CAST(? AS JSON) so MySQL's
// equality operator does a JSON-vs-JSON comparison rather than a
// JSON-vs-string-literal comparison (which silently never matches).
// Every other column type uses a bare ?.
func placeholderFor(colTypes map[string]*ir.Column, colName string) string {
	if colTypes == nil {
		return "?"
	}
	col, ok := colTypes[colName]
	if !ok || col == nil {
		return "?"
	}
	if _, isJSON := col.Type.(ir.JSON); isJSON {
		return "CAST(? AS JSON)"
	}
	return "?"
}

// prepareApplierValue is the applier's wrapper around prepareValue:
// it looks up the column's IR type and routes the value through the
// shared shaping helper from row_writer.go. When the column isn't in
// the map (cache cold or column unknown — defensive), it falls back
// to the raw value, mirroring the pre-Bug-6 behavior so the SQL is
// still valid in pathological setups.
//
// Routing through the shared helper rather than re-implementing the
// JSON []byte → string conversion here means new shaping rules added
// to prepareValue (for future IR types) are automatically picked up
// by the applier without touching this file.
func prepareApplierValue(v any, colTypes map[string]*ir.Column, colName string) any {
	if colTypes == nil {
		return v
	}
	col, ok := colTypes[colName]
	if !ok || col == nil {
		return v
	}
	return prepareValue(v, col)
}

// (sortedKeys is shared with the schema reader — see schema_reader.go
// for the implementation. The applier uses it to render generated SQL
// in a deterministic column order.)
