package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// # Bug-6 fix: route applier args through prepareValue
//
// The MySQL side of this fix has the load-bearing motivation (see
// internal/engines/mysql/change_applier.go for the full write-up of
// the JSON-column wire-format bug — both the loud PG → MySQL and the
// silent MySQL → MySQL manifestations). The Postgres applier
// historically didn't surface the same failure mode because pgx
// inspects per-column type metadata before sending parameters, so
// JSON values arrived correctly even without the shaping path.
//
// However, the structural omission was symmetric: the PG applier
// also bypassed prepareValue, which means ir.Array values (canonical
// []any) and ir.Geometry values (raw WKB needing EWKB framing) would
// hit pgx in their IR shape rather than the driver-acceptable form
// the bulk-copy path uses. Mirroring the MySQL fix here keeps the
// two engines symmetric and inoculates the PG applier against the
// same class of bug for any future IR type whose shaping is
// non-trivial.
//
// As on the MySQL side, dispatch logs zero-rows-affected at debug
// level for Update / Delete so the silent-divergence failure mode
// has at least one observable footprint.

// ChangeApplier applies [ir.Change] events to a Postgres target,
// one source change per target transaction. It implements
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
//   - **Tables without any PK fall back to plain INSERT.** Postgres'
//     ON CONFLICT requires a unique-index target; without one, the
//     syntax is unusable. Plain INSERT means a re-applied Insert
//     produces a duplicate row. Resume idempotency on no-PK tables
//     is therefore best-effort, and continuous-sync on such tables
//     is not recommended. Add a PRIMARY KEY to the source table
//     before running sluice in continuous-sync mode.
//
//   - **Tables with a UNIQUE INDEX/CONSTRAINT but no PRIMARY KEY**
//     would be a candidate conflict target on PG (unlike MySQL),
//     but sluice doesn't special-case it. The applier behaves as
//     if there's no PK (plain INSERT path). If you need upsert
//     semantics here, declare the unique column as the PRIMARY KEY
//     on the source table.
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

	// colTypeCache maps "schema.table" → column-name → IR type. It
	// is the input to prepareValue for every value the applier
	// binds; see the file-header comment for the JSON-column bug
	// the parallel MySQL fix exists to address. Populated lazily on
	// the first sight of a table — same shape as pkCache.
	colTypeCache map[string]map[string]ir.Type
}

// Close releases the underlying connection pool.
func (a *ChangeApplier) Close() error {
	if a.db == nil {
		return nil
	}
	return a.db.Close()
}

// EnsureControlTable creates the per-target sluice_cdc_state table
// (in the applier's schema) if it doesn't exist. Idempotent.
func (a *ChangeApplier) EnsureControlTable(ctx context.Context) error {
	return ensureControlTable(ctx, a.db, a.schema)
}

// ReadPosition returns the last persisted source position for
// streamID, or ok=false when no row exists. The returned Position
// always has Engine = "postgres".
func (a *ChangeApplier) ReadPosition(ctx context.Context, streamID string) (ir.Position, bool, error) {
	token, ok, err := readPosition(ctx, a.db, a.schema, streamID)
	if err != nil {
		return ir.Position{}, false, err
	}
	if !ok {
		return ir.Position{}, false, nil
	}
	return ir.Position{Engine: engineNamePostgres, Token: token}, true, nil
}

// ListStreams returns all rows in the per-target control table.
// Used by `sluice sync status` for operational visibility. Tolerant
// of the table being absent — operators querying status against a
// fresh target should see "no streams" rather than an error.
func (a *ChangeApplier) ListStreams(ctx context.Context) ([]ir.StreamStatus, error) {
	return listStreams(ctx, a.db, a.schema, engineNamePostgres)
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
		return errors.New("postgres: applier: RequestStop: streamID is empty")
	}
	return requestStop(ctx, a.db, a.schema, streamID)
}

// ReadStopRequested returns true when the named stream's row has a
// non-NULL stop_requested_at column. The pipeline's Streamer poll
// goroutine consults this method via a structural interface (the
// internal pipeline.stopFlagReader). Exported because Go's method-
// set rules require an exported method to satisfy an interface from
// another package — even when that interface is itself unexported.
func (a *ChangeApplier) ReadStopRequested(ctx context.Context, streamID string) (bool, error) {
	return readStopRequested(ctx, a.db, a.schema, streamID)
}

// ClearStopRequested resets stop_requested_at to NULL for the named
// stream. The Streamer calls this at startup so a previous
// `sluice sync stop` doesn't leave a sticky signal that immediately
// exits the next `sluice sync start` (Bug 11 in v0.3.2 testing).
// Idempotent and tolerant of a missing row.
func (a *ChangeApplier) ClearStopRequested(ctx context.Context, streamID string) error {
	if streamID == "" {
		return errors.New("postgres: applier: ClearStopRequested: streamID is empty")
	}
	return clearStopRequested(ctx, a.db, a.schema, streamID)
}

// Apply consumes changes from the channel and applies each to the
// target in its own transaction. The position write happens inside
// the same transaction as the data write (per ADR-0007); a crash
// between them rolls back both, so progress and data can never
// diverge.
//
// Returns when the channel closes (clean shutdown), when ctx is
// cancelled, or when a target write fails.
func (a *ChangeApplier) Apply(ctx context.Context, streamID string, changes <-chan ir.Change) error {
	if streamID == "" {
		return errors.New("postgres: applier: streamID is empty (Streamer is responsible for resolving it)")
	}
	for {
		select {
		case c, ok := <-changes:
			if !ok {
				return nil
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
		return fmt.Errorf("postgres: applier: begin tx: %w", err)
	}
	if err := a.dispatch(ctx, tx, c); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := writePositionTx(ctx, tx, a.schema, streamID, c.Pos().Token); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("postgres: applier: commit: %w", err)
	}
	return nil
}

// dispatch routes a single change to its SQL form on the open tx.
func (a *ChangeApplier) dispatch(ctx context.Context, tx *sql.Tx, c ir.Change) error {
	switch v := c.(type) {
	case ir.Insert:
		schema := applierSchema(a.schema, v.Schema)
		pk, err := a.pkFor(ctx, tx, schema, v.Table)
		if err != nil {
			return fmt.Errorf("postgres: applier: pk lookup for %s.%s: %w", schema, v.Table, err)
		}
		colTypes, err := a.colTypesFor(ctx, schema, v.Table)
		if err != nil {
			return fmt.Errorf("postgres: applier: column types for %s.%s: %w", schema, v.Table, err)
		}
		stmt, args, err := buildInsertSQL(schema, v.Table, v.Row, pk, colTypes)
		if err != nil {
			return fmt.Errorf("postgres: applier: build insert for %s.%s: %w", schema, v.Table, err)
		}
		if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
			return fmt.Errorf("postgres: applier: insert into %s.%s: %w", schema, v.Table, err)
		}
		return nil

	case ir.Update:
		schema := applierSchema(a.schema, v.Schema)
		colTypes, err := a.colTypesFor(ctx, schema, v.Table)
		if err != nil {
			return fmt.Errorf("postgres: applier: column types for %s.%s: %w", schema, v.Table, err)
		}
		stmt, args, err := buildUpdateSQL(schema, v.Table, v.Before, v.After, colTypes)
		if err != nil {
			return fmt.Errorf("postgres: applier: build update for %s.%s: %w", schema, v.Table, err)
		}
		// Update misses are tolerated (zero rows affected) for resume
		// idempotency; the same caveat as MySQL applies — see the
		// MySQL applier's dispatch comment for the rationale and the
		// debug-log defence-in-depth.
		res, err := tx.ExecContext(ctx, stmt, args...)
		if err != nil {
			return fmt.Errorf("postgres: applier: update %s.%s: %w", schema, v.Table, err)
		}
		logZeroRowsAffected(ctx, "update", schema, v.Table, res)
		return nil

	case ir.Delete:
		schema := applierSchema(a.schema, v.Schema)
		colTypes, err := a.colTypesFor(ctx, schema, v.Table)
		if err != nil {
			return fmt.Errorf("postgres: applier: column types for %s.%s: %w", schema, v.Table, err)
		}
		stmt, args, err := buildDeleteSQL(schema, v.Table, v.Before, colTypes)
		if err != nil {
			return fmt.Errorf("postgres: applier: build delete for %s.%s: %w", schema, v.Table, err)
		}
		res, err := tx.ExecContext(ctx, stmt, args...)
		if err != nil {
			return fmt.Errorf("postgres: applier: delete from %s.%s: %w", schema, v.Table, err)
		}
		logZeroRowsAffected(ctx, "delete", schema, v.Table, res)
		return nil

	case ir.Truncate:
		schema := applierSchema(a.schema, v.Schema)
		stmt := buildTruncateSQL(schema, v.Table)
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("postgres: applier: truncate %s.%s: %w", schema, v.Table, err)
		}
		return nil
	}
	return fmt.Errorf("postgres: applier: unknown change type %T", c)
}

// logZeroRowsAffected emits a debug-level log line when a target Exec
// reports zero rows affected. Mirrors the MySQL applier's helper of
// the same name; see that file for the full rationale (Bug-6 silent-
// divergence visibility without violating resume idempotency).
func logZeroRowsAffected(ctx context.Context, op, schema, table string, res sql.Result) {
	if res == nil {
		return
	}
	n, err := res.RowsAffected()
	if err != nil {
		return
	}
	if n == 0 {
		slog.DebugContext(ctx, "postgres: applier: zero rows affected",
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
	qn := schema + "." + table
	if cached, ok := a.pkCache[qn]; ok {
		return cached, nil
	}
	pk, err := loadPrimaryKey(ctx, tx, schema, table)
	if err != nil {
		return nil, err
	}
	a.pkCache[qn] = pk
	return pk, nil
}

// colTypesFor returns the cached column-name → IR type map for the
// named table, loading it on the first sight of the table. The map
// is consulted for every value the applier binds so prepareValue can
// shape Array / Geometry / future special-cased values for pgx.
//
// The lookup uses information_schema directly (the same source the
// schema reader's populateColumns uses) and runs the existing
// translateType to produce IR types — keeping the applier's view of
// "what does this column hold" in lockstep with the rest of the
// engine.
func (a *ChangeApplier) colTypesFor(ctx context.Context, schema, table string) (map[string]ir.Type, error) {
	qn := schema + "." + table
	if cached, ok := a.colTypeCache[qn]; ok {
		return cached, nil
	}
	out, err := loadColumnTypes(ctx, a.db, schema, table)
	if err != nil {
		return nil, err
	}
	a.colTypeCache[qn] = out
	return out, nil
}

// loadColumnTypes queries information_schema for the column list of
// schema.table and produces a column-name → IR type map. Mirrors
// SchemaReader.populateColumns minus the index/FK/PK plumbing the
// applier doesn't need.
//
// Enum-valued columns are resolved through readEnumValuesForSchema,
// which loads the enum type values in the same call. Arrays of
// supported scalar element types are also resolved. Geometry is left
// without per-column subtype/SRID metadata — the applier doesn't
// re-encode geometry on the write path's IR-Geometry → EWKB step
// (that happens via prepareValue using the column's SRID, which here
// defaults to 0; row_writer's PostGIS-aware path is the canonical
// place to recover the SRID, and the applier does not currently do
// so for replicated UPDATE/DELETE rows). When the cross-engine
// PostGIS replication path lands we'll need to extend this to read
// the geometry_columns view too.
func loadColumnTypes(ctx context.Context, db *sql.DB, schema, table string) (map[string]ir.Type, error) {
	enumValues, err := readEnumValuesForSchema(ctx, db, schema)
	if err != nil {
		return nil, fmt.Errorf("postgres: applier: read enum values: %w", err)
	}

	const q = `
		SELECT
			column_name,
			LOWER(data_type),
			udt_name,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			datetime_precision,
			is_identity,
			column_default
		FROM   information_schema.columns
		WHERE  table_schema = $1
		  AND  table_name   = $2
		ORDER  BY ordinal_position`

	rows, err := db.QueryContext(ctx, q, schema, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := map[string]ir.Type{}
	for rows.Next() {
		var (
			colName, dataType, udtName string
			charMaxLen, numPrec        sql.NullInt64
			numScale, dtPrec           sql.NullInt64
			isIdentity                 string
			columnDefault              sql.NullString
		)
		if err := rows.Scan(
			&colName, &dataType, &udtName,
			&charMaxLen, &numPrec, &numScale, &dtPrec,
			&isIdentity, &columnDefault,
		); err != nil {
			return nil, err
		}

		meta := columnMeta{
			DataType:        dataType,
			UDTName:         udtName,
			CharMaxLen:      nullInt64ToPtr(charMaxLen),
			NumPrec:         nullInt64ToPtr(numPrec),
			NumScale:        nullInt64ToPtr(numScale),
			DTPrec:          nullInt64ToPtr(dtPrec),
			IsAutoIncrement: isAutoIncrement(isIdentity, columnDefault),
		}
		if dataType == "user-defined" || dataType == "USER-DEFINED" {
			if values, ok := enumValues[udtName]; ok {
				meta.EnumValues = values
			}
		}
		if dataType == "array" || dataType == "ARRAY" {
			elemDataType, ok := arrayElementDataType(udtName)
			if !ok {
				// Unknown array element types are surfaced (rather
				// than silently passing through) for the same reason
				// the schema reader surfaces them: an applier that
				// can't shape the value will fail on the wire and the
				// operator deserves a precise message.
				return nil, fmt.Errorf("postgres: applier: array column %s.%s has unsupported element type %q", table, colName, udtName)
			}
			meta.ArrayElement = &columnMeta{
				DataType: elemDataType,
				UDTName:  strings.TrimPrefix(udtName, "_"),
			}
		}

		typ, err := translateType(meta)
		if err != nil {
			return nil, fmt.Errorf("postgres: applier: translate %s.%s: %w", table, colName, err)
		}
		out[colName] = typ
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("postgres: applier: table %s.%s has no columns (does it exist?)", schema, table)
	}
	return out, nil
}

// readEnumValuesForSchema is the standalone variant of
// SchemaReader.readEnumValues — same query, no receiver, callable
// from the applier without instantiating a SchemaReader.
func readEnumValuesForSchema(ctx context.Context, db *sql.DB, schema string) (map[string][]string, error) {
	const q = `
		SELECT t.typname, e.enumlabel
		FROM   pg_enum e
		JOIN   pg_type t      ON t.oid = e.enumtypid
		JOIN   pg_namespace n ON n.oid = t.typnamespace
		WHERE  n.nspname = $1
		ORDER  BY t.typname, e.enumsortorder`

	rows, err := db.QueryContext(ctx, q, schema)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := map[string][]string{}
	for rows.Next() {
		var typname, label string
		if err := rows.Scan(&typname, &label); err != nil {
			return nil, err
		}
		out[typname] = append(out[typname], label)
	}
	return out, rows.Err()
}

// applierSchema picks the schema name to use in SQL. The applier's
// configured schema (a.schema, derived from the target DSN) is
// authoritative — it is the destination database the operator
// pointed sluice at. The change's source-side schema is metadata
// only; using it would route writes to a same-named schema on the
// target, which is wrong whenever source and target schema names
// differ (e.g. cross-engine MySQL source_db → PG public). v.Schema
// is honoured only as a fallback when the applier wasn't configured
// with one — which shouldn't happen in practice but keeps the
// function total.
func applierSchema(defaultSchema, changeSchema string) string {
	if defaultSchema != "" {
		return defaultSchema
	}
	return changeSchema
}

// loadPrimaryKey reads the PK columns for the named table from
// pg_index. Returns an empty slice (not nil) for tables with no PK.
// Uses pg_index directly rather than information_schema.table_
// constraints so we get the column order from indkey natively.
func loadPrimaryKey(ctx context.Context, tx *sql.Tx, schema, table string) ([]string, error) {
	const q = `
		SELECT a.attname
		FROM   pg_index ix
		JOIN   pg_class      cl ON cl.oid = ix.indrelid
		JOIN   pg_namespace  n  ON n.oid  = cl.relnamespace
		JOIN   LATERAL unnest(ix.indkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE
		LEFT JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = u.attnum
		WHERE  n.nspname = $1
		  AND  cl.relname = $2
		  AND  ix.indisprimary
		ORDER  BY u.ord`

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
// uses ON CONFLICT (pk) DO UPDATE:
//
//	INSERT INTO "s"."t" ("a", "b", "id") VALUES ($1, $2, $3)
//	ON CONFLICT ("id") DO UPDATE SET "a" = EXCLUDED."a", "b" = EXCLUDED."b"
//
// With an empty PK list (tables without a PRIMARY KEY), falls back
// to a plain INSERT — see the ChangeApplier package doc for the
// resume-idempotency caveat.
//
// colTypes maps column names to their IR types and is the input to
// prepareValue. A missing entry (nil map, or column not present) is
// tolerated and the raw value is bound — preserving the pre-Bug-6
// shape so unit tests without a populated cache still produce valid
// SQL.
func buildInsertSQL(schema, table string, row ir.Row, pk []string, colTypes map[string]ir.Type) (sqlStmt string, args []any, err error) {
	cols := sortedKeys(row)
	args = make([]any, 0, len(cols))
	colSQL := make([]string, len(cols))
	for i, c := range cols {
		colSQL[i] = quoteIdent(c)
		v, perr := prepareApplierValue(row[c], colTypes, c)
		if perr != nil {
			return "", nil, fmt.Errorf("column %q: %w", c, perr)
		}
		args = append(args, v)
	}

	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	placeholders := make([]string, len(cols))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableRef)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(colSQL, ", "))
	sb.WriteString(") VALUES (")
	sb.WriteString(strings.Join(placeholders, ", "))
	sb.WriteByte(')')

	if len(pk) > 0 {
		// ON CONFLICT (pk) DO UPDATE SET non-pk-cols = EXCLUDED.non-pk-cols
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

		conflictTarget := make([]string, len(pk))
		for i, p := range pk {
			conflictTarget[i] = quoteIdent(p)
		}
		sb.WriteString(" ON CONFLICT (")
		sb.WriteString(strings.Join(conflictTarget, ", "))
		sb.WriteByte(')')

		if len(nonPK) > 0 {
			sb.WriteString(" DO UPDATE SET ")
			parts := make([]string, len(nonPK))
			for i, c := range nonPK {
				parts[i] = fmt.Sprintf("%s = EXCLUDED.%s", quoteIdent(c), quoteIdent(c))
			}
			sb.WriteString(strings.Join(parts, ", "))
		} else {
			// All columns are PK — nothing to update on conflict.
			// DO NOTHING absorbs the conflict silently.
			sb.WriteString(" DO NOTHING")
		}
	}
	return sb.String(), args, nil
}

// buildUpdateSQL builds an UPDATE statement. SET uses every column
// in After (unchanged-column detection is a v1.5 optimization).
// WHERE uses every column in Before with NULL-aware predicate
// building.
func buildUpdateSQL(schema, table string, before, after ir.Row, colTypes map[string]ir.Type) (sqlStmt string, args []any, err error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	setSQL, setArgs, err := buildSetClause(after, 1, colTypes)
	if err != nil {
		return "", nil, err
	}
	whereSQL, whereArgs, err := buildWhereClause(before, len(setArgs)+1, colTypes)
	if err != nil {
		return "", nil, err
	}

	args = make([]any, 0, len(setArgs)+len(whereArgs))
	args = append(args, setArgs...)
	args = append(args, whereArgs...)
	return "UPDATE " + tableRef + " SET " + setSQL + " WHERE " + whereSQL, args, nil
}

// buildDeleteSQL builds a DELETE statement using the Before image
// as the WHERE predicate.
func buildDeleteSQL(schema, table string, before ir.Row, colTypes map[string]ir.Type) (sqlStmt string, args []any, err error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(table)
	whereSQL, whereArgs, err := buildWhereClause(before, 1, colTypes)
	if err != nil {
		return "", nil, err
	}
	return "DELETE FROM " + tableRef + " WHERE " + whereSQL, whereArgs, nil
}

// buildTruncateSQL builds a TRUNCATE TABLE statement.
func buildTruncateSQL(schema, table string) string {
	return "TRUNCATE TABLE " + quoteIdent(schema) + "." + quoteIdent(table)
}

// buildSetClause renders "col1 = $N, col2 = $N+1" for an UPDATE SET.
// startIdx is the next available placeholder number — Postgres uses
// numbered placeholders (unlike MySQL's `?`), so a SET + WHERE
// combination needs to share a sequence.
func buildSetClause(row ir.Row, startIdx int, colTypes map[string]ir.Type) (clause string, args []any, err error) {
	cols := sortedKeys(row)
	parts := make([]string, len(cols))
	args = make([]any, 0, len(cols))
	for i, c := range cols {
		parts[i] = fmt.Sprintf("%s = $%d", quoteIdent(c), startIdx+i)
		v, perr := prepareApplierValue(row[c], colTypes, c)
		if perr != nil {
			return "", nil, fmt.Errorf("column %q: %w", c, perr)
		}
		args = append(args, v)
	}
	return strings.Join(parts, ", "), args, nil
}

// buildWhereClause renders an AND-joined predicate with NULL-aware
// handling: nil row values produce "col IS NULL" (no parameter) so
// SQL's NULL semantics don't make the predicate unsatisfiable.
// startIdx is the next available placeholder number.
func buildWhereClause(row ir.Row, startIdx int, colTypes map[string]ir.Type) (clause string, args []any, err error) {
	cols := sortedKeys(row)
	parts := make([]string, 0, len(cols))
	args = make([]any, 0, len(cols))
	idx := startIdx
	for _, c := range cols {
		v := row[c]
		if v == nil {
			parts = append(parts, quoteIdent(c)+" IS NULL")
			continue
		}
		parts = append(parts, fmt.Sprintf("%s = $%d", quoteIdent(c), idx))
		prepared, perr := prepareApplierValue(v, colTypes, c)
		if perr != nil {
			return "", nil, fmt.Errorf("column %q: %w", c, perr)
		}
		args = append(args, prepared)
		idx++
	}
	return strings.Join(parts, " AND "), args, nil
}

// prepareApplierValue is the applier's wrapper around prepareValue.
// It looks up the column's IR type and routes the value through the
// shared shaping helper from row_writer.go. When the column isn't in
// the map (cache cold or column unknown — defensive), it falls back
// to the raw value, preserving the pre-Bug-6 shape.
//
// Routing through the shared helper keeps any future shaping rules
// added to prepareValue (for new IR types or new corner cases)
// automatically picked up by the applier without touching this file.
func prepareApplierValue(v any, colTypes map[string]ir.Type, colName string) (any, error) {
	if colTypes == nil {
		return v, nil
	}
	t, ok := colTypes[colName]
	if !ok {
		return v, nil
	}
	return prepareValue(v, t)
}

// (sortedKeys is shared with the schema reader — see schema_reader.go
// for the implementation. The applier uses it to render generated SQL
// in a deterministic column order.)
