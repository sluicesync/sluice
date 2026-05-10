// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/orware/sluice/internal/ir"
)

// defaultMaxRowsPerBatch caps how many rows go into a single INSERT
// statement on the batched-insert path. Conservative for now; can
// tune with real-world data.
const defaultMaxRowsPerBatch = 500

// defaultMaxBufferBytes is the soft per-batch byte cap when the
// caller doesn't set one explicitly. Bounds heap usage at ~64 MiB
// for wide-row workloads; tunable via --max-buffer-bytes. See
// ADR-0028.
const defaultMaxBufferBytes int64 = 64 << 20 // 64 MiB

// RowWriter performs bulk inserts into PostgreSQL tables. It
// implements [ir.RowWriter].
//
// Two strategies are supported, selected by useCopy:
//
//   - **COPY FROM STDIN** (default for vanilla Postgres). Uses pgx's
//     CopyFrom against the underlying *pgx.Conn — 3-5× the
//     throughput of multi-row INSERT for bulk loads, and the
//     canonical Postgres bulk-load protocol.
//   - **Batched multi-row INSERT** (fallback). Builds parameterised
//     INSERT ... VALUES (...) statements and Execs them through
//     database/sql. Retained for engines whose BulkLoad capability
//     declines COPY, and as the strategy unit/integration tests can
//     force when they need to exercise this path.
//
// OpenRowWriter consults the engine's [ir.Capabilities.BulkLoad] to
// decide; vanilla PG declares BulkLoadCopy → useCopy == true.
type RowWriter struct {
	db     *sql.DB
	schema string

	// useCopy selects the bulk-load strategy. true → writeViaCopy;
	// false → writeViaBatch (the original batched-insert path).
	useCopy bool

	// hasPostGIS records whether the target database has the postgis
	// extension installed. Set at engine open time via detectPostGIS.
	// Drives the value-side conversion for ir.Geometry columns: when
	// true, prepareValue wraps WKB bytes in PostGIS EWKB framing
	// using the column's SRID; when false, ir.Geometry columns are
	// rejected at the schema phase before any rows reach the writer.
	hasPostGIS bool

	// maxRowsPerBatch caps the number of rows folded into a single
	// INSERT on the batched path. Tests can override; callers leave
	// it at zero (which causes defaultMaxRowsPerBatch to be used).
	// Ignored on the COPY path.
	maxRowsPerBatch int

	// maxBufferBytes is the soft byte-size cap on per-batch buffered
	// row values. Implements [ir.MaxBufferBytesSetter] via
	// [SetMaxBufferBytes]. Zero or negative means "no byte cap"; the
	// row-count cap (maxRowsPerBatch) remains the only flush
	// trigger. The COPY path also honours this — pgx CopyFrom drains
	// rows from a generator we control, so the source can flush
	// early on byte accumulation. See ADR-0028.
	maxBufferBytes int64
}

// SetMaxBufferBytes implements [ir.MaxBufferBytesSetter]. The
// orchestrator calls this immediately after [Engine.OpenRowWriter]
// returns when --max-buffer-bytes is set, before WriteRows runs.
// Zero or negative means "no byte cap"; the row-count cap remains
// the only flush trigger.
func (w *RowWriter) SetMaxBufferBytes(bytes int64) {
	w.maxBufferBytes = bytes
}

// SetSchema implements [ir.SchemaSetter]. Called by the pipeline
// orchestrator when `--target-schema NAME` is set (ADR-0031). The
// row writer's bulk-load + DROP / TRUNCATE / DELETE statements
// target the named schema rather than the DSN's default. Empty
// input is a no-op.
func (w *RowWriter) SetSchema(name string) {
	if name == "" {
		return
	}
	w.schema = name
}

// Close releases the underlying connection pool.
func (w *RowWriter) Close() error {
	if w.db == nil {
		return nil
	}
	return w.db.Close()
}

// TruncateTable empties the target table. Used by the resume path in
// [pipeline.Migrator] to clear an `in_progress` table before
// re-running its bulk copy. Implements [ir.TableTruncator].
//
// TRUNCATE in Postgres is fast (it doesn't scan rows) and acquires
// ACCESS EXCLUSIVE — fine here because the resume path runs single-
// threaded. RESTART IDENTITY isn't applied: the orchestrator's
// SyncIdentitySequences phase will reconcile sequences after the
// re-copy finishes.
func (w *RowWriter) TruncateTable(ctx context.Context, table *ir.Table) error {
	if table == nil {
		return errors.New("postgres: TruncateTable: table is nil")
	}
	stmt := "TRUNCATE TABLE " + quoteIdent(w.schema) + "." + quoteIdent(table.Name)
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("postgres: truncate %q: %w", table.Name, err)
	}
	return nil
}

// DropTable drops the target table with CASCADE so dependent foreign
// keys, views, and constraints come down with it. Used by the
// `--reset-target-data` recovery path (ADR-0023). Implements
// [ir.TableDropper].
//
// IF EXISTS keeps the call idempotent: a partial-failure retry that
// already dropped some tables is not an error on the second pass. The
// schema readers exclude `sluice_*_state` tables, so the bookkeeping
// row is cleared via [MigrationStateStore.ClearMigration] /
// [ChangeApplier.ClearStream] rather than ever reaching this method.
func (w *RowWriter) DropTable(ctx context.Context, table *ir.Table) error {
	if table == nil {
		return errors.New("postgres: DropTable: table is nil")
	}
	stmt := "DROP TABLE IF EXISTS " + quoteIdent(w.schema) + "." + quoteIdent(table.Name) + " CASCADE"
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("postgres: drop %q: %w", table.Name, err)
	}
	return nil
}

// DropTables drops every named table with one DROP TABLE statement.
// Implements [ir.BulkTableDropper] for the reset path on databases
// with many tables — collapses N round-trips into one. CASCADE is
// applied once at statement level (PG accepts only one CASCADE per
// statement) so foreign keys, views, and dependent constraints come
// down with the listed tables. IF EXISTS preserves idempotency.
//
// An empty input list is a no-op; nil entries are skipped silently
// (the per-table DropTable rejects nil with an error, but a bulk caller
// passing a nil-padded slice is more often a programming convenience
// than an error case worth surfacing).
func (w *RowWriter) DropTables(ctx context.Context, tables []*ir.Table) error {
	if len(tables) == 0 {
		return nil
	}
	parts := make([]string, 0, len(tables))
	for _, t := range tables {
		if t == nil {
			continue
		}
		parts = append(parts, quoteIdent(w.schema)+"."+quoteIdent(t.Name))
	}
	if len(parts) == 0 {
		return nil
	}
	stmt := "DROP TABLE IF EXISTS " + strings.Join(parts, ", ") + " CASCADE"
	if _, err := w.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("postgres: drop %d tables: %w", len(parts), err)
	}
	return nil
}

// DropSchemaTypes drops every Postgres enum type that the source IR
// schema would create on a fresh cold-start. Used by the
// `--reset-target-data` recovery path (ADR-0023) to fix Bug 18: when
// a partial cold-start fails after CREATE TYPE but before the table
// is committed, the next reset attempt drops the table cleanly but
// leaves the enum behind, causing the re-run's CREATE TYPE to fail
// with "type X already exists".
//
// The names walked here mirror [enumTypeName] — the same convention
// the schema writer uses when creating the types — so the drop list
// is exactly the set sluice would have created. Enum types belonging
// to other applications on a shared target database are not touched.
//
// Implements [ir.SchemaTypeDropper]. Order matters: callers must drop
// tables first (columns may reference these types). DROP TYPE IF
// EXISTS ... CASCADE keeps the call idempotent across partial-failure
// retries and handles the corner case where a stray dependent column
// outlives the table drop.
func (w *RowWriter) DropSchemaTypes(ctx context.Context, schema *ir.Schema) error {
	if schema == nil {
		return nil
	}
	for _, table := range schema.Tables {
		if table == nil {
			continue
		}
		for _, col := range table.Columns {
			if _, isEnum := col.Type.(ir.Enum); !isEnum {
				continue
			}
			stmt := "DROP TYPE IF EXISTS " + quoteIdent(w.schema) + "." + quoteIdent(enumTypeName(table.Name, col.Name)) + " CASCADE"
			if _, err := w.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("postgres: drop enum type for %s.%s: %w", table.Name, col.Name, err)
			}
		}
	}
	return nil
}

// IsTableEmpty reports whether the target table has no rows. A
// missing table is treated as empty so the cold-start pre-flight
// doesn't double up with the subsequent CREATE TABLE IF NOT EXISTS
// step. Implements [ir.TableEmptyChecker].
//
// We use SELECT 1 ... LIMIT 1 rather than COUNT(*) so the cost is
// constant regardless of table size — the pre-flight only needs to
// know "is anything there", not "how many rows".
func (w *RowWriter) IsTableEmpty(ctx context.Context, table *ir.Table) (bool, error) {
	if table == nil {
		return false, errors.New("postgres: IsTableEmpty: table is nil")
	}
	q := "SELECT 1 FROM " + quoteIdent(w.schema) + "." + quoteIdent(table.Name) + " LIMIT 1"
	var dummy int
	err := w.db.QueryRowContext(ctx, q).Scan(&dummy)
	if err == nil {
		return false, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	// Postgres SQLSTATE 42P01 = undefined_table. The driver surfaces
	// it as a *pgconn.PgError; we check the message text rather than
	// importing pgconn here just for one check.
	if strings.Contains(err.Error(), "does not exist") {
		return true, nil
	}
	return false, fmt.Errorf("postgres: probe %q for emptiness: %w", table.Name, err)
}

// WriteRows is the dispatcher. Validates inputs, then routes to the
// strategy chosen by useCopy. See [ir.RowWriter.WriteRows] for the
// contract.
func (w *RowWriter) WriteRows(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	if table == nil {
		return errors.New("postgres: WriteRows: table is nil")
	}
	if len(table.Columns) == 0 {
		return fmt.Errorf("postgres: WriteRows: table %q has no columns", table.Name)
	}
	if rows == nil {
		return errors.New("postgres: WriteRows: rows channel is nil")
	}
	if w.useCopy {
		return w.writeViaCopy(ctx, table, rows)
	}
	return w.writeViaBatch(ctx, table, rows)
}

// writeViaCopy runs Postgres COPY FROM STDIN for one table. It pins
// a single connection from the pool, escapes database/sql via
// Conn.Raw to reach the underlying *pgx.Conn, and feeds rows
// through pgx's CopyFrom + our chanCopySource adapter.
//
// Before launching CopyFrom, any extension-owned column types whose
// wire format pgx doesn't natively understand (today: pgvector's
// `vector`) get a per-conn pgtype codec registered against the
// runtime OID resolved from pg_type. Without this, pgx's binary COPY
// path silently routes vector values through the `text` codec — the
// canonical text form ships as raw bytes inside a binary-format
// column, and the receiver's `vector_in()` parser interprets the
// first two bytes as a big-endian dimension count and refuses with
// "vector cannot have more than 16000 dimensions" (Bug 47, ADR-0032).
//
// COPY is atomic at the table level: a mid-stream error rolls back
// the entire copy. The error message names how many rows landed
// before the failure so operators can scope the impact.
func (w *RowWriter) writeViaCopy(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	sqlConn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("postgres: copy: acquire conn: %w", err)
	}
	defer func() { _ = sqlConn.Close() }() // returns conn to pool

	cols := nonGeneratedColumns(table.Columns)
	columnNames := make([]string, len(cols))
	for i, c := range cols {
		columnNames[i] = c.Name
	}
	source := newChanCopySource(ctx, table, rows)

	needsPGVectorCodec := tableHasPGVectorColumn(table)

	var copied int64
	rawErr := sqlConn.Raw(func(driverConn any) error {
		stdlibConn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("postgres: copy: expected *stdlib.Conn, got %T", driverConn)
		}
		conn := stdlibConn.Conn()
		if needsPGVectorCodec {
			if err := registerPGVectorCodec(ctx, conn, w.db); err != nil {
				return err
			}
		}
		n, copyErr := conn.CopyFrom(
			ctx,
			pgx.Identifier{w.schema, table.Name},
			columnNames,
			source,
		)
		copied = n
		return copyErr
	})
	if rawErr != nil {
		return fmt.Errorf("postgres: copy into %q (%d rows copied before error): %w",
			table.Name, copied, rawErr)
	}
	return nil
}

// tableHasPGVectorColumn reports whether table has any column whose
// IR type is the pgvector ExtensionType. Used to decide whether the
// COPY path needs to register the per-conn pgtype codec.
func tableHasPGVectorColumn(table *ir.Table) bool {
	for _, col := range table.Columns {
		ext, ok := col.Type.(ir.ExtensionType)
		if !ok {
			continue
		}
		if ext.Extension == "vector" {
			return true
		}
	}
	return false
}

// registerPGVectorCodec resolves the runtime OID for the `vector` type
// and registers [pgvectorBinaryCodec] against it on conn. The lookup
// runs against the *sql.DB pool (not conn) because pgx's *Conn here
// is mid-Raw and using it for an out-of-band query is awkward; the
// pool query lands on a sibling conn but pg_type is database-global,
// so the OID is the same. Idempotent: re-registering on a conn that
// already has the type is harmless.
func registerPGVectorCodec(ctx context.Context, conn *pgx.Conn, db *sql.DB) error {
	tm := conn.TypeMap()
	if _, alreadyRegistered := tm.TypeForName("vector"); alreadyRegistered {
		return nil
	}
	oid, err := lookupVectorOID(ctx, db)
	if err != nil {
		return fmt.Errorf("postgres: copy: register pgvector codec: %w", err)
	}
	tm.RegisterType(&pgtype.Type{
		Name:  "vector",
		OID:   oid,
		Codec: pgvectorBinaryCodec{},
	})
	return nil
}

// writeViaBatch is the fallback batched-INSERT path. Builds
// parameterised INSERT ... VALUES (...) statements and Execs them
// through database/sql. Retained for engines whose BulkLoad
// capability declines COPY (none today, but the hook is here).
//
// The flush trigger fires on whichever cap hits first: the row-count
// cap (maxRowsPerBatch) or the byte-size cap (maxBufferBytes,
// ADR-0028). The byte cap is a soft target — a single row larger
// than the cap still applies (the row's already in `batch` when the
// post-append check fires), bounded only by the engine's own
// per-statement limits. This matches what pscale-cli's batcher does:
// flush at ~1 MB of statement body rather than a fixed row count.
func (w *RowWriter) writeViaBatch(ctx context.Context, table *ir.Table, rows <-chan ir.Row) error {
	limit := w.maxRowsPerBatch
	if limit <= 0 {
		limit = defaultMaxRowsPerBatch
	}
	byteCap := w.maxBufferBytes
	if byteCap <= 0 {
		byteCap = defaultMaxBufferBytes
	}

	batch := make([]ir.Row, 0, limit)
	var batchBytes int64
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		query := buildBatchInsert(w.schema, table, len(batch))
		args, err := flattenArgs(batch, table)
		if err != nil {
			return fmt.Errorf("postgres: prepare args for %q: %w", table.Name, err)
		}
		if _, err := w.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("postgres: insert into %q (%d rows): %w", table.Name, len(batch), err)
		}
		batch = batch[:0]
		batchBytes = 0
		return nil
	}

	for {
		select {
		case row, ok := <-rows:
			if !ok {
				return flush()
			}
			batch = append(batch, row)
			batchBytes += ir.ApproximateRowBytes(row)
			if len(batch) >= limit || batchBytes >= byteCap {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// buildBatchInsert returns the parameterised INSERT statement for the
// given table and row count. Generated columns are excluded — the
// reader doesn't emit values for them, and INSERT into a generated
// column is a database error. Postgres uses $1, $2, ... placeholders
// (numbered, not positional like MySQL's ?).
//
// The numbering is global across rows: row 1 is $1..$N, row 2 is
// $(N+1)..$(2N), etc.
func buildBatchInsert(schema string, table *ir.Table, rowCount int) string {
	cols := nonGeneratedColumns(table.Columns)
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = quoteIdent(c.Name)
	}

	numCols := len(cols)
	rowParts := make([]string, rowCount)
	paramIdx := 0
	for i := range rowParts {
		params := make([]string, numCols)
		for j := range params {
			paramIdx++
			params[j] = fmt.Sprintf("$%d", paramIdx)
		}
		rowParts[i] = "(" + strings.Join(params, ", ") + ")"
	}

	tableRef := quoteIdent(schema) + "." + quoteIdent(table.Name)
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		tableRef,
		strings.Join(colNames, ", "),
		strings.Join(rowParts, ", "),
	)
}

// flattenArgs walks the batch column-major-by-row and produces the
// flat []any the driver expects, with each value passed through
// prepareValue for any IR-canonical → driver-acceptable adjustments.
// Generated columns are skipped so the column-list and value-list
// stay in lockstep with buildBatchInsert.
func flattenArgs(batch []ir.Row, table *ir.Table) ([]any, error) {
	cols := nonGeneratedColumns(table.Columns)
	args := make([]any, 0, len(batch)*len(cols))
	for _, row := range batch {
		for _, col := range cols {
			v, err := prepareValue(row[col.Name], col.Type)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", col.Name, err)
			}
			args = append(args, v)
		}
	}
	return args, nil
}

// prepareValue adjusts an IR Row value into a form pgx will accept.
//
// Most IR-canonical Go values pass through to pgx unchanged: bool,
// int64, float64, string, []byte, time.Time, and nil all serialise
// correctly without intervention. The exceptions are:
//
//   - [ir.Array] values, whose canonical Go form is []any; pgx wants
//     a typed slice for native array binding. We convert based on the
//     element type.
//   - [ir.Geometry] values, whose canonical Go form is raw WKB bytes
//     (per docs/value-types.md); PostGIS columns expect EWKB framing
//     with the SRID encoded in the type byte. wkbToEWKB handles the
//     conversion and is a no-op when the value is already EWKB.
//   - Nothing else (Postgres handles the rest natively via pgx).
//
// Returning an error here means the IR value didn't match the
// declared column type — usually a translator bug upstream.
func prepareValue(v any, t ir.Type) (any, error) {
	if v == nil {
		return nil, nil
	}

	if arr, isArr := t.(ir.Array); isArr {
		elems, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("expected []any for Array column, got %T", v)
		}
		return convertArray(elems, arr.Element)
	}

	if geom, isGeom := t.(ir.Geometry); isGeom {
		b, ok := v.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte for Geometry column, got %T", v)
		}
		// nil-but-typed empty slice is meaningless for geometry;
		// surface it rather than producing malformed EWKB.
		if len(b) == 0 {
			return nil, errors.New("geometry column has empty bytes")
		}
		ewkb, err := wkbToEWKB(b, uint32(geom.SRID))
		if err != nil {
			return nil, fmt.Errorf("wrap WKB → EWKB: %w", err)
		}
		return ewkb, nil
	}

	// ADR-0032: ExtensionType columns pass through as their decoded
	// shape — pgvector emits as `[1,2,3]`-style strings under pgx
	// stdlib mode, which PG's `vector` parser accepts on the INSERT
	// side. Bytes are also accepted (a future extension's
	// binary-format codec would land here). Any other Go type is a
	// translator bug upstream.
	if _, isExt := t.(ir.ExtensionType); isExt {
		switch x := v.(type) {
		case string, []byte:
			return x, nil
		}
		return nil, fmt.Errorf("expected string or []byte for ExtensionType column, got %T", v)
	}

	return v, nil
}

// convertArray turns []any (the IR canonical form for arrays) into a
// typed Go slice that pgx can serialise as a Postgres array.
//
// We support the element types most common in practice. For others,
// the function returns an error so the upstream caller knows to
// translate first.
func convertArray(v []any, elem ir.Type) (any, error) {
	switch elem.(type) {
	case ir.Boolean:
		out := make([]bool, len(v))
		for i, e := range v {
			b, ok := e.(bool)
			if !ok {
				return nil, fmt.Errorf("array element %d: expected bool, got %T", i, e)
			}
			out[i] = b
		}
		return out, nil
	case ir.Integer:
		out := make([]int64, len(v))
		for i, e := range v {
			n, ok := e.(int64)
			if !ok {
				return nil, fmt.Errorf("array element %d: expected int64, got %T", i, e)
			}
			out[i] = n
		}
		return out, nil
	case ir.Float:
		out := make([]float64, len(v))
		for i, e := range v {
			f, ok := e.(float64)
			if !ok {
				return nil, fmt.Errorf("array element %d: expected float64, got %T", i, e)
			}
			out[i] = f
		}
		return out, nil
	case ir.Char, ir.Varchar, ir.Text, ir.UUID, ir.Inet, ir.Cidr, ir.Macaddr, ir.Decimal, ir.Time:
		out := make([]string, len(v))
		for i, e := range v {
			s, ok := e.(string)
			if !ok {
				return nil, fmt.Errorf("array element %d: expected string, got %T", i, e)
			}
			out[i] = s
		}
		return out, nil
	}
	return nil, fmt.Errorf("postgres: array of element type %T not supported", elem)
}
