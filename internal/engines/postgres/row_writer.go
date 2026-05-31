// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

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

// HasNullShardColumn reports whether the named discriminator column
// exists on the target table AND at least one existing row has it
// NULL. ADR-0048 Shape A populated-target preflight check (1);
// catalog Bug 81. Returns (false, nil) when:
//   - the table doesn't exist (the orchestrator's empty-check
//     short-circuits earlier; defensive),
//   - the column doesn't exist on the target (caught structurally —
//     CompositePKLeadsWith later refuses for the same reason), OR
//   - every row has the column NOT NULL (the legal Shape-A shape).
//
// A genuine engine error (permission denied, network drop) surfaces
// verbatim; the orchestrator wraps with operator-actionable context.
func (w *RowWriter) HasNullShardColumn(ctx context.Context, table *ir.Table, column string) (bool, error) {
	if table == nil {
		return false, errors.New("postgres: HasNullShardColumn: table is nil")
	}
	exists, err := w.columnExistsOnTarget(ctx, table.Name, column)
	if err != nil {
		return false, fmt.Errorf("postgres: HasNullShardColumn: %w", err)
	}
	if !exists {
		return false, nil
	}
	q := "SELECT 1 FROM " + quoteIdent(w.schema) + "." + quoteIdent(table.Name) +
		" WHERE " + quoteIdent(column) + " IS NULL LIMIT 1"
	var dummy int
	err = w.db.QueryRowContext(ctx, q).Scan(&dummy)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, fmt.Errorf("postgres: probe %q for NULL %q: %w", table.Name, column, err)
}

// ShardValuePresent reports whether the named discriminator column
// on the target table already has any row matching `value`. ADR-0048
// Shape A populated-target preflight check (2). Returns (false, nil)
// when the column is absent (CompositePKLeadsWith catches that case
// structurally) or no matching row exists. Catalog Bug 81.
func (w *RowWriter) ShardValuePresent(ctx context.Context, table *ir.Table, column string, value any) (bool, error) {
	if table == nil {
		return false, errors.New("postgres: ShardValuePresent: table is nil")
	}
	exists, err := w.columnExistsOnTarget(ctx, table.Name, column)
	if err != nil {
		return false, fmt.Errorf("postgres: ShardValuePresent: %w", err)
	}
	if !exists {
		return false, nil
	}
	q := "SELECT 1 FROM " + quoteIdent(w.schema) + "." + quoteIdent(table.Name) +
		" WHERE " + quoteIdent(column) + " = $1 LIMIT 1"
	var dummy int
	err = w.db.QueryRowContext(ctx, q, value).Scan(&dummy)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, fmt.Errorf("postgres: probe %q for %q = %v: %w", table.Name, column, value, err)
}

// CompositePKLeadsWith reports whether the target table has a
// composite PRIMARY KEY (>1 column) whose leading column is the
// named discriminator. ADR-0048 Shape A populated-target preflight
// check (3) — the disjointness invariant the bypass rests on.
// Single-column PKs and PKs that don't lead with the discriminator
// both return (false, nil). Tables without any PK return (false,
// nil) too — the InjectShardColumn IR pass refuses upstream when the
// source has no PK, so this is defensive. Catalog Bug 81.
//
// Queries pg_index joined with pg_attribute to recover PK column
// ordering. The `indkey` smallint vector encodes attribute numbers
// in PK declaration order; element 0 is the leading column.
func (w *RowWriter) CompositePKLeadsWith(ctx context.Context, table *ir.Table, column string) (bool, error) {
	if table == nil {
		return false, errors.New("postgres: CompositePKLeadsWith: table is nil")
	}
	const q = `
		SELECT a.attname, array_length(i.indkey::int[], 1)
		FROM   pg_index    i
		JOIN   pg_class    cl ON cl.oid = i.indrelid
		JOIN   pg_namespace n ON n.oid  = cl.relnamespace
		JOIN   pg_attribute a  ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
		WHERE  i.indisprimary
		  AND  cl.relname = $1
		  AND  n.nspname  = $2`
	var leadName string
	var pkLen int
	err := w.db.QueryRowContext(ctx, q, table.Name, w.schema).Scan(&leadName, &pkLen)
	if err == nil {
		return pkLen > 1 && leadName == column, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, fmt.Errorf("postgres: probe %q PK lead: %w", table.Name, err)
}

// columnExistsOnTarget is a small helper for the preflight probers —
// returns false when the table doesn't exist or the column doesn't
// exist on it; an unrelated query error surfaces verbatim.
func (w *RowWriter) columnExistsOnTarget(ctx context.Context, tableName, column string) (bool, error) {
	const q = `
		SELECT 1
		FROM   information_schema.columns
		WHERE  table_schema = $1
		  AND  table_name   = $2
		  AND  column_name  = $3
		LIMIT  1`
	var dummy int
	err := w.db.QueryRowContext(ctx, q, w.schema, tableName, column).Scan(&dummy)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, err
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
	// ADR-0047: a table with a verbatim (uncatalogued) extension
	// column takes the batched-INSERT path even when COPY is the
	// engine default. pgx's binary COPY can't encode an unknown-OID
	// type's value (no sluice-side codec by construction — the
	// verbatim tier deliberately ships zero per-extension code); the
	// parameterised INSERT sends the value as text and PG's type input
	// function re-parses it, which is the text-I/O round-trip the ADR
	// specifies. This is contained: it fires ONLY for tables that
	// actually carry a verbatim column (the catalogued / core path is
	// untouched and keeps using COPY).
	if w.useCopy && !tableHasVerbatimColumn(table) {
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
// wire format pgx doesn't natively understand get a per-conn pgtype
// codec registered against the runtime OID resolved from pg_type.
// Two codecs ship today (v0.32.1):
//
//   - pgvector's `vector` (v0.26.0, Bug 47, ADR-0032). Without the
//     codec, pgx's binary COPY path silently routes vector values
//     through the `text` codec — the canonical text form ships as
//     raw bytes inside a binary-format column, and the receiver's
//     `vector_in()` parser interprets the first two bytes as a
//     big-endian dimension count and refuses with "vector cannot
//     have more than 16000 dimensions".
//   - hstore's `hstore` (v0.32.1, ADR-0032 Tier 1 follow-on). Same
//     failure shape: the IR carries text-form hstore bytes
//     (`"k"=>"v"`) and PG's binary COPY protocol expects hstore's
//     pair-array wire format. The codec translates at encode time.
//
// The two-codec layout establishes the pattern future Tier 2+
// extensions with their own wire formats (e.g. PostGIS EWKB) will
// follow: add a codec file + a `tableHas<Ext>Column` helper + a
// `registerPG<Ext>Codec` helper + register both in the gate below.
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
	needsPGHstoreCodec := tableHasHstoreColumn(table)
	needsPGTimetzCodec := tableHasTimetzColumn(table)

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
		if needsPGHstoreCodec {
			if err := registerPGHstoreCodec(ctx, conn, w.db); err != nil {
				return err
			}
		}
		if needsPGTimetzCodec {
			registerPGTimetzCodec(conn)
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

// tableHasHstoreColumn reports whether table has any column whose
// IR type is the hstore ExtensionType. Used to decide whether the
// COPY path needs to register the per-conn hstore pgtype codec.
// Mirrors [tableHasPGVectorColumn].
func tableHasHstoreColumn(table *ir.Table) bool {
	for _, col := range table.Columns {
		ext, ok := col.Type.(ir.ExtensionType)
		if !ok {
			continue
		}
		if ext.Extension == "hstore" {
			return true
		}
	}
	return false
}

// tableHasVerbatimColumn reports whether table has any column whose
// IR type is [ir.VerbatimType] (ADR-0047). Used by [WriteRows] to
// route the table through the batched-INSERT path instead of binary
// COPY: an uncatalogued extension type has no sluice-side wire-format
// codec (that is the whole point of the verbatim tier — zero
// per-extension code), and pgx's binary COPY would mis-encode the
// value the same way it did for pgvector/hstore before their codecs.
// The parameterised-INSERT path sends the value as text and PG's own
// type input function parses it — exactly the text-I/O round-trip
// ADR-0047 §2 specifies.
func tableHasVerbatimColumn(table *ir.Table) bool {
	if table == nil {
		return false
	}
	for _, col := range table.Columns {
		if col == nil {
			continue
		}
		if _, ok := col.Type.(ir.VerbatimType); ok {
			return true
		}
	}
	return false
}

// registerPGHstoreCodec resolves the runtime OID for the `hstore` type
// and registers [pgHstoreBinaryCodec] against it on conn. Mirrors
// [registerPGVectorCodec] — the lookup runs against the *sql.DB pool
// for the same reason (the conn here is mid-Raw; pg_type is database-
// global so a sibling-conn query returns the right OID). Idempotent:
// re-registering on a conn that already has the type is harmless.
func registerPGHstoreCodec(ctx context.Context, conn *pgx.Conn, db *sql.DB) error {
	tm := conn.TypeMap()
	if _, alreadyRegistered := tm.TypeForName("hstore"); alreadyRegistered {
		return nil
	}
	oid, err := lookupHstoreOID(ctx, db)
	if err != nil {
		return fmt.Errorf("postgres: copy: register hstore codec: %w", err)
	}
	tm.RegisterType(&pgtype.Type{
		Name:  "hstore",
		OID:   oid,
		Codec: pgHstoreBinaryCodec{},
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

	// catalog Bug 62 / Bug 75: ir.Bit columns. The IR carries a bit
	// value as its canonical '0'/'1' bit-string (see internal/ir/bit.go),
	// regardless of source engine — the MySQL reader converts MySQL's
	// right-justified big-endian bytes into that form, and the PG reader
	// passes pgx's '0'/'1' text through. PG's bit(N) binary wire format
	// is ceil(N/8) bytes *left*-aligned (bit 1 = MSB of byte 0), framed
	// by an int32 bit-length. ir.BitStringToBytesPG produces exactly
	// that left-aligned buffer; we hand pgx a pgtype.Bits so its
	// BitsCodec encodes it correctly under COPY binary.
	if _, isBit := t.(ir.Bit); isBit {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected bit-string string for Bit column, got %T", v)
		}
		pgBytes, err := ir.BitStringToBytesPG(s)
		if err != nil {
			return nil, err
		}
		// PG's bit/varbit binary wire format is `int32 bit-length` +
		// ceil(bitLen/8) bytes. The length is the VALUE's bit count,
		// not the column's declared width — a `bit varying(16)` row
		// holding 4 bits ships Len=4 with 1 byte. Using ir.Bit.Length
		// (the declared max) here desynced Len from len(Bytes) and PG
		// rejected the COPY stream with 08P01 "insufficient data". For
		// fixed bit(N) the value is always exactly N chars so len(s)
		// equals the declared width anyway.
		return pgtype.Bits{
			Bytes: pgBytes,
			Len:   int32(len(s)),
			Valid: true,
		}, nil
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

	// ADR-0047: VerbatimType columns pass through as their decoded
	// text/bytes shape — the type's text-output string (pgx stdlib
	// mode) or raw bytes — straight back to PG's type input function
	// on the (PG-only) target. Same opaque shape as ExtensionType.
	// This path is only reached on a same-engine PG → PG / PG-restore
	// run; a non-PG target is refused before any value moves.
	if _, isVerbatim := t.(ir.VerbatimType); isVerbatim {
		switch x := v.(type) {
		case string, []byte:
			return x, nil
		}
		return nil, fmt.Errorf("expected string or []byte for VerbatimType column (ADR-0047), got %T", v)
	}

	// Bug 122 (v0.95.3): a DOMAIN-typed column carries values in the
	// base type's representation (PG's wire / text I/O is identical
	// for a DOMAIN and its base type; the DOMAIN's CHECK applies at
	// INSERT/UPDATE time on the source and target). Dispatch
	// prepareValue recursively against the base type so every
	// downstream specialization (Array / Geometry / Bit / Extension /
	// Verbatim / scalar passthrough) reaches its existing branch.
	if dom, isDomain := t.(ir.Domain); isDomain {
		if dom.BaseType == nil {
			return nil, fmt.Errorf("DOMAIN %q has nil BaseType", dom.Name)
		}
		return prepareValue(v, dom.BaseType)
	}

	return v, nil
}

// convertArray turns []any (the IR canonical form for arrays, possibly
// nested for multi-dim) into a pgx-encodable Postgres array value —
// faithfully for both NULL elements and multi-dimensional arrays
// (catalog Bug 70).
//
// The vehicle is pgtype.Array[*T], NOT a plain/nested Go slice. Two
// reasons, both load-bearing:
//
//   - **NULL elements.** A SQL NULL inside an array decodes to a Go nil
//     slot (decodePGArrayText). A non-pointer element slice ([]int64,
//     …) can't carry that and a bare type-assertion blows up with
//     "expected T, got <nil>". With *T elements a nil slot is a typed
//     nil pointer, which pgx's array codec encodes as the array NULL
//     token.
//   - **Multi-dimensional arrays.** decodePGArrayText (Bug 68) yields
//     nested []any for int[][]. A nested Go slice does NOT round-trip
//     dimensions through pgx: pgtype.Map's wrap chain tries
//     TryWrapSliceEncodePlan (plain slice) BEFORE
//     TryWrapMultiDimSliceEncodePlan, and the plain-slice reflect
//     branch greedily matches [][]*T and flattens it to one dimension
//     ({1,2,3,4} instead of {{1,2},{3,4}}). pgtype.Array[*T] sidesteps
//     the wrap chain entirely — it is itself an ArrayGetter, so
//     ArrayCodec.PlanEncode consumes it directly — and carries the
//     dimensions explicitly in Dims with a flat row-major Elements
//     slice, which is exactly the multi-dim wire shape.
//
// The leaf Go type is chosen by the IR element type and is NOT free:
// pgx's ArrayCodec.PlanEncode plans the element encode against the
// *target column's element OID* using the leaf type Array[*T].IndexType()
// reports. If that OID's element codec can't plan the leaf type,
// ArrayCodec declines and pgx silently falls back through its wrap
// chain to a plain-slice encoder that FLATTENS multi-dimensional
// arrays with no error (catalog Bug 74 — a v0.69.3 silent-loss
// regression). A bare *string leaf only round-trips for the element
// codecs that accept string (text/varchar/char/macaddr); for
// numeric/uuid/inet/cidr/time/timestamp/date OIDs it silently
// flattens ≥2-D. The fix is to pick, per element family, a leaf type
// the target element codec actually plans:
//
//   - native bool/int64/float64 — encoded by the bool/int8/float8
//     codecs directly (unchanged from Bug 70; proven faithful).
//   - string-shaped (text, varchar, char, uuid, inet, cidr, macaddr) —
//     *pgtype.Text. It implements TextValuer/driver.Valuer, which
//     makes pgx select a dimension-preserving array plan for every one
//     of these element OIDs (bare *string does not for uuid/inet/cidr).
//   - numeric/decimal — *pgtype.Numeric (built from the IR's canonical
//     numeric string; NumericCodec plans NumericValuer, never string).
//   - date — *pgtype.Date; datetime / timestamp-without-tz —
//     *pgtype.Timestamp; timestamp-with-tz — *pgtype.Timestamptz;
//     time-without-tz — *pgtype.Time. The temporal codecs plan their
//     own pgtype valuer, never string (catalog Bug 73).
//
// timetz arrays (`ir.Time{WithTimeZone:true}`) have no faithful
// binary array leaf — the per-conn scalar timetz codec isn't
// registered for the `_timetz` array OID, and pgx's built-in Time
// codec drops the zone. Rather than silently flatten/corrupt them we
// refuse loudly here (the loud-failure tenet: a refused migration
// beats a silently corrupted one).
//
// Dimensions and shape are read from the value (ir.Array.Element is
// the scalar leaf type even for multi-dim, per the Bug 68 reader). PG
// only ever emits rectangular arrays, so dimension lengths are taken
// from the first element at each depth.
func convertArray(v []any, elem ir.Type) (any, error) {
	switch e := elem.(type) {
	case ir.Boolean:
		return buildPGArray(v, func(x any) (bool, error) {
			b, ok := x.(bool)
			if !ok {
				return false, fmt.Errorf("expected bool, got %T", x)
			}
			return b, nil
		})
	case ir.Integer:
		return buildPGArray(v, func(x any) (int64, error) {
			n, ok := x.(int64)
			if !ok {
				return 0, fmt.Errorf("expected int64, got %T", x)
			}
			return n, nil
		})
	case ir.Float:
		return buildPGArray(v, func(x any) (float64, error) {
			f, ok := x.(float64)
			if !ok {
				return 0, fmt.Errorf("expected float64, got %T", x)
			}
			return f, nil
		})
	case ir.Char, ir.Varchar, ir.Text, ir.UUID, ir.Inet, ir.Cidr, ir.Macaddr:
		return buildPGArray(v, func(x any) (pgtype.Text, error) {
			s, ok := x.(string)
			if !ok {
				return pgtype.Text{}, fmt.Errorf("expected string, got %T", x)
			}
			return pgtype.Text{String: s, Valid: true}, nil
		})
	case ir.Decimal:
		return buildPGArray(v, func(x any) (pgtype.Numeric, error) {
			s, ok := x.(string)
			if !ok {
				return pgtype.Numeric{}, fmt.Errorf("expected string, got %T", x)
			}
			var n pgtype.Numeric
			if err := n.Scan(s); err != nil {
				return pgtype.Numeric{}, fmt.Errorf("parse numeric %q: %w", s, err)
			}
			return n, nil
		})
	case ir.Date:
		return buildPGArray(v, func(x any) (pgtype.Date, error) {
			t, ok := x.(time.Time)
			if !ok {
				return pgtype.Date{}, fmt.Errorf("expected time.Time, got %T", x)
			}
			return pgtype.Date{Time: t, Valid: true}, nil
		})
	case ir.DateTime:
		return buildPGArray(v, func(x any) (pgtype.Timestamp, error) {
			t, ok := x.(time.Time)
			if !ok {
				return pgtype.Timestamp{}, fmt.Errorf("expected time.Time, got %T", x)
			}
			return pgtype.Timestamp{Time: t, Valid: true}, nil
		})
	case ir.Timestamp:
		if e.WithTimeZone {
			return buildPGArray(v, func(x any) (pgtype.Timestamptz, error) {
				t, ok := x.(time.Time)
				if !ok {
					return pgtype.Timestamptz{}, fmt.Errorf("expected time.Time, got %T", x)
				}
				return pgtype.Timestamptz{Time: t, Valid: true}, nil
			})
		}
		return buildPGArray(v, func(x any) (pgtype.Timestamp, error) {
			t, ok := x.(time.Time)
			if !ok {
				return pgtype.Timestamp{}, fmt.Errorf("expected time.Time, got %T", x)
			}
			return pgtype.Timestamp{Time: t, Valid: true}, nil
		})
	case ir.Time:
		if e.WithTimeZone {
			// timetz array: no faithful binary leaf (see doc comment).
			// Loud-refuse rather than silently flatten/corrupt.
			return nil, errors.New("postgres: timetz (time with time zone) arrays are not supported for COPY; migrate the column as a scalar timetz or exclude the table")
		}
		return buildPGArray(v, func(x any) (pgtype.Time, error) {
			s, ok := x.(string)
			if !ok {
				return pgtype.Time{}, fmt.Errorf("expected string, got %T", x)
			}
			usec, err := timeOfDayMicros(s)
			if err != nil {
				return pgtype.Time{}, err
			}
			return pgtype.Time{Microseconds: usec, Valid: true}, nil
		})
	}
	return nil, fmt.Errorf("postgres: array of element type %T not supported", elem)
}

// timeOfDayMicros parses the IR canonical time-of-day string
// ("HH:MM:SS" or "HH:MM:SS.ffffff", as decodeTimeAsString emits) into
// microseconds since midnight — the unit pgtype.Time encodes. The
// fractional part is right-padded/truncated to microsecond precision
// (PG's `time` resolution). Any malformed token is an upstream decode
// bug and surfaces as a loud error rather than a wrong value.
func timeOfDayMicros(s string) (int64, error) {
	hms := s
	var frac string
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		hms, frac = s[:dot], s[dot+1:]
	}
	parts := strings.Split(hms, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("postgres: malformed time-of-day %q", s)
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("postgres: malformed time-of-day %q: %w", s, err)
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, fmt.Errorf("postgres: malformed time-of-day %q: %w", s, err)
	}
	sec, err := strconv.Atoi(parts[2])
	if err != nil {
		return 0, fmt.Errorf("postgres: malformed time-of-day %q: %w", s, err)
	}
	var usec int
	if frac != "" {
		if len(frac) > 6 {
			frac = frac[:6]
		}
		for len(frac) < 6 {
			frac += "0"
		}
		usec, err = strconv.Atoi(frac)
		if err != nil {
			return 0, fmt.Errorf("postgres: malformed time-of-day %q: %w", s, err)
		}
	}
	return int64(h)*3_600_000_000 + int64(m)*60_000_000 + int64(sec)*1_000_000 + int64(usec), nil
}

// buildPGArray flattens v (a possibly-nested []any) row-major into a
// pgtype.Array[*T] with explicit Dims. A nil slot becomes a typed nil
// *T (SQL NULL element); a present slot is converted via conv. The
// dimension lengths come from the first element at each depth (PG
// arrays are rectangular). A type mismatch from conv is an upstream
// translator bug and surfaces as an error.
//
// T is chosen by convertArray per element family so that pgx's
// ArrayCodec plans the element encode against the target column's
// element OID (a wrong leaf makes pgx silently flatten ≥2-D arrays —
// catalog Bug 74). pgtype.Array[*T] is itself an ArrayGetter, so its
// explicit Dims survive the encode path; the *T element pointer lets
// a SQL NULL element round-trip as a typed nil.
func buildPGArray[T any](v []any, conv func(any) (T, error)) (any, error) {
	dims := arrayDims(v)
	elems := make([]*T, 0, arrayCardinality(dims))
	var flatten func(level []any, depth int) error
	flatten = func(level []any, depth int) error {
		if depth == len(dims)-1 {
			for _, e := range level {
				if e == nil {
					elems = append(elems, nil) // SQL NULL element
					continue
				}
				cv, err := conv(e)
				if err != nil {
					return err
				}
				elems = append(elems, &cv)
			}
			return nil
		}
		for _, e := range level {
			sub, ok := e.([]any)
			if !ok {
				return fmt.Errorf("array element: mixed nested and scalar elements (got %T)", e)
			}
			if err := flatten(sub, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if err := flatten(v, 0); err != nil {
		return nil, err
	}
	return pgtype.Array[*T]{
		Elements: elems,
		Dims:     dims,
		Valid:    true,
	}, nil
}

// arrayDims walks the first element at each depth to recover the
// rectangular dimension lengths of a (possibly nested) PG array value.
// An empty array is a single zero-length dimension.
func arrayDims(v []any) []pgtype.ArrayDimension {
	var dims []pgtype.ArrayDimension
	level := v
	for {
		dims = append(dims, pgtype.ArrayDimension{Length: int32(len(level)), LowerBound: 1})
		if len(level) == 0 {
			return dims
		}
		first, ok := level[0].([]any)
		if !ok {
			return dims
		}
		level = first
	}
}

// arrayCardinality is the total element count across all dimensions
// (product of the dimension lengths) — the capacity the flat Elements
// slice needs.
func arrayCardinality(dims []pgtype.ArrayDimension) int {
	if len(dims) == 0 {
		return 0
	}
	n := 1
	for _, d := range dims {
		n *= int(d.Length)
	}
	return n
}
