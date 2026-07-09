// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package postgres implements the sluice [ir.Engine] for PostgreSQL.
// It reads schema and rows via the standard database/sql interface
// backed by the pgx driver in stdlib mode (github.com/jackc/pgx/v5/stdlib),
// and produces IR values the orchestrator can pass to a target engine.
//
// The engine is registered automatically when this package is imported:
//
//	import _ "sluicesync.dev/sluice/internal/engines/postgres"
//
// At this stage of the project, only [SchemaReader] is implemented;
// the other Open* methods return [ErrNotImplemented]. RowReader,
// SchemaWriter, RowWriter, CDCReader, and ChangeApplier will land in
// subsequent commits, mirroring the MySQL roll-out.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/stdlib"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// ErrNotImplemented is returned by Engine methods whose underlying
// reader/writer has not yet been implemented in this version of the
// engine. Callers should check for it with [errors.Is].
var ErrNotImplemented = errors.New("postgres engine: not implemented yet")

// Engine is the Postgres implementation of [ir.Engine]. Each Open* call
// creates an independent connection. The value is cheap to copy and
// holds no connection state — its only field is the connection-label id
// (see [Engine.WithConnectionLabel]); the zero value is fully usable
// and labels its connections with the stable `sluice/<role>/-` fallback,
// so bare Go-API callers and `go test` paths behave exactly as before
// the id existed.
type Engine struct {
	// appID is the stream-/migration-id segment of the application_name
	// (`sluice/<role>/<appID>`) stamped on every connection this engine
	// value opens. Set only via [Engine.WithConnectionLabel]; empty (the
	// zero value) is normalised to "-" at the [withApplicationName]
	// choke point.
	appID string
}

// Name returns the engine's short identifier as used in configuration
// files and on the command line.
func (Engine) Name() string { return "postgres" }

// Compile-time check that the engine keeps satisfying the CLI's
// connection-labeling type-assertion (labelEngine in cmd/sluice).
var _ ir.ConnectionLabeler = Engine{}

// WithConnectionLabel implements [ir.ConnectionLabeler]: it returns a
// copy of the engine configured to stamp `sluice/<role>/<id>` as the
// application_name on every connection the copy opens, so operators can
// find this run's sessions in pg_stat_activity and the stale-backend
// probe can scope to its own stream. The CLI applies it once per run
// with the resolved --stream-id / --migration-id, before any connection
// opens. An empty id is normalised to "-" so the label stays
// well-formed and greppable.
func (e Engine) WithConnectionLabel(id string) ir.Engine {
	if id == "" {
		id = "-"
	}
	e.appID = id
	return e
}

// Capabilities returns the static capability declaration for vanilla
// PostgreSQL (14+ baseline). Service variants — Aurora Postgres, GCP
// AlloyDB, etc. — would follow the same Flavor pattern the mysql
// package uses, when a real need surfaces.
func (Engine) Capabilities() ir.Capabilities { return capabilities }

// HasCrossEngineDefaultTranslator implements
// [ir.CrossEngineExtensionTranslator]. The pipeline's engine-name
// gate in `validateEnabledPGExtensions` calls this to decide whether
// `--enable-pg-extension EXT` may be paired with a non-PG target.
// PG declares its v1 cross-engine-translatable extensions via the
// catalog's `crossEngineDefaultTranslatedExtensions` registry —
// today: hstore and citext (ADR-0032 § "Cross-engine policy").
func (Engine) HasCrossEngineDefaultTranslator(name string) bool {
	return HasCrossEngineDefaultTranslator(name)
}

// OpenSchemaReader returns a [SchemaReader] bound to the database
// identified by dsn. The schema name to read is taken from the DSN's
// `schema` query parameter; if absent, defaults to "public".
//
// The caller is responsible for closing the returned SchemaReader
// (via its Close method) to release the underlying connection pool.
func (e Engine) OpenSchemaReader(ctx context.Context, dsn string) (ir.SchemaReader, error) {
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDBAs(ctx, cfg, roleSchema)
	if err != nil {
		return nil, err
	}
	return &SchemaReader{db: db, schema: cfg.schema}, nil
}

// OpenSchemaWriter returns a [SchemaWriter] bound to the database
// identified by dsn. The schema name to write into is taken from the
// DSN's `schema` query parameter; if absent, defaults to "public".
//
// The caller is responsible for closing the returned SchemaWriter
// (via its Close method) to release the underlying connection pool.
func (e Engine) OpenSchemaWriter(ctx context.Context, dsn string) (ir.SchemaWriter, error) {
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDBAs(ctx, cfg, roleSchema)
	if err != nil {
		return nil, err
	}
	hasGIS, err := detectPostGIS(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &SchemaWriter{db: db, schema: cfg.schema, hasPostGIS: hasGIS}, nil
}

// OpenRowReader returns a [RowReader] bound to the database identified
// by dsn. The caller is responsible for closing the returned RowReader
// (via its Close method) to release the underlying connection pool.
func (e Engine) OpenRowReader(ctx context.Context, dsn string) (ir.RowReader, error) {
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDBAs(ctx, cfg, roleSnapshot)
	if err != nil {
		return nil, err
	}
	return &RowReader{q: db, schema: cfg.schema, closer: db}, nil
}

// OpenRowWriter returns a [RowWriter] bound to the database
// identified by dsn. The bulk-load strategy (COPY FROM STDIN vs.
// batched multi-row INSERT) is chosen from the engine's BulkLoad
// capability: vanilla PG declares BulkLoadCopy, so useCopy is true
// by default. The caller is responsible for closing the returned
// RowWriter (via its Close method) to release the underlying
// connection pool.
func (e Engine) OpenRowWriter(ctx context.Context, dsn string) (ir.RowWriter, error) {
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDBAs(ctx, cfg, roleSnapshot)
	if err != nil {
		return nil, err
	}
	hasGIS, err := detectPostGIS(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &RowWriter{
		db:         db,
		schema:     cfg.schema,
		useCopy:    e.Capabilities().BulkLoad == ir.BulkLoadCopy,
		hasPostGIS: hasGIS,
	}, nil
}

// OpenCDCReader returns a [CDCReader] bound to the database identified
// by dsn. The reader streams pgoutput logical-replication output via a
// dedicated replication-mode connection it opens internally; the
// returned reader also holds a regular *sql.DB pool for precondition
// queries and one-time DDL (CREATE PUBLICATION on demand). Caller
// closes the returned reader to release both connections.
//
// Requires Postgres 14+ (pgoutput protocol v2) and wal_level=logical
// on the source. The connecting role needs the REPLICATION attribute
// to create the replication slot. Both preconditions surface as
// startup errors rather than mid-stream failures.
func (e Engine) OpenCDCReader(ctx context.Context, dsn string) (ir.CDCReader, error) {
	return e.OpenCDCReaderWithSlot(ctx, dsn, defaultSlot)
}

// OpenCDCReaderWithSlot satisfies [ir.CDCReaderWithSlotOpener]. The
// orchestrator picks this path over [OpenCDCReader] when an operator
// supplies `--slot-name` on `sync start`. Empty slotName is replaced
// with the default `sluice_slot` so the same code path serves both
// the default and the override case.
func (e Engine) OpenCDCReaderWithSlot(ctx context.Context, dsn, slotName string) (ir.CDCReader, error) {
	if slotName == "" {
		slotName = defaultSlot
	}
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDBAs(ctx, cfg, roleCDCReader)
	if err != nil {
		return nil, err
	}
	return &CDCReader{
		db:           db,
		schema:       cfg.schema,
		dsn:          cfg.dsn,
		appID:        cfg.appID,
		publication:  defaultPublication,
		slotName:     slotName,
		protoVersion: 2,
	}, nil
}

// EnsurePublication creates or rescopes the sluice publication on
// the source. Pass the list of source tables that should be in the
// CDC stream; the publication is set to FOR TABLE <list> rather
// than the legacy FOR ALL TABLES (Bug 13, ADR-0021).
//
// Idempotent and safe to call repeatedly: an existing publication
// with the same scope is left alone; a publication with a different
// scope (or the v0.4.0-style FOR ALL TABLES) is altered or replaced
// to match.
//
// Discovered by the [pipeline.Streamer] via structural interface
// (publicationEnsurer); engines that don't have logical-replication
// publications simply omit the method.
func (e Engine) EnsurePublication(ctx context.Context, dsn string, tables []string) error {
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return ensurePublication(ctx, db, defaultPublication, cfg.schema, tables)
}

// AddPublicationTables extends the sluice publication's table list
// to include every name in tables, leaving the existing scope
// untouched. Used by the mid-stream add-table flow where a `SET
// TABLE` would silently drop tables that are already in scope.
//
// Refuses (with a clear error) when the publication is FOR ALL
// TABLES — adding a specific table to such a publication is
// meaningless. The publication must already exist; the add-table
// orchestrator pre-flights the active stream's existence before
// reaching this call.
//
// Idempotent: tables already in the publication are skipped, so a
// re-run after a partial-add lands cleanly. Discovered by the
// pipeline.AddTable orchestrator via structural interface
// (publicationAdder); engines without publications simply omit the
// method.
func (e Engine) AddPublicationTables(ctx context.Context, dsn string, tables []string) error {
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return addTablesToPublication(ctx, db, defaultPublication, cfg.schema, tables)
}

// ExtractSnapshotLSN decodes a Postgres snapshot stream's
// [ir.Position] and returns the LSN it carries. Used by the live
// mid-stream add-table flow (ADR-0030) to enforce the
// snapshot-LSN ≥ slot-LSN invariant without leaking the engine's
// position envelope into the orchestrator.
//
// Returns ("", false, nil) when the position is the zero value
// (the "from now" sentinel) — semantically "no LSN floor", which
// the orchestrator treats as skip-the-check rather than refuse.
// Returns a wrapped error on a non-zero position with a malformed
// envelope; the orchestrator surfaces that as a refusal because a
// malformed position from the snapshot path is itself a bug worth
// investigating before touching production data.
func (Engine) ExtractSnapshotLSN(pos ir.Position) (lsn string, ok bool, err error) {
	decoded, ok, err := decodePGPos(pos)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}
	return decoded.LSN, true, nil
}

// CompareLSN compares two PG LSN strings in the canonical
// "X/XXXXXXXX" form. Returns -1 if a < b, 0 if equal, +1 if a > b.
// Wraps pglogrepl.ParseLSN on each operand; a malformed LSN
// surfaces as a wrapped error. Used by the live add-table
// invariant check (ADR-0030).
func (Engine) CompareLSN(a, b string) (int, error) {
	la, err := pglogrepl.ParseLSN(a)
	if err != nil {
		return 0, fmt.Errorf("postgres: compare LSN: parse %q: %w", a, err)
	}
	lb, err := pglogrepl.ParseLSN(b)
	if err != nil {
		return 0, fmt.Errorf("postgres: compare LSN: parse %q: %w", b, err)
	}
	switch {
	case la < lb:
		return -1, nil
	case la > lb:
		return 1, nil
	}
	return 0, nil
}

// PrecedesOrEqual implements [ir.PositionMonotonicChecker] for the
// inline-rotation FSM's S>=P_N hard-fail assertion (ADR-0046 §2). It
// decodes both positions' LSNs and reports a.lsn <= b.lsn in WAL
// order. A malformed / cross-engine position is a non-nil error (the
// FSM treats that as "cannot prove monotonic" and hard-aborts the
// rotation — loud-failure, never a silent gap).
func (e Engine) PrecedesOrEqual(a, b ir.Position) (bool, error) {
	da, oka, err := decodePGPos(a)
	if err != nil {
		return false, fmt.Errorf("postgres: monotonic check: decode a: %w", err)
	}
	if !oka {
		return false, errors.New("postgres: monotonic check: position a is the empty 'from now' sentinel; cannot compare")
	}
	db, okb, err := decodePGPos(b)
	if err != nil {
		return false, fmt.Errorf("postgres: monotonic check: decode b: %w", err)
	}
	if !okb {
		return false, errors.New("postgres: monotonic check: position b is the empty 'from now' sentinel; cannot compare")
	}
	cmp, err := e.CompareLSN(da.LSN, db.LSN)
	if err != nil {
		return false, err
	}
	return cmp <= 0, nil
}

// ReadSlotPosition returns the named replication slot's
// confirmed_flush_lsn as a canonical "X/XXXXXXXX" string. Used by
// the mid-stream live add-table flow (ADR-0030) to capture the
// active stream's slot position before a publication change, so the
// orchestrator can verify the subsequent snapshot LSN is at or
// beyond the slot's current confirmed-flush position. If that
// invariant ever fails, events on the new table in
// [snapshot-LSN, confirmed_flush_lsn] would be silently dropped;
// the live-mode preflight catches it loudly instead.
//
// Returns a clear error when the slot doesn't exist (typo on the
// operator side, or a fresh stream that hasn't created its slot
// yet). Returns the empty string when the slot exists but
// confirmed_flush_lsn is NULL — fresh slot with no consumer
// progress yet; the caller treats that as "no floor".
//
// Discovered structurally on the pipeline side via slotPositionReader;
// engines without replication slots simply omit the method.
func (e Engine) ReadSlotPosition(ctx context.Context, dsn, slotName string) (string, error) {
	if slotName == "" {
		return "", errors.New("postgres: read slot position: slot name is empty")
	}
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return "", err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return "", err
	}
	defer func() { _ = db.Close() }()

	const q = `
		SELECT COALESCE(confirmed_flush_lsn::text, '')
		FROM   pg_replication_slots
		WHERE  slot_name = $1`
	var lsn string
	switch err := db.QueryRowContext(ctx, q, slotName).Scan(&lsn); {
	case errors.Is(err, sql.ErrNoRows):
		return "", fmt.Errorf("postgres: read slot position: slot %q not found in pg_replication_slots — verify the active stream's slot name", slotName)
	case err != nil:
		return "", fmt.Errorf("postgres: read slot position: %w", err)
	}
	return lsn, nil
}

// ReadCurrentWALPosition returns pg_current_wal_lsn() against the
// supplied DSN as a canonical "X/XXXXXXXX" string. ADR-0036 (Path D
// Phase A) instrumentation surface: lets the live add-table
// orchestrator log the WAL position before AND after the publication-
// add step so the diagnostic test can attribute observed loss to a
// specific LSN window. Independent from the SchemaReader's
// SourceCurrentPosition surface because the live-add flow closes its
// SchemaReader before publication-add runs.
//
// Discovered structurally on the pipeline side via the
// currentWALPositionReader interface; engines without WAL semantics
// simply omit the method. The diagnostic instrumentation is best-
// effort: a query failure logs at WARN and returns empty, since this
// is purely diagnostic and must not abort the live add.
func (e Engine) ReadCurrentWALPosition(ctx context.Context, dsn string) (string, error) {
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return "", err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return "", err
	}
	defer func() { _ = db.Close() }()

	var lsn string
	if err := db.QueryRowContext(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&lsn); err != nil {
		return "", fmt.Errorf("postgres: read current WAL position: %w", err)
	}
	return lsn, nil
}

// OpenSlotManager returns a [SlotManager] bound to the database
// identified by dsn. Used by the `sluice slot list` and `sluice slot
// drop` CLI commands to manage logical-replication slots from the
// outside (separate from the CDC reader's implicit slot lifecycle).
//
// Implements [ir.SlotManagerOpener]; the CLI checks for this method
// via type assertion so engines without slot management (e.g. MySQL)
// can simply omit the method.
func (e Engine) OpenSlotManager(ctx context.Context, dsn string) (ir.SlotManager, error) {
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &SlotManager{db: db}, nil
}

// OpenMigrationStateStore returns a [MigrationStateStore] bound to
// the database identified by dsn. Implements
// [ir.MigrationStateStoreOpener]; the pipeline orchestrator type-
// asserts on this method so engines without a SQL surface for
// resumable migrations can omit it.
func (e Engine) OpenMigrationStateStore(ctx context.Context, dsn string) (ir.MigrationStateStore, error) {
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return newMigrationStateStore(db, cfg.schema), nil
}

// OpenChangeApplier returns a [ChangeApplier] bound to the database
// identified by dsn. The caller is responsible for closing the
// returned applier (via its Close method) to release the underlying
// connection pool.
//
// See the [ChangeApplier] doc comment for important details about
// no-PK and unique-key-without-PK tables.
func (e Engine) OpenChangeApplier(ctx context.Context, dsn string) (ir.ChangeApplier, error) {
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	// Register the PostGIS geometry codec on every serial applier backend
	// (the per-change Apply path + the serial batch fall-back) so a geometry
	// column applies as BINARY EWKB rather than being TEXT-refused (see
	// [afterConnectRegisterGeometry]); a no-op when PostGIS isn't installed.
	// The pipelined pool registers it independently in [pipelinePool].
	db, err := openDBAs(ctx, cfg, roleApplier, stdlib.OptionAfterConnect(afterConnectRegisterGeometry))
	if err != nil {
		return nil, err
	}
	return &ChangeApplier{
		db: db,
		// pipelineCfg carries the parsed DSN so the ADR-0092 pipelined
		// pool can be opened lazily on the first batch (Exec-mode default).
		pipelineCfg:      cfg,
		schema:           cfg.schema,
		controlSchema:    cfg.schema,
		pkCache:          make(map[string][]string),
		conflictKeyCache: make(map[string][]string),
		colTypeCache:     make(map[string]map[string]*ir.Column),
		activeSchema:     make(map[string]activeSchemaVersion),
	}, nil
}

// capabilities declares what this engine supports. Values reflect a
// vanilla PostgreSQL 14+ baseline.
var capabilities = ir.Capabilities{
	BulkLoad:    ir.BulkLoadCopy,
	CDC:         ir.CDCLogicalReplication,
	SchemaScope: ir.SchemaScopeNamespaced,
	SupportedTypes: ir.NewTypeSet(
		ir.ExtEnum,  // CREATE TYPE ... AS ENUM
		ir.ExtUUID,  // native uuid type
		ir.ExtArray, // native T[] arrays
		ir.ExtInet,  // network address types
		ir.ExtCidr,
		ir.ExtMacaddr,
	),
	SupportsCheckConstraint:  true,
	SupportsGeneratedColumns: true, // 12+
	SupportsPartitioning:     true, // 10+
	EnumSupport:              ir.EnumTypeLevel,
	JSONSupport:              ir.JSONBoth, // json + jsonb
	UnsignedIntegers:         false,       // Postgres has no unsigned integers
	DDLDialect:               ir.DDLDialectANSI,
	PostgresBackend:          true, // PG catalogs / XID wraparound / declarative partitioning
	PGExtensionCatalog:       true, // --enable-pg-extension resolves extension-owned types (ADR-0032)
	VerbatimExtensionTypes:   true, // uncatalogued-extension verbatim passthrough (ADR-0047)
}

// init registers this engine with the engines registry. The blank
// import in cmd/sluice triggers this on binary startup.
func init() {
	engines.Register(Engine{})
}
