// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package postgres implements the sluice [ir.Engine] for PostgreSQL.
// It reads schema and rows via the standard database/sql interface
// backed by the pgx driver in stdlib mode (github.com/jackc/pgx/v5/stdlib),
// and produces IR values the orchestrator can pass to a target engine.
//
// The engine is registered automatically when this package is imported:
//
//	import _ "github.com/orware/sluice/internal/engines/postgres"
//
// At this stage of the project, only [SchemaReader] is implemented;
// the other Open* methods return [ErrNotImplemented]. RowReader,
// SchemaWriter, RowWriter, CDCReader, and ChangeApplier will land in
// subsequent commits, mirroring the MySQL roll-out.
package postgres

import (
	"context"
	"errors"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"
)

// ErrNotImplemented is returned by Engine methods whose underlying
// reader/writer has not yet been implemented in this version of the
// engine. Callers should check for it with [errors.Is].
var ErrNotImplemented = errors.New("postgres engine: not implemented yet")

// Engine is the Postgres implementation of [ir.Engine]. It is stateless;
// each Open* call creates an independent connection.
type Engine struct{}

// Name returns the engine's short identifier as used in configuration
// files and on the command line.
func (Engine) Name() string { return "postgres" }

// Capabilities returns the static capability declaration for vanilla
// PostgreSQL (14+ baseline). Service variants — Aurora Postgres, GCP
// AlloyDB, etc. — would follow the same Flavor pattern the mysql
// package uses, when a real need surfaces.
func (Engine) Capabilities() ir.Capabilities { return capabilities }

// OpenSchemaReader returns a [SchemaReader] bound to the database
// identified by dsn. The schema name to read is taken from the DSN's
// `schema` query parameter; if absent, defaults to "public".
//
// The caller is responsible for closing the returned SchemaReader
// (via its Close method) to release the underlying connection pool.
func (Engine) OpenSchemaReader(ctx context.Context, dsn string) (ir.SchemaReader, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
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
func (Engine) OpenSchemaWriter(ctx context.Context, dsn string) (ir.SchemaWriter, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
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
func (Engine) OpenRowReader(ctx context.Context, dsn string) (ir.RowReader, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
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
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
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
func (Engine) OpenCDCReaderWithSlot(ctx context.Context, dsn, slotName string) (ir.CDCReader, error) {
	if slotName == "" {
		slotName = defaultSlot
	}
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &CDCReader{
		db:           db,
		schema:       cfg.schema,
		dsn:          cfg.dsn,
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
func (Engine) EnsurePublication(ctx context.Context, dsn string, tables []string) error {
	cfg, err := parseDSN(dsn)
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
func (Engine) AddPublicationTables(ctx context.Context, dsn string, tables []string) error {
	cfg, err := parseDSN(dsn)
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

// OpenSlotManager returns a [SlotManager] bound to the database
// identified by dsn. Used by the `sluice slot list` and `sluice slot
// drop` CLI commands to manage logical-replication slots from the
// outside (separate from the CDC reader's implicit slot lifecycle).
//
// Implements [ir.SlotManagerOpener]; the CLI checks for this method
// via type assertion so engines without slot management (e.g. MySQL)
// can simply omit the method.
func (Engine) OpenSlotManager(ctx context.Context, dsn string) (ir.SlotManager, error) {
	cfg, err := parseDSN(dsn)
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
func (Engine) OpenMigrationStateStore(ctx context.Context, dsn string) (ir.MigrationStateStore, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &MigrationStateStore{db: db, schema: cfg.schema}, nil
}

// OpenChangeApplier returns a [ChangeApplier] bound to the database
// identified by dsn. The caller is responsible for closing the
// returned applier (via its Close method) to release the underlying
// connection pool.
//
// See the [ChangeApplier] doc comment for important details about
// no-PK and unique-key-without-PK tables.
func (Engine) OpenChangeApplier(ctx context.Context, dsn string) (ir.ChangeApplier, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &ChangeApplier{
		db:           db,
		schema:       cfg.schema,
		pkCache:      make(map[string][]string),
		colTypeCache: make(map[string]map[string]ir.Type),
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
}

// init registers this engine with the engines registry. The blank
// import in cmd/sluice triggers this on binary startup.
func init() {
	engines.Register(Engine{})
}
