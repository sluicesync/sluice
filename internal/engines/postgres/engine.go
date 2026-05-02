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
	return &SchemaWriter{db: db, schema: cfg.schema}, nil
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
	return &RowReader{db: db, schema: cfg.schema}, nil
}

// OpenRowWriter returns a [RowWriter] bound to the database identified
// by dsn. The caller is responsible for closing the returned RowWriter
// (via its Close method) to release the underlying connection pool.
func (Engine) OpenRowWriter(ctx context.Context, dsn string) (ir.RowWriter, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &RowWriter{db: db, schema: cfg.schema}, nil
}

// OpenCDCReader is not yet implemented.
func (Engine) OpenCDCReader(_ context.Context, _ string) (ir.CDCReader, error) {
	return nil, ErrNotImplemented
}

// OpenChangeApplier is not yet implemented.
func (Engine) OpenChangeApplier(_ context.Context, _ string) (ir.ChangeApplier, error) {
	return nil, ErrNotImplemented
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
