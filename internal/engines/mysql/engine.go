// Package mysql implements the sluice [ir.Engine] for MySQL and its
// wire-compatible variants. It reads schema and rows via the standard
// database/sql driver (github.com/go-sql-driver/mysql), and produces
// IR values the orchestrator can pass to a target engine.
//
// The package supports multiple [Flavor] variants. Each registered
// flavor has its own engine name and its own [ir.Capabilities]
// declaration; the rest of the engine code (schema reader, row reader,
// DDL emitter, value decoder) is flavor-independent. See flavor.go for
// the list of recognised flavors and their capability declarations.
//
// The engine is registered automatically when this package is imported:
//
//	import _ "github.com/orware/sluice/internal/engines/mysql"
//
// Users select a flavor via the engine name in their configuration
// (`driver: mysql` or `driver: planetscale`).
//
// At this stage of the project the read side and the schema-write side
// are implemented; OpenRowWriter, OpenCDCReader, and OpenChangeApplier
// return [ErrNotImplemented] until those layers land.
package mysql

import (
	"context"
	"errors"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"
)

// ErrNotImplemented is returned by Engine methods whose underlying
// reader/writer has not yet been implemented in this version of the
// engine. Callers should check for it with [errors.Is].
var ErrNotImplemented = errors.New("mysql engine: not implemented yet")

// Engine is the MySQL implementation of [ir.Engine]. It is stateless:
// each Open* call creates an independent connection. Multiple Engines
// may safely coexist.
//
// The Flavor field selects between MySQL-compatible service variants
// (vanilla MySQL, PlanetScale, etc.). The zero value is FlavorVanilla
// so Engine{} continues to behave as a vanilla MySQL engine.
type Engine struct {
	Flavor Flavor
}

// Name returns the engine's short identifier as used in configuration
// files and on the command line. The name is determined by the engine's
// Flavor — "mysql" for vanilla, "planetscale" for PlanetScale, etc.
func (e Engine) Name() string { return e.Flavor.String() }

// Capabilities returns the capability declaration for this engine's
// flavor. The orchestrator consults this to pick strategies (bulk-load
// method, CDC mechanism, etc.) without having to know which flavor it
// is talking to.
func (e Engine) Capabilities() ir.Capabilities { return e.Flavor.capabilities() }

// OpenSchemaReader returns a [SchemaReader] bound to the database
// identified by dsn. The caller is responsible for closing the
// returned SchemaReader (via its Close method) to release the
// underlying connection pool.
func (Engine) OpenSchemaReader(ctx context.Context, dsn string) (ir.SchemaReader, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &SchemaReader{db: db, schema: cfg.DBName}, nil
}

// OpenSchemaWriter returns a [SchemaWriter] bound to the database
// identified by dsn. The caller is responsible for closing the
// returned SchemaWriter (via its Close method) to release the
// underlying connection pool.
func (Engine) OpenSchemaWriter(ctx context.Context, dsn string) (ir.SchemaWriter, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &SchemaWriter{db: db, schema: cfg.DBName}, nil
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
	return &RowReader{db: db, schema: cfg.DBName}, nil
}

// OpenRowWriter is not yet implemented.
func (Engine) OpenRowWriter(_ context.Context, _ string) (ir.RowWriter, error) {
	return nil, ErrNotImplemented
}

// OpenCDCReader is not yet implemented.
func (Engine) OpenCDCReader(_ context.Context, _ string) (ir.CDCReader, error) {
	return nil, ErrNotImplemented
}

// OpenChangeApplier is not yet implemented.
func (Engine) OpenChangeApplier(_ context.Context, _ string) (ir.ChangeApplier, error) {
	return nil, ErrNotImplemented
}

// init registers each supported flavor under its own name in the
// engines registry. Adding a new flavor is a one-line addition here
// plus the corresponding entry in flavor.go.
func init() {
	engines.Register(Engine{Flavor: FlavorVanilla})
	engines.Register(Engine{Flavor: FlavorPlanetScale})
}
