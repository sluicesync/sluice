// Package mysql implements the sluice [ir.Engine] for MySQL (and
// fully-compatible variants such as MariaDB and Percona Server). It
// reads schema and rows via the standard database/sql driver
// (github.com/go-sql-driver/mysql), and produces IR values that the
// orchestrator can pass to a target engine.
//
// The engine is registered automatically when this package is imported:
//
//	import _ "github.com/orware/sluice/internal/engines/mysql"
//
// At this stage of the project, only [SchemaReader] is implemented;
// the other Open* methods return [ErrNotImplemented]. This is a
// deliberate, build-by-build expansion of the engine surface — see the
// roadmap in docs/architecture.md.
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
type Engine struct{}

// Name returns the engine's short identifier as used in configuration
// files and on the command line.
func (Engine) Name() string { return "mysql" }

// Capabilities returns the static capability declaration for MySQL
// (8.0 baseline). When per-version variation matters, Open* methods
// can detect the server version on connection and report it through
// engine-specific surfaces.
func (Engine) Capabilities() ir.Capabilities { return capabilities }

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

// OpenSchemaWriter is not yet implemented.
func (Engine) OpenSchemaWriter(_ context.Context, _ string) (ir.SchemaWriter, error) {
	return nil, ErrNotImplemented
}

// OpenRowReader is not yet implemented.
func (Engine) OpenRowReader(_ context.Context, _ string) (ir.RowReader, error) {
	return nil, ErrNotImplemented
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

// capabilities declares what this engine implementation supports.
// Values reflect MySQL 8.0+ behaviour; older servers may diverge in
// specific details (notably CHECK constraint enforcement, which became
// real in 8.0.16 — earlier versions parsed but ignored CHECK).
var capabilities = ir.Capabilities{
	BulkLoad:    ir.BulkLoadLoadDataInfile,
	CDC:         ir.CDCBinlog,
	SchemaScope: ir.SchemaScopeFlat,
	SupportedTypes: ir.NewTypeSet(
		ir.ExtEnum,     // column-level ENUM
		ir.ExtSet,      // column-level SET
		ir.ExtGeometry, // built-in spatial types
	),
	SupportsCheckConstraint:  true, // 8.0.16+
	SupportsGeneratedColumns: true,
	SupportsPartitioning:     true,
	EnumSupport:              ir.EnumColumnLevel,
	JSONSupport:              ir.JSONBinary, // MySQL has only one JSON type, binary-stored
	UnsignedIntegers:         true,
}

// init registers this engine with the engines registry. The blank
// import in cmd/sluice triggers this on binary startup.
func init() {
	engines.Register(Engine{})
}
