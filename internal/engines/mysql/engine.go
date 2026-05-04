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
	"fmt"

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
	return &RowReader{q: db, schema: cfg.DBName, closer: db}, nil
}

// OpenRowWriter returns a [RowWriter] bound to the database identified
// by dsn. The writer chooses a bulk-load strategy based on the engine's
// declared [ir.Capabilities.BulkLoad], so vanilla MySQL and PlanetScale
// pick different paths from the same call. The caller is responsible
// for closing the returned RowWriter (via its Close method) to release
// the underlying connection pool.
func (e Engine) OpenRowWriter(ctx context.Context, dsn string) (ir.RowWriter, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &RowWriter{
		db:       db,
		schema:   cfg.DBName,
		bulkLoad: e.Capabilities().BulkLoad,
	}, nil
}

// OpenCDCReader returns a [ir.CDCReader] bound to the database
// identified by dsn. The concrete reader depends on the engine's
// flavor:
//
//   - FlavorVanilla → [CDCReader], speaking MySQL's row-based
//     binary log via a separate connection from the schema-cache
//     *sql.DB; the account in the DSN must have REPLICATION SLAVE
//     (and REPLICATION CLIENT, for gtid_mode / master-status
//     queries) in addition to SELECT on information_schema.
//   - FlavorPlanetScale → [vstreamCDCReader], speaking Vitess's
//     VStream gRPC protocol against the PlanetScale-supplied
//     vtgate endpoint. Auth is HTTP Basic (service-token name +
//     value) ridden as gRPC metadata; the endpoint defaults to
//     `<sql-host>:443` and overrides via the `vstream_endpoint`
//     DSN parameter.
//
// Flavors declaring [ir.CDCNone] receive [ErrNotImplemented]; check
// the engine's [ir.Capabilities.CDC] before requesting a CDC reader.
//
// The caller is responsible for calling Close on the returned
// reader.
func (e Engine) OpenCDCReader(ctx context.Context, dsn string) (ir.CDCReader, error) {
	if e.Capabilities().CDC == ir.CDCNone {
		return nil, fmt.Errorf("%s: CDC not supported by this flavor: %w", e.Name(), ErrNotImplemented)
	}
	if e.Flavor == FlavorPlanetScale {
		return openVStreamReader(ctx, dsn)
	}
	// FlavorVanilla and any future binlog-based flavor land here.
	return openBinlogCDCReader(ctx, dsn)
}

// openBinlogCDCReader is the FlavorVanilla path of OpenCDCReader.
// Lifted out of OpenCDCReader so the flavor dispatch above stays
// readable.
func openBinlogCDCReader(ctx context.Context, dsn string) (ir.CDCReader, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	host, port, err := hostPortFromAddr(cfg.Addr)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql: parse host/port: %w", err)
	}
	return &CDCReader{
		db:          db,
		schema:      cfg.DBName,
		host:        host,
		port:        port,
		user:        cfg.User,
		password:    cfg.Passwd,
		serverID:    generateServerID(),
		tableMap:    make(map[uint64]string),
		schemaCache: make(map[string]*tableSchema),
	}, nil
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
	return &MigrationStateStore{db: db}, nil
}

// OpenChangeApplier returns a [ChangeApplier] bound to the database
// identified by dsn. The applier targets MySQL 8.0.20+ for its
// row-alias UPSERT syntax (INSERT ... AS new ON DUPLICATE KEY UPDATE
// col = new.col); older versions are not supported. The caller is
// responsible for closing the returned applier (via its Close method)
// to release the underlying connection pool.
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
		schema:       cfg.DBName,
		pkCache:      make(map[string][]string),
		colTypeCache: make(map[string]map[string]ir.Type),
	}, nil
}

// init registers each supported flavor under its own name in the
// engines registry. Adding a new flavor is a one-line addition here
// plus the corresponding entry in flavor.go.
func init() {
	engines.Register(Engine{Flavor: FlavorVanilla})
	engines.Register(Engine{Flavor: FlavorPlanetScale})
}
