// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

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
//	import _ "sluicesync.dev/sluice/internal/engines/mysql"
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
	"strings"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
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
func (e Engine) OpenSchemaReader(ctx context.Context, dsn string) (ir.SchemaReader, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &SchemaReader{db: db, schema: cfg.DBName, flavor: e.Flavor}, nil
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
	w := &SchemaWriter{db: db, schema: cfg.DBName}
	// Probe SELECT VERSION() once at open so the v0.97.0 inline-CHECK
	// path knows whether the target is MySQL 8.0.16+. A probe failure
	// is non-fatal: zero-value inlineCheckSupported (false) preserves
	// the pre-v0.97.0 WARN-only behavior, which is the safe default
	// — no inline CHECK is emitted, no regression from prior releases.
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err == nil {
		w.inlineCheckSupported = mysqlVersionSupportsInlineCheck(version)
	}
	return w, nil
}

// OpenRowReader returns a [RowReader] bound to the database identified
// by dsn. The caller is responsible for closing the returned RowReader
// (via its Close method) to release the underlying connection pool.
func (e Engine) OpenRowReader(ctx context.Context, dsn string) (ir.RowReader, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	// Vitess/PlanetScale (CDCVStream flavors): the bulk-read is a single
	// streaming SELECT (buildSelect → one QueryContext, no transaction), and
	// vtgate's default OLTP workload caps a result set at ~100k rows. A no-PK
	// source table can't be PK-chunked, so its full-scan copy is one big
	// SELECT — which the cap would silently truncate at 100k. `workload=olap`
	// lifts the cap and streams, exactly the pscale-dumper convention (see
	// flavor.go). Applied ONLY to the reader's session — never the
	// writer/applier, which DO use transactions that OLAP mode forbids. Not a
	// valid var on vanilla MySQL, so it is gated on the VStream flavor. A
	// DSN-supplied `workload` wins (operator override).
	if e.Capabilities().CDC == ir.CDCVStream {
		if cfg.Params == nil {
			cfg.Params = map[string]string{}
		}
		if _, ok := cfg.Params["workload"]; !ok {
			cfg.Params["workload"] = "'olap'"
		}
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
	if e.Flavor.usesVStream() {
		return openVStreamReader(ctx, dsn, e.Flavor)
	}
	// FlavorVanilla and any future binlog-based flavor land here.
	return openBinlogCDCReader(ctx, dsn)
}

// openBinlogCDCReader is the FlavorVanilla path of OpenCDCReader.
// Lifted out of OpenCDCReader so the flavor dispatch above stays
// readable.
func openBinlogCDCReader(ctx context.Context, dsn string) (ir.CDCReader, error) {
	return openBinlogCDCReaderShared(ctx, dsn, false)
}

// openBinlogServerCDCReader opens a binlog CDC reader against a *server*
// DSN whose database component may be empty (ADR-0074 Phase 1b.2
// multi-database fan-out). The binlog is server-wide, so the reader's
// bound `schema` stays empty and the selected-database set is supplied
// separately via [CDCReader.SetCDCDatabaseScope]. The single-database
// path keeps the strict [parseDSN] (database required); this sibling
// relaxes only that precondition.
func openBinlogServerCDCReader(ctx context.Context, dsn string) (ir.CDCReader, error) {
	return openBinlogCDCReaderShared(ctx, dsn, true)
}

// OpenServerCDCReader opens a server-wide binlog CDC reader against a
// database-optional DSN (ADR-0074 Phase 1b.3, [ir.ServerCDCReaderOpener]).
// It is the snapshot-less sibling of [Engine.OpenMultiDatabaseSnapshotStream]'s
// Changes reader: a multi-database `sync start` WARM-RESUME has a persisted
// server-wide binlog position and must resume the single server-wide stream
// (re-scoped to the selected database set) WITHOUT a fresh cold-start
// snapshot. The orchestrator scopes the returned reader via
// [ir.CDCDatabaseScoper.SetCDCDatabaseScope] and resumes StreamChanges from
// the persisted position.
//
// VStream flavors (planetscale / vitess) are keyspace-scoped, so a
// server-wide CDC reader is not their model; they refuse loudly (multi-
// keyspace CDC is the Phase 1c N-stream design).
func (e Engine) OpenServerCDCReader(ctx context.Context, dsn string) (ir.CDCReader, error) {
	if e.Capabilities().CDC == ir.CDCNone {
		return nil, fmt.Errorf("%s: CDC not supported by this flavor: %w", e.Name(), ErrNotImplemented)
	}
	if e.Flavor.usesVStream() {
		return nil, fmt.Errorf(
			"%s: server-wide CDC resume is not supported on the VStream flavors (planetscale / vitess); "+
				"VStream is keyspace-scoped and multi-keyspace CDC is a distinct N-stream design (ADR-0074 Phase 1c): %w",
			e.Name(), ErrNotImplemented,
		)
	}
	return openBinlogServerCDCReader(ctx, dsn)
}

func openBinlogCDCReaderShared(ctx context.Context, dsn string, serverScope bool) (ir.CDCReader, error) {
	parse := parseDSN
	if serverScope {
		parse = parseServerDSN
	}
	cfg, err := parse(dsn)
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
		snapshotSig: make(map[string]ir.SchemaSignature),
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
		colTypeCache: make(map[string]map[string]*ir.Column),
		activeSchema: make(map[string]activeSchemaVersion),
	}, nil
}

// systemDatabases is the closed set of MySQL server-internal databases
// that are NEVER user data and are always excluded from a
// multi-database fan-out (ADR-0074), even under `--all-databases`. The
// lookup is lowercase-keyed: MySQL database names are case-insensitive
// on the default (Linux server / lower_case_table_names) configurations
// sluice targets, and these four are spelled lowercase by convention.
var systemDatabases = map[string]struct{}{
	"information_schema": {},
	"performance_schema": {},
	"mysql":              {},
	"sys":                {},
}

// ListDatabases implements [ir.DatabaseLister]: it enumerates every
// non-system database visible to the connection in dsn. Used by the
// multi-database migrate orchestrator to resolve `--all-databases` /
// `--include-database` / `--exclude-database` globs into a concrete
// database set (ADR-0074).
//
// The dsn is a *server* connection — its database component may be
// empty (the operator drove a multi-database run without naming one),
// so this opens via [parseServerDSN] rather than [parseDSN]. The system
// databases (information_schema, performance_schema, mysql, sys) are
// filtered out unconditionally per the ADR.
func (Engine) ListDatabases(ctx context.Context, dsn string) ([]string, error) {
	cfg, err := parseServerDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	const q = `SELECT schema_name FROM information_schema.schemata ORDER BY schema_name`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("mysql: list databases: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("mysql: list databases: scan: %w", err)
		}
		if _, sys := systemDatabases[strings.ToLower(name)]; sys {
			continue
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: list databases: %w", err)
	}
	return out, nil
}

// WithDatabase implements [ir.DatabaseDSNDeriver]: it returns a clone of
// dsn whose database component is set to database, leaving every other
// DSN element (credentials, host, params) untouched. Used by the
// multi-database fan-out orchestrator (ADR-0074) to re-open a
// single-database reader/writer per selected database. The clone is
// produced via the driver's own ParseDSN/FormatDSN so the round-trip
// is faithful for unix sockets, params, and TLS configs alike.
func (Engine) WithDatabase(dsn, database string) (string, error) {
	cfg, err := parseServerDSN(dsn)
	if err != nil {
		return "", err
	}
	// FormatDSN re-emits the keep-alive network name finishParseDSN
	// swapped in; reset it to the wire-standard `tcp` so the re-parsed
	// clone routes through parseDSN's own keep-alive swap rather than
	// double-prefixing. The vstream_* params are stripped at openDB, so
	// leaving them on the clone is harmless.
	if cfg.Net == keepaliveNet {
		cfg.Net = "tcp"
	}
	clone := cfg.Clone()
	clone.DBName = database
	return clone.FormatDSN(), nil
}

// EnsureDatabase implements [ir.DatabaseDSNDeriver]: it issues
// `CREATE DATABASE IF NOT EXISTS` for database against the server dsn
// points at (ADR-0074, MySQL → MySQL auto-create-target-database). It
// connects at the server level (database component optional) so a
// freshly-provisioned target server with no per-source databases yet
// still works. Idempotent.
//
// The database identifier is backtick-quoted via quoteIdent so a
// database name with reserved-word or special-character shape is safe;
// embedded backticks are doubled by quoteIdent.
func (Engine) EnsureDatabase(ctx context.Context, dsn, database string) error {
	if database == "" {
		return errors.New("mysql: EnsureDatabase: database name is empty")
	}
	cfg, err := parseServerDSN(dsn)
	if err != nil {
		return err
	}
	// Connect at the server level — the target database may not exist
	// yet, so a database-scoped DSN would fail to connect.
	cfg = cfg.Clone()
	cfg.DBName = ""
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	stmt := "CREATE DATABASE IF NOT EXISTS " + quoteIdent(database)
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("mysql: create database %q: %w", database, wrapDDLError(err))
	}
	return nil
}

// DefaultExcludePatterns returns flavor-specific or DSN-derived
// table patterns the orchestrator should merge into the operator's
// exclude list (only when the operator hasn't supplied
// --include-table). Implements [ir.DefaultTableExcluder].
//
// Vitess maintains internal lifecycle "shadow tables" —
// `_vt_HOLD_<uuid>_<ts>` (legacy naming) and `_vt_hld_<uuid>_<ts>_`
// / `_vt_prg_*` / `_vt_evc_*` / `_vt_drp_*` (post-PR-14613 naming
// with a trailing underscore for FK-suffix headroom) — that aren't
// user data. Including them in publication / bulk-copy generates
// quiet write churn against the target with no operator-visible
// signal. The `_vt_` prefix is the Vitess-reserved namespace; a
// single `_vt_*` glob covers both legacy and new naming.
//
// Two paths trigger the auto-exclusion:
//
//  1. **Driver-flag-keyed (v0.8.0):** when the operator chose
//     `--source-driver=planetscale`, the flavor declares the default
//     unconditionally — the operator's choice is unambiguous.
//
//  2. **DSN-hostname-keyed (v0.8.1):** when the operator chose
//     `--source-driver=mysql` (vanilla flavor) but the DSN points at
//     a PlanetScale endpoint, sluice still applies the exclusion. A
//     vanilla MySQL connection to a PlanetScale endpoint is a
//     legitimate configuration — the operator gets binlog CDC
//     instead of VStream — but the underlying server is still
//     Vitess and the shadow tables are still there. PlanetScale's
//     hostnames follow stable patterns that we can sniff at
//     orchestrator startup before any DB call:
//
//     - `*.connect.psdb.cloud` (public PlanetScale MySQL)
//     - `*.private-connect.psdb.cloud` (AWS PrivateLink)
//
//     PG-side PlanetScale endpoints (`*.pg.psdb.cloud`,
//     `*.private-pg.psdb.cloud`) aren't Vitess-backed and don't
//     have `_vt_*` shadow tables; they're noted here for symmetry
//     and would slot into the PG engine's own `DefaultTableExcluder`
//     implementation if that future need ever surfaces.
//
// Non-PlanetScale Vitess deployments (custom domains) still need a
// manual `--exclude-table='_vt_*'`. Auto-detect via `@@version_comment`
// would catch them but requires a connection round-trip and adds a
// failure mode (auth/network race); the hostname sniff is cheap and
// deterministic. If a non-PlanetScale Vitess user reports the gap,
// the connection-probe path can be added then.
//
// Operators who need to inspect or migrate `_vt_*` tables (rare —
// usually a debugging exercise) override by passing
// `--include-table` explicitly, which short-circuits the default.
func (e Engine) DefaultExcludePatterns(dsn string) []string {
	if e.Flavor.usesVStream() {
		return []string{"_vt_*"}
	}
	if isPlanetScaleMySQLHost(dsn) {
		return []string{"_vt_*"}
	}
	return nil
}

// planetScaleMySQLHostSuffixes is the closed set of DNS suffixes
// PlanetScale's MySQL service uses today. Lowercase comparison; the
// wider PSDB platform may add suffixes (region-specific shards,
// etc.) — kept as a package-level slice so adding one is a one-line
// edit.
var planetScaleMySQLHostSuffixes = []string{
	".connect.psdb.cloud",
	".private-connect.psdb.cloud",
}

// isPlanetScaleMySQLHost reports whether dsn parses to a hostname
// matching one of the documented PlanetScale MySQL endpoint
// suffixes. Returns false on parse failure or non-host DSN forms
// (Unix socket, etc.) — those configurations don't match PSDB by
// construction. Lowercases the host before matching: PSDB hostnames
// are case-insensitive and operators sometimes paste mixed-case.
func isPlanetScaleMySQLHost(dsn string) bool {
	if dsn == "" {
		return false
	}
	cfg, err := parseDSN(dsn)
	if err != nil {
		return false
	}
	host, _, err := hostPortFromAddr(cfg.Addr)
	if err != nil {
		return false
	}
	host = strings.ToLower(host)
	for _, suffix := range planetScaleMySQLHostSuffixes {
		if strings.HasSuffix(host, suffix) {
			return true
		}
	}
	return false
}

// init registers each supported flavor under its own name in the
// engines registry. Adding a new flavor is a one-line addition here
// plus the corresponding entry in flavor.go.
func init() {
	engines.Register(Engine{Flavor: FlavorVanilla})
	engines.Register(Engine{Flavor: FlavorPlanetScale})
	engines.Register(Engine{Flavor: FlavorVitess})
}
