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

// Engine is the MySQL implementation of [ir.Engine]. Each Open* call
// creates an independent connection; the value is cheap to copy and holds
// no connection state. Multiple Engines may safely coexist.
//
// The Flavor field selects between MySQL-compatible service variants
// (vanilla MySQL, PlanetScale, etc.). The zero value is FlavorVanilla
// so Engine{} continues to behave as a vanilla MySQL engine.
//
// The opts field carries the per-instance CLI-flag overrides that were
// formerly process-wide MUTABLE package globals set by the CLI (audit
// task 2.5 / finding A-4: SetSessionSQLMode, SetZeroDateMode,
// Set{Native,VStream}CopyTableParallelismOverride,
// SetVStreamPreserveSkewOverride). They are applied to the engine value
// the CLI resolves — via the With* builders (engine_options.go) — BEFORE
// it opens any reader/writer/CDC-reader, mirroring the [ir.ConnectionLabeler]
// pattern, so a fleet `sync run` can carry DISTINCT values per sync instead
// of one global spanning the whole process. Every opts zero value
// reproduces today's default behaviour for the many constructions (tests,
// broker/chain paths, non-CLI callers) that never set them; see
// [engineOptions] for the per-field zero-value contract.
type Engine struct {
	Flavor Flavor
	opts   engineOptions
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
	cfg, err := parseDSNForFlavor(dsn, e.Flavor)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg, e.opts.sqlMode)
	if err != nil {
		return nil, err
	}
	return &SchemaReader{db: db, schema: cfg.DBName, flavor: e.Flavor}, nil
}

// OpenSchemaWriter returns a [SchemaWriter] bound to the database
// identified by dsn. The caller is responsible for closing the
// returned SchemaWriter (via its Close method) to release the
// underlying connection pool.
func (e Engine) OpenSchemaWriter(ctx context.Context, dsn string) (ir.SchemaWriter, error) {
	cfg, err := parseDSNForFlavor(dsn, e.Flavor)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg, e.opts.sqlMode)
	if err != nil {
		return nil, err
	}
	// Thread the flavor so the overlapped index-build path (ADR-0080) can
	// decline the overlap on PlanetScale/Vitess targets and fall back to
	// the post-copy whole-schema CreateIndexes. The emitter carries the
	// resolved --mysql-sql-mode backslash policy (task 2.5) so every DDL
	// string literal this writer emits is escaped for the SAME sql_mode its
	// connections run under.
	w := &SchemaWriter{db: db, schema: cfg.DBName, flavor: e.Flavor, emitter: newMySQLEmitter(e.opts.sqlMode)}
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
	// ADR-0153 read-fidelity exemption: ROW-DATA READ sessions keep the
	// binary protocol — deliberately NOT parseDSNForFlavor. The exemption
	// was forced by ground truth: MySQL FLOAT→text conversion does not
	// round-trip float32 (8.0.46: stored 8388608 prints "8388610"), so a
	// TEXT-protocol page display-rounds FLOAT columns. That specific
	// hazard is now ALSO closed at the projection — selectColumnExpr
	// reads FLOAT through CAST(... AS DOUBLE), which fixed the arg-less
	// full-scan/first-page pages that were text-protocol on every release
	// (the pre-existing wart the ADR-0153 sweep found) — but the read
	// sessions stay on the binary protocol as defense in depth: the
	// prepared path is the one whose value fidelity does not depend on
	// the server's text formatter at all. Revisit (and re-run the read
	// parity matrix) if the chunk-read RTT ever measures as material. An
	// explicit operator interpolateParams=true in the DSN still wins.
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	// Per-sync zero-date policy (ADR-0127). Resolved before openDB so an
	// invalid zero_date DSN param refuses loudly without opening a connection.
	// The DSN param wins; absent, the engine's --zero-date default applies.
	zeroDate, err := readerZeroDateMode(cfg)
	if err != nil {
		return nil, err
	}
	zeroDate = e.resolveReaderZeroDate(zeroDate)
	// ADR-0109 §A: raise this source read pool's net_write_timeout /
	// net_read_timeout so a transient target-stall-induced backpressure
	// (the source read sitting idle while the writer can't drain) doesn't
	// trip the source server's default 60s net_write_timeout and drop the
	// cold-copy read. Bounded (10 min), operator-override-respecting.
	applySourceReadSessionTimeouts(cfg)
	db, err := openDB(ctx, cfg, e.opts.sqlMode)
	if err != nil {
		return nil, err
	}
	// Vitess/PlanetScale (CDCVStream flavors): a no-PK source table can't be
	// PK-chunked, so it is read as ONE unbounded streaming SELECT
	// ([RowReader.ReadRows]) — which vtgate's default OLTP workload silently
	// TRUNCATES at ~100k rows. That full scan needs `workload=olap` to lift
	// the cap (the pscale-dumper convention, see flavor.go).
	//
	// But olap must be scoped to JUST that full scan. Setting it session-wide
	// (a `workload` DSN param, as v0.99.14 did) makes it ALSO cover the
	// PK-bounded, LIMIT-paged [RowReader.ReadRowsBatch] the parallel chunked
	// bulk-copy uses — where vtgate's olap streaming truncates each
	// concurrently-read chunk's page, silently copying a tiny fraction of a
	// large PK table at default parallelism (Bug 132, the v0.99.14
	// regression). LIMIT-paged reads never approach the 100k cap, so they
	// never needed olap. The fix carries the intent to the reader and applies
	// `SET workload='olap'` on a DEDICATED connection inside ReadRows only;
	// ReadRowsBatch (PK-only by construction) stays olap-free, exactly as it
	// was before v0.99.14. An operator-supplied `workload` DSN param is their
	// explicit session choice and is left untouched (olapFullScan off).
	_, operatorSetWorkload := cfg.Params["workload"]
	olapFullScan := e.Capabilities().CDC == ir.CDCVStream && !operatorSetWorkload
	return &RowReader{q: db, schema: cfg.DBName, closer: db, olapFullScan: olapFullScan, zeroDate: zeroDate}, nil
}

// OpenRowWriter returns a [RowWriter] bound to the database identified
// by dsn. The writer chooses a bulk-load strategy based on the engine's
// declared [ir.Capabilities.BulkLoad], so vanilla MySQL and PlanetScale
// pick different paths from the same call. The caller is responsible
// for closing the returned RowWriter (via its Close method) to release
// the underlying connection pool.
func (e Engine) OpenRowWriter(ctx context.Context, dsn string) (ir.RowWriter, error) {
	cfg, err := parseDSNForFlavor(dsn, e.Flavor)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg, e.opts.sqlMode)
	if err != nil {
		return nil, err
	}
	return &RowWriter{
		db:       db,
		schema:   cfg.DBName,
		bulkLoad: e.Capabilities().BulkLoad,
		// Resolved --mysql-sql-mode (task 2.5): the LOAD DATA warning path keys
		// its WARN-vs-refuse decision off whether the operator opted into the
		// relaxed "" mode, replacing the former sessionSQLMode global read.
		sqlMode: e.opts.sqlMode,
		// ADR-0150 companion hint: only the HOSTED PlanetScale flavor
		// is tier-CPU-bound (self-hosted vitess runs on the operator's
		// own hardware, so the PS-tier ceiling doesn't apply).
		tierCPUBoundTarget: e.Flavor == FlavorPlanetScale,
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
		return openVStreamReader(ctx, dsn, e.Flavor, e.opts)
	}
	// FlavorVanilla and any future binlog-based flavor land here.
	return openBinlogCDCReader(ctx, dsn, e.opts)
}

// openBinlogCDCReader is the FlavorVanilla path of OpenCDCReader.
// Lifted out of OpenCDCReader so the flavor dispatch above stays
// readable.
func openBinlogCDCReader(ctx context.Context, dsn string, opts engineOptions) (ir.CDCReader, error) {
	return openBinlogCDCReaderShared(ctx, dsn, false, opts)
}

// openBinlogServerCDCReader opens a binlog CDC reader against a *server*
// DSN whose database component may be empty (ADR-0074 Phase 1b.2
// multi-database fan-out). The binlog is server-wide, so the reader's
// bound `schema` stays empty and the selected-database set is supplied
// separately via [CDCReader.SetCDCDatabaseScope]. The single-database
// path keeps the strict [parseDSN] (database required); this sibling
// relaxes only that precondition.
func openBinlogServerCDCReader(ctx context.Context, dsn string, opts engineOptions) (ir.CDCReader, error) {
	return openBinlogCDCReaderShared(ctx, dsn, true, opts)
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
	return openBinlogServerCDCReader(ctx, dsn, e.opts)
}

func openBinlogCDCReaderShared(ctx context.Context, dsn string, serverScope bool, opts engineOptions) (ir.CDCReader, error) {
	// Package-level parse (not parseDSNForFlavor): every caller is the
	// binlog path, reachable only from the vanilla flavor (the VStream
	// flavors branch to openVStreamReader before this), and vanilla keeps
	// the binary-protocol default under ADR-0153. A future binlog-based
	// non-vanilla flavor should switch this to parseDSNForFlavor.
	parse := parseDSN
	if serverScope {
		parse = parseServerDSN
	}
	cfg, err := parse(dsn)
	if err != nil {
		return nil, err
	}
	// Per-sync zero-date policy (ADR-0127), resolved before openDB so an
	// invalid zero_date DSN param refuses loudly without opening a connection.
	// The DSN param wins; absent, the engine's --zero-date default applies.
	zeroDate, err := readerZeroDateMode(cfg)
	if err != nil {
		return nil, err
	}
	zeroDate = foldZeroDate(zeroDate, opts.zeroDate)
	db, err := openDB(ctx, cfg, opts.sqlMode)
	if err != nil {
		return nil, err
	}
	host, port, err := hostPortFromAddr(cfg.Addr)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql: parse host/port: %w", err)
	}
	return &CDCReader{
		db:       db,
		schema:   cfg.DBName,
		zeroDate: zeroDate,
		host:     host,
		port:     port,
		user:     cfg.User,
		password: cfg.Passwd,
		// The binlog stream inherits the DSN's tls= transport (audit
		// finding N-3); an unregistered custom tls config name never
		// reaches here — parseDSN already refused it loudly.
		binlogTLS:      binlogTLSFromConfig(cfg, host),
		binlogTLSMode:  cfg.TLSConfig,
		serverID:       generateServerID(),
		tableMap:       make(map[uint64]string),
		schemaCache:    make(map[string]*tableSchema),
		snapshotSig:    make(map[string]ir.SchemaSignature),
		forwardNullSig: make(map[string]string),
	}, nil
}

// OpenMigrationStateStore returns a [MigrationStateStore] bound to
// the database identified by dsn. Implements
// [ir.MigrationStateStoreOpener]; the pipeline orchestrator type-
// asserts on this method so engines without a SQL surface for
// resumable migrations can omit it.
func (e Engine) OpenMigrationStateStore(ctx context.Context, dsn string) (ir.MigrationStateStore, error) {
	cfg, err := parseDSNForFlavor(dsn, e.Flavor)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg, e.opts.sqlMode)
	if err != nil {
		return nil, err
	}
	return newMigrationStateStore(db), nil
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
func (e Engine) OpenChangeApplier(ctx context.Context, dsn string) (ir.ChangeApplier, error) {
	cfg, err := parseDSNForFlavor(dsn, e.Flavor)
	if err != nil {
		return nil, err
	}
	// Bug 164: bypass target FK enforcement on every apply connection. A CDC
	// change stream is NOT FK-dependency-ordered — a source that doesn't
	// enforce FKs (SQLite default-off, MyISAM, or any app that deletes a
	// parent with children) emits orphaning changes, and ADR-0104's concurrent
	// key-hash lanes can commit a child INSERT before its parent in a different
	// lane. Enforcing target FKs against such a stream rejects a routine source
	// operation (Error 1452) and halts the sync. The go-sql-driver session init
	// emits each cfg.Params entry as `SET <key>=<value>` after the handshake
	// (see openDB / Bug 126), so this sets `foreign_key_checks=0` ONCE per
	// connection — covering both the serial pool (db) and the ADR-0104 lane
	// pool (openDB(pipelineCfg)) — with no per-statement overhead and no special
	// privilege. Constraint integrity is the SOURCE's responsibility (already
	// validated there); the target faithfully mirrors the source. No FK
	// constraints exist on sluice_cdc_state, so the control-table writes on
	// these connections are unaffected.
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["foreign_key_checks"] = "0"
	db, err := openDB(ctx, cfg, e.opts.sqlMode)
	if err != nil {
		return nil, err
	}
	return &ChangeApplier{
		db:     db,
		schema: cfg.DBName,
		// pipelineCfg retains the parsed DSN so the ADR-0104 concurrent
		// key-hash apply path can open its dedicated pool lazily on the first
		// concurrent batch (only when --apply-concurrency > 1 is wired via
		// SetApplyConcurrency). sqlMode is the engine's resolved --mysql-sql-mode
		// override (task 2.5) so the lazily-opened lane pool injects the SAME
		// session sql_mode as the serial pool.
		pipelineCfg:     cfg,
		sqlMode:         e.opts.sqlMode,
		controlKeyspace: e.opts.controlKeyspace,
		pkCache:         make(map[string][]string),
		colTypeCache:    make(map[string]map[string]*ir.Column),
		keylessCache:    make(map[string]bool),
		warnedKeyless:   make(map[string]bool),
		activeSchema:    make(map[string]activeSchemaVersion),
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
func (e Engine) ListDatabases(ctx context.Context, dsn string) ([]string, error) {
	cfg, err := parseServerDSNForFlavor(dsn, e.Flavor)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg, e.opts.sqlMode)
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
	// ADR-0153 explicit-DSN-wins preservation: FormatDSN omits any param
	// whose cfg value equals the driver default, so an operator's explicit
	// `interpolateParams=false` would silently VANISH from the derived DSN
	// — and a downstream flavor-aware parse would then apply the
	// PlanetScale/Vitess interpolation default the operator opted out of.
	// Re-materialize the explicit false as a Params entry so FormatDSN
	// carries it. (Explicit true survives on its own: a non-default cfg
	// field is always emitted. Today's multi-database fan-out is
	// vanilla-only, where no default exists — this guards the contract for
	// any future VStream multi-database path.)
	if dsnSetsInterpolateParams(dsn) && !clone.InterpolateParams {
		if clone.Params == nil {
			clone.Params = map[string]string{}
		}
		clone.Params["interpolateParams"] = "false"
	}
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
func (e Engine) EnsureDatabase(ctx context.Context, dsn, database string) error {
	if database == "" {
		return errors.New("mysql: EnsureDatabase: database name is empty")
	}
	cfg, err := parseServerDSN(dsn)
	if err != nil {
		return err
	}
	// Connect at the server level — the target database may not exist
	// yet, so a database-scoped DSN would fail to connect. (The
	// interpolation flavor default is immaterial on this single-DDL
	// probe connection but applied for consistency.)
	cfg = cfg.Clone()
	cfg.DBName = ""
	db, err := openDB(ctx, cfg, e.opts.sqlMode)
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

// FoldNamespace implements [ir.NamespaceFolder]: it reports the MySQL
// database identifier a source namespace `name` would land under on the
// server dsn points at, accounting for the server's
// `lower_case_table_names` (lct) setting (ADR-0075 resolved decision #1).
//
// MySQL folds database (and table) names per lct:
//
//   - lct=0 (the common Linux default): names are stored and compared
//     case-SENSITIVELY — no fold, identity.
//   - lct=1 (the Windows / macOS default, and some managed services):
//     names are stored lowercased and compared case-insensitively — the
//     fold is strings.ToLower.
//   - lct=2 (macOS default): stored as given but compared
//     case-insensitively — for COLLISION purposes this behaves like a
//     lowercase fold (two names equal under case-insensitive compare).
//
// So lct != 0 ⇒ lowercase fold. The orchestrator uses the returned value
// only to detect two distinct source namespaces folding to the same
// target database (a silent-merge hazard it refuses loudly). Identity
// (lct=0) means no folding-induced collision is possible.
//
// Used on a MySQL TARGET of a PG-source (or MySQL-source) multi-namespace
// fan-out. dsn may be a server DSN (database component optional).
func (e Engine) FoldNamespace(ctx context.Context, dsn, name string) (string, error) {
	cfg, err := parseServerDSN(dsn)
	if err != nil {
		return "", err
	}
	db, err := openDB(ctx, cfg, e.opts.sqlMode)
	if err != nil {
		return "", err
	}
	defer func() { _ = db.Close() }()

	var lct int
	if err := db.QueryRowContext(ctx, "SELECT @@global.lower_case_table_names").Scan(&lct); err != nil {
		return "", fmt.Errorf("mysql: read lower_case_table_names: %w", err)
	}
	if lct != 0 {
		return strings.ToLower(name), nil
	}
	return name, nil
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

// DiscoverShards implements [ir.ShardDiscoverer] (Bug 152): it reports
// the source keyspace's shard layout so the orchestrator's cross-shard-
// collision preflight can refuse a multi-shard source merging into a
// single non-discriminated target.
//
// Only VStream flavors (PlanetScale / self-hosted Vitess) can be sharded;
// a non-VStream flavor (vanilla MySQL) returns (nil, nil) WITHOUT
// connecting, so the guard is free for the common case. For a VStream
// flavor it queries the vtgate (`SHOW VITESS_SHARDS`) via the same
// [discoverShards] helper the reader uses to enumerate shards — an
// unsharded keyspace returns a single shard, a sharded one returns N.
func (e Engine) DiscoverShards(ctx context.Context, dsn string) ([]string, error) {
	if !e.Flavor.usesVStream() {
		return nil, nil
	}
	cfg, err := parseDSNForFlavor(dsn, e.Flavor)
	if err != nil {
		return nil, fmt.Errorf("mysql: discover shards: %w", err)
	}
	return discoverShards(ctx, cfg, cfg.DBName)
}

// ValidateDSN implements [ir.DSNValidator]: it refuses, from the DSN
// string alone (no connection), a DSN this flavor cannot faithfully
// drive. The one such case today is the vanilla MySQL flavor pointed
// at a PlanetScale endpoint (*.connect.psdb.cloud): the vanilla flavor
// uses binlog CDC and LOAD DATA cold-copy, both of which Vitess blocks,
// so the run would otherwise fail obscurely partway through — the
// operator wants the `planetscale` driver for a PlanetScale host.
//
// The VStream flavors (planetscale / vitess) correctly drive such a
// host and are a no-op here. The error is role-AGNOSTIC — ValidateDSN
// doesn't know whether this DSN is the source or the target — so it
// names the host and the reason; the pipeline supplies the role and
// the exact --source-driver / --target-driver flag.
func (e Engine) ValidateDSN(dsn string) error {
	if e.Flavor.usesVStream() {
		return nil
	}
	host, ok := planetScaleMySQLHost(dsn)
	if !ok {
		return nil
	}
	return fmt.Errorf(
		"the DSN host %q is a PlanetScale endpoint, which the vanilla mysql flavor cannot drive: "+
			"it uses binlog CDC and LOAD DATA cold-copy, both blocked by Vitess/PlanetScale — "+
			"use the planetscale driver for this host instead",
		host,
	)
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

// planetScaleMySQLHost reports whether dsn parses to a hostname matching
// one of the documented PlanetScale MySQL endpoint suffixes, returning
// the matched (lowercased) host so callers can name it. Returns
// ("", false) on parse failure or non-host DSN forms (Unix socket,
// etc.) — those configurations don't match PSDB by construction. The
// host is lowercased before matching: PSDB hostnames are
// case-insensitive and operators sometimes paste mixed-case.
func planetScaleMySQLHost(dsn string) (string, bool) {
	if dsn == "" {
		return "", false
	}
	cfg, err := parseDSN(dsn)
	if err != nil {
		return "", false
	}
	host, _, err := hostPortFromAddr(cfg.Addr)
	if err != nil {
		return "", false
	}
	host = strings.ToLower(host)
	for _, suffix := range planetScaleMySQLHostSuffixes {
		if strings.HasSuffix(host, suffix) {
			return host, true
		}
	}
	return "", false
}

// isPlanetScaleMySQLHost reports whether dsn is a PlanetScale MySQL
// endpoint (see [planetScaleMySQLHost]).
func isPlanetScaleMySQLHost(dsn string) bool {
	_, ok := planetScaleMySQLHost(dsn)
	return ok
}

// init registers each supported flavor under its own name in the
// engines registry. Adding a new flavor is a one-line addition here
// plus the corresponding entry in flavor.go.
func init() {
	engines.Register(Engine{Flavor: FlavorVanilla})
	engines.Register(Engine{Flavor: FlavorPlanetScale})
	engines.Register(Engine{Flavor: FlavorVitess})
}
