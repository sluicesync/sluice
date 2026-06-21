// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/netkeepalive"
)

// keepaliveNet is a custom driver "network" name registered with the
// MySQL driver that routes plain-TCP connections through sluice's
// shared TCP keep-alive dialer (see [netkeepalive]). parseDSN swaps a
// `tcp` DSN onto this network so every MySQL query connection inherits
// the keep-alive policy; unix sockets and operator-specified networks
// are left untouched (TCP keep-alive is meaningless off TCP).
const keepaliveNet = "tcp+sluicekeepalive"

func init() {
	mysql.RegisterDialContext(keepaliveNet, func(ctx context.Context, addr string) (net.Conn, error) {
		return netkeepalive.Dialer().DialContext(ctx, "tcp", addr)
	})
}

// defaultStrictSQLMode is the v0.92.1 strict-by-default mode list
// applied to every MySQL connection unless the operator overrides
// it via --mysql-sql-mode (CLI) or `sql_mode` in the DSN params.
// Closes Bugs 102/103 silent-loss class by surfacing MySQL's own
// loud-error path instead of inheriting whatever sql_mode the
// server defaults to (often relaxed on dev / older / managed
// deployments).
const defaultStrictSQLMode = "STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO"

// sessionSQLMode is the value sluice injects into every MySQL
// connection's `SET SESSION sql_mode = '...'` post-handshake. Set
// once at process startup by [SetSessionSQLMode] (called from
// main.go with the operator's --mysql-sql-mode value). Default
// value matches [defaultStrictSQLMode] so a bare `go test ./...`
// or any path that doesn't go through main() still gets the strict
// mode.
//
// Empty string is the explicit "fall through to server default"
// path — operators with legacy MySQL data (zero-dates / silently-
// truncated values) opt out via --mysql-sql-mode=” .
var sessionSQLMode = defaultStrictSQLMode

// SetSessionSQLMode overrides the sql_mode sluice forces on every
// MySQL connection. main.go calls this once at startup with the
// operator's --mysql-sql-mode CLI value. The DSN-level override
// (`?sql_mode=...` query param in the connection string) takes
// precedence over this value if both are set.
//
// Empty string disables the override — no SET SESSION sql_mode is
// issued, so the server's own default applies. Use this when
// migrating legacy MySQL data with zero-dates / silently-truncated
// values that a strict mode would refuse.
//
// Concurrency: this is process-wide global state set once at
// startup, before any engine opens a connection. Don't call it
// from long-lived goroutines.
func SetSessionSQLMode(modes string) {
	sessionSQLMode = modes
}

// dsnShapeHint inspects a DSN that failed to parse and returns a
// short, leading-newline-terminated hint when sluice can recognise
// a known operator-side mistake. Returns the empty string for
// unknown shapes so the driver's own error message stays first.
//
// Currently recognises:
//
//   - "/db/branch" path segment — PlanetScale credentials are
//     branch-scoped (the branch is implicit in the user/password),
//     so the DSN path should be just the database name. The
//     driver's generic "did you forget to escape a param value?"
//     hint is misleading here.
//
// More patterns can be added as operator reports surface them.
func dsnShapeHint(dsn string) string {
	// MySQL DSN shape: `user:pw@protocol(address)/dbname?params`.
	// The path component is after `protocol(address)` — we have to
	// skip the `(...)` block because addresses can contain `/`
	// (unix sockets like `/tmp/mysql.sock`).
	at := strings.LastIndex(dsn, "@")
	if at < 0 {
		return ""
	}
	rest := dsn[at+1:]
	// Strip query string before counting path segments.
	if q := strings.Index(rest, "?"); q >= 0 {
		rest = rest[:q]
	}
	// Skip a `(...)` address block if present. This handles unix
	// sockets like `unix(/tmp/mysql.sock)/foo` whose internal `/`
	// would otherwise be mis-read as a path separator.
	if openIdx := strings.Index(rest, "("); openIdx >= 0 {
		if closeIdx := strings.Index(rest[openIdx:], ")"); closeIdx >= 0 {
			rest = rest[openIdx+closeIdx+1:]
		}
	}
	// Now `rest` is the path component (with leading `/` if present).
	if !strings.HasPrefix(rest, "/") {
		return ""
	}
	path := rest[1:]
	if strings.Contains(path, "/") {
		return "DSN path appears to contain `database/branch` (PlanetScale-style); credentials are branch-scoped so the path should be just the database name — try removing the `/branch` segment. Underlying error: "
	}
	return ""
}

// parseDSN parses and validates a MySQL DSN, applying the parameter
// adjustments sluice requires for correct behaviour:
//
//   - parseTime=true: driver returns time.Time for DATE/DATETIME/TIMESTAMP
//     instead of []byte, which lets the row pipeline use Go-native types.
//   - loc=UTC: timestamps are returned in UTC regardless of session
//     timezone, removing one source of cross-engine ambiguity.
//   - time_zone='+00:00' (issued via cfg.Params on every new connection):
//     forces the MySQL session to emit TIMESTAMP wire values in UTC
//     regardless of the server's default_time_zone or the host the
//     server is running on. Without this, a MySQL server whose session
//     time_zone inherits the host TZ (e.g. PT) converts the column's
//     UTC-stored TIMESTAMP into PT for the wire format; the driver then
//     parses that wall-clock as UTC (because of cfg.Loc), corrupting
//     the value by exactly the offset. Bug 19. The CDC binlog path is
//     immune to the SESSION time_zone variable (binlog encodes UTC
//     epoch directly) but susceptible to a separate process-local-TZ
//     formatting bug; that one is fixed in cdc_reader.go via
//     TimestampStringLocation.
//
// The DSN must include a database name; sluice operates against an
// explicit schema rather than connecting at the server level.
func parseDSN(dsn string) (*mysql.Config, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		// GitHub issue #17 papercut: the driver's error message already
		// starts with "invalid DSN: ..."; wrapping with our own
		// "mysql: invalid DSN: %w" produces a confusing double prefix
		// ("mysql: invalid DSN: invalid DSN: ..."). Strip the driver's
		// own "invalid DSN:" prefix before wrapping; if the driver
		// reports a different shape (a future driver version may
		// change the prefix), the original wrap still applies.
		msg := err.Error()
		const dupPrefix = "invalid DSN: "
		if strings.HasPrefix(msg, dupPrefix) {
			//nolint:errorlint // intentional: rewriting prefix for operator readability; original chain preserved via errors.Is below if needed
			return nil, fmt.Errorf("mysql: invalid DSN: %s%s", dsnShapeHint(dsn), msg[len(dupPrefix):])
		}
		return nil, fmt.Errorf("mysql: invalid DSN: %s%w", dsnShapeHint(dsn), err)
	}
	if cfg.DBName == "" {
		return nil, errors.New("mysql: DSN must include a database name")
	}
	return finishParseDSN(cfg), nil
}

// parseServerDSN is the database-OPTIONAL sibling of [parseDSN], used
// by the multi-database fan-out path (ADR-0074). The single-database
// migrate / sync path requires a database in the DSN ([parseDSN]); when
// the operator drives a multi-database run with `--all-databases` /
// `--include-database` / `--exclude-database`, the source DSN is a
// *server* connection whose database component may legitimately be
// empty — the orchestrator enumerates databases via [DatabaseLister]
// and re-opens a single-database reader per database. Every other DSN
// adjustment ([finishParseDSN]: keep-alive dialer, parseTime, UTC loc,
// time_zone, sql_mode, utf8mb4) applies identically; only the
// non-empty-DBName precondition is relaxed.
func parseServerDSN(dsn string) (*mysql.Config, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		msg := err.Error()
		const dupPrefix = "invalid DSN: "
		if strings.HasPrefix(msg, dupPrefix) {
			//nolint:errorlint // intentional: rewriting prefix for operator readability; matches parseDSN
			return nil, fmt.Errorf("mysql: invalid DSN: %s%s", dsnShapeHint(dsn), msg[len(dupPrefix):])
		}
		return nil, fmt.Errorf("mysql: invalid DSN: %s%w", dsnShapeHint(dsn), err)
	}
	return finishParseDSN(cfg), nil
}

// finishParseDSN applies the sluice-required parameter adjustments to a
// parsed [mysql.Config] — the shared tail of [parseDSN] and
// [parseServerDSN]. Split out so the only difference between the two
// entry points is whether an empty DBName is an error.
func finishParseDSN(cfg *mysql.Config) *mysql.Config {
	// Route plain-TCP query connections through the keep-alive dialer.
	// Long-lived pools (the change applier, schema reader) would
	// otherwise sit idle behind cloud NAT and stall on a dropped
	// mapping. Non-TCP networks (unix sockets) are left as-is.
	if cfg.Net == "tcp" {
		cfg.Net = keepaliveNet
	}

	cfg.ParseTime = true
	cfg.Loc = time.UTC

	// The driver's handleParams emits each cfg.Params entry as
	// `SET <key> = <value>` after the connection handshake. Quoting
	// is preserved verbatim, so the value must include the SQL
	// quotes for a literal time-zone offset string.
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	if _, ok := cfg.Params["time_zone"]; !ok {
		cfg.Params["time_zone"] = "'+00:00'"
	}
	// Bug 102 + Bug 103 (CRITICAL silent-loss, v0.92.1). Pre-fix
	// sluice inherited the MySQL server's sql_mode, which on dev
	// containers and some managed deployments doesn't include the
	// strict modes — so PG `NUMERIC(40,5)` values overflowing
	// MySQL `DECIMAL(65,30)` silently clamped to the column max
	// (every overflowing row → same constant; Bug 102 CRITICAL silent
	// data loss), and PG `TIMESTAMPTZ` values outside MySQL
	// `TIMESTAMP` range silently became `0000-00-00 00:00:00` (Bug
	// 103). Both manifestations LOUD-refused on a PG target because
	// PG always enforces; sluice's silent-on-MySQL was a pure
	// connection-config oversight.
	//
	// The injected sql_mode follows a two-tier override policy:
	//
	//   1. DSN-level override (`sql_mode=...` in the connection
	//      string params) wins absolutely. An operator who needs
	//      different modes for source vs target sets each DSN's own
	//      sql_mode.
	//   2. CLI-level override via --mysql-sql-mode (threaded into
	//      [sessionSQLMode] from main.go). Empty string means "fall
	//      through to server default" — the legacy-data escape hatch
	//      for migrations involving zero-dates / silently-truncated
	//      values that pre-MySQL-5.7 schemas commonly carry.
	//
	// If neither is set, [defaultStrictSQLMode] applies (the
	// loud-failure-tenet default). The literal-quotes pattern matches
	// the time_zone override above.
	if _, ok := cfg.Params["sql_mode"]; !ok && sessionSQLMode != "" {
		cfg.Params["sql_mode"] = "'" + sessionSQLMode + "'"
	}
	// Bug 106 (v0.92.1). Pre-fix the connection's default character
	// set could fall back to 3-byte utf8 on older MySQL servers /
	// managed deployments, silently corrupting 4-byte UTF-8 sequences
	// (emoji, supplementary-plane glyphs) — observed concretely when
	// MySQL → PG schema-read encountered an ENUM whose labels
	// contained 4-byte UTF-8, which arrived in sluice's IR as `?`
	// substitutes and then loud-failed at the target row INSERT (the
	// loud-fail was the visible symptom; the silent label corruption
	// was the silent class). Forcing utf8mb4 here ensures the
	// connection charset always supports the full Unicode range, so
	// 4-byte sequences round-trip cleanly. utf8mb4_general_ci is the
	// safe default — operators who need a different collation can
	// override in the DSN.
	if cfg.Collation == "" {
		cfg.Collation = "utf8mb4_general_ci"
	}

	return cfg
}

// sourceReadSessionTimeoutSeconds is the bounded value sluice applies to
// `net_write_timeout` / `net_read_timeout` on every MySQL SOURCE read
// session it opens for a cold-copy (ADR-0109 §A — PRIMARY defense).
//
// The mechanism it prevents: a transient TARGET stall (a non-Metal
// PlanetScale storage auto-grow that BLOCKS the target's writes for
// seconds-to-minutes under semi-sync) backpressures sluice's reader/writer
// pipeline — the writer can't drain, so the reader stops consuming, so the
// SOURCE read connection sits idle. The source server's default
// `net_write_timeout` is 60s; once the idle read crosses it, the source
// CLOSES the connection (`unexpected EOF` / `invalid connection`) and the
// whole cold-copy aborts. Raising the source session's timeout to a
// generous bound lets the read survive the stall: when the target recovers,
// the writer drains, the reader resumes, and the copy continues — no
// reconnect, no re-snapshot, no consistency problem (the per-table reconnect
// (C) and the cold-start auto-restart (B) are the BACKSTOPS for a stall that
// outlives even this raised bound).
//
// 600s (10 min) is deliberately FINITE: a genuinely-dead target still
// surfaces (the read eventually drops and sluice's source-unresponsive
// detection + the (B)/(C) retries take over) rather than hanging forever.
//
// Zero-value-safe by construction: this is a package CONSTANT, not a config
// field — there is no EnableX-defaulting-true trap (the v0.99.51 lesson).
// Every construction path that opens a source read session
// ([applySourceReadSessionTimeouts] below) gets the same bound; an operator
// who needs a different value sets `net_write_timeout` / `net_read_timeout`
// directly in the source DSN params, which wins (the helper never overwrites
// an operator-supplied value).
const sourceReadSessionTimeoutSeconds = 600

// applySourceReadSessionTimeouts injects `net_write_timeout` /
// `net_read_timeout` into cfg.Params (ADR-0109 §A) so the go-sql-driver
// emits `SET <key> = <value>` on every connection in the pool at session
// init — covering EVERY source read session for free: the dedicated
// full-scan conn, the LIMIT-paged chunked ReadRowsBatch reads, and the
// snapshot path's pinned REPEATABLE-READ connection(s). Scoped to the
// SOURCE-read open paths (OpenRowReader + the binlog snapshot openers) so
// the target write/applier sessions are untouched — the timeout is a
// source-side defense, not a target one.
//
// An operator-supplied DSN value for either key wins absolutely (same
// two-tier override shape as sql_mode / time_zone above): the helper only
// sets a key that is absent, so a deliberate per-source tuning is never
// clobbered. The numeric value is emitted bare (no SQL quotes) — these are
// integer session variables, unlike the quoted string literals time_zone /
// sql_mode require.
func applySourceReadSessionTimeouts(cfg *mysql.Config) {
	if cfg == nil {
		return
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	val := fmt.Sprintf("%d", sourceReadSessionTimeoutSeconds)
	if _, ok := cfg.Params["net_write_timeout"]; !ok {
		cfg.Params["net_write_timeout"] = val
	}
	if _, ok := cfg.Params["net_read_timeout"]; !ok {
		cfg.Params["net_read_timeout"] = val
	}
}

// vstreamParamPrefix is the DSN-parameter namespace sluice reserves
// for its Vitess VStream extensions (vstream_endpoint, vstream_transport,
// vstream_auth, vstream_shards, vstream_auto_discover_shards,
// vstream_insecure_tls, …). These are sluice-internal DSN flags; they
// are never valid MySQL session variables.
const vstreamParamPrefix = "vstream_"

// nativeSluiceParams are sluice-internal source-DSN knobs that are NOT under
// the vstream_ prefix but must STILL be stripped before a MySQL session, for
// the same Bug-126 reason: the go-sql-driver emits each cfg.Params entry as
// `SET <key>=<value>` at session init, and these are not valid MySQL system
// variables. copy_table_parallelism (ADR-0101) is the native-binlog
// concurrent-cold-copy reader count; it governs sluice's snapshot opener,
// never a MySQL session. Listed explicitly (an allowlist, not a prefix) so a
// real future MySQL variable starting with "copy_" is never accidentally
// swallowed.
var nativeSluiceParams = map[string]struct{}{
	"copy_table_parallelism": {},
}

// stripVStreamParams returns a clone of cfg with every cfg.Params entry
// whose key carries the vstream_ prefix removed (plus the explicit
// nativeSluiceParams). It never mutates the caller's cfg (it Clone()s
// first), so a caller may continue to read the original cfg.Params after the
// call.
//
// Bug 126. sluice's vstream_* DSN extensions are consumed only by the
// VStream CDC reader (cdc_vstream.go), which reads them out of cfg.Params
// at openVStreamReader time and then dials vtgate over gRPC — it never
// hands these params to a MySQL connection. Every *other* path
// (schema-reader, row-reader, schema-writer, row-writer, change-applier,
// migration-state-store, and the CDC reader's own shard-discovery
// connection) opens a database/sql handle through [openDB]; the
// go-sql-driver's session init emits each cfg.Params entry as a
// `SET <key> = <value>` after the handshake. Self-hosted Vitess /
// vttestserver rejects the unknown vstream_* vars (Error 1105 for the
// IP-bearing vstream_endpoint, VT05006 unknown system variable for the
// rest), killing a planetscale-flavored cold-start at "open source
// schema reader" before any data moves. Stripping at the openDB choke
// point makes the leak impossible for any present or future Open* path,
// while leaving the CDC reader's earlier cfg.Params reads intact (it has
// already extracted them before the gRPC dial; it never reaches openDB
// except via discoverShards, which is a MySQL connection and correctly
// wants them stripped).
func stripVStreamParams(cfg *mysql.Config) *mysql.Config {
	if cfg == nil {
		return nil
	}
	clone := cfg.Clone()
	for k := range clone.Params {
		if strings.HasPrefix(k, vstreamParamPrefix) {
			delete(clone.Params, k)
			continue
		}
		if _, ok := nativeSluiceParams[k]; ok {
			delete(clone.Params, k)
		}
	}
	return clone
}

// openDB connects to MySQL and verifies the connection is usable.
// It returns a *sql.DB ready for queries; callers are responsible for
// calling Close() when finished.
//
// sluice's vstream_* DSN extensions are stripped here (see
// [stripVStreamParams]) so they never reach a MySQL session as a
// `SET vstream_* = …` statement — Bug 126. This is the single choke
// point every MySQL connection passes through, so the strip is
// leak-proof against future Open* paths.
func openDB(ctx context.Context, cfg *mysql.Config) (*sql.DB, error) {
	cfg = stripVStreamParams(cfg)
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("mysql: build connector: %w", err)
	}
	db := sql.OpenDB(connector)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql: ping: %w", err)
	}
	return db, nil
}
