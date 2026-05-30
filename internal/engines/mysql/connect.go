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

	"github.com/orware/sluice/internal/netkeepalive"
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
	// Forcing the strict modes here on every sluice MySQL connection
	// turns the silent-loss class into loud MySQL errors
	// (1264 / 1265 / 1292) which then surface through the existing
	// applier error path. An operator who explicitly wants the
	// relaxed-mode behaviour can override by setting sql_mode in the
	// DSN params; the literal-quotes pattern matches the time_zone
	// override above.
	if _, ok := cfg.Params["sql_mode"]; !ok {
		cfg.Params["sql_mode"] = "'STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'"
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

	return cfg, nil
}

// openDB connects to MySQL and verifies the connection is usable.
// It returns a *sql.DB ready for queries; callers are responsible for
// calling Close() when finished.
func openDB(ctx context.Context, cfg *mysql.Config) (*sql.DB, error) {
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
