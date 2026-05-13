// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

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
