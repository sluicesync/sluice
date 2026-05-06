package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

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
		return nil, fmt.Errorf("mysql: invalid DSN: %w", err)
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
