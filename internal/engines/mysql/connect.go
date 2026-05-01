package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

// parseDSN parses and validates a MySQL DSN, applying the parameter
// adjustments sluice requires for correct behaviour:
//
//   - parseTime=true: drive returns time.Time for DATE/DATETIME/TIMESTAMP
//     instead of []byte, which lets the row pipeline use Go-native types.
//   - loc=UTC: timestamps are returned in UTC regardless of session
//     timezone, removing one source of cross-engine ambiguity.
//
// The DSN must include a database name; sluice operates against an
// explicit schema rather than connecting at the server level.
func parseDSN(dsn string) (*mysql.Config, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql: invalid DSN: %w", err)
	}
	if cfg.DBName == "" {
		return nil, fmt.Errorf("mysql: DSN must include a database name")
	}

	cfg.ParseTime = true
	cfg.Loc = time.UTC

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
