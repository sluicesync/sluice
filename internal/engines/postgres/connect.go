package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	// stdlib registers pgx as a database/sql driver under the name "pgx".
	_ "github.com/jackc/pgx/v5/stdlib"
)

// pgConfig captures the bits of the DSN sluice cares about beyond what
// the driver itself parses.
type pgConfig struct {
	dsn    string // the DSN passed through to the driver, with `schema` stripped
	schema string // the Postgres schema (namespace) to operate on
}

// parseDSN extracts the schema name from a Postgres DSN and returns a
// config the rest of the engine can use. The schema is taken from the
// `schema` query parameter; if absent, it defaults to "public".
//
// Both DSN forms are accepted:
//
//   - URI: postgres://user:pass@host:port/dbname?sslmode=disable&schema=public
//   - libpq KV: host=localhost user=postgres dbname=postgres sslmode=disable schema=public
//
// In both cases the schema parameter is custom to sluice — Postgres
// itself doesn't honour it. We strip it from the DSN before passing
// the remainder to the driver, so pgx doesn't reject it as unknown.
func parseDSN(dsn string) (*pgConfig, error) {
	if dsn == "" {
		return nil, errors.New("postgres: DSN is empty")
	}

	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		return parseURIDSN(dsn)
	}
	return parseKVDSN(dsn)
}

func parseURIDSN(dsn string) (*pgConfig, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: invalid DSN URI: %w", err)
	}
	if strings.TrimPrefix(u.Path, "/") == "" {
		return nil, errors.New("postgres: DSN must include a database name")
	}

	q := u.Query()
	schema := q.Get("schema")
	if schema == "" {
		schema = "public"
	}
	q.Del("schema")
	u.RawQuery = q.Encode()

	return &pgConfig{dsn: u.String(), schema: schema}, nil
}

func parseKVDSN(dsn string) (*pgConfig, error) {
	// libpq-style KV pairs separated by whitespace. We do a simple
	// tokenize; quoted values with embedded spaces are not supported in
	// this first cut (pgx does support them, so connections still work
	// — we just don't pull `schema` out of quoted values).
	schema := ""
	keepers := []string{}
	for _, tok := range strings.Fields(dsn) {
		k, v, ok := strings.Cut(tok, "=")
		if !ok {
			keepers = append(keepers, tok)
			continue
		}
		if strings.EqualFold(k, "schema") {
			schema = v
			continue // strip
		}
		keepers = append(keepers, tok)
	}
	if schema == "" {
		schema = "public"
	}
	return &pgConfig{dsn: strings.Join(keepers, " "), schema: schema}, nil
}

// openDB opens a *sql.DB against the Postgres server and pings to
// confirm the connection is usable. The pgx driver registered itself
// under the name "pgx" via the blank import at the top of this file.
func openDB(ctx context.Context, cfg *pgConfig) (*sql.DB, error) {
	db, err := sql.Open("pgx", cfg.dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return db, nil
}
