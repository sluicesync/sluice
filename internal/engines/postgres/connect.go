// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/orware/sluice/internal/netkeepalive"
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

// OpenPgxDB opens a lazy *sql.DB against the Postgres server named by
// dsn, with sluice's standard TCP keep-alive policy installed on the
// dial path (see [netkeepalive]). It does not ping — like [sql.Open],
// the first real connection is established on first use; callers that
// need an eager liveness check should call PingContext themselves.
//
// This is the single funnel for every pgx-backed pool in sluice
// (the postgres engine's own pools and the postgres-trigger poller),
// so the keep-alive policy is applied uniformly. The DSN must already
// have any sluice-custom parameters (such as `schema`) stripped — see
// [parseDSN].
//
// Connections opened here are labelled with the [roleControl]
// application_name; the postgres engine's own pools call [openDBAs]
// with a more specific role.
func OpenPgxDB(dsn string) (*sql.DB, error) {
	return openPgxDBAs(dsn, roleControl)
}

// openPgxDBAs is the role-aware variant behind [OpenPgxDB]. It stamps
// the application_name for role (unless the operator already set one)
// before handing the DSN to pgx.
func openPgxDBAs(dsn string, role connRole) (*sql.DB, error) {
	connConfig, err := pgx.ParseConfig(withApplicationName(dsn, role))
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn: %w", err)
	}
	connConfig.DialFunc = netkeepalive.Dialer().DialContext
	return stdlib.OpenDB(*connConfig), nil
}

// openDB opens a *sql.DB against the Postgres server and pings to
// confirm the connection is usable. The connection is labelled with the
// [roleControl] application_name; callers that know their subsystem use
// [openDBAs] to label more precisely.
func openDB(ctx context.Context, cfg *pgConfig) (*sql.DB, error) {
	return openDBAs(ctx, cfg, roleControl)
}

// openDBAs is the role-aware variant of [openDB]: it stamps the
// application_name for role on the connection (see [withApplicationName])
// so the engine's snapshot / applier / cdc-reader / schema pools are
// distinguishable in pg_stat_activity.
func openDBAs(ctx context.Context, cfg *pgConfig, role connRole) (*sql.DB, error) {
	db, err := openPgxDBAs(cfg.dsn, role)
	if err != nil {
		return nil, fmt.Errorf("postgres: open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return db, nil
}
