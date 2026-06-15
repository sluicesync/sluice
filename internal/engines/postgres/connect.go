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

	"sluicesync.dev/sluice/internal/netkeepalive"
)

// pgConfig captures the bits of the DSN sluice cares about beyond what
// the driver itself parses.
type pgConfig struct {
	dsn    string // the DSN passed through to the driver, with `schema` stripped
	schema string // the Postgres schema (namespace) to operate on
	appID  string // stream-/migration-id for the application_name label; "" → "-" fallback
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
//
// It is an Engine method so the engine's connection-label id (see
// [Engine.WithConnectionLabel]) rides along on the returned config —
// every connection the engine opens flows through a pgConfig, which is
// how the id reaches [withApplicationName] without any package-global
// state. The zero-value Engine yields an empty appID, which the label
// choke point normalises to the "-" fallback.
func (e Engine) parseDSN(dsn string) (*pgConfig, error) {
	if dsn == "" {
		return nil, errors.New("postgres: DSN is empty")
	}

	var (
		cfg *pgConfig
		err error
	)
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		cfg, err = parseURIDSN(dsn)
	} else {
		cfg, err = parseKVDSN(dsn)
	}
	if err != nil {
		return nil, err
	}
	cfg.appID = e.appID
	return cfg, nil
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
// [Engine.parseDSN].
//
// Connections opened here are labelled with the [roleControl]
// application_name carrying the given stream-/migration-id (empty →
// the "-" fallback); the postgres engine's own pools call [openDBAs]
// with a more specific role.
func OpenPgxDB(dsn, label string) (*sql.DB, error) {
	return openPgxDBAs(dsn, roleControl, label)
}

// openPgxDBAs is the role-aware variant behind [OpenPgxDB]. It stamps
// the application_name for role and id (unless the operator already
// set one) before handing the DSN to pgx.
func openPgxDBAs(dsn string, role connRole, appID string) (*sql.DB, error) {
	connConfig, err := pgx.ParseConfig(withApplicationName(dsn, role, appID))
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn: %w", err)
	}
	connConfig.DialFunc = netkeepalive.Dialer().DialContext
	return stdlib.OpenDB(*connConfig), nil
}

// openPgxDBDescribeExec opens a lazy *sql.DB whose backends default to
// pgx's [pgx.QueryExecModeDescribeExec] instead of the usual
// prepared-statement cache. The CDC pipelined-apply path (ADR-0092) uses
// this pool because its SendBatch flush, under this mode, describes each
// DISTINCT queued statement FRESH via an unnamed prepare (pgx passes a nil
// statement-description cache, so there is no client-side cache and never a
// stale pre-DDL OID), then binds + executes every statement with the real
// described parameter OID in BINARY format — byte-IDENTICAL value encoding
// to the serial CacheStatement path the applier's primary pool uses. That
// is what makes the "pipelining changes only WHEN statements are sent,
// never HOW a value is encoded" invariant literally true (an Exec-mode
// pool would instead send OID-0 TEXT, a different wire encoding), and it
// subsumes the ADR-0091 GAP #3 stale-OID hazard via the live re-describe (a
// widened column is re-described, never bound against a cached pre-DDL OID)
// without any per-statement special-casing. The cost is ~2 round trips per
// batch (one describe/prepare flush for the distinct statement templates,
// one execute flush for all N) — still O(1) in N, so the throughput win
// over N+2 serial round trips is preserved. Same role/label/keep-alive
// shape as [openPgxDBAs]; only the exec mode differs, and only for this
// dedicated pool — the applier's primary pool (per-change Apply path) keeps
// the cached fast path.
func openPgxDBDescribeExec(dsn string, role connRole, appID string) (*sql.DB, error) {
	connConfig, err := pgx.ParseConfig(withApplicationName(dsn, role, appID))
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn: %w", err)
	}
	connConfig.DialFunc = netkeepalive.Dialer().DialContext
	connConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
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
// application_name for role and the config's stream-/migration-id on
// the connection (see [withApplicationName]) so the engine's snapshot /
// applier / cdc-reader / schema pools are distinguishable in
// pg_stat_activity.
func openDBAs(ctx context.Context, cfg *pgConfig, role connRole) (*sql.DB, error) {
	db, err := openPgxDBAs(cfg.dsn, role, cfg.appID)
	if err != nil {
		return nil, fmt.Errorf("postgres: open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return db, nil
}
