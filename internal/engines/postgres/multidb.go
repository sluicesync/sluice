// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// Multi-schema fan-out (ADR-0075). The Postgres engine is the symmetric
// reverse of MySQL's multi-database fan-out (ADR-0074): a MySQL *server*
// carries N databases; a Postgres *database* carries N schemas. The
// engine-neutral orchestrator drives both through the same
// [ir.DatabaseLister] / [ir.DatabaseDSNDeriver] / [ir.MultiDatabaseScoper]
// interfaces — "database" in the interface name is the engine-neutral
// term for "namespace to fan out", which for Postgres is a SCHEMA.
//
// This file implements the read-side enumeration + per-schema DSN
// derivation. The schema reader's [ir.MultiDatabaseScoper] lives in
// schema_reader.go (it already stamps Table.Schema; the scoper only adds
// the FK carve-out predicate).

// systemSchemas is the closed set of Postgres server-internal schemas
// that are NEVER user data and are always excluded from a multi-schema
// fan-out (ADR-0075), even under `--all-schemas`. pg_temp* /
// pg_toast_temp* session-temp namespaces are matched by prefix in
// [isSystemSchema] rather than enumerated here (their numeric suffix is
// per-backend).
var systemSchemas = map[string]struct{}{
	"pg_catalog":         {},
	"information_schema": {},
	"pg_toast":           {},
}

// isSystemSchema reports whether name is a Postgres-internal namespace
// that must never be migrated as a user schema. The exact-match set
// (pg_catalog, information_schema, pg_toast) is joined by a prefix match
// on the per-backend session-temp namespaces (pg_temp_NNN /
// pg_toast_temp_NNN). The prefix match is deliberately scoped to the
// `pg_temp` / `pg_toast_temp` prefixes ONLY — a user schema named
// `information_schema_data` or `pg_catalogue` (a lookalike, not a real
// system schema) is NOT excluded, mirroring ADR-0075's non-system-
// lookalike battery: a false exclusion silently drops a user schema,
// the inverse of the MySQL `_vt_*` over-broad-match lesson.
func isSystemSchema(name string) bool {
	if _, ok := systemSchemas[name]; ok {
		return true
	}
	// Session-temp namespaces: pg_temp_<backendid> and the matching
	// pg_toast_temp_<backendid>. Exact-prefix match on the underscore-
	// delimited form so `pg_temporary` (a hypothetical user schema)
	// does not get swept up.
	return strings.HasPrefix(name, "pg_temp_") || strings.HasPrefix(name, "pg_toast_temp_")
}

// ListDatabases implements [ir.DatabaseLister]: it enumerates every
// non-system SCHEMA in the database the dsn connects to. Used by the
// multi-schema migrate orchestrator to resolve `--all-schemas` /
// `--include-schema` / `--exclude-schema` globs into a concrete schema
// set (ADR-0075). "Database" in the interface name is the engine-neutral
// term for "namespace to fan out"; for Postgres a namespace is a schema.
//
// The dsn names the containing database (a PG "database" is a connection
// boundary, not a same-server namespace sluice fans out across — a PG
// logical slot cannot span databases). The system namespaces
// (pg_catalog, information_schema, pg_toast, pg_temp*, pg_toast_temp*)
// are filtered out unconditionally per the ADR.
func (Engine) ListDatabases(ctx context.Context, dsn string) ([]string, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	// information_schema.schemata lists every schema visible to the
	// connecting role. We filter the system set in Go (rather than a
	// WHERE clause) so the exclusion logic lives in one place
	// ([isSystemSchema]) and is unit-testable without a live database.
	const q = `SELECT schema_name FROM information_schema.schemata ORDER BY schema_name`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("postgres: list schemas: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("postgres: list schemas: scan: %w", err)
		}
		if isSystemSchema(name) {
			continue
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list schemas: %w", err)
	}
	return out, nil
}

// WithDatabase implements [ir.DatabaseDSNDeriver]: it returns a clone of
// dsn bound to schema by setting the sluice-custom `schema` query
// parameter (the same parameter [parseDSN] reads to pick the reader /
// writer's namespace). Used by the multi-schema fan-out orchestrator
// (ADR-0075) to re-open a single-schema reader/writer per selected
// schema. Every other DSN element (credentials, host, database, params)
// is left untouched.
//
// Both DSN forms parseDSN accepts are handled: the URI form rewrites the
// `schema` query parameter; the libpq KV form replaces (or appends) the
// `schema=` token.
func (Engine) WithDatabase(dsn, schema string) (string, error) {
	if schema == "" {
		return "", errors.New("postgres: WithDatabase: schema name is empty")
	}
	if dsn == "" {
		return "", errors.New("postgres: WithDatabase: DSN is empty")
	}
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		return withSchemaURI(dsn, schema)
	}
	return withSchemaKV(dsn, schema), nil
}

func withSchemaURI(dsn, schema string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("postgres: WithDatabase: invalid DSN URI: %w", err)
	}
	q := u.Query()
	q.Set("schema", schema)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func withSchemaKV(dsn, schema string) string {
	keepers := make([]string, 0, len(strings.Fields(dsn))+1)
	for _, tok := range strings.Fields(dsn) {
		k, _, ok := strings.Cut(tok, "=")
		if ok && strings.EqualFold(k, "schema") {
			continue // drop the existing schema= token; we re-append below
		}
		keepers = append(keepers, tok)
	}
	keepers = append(keepers, "schema="+schema)
	return strings.Join(keepers, " ")
}

// EnsureDatabase implements [ir.DatabaseDSNDeriver]: it issues
// `CREATE SCHEMA IF NOT EXISTS` for schema against the database the dsn
// connects to (ADR-0075, PG → PG auto-create-target-schema). Idempotent.
//
// The schema identifier is double-quoted via [quoteIdent] so a name with
// reserved-word or special-character shape is safe; embedded quotes are
// doubled by quoteIdent.
func (Engine) EnsureDatabase(ctx context.Context, dsn, schema string) error {
	if schema == "" {
		return errors.New("postgres: EnsureDatabase: schema name is empty")
	}
	cfg, err := parseDSN(dsn)
	if err != nil {
		return err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	stmt := "CREATE SCHEMA IF NOT EXISTS " + quoteIdent(schema)
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("postgres: create schema %q: %w", schema, err)
	}
	return nil
}

// compile-time assertions that the engine satisfies the Phase-2a
// fan-out interfaces (ADR-0075).
var (
	_ ir.DatabaseLister     = Engine{}
	_ ir.DatabaseDSNDeriver = Engine{}
)
