// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

var _ ir.TableIdentityReader = Engine{}

// TableIdentities implements [ir.TableIdentityReader]: each named table's
// pg_class.oid in the DSN's schema. ALTER TABLE … RENAME keeps the oid; a
// table dropped and re-created gets a new one. (An oid is reused only after
// the 32-bit oid counter wraps, so a long-dropped table's oid landing on a
// new table under the old record's lookup is possible in principle and is
// the residual of this identity.) Read-only.
func (e Engine) TableIdentities(ctx context.Context, dsn string, tables []string) (map[string]string, error) {
	out := map[string]string{}
	if len(tables) == 0 {
		return out, nil
	}
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	want := make(map[string]bool, len(tables))
	for _, t := range tables {
		want[t] = true
	}
	rows, err := db.QueryContext(ctx,
		"SELECT c.relname, c.oid::text FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "+
			"WHERE n.nspname = $1 AND c.relkind IN ('r', 'p')", cfg.schema)
	if err != nil {
		return nil, fmt.Errorf("postgres: read table identities: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name, oid string
		if err := rows.Scan(&name, &oid); err != nil {
			return nil, fmt.Errorf("postgres: read table identities: %w", err)
		}
		if want[name] {
			out[name] = oid
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read table identities: %w", err)
	}
	return out, nil
}

// TableIdentitySurvivesRebuild implements [ir.TableIdentityReader]: a table
// rewrite (VACUUM FULL, CLUSTER, TRUNCATE, a rewriting ALTER) changes
// relfilenode, never pg_class.oid — measured by
// TestTableIdentities_SurviveRename — so an oid that changed under the same
// name is a different relation.
func (Engine) TableIdentitySurvivesRebuild() bool { return true }
