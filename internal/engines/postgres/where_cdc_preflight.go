// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// PreflightFilteredCDCBeforeImage implements [ir.FilteredCDCPreflighter]
// for a continuous filtered sync (`sync --where`, ADR-0173 Phase 2). The
// client-side row-move evaluation needs the FULL before-image of every
// UPDATE/DELETE on a filtered table (to decide whether the row moved into
// or out of the filter's scope). Postgres delivers only the primary key in
// the before-image under the default `REPLICA IDENTITY DEFAULT`, so a
// predicate on a non-key column could not be evaluated on the old row —
// which would silently mis-classify a move and leak/drop rows.
//
// It refuses loudly ([sluicecode.CodeWhereCDCBeforeImage]) when any named
// table is not set to `REPLICA IDENTITY FULL`, naming every offending
// table and the exact `ALTER TABLE … REPLICA IDENTITY FULL` remedy.
//
// tables are the SOURCE table names carrying a `--where` predicate,
// resolved against the DSN's schema. An empty list is a no-op.
func (e Engine) PreflightFilteredCDCBeforeImage(ctx context.Context, dsn string, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	var notFull []string
	for _, table := range tables {
		full, err := tableHasReplicaIdentityFull(ctx, db, cfg.schema, table)
		if err != nil {
			return err
		}
		if !full {
			notFull = append(notFull, table)
		}
	}
	if len(notFull) == 0 {
		return nil
	}
	sort.Strings(notFull)
	remedy := make([]string, len(notFull))
	for i, t := range notFull {
		remedy[i] = fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL;", qualifyForRemedy(cfg.schema, t))
	}
	return sluicecode.Wrap(
		sluicecode.CodeWhereCDCBeforeImage,
		"run "+strings.Join(remedy, " ")+" then restart the sync",
		fmt.Errorf(
			"continuous filtered sync: --where is set on table(s) %s, but they are not configured for full row "+
				"before-images (REPLICA IDENTITY is not FULL). The --where row-move evaluation needs the complete "+
				"before-image of each UPDATE/DELETE to decide whether a row moved into or out of the filter's scope; "+
				"under the default REPLICA IDENTITY DEFAULT the before-image carries only the primary key, so a "+
				"predicate on a non-key column cannot be evaluated on the old row and the stream would silently leak "+
				"out-of-scope rows or drop newly-in-scope ones. Set full before-images before starting CDC: %s Then re-run",
			strings.Join(notFull, ", "),
			strings.Join(remedy, " "),
		),
	)
}

// tableHasReplicaIdentityFull reports whether schema.table has
// `REPLICA IDENTITY FULL` (pg_class.relreplident = 'f'). A table that
// cannot be found is treated as not-full so the refusal names it rather
// than silently passing (the operator's --where key did not resolve to a
// real table, which is itself worth surfacing).
func tableHasReplicaIdentityFull(ctx context.Context, db *sql.DB, schema, table string) (bool, error) {
	const q = `
SELECT c.relreplident
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')`
	nspname := schema
	if nspname == "" {
		nspname = "public"
	}
	var relreplident string
	switch err := db.QueryRowContext(ctx, q, nspname, table).Scan(&relreplident); err {
	case nil:
		return relreplident == "f", nil
	case sql.ErrNoRows:
		return false, nil
	default:
		return false, fmt.Errorf("postgres: read REPLICA IDENTITY for %s.%s: %w", nspname, table, err)
	}
}

// qualifyForRemedy builds a schema-qualified table reference for the
// ALTER TABLE remedy text. Identifiers are echoed as the operator gave
// them (the --where key + DSN schema); this string is advisory prose, not
// an executed statement.
func qualifyForRemedy(schema, table string) string {
	if schema == "" || schema == "public" {
		return table
	}
	return schema + "." + table
}

// compile-time assertion that Engine satisfies the preflighter surface.
var _ ir.FilteredCDCPreflighter = Engine{}
