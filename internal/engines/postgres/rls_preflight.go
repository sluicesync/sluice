// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Postgres-side implementation of the orchestrator's RLS-preflight
// prober surface (task #52 sub-deliverable 1; see
// `internal/pipeline/rls_preflight.go` for the operator-facing
// rationale).
//
// Two surfaces implement the probe:
//
//   - [SchemaReader] for the source-side check, since the schema
//     reader is the handle that's live when the orchestrator probes
//     source-side state before snapshot-open / cold-start.
//   - [RowWriter] for the target-side check, since the row writer is
//     the handle the existing cold-start preflight uses and the
//     orchestrator already keeps it open across bulk-copy / CDC
//     engagement.
//
// Both surfaces issue identical SQL — the catalog lookups are role/
// schema-scoped, not reader/writer-scoped — so the methods share a
// pair of free-standing helpers (`probeTableRLSStatus`,
// `probeCurrentRoleBypassesRLS`). Keeping them as package-private
// helpers (rather than methods on a shared struct) avoids reshuffling
// the existing engine surfaces just for this preflight.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// TableRLSStatus reports whether RLS is enabled for the source-side
// table, and whether FORCE ROW LEVEL SECURITY is set. Implements the
// orchestrator's `rlsPreflightProber` surface for the source side
// (task #52 sub-deliverable 1).
//
// `enabled` corresponds to `pg_class.relrowsecurity`; `forced` to
// `relforcerowsecurity`. The two are orthogonal: a table can be
// ENABLE'd without FORCE (the owner bypasses) or both ENABLE'd and
// FORCE'd (no one bypasses without role-level BYPASSRLS).
//
// A missing table returns (false, false, nil) — there's nothing to
// refuse against; downstream phases will surface the missing-table
// error with a more useful diagnostic than the RLS preflight could.
func (r *SchemaReader) TableRLSStatus(ctx context.Context, table *ir.Table) (enabled, forced bool, err error) {
	if table == nil {
		return false, false, errors.New("postgres: TableRLSStatus: table is nil")
	}
	return probeTableRLSStatus(ctx, r.db, r.schema, table.Name)
}

// CurrentRoleBypassesRLS reports whether the source-side connecting
// role has the BYPASSRLS attribute. The role name is returned so the
// orchestrator can surface it in the refusal message (operators need
// to know which role to grant BYPASSRLS to).
func (r *SchemaReader) CurrentRoleBypassesRLS(ctx context.Context) (bypass bool, role string, err error) {
	return probeCurrentRoleBypassesRLS(ctx, r.db)
}

// TableRLSStatus reports target-side RLS state. Same shape as the
// source-side method on [SchemaReader]; the catalog query is identical
// because `pg_class` scopes by relation name + namespace regardless
// of read/write direction.
func (w *RowWriter) TableRLSStatus(ctx context.Context, table *ir.Table) (enabled, forced bool, err error) {
	if table == nil {
		return false, false, errors.New("postgres: TableRLSStatus: table is nil")
	}
	return probeTableRLSStatus(ctx, w.db, w.schema, table.Name)
}

// CurrentRoleBypassesRLS reports target-side BYPASSRLS attribute for
// the connecting role.
func (w *RowWriter) CurrentRoleBypassesRLS(ctx context.Context) (bypass bool, role string, err error) {
	return probeCurrentRoleBypassesRLS(ctx, w.db)
}

// probeTableRLSStatus issues the catalog lookup that powers both
// surfaces' `TableRLSStatus`. Scoped via `pg_class.relname` +
// `pg_namespace.nspname` to the named schema so a same-named table in
// another namespace doesn't shadow the result.
//
// Returns (false, false, nil) for a missing table — downstream phases
// produce a better error than the preflight could.
func probeTableRLSStatus(ctx context.Context, db *sql.DB, schemaName, tableName string) (enabled, forced bool, err error) {
	const q = `
		SELECT cl.relrowsecurity, cl.relforcerowsecurity
		FROM   pg_class     cl
		JOIN   pg_namespace n ON n.oid = cl.relnamespace
		WHERE  cl.relname = $1
		  AND  n.nspname  = $2
		  AND  cl.relkind IN ('r', 'p')`
	switch err := db.QueryRowContext(ctx, q, tableName, schemaName).Scan(&enabled, &forced); {
	case errors.Is(err, sql.ErrNoRows):
		// Missing table — let the downstream phases surface it.
		return false, false, nil
	case err != nil:
		return false, false, fmt.Errorf("postgres: probe RLS state for %q.%q: %w", schemaName, tableName, err)
	}
	return enabled, forced, nil
}

// probeCurrentRoleBypassesRLS issues `SELECT rolbypassrls, rolname
// FROM pg_roles WHERE rolname = current_user`. Returns the
// (bypass, role) pair plus any error.
//
// `pg_roles` is queryable by any role (it's the public-readable view
// over `pg_authid`'s identity columns; the password hash column is
// the only redacted field, which we don't read). So this probe doesn't
// itself need extra privileges — it works for the unprivileged sluice
// role that the preflight is designed to catch.
func probeCurrentRoleBypassesRLS(ctx context.Context, db *sql.DB) (bypass bool, role string, err error) {
	const q = `SELECT rolbypassrls, rolname FROM pg_roles WHERE rolname = current_user`
	switch err := db.QueryRowContext(ctx, q).Scan(&bypass, &role); {
	case errors.Is(err, sql.ErrNoRows):
		// `current_user` always has a row in pg_roles by construction;
		// no-rows means a catalog mismatch worth surfacing rather than
		// silently treating as "no bypass" (which would be the
		// conservative-but-misleading default).
		return false, "", errors.New("postgres: probe BYPASSRLS: pg_roles row for current_user not found")
	case err != nil:
		return false, "", fmt.Errorf("postgres: probe BYPASSRLS: %w", err)
	}
	return bypass, role, nil
}
