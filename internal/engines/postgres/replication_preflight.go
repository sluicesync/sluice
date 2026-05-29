// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Postgres-side implementation of the orchestrator's replication-
// capability preflight prober surface (task #61; see
// `internal/pipeline/replication_preflight.go` for the operator-facing
// rationale).
//
// The slot-based `postgres` CDC engine needs the connecting role to be
// a superuser OR carry the REPLICATION attribute to create a logical
// replication slot. Managed-PG tiers that forbid REPLICATION (Heroku
// Postgres Essential, Render Basic, Supabase free) otherwise fail
// mid-cold-start with a raw wrapped permission error from slot creation
// (`ERROR: permission denied ...`, SQLSTATE 42501) — opaque and not
// actionable. The orchestrator preflight catches the missing capability
// UPFRONT and points the operator at `--source-driver=postgres-trigger`,
// the slot-less trigger-capture engine built for exactly this tier.
//
// Implemented on [SchemaReader] only: the capability gates SOURCE-side
// slot creation, and the schema reader is the source-side handle that's
// live when the orchestrator probes before cold-start. (RLS, by
// contrast, needs both source and target probes — hence its surface
// also rides on RowWriter.)
//
// The probe mirrors `probeCurrentRoleBypassesRLS` exactly: it reads
// `pg_roles` filtered by `current_user`, which is world-readable, so the
// probe itself needs no extra privilege — it works for the unprivileged
// managed-PG role the preflight is designed to catch.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// SourceReplicationCapability reports whether the source-side connecting
// role can create a logical replication slot — i.e. it is a superuser OR
// carries the REPLICATION attribute (`pg_roles.rolsuper OR
// rolreplication`). The role name is returned so the orchestrator can
// name it in the refusal message (operators need to know which role to
// grant REPLICATION to, or which role to swap for).
//
// Implements the orchestrator's `replicationCapabilityProber` surface
// (task #61). `pg_roles` is queryable by any role, so this probe doesn't
// itself need extra privileges.
func (r *SchemaReader) SourceReplicationCapability(ctx context.Context) (canReplicate bool, role string, err error) {
	return probeSourceReplicationCapability(ctx, r.db)
}

// probeSourceReplicationCapability issues `SELECT (rolsuper OR
// rolreplication), rolname FROM pg_roles WHERE rolname = current_user`.
// Returns the (canReplicate, role) pair plus any error.
//
// `pg_roles` is the public-readable view over `pg_authid`'s identity
// columns (only the password hash is redacted, which we don't read), so
// the probe works for the unprivileged sluice role the preflight is
// designed to catch.
func probeSourceReplicationCapability(ctx context.Context, db *sql.DB) (canReplicate bool, role string, err error) {
	const q = `SELECT (rolsuper OR rolreplication), rolname FROM pg_roles WHERE rolname = current_user`
	switch err := db.QueryRowContext(ctx, q).Scan(&canReplicate, &role); {
	case errors.Is(err, sql.ErrNoRows):
		// `current_user` always has a row in pg_roles by construction;
		// no-rows means a catalog mismatch worth surfacing rather than
		// silently treating as "cannot replicate" (which would refuse a
		// valid role) or "can replicate" (which would defer to the raw
		// mid-cold-start permission error the preflight exists to avoid).
		return false, "", errors.New("postgres: probe REPLICATION capability: pg_roles row for current_user not found")
	case err != nil:
		return false, "", fmt.Errorf("postgres: probe REPLICATION capability: %w", err)
	}
	return canReplicate, role, nil
}
