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
// AWS RDS and Aurora Postgres grant slot capability through a THIRD
// mechanism the attribute check can't see: membership in the
// `rds_replication` role (the master user has it at creation;
// `rolsuper` and `rolreplication` are both false BY DESIGN — RDS
// patches the permission check server-side). Live-proven on RDS PG 16
// (2026-07-16 validation): the master role created and dropped pgoutput
// slots with `rolreplication=f`. The probe therefore also accepts
// `pg_has_role(current_user, 'rds_replication', 'MEMBER')`, guarded on
// the role existing so stock PG (where it doesn't) is unaffected.
//
// Why a catalog probe and not an attempt-based one (create + drop a
// throwaway slot — the only check that can't drift from a provider's
// patched permission model): slot creation is not transactional, so a
// crash between create and drop leaks a WAL-retaining slot — the exact
// hazard the ADR-0059 slot-health probe exists to police; it consumes a
// slot from `max_replication_slots`; and it fails for REASONS OTHER
// than role capability (wal_level != logical, slot exhaustion), each of
// which has its own dedicated loud refusal that an attempt probe would
// mis-attribute as role incapability. A provider with yet another
// unrecognized grant model falls through to the raw mid-cold-start
// 42501 — loud, never silent.
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
// role can create a logical replication slot — i.e. it is a superuser,
// carries the REPLICATION attribute (`pg_roles.rolsuper OR
// rolreplication`), or is a member of the managed-provider grant role
// `rds_replication` (AWS RDS / Aurora — see the package doc). The role
// name is returned so the orchestrator can name it in the refusal
// message (operators need to know which role to grant REPLICATION to,
// or which role to swap for).
//
// Implements the orchestrator's `replicationCapabilityProber` surface
// (task #61). `pg_roles` is queryable by any role, so this probe doesn't
// itself need extra privileges.
func (r *SchemaReader) SourceReplicationCapability(ctx context.Context) (canReplicate bool, role string, err error) {
	return probeSourceReplicationCapability(ctx, r.db)
}

// probeSourceReplicationCapability reads the connecting role's
// slot-creation capability from the catalog: `rolsuper OR
// rolreplication` (stock PG), or membership in `rds_replication` (AWS
// RDS / Aurora — see the package doc). Returns the (canReplicate, role)
// pair plus any error.
//
// The membership arm is a CASE, not a bare `AND`: SQL doesn't guarantee
// short-circuit evaluation, and `pg_has_role` ERRORs on a role name that
// doesn't exist — a CASE's WHEN is the one construct PG guarantees is
// evaluated before its THEN, so stock PG (no `rds_replication` row)
// never reaches the pg_has_role call. `pg_has_role(..., 'MEMBER')`
// covers indirect membership too, matching how RDS grants it.
//
// `pg_roles` is the public-readable view over `pg_authid`'s identity
// columns (only the password hash is redacted, which we don't read), so
// the probe works for the unprivileged sluice role the preflight is
// designed to catch.
func probeSourceReplicationCapability(ctx context.Context, db *sql.DB) (canReplicate bool, role string, err error) {
	const q = `
SELECT (
    rolsuper
    OR rolreplication
    OR CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_replication')
            THEN pg_has_role(current_user, 'rds_replication', 'MEMBER')
            ELSE false
       END
), rolname
  FROM pg_roles
 WHERE rolname = current_user`
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
