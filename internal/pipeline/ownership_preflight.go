// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"
	"strings"
)

// currentRoleReporter is the optional surface a target engine handle
// exposes to report the role its connection authenticates as. Only the
// Postgres RowWriter implements it today (`SELECT current_user`); any
// other target opts out by not having the method.
type currentRoleReporter interface {
	CurrentRole(ctx context.Context) (string, error)
}

// planetScaleEphemeralRolePrefix is the prefix PlanetScale Postgres gives
// its per-connection "user-defined roles". A sluice target authenticating
// as such a role OWNS every table it creates, and that role is ephemeral
// (deleting it, or running later DDL as a *different* pscale_api_* role,
// breaks ownership even though both inherit `postgres`). The prefix is
// PlanetScale-specific enough to be a reliable signal on its own.
const planetScaleEphemeralRolePrefix = "pscale_api_"

// isPlanetScaleEphemeralRole reports whether a role name is one of
// PlanetScale Postgres's ephemeral per-token roles (soak finding F10).
func isPlanetScaleEphemeralRole(role string) bool {
	return strings.HasPrefix(role, planetScaleEphemeralRolePrefix)
}

// preflightTargetOwnershipAdvisory warns — never refuses — when the PG
// target connection authenticates as a PlanetScale ephemeral
// `pscale_api_*` role. Every table sluice creates would be owned by that
// role; if it is later deleted (or a different pscale_api_* role runs DDL
// against the tables) the operator hits ownership/permission errors.
//
// Advisory-only by design (soak finding F10): the "contain Postgres
// complexity — surface, don't silently auto-handle" tenet rules out
// auto-reassigning ownership (a privileged action), and the pitfall is
// recoverable after the fact (`pscale role reset-default` /
// `pscale role reassign` / the PlanetScale UI "Reassign objects"). So we
// name the hazard and the fix and let the operator decide. A probe
// failure is swallowed at debug — this must never block a migrate/sync.
//
// No-op when the handle isn't a Postgres target (structural opt-out).
func preflightTargetOwnershipAdvisory(ctx context.Context, handle any) {
	reporter, ok := handle.(currentRoleReporter)
	if !ok {
		return // non-PG target, or a PG surface without the probe
	}
	role, err := reporter.CurrentRole(ctx)
	if err != nil {
		slog.DebugContext(ctx, "pipeline: ownership advisory: current-role probe failed (skipping)",
			slog.String("err", err.Error()))
		return
	}
	if !isPlanetScaleEphemeralRole(role) {
		return
	}
	slog.WarnContext(
		ctx,
		"target tables will be OWNED by an ephemeral PlanetScale role — reassign to a stable role to avoid the pitfall below",
		slog.String("target_role", role),
		slog.String("pitfall", "every table sluice creates is owned by this pscale_api_* role; if it is later deleted, or a different pscale_api_* role runs DDL against these tables, ownership/permission errors follow (the new role is not the owner even though both inherit postgres)"),
		slog.String("recommended", "connect the sluice target as the Default 'postgres' role (PlanetScale: `pscale role reset-default`) so created tables are owned by a stable role"),
		slog.String("recovery", "already ran as pscale_api_*? reassign via `pscale role reassign`, or the PlanetScale UI Settings > Roles > 'Reassign objects'"),
	)
}
