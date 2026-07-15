// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Arming the ADR-0148 deploy-request index-build fallback (roadmap item 67)
//
// The CLI is the composer: the engine-neutral pipeline threads the value
// opaquely and the mysql engine consumes it — neither imports the
// PlanetScale control plane. Arming is deliberately OPPORTUNISTIC, never a
// refusal: the org + token typically arrive from ambient env vars
// (PLANETSCALE_ORG / PLANETSCALE_SERVICE_TOKEN_ID / _TOKEN — the pscale
// CLI convention), so a missing or partial credential set logs a WARN at
// most and leaves the migrate exactly as it was — the fallback only ever
// ADDS a recovery for an index build that would otherwise fail.
//
// Why flags on migrate rather than deriving from the ADR-0107 telemetry
// flags: those flags exist only on `sync start` (migrate has no telemetry
// surface), so there is nothing to derive from here. The database name,
// however, IS derivable from the data-plane --target DSN — the same
// convention --planetscale-metrics-db uses on sync — so only the org (no
// derivable source) plus the token are genuinely new inputs.

package main

import (
	"log/slog"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/planetscale/expandcontract"
)

// planetScaleIndexFallback composes the [expandcontract.IndexFallback]
// for this migrate run, or nil when it should stay unarmed. nil is the
// no-behavior-change path: the deferred index build (and its errno-3024
// --upfront-indexes hint) is byte-identical to before ADR-0148 shipped.
func (m *MigrateCmd) planetScaleIndexFallback() ir.IndexBuildFallback {
	// PlanetScale-flavor targets only: the fallback drives the PlanetScale
	// control plane, which vanilla MySQL / self-hosted Vitess (and every
	// other engine) don't have. Silent on non-planetscale targets because
	// --planetscale-org routinely arrives from the ambient PLANETSCALE_ORG
	// env var — refusing would break unrelated migrates in pscale-shaped
	// shells.
	if m.TargetDriver != "planetscale" || m.PlanetScaleOrg == "" {
		return nil
	}
	if m.PlanetScaleServiceTokenID == "" || m.PlanetScaleServiceToken == "" {
		slog.Warn("--planetscale-org is set but the service token is incomplete; the deploy-request index-build fallback stays OFF (an errno-3024/1105 index failure will surface the --upfront-indexes hint)",
			slog.String("need", "PLANETSCALE_SERVICE_TOKEN_ID and PLANETSCALE_SERVICE_TOKEN (or --planetscale-service-token-id/--planetscale-service-token)"))
		return nil
	}
	database := m.PlanetScaleDatabase
	if database == "" {
		database = mysqlDSNDatabase(m.Target)
	}
	if database == "" {
		slog.Warn("cannot derive the PlanetScale database name from the --target DSN; the deploy-request index-build fallback stays OFF — pass --planetscale-database explicitly")
		return nil
	}
	return &expandcontract.IndexFallback{
		API:           api.New(api.Config{TokenID: m.PlanetScaleServiceTokenID, Token: m.PlanetScaleServiceToken}),
		Org:           m.PlanetScaleOrg,
		Database:      database,
		Branch:        m.PlanetScaleBranch,
		DeployTimeout: m.PlanetScaleDeployTimeout,
	}
}

// mysqlDSNDatabase extracts the database name from a go-sql-driver MySQL
// DSN, or "" when it doesn't parse — the caller then requires the
// explicit flag instead of guessing.
func mysqlDSNDatabase(dsn string) string {
	cfg, err := gomysql.ParseDSN(dsn)
	if err != nil {
		return ""
	}
	return cfg.DBName
}
