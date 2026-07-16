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
// most and leaves the run exactly as it was — the fallback only ever
// ADDS a recovery for an index build that would otherwise fail.
//
// Why flags on migrate rather than deriving from the ADR-0107 telemetry
// flags: those flags exist only on `sync start` (migrate has no telemetry
// surface), so there is nothing to derive from here. The database name,
// however, IS derivable from the data-plane --target DSN — the same
// convention --planetscale-metrics-db uses on sync — so only the org (no
// derivable source) plus the token are genuinely new inputs.
//
// Audit 2026-07-15 MED-A1: the same walled deferred CreateIndexes also
// runs on `restore` (Phase 4) and the sync cold-start, so those commands
// arm through the shared [composePlanetScaleIndexFallback] too. One
// wrinkle there (absent on migrate): `restore` and `sync start` already
// carry a `--planetscale-org` flag with the ADR-0107/0115 TELEMETRY
// meaning. Reconciliation: ONE flag names the org for BOTH consumers, and
// each consumer arms on its OWN token pair (metrics tokens → telemetry,
// service tokens → this fallback); see [telemetryParamsSharedOrg] for how
// the telemetry all-or-nothing refusal is preserved without refusing a
// fallback-only arming. On those two commands the org flag deliberately
// keeps NO env binding — binding PLANETSCALE_ORG there would make the
// ambient var trip the telemetry refusal in pscale-shaped shells — so
// arming the fallback on restore/sync requires the explicit flag, while
// the service tokens still come from the env.

package main

import (
	"context"
	"log/slog"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/planetscale/expandcontract"
)

// indexFallbackParams is one command's ADR-0148 arming input — the
// planetscale-target check plus the control-plane credential set. Each
// command (migrate, restore, sync start) fills it from its own flags.
type indexFallbackParams struct {
	targetDriver string // registry name of the target engine ("planetscale" arms)
	targetDSN    string // data-plane DSN; the database name is derived from it

	org           string
	database      string // empty = derive from targetDSN
	branch        string
	tokenID       string
	token         string
	deployTimeout time.Duration
}

// composePlanetScaleIndexFallback composes the [expandcontract.IndexFallback]
// for a run, or nil when it should stay unarmed. nil is the
// no-behavior-change path: the deferred index build (and its errno-3024
// --upfront-indexes hint) is byte-identical to before ADR-0148 shipped.
func composePlanetScaleIndexFallback(p indexFallbackParams) ir.IndexBuildFallback {
	// PlanetScale-flavor targets only: the fallback drives the PlanetScale
	// control plane, which vanilla MySQL / self-hosted Vitess (and every
	// other engine) don't have. Silent on non-planetscale targets because
	// --planetscale-org routinely arrives from the ambient PLANETSCALE_ORG
	// env var (migrate) or is shared with the telemetry opt-in
	// (restore/sync) — refusing would break unrelated runs.
	if p.targetDriver != "planetscale" || p.org == "" {
		return nil
	}
	if p.tokenID == "" || p.token == "" {
		slog.Warn("--planetscale-org is set but the service token is incomplete; the deploy-request index-build fallback stays OFF (an errno-3024/1105 index failure will surface the --upfront-indexes hint)",
			slog.String("need", "PLANETSCALE_SERVICE_TOKEN_ID and PLANETSCALE_SERVICE_TOKEN (or --planetscale-service-token-id/--planetscale-service-token)"))
		return nil
	}
	database := p.database
	if database == "" {
		database = mysqlDSNDatabase(p.targetDSN)
	}
	if database == "" {
		slog.Warn("cannot derive the PlanetScale database name from the --target DSN; the deploy-request index-build fallback stays OFF — pass --planetscale-database explicitly")
		return nil
	}
	return &expandcontract.IndexFallback{
		API:           api.New(api.Config{TokenID: p.tokenID, Token: p.token}),
		Org:           p.org,
		Database:      database,
		Branch:        p.branch,
		DeployTimeout: p.deployTimeout,
	}
}

// planetScaleIndexFallback composes migrate's fallback from its flags.
func (m *MigrateCmd) planetScaleIndexFallback() ir.IndexBuildFallback {
	return composePlanetScaleIndexFallback(indexFallbackParams{
		targetDriver:  m.TargetDriver,
		targetDSN:     m.Target,
		org:           m.PlanetScaleOrg,
		database:      m.PlanetScaleDatabase,
		branch:        m.PlanetScaleBranch,
		tokenID:       m.PlanetScaleServiceTokenID,
		token:         m.PlanetScaleServiceToken,
		deployTimeout: m.PlanetScaleDeployTimeout,
	})
}

// telemetryParamsSharedOrg reconciles the two consumers of a shared
// `--planetscale-org` flag on `restore` / `sync start` (audit MED-A1):
// the ADR-0107/0115 telemetry opt-in (metrics token pair, all-or-nothing
// loud refusal) and the ADR-0148 index-build fallback (service token
// pair, opportunistic). When the org is evidently set FOR the fallback —
// the fallback armed and NO metrics token piece was supplied — the org is
// blanked for the telemetry builder so a fallback-only arming doesn't
// trip the telemetry refusal; telemetry simply stays off, named by a
// WARN. Every other combination is byte-identical to before: a complete
// metrics pair runs telemetry alongside the fallback, and a PARTIAL
// metrics pair (evident telemetry intent, typo'd) keeps the loud
// all-or-nothing refusal.
func telemetryParamsSharedOrg(ctx context.Context, p telemetryParams, fallbackArmed bool) telemetryParams {
	if !fallbackArmed || p.org == "" || p.tokenID != "" || p.token != "" {
		return p
	}
	slog.WarnContext(ctx,
		"--planetscale-org arms the deploy-request index-build fallback (service token resolved); target-health telemetry stays OFF — supply --planetscale-metrics-token-id/--planetscale-metrics-token to enable it too")
	p.org = ""
	return p
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
