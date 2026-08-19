// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Composing the PlanetScale foreign-key preflight probe
//
// The CLI is the composer: it builds the [ir.PlanetScaleForeignKeyChecker] from
// the SAME shared PlanetScale flags the ADR-0148 index fallback and the ADR-0182
// query-timeout raise use (org, database, branch, service token), and hands it
// to the engine-neutral pipeline, which never imports the PlanetScale control
// plane.
//
// Unlike the query-timeout raise, this probe is NOT armed by a flag — it is a
// pre-copy advisory that runs whenever it CAN (a planetscale target whose
// credentials resolve). When the credentials don't resolve the pipeline still
// runs the preflight, downgraded to a WARN it cannot confirm (see
// migcore.PreflightPlanetScaleForeignKeys). So a missing token is never a
// refusal here — nil is a valid, expected input.

package main

import (
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/planetscale/fkcheck"
)

// composePlanetScaleForeignKeyChecker builds the FK-preflight probe from a
// command's shared PlanetScale credentials, or nil when they don't resolve to a
// planetscale target with an org + complete service token + derivable database.
// nil disables the confirmable refusal but not the WARN — the pipeline still
// runs the preflight.
func composePlanetScaleForeignKeyChecker(p indexFallbackParams) ir.PlanetScaleForeignKeyChecker {
	if p.targetDriver != "planetscale" || p.org == "" || p.tokenID == "" || p.token == "" {
		return nil
	}
	database := p.database
	if database == "" {
		database = mysqlDSNDatabase(p.targetDSN)
	}
	if database == "" {
		return nil
	}
	return &fkcheck.Checker{
		API:      api.New(api.Config{TokenID: p.tokenID, Token: p.token}),
		Org:      p.org,
		Database: database,
		Branch:   p.branch,
	}
}

// planetScaleForeignKeyChecker composes migrate's FK-preflight probe from its
// flags. Never an error: an unresolved credential set is nil, and nil is a valid
// input (the pipeline warns instead of refusing).
func (m *MigrateCmd) planetScaleForeignKeyChecker() ir.PlanetScaleForeignKeyChecker {
	return composePlanetScaleForeignKeyChecker(indexFallbackParams{
		targetDriver: m.TargetDriver,
		targetDSN:    m.Target,
		org:          m.PlanetScaleOrg,
		database:     m.PlanetScaleDatabase,
		branch:       m.PlanetScaleBranch,
		tokenID:      m.PlanetScaleServiceTokenID,
		token:        m.PlanetScaleServiceToken,
	})
}
