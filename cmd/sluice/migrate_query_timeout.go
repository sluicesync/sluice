// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Arming the ADR-0182 query-timeout raise (roadmap item 110)
//
// The CLI is the composer: it builds the [ir.QueryTimeoutController] from the
// SAME shared PlanetScale flags the ADR-0148 index fallback uses (org,
// database, branch, service token), and hands it to the engine-neutral
// pipeline, which never imports the PlanetScale control plane.
//
// Unlike the index fallback's OPPORTUNISTIC arming (a missing credential WARNs
// and leaves the fallback off), the query-timeout raise is EXPLICITLY
// requested by a flag, so a flag set on a target/credential combination that
// cannot honor it is a LOUD refusal — the operator asked for it and should
// learn why it can't happen. The controller itself is composed whenever the
// credentials resolve (so it is also available for the crash-recovery
// auto-revert on a run that does NOT pass the flag but does carry the
// credentials); the flag only ARMS the raise.

package main

import (
	"errors"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/planetscale/querytimeout"
)

// composePlanetScaleQueryTimeoutController builds the ADR-0182 controller from
// a command's shared PlanetScale credentials, or nil when they don't resolve
// to a planetscale target with an org + complete service token + derivable
// database. nil disables both the raise and the auto-revert.
func composePlanetScaleQueryTimeoutController(p indexFallbackParams) ir.QueryTimeoutController {
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
	return &querytimeout.Raiser{
		API:      api.New(api.Config{TokenID: p.tokenID, Token: p.token}),
		Org:      p.org,
		Database: database,
		Branch:   p.branch,
	}
}

// planetScaleQueryTimeoutController composes migrate's controller from its
// flags and, when the raise flag is set but nothing can honor it, returns a
// loud refusal instead of a silent no-op.
func (m *MigrateCmd) planetScaleQueryTimeoutController() (ir.QueryTimeoutController, error) {
	ctrl := composePlanetScaleQueryTimeoutController(indexFallbackParams{
		targetDriver: m.TargetDriver,
		targetDSN:    m.Target,
		org:          m.PlanetScaleOrg,
		database:     m.PlanetScaleDatabase,
		branch:       m.PlanetScaleBranch,
		tokenID:      m.PlanetScaleServiceTokenID,
		token:        m.PlanetScaleServiceToken,
	})
	if m.PlanetScaleRaiseQueryTimeout && ctrl == nil {
		return nil, errors.New("--planetscale-raise-query-timeout requires a PlanetScale target (--target-driver planetscale) with --planetscale-org and a service token " +
			"(PLANETSCALE_SERVICE_TOKEN_ID / PLANETSCALE_SERVICE_TOKEN); it cannot raise a keyspace query timeout without them")
	}
	return ctrl, nil
}
