// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package fkcheck is the control-plane half of the PlanetScale foreign-key
// preflight — the read-only sibling of the querytimeout package.
//
// [Checker] implements [ir.PlanetScaleForeignKeyChecker]: it reads the target
// DATABASE's foreign_keys_enabled (the read-back of allow_foreign_key_constraints)
// and the target BRANCH's safe_migrations, so the migrate/sync preflight can
// tell — before the copy — whether the foreign keys a run will add can actually
// land. Both are cheap single GETs.
//
// It lives here (not the engine-neutral pipeline, not the mysql engine) so it
// composes the api.Client directly, exactly as the querytimeout.Raiser does;
// the CLI is the composer. The pipeline owns the WARN/refuse WORKFLOW; this type
// owns the control-plane HOW.
package fkcheck

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/planetscale/api"
)

// Checker is the PlanetScale implementation of [ir.PlanetScaleForeignKeyChecker].
// Construct once per run (the CLI arms it only for a planetscale target with an
// org + service token + derivable database).
type Checker struct {
	// API is the shared control-plane client. Required.
	API *api.Client

	// Org, Database, Branch identify the target. Branch defaults to "main"
	// (the production branch), mirroring the querytimeout.Raiser.
	//
	// PREMISE, shared with that Raiser (2026-08-22 invariant sweep):
	// safe_migrations is BRANCH-scoped, and nothing derives the branch from
	// the target DSN — an operator writing into a non-default branch without
	// passing --planetscale-branch gets main's safe-migrations verdict, so
	// the WARN/all-clear may describe the wrong branch. foreign_keys_enabled
	// is DATABASE-scoped, so the preflight's only REFUSAL is immune to this.
	Org      string
	Database string
	Branch   string
}

// Compile-time proof the checker satisfies the pipeline-facing surface.
var _ ir.PlanetScaleForeignKeyChecker = (*Checker)(nil)

// ForeignKeyStatus implements [ir.PlanetScaleForeignKeyChecker]: two GETs, the
// database's foreign_keys_enabled and the branch's safe_migrations.
func (c *Checker) ForeignKeyStatus(ctx context.Context) (ir.PlanetScaleForeignKeyStatus, error) {
	db, err := c.API.GetDatabase(ctx, c.Org, c.Database)
	if err != nil {
		return ir.PlanetScaleForeignKeyStatus{}, fmt.Errorf("planetscale: read database %q foreign-key support: %w", c.Database, err)
	}
	branch, err := c.API.GetBranch(ctx, c.Org, c.Database, c.branch())
	if err != nil {
		return ir.PlanetScaleForeignKeyStatus{}, fmt.Errorf("planetscale: read branch %q safe-migrations: %w", c.branch(), err)
	}
	return ir.PlanetScaleForeignKeyStatus{
		ForeignKeysEnabled: db.ForeignKeysEnabled,
		SafeMigrations:     branch.SafeMigrations,
	}, nil
}

func (c *Checker) branch() string {
	if c.Branch == "" {
		return "main"
	}
	return c.Branch
}
