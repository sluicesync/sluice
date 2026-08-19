// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Database resource read (foreign-key-support preflight)
//
// The one field sluice reads off the database object is foreign_keys_enabled —
// the read-back of the `allow_foreign_key_constraints` setting. On a fresh
// PlanetScale database it defaults OFF, and with it off `ADD FOREIGN KEY` is
// rejected outright, so a migration that carries foreign keys walls at the
// constraints phase AFTER the whole copy. The migrate preflight reads this
// (paired with the branch's safe_migrations flag) to warn — or, when it is
// confirmably OFF, to refuse loudly — BEFORE the copy.

package api

import (
	"context"
	"net/url"
)

// Database is the subset of the PlanetScale database object sluice reads.
// ForeignKeysEnabled is the control plane's read-back of
// `allow_foreign_key_constraints` (json `foreign_keys_enabled`): false on a
// fresh database, and while false the platform rejects every `ADD FOREIGN KEY`.
type Database struct {
	Name               string `json:"name"`
	ForeignKeysEnabled bool   `json:"foreign_keys_enabled"`
}

// databasePath builds the escaped org/database resource path.
func databasePath(org, db string) string {
	return "/v1/organizations/" + url.PathEscape(org) + "/databases/" + url.PathEscape(db)
}

// GetDatabase reads one database object — the foreign-key-support preflight's
// foreign_keys_enabled read.
func (c *Client) GetDatabase(ctx context.Context, org, db string) (*Database, error) {
	var out Database
	if err := c.Get(ctx, databasePath(org, db), &out); err != nil {
		return nil, err
	}
	return &out, nil
}
