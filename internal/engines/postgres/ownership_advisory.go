// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
)

// CurrentRole reports the role the target connection authenticates as
// (`SELECT current_user`). It exists for the pipeline's PlanetScale-
// Postgres ownership advisory (soak finding F10): every table sluice
// creates is OWNED by this role, so a connection made as an ephemeral
// PlanetScale `pscale_api_*` role leaves the created objects owned by a
// role that may later be deleted or superseded — an operator hazard the
// pipeline surfaces (never auto-handles, per the contain-Postgres-
// complexity tenet).
//
// This is an advisory probe: callers treat any error as "unknown, skip
// the advisory", never as a hard failure. It satisfies the pipeline's
// optional `currentRoleReporter` surface by structural match, so a non-PG
// target (no such method) simply opts out.
func (w *RowWriter) CurrentRole(ctx context.Context) (string, error) {
	var role string
	if err := w.db.QueryRowContext(ctx, `SELECT current_user`).Scan(&role); err != nil {
		return "", fmt.Errorf("postgres: probe current role: %w", err)
	}
	return role, nil
}
