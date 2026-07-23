// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// IsReadTransientSQLState reports whether err carries a PG SQLSTATE from the
// CONNECTION-AVAILABILITY transient set — the shapes a read-only poll hits
// when the server restarts, a standby promotes, or the connection drops
// server-side:
//
//   - 57P01 admin_shutdown / 57P02 crash_shutdown / 57P03
//     cannot_connect_now — operator- or controller-initiated restart; the
//     server comes back.
//   - class 08 connection_exception (08000/08003/08006/08007/08P01) —
//     network / connection-state faults.
//
// It exists for the pgtrigger sibling engine's change-log poll (the tracked
// v0.99.286 follow-up: its transport-level classifier could not see
// SQLSTATE-level transients because this package's applier classifier is
// deliberately private). It is a PREDICATE, not a wrapper — the caller owns
// the retriable wrapping (triggercdc.AsTransient), so no wrapper type leaks
// across the package boundary.
//
// Deliberately NARROWER than the applier's classifyApplierError: no
// 40001/40P01 (a plain poll SELECT has no serialization-retry semantics),
// no class 53 (the cold-copy grow-window shape, not a poll's), and above
// all no 42703/42P01 — a missing change-log table is an operator/setup
// fault that must stay TERMINAL on the trigger engines, while the applier
// maps those to schema-drift semantics.
func IsReadTransientSQLState(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case "57P01", "57P02", "57P03":
		return true
	}
	return strings.HasPrefix(pgErr.Code, "08")
}
