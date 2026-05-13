// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"
	"strings"

	gomysql "github.com/go-sql-driver/mysql"
)

// # MySQL DDL error wrapping for operator-friendly hints
//
// MySQL / Vitess return generic error codes (typically `Error 1105
// (HY000): ...`) for several operationally-distinct conditions.
// The raw error text is correct but not actionable — operators
// hitting it for the first time don't know whether the cause is
// transient (retry), configuration (fix the server), or workflow
// (use a different sluice flag). This file maps the known DDL
// error shapes to actionable hints.

// ErrSafeMigrationsBlocked is the sentinel returned by
// [wrapDDLError] when MySQL/Vitess refuses a DDL because the target
// PlanetScale branch has Safe Migrations enabled. Wrapped with the
// underlying [*gomysql.MySQLError] so errors.As recovers the driver
// error if needed. Operators see a hint pointing at the new
// `--schema-already-applied` flag (v0.45.0) or the recovery dance
// of temporarily disabling Safe Migrations.
//
// GitHub issue #17.
var ErrSafeMigrationsBlocked = errors.New("mysql: target branch has Safe Migrations enabled; direct DDL is blocked")

// wrapDDLError inspects an error returned by a DDL exec and, when
// the error matches a recognised operator-actionable shape,
// returns a wrapped error with a clear hint. Returns err unchanged
// for unrecognised shapes (the caller's existing wrap stays the
// outermost).
//
// Currently recognised shapes:
//
//   - Error 1105 (HY000) with "direct DDL is disabled" — Vitess /
//     PlanetScale Safe Migrations on the target branch. Operators
//     hit this when their PlanetScale branch has Safe Migrations
//     enabled (the recommended production configuration). Wrap
//     with [ErrSafeMigrationsBlocked] + a multi-line hint pointing
//     at the v0.45.0 `--schema-already-applied` flag and the
//     temporary-disable workaround.
//
// More patterns added here as operator reports surface them. The
// wrapper is intentionally cheap (no allocation in the happy path)
// so it's safe to invoke at every DDL Exec call site.
func wrapDDLError(err error) error {
	if err == nil {
		return nil
	}
	var mysqlErr *gomysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return err
	}
	// Vitess wraps various upstream errors with code 1105; match on
	// the message text since the code alone isn't specific enough.
	// "direct DDL is disabled" is the PlanetScale Safe Migrations
	// refusal — the only 1105 shape this wrapper recognises today.
	if mysqlErr.Number == 1105 && strings.Contains(mysqlErr.Message, "direct DDL is disabled") {
		return fmt.Errorf("%w: %w | "+
			"To bootstrap a sync stream against a Safe-Migrations-enabled target: "+
			"(a) pre-create the source schema (and the `sluice_cdc_state` table) via a PlanetScale "+
			"deploy request, then re-run with `--schema-already-applied` to skip sluice's "+
			"schema-apply phase entirely; or "+
			"(b) temporarily disable Safe Migrations on the target branch, run sluice to bootstrap "+
			"the schema and the control table, then re-enable Safe Migrations once the stream "+
			"is in CDC mode. "+
			"See `sluice sync start --help` for `--schema-already-applied` details. GitHub issue #17",
			ErrSafeMigrationsBlocked, err)
	}
	return err
}
