// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// isUndefinedTableErr reports whether err carries PG SQLSTATE 42P01
// (undefined_table) — "that relation is not there".
//
// This is the engine's ONE such classifier. It previously existed twice under
// names one letter apart (diagnose.go's isUndefinedTableErr and
// health_reporter.go's isUndefinedTableError), while the site that actually
// gated a refusal — [RowWriter.IsTableEmpty] — used neither and matched the
// substring "does not exist" instead (audit backlog C-1).
//
// # Why the substring was the wrong instrument, measured rather than assumed
//
// The text is not stable and it is not specific:
//
//   - PG renders errors through `lc_messages`; a server with a translation
//     installed keeps SQLSTATE 42P01 and loses the English phrase entirely.
//   - "does not exist" is PostgreSQL's house phrasing for a whole family of
//     unrelated conditions. Ground-truthed on a real PG 16: a missing function
//     is 42883, a missing role is 22023 from SET ROLE but 42704 from DROP ROLE
//     or GRANT (measured on a real PG 16 — the code depends on the STATEMENT,
//     not on the object), and — the one that matters here — a
//     connection whose database is gone answers
//     `FATAL: database "app" does not exist` with SQLSTATE 3D000. A pooled
//     `*sql.DB` re-dials, so that error can surface from any query, and reading
//     it as "the table is absent, so the target is empty" is exactly the
//     bypassed-refusal direction.
//
// Two things the SQLSTATE genuinely does cover, verified on the same server, so
// nothing is lost by dropping the text: a missing SCHEMA in a qualified
// reference (`SELECT 1 FROM noschema.t`) is also reported as 42P01, and so is a
// view whose base relation is gone.
func isUndefinedTableErr(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "42P01"
}
