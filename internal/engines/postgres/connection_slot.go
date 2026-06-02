// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// connectionSlotExhaustedSQLSTATE is PostgreSQL's class-53 SQLSTATE for
// `too_many_connections`. PostgreSQL raises it for BOTH slot-exhaustion
// shapes the connection-resilience note observed:
//
//   - the plain `FATAL: sorry, too many clients already`, and
//   - the `FATAL: remaining connection slots are reserved for roles with
//     the SUPERUSER attribute` raised when a non-superuser hits the
//     superuser_reserved_connections floor.
//
// Both carry SQLSTATE 53300, so a single code match covers the class.
const connectionSlotExhaustedSQLSTATE = "53300"

// superuserReservedSlotsFragment is a lower-cased substring of the
// well-known superuser-reserved-slots FATAL message. It is a
// belt-and-suspenders fallback for the rare path where the FATAL is
// raised so early in connection startup that pgx surfaces it without a
// structured *pgconn.PgError (a raw startup error string) — in that case
// the SQLSTATE match below can't fire, but the message text still
// identifies the class. Matched case-insensitively against the wrapped
// error string. Kept as a fragment (not the full sentence) so a minor
// wording change across PG versions doesn't silently stop matching.
const superuserReservedSlotsFragment = "remaining connection slots are reserved"

// IsConnectionSlotExhausted implements [ir.ConnectionSlotClassifier]. It
// reports true ONLY for the connection-slot-exhaustion class (SQLSTATE
// 53300 — `too_many_connections`, which subsumes the
// superuser-reserved-slots FATAL). Every other error returns false so
// the parallel bulk-copy pool fails it loudly instead of masking it as
// backpressure. See the interface doc for why precision here is a
// correctness property.
//
// The error is inspected through any wrapping (errors.As / a
// lower-cased substring scan) because the engine wraps open/ping
// failures as `postgres: open: %w` / `postgres: ping: %w` before they
// reach the orchestrator.
//
// nil in → false.
func (Engine) IsConnectionSlotExhausted(err error) bool {
	if err == nil {
		return false
	}

	// Primary, precise path: the server returned a structured SQLSTATE.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == connectionSlotExhaustedSQLSTATE
	}

	// Fallback for a startup-phase FATAL that bypassed *pgconn.PgError:
	// match only the superuser-reserved-slots fragment, NOT a broad
	// "too many" scan, to keep the false-positive surface minimal.
	return strings.Contains(strings.ToLower(err.Error()), superuserReservedSlotsFragment)
}
