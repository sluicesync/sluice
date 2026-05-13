// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// # Applier error classification for ADR-0038's retry policy
//
// PG-side mirror of the MySQL classifier. The applier wraps its
// raw driver returns through [classifyApplierError] before bubbling
// them up to the pipeline's retry loop. The wrapper implements
// [ir.RetriableError] for the documented transient SQLSTATE shapes
// and returns the original error verbatim for non-retriable shapes.
// Non-classified errors are non-retriable by default (errors.As
// against [ir.RetriableError] simply fails), so pre-v0.42.0 fail-fast
// behaviour is preserved for any uncategorised error.
//
// Shapes considered retriable (ADR-0038 classifier table):
//
//   - 40001 — serialization_failure. The standard PG retry signal
//     under SERIALIZABLE / REPEATABLE READ.
//   - 40P01 — deadlock_detected. Same as MySQL 1213.
//   - 57P01 (admin_shutdown) / 57P02 (crash_shutdown) /
//     57P03 (cannot_connect_now) — operator-initiated or controller-
//     initiated server restart; the connection comes back.
//   - 08* — connection_exception class (connection lost, broken
//     during the request, etc.). Auto-reconnect on next attempt.
//   - driver.ErrBadConn / io.EOF — pool-level transients.
//
// Shapes explicitly NOT retriable:
//
//   - 23505 — unique_violation. Mirror of MySQL 1062: either a
//     non-PK uniqueness violation (operator data) or a sluice
//     idempotency gap (e.g. GitHub issue #14). Retrying would mask
//     the underlying issue; failing loudly surfaces it.
//   - All other SQLSTATEs — default-deny per the ADR.

// retriablePGError satisfies [ir.RetriableError] for a classified
// transient. The wrapped underlying error is preserved via Unwrap.
type retriablePGError struct {
	err  error
	hint time.Duration
}

func (e *retriablePGError) Error() string            { return e.err.Error() }
func (e *retriablePGError) Unwrap() error            { return e.err }
func (e *retriablePGError) Retriable() bool          { return true }
func (e *retriablePGError) RetryHint() time.Duration { return e.hint }

// classifyApplierError inspects err and returns a value satisfying
// [ir.RetriableError] when err matches one of the documented PG
// transient SQLSTATEs. Returns err unchanged for non-retriable
// shapes.
//
// nil in → nil out.
func classifyApplierError(err error) error {
	if err == nil {
		return nil
	}

	// Driver-level "bad connection" / EOF — auto-reconnect on retry.
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) {
		return &retriablePGError{err: err}
	}

	// pgx surfaces server-side errors as *pgconn.PgError carrying a
	// SQLSTATE in .Code. Match against the ADR-0038 retriable set.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", "40P01",
			"57P01", "57P02", "57P03":
			return &retriablePGError{err: err}
		case "23505":
			// Explicit non-retriable per ADR-0038 — fall through
			// to the bare return.
		}
		// Class 08: connection_exception. Includes 08000, 08003,
		// 08006, 08007, 08P01. All are network / connection-state
		// transients.
		if strings.HasPrefix(pgErr.Code, "08") {
			return &retriablePGError{err: err}
		}
	}

	// Connection-string-level transients that bypass *pgconn.PgError
	// (the driver couldn't reach the server to even get a SQLSTATE).
	if msg := err.Error(); msg != "" {
		switch {
		case strings.Contains(msg, "connection reset by peer"),
			strings.Contains(msg, "connection refused"),
			strings.Contains(msg, "broken pipe"),
			strings.Contains(msg, "i/o timeout"),
			strings.Contains(msg, "the database system is starting up"),
			strings.Contains(msg, "the database system is shutting down"):
			return &retriablePGError{err: err}
		}
	}

	return err
}
