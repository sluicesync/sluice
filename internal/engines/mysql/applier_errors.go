// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
)

// # Applier error classification for ADR-0038's retry policy
//
// The applier wraps its raw driver returns through [classifyApplierError]
// before bubbling them up to the pipeline's retry loop. The wrapper
// implements [ir.RetriableError] for the documented transient shapes
// and returns the original error verbatim for non-retriable shapes.
// Non-classified errors are non-retriable by default (errors.As against
// [ir.RetriableError] simply fails), so a previously-fail-fast error
// stays fail-fast — the classifier never introduces a NEW retry path.
//
// Shapes considered retriable (ADR-0038 classifier table):
//
//   - Error 1213 (40001) — InnoDB deadlock detected. Idempotent
//     replay against the new lock order is the standard recovery.
//   - Error 1105 (HY000) with vttablet message AND code = Aborted /
//     Unavailable / ResourceExhausted — Vitess tx-killer rollback,
//     vttablet not ready, throttler. Routinely transient on
//     PlanetScale / managed-Vitess.
//   - driver.ErrBadConn / io.EOF / connection-reset shapes — the
//     driver auto-reconnects on the next exec; retrying the batch
//     on a fresh connection is the right move.
//
// Shapes explicitly NOT retriable:
//
//   - Error 1062 (23000) — duplicate key. Either a non-PK uniqueness
//     violation (operator data bug) or a sluice idempotency gap
//     (e.g. GitHub issue #14). Retrying would mask the underlying
//     issue; failing loudly surfaces it.
//   - All other errors — default-deny per the ADR. Adding to the
//     retriable set requires a documented justification.

// retriableMySQLError satisfies [ir.RetriableError] for a classified
// transient. The wrapped underlying error is preserved via Unwrap so
// errors.Is / errors.As against the driver's *MySQLError still works
// from the consumer side.
type retriableMySQLError struct {
	err  error
	hint time.Duration
}

func (e *retriableMySQLError) Error() string            { return e.err.Error() }
func (e *retriableMySQLError) Unwrap() error            { return e.err }
func (e *retriableMySQLError) Retriable() bool          { return true }
func (e *retriableMySQLError) RetryHint() time.Duration { return e.hint }

// classifyApplierError inspects err and returns a value satisfying
// [ir.RetriableError] when err matches one of the documented MySQL /
// Vitess transient shapes. Returns err unchanged for non-retriable
// shapes (the pipeline's retry loop treats those as terminal).
//
// nil in → nil out.
//
// See the file-header comment for the classifier table; ADR-0038 is
// the source of the policy decisions.
func classifyApplierError(err error) error {
	if err == nil {
		return nil
	}

	// Driver-level "bad connection" / EOF — auto-reconnect on retry.
	// These wrap as the bare sentinels; check via errors.Is for the
	// standard cases the driver returns.
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) {
		return &retriableMySQLError{err: err}
	}

	// MySQL-protocol errors carry a numeric code. The wrappers we
	// care about for transients:
	//
	//   - 1213: InnoDB deadlock (always retriable)
	//   - 1105: HY000 — Vitess uses this code to wrap upstream
	//     gRPC status codes. The message contains "vttablet: rpc
	//     error: code = X desc = ..." where X is the gRPC code.
	//     Aborted / Unavailable / ResourceExhausted are transients;
	//     other gRPC codes (InvalidArgument, NotFound, etc.) are
	//     terminal.
	//   - 1062: duplicate key — explicitly NOT retriable.
	var mysqlErr *gomysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1213:
			return &retriableMySQLError{err: err}
		case 1105:
			if classifyVitessMessage(mysqlErr.Message) {
				return &retriableMySQLError{err: err}
			}
		case 1062:
			// Explicit non-retriable: don't wrap. Falls through to
			// the bare return below.
		}
	}

	// Connection-string-level transients that don't surface as a
	// MySQLError but do appear as raw error text from the driver
	// or the connection pool. Pattern-match defensively.
	if msg := err.Error(); msg != "" {
		switch {
		case strings.Contains(msg, "connection reset by peer"),
			strings.Contains(msg, "connection refused"),
			strings.Contains(msg, "broken pipe"),
			strings.Contains(msg, "i/o timeout"):
			return &retriableMySQLError{err: err}
		}
	}

	return err
}

// classifyVitessMessage returns true when a MySQL Error 1105's text
// contains a Vitess gRPC code that ADR-0038 marks as transient.
// vttablet error messages have the shape:
//
//	target: <keyspace>.<shard>.<tablettype>: vttablet: rpc error:
//	code = <CODE> desc = <reason> (<details>)
//
// We discriminate on the gRPC code:
//
//   - Aborted: tx-killer rollback, tablet primary stepping down
//   - Unavailable: vttablet not ready, primary in-flight failover
//   - ResourceExhausted: throttler engaged, connection pool full
//
// Other codes (InvalidArgument, FailedPrecondition, etc.) are
// terminal — the operator's SQL is wrong, or a constraint is being
// violated. Retrying those would mask real bugs.
func classifyVitessMessage(msg string) bool {
	if !strings.Contains(msg, "vttablet") {
		return false
	}
	// Match on "code = X" — Vitess emits these verbatim in the
	// error text per the gRPC status proto.
	switch {
	case strings.Contains(msg, "code = Aborted"),
		strings.Contains(msg, "code = Unavailable"),
		strings.Contains(msg, "code = ResourceExhausted"):
		return true
	}
	return false
}
