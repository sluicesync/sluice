// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
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
//     Unknown / Unavailable / ResourceExhausted — Vitess tx-killer
//     rollback, vttablet not ready, throttler. Routinely transient
//     on PlanetScale / managed-Vitess.
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

// isMySQLDeadlock reports whether err is (or wraps) an InnoDB deadlock —
// MySQL error 1213 / SQLSTATE 40001. The deadlock victim's transaction is
// rolled back and should be retried; classifyApplierError already treats
// it as retriable on the apply path, and the shard-lease acquire uses this
// to retry its acquire transaction under concurrent-shard contention.
func isMySQLDeadlock(err error) bool {
	var mysqlErr *gomysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1213
}

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
	//
	// gomysql.ErrInvalidConn is the go-sql-driver/mysql sentinel for
	// "connection marked bad" (errors.go:20 `errors.New("invalid
	// connection")`). It is distinct from database/sql's
	// driver.ErrBadConn — the driver pool surfaces ErrInvalidConn at
	// the application layer when a pooled connection's underlying
	// socket has been closed by the peer (typical shape: PlanetScale
	// TCP reset). GitHub issue #21: pre-v0.48.0 the classifier missed
	// this sentinel and the applier exited instead of retrying, even
	// though the same connection-reset class on PG retries fine.
	//
	// context.DeadlineExceeded surfaces when a per-exec timeout
	// expires on the apply path's tx.ExecContext call (GitHub #23
	// Phase B fix, v0.52.0). The destination connection is closed
	// by the driver's watchCancel; the next attempt opens a fresh
	// connection from the pool. Classifying this as retriable closes
	// the silent-stall failure mode where a half-closed destination
	// connection blocked the apply goroutine indefinitely.
	if errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, gomysql.ErrInvalidConn) ||
		errors.Is(err, context.DeadlineExceeded) {
		return &retriableMySQLError{err: err}
	}

	// MySQL-protocol errors carry a numeric code. The wrappers we
	// care about for transients:
	//
	//   - 1213: InnoDB deadlock (always retriable)
	//   - 1105: HY000 — Vitess uses this code to wrap upstream
	//     gRPC status codes. The message contains "vttablet: rpc
	//     error: code = X desc = ..." where X is the gRPC code.
	//     Aborted / Unknown / Unavailable / ResourceExhausted are
	//     transients; other gRPC codes (InvalidArgument, NotFound,
	//     etc.) are terminal.
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
		case 1054, 1146:
			// Schema drift (Bug F8): 1054 ER_BAD_FIELD_ERROR (unknown
			// column) / 1146 ER_NO_SUCH_TABLE — the source has a
			// column/table the target lacks (sluice does not auto-apply
			// DDL). Symmetric to the PG 42703/42P01 case, so a
			// MySQL→MySQL (incl. PlanetScale→PlanetScale) sync gets the
			// same self-healing behavior instead of a terminal exit →
			// supervisor tight-restart crash-loop. Retriable so the
			// ADR-0038 backoff rides it out; heals when the operator adds
			// the column/table on the target. The wrap names the remedy
			// and keeps the underlying *MySQLError reachable via
			// errors.As. NOT silent — each attempt logs loudly.
			return &retriableMySQLError{err: fmt.Errorf(
				"schema drift: the target is missing a column/table the source has — add it on the target to resume (sluice does not auto-apply DDL): %w", err,
			)}
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

// vitessRetriableSubstrings is the EXACT set of substrings that mark a
// MySQL Error 1105 (HY000) as a Vitess-class transient under ADR-0038.
//
// ADR-0038 pin-down 4 (Operator-review sign-off, 2026-05-18): Vitess
// wraps every transient in a free-text `1105 (HY000)` payload — there
// is no structured gRPC status code to match on, so classification is
// substring-based. This slice is the single source of truth for that
// match set; [TestVitessRetriableSubstrings_PinDown4] pins these
// literals so a future Vitess wording change fails a test rather than
// silently non-retrying a production transient. Do NOT inline these
// strings elsewhere — extend this slice and the pin test together.
//
//	"vttablet"            — the discriminator tag. A bare HY000
//	                        without it is a non-Vitess generic error
//	                        and stays terminal.
//	"code = Aborted"      — tx-killer rollback, primary stepping down.
//	"code = Unknown"      — vttablet wraps several internal transients
//	                        (e.g. caller-id / pool churn) as Unknown;
//	                        ADR-0038's MySQL table lists it retriable.
//	"code = Unavailable"  — vttablet not ready, in-flight failover.
//	"code = ResourceExhausted" — throttler engaged, pool full.
//
// Other gRPC codes (InvalidArgument, FailedPrecondition, NotFound,
// PermissionDenied, …) are terminal — the operator's SQL is wrong or
// a constraint is being violated; retrying those would mask real bugs.
var vitessRetriableSubstrings = []string{
	"code = Aborted",
	"code = Unknown",
	"code = Unavailable",
	"code = ResourceExhausted",
}

// classifyVitessMessage returns true when a MySQL Error 1105's text
// contains a Vitess gRPC code that ADR-0038 marks as transient.
// vttablet error messages have the shape:
//
//	target: <keyspace>.<shard>.<tablettype>: vttablet: rpc error:
//	code = <CODE> desc = <reason> (<details>)
//
// The match is "vttablet" present AND one of
// [vitessRetriableSubstrings] present. See that slice's doc comment
// and ADR-0038 pin-down 4 for why this is substring-based.
func classifyVitessMessage(msg string) bool {
	if !strings.Contains(msg, "vttablet") {
		return false
	}
	for _, sub := range vitessRetriableSubstrings {
		if strings.Contains(msg, sub) {
			return true
		}
	}
	return false
}
