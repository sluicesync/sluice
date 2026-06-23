// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
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
	//
	// context.DeadlineExceeded surfaces when a per-exec timeout
	// expires on the apply path's tx.ExecContext call (GitHub #23
	// Phase B fix, v0.52.0). The destination connection is closed
	// by pgx's ctx-watcher; the next attempt opens a fresh
	// connection from the pool. Classifying this as retriable closes
	// the silent-stall failure mode where a half-closed destination
	// connection blocked the apply goroutine indefinitely.
	if errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, context.DeadlineExceeded) {
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
		case "53100", "53000", "53200":
			// Class 53 — insufficient_resources (roadmap item 38). On a
			// cold-copy COPY into a PlanetScale-class PG target whose volume
			// auto-grows under the streaming write, a 53100 disk_full ("could
			// not extend file … No space left on device") surfaces mid-COPY
			// while the volume grows ahead of the write. Classifying the class
			// as retriable lets the cold-copy grow-gate / chunked-COPY retry
			// (copyChunkWithRetry) ride the grow window and replay the chunk
			// once headroom returns, instead of a terminal exit →
			// supervisor crash-loop (live finding #94). 53000
			// (insufficient_resources, the class root) and 53200
			// (out_of_memory) share the transient grow/back-pressure shape —
			// a momentary resource squeeze that clears. NOT over-matched:
			// 53300 (too_many_connections) and 53400
			// (configuration_limit_exceeded) are deliberately EXCLUDED —
			// those are config/operator faults that do not self-heal by
			// retrying. The retry budget is wall-clock bounded and loud on
			// genuine exhaustion (a target truly out of disk surfaces after
			// ~30 min, never silently).
			return &retriablePGError{err: err}
		case "42703", "42P01":
			// Schema drift (Bug F8): 42703 undefined_column / 42P01
			// undefined_table — the source has a column/table the target
			// lacks (sluice does not auto-apply DDL). Treat as retriable
			// so the ADR-0038 backoff rides it out instead of a terminal
			// exit → supervisor tight-restart crash-loop; it self-heals
			// the moment the operator adds the column/table on the
			// target. The wrap names the remedy and keeps the underlying
			// *pgconn.PgError reachable via errors.As (the offending
			// column stays visible every retry). NOT silent — each
			// attempt logs loudly through the retry loop.
			return &retriablePGError{err: fmt.Errorf(
				"schema drift: the target is missing a column/table the source has — add it on the target to resume (sluice does not auto-apply DDL): %w", err,
			)}
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
