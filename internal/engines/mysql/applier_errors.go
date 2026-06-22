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
//
// txKilled additionally satisfies [ir.TransactionKilledError] when the
// classified transient is a Vitess tx-killer abort (Error 1105 with a
// `code = Aborted ... for tx killer rollback` payload). The AIMD
// controller reads that surface as a strong, immediate shrink signal
// (ADR-0052; the v0.99.69 sustained-tx-killer finding) — a batch the
// target rolled back for exceeding its tx-timeout window must shrink,
// not re-submit at the same size and be killed again.
type retriableMySQLError struct {
	err      error
	hint     time.Duration
	txKilled bool
}

func (e *retriableMySQLError) Error() string            { return e.err.Error() }
func (e *retriableMySQLError) Unwrap() error            { return e.err }
func (e *retriableMySQLError) Retriable() bool          { return true }
func (e *retriableMySQLError) RetryHint() time.Duration { return e.hint }
func (e *retriableMySQLError) TransactionKilled() bool  { return e.txKilled }

// isMySQLDeadlock reports whether err is (or wraps) an InnoDB deadlock —
// MySQL error 1213 / SQLSTATE 40001. The deadlock victim's transaction is
// rolled back and should be retried; classifyApplierError already treats
// it as retriable on the apply path, and the shard-lease acquire uses this
// to retry its acquire transaction under concurrent-shard contention.
func isMySQLDeadlock(err error) bool {
	var mysqlErr *gomysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1213
}

// isMySQLDupKey reports whether err is (or wraps) a duplicate-key error —
// MySQL error 1062 / SQLSTATE 23000. classifyApplierError keeps 1062
// firmly NON-retriable (ADR-0038: a real uniqueness violation or an
// idempotency gap must fail loudly). This predicate exists ONLY for the
// ADR-0108 plain-cold-copy "tolerate-1062-on-retry" wart: a byte-
// identical atomic INSERT re-applied after a classified transient may
// have committed-but-lost-the-ack on the prior attempt, so a 1062 on the
// RETRY of the same batch means those exact rows already landed durably.
// See writeBatchedConn for the full safety argument and why this is
// scoped to retry-only.
func isMySQLDupKey(err error) bool {
	var mysqlErr *gomysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}

// isDiskFullSignal reports whether err is (or wraps / textually carries) a
// source-side OUT-OF-DISK signal. Used to enrich the source-unresponsive
// diagnosis (the verify-timeout path) — a full source datadir is a leading
// cause of a wedged source, but MySQL surfaces it inconsistently: sometimes
// as ER_DISK_FULL (1021), often as the OS ENOSPC text ("No space left on
// device" / "errno: 28"), and frequently NOT as a returned error at all —
// MySQL famously BLOCKS on a full disk ("Disk full ...; waiting for someone
// to free some space"), which is why the verify times out rather than erroring
// (so this matcher is best-effort enrichment, never the sole detector). The
// match is broad-but-specific: these phrases do not appear in healthy errors.
func isDiskFullSignal(err error) bool {
	if err == nil {
		return false
	}
	var mysqlErr *gomysql.MySQLError
	if errors.As(err, &mysqlErr) && (mysqlErr.Number == 1021 || mysqlErr.Number == 1114) {
		// 1021 ER_DISK_FULL; 1114 ER_RECORD_FILE_FULL ("The table is full").
		// On a managed InnoDB target the latter means the tablespace/volume
		// is out of space — the SAME root as ER_DISK_FULL, just a different
		// code (vttablet wraps it as `code = ResourceExhausted desc = The
		// table '<t>' is full`). The v0.99.96 PS-320 finding covered errno-28
		// / Error 3 / 1021 but missed 1114, which the next storage-grow step
		// surfaced (v0.99.97 PS-320-v6); both must be treated as a transient
		// out-of-disk so the bounded retry rides the auto-grow.
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no space left on device") ||
		strings.Contains(msg, "errno: 28") ||
		strings.Contains(msg, "disk full") ||
		strings.Contains(msg, "waiting for someone to free some space") ||
		strings.Contains(msg, "the table is full") ||
		strings.Contains(msg, "is full (errno")
}

// isReadOnlyTargetSignal reports whether err is a target that is
// transiently READ-ONLY — another face of a PlanetScale storage
// auto-grow / reparent window (the v0.99.100 PS-320-v10 live finding).
// During a grow's serving transition the target tablet briefly runs with
// `--read-only` (it has not yet been promoted to the new primary), and an
// in-flight write surfaces ER_OPTION_PREVENTS_STATEMENT (1290): "The MySQL
// server is running with the --read-only option so it cannot execute this
// statement" (vttablet frames it as `code = Code(17)` but the driver still
// parses Number==1290). It is TRANSIENT — once the new primary is serving,
// the retry succeeds — so it belongs to the same bounded-retry class as the
// reparent / disk-full / lock-wait faces. A genuinely read-only target
// (e.g. a replica endpoint, a misconfigured DSN) exhausts the retry budget
// and fails LOUDLY, never an infinite wait.
//
// 1290 (ER_OPTION_PREVENTS_STATEMENT) is a GENERIC code ("running with the
// %s option"); only the read-only variant is the grow transient, so the
// match requires the read-only wording — an unrelated 1290 stays terminal.
func isReadOnlyTargetSignal(err error) bool {
	if err == nil {
		return false
	}
	var mysqlErr *gomysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1290 {
		m := strings.ToLower(mysqlErr.Message)
		if strings.Contains(m, "read-only") || strings.Contains(m, "read only") {
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "--read-only option") ||
		strings.Contains(msg, "--super-read-only option") ||
		strings.Contains(msg, "running with the --read-only")
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
	//   - 1205: InnoDB lock-wait-timeout (ER_LOCK_WAIT_TIMEOUT) —
	//     deadlock's sibling and the textbook "retry the transaction"
	//     transient (the rolled-back txn succeeds on a retry once the
	//     contending lock releases). vttablet wraps it as
	//     `code = DeadlineExceeded desc = Lock wait timeout exceeded`.
	//     Surfaces heavily under a prolonged PlanetScale storage-grow
	//     stall when the concurrent cold-copy writers contend (the
	//     v0.99.96 PS-320-v5 live finding — the copy rode ~13 min of
	//     disk-full/query-killer retries, then died here). Always
	//     retriable, like 1213.
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
		case 1213, 1205:
			// 1213 InnoDB deadlock + 1205 lock-wait-timeout — the
			// canonical "retry the transaction" InnoDB transients.
			return &retriableMySQLError{err: err}
		case 1105:
			if classifyVitessMessage(mysqlErr.Message) {
				return &retriableMySQLError{
					err:      err,
					txKilled: isVitessTxKillerMessage(mysqlErr.Message),
				}
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
	//
	// "not serving" / "reparent" (ADR-0108): a target PRIMARY REPARENT
	// (e.g. a PlanetScale non-Metal storage auto-grow at the ~39 GB
	// boundary) makes the in-flight tablet briefly "not serving". The
	// vttablet-framed shape (Error 1105 `code = Unavailable`) is already
	// caught above, but a PlanetScale/vtgate reparent can ALSO surface
	// without that framing — belt-and-suspenders so the cold-copy
	// reparent-retry (ADR-0108) AND the CDC apply path (ADR-0038) both
	// ride it out. Case-insensitive: vtgate/vttablet wording varies in
	// case across versions. These phrases do not appear in healthy or
	// terminal-semantic errors.
	if msg := err.Error(); msg != "" {
		switch {
		case strings.Contains(msg, "connection reset by peer"),
			strings.Contains(msg, "connection refused"),
			strings.Contains(msg, "broken pipe"),
			strings.Contains(msg, "i/o timeout"):
			return &retriableMySQLError{err: err}
		}
		lower := strings.ToLower(msg)
		for _, sub := range reparentRetriableSubstrings {
			if strings.Contains(lower, sub) {
				return &retriableMySQLError{err: err}
			}
		}
	}

	// Target transiently OUT OF DISK — the ROOT face of a PlanetScale
	// non-Metal storage auto-grow (ADR-0108/0109). While the volume is full
	// and growing, a write surfaces "No space left on device" (OS errno 28),
	// which MySQL/vttablet wraps inconsistently: as Error 3 "Error writing
	// file" (code = Unknown), ER_DISK_FULL (1021), or the ENOSPC text — none
	// of which the Error-1105 vttablet-code branch above catches (it gates on
	// Number==1105). It is TRANSIENT: the auto-grow adds space and the retry
	// succeeds (the v0.99.95 PS-320-v4 live finding — the copy rode ~8 min of
	// query-killer retries, then died here on the unretried disk-full). A
	// bounded retry rides the grow out; a genuinely-full, NON-growing target
	// (e.g. an undersized fixed-storage Metal) exhausts the retry budget and
	// fails LOUDLY — never an infinite wait. isDiskFullSignal matches the
	// errno-28 text + ER_DISK_FULL; reused here (it already exists for the
	// source-unresponsive diagnosis) so the same shape is recognized on the
	// target write path.
	if isDiskFullSignal(err) {
		return &retriableMySQLError{err: err}
	}

	// Target transiently READ-ONLY — another face of a PlanetScale storage
	// auto-grow / reparent window (the v0.99.100 PS-320-v10 live finding).
	// During the grow's serving transition the tablet briefly runs with
	// `--read-only` before the new primary is promoted, and an in-flight
	// write surfaces ER_OPTION_PREVENTS_STATEMENT (1290). It is TRANSIENT —
	// the retry succeeds once the new primary serves — so it joins the same
	// bounded-retry class as the reparent / disk-full / lock-wait faces, and
	// the cold-copy grow-gate (ADR-0110) then quiesces the lanes for the
	// window. A genuinely read-only target exhausts the budget and fails
	// loudly. The entire v0.99.92–v0.99.99 arc never saw this face; the
	// ADR-0110 live validation surfaced it (it died unretried before the
	// grow-gate could engage, because the gate only fires on a CLASSIFIED
	// transient).
	if isReadOnlyTargetSignal(err) {
		return &retriableMySQLError{err: err}
	}

	return err
}

// reparentRetriableSubstrings is the EXACT (lower-cased) substring set
// that marks an un-framed target primary-reparent / "not serving"
// transient as retriable (ADR-0108) — the belt-and-suspenders fallback
// for a PlanetScale/vtgate reparent that surfaces WITHOUT the vttablet
// `code = Unavailable` framing [classifyVitessMessage] already catches.
//
// Pinned as a standalone slice in the same discipline as
// [vitessRetriableSubstrings] / [vitessTxKillerSubstrings]:
// [TestReparentRetriableSubstrings_PinDown] pins these literals so a
// future Vitess/PlanetScale wording change fails a test rather than
// silently non-retrying a production reparent. The matcher lower-cases
// the error text before comparing, so these MUST be lower-case. Do NOT
// inline these strings elsewhere — extend this slice and the pin test
// together.
//
//	"not serving" — the tablet-state phrase a reparent surfaces while the
//	                new primary is being promoted ("tablet ... is not
//	                serving", "primary is not serving").
//	"reparent"    — the operation name itself, in case it appears in the
//	                error text (PlanetScale/vtgate emergency-reparent
//	                / planned-reparent messages).
var reparentRetriableSubstrings = []string{
	"not serving",
	"reparent",
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
//	"QueryList.TerminateAll" — vttablet's QUERY-killer (surfaces as
//	                        `code = Canceled desc = QueryList.TerminateAll()
//	                        ... killing connection ID N`). vttablet kills a
//	                        long-running query/connection when it exceeds the
//	                        query timeout OR during a pool drain on a
//	                        reparent/storage-grow stall — e.g. a PlanetScale
//	                        non-Metal storage auto-grow blocks the in-flight
//	                        INSERT past the query timeout, then vttablet
//	                        terminates it (the v0.99.93 PS-320-v3 live
//	                        finding). It is TRANSIENT: retrying after the
//	                        stall clears succeeds. Matched by the SPECIFIC
//	                        reason `QueryList.TerminateAll`, NOT a blanket
//	                        `code = Canceled` — a bare Canceled also covers a
//	                        CLIENT-side ctx cancel (clean shutdown) which must
//	                        stay terminal, and that shape never carries the
//	                        server-side TerminateAll reason. Sibling of the
//	                        tx-killer (`code = Aborted "tx killer"`, #54) but
//	                        NOT flagged TransactionKilled — it is a stall, not
//	                        an oversized-tx, so it should retry-at-size, not
//	                        force an AIMD shrink.
//
// Other gRPC codes (InvalidArgument, FailedPrecondition, NotFound,
// PermissionDenied, …) are terminal — the operator's SQL is wrong or
// a constraint is being violated; retrying those would mask real bugs.
// A bare `code = Canceled` is deliberately ABSENT (client-cancel ambiguity);
// only the server-side `QueryList.TerminateAll` reason is retriable.
var vitessRetriableSubstrings = []string{
	"code = Aborted",
	"code = Unknown",
	"code = Unavailable",
	"code = ResourceExhausted",
	"QueryList.TerminateAll",
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

// vitessTxKillerSubstrings are the markers that distinguish a Vitess
// transaction-killer abort from the other retriable 1105 shapes. The
// tx-killer rolls back a transaction held longer than vttablet's
// wall-clock timeout; its payload is `code = Aborted ... for tx killer
// rollback` (the live v0.99.69 finding) — but vttablet has worded the
// reason differently across versions, so the match is the union of:
//
//	"tx killer"           — the canonical reason fragment ("for tx
//	                        killer rollback"), version-stable.
//	"exceeded ... timeout" markers are NOT matched here because they
//	                        also cover non-killer Aborted shapes; the
//	                        "tx killer" fragment is the precise signal.
//
// A bare `code = Aborted` WITHOUT the tx-killer fragment (e.g. a
// primary stepping down) is still retriable (classifyVitessMessage
// returns true) but is NOT a tx-killer — re-applying the same batch
// after a failover succeeds, so it should not force a shrink. Keeping
// the tx-killer match narrow avoids shrinking the batch on transients
// that a same-size retry would clear.
//
// [TestVitessTxKillerSubstrings_PinDown] pins these literals so a
// future Vitess wording change fails a test rather than silently
// classifying a tx-killer abort as a generic transient (which would
// re-open the v0.99.69 die-on-sustained-kill failure mode). Extend
// this slice and the pin test together.
var vitessTxKillerSubstrings = []string{
	"tx killer",
}

// isVitessTxKillerMessage reports whether a MySQL Error 1105's text is
// specifically a Vitess transaction-killer abort (a subset of the
// shapes [classifyVitessMessage] marks retriable). Callers gate the
// call on classifyVitessMessage already returning true, so this only
// needs to test the tx-killer discriminator on a known-vttablet
// message.
func isVitessTxKillerMessage(msg string) bool {
	if !strings.Contains(msg, "vttablet") {
		return false
	}
	for _, sub := range vitessTxKillerSubstrings {
		if strings.Contains(msg, sub) {
			return true
		}
	}
	return false
}
