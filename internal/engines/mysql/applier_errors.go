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

	"sluicesync.dev/sluice/internal/nettransient"
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
//   - Error 1105 (HY000) carrying a VTGATE availability sentence
//     ("primary is not serving, there may be a reparent operation in
//     progress", "inconsistent state detected, primary is serving …",
//     "no healthy tablet available for …") — the same failover/
//     discovery class, but emitted by vtgate itself, so it carries NO
//     `vttablet` tag and the branch above cannot see it. See
//     [vtgateTransientSubstrings].
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
//
// idleProgressTimeout additionally satisfies [ir.LivenessProgressTimeoutError]
// when the classified transient is the VStream Phase-2 "established then went
// idle" progress timeout ([vstreamProgressTimeoutError]). The pipeline's
// retry loop reads that surface to keep an idle-but-healthy source's benign
// reconnects OUT of the give-up budget (loose end 2b). It is set ONLY by the
// Phase-2 constructor; a Phase-1 liveness timeout / connection error leaves it
// false so a stream that never established still fails loudly.
type retriableMySQLError struct {
	err                 error
	hint                time.Duration
	txKilled            bool
	idleProgressTimeout bool
	// reshardPrimaryWindow marks the transient post-SwitchTraffic
	// "resharded shard's PRIMARY not routable yet" shape (item 72(b)).
	// The reshard-follow reopen reads this to keep the recovery pinned to
	// PRIMARY (reopening the reshard tail in-place) instead of letting the
	// generic ADR-0038 warm-resume take over — which would drop to the
	// ADR-0072 REPLICA default and bounce onto the freshly-resharded
	// shard's not-yet-settled replica. See reopenReshardWindow.
	reshardPrimaryWindow bool
}

func (e *retriableMySQLError) Error() string               { return e.err.Error() }
func (e *retriableMySQLError) Unwrap() error               { return e.err }
func (e *retriableMySQLError) Retriable() bool             { return true }
func (e *retriableMySQLError) RetryHint() time.Duration    { return e.hint }
func (e *retriableMySQLError) TransactionKilled() bool     { return e.txKilled }
func (e *retriableMySQLError) IsIdleProgressTimeout() bool { return e.idleProgressTimeout }

// isReshardPrimaryWindowError reports whether err is (or wraps) the transient
// post-SwitchTraffic primary-routable-window retriable shape (item 72(b)). The
// reshard-follow reopen uses it to keep the recovery on the PRIMARY-pinned
// reshard tail rather than escaping to the REPLICA-defaulting warm-resume.
func isReshardPrimaryWindowError(err error) bool {
	var re *retriableMySQLError
	return errors.As(err, &re) && re.reshardPrimaryWindow
}

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
	//
	// TERMINAL-CODE SHIELD (audit 2026-07-23 D0-3): when the chain carries
	// a structured *MySQLError, the server RESPONDED and the code alone
	// decides the classification — this block returns on EVERY structured
	// error, so the transport-text legs below are unreachable for it. A
	// server error's message routinely echoes row data, key values, and
	// (via the flush wrapper's `flush table %q` frame) table names, so
	// bare text matching over it can flip a terminal code retriable: the
	// audit's observed cell was a 1062 on a table named reparent_history
	// classifying RETRIABLE via the reparent fallback, whereupon the
	// ADR-0108 tolerate-1062-on-retry wart swallowed the retry's 1062 as
	// "rows already landed" — the whole batch silently absent at exit 0.
	// The only message consultation allowed here is a structured-code +
	// message AND-gate on a code whose semantics are message-dependent
	// (1105 vttablet framing + vtgate cluster-event sentences, 1290
	// read-only, Error 3 ENOSPC) — never a bare substring scan across all
	// codes. Pinned by [TestClassifyApplierError_TerminalCodeShield]'s
	// cross-product, which includes 1105 itself: under that code only the
	// FULL canonical sentences flip retriable, never the loose
	// [reparentRetriableSubstrings] tokens an echoed statement can carry.
	var mysqlErr *gomysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1213, 1205:
			// 1213 InnoDB deadlock + 1205 lock-wait-timeout — the
			// canonical "retry the transaction" InnoDB transients.
			return &retriableMySQLError{err: err}
		case 2006, 2013:
			// Connection-lost family (go-sql-driver client codes 2006
			// CR_SERVER_GONE_ERROR / 2013 CR_SERVER_LOST). vtgate/vttablet
			// surfaces a dropped tablet connection as a MySQL ERR packet
			// carrying errno 2013 — e.g. the live bug175-repro finding:
			//
			//   Error 2013 (HY000): target: <db>.-.primary: vttablet: rpc
			//   error: code = Canceled desc = EOF (errno 2013) (sqlstate
			//   HY000) (CallerID: ...): Sql: "insert ... into events ..."
			//
			// This is a transport-loss shape in the SAME class as
			// driver.ErrBadConn / gomysql.ErrInvalidConn / io.EOF /
			// "connection reset by peer" (all already retriable above): the
			// server/tablet connection died — typically a PlanetScale
			// non-Metal storage-grow reparent dropping the in-flight write —
			// and the cold-copy reparent-retry (ADR-0108) re-acquires a FRESH
			// connection on the next attempt, so retrying on it is correct.
			//
			// Why the existing branches MISS it (the classifier gap this
			// closes): the leading `desc = EOF` is TEXT inside a *MySQLError
			// message, NOT the io.EOF sentinel, so `errors.Is(err, io.EOF)`
			// does not match; the Number is 2013, not 1105, so the vttablet
			// gRPC-code branch (classifyVitessMessage) is never entered; and
			// the reparent text fallback ("not serving"/"reparent") does not
			// appear in this wording. Result pre-fix: a loud terminal abort
			// mid-copy (rc=1) even though the write path could ride it.
			//
			// This is keyed on the NUMBER (structured, unambiguous), so it is
			// ORTHOGONAL to the deliberate bare-`code = Canceled` client-cancel
			// exclusion (v0.99.94): that guard lives in the 1105 message-text
			// branch and is untouched here. A genuine client-side ctx cancel
			// surfaces as context.Canceled (a Go sentinel, NOT retriable) or
			// `code = Canceled desc = context canceled` under Number 1105 —
			// never as errno 2013 — so a clean shutdown still fails terminally.
			//
			// Bounded, never infinite: if the tablet genuinely died (e.g.
			// out-of-disk that never recovers), the ADR-0108 wall-clock
			// deadline (~30 min) exhausts and the copy fails LOUDLY, exactly
			// as it does today — this only gives the transient a chance to
			// clear first instead of aborting on the first drop.
			return &retriableMySQLError{err: err}
		case 1105:
			if classifyVitessMessage(mysqlErr.Message) {
				return &retriableMySQLError{
					err:      err,
					txKilled: isVitessTxKillerMessage(mysqlErr.Message),
				}
			}
			// VTGATE-emitted availability sentence (the 2026-07-28 live
			// PS-160 122 GB findings). vtgate answers a query for a shard
			// whose primary it cannot route to with `Error 1105 (HY000):
			// target: <ks>.<shard>.primary: <sentence>` — generated by
			// VTGATE, not by a tablet, so it carries NO `vttablet` tag and
			// classifyVitessMessage above (which requires that tag by
			// design) cannot match it. Pre-fix these fell to the
			// terminal-code shield below and aborted a 122 GB cold copy,
			// with NEITHER ADR-0108 bound (30m wall-clock / 100000
			// attempts) reached — while the vttablet-framed sibling in the
			// SAME reparent window was ridden out. The belt the ADR-0108
			// text legs were supposed to provide has been unreachable for
			// every structured *MySQLError since the shield landed.
			//
			// AND-gated on the FULL canonical sentences, deliberately NOT on
			// the loose [reparentRetriableSubstrings] tokens: a 1105 message
			// can carry an echoed statement (`... : Sql: "insert into
			// reparent_history ..."`), and a false-positive transient ARMS
			// the cold-copy tolerate-1062-on-retry wart on the next attempt
			// — the D0-3 silent-batch-skip chain the shield exists to
			// prevent. See [vtgateTransientSubstrings] for the derivation,
			// the rejected sentences, and why upstream's own constant list
			// was NOT a sufficient source.
			if isVtgateTransientMessage(mysqlErr.Message) || isVitessTransportLossMessage(mysqlErr.Message) {
				return &retriableMySQLError{err: err}
			}
		case 3:
			// Error 3 "Error writing file" — the ENOSPC face the
			// PS-320-v4 grow arc surfaced (vttablet wraps the OS errno-28
			// write failure under this generic code). Message-gated
			// AND-gate: only the disk-full wording is the grow transient;
			// an unrelated Error 3 stays terminal via the shield below.
			if isDiskFullSignal(err) {
				return &retriableMySQLError{err: err}
			}
		case 1021, 1114:
			// ER_DISK_FULL / ER_RECORD_FILE_FULL — target transiently out
			// of disk during a storage auto-grow (PS-320-v4/-v6); the
			// bounded retry rides the grow out. Code-only: these codes
			// mean disk-full unconditionally.
			return &retriableMySQLError{err: err}
		case 1290:
			// ER_OPTION_PREVENTS_STATEMENT is GENERIC ("running with the
			// %s option") — only the read-only variant is the grow/
			// reparent serving-transition transient (PS-320-v10).
			// Message-gated AND-gate; an unrelated 1290 stays terminal.
			if isReadOnlyTargetSignal(err) {
				return &retriableMySQLError{err: err}
			}
		case 1062:
			// Explicit non-retriable per ADR-0038 — reaches the
			// terminal-code shield's bare return below. (Pre-shield this
			// empty case fell THROUGH to the text legs, which is exactly
			// the D0-3 silent-batch-skip bug.)
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
		// Terminal-code shield: every structured code not explicitly
		// classified above is terminal — return verbatim WITHOUT entering
		// the text legs below (see the block comment above the errors.As).
		return err
	}

	// Transport-level transients that don't surface as a MySQLError but
	// do appear as raw error text from the driver or the connection
	// pool — reachable ONLY when no structured *MySQLError is present
	// (the terminal-code shield above returned for every structured
	// error), so the matched text is transport/driver framing, never a
	// server message echoing row data or table names.
	//
	// The generic network-shape vocabulary (POSIX + the Bug 199a/200
	// Windows winsock/`connectex:` dial wordings; "no such host" stays
	// deliberately terminal) lives in the shared
	// [nettransient.IsTransientShape] matcher (audit 2026-07-23
	// QUAL-1/G-9); [TestClassifyApplierError_NetTransientCorpusParity]
	// fails if this site ever drifts from the corpus.
	if nettransient.IsTransientShape(err) {
		return &retriableMySQLError{err: err}
	}

	// SITE-SPECIFIC extension — "not serving" / "reparent" (ADR-0108):
	// a target PRIMARY REPARENT (e.g. a PlanetScale non-Metal storage
	// auto-grow at the ~39 GB boundary) makes the in-flight tablet
	// briefly "not serving". The vttablet-framed shape (Error 1105
	// `code = Unavailable`) and the vtgate-emitted shape (1105 carrying a
	// canonical [vtgateTransientSubstrings] sentence) are
	// both caught in the switch above; this leg is the loose fallback for
	// a reparent that surfaces with NO structured *MySQLError at all —
	// so the matched text is transport/driver framing, never a server
	// message echoing row data.
	//
	// It is loose ON PURPOSE here and NARROW inside the switch: the
	// shield's whole point is that once the server responded, only a
	// full-sentence AND-gate may consult the message. Do NOT "unify"
	// these two by widening the structured side.
	// Case-insensitive: vtgate/vttablet wording varies in case across
	// versions. These phrases do not appear in healthy or
	// terminal-semantic errors. Server-state wording, not a network
	// shape — stays local, not in the shared corpus.
	if msg := err.Error(); msg != "" {
		if isVtgateTransientMessage(msg) {
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
	// non-Metal storage auto-grow (ADR-0108/0109). The STRUCTURED faces
	// (Error 3 + ENOSPC wording, ER_DISK_FULL 1021, ER_RECORD_FILE_FULL
	// 1114) are classified inside the *MySQLError switch above; this leg
	// catches the ENOSPC text arriving WITHOUT a structured MySQLError
	// (e.g. an OS-level write error surfaced as plain driver text). It is
	// TRANSIENT: the auto-grow adds space and the retry succeeds (the
	// v0.99.95 PS-320-v4 live finding — the copy rode ~8 min of
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
	// The structured face (Number 1290 + read-only wording) is classified
	// inside the *MySQLError switch above; this leg catches the read-only
	// wording arriving without a structured MySQLError.
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

// vtgateTransientSubstrings is the EXACT (lower-cased) set of VTGATE-EMITTED
// availability sentences that mark a structured Error 1105 as a target
// failover / tablet-discovery transient — the ONE message consultation the
// terminal-code shield allows for that code beyond the vttablet framing.
//
// These are vtgate's OWN errors, raised when it cannot route a query to a
// serving primary. They never carry the `vttablet` tag (no tablet answered),
// which is precisely why [classifyVitessMessage] — which requires that tag
// by design — cannot see them, and why they reached the shield and killed
// two separate 122 GB cold copies in the field.
//
// # Why a second, NARROWER set instead of reusing [reparentRetriableSubstrings]
//
// These literals are matched with a structured *MySQLError in hand, where
// the message can echo the offending statement (`... (CallerID: x): Sql:
// "insert into reparent_history ..."`). A loose "reparent" / "not serving"
// token there would let an echoed identifier or row value flip a terminal
// 1105 retriable, and a false-positive transient ARMS the cold-copy
// tolerate-1062-on-retry wart on the next attempt (see writeBatchedConn) —
// the D0-3 silent-batch-skip chain. Full sentences cannot be produced by an
// identifier echo. The looser set stays in force ONLY below the shield,
// where no server response exists.
// [TestClassifyApplierError_TerminalCodeShield] pins 1105 itself against the
// loose tokens so the two sets cannot be "unified".
//
// # Where to re-derive this set (BOTH sources are required)
//
// Ground truth is vitess.io/vitess@v0.24.2. The set is derived from two
// places, and the second one is the whole lesson:
//
//  1. `go/vt/vtgate/buffer/buffer.go:72-73` — the `buffer.ClusterEvents`
//     constants, raised as `Code_CLUSTER_EVENT` at
//     `go/vt/vtgate/tabletgateway.go:388` (resharding) and `:393`
//     (reparent). vtgate's own query buffer BUFFERS AND RETRIES this class,
//     which is upstream's statement that it is transient.
//
//  2. `go/vt/vtgate/tabletgateway.go` — the `Code_UNAVAILABLE` raises in the
//     SAME `withRetry` function, which are NOT in `buffer.ClusterEvents`:
//     `:400` inconsistent-state, `:406` no-healthy-tablet, `:422`
//     (`vterrors.VT14003`, code.go:121) no-connection-for-tablet; plus the
//     buffer's own `Code_UNAVAILABLE` sentinels at `buffer.go:47-49`, which
//     reach the client through the `WaitForFailoverEnd` wrap at
//     `tabletgateway.go:373` (vterrors.Wrapf → `msg + ": " + cause`, so the
//     sentinel text is present in the final message).
//
// Deriving ONLY from (1) is what shipped an incomplete set on 2026-07-28:
// upstream raises the same availability class OUTSIDE its own constant list,
// and the live rig found the gap within hours. Upstream's flaky test at
// `tabletgateway_flaky_test.go:349` pins the inconsistent-state and
// no-healthy-tablet sentences TOGETHER — "depending on whether the health
// check ticks before or after the buffering code, we might get different
// errors" — i.e. they are two faces of ONE race, and matching one without
// the other is arbitrary.
//
// # Deliberately REJECTED (the boundary was considered, not guessed)
//
//   - `tabletgateway.go:337` Code_INTERNAL "tabletGateway's query service can
//     only be used for non-transactional queries on replicas" — an internal
//     invariant violation, not an availability window. Never self-heals.
//   - `tabletgateway.go:349` Code_FAILED_PRECONDITION "requested tablet type
//     ... is not part of the allowed tablet types for this vtgate" — vtgate
//     configuration. Retrying can never clear it; failing loudly names the
//     fix.
//   - `tabletgateway.go:414` `vterrors.VT14002` Code_UNAVAILABLE "no available
//     connection" — availability, but three generic English words that a
//     migrated log/monitoring row can legitimately contain, so it fails the
//     echo-safety rule this set is built on. It is also raised ONLY when
//     `err == nil` ("do not override error from last attempt"), so whenever a
//     real availability failure occurred in the same pass, THAT error — one
//     this set does match — is the one preserved and returned.
//   - `buffer.go:50` contextCanceledError "context was canceled before
//     failover finished" — cancel-flavored, so it is excluded for the same
//     reason a bare `code = Canceled` is absent from
//     [vitessRetriableSubstrings] (v0.99.94): a CLIENT-side shutdown must
//     stay terminal.
//   - `tabletgateway.go:373` the wrap "failed to automatically buffer and
//     retry failed request during failover" — NOT matched even though it
//     denotes a real failover, because it wraps ALL FOUR buffer sentinels
//     INCLUDING the canceled one; matching it would silently defeat the
//     exclusion directly above. The three transient sentinels are matched by
//     their own sentences, which appear in that same wrapped text anyway.
//   - `buffer.go:74` `ClusterEventMoveTables` ("disallowed due to rule") — a
//     routing-rule denial during a MoveTables cutover. Far too generic a
//     phrase to match safely, and it does not self-heal the way a failover
//     window does.
//
// Multi-word sentences are trimmed to their stable, Vitess-specific portion
// (e.g. the reparent sentence is split into its two halves) so a reword of
// the variable tail still matches. The matcher lower-cases the error text
// before comparing, so these MUST be lower-case.
// [TestVtgateTransientSubstrings_PinDown] pins the literals; extend that
// test and this slice together.
//
// # Why this is a concatenation rather than a literal list (item 143)
//
// The groupings below — "(1)/(2) vtgate availability sentences" versus
// "(3) vtgate's own TRANSPORT-loss framing" — were comments describing a
// distinction the code did not hold. Item 143 needs that distinction as a
// VALUE (a trip's grow-gate log states which class it observed instead of
// asserting "likely a primary reparent"), so the two halves are now the
// declarations and this set is their union. RETRIABILITY IS UNCHANGED: the
// matcher still scans the whole set, and the concatenation preserves the
// original order literal-for-literal. See
// [vtgateServingTransitionSubstrings] / [vtgateTransportSubstrings] in
// grow_evidence.go, and [TestVtgateSubstringHalves_PartitionTheTransientSet],
// which fails if a literal is added to one half but not to the union — the
// drift this shape exists to make impossible.
//
// The per-entry provenance for each half stays with its own declaration; what
// follows is the derivation of the SET, which is the part that must not be
// lost:
//
//	(1) buffer.ClusterEvents — Code_CLUSTER_EVENT.
//	(2) tabletgateway.go withRetry — Code_UNAVAILABLE, plus the buffer.go
//	    sentinels reached via the WaitForFailoverEnd wrap.
//	(3) vtgate's own TRANSPORT-loss framing — the connection between vtgate
//	    and the tablet died mid-statement, which vtgate reports in its own
//	    wording rather than in vttablet's gRPC framing:
//
//	Error 1105 (HY000): internal: vtgate connection error
//	  (read: connection reset by peer)
//
// Provenance, stated because it differs from (1) and (2): (3) is
// FIELD-DERIVED (a 2026-08-04 report of a MariaDB→PlanetScale cold copy
// failing 5 times out of 5 on the same wide table, 90–130s in), not read off
// an upstream constant. The sentence above is the observed text. It is the
// SIBLING of the Number-2013 branch above, one wire framing over, and the miss
// has exactly the shape that branch's comment describes: the transport cause
// is TEXT inside a *MySQLError message, so errors.Is(err, io.EOF) cannot see
// it, and under Number 1105 only this substring set is consulted. sluice
// already retried the SAME underlying loss when vttablet framed it
// (`... vttablet: rpc error: code = Unavailable desc = connection reset by
// peer` matches via classifyVitessMessage) — so the two framings of one
// condition disagreed, and the one an operator actually hits on a cold copy
// was the terminal one.
//
// Echo-safety (the rule this set is built on): "vtgate connection error" is
// vtgate's own product noun, not three generic English words — it cannot
// plausibly appear in a migrated row the way "no available connection" could.
// The generic transport tails ("connection reset by peer", "unexpected EOF")
// are deliberately NOT matched on their own for exactly that reason; the
// vtgate framing is the anchor and it carries any tail.
var vtgateTransientSubstrings = append(
	append([]string{}, vtgateServingTransitionSubstrings...),
	vtgateTransportSubstrings...,
)

// vitessTransportNouns and vitessTransportLossTokens form the AND-gate that
// catches a Vitess-layer TRANSPORT loss whose exact sentence sluice does not
// know — the case [vtgateTransientSubstrings]'s literal list cannot cover.
//
// # Why an AND-gate rather than another literal
//
// The literal "vtgate connection error" came from a field report (2026-08-04)
// and is matched above. But it does NOT appear anywhere in upstream Vitess
// v0.24.2 — checked, not assumed — and the report's own text reads as two
// messages merged ("(read: connection reset by peer  /  unexpected EOF)"), so
// the verbatim wording is UNCONFIRMED. It is probably PlanetScale's edge
// proxy rather than vtgate itself; `aws.connect.psdb.cloud` is not raw vtgate.
//
// Anchoring a retry on one uncertain literal risks a fix that is simply
// INERT: the operator's copy still dies, and nothing says why. So the literal
// stays (it costs nothing if it never matches) and this gate covers the class
// it belongs to — a message that names a Vitess component AND carries a
// transport-loss phrase is a dropped connection whatever the surrounding
// prose, and retrying it on a fresh connection is right.
//
// # Echo-safety, which is why it is an AND and not an OR
//
// A 1105 message can carry an echoed statement (`... : Sql: "insert into
// ..."`), and a false transient is worse than a missed one here: it also arms
// the cold-copy tolerate-1062-on-retry wart on the next attempt. Requiring
// BOTH halves means migrated row data would have to contain a Vitess
// component noun *and* a transport phrase to false-positive. The tokens are
// deliberately transport-specific: a bare "EOF" is excluded because it is
// three characters that appear in ordinary text, while "unexpected eof" and
// "connection reset by peer" are not phrases business data carries.
var (
	vitessTransportNouns      = []string{"vtgate", "vttablet"}
	vitessTransportLossTokens = []string{
		"connection reset by peer",
		"unexpected eof",
		"broken pipe",
		"use of closed network connection",
		"no such host",
	}
)

// isVitessTransportLossMessage reports whether msg names a Vitess component
// AND carries a transport-loss phrase. See the vars above for the reasoning.
func isVitessTransportLossMessage(msg string) bool {
	lower := strings.ToLower(msg)
	named := false
	for _, n := range vitessTransportNouns {
		if strings.Contains(lower, n) {
			named = true
			break
		}
	}
	if !named {
		return false
	}
	for _, t := range vitessTransportLossTokens {
		if strings.Contains(lower, t) {
			return true
		}
	}
	return false
}

// isVtgateTransientMessage reports whether msg carries one of the canonical
// vtgate availability sentences ([vtgateTransientSubstrings]).
// Case-insensitive — vtgate/vttablet wording varies in case across versions.
func isVtgateTransientMessage(msg string) bool {
	lower := strings.ToLower(msg)
	for _, sub := range vtgateTransientSubstrings {
		if strings.Contains(lower, sub) {
			return true
		}
	}
	return false
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
	// GRACEFUL HTTP/2 DRAIN wrapped in the 1105 envelope (roadmap item 79,
	// sibling sweep). Vitess wraps the vtgate→vttablet gRPC status inside this
	// message, so the same server-side drain that killed the VStream READER
	// arrives here as `vttablet: rpc error: code = InvalidArgument desc = …
	// GOAWAY … ErrCode=NO_ERROR` on the APPLY path. InvalidArgument is not in
	// [vitessRetriableSubstrings] — correctly, since a genuinely malformed
	// statement must stay terminal — so without this the drain failed the
	// batch instead of retrying it.
	//
	// Deliberately NOT a [vitessRetriableSubstrings] entry: that list is a
	// bare substring scan, and the bare word "goaway" must never be retriable
	// (a GOAWAY carrying PROTOCOL_ERROR or ENHANCE_YOUR_CALM means the peer is
	// rejecting how we speak to it). The conjunction lives in nettransient, so
	// the reader and the applier cannot drift apart on what "graceful" means.
	return nettransient.IsGracefulGoAwayText(msg)
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
