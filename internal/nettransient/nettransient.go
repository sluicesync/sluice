// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package nettransient is the single home of the transient
// network/transport error-shape vocabulary (audit 2026-07-23 QUAL-1 /
// gate G-9).
//
// Before this package the same "is this blip transient by
// construction?" decision was hand-mirrored across four sites — the
// trigger-CDC poll classifier (internal/engines/internal/triggercdc),
// the pipeline's connect-phase retry (isTransientNetworkShape), and
// both engine applier classifiers' transport-text legs — and the lists
// drifted one release after Bug 199: the Windows dial wordings
// (`connectex:` / "actively refused") added to the pipeline and the
// appliers never reached triggercdc, so a pgtrigger-source sync on
// Windows exited terminally on a routine managed-PG restart — the exact
// class the delta had just fixed twice, one file over ("fixed the
// representative, missed the siblings", Bug 199's own commit phrase).
//
// The contract:
//
//   - [IsTransientShape] is the ONE matcher. Every consumer delegates
//     to it for the generic transport-shape verdict; per-site checks
//     stay local ONLY when genuinely site-specific (MySQL reparent /
//     disk-full / read-only wordings, PG server-lifecycle wordings) —
//     never as a second copy of a generic network shape.
//   - [TextShapes] is the pinned corpus. Its exact content is pinned
//     byte-for-byte by TestTextShapes_PinDown, and each consumer
//     package carries a corpus-parity change-detector feeding every
//     corpus entry (and the structured sentinels) through its own
//     classifier — a site that stops delegating, or a corpus addition
//     a site's wrapper filters out, fails CI instead of drifting.
//   - Positive-match only: the DEFAULT is "not transient". Widening
//     the retry surface is a deliberate act that updates the corpus,
//     its pin, and every consumer's parity test in one diff.
//
// IMPORTANT ORDERING NOTE for the engine appliers: this matcher decides
// TRANSPORT shapes only, and it must run strictly AFTER the terminal-
// code shield (audit 2026-07-23 D0-3/D0-8) — a structured driver error
// (*gomysql.MySQLError / *pgconn.PgError) means the server RESPONDED,
// its message routinely echoes row data and table names, and the code
// alone decides the classification. [IsTransientShape] must never be
// consulted for an error chain carrying a structured server response.
// The separate [IsConnectionAvailabilitySQLState] predicate (Bug 203) is
// the structured companion for exactly those chains: it reads ONLY the
// SQLSTATE code — never message text — so the shield's data-echo hazard
// does not apply to it.
package nettransient

import (
	"errors"
	"io"
	"net"
	"strings"
	"syscall"
)

// TextShapes is the canonical lower-cased substring corpus for
// transient transport shapes that carry no reliable structured form
// through the driver/HTTP stacks. The matcher lower-cases the error
// text before comparing, so entries MUST be lower-case.
//
// Do NOT inline any of these strings in a consumer — extend this slice,
// its TestTextShapes_PinDown pin, and the consumers' parity tests
// together (they will fail loudly otherwise; that is their job).
//
// Deliberately EXCLUDED as terminal-by-design (pinned negative in every
// consumer's parity test):
//   - "no such host" — a wrong/typo'd endpoint is an operator error;
//     failing fast beats burning the retry budget on it. (The
//     explicitly-temporary DNS wording IS included below.)
//   - auth/permission ("Access denied", SASL/password failures), DSN
//     parse errors, coded sluice refusals (SLUICE-E-…), and decode
//     faults — retrying those masks a real misconfiguration.
var TextShapes = []string{
	// go-sql-driver's post-mortem for a pool conn the peer dropped; it
	// swallows the underlying cause, so no structured form survives
	// (the 2026-07-22 scale-soak incident's exact shape).
	"invalid connection",
	// stdlib net/http string with no reliable structured form (arrives
	// on a *url.Error whose Timeout() is not always set); the observed
	// 2026-07-22 D1 soak killer.
	"tls handshake timeout",
	// POSIX socket wordings.
	"connection reset by peer",
	"connection refused",
	"connection timed out",
	"broken pipe",
	"i/o timeout",
	// The server severed the stream mid-flight; pgx surfaces this as a
	// plain string, not always wrapping the io.ErrUnexpectedEOF
	// sentinel (live finding, item 38 re-validation 2026-06-23).
	"unexpected eof",
	// stdlib net.ErrClosed wording — a read/write raced a peer-side (or
	// pool-side) close; the next attempt draws a fresh conn.
	"use of closed network connection",
	// Windows winsock wordings — syscall.Errno equivalents exist but
	// driver wrapping routinely reduces them to text (Bug 199a: pgx v5
	// flattens its multi-host connect error, defeating the structural
	// errors.Is legs).
	"forcibly closed by the remote host",
	"wsarecv:",
	"wsasend:",
	// Windows dial-time refusal (Bug 199a/200): "connectex: No
	// connection could be made because the target machine actively
	// refused it." — the POSIX "connection refused" wording never
	// matches it, and the refused window is most of any target restart,
	// so without these the retry surface was effectively inert on
	// Windows for the canonical local transient.
	"connectex:",
	"actively refused",
	// net/http pool churn.
	"server closed idle connection",
	// The explicitly-TEMPORARY DNS failure (glibc wording) — unlike
	// "no such host", the resolver itself says to retry.
	"temporary failure in name resolution",
}

// IsTransientShape reports whether err is a network/transport shape
// that is transient by construction — the connection or TLS handshake
// failed, timed out, or was reset mid-flight. Positive-match only; the
// default is "not transient". nil → false.
//
// Structured checks come first (they survive wording changes); the
// [TextShapes] corpus covers shapes with no reliable structured form.
//
// Posture pin (audit 2026-07-23 ARCH-5): a wrapped
// context.DeadlineExceeded matches the net.Error leg (it implements
// Timeout() == true) and therefore classifies TRANSIENT. That inclusion
// is deliberate, not incidental: a per-dial/per-exec deadline against a
// briefly-down peer surfaces exactly as a wrapped ctx deadline (pgx
// connect_timeout, driver watchCancel), and refusing it would make a
// timeout-shaped outage terminal while its RST-shaped sibling retries.
// The cost on a genuine run-ctx deadline-driven shutdown is bounded to
// one misleading INFO line: every retry loop's backoff selects on
// ctx.Done() and exits with ctx.Err() immediately. A bare
// context.Canceled matches nothing here and stays terminal, so clean
// shutdowns are never absorbed. Pinned by
// TestIsTransientShape_DeadlineExceededPosture.
func IsTransientShape(err error) bool {
	if err == nil {
		return false
	}
	// Structured: stream/connection ended mid-flight.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	// Structured: socket-level resets / refusals / broken pipes.
	if errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}
	// Structured: any net.Error reporting itself as a timeout (covers
	// *url.Error, *net.OpError, dial/response-header deadlines, and —
	// see the posture note above — wrapped context.DeadlineExceeded).
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	// Text fallback for shapes with no reliable structured form.
	msg := strings.ToLower(err.Error())
	for _, s := range TextShapes {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// sqlStater is the structural surface a SQLSTATE-carrying driver error
// exposes — pgx's *pgconn.PgError implements it (SQLState() returns .Code).
// Matched structurally (errors.As onto the interface) so this package needs
// no driver import and the pipeline consumer stays engine-neutral; the
// go-sql-driver *MySQLError carries its SQLState as a field, not a method,
// so MySQL errors never match here (see the asymmetry note below).
type sqlStater interface {
	SQLState() string
}

// IsConnectionAvailabilitySQLState reports whether err carries a SQLSTATE
// from the Postgres CONNECTION-AVAILABILITY transient set — the structured
// shapes a connect/ping/read hits when the server is restarting, promoting,
// or dropping the connection server-side:
//
//   - 57P01 admin_shutdown / 57P02 crash_shutdown / 57P03
//     cannot_connect_now (the "database system is starting up" window a
//     managed-PG restart holds open for 10–60s) — the server comes back.
//   - class 08 connection_exception (08000/08003/08006/08007/08P01).
//
// This is the SINGLE HOME of that SQLSTATE set (Bug 203 — the QUAL-1
// vocabulary discipline): postgres.IsReadTransientSQLState delegates here,
// and the pipeline's connect-phase retry consults it directly for the
// structured leg its network-shape matcher deliberately lacks (a
// re-establish attempt landing in a restarting server's 57P03 window gets a
// structured PgError, no network shape matches, and pre-fix it exited
// terminal while the applier and trigger-CDC poll classifiers both retried
// the same shape). Everything else stays terminal by omission — notably
// auth (28000/28P01), invalid catalog (3D000), and undefined objects
// (42P01/42703), which are operator faults retrying would mask.
//
// MySQL asymmetry (ground-truthed 2026-07-23, docker restart of mysql:8.0
// with a 50ms connect probe): a restarting MySQL never presented a
// structured handshake errno — the listener simply is not up until InnoDB
// recovery completes, so the observed restart window surfaced entirely as
// transport shapes (every sample was the driver's "invalid connection";
// direct dials additionally yield the refused/i-o-timeout wordings), all of
// which [IsTransientShape] already classifies. There is no MySQL leg to add
// here; 1053 ER_SERVER_SHUTDOWN is a mid-session shape on established
// connections, not a connect-phase one, and adding it without connect-phase
// evidence would widen the retry surface on speculation.
func IsConnectionAvailabilitySQLState(err error) bool {
	if err == nil {
		return false
	}
	var st sqlStater
	if !errors.As(err, &st) {
		return false
	}
	switch code := st.SQLState(); code {
	case "57P01", "57P02", "57P03":
		return true
	default:
		return strings.HasPrefix(code, "08")
	}
}
