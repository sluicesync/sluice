// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package netdeadline centralises the SOCKET WRITE DEADLINE sluice applies
// to its database connections — the transport-layer complement to
// [sluicesync.dev/sluice/internal/netkeepalive].
//
// Why this exists (Bug 229 / roadmap item 146). A cold copy whose TARGET
// connection died mid-`LOAD DATA` never returned: no error, no exit code, no
// log line, ~0.3s of CPU, zero rows. The goroutine dump showed it parked in
// net.(*conn).Write under go-sql-driver's handleInFileRequest. A socket
// write with no deadline blocks for as long as the peer refuses to drain it,
// and a peer that is GONE — as opposed to one that closed — never drains and
// never sends a RST, so "as long as" is "forever".
//
// # Why keep-alive does not already cover this
//
// [netkeepalive] advertises a ~60s dead-peer bound (Idle + Interval*Count),
// which reads like it should have surfaced this. It does not, and the reason
// is the load-bearing premise of this package: TCP keep-alive probes are only
// sent on an IDLE connection. A connection with unacknowledged data in the
// send queue is not idle — the retransmission timer governs instead, and its
// budget is many minutes at best and unbounded against a peer that silently
// absorbs packets. A blocked Write is precisely the state keep-alive cannot
// see. The premise is bound to a test rather than asserted: the injected
// dead-socket integration pin asserts the surfaced error is an i/o TIMEOUT
// (this deadline) and not a connection reset (keep-alive) — so if the
// transport behaviour ever changed, the pin changes shape and fails.
//
// # What the deadline is, and what it deliberately is not
//
// It bounds ONE socket write: how long the peer may go without draining a
// single packet. It is NOT a statement timeout and NOT a whole-copy bound. A
// legitimately slow-but-live target keeps its receive window opening, so each
// packet completes and each subsequent packet is re-armed with a fresh
// deadline — a four-hour `LOAD DATA` of a huge segment against a loaded
// server is unaffected. That distinction is why a deadline is the right
// primitive here and a per-statement context deadline is the wrong one: the
// statement deadline would have to be longer than the slowest legitimate
// statement, which is unbounded, while the write deadline only has to be
// longer than the longest legitimate PAUSE.
//
// # The READ side is deliberately left alone
//
// A read deadline is a whole-query timeout in disguise: the client blocks in
// Read from the moment it sends a query until the server's first result
// packet, so any bound short enough to catch a dead peer is also short enough
// to kill a legitimate long-running COUNT(*), ALTER, or index build. sluice's
// read-side defenses are elsewhere and are named as such: the ADR-0109 §A
// source session `net_read_timeout`/`net_write_timeout` (server-side, 600s),
// TCP keep-alive on a genuinely idle socket, and — for the class no deadline
// can see — the idle-progress copy watchdog in internal/pipeline. That is a
// residual, stated here rather than implied.
package netdeadline

import (
	"context"
	"net"
	"sync"
	"time"

	"sluicesync.dev/sluice/internal/netkeepalive"
)

// Write is the per-socket-write deadline sluice installs on every database
// connection it opens. It bounds how long ONE write may block on a peer that
// is not draining, never how long a statement may take.
//
// 10 minutes, chosen against the longest LEGITIMATE non-draining pause sluice
// knows about rather than against the shortest useful detection time. A
// PlanetScale storage auto-grow blocks the target's writes for
// seconds-to-minutes under semi-sync (ADR-0109's premise, and the reason
// sourceReadSessionTimeoutSeconds is 600): during one, the target stops
// consuming the socket, the receive window closes, and sluice's in-flight
// write parks — a live, healthy, expected pause that must not be turned into
// a failure. 10 minutes is ~10× the documented worst case and matches the
// ADR-0109 source-side bound, so the two halves of the same stall are bounded
// on the same clock.
//
// Deliberately FINITE for the same reason ADR-0109's is: a genuinely dead
// target surfaces as a loud i/o timeout that the ADR-0108 retry path can
// classify and ride, instead of hanging forever.
//
// Zero-value-safe by construction (the v0.99.51 trap): a package constant,
// not a config field — there is no EnableX-defaulting-true to invert. The
// MySQL escape hatch is the driver's own `writeTimeout=` DSN parameter, which
// wins absolutely; Postgres has no DSN spelling for it, which is a recorded
// residual rather than an oversight (see the engine sweep in the item-146
// roadmap entry).
const Write = 10 * time.Minute

// dialFunc is the dial-hook signature shared by pgx/pgconn
// ([pgconn.Config.DialFunc]) and the go-mysql binlog syncer
// ([replication.BinlogSyncerConfig.Dialer]). It is spelled UNNAMED at every
// boundary below so the result assigns straight into either driver's own
// named type without a conversion. The MySQL query driver uses a narrower
// signature and its own [mysql.Config.WriteTimeout] field instead of this
// wrapper — see the engine roster in the item-146 sweep.
type dialFunc = func(ctx context.Context, network, addr string) (net.Conn, error)

// Dialer returns sluice's standard database dial hook — the shared TCP
// keep-alive policy ([netkeepalive.Dialer]) composed with this package's
// per-write deadline. The two are complements, not alternatives: keep-alive
// bounds a dead peer on an IDLE socket, the deadline bounds one on a socket
// with a write in flight, and a database connection meets both.
//
// This is the ONE spelling every socket-dialing connection funnel uses, so a
// funnel that grows a fifth site cannot half-compose the policy. The MySQL
// query driver is the deliberate exception and says so at its own dial
// registration.
func Dialer() dialFunc {
	return Dial(netkeepalive.Dialer().DialContext)
}

// Dial wraps a dial hook so every connection it returns enforces [Write] on
// each socket write.
func Dial(next dialFunc) dialFunc {
	return DialWith(next, Write)
}

// DialWith is [Dial] with an explicit deadline — the seam the tests use to
// exercise the wrapper without waiting ten minutes. A non-positive d returns
// next unchanged (no wrapper, no cost), so a caller that wants the
// pre-item-146 behaviour can express it.
func DialWith(next dialFunc, d time.Duration) dialFunc {
	if next == nil || d <= 0 {
		return next
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := next(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return WrapConn(c, d), nil
	}
}

// WrapConn returns c with a per-write deadline of d installed. A non-positive
// d (or a nil conn) returns the input untouched.
func WrapConn(c net.Conn, d time.Duration) net.Conn {
	if c == nil || d <= 0 {
		return c
	}
	return &deadlineConn{Conn: c, d: d}
}

// deadlineConn arms a write deadline before every Write.
//
// # Why it tracks the owner's deadline instead of just setting its own
//
// pgconn cancels an in-flight operation by calling SetDeadline(time.Now()) on
// the connection from its context-watcher goroutine, and restores it with the
// zero time afterwards. A wrapper that unconditionally armed now+d on the
// next Write could push that cancellation deadline back into the future and
// UN-cancel the operation — turning a hang-prevention measure into a
// hang-causing one. So every owner-set deadline is recorded, and the armed
// value is min(owner, now+d): the wrapper may only ever SHORTEN.
//
// The mutex serialises the record-and-arm pair against the owner's setters,
// which is what makes the two possible interleavings both correct: if the
// owner's cancel lands first we read its past deadline and arm the past; if
// our arm lands first the owner's pass-through immediately overwrites it with
// the past and the blocked Write unwinds. Never both-wrong, which an
// unsynchronised read-then-arm could be.
//
// Read is deliberately not intercepted — see the package doc's read-side note.
type deadlineConn struct {
	net.Conn

	d time.Duration

	mu sync.Mutex
	// ownerWrite is the write deadline the connection's OWNER (the driver)
	// last set, zero for "none". Read under mu by arm.
	ownerWrite time.Time
}

func (c *deadlineConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ownerWrite = t
	return c.Conn.SetDeadline(t)
}

func (c *deadlineConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ownerWrite = t
	return c.Conn.SetWriteDeadline(t)
}

func (c *deadlineConn) Write(b []byte) (int, error) {
	c.arm(time.Now())
	return c.Conn.Write(b)
}

// arm sets the effective write deadline for the write about to happen:
// now+d, unless the owner already asked for something sooner.
func (c *deadlineConn) arm(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	eff := now.Add(c.d)
	if !c.ownerWrite.IsZero() && c.ownerWrite.Before(eff) {
		eff = c.ownerWrite
	}
	// A SetWriteDeadline failure means the connection is already dead or
	// closed; the Write that follows reports that far better than a
	// swallowed error here would. Deliberately ignored, not unchecked.
	_ = c.Conn.SetWriteDeadline(eff)
}
