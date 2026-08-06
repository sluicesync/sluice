//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Item 146 / Bug 229 — the crux gate. A cold copy whose TARGET connection
// dies mid-`LOAD DATA` must fail LOUDLY, not hang.
//
// The reported defect: no error, no exit code, no log line, ~0.3s of CPU,
// zero rows, forever, with the goroutine parked in net.(*conn).Write under
// go-sql-driver's handleInFileRequest. Nothing in the unit suite can see
// that — it needs a real server, a real socket, and a peer that goes dark
// WITHOUT closing (a closed socket produces a clean error and was never the
// bug).
//
// The injection is a TCP proxy in front of the real mysqld. Per connection it
// forwards the handshake and the first slice of the LOAD DATA stream, then
// simply stops reading, holding both sockets open. That is the shape a
// vanished peer presents: no FIN, no RST, just a receive window that never
// opens again. Per CONNECTION rather than per proxy so the ADR-0108 retry can
// still re-acquire — otherwise the test would measure a reconnect hang
// instead of the write hang it is about (see the residual note below).
//
// Both shapes the roadmap entry names are covered, because they take
// different exits and only one of them was measured in the field:
//
//   - KEYLESS — the reported repro. The audit-B-9 carve-out refuses to replay
//     an ambiguous batch, so the deadline's error surfaces immediately as the
//     documented SLUICE-E-COPY-RETRY-AMBIGUOUS-KEYLESS.
//   - KEYED at bulk-parallelism 1 — the entry's "stalls identically" case.
//     Here the retry IS allowed, so the deadline's error is ridden until the
//     wall-clock budget is spent and then surfaces loudly naming the window.
//
// Each carries its own bound, because a test that reproduces a hang by
// hanging is not a test.
//
// # A residual this test found, recorded where it was found
//
// The deadline covers WRITES. If the peer also goes dark for READS — the
// proxy's original per-proxy form did exactly that — the retry's fresh
// connection then blocks in the handshake READ instead, with no deadline to
// bound it (see the netdeadline package doc for why the read side
// deliberately has none). Observed here directly: with a proxy-wide budget
// the first attempt failed on the write deadline in 5s, and the run then hung
// in the re-acquire. That residual is what the internal/pipeline copy stall
// WATCHDOG covers — a copy stuck in a handshake read makes no forward
// progress and is reported as such — and it is the concrete argument for
// having built both halves of item 146 rather than only the deadline.
//
// To run:
//   go test -tags=integration ./internal/engines/mysql/ -run WriteDeadline

package mysql

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func TestLoadDataWriteDeadline_DeadSocketFailsLoudly(t *testing.T) {
	cases := []struct {
		name string
		// pk drives the audit-B-9 replay-safety carve-out: a keyless table
		// refuses to replay, a keyed one rides the retry budget.
		pk bool
		// wantCode is the machine-readable code the surfaced error must
		// carry (empty = none expected), and wantIn a substring of its
		// prose. Together they prove WHICH loud exit was taken and not
		// merely that something failed. The code is checked through
		// sluicecode.FromError, not by substring: CodedError.Error()
		// delegates to the wrapped error, so the identifier an operator
		// greps for is in the envelope, never in the message.
		wantCode sluicecode.Code
		wantIn   string
		// oneAttempt marks the shape that takes exactly ONE flush attempt, so
		// its wall time is the write deadline itself and can be asserted
		// against it. That timing assertion is what BINDS the surfaced
		// failure to the deadline rather than to some other route: if the
		// error arrived by a reset, an EOF, or a keep-alive probe, it would
		// not track the deadline's value. (The transport-level "it is a
		// net.Error timeout" claim is pinned deterministically on a real
		// socket in internal/netdeadline; doing it here as well needs an
		// unblockable write, which loopback kernel buffers make unreliable —
		// an earlier attempt at this test measured the READ side by accident.)
		oneAttempt bool
	}{
		{
			name:       "keyless refuses immediately with the documented code",
			pk:         false,
			wantCode:   sluicecode.CodeCopyRetryAmbiguousKeyless,
			wantIn:     "keyless carve-out",
			oneAttempt: true,
		},
		{
			name:   "keyed rides the retry budget and then fails loudly",
			pk:     true,
			wantIn: "reparent-retry window",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsn, cleanup := startMySQL(t)
			defer cleanup()
			enableLocalInfile(t, dsn)

			table := "deadsocket_keyless"
			ddl := `CREATE TABLE deadsocket_keyless (id BIGINT NOT NULL, payload MEDIUMTEXT NOT NULL) ENGINE=InnoDB`
			if tc.pk {
				table = "deadsocket_keyed"
				ddl = `CREATE TABLE deadsocket_keyed (id BIGINT NOT NULL, payload MEDIUMTEXT NOT NULL,
					PRIMARY KEY (id)) ENGINE=InnoDB`
			}
			applyDDL(t, dsn, ddl)

			// Shorten the ADR-0108 wall-clock retry budget. 30 minutes is the
			// right production value (it must ride a prolonged reparent) and
			// the wrong test value; the KEYED case exists precisely to prove
			// the budget terminates, so it has to be reachable.
			origWall := coldCopyReparentMaxWallVar
			coldCopyReparentMaxWallVar = 20 * time.Second
			defer func() { coldCopyReparentMaxWallVar = origWall }()

			// Go dark after 64 KiB of client→server traffic ON EACH
			// CONNECTION: comfortably past the handshake and the session SETs,
			// and inside the LOAD DATA data stream.
			px := startBlackholeProxy(t, dsn, 64*1024)
			defer px.Close()

			// The DSN carries an explicit, short writeTimeout: it keeps the
			// test to seconds instead of the 10-minute production default AND
			// exercises the operator-override path on a REAL connection (the
			// unit pin only proves the parse).
			const deadline = 5 * time.Second
			proxyDSN := px.dsn(t, deadline)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			rw := openRowWriter(t, ctx, proxyDSN)
			defer func() {
				if c, ok := rw.(interface{ Close() error }); ok {
					_ = c.Close()
				}
			}()
			mustBeLoadData(t, rw)

			irTable := &ir.Table{
				Name: table,
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
					{Name: "payload", Type: ir.Text{}},
				},
			}
			if tc.pk {
				irTable.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}
			}

			// ~16 MiB of payload: far more than any socket send buffer, so
			// once the proxy stops reading the writer is guaranteed to park
			// in Write.
			const (
				rowCount = 1024
				rowBytes = 16 * 1024
			)
			blob := strings.Repeat("x", rowBytes)
			rows := make(chan ir.Row, 64)
			go func() {
				defer close(rows)
				for i := range rowCount {
					select {
					case rows <- ir.Row{"id": int64(i), "payload": blob}:
					case <-ctx.Done():
						return
					}
				}
			}()

			start := time.Now()
			done := make(chan error, 1)
			go func() { done <- rw.WriteRows(ctx, irTable, rows) }()

			select {
			case err := <-done:
				elapsed := time.Since(start)
				t.Logf("returned after %v", elapsed)
				if tc.oneAttempt {
					// One flush attempt, so the wall time IS the deadline (plus
					// the encode). Bounded on BOTH sides: too fast means
					// something other than the deadline surfaced it; too slow
					// means the deadline is not what bounded the write.
					if elapsed < deadline {
						t.Errorf("returned in %v, faster than the %v write deadline — the failure did not come from "+
							"the deadline, so this run does not prove the deadline works", elapsed, deadline)
					}
					if elapsed > 4*deadline {
						t.Errorf("returned in %v, far beyond the %v write deadline — the write is not being bounded by "+
							"it", elapsed, deadline)
					}
				}
				if err == nil {
					t.Fatal("WriteRows SUCCEEDED against a peer that stopped reading — the rows cannot have landed")
				}
				t.Logf("surfaced: %v", err)
				if !strings.Contains(err.Error(), tc.wantIn) {
					t.Errorf("error does not carry %q — the exit taken is not the one this case is about.\ngot: %v",
						tc.wantIn, err)
				}
				if tc.wantCode != "" {
					ce, ok := sluicecode.FromError(err)
					if !ok || ce.Code != tc.wantCode {
						t.Errorf("error carries code %v (present=%t), want %s — an operator matching on the documented "+
							"code, which is what the error-codes table tells them to do, would not have matched this",
							codeOf(ce, ok), ok, tc.wantCode)
					}
				}
				if px.forwarded() == 0 {
					t.Fatal("the proxy forwarded nothing; the injection never reached the LOAD DATA stream, so this run proves nothing")
				}
			case <-time.After(120 * time.Second):
				t.Fatalf("WriteRows did not return within 120s against a dead socket (write deadline %v, retry wall %v). "+
					"This is Bug 229: without a write deadline the driver blocks in net.(*conn).Write forever, and "+
					"item 114's retry never receives the error it would have ridden",
					deadline, coldCopyReparentMaxWallVar)
			}
		})
	}
}

// codeOf renders the extracted code for a failure message.
func codeOf(ce *sluicecode.CodedError, ok bool) sluicecode.Code {
	if !ok {
		return "<none>"
	}
	return ce.Code
}

// --- the blackhole proxy ---------------------------------------------

// blackholeProxy forwards each MySQL connection until afterBytes of that
// CONNECTION's client→server traffic have passed, then STOPS READING and
// holds both sockets open — no FIN, no RST. That is the distinction that
// matters: a closed socket surfaces a clean error and was never the bug.
//
// Per connection, not per proxy, so a reconnect still completes its handshake
// and the test measures the WRITE hang rather than a reconnect hang (see the
// residual note in this file's header).
type blackholeProxy struct {
	ln     net.Listener
	target string
	after  int64
	// upstream is the parsed real-server DSN; [blackholeProxy.dsn] clones it
	// and re-points Addr, so the test never has to reconstruct credentials.
	upstream *driver.Config

	total atomic.Int64

	mu    sync.Mutex
	conns []net.Conn
}

func startBlackholeProxy(t *testing.T, upstreamDSN string, afterBytes int64) *blackholeProxy {
	t.Helper()
	cfg, err := driver.ParseDSN(upstreamDSN)
	if err != nil {
		t.Fatalf("parse upstream DSN: %v", err)
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &blackholeProxy{ln: ln, target: cfg.Addr, after: afterBytes, upstream: cfg}
	go p.serve()
	return p
}

func (p *blackholeProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *blackholeProxy) handle(client net.Conn) {
	var d net.Dialer
	up, err := d.DialContext(context.Background(), "tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}
	p.track(client, up)

	var sent atomic.Int64

	// server → client: forwarded until this connection's blackhole trips,
	// then dropped on the floor so no error travels back to the client.
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, rerr := up.Read(buf)
			if n > 0 && sent.Load() < p.after {
				if _, werr := client.Write(buf[:n]); werr != nil {
					return
				}
			}
			if rerr != nil {
				return
			}
		}
	}()

	// client → server: the direction that goes dark. Once this connection's
	// budget is spent the loop RETURNS without closing anything, so nothing
	// ever drains this client socket again.
	buf := make([]byte, 32*1024)
	for sent.Load() < p.after {
		n, rerr := client.Read(buf)
		if n > 0 {
			if _, werr := up.Write(buf[:n]); werr != nil {
				return
			}
			sent.Add(int64(n))
			p.total.Add(int64(n))
		}
		if rerr != nil {
			return
		}
	}
}

func (p *blackholeProxy) track(cs ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns = append(p.conns, cs...)
}

func (p *blackholeProxy) forwarded() int64 { return p.total.Load() }

// dsn returns the upstream DSN re-pointed at the proxy, with an explicit
// write deadline.
func (p *blackholeProxy) dsn(t *testing.T, write time.Duration) string {
	t.Helper()
	cfg := p.upstream.Clone()
	cfg.Addr = p.ln.Addr().String()
	cfg.WriteTimeout = write
	return cfg.FormatDSN()
}

func (p *blackholeProxy) Close() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
}
