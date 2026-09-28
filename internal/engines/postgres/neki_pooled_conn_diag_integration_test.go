//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPostgresSuite_NekiPooledConnDiagApparatus proves the NEKI-NK306-DIAG
// instrument on an ordinary PostgreSQL, so the weekly Neki run is not what
// discovers that the instrument cannot tell its verdicts apart.
//
// # What it pins
//
//   - THE PREMISE the diagnosis is read against: a pooled connection whose
//     peer has gone silent fails with a BARE `context deadline exceeded`
//     (pgx's `ResetSession` ping consumes the deadline, then `database/sql`'s
//     retry returns the context error), while a statement that times out
//     WHILE RUNNING does not produce that spelling. Run 36347111193's control
//     INSERT reported the bare form; if a pgx or Go upgrade changes either
//     spelling, this fails rather than the next Sunday's log being misread.
//   - Every verdict [nekiDiagnosePooledConn] can reach from a dead pooled
//     connection is reachable and distinguishable: HEALTHY, HOLDS, REFUTED.
//
// # How a "dead" connection is made
//
// An in-process TCP proxy that can stop forwarding the bytes of the
// connections it already carries, while still serving new ones. That is the
// shape the Neki stall presented — one inherited connection silent, a fresh
// one fine — and it cannot be made by pausing the container, which this
// package shares across tests and which would silence every connection.
func TestPostgresSuite_NekiPooledConnDiagApparatus(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	applyDDL(t, dsn, `DROP TABLE IF EXISTS nk_diag_probe;
		CREATE TABLE nk_diag_probe (tenant_id int NOT NULL, id int NOT NULL, v text, PRIMARY KEY (tenant_id, id))`)

	proxy := newFreezableProxy(t, dsn)
	proxyDSN := proxy.dsn(t, dsn)

	pool, err := sql.Open("pgx", proxyDSN)
	if err != nil {
		t.Fatalf("open the pool: %v", err)
	}
	defer func() { _ = pool.Close() }()

	insertProbe := func(ctx context.Context, fresh *sql.DB) error {
		_, err := fresh.ExecContext(ctx, `INSERT INTO nk_diag_probe (tenant_id, id, v) VALUES (1, 90003, 'fresh')
			ON CONFLICT DO NOTHING`)
		return err
	}
	failingProbe := func(ctx context.Context, fresh *sql.DB) error {
		_, err := fresh.ExecContext(ctx, `INSERT INTO nk_diag_no_such_table VALUES (1)`)
		return err
	}

	// deadPooledConn seeds the pool with one idle connection, lets it idle
	// past pgx's one-second reset-ping threshold, and silences it.
	deadPooledConn := func() {
		t.Helper()
		if _, err := pool.ExecContext(ctx, `SELECT 1`); err != nil {
			t.Fatalf("seed the pool: %v", err)
		}
		time.Sleep(1100 * time.Millisecond)
		proxy.freezeExisting()
	}

	const pingBudget = 2 * time.Second

	t.Run("HEALTHY: a live pooled connection answers", func(t *testing.T) {
		d := nekiDiagnosePooledConn(ctx, t, pool, proxyDSN, pingBudget, insertProbe)
		if d.verdict != nekiPooledConnHealthy {
			t.Fatalf("verdict %q on a live pooled connection, want %q (pooled err: %v)",
				d.verdict, nekiPooledConnHealthy, d.pooledErr)
		}
	})

	t.Run("HOLDS: a silent pooled connection, a working fresh one", func(t *testing.T) {
		deadPooledConn()
		d := nekiDiagnosePooledConn(ctx, t, pool, proxyDSN, pingBudget, insertProbe)
		if d.verdict != nekiPooledConnHypothesisHolds {
			t.Fatalf("verdict %q, want %q (pooled step %q err %v; fresh err %v)",
				d.verdict, nekiPooledConnHypothesisHolds, d.pooledStep, d.pooledErr, d.freshErr)
		}
		// The premise: the hang is in ACQUIRE (the reset ping), and it reads
		// as the bare context error — the spelling run 36347111193 reported.
		if d.pooledStep != "acquire" {
			t.Errorf("the silent pooled connection failed at %q, want \"acquire\" — pgx's ResetSession "+
				"ping is no longer where a dead pooled connection surfaces", d.pooledStep)
		}
		if !nekiIsBareDeadline(d.pooledErr) {
			t.Errorf("the silent pooled connection failed with %q, not the bare %q — the spelling the "+
				"NK306 diagnosis is read against has changed", d.pooledErr, context.DeadlineExceeded)
		}
		if d.pooledElapsed < pingBudget*9/10 {
			t.Errorf("the pooled connection failed after %s, well inside its %s budget — it failed fast "+
				"rather than hanging, so the proxy did not reproduce a silent peer", d.pooledElapsed, pingBudget)
		}
	})

	t.Run("REFUTED: a silent pooled connection, and the fresh one fails too", func(t *testing.T) {
		deadPooledConn()
		d := nekiDiagnosePooledConn(ctx, t, pool, proxyDSN, pingBudget, failingProbe)
		if d.verdict != nekiPooledConnHypothesisRefuted {
			t.Fatalf("verdict %q, want %q (pooled err %v; fresh err %v)",
				d.verdict, nekiPooledConnHypothesisRefuted, d.pooledErr, d.freshErr)
		}
	})

	// The other direction of the premise: a statement that times out while
	// RUNNING on a live connection must NOT read as the bare context error,
	// or the bare spelling would not discriminate anything.
	t.Run("a statement that times out while running is NOT the bare spelling", func(t *testing.T) {
		direct, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("open direct: %v", err)
		}
		defer func() { _ = direct.Close() }()
		if err := direct.PingContext(ctx); err != nil {
			t.Fatalf("ping direct: %v", err)
		}
		sctx, scancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer scancel()
		_, err = direct.ExecContext(sctx, `SELECT pg_sleep(5)`)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("pg_sleep under a 500ms deadline returned %v, want a deadline error", err)
		}
		if nekiIsBareDeadline(err) {
			t.Fatalf("a statement that timed out while running reads as the bare %q — the bare spelling "+
				"no longer distinguishes a dead pooled connection from a slow statement", err)
		}
		t.Logf("a running statement's timeout reads: %v", err)
	})
}

// freezableProxy forwards TCP to one upstream and can silence the
// connections it already carries — dropping their bytes in both directions
// while keeping the sockets open — without affecting connections accepted
// afterwards.
type freezableProxy struct {
	ln       net.Listener
	upstream string

	mu     sync.Mutex
	frozen []*atomic.Bool
	conns  []net.Conn
}

func newFreezableProxy(t *testing.T, dsn string) *freezableProxy {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &freezableProxy{ln: ln, upstream: u.Host}
	t.Cleanup(p.close)
	go p.serve()
	return p
}

// dsn is the upstream DSN re-pointed at the proxy.
func (p *freezableProxy) dsn(t *testing.T, upstreamDSN string) string {
	t.Helper()
	u, err := url.Parse(upstreamDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.Host = p.ln.Addr().String()
	return u.String()
}

func (p *freezableProxy) serve() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		up, err := net.Dial("tcp", p.upstream)
		if err != nil {
			_ = client.Close()
			continue
		}
		frozen := &atomic.Bool{}
		p.mu.Lock()
		p.frozen = append(p.frozen, frozen)
		p.conns = append(p.conns, client, up)
		p.mu.Unlock()
		go forwardUnlessFrozen(up, client, frozen)
		go forwardUnlessFrozen(client, up, frozen)
	}
}

// freezeExisting silences every connection accepted so far.
func (p *freezableProxy) freezeExisting() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, f := range p.frozen {
		f.Store(true)
	}
}

func (p *freezableProxy) close() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
}

// forwardUnlessFrozen copies src to dst until src closes, discarding what it
// reads once frozen — the peer sees an open socket that never answers.
func forwardUnlessFrozen(dst io.Writer, src io.Reader, frozen *atomic.Bool) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 && !frozen.Load() {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}
