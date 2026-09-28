//go:build integration || nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// nekiNK306DiagMarker prefixes every line [nekiDiagnosePooledConn] logs, so a
// run's answer is one grep: `grep NEKI-NK306-DIAG`.
const nekiNK306DiagMarker = "NEKI-NK306-DIAG"

// The verdicts [nekiDiagnosePooledConn] can reach. Each is logged verbatim as
// `VERDICT=<value>`.
const (
	// The pooled connection answered its ping. The hypothesis was not
	// exercised: a stall later in the arm is NOT a dead pooled connection.
	nekiPooledConnHealthy = "HEALTHY"
	// The pooled connection did not answer, and a fresh connection ran the
	// probe. The dead-pooled-connection hypothesis holds.
	nekiPooledConnHypothesisHolds = "HOLDS"
	// The pooled connection did not answer, and a fresh connection reached
	// the server but the probe failed too. The stall is not the pooled
	// connection; the platform itself is not serving the statement.
	nekiPooledConnHypothesisRefuted = "REFUTED"
	// The pooled connection did not answer, and a fresh connection could not
	// be established either, so the two cannot be told apart.
	nekiPooledConnInconclusive = "INCONCLUSIVE"
)

// nekiPooledConnDiag is what [nekiDiagnosePooledConn] found, returned so the
// apparatus test can assert it; the live arm only logs it.
type nekiPooledConnDiag struct {
	verdict string
	// pooledStep is "acquire" or "ping": which step failed on the pooled
	// connection. Empty when it was healthy.
	pooledStep    string
	pooledErr     error
	pooledElapsed time.Duration
	freshErr      error
}

// nekiDiagnosePooledConn is Phase-A instrumentation for the NK306 arm's
// control-INSERT stall (runs 34932058458 and 36347111193). It answers one
// question: is the pooled connection the arm inherits from before the
// concurrent-COPY probe dead, while a fresh connection to the same router
// works?
//
// # Why the pooled-connection step is split into acquire and ping
//
// Taking a connection from the pool is NOT free of network traffic. pgx's
// stdlib `ResetSession` pings any connection that has been idle for more than
// a second, and on a failed ping returns `driver.ErrBadConn`; `database/sql`
// then discards it and retries — and the retry's first act is to check the
// context, which by then has expired. So on a connection whose peer has gone
// silent, the hang happens inside `pool.Conn`, and the error is a BARE
// `context deadline exceeded`. A statement that timed out while actually
// running reads `timeout: context deadline exceeded` (pgconn's `errTimeout`)
// instead. The bare form is what run 36347111193's control INSERT reported;
// [TestPostgresSuite_NekiPooledConnDiagApparatus] pins both spellings.
//
// # What it does NOT do
//
// It grades nothing. Every outcome is a log line. freshProbe's success is not
// evidence for the premise the calling arm tests, and the caller must not
// treat it as such.
//
// It is not side-effect free, and that is worth knowing when reading a run:
// on a dead pooled connection the acquire step makes `database/sql` DISCARD
// that connection, so the caller's next statement on the pool is likely to
// get a fresh one. A run where the diagnosis says HOLDS and the arm then
// passes is the expected shape, not a contradiction.
func nekiDiagnosePooledConn(ctx context.Context, t *testing.T, pool *sql.DB, dsn string,
	pingBudget time.Duration, freshProbe func(context.Context, *sql.DB) error,
) nekiPooledConnDiag {
	t.Helper()

	var d nekiPooledConnDiag
	pctx, cancel := context.WithTimeout(ctx, pingBudget)
	start := time.Now()
	conn, err := pool.Conn(pctx)
	d.pooledStep = "acquire"
	if err == nil {
		d.pooledStep = "ping"
		err = conn.PingContext(pctx)
		_ = conn.Close()
	}
	d.pooledElapsed = time.Since(start)
	cancel()

	if err == nil {
		d.pooledStep = ""
		d.verdict = nekiPooledConnHealthy
		t.Logf("%s: VERDICT=%s — the pooled connection answered acquire+ping in %s. The dead-pooled-connection "+
			"hypothesis was NOT exercised this run; if the statement below still stalls, the stall is not a "+
			"dead pooled connection.", nekiNK306DiagMarker, d.verdict, d.pooledElapsed.Round(time.Millisecond))
		return d
	}
	d.pooledErr = err
	t.Logf("%s: the pooled connection FAILED at %s after %s (budget %s): %v [bare-deadline=%t]",
		nekiNK306DiagMarker, d.pooledStep, d.pooledElapsed.Round(time.Millisecond), pingBudget, err,
		nekiIsBareDeadline(err))

	fresh, err := sql.Open("pgx", dsn)
	if err != nil {
		d.freshErr = err
		d.verdict = nekiPooledConnInconclusive
		t.Logf("%s: VERDICT=%s — could not open a fresh connection pool: %v", nekiNK306DiagMarker, d.verdict, err)
		return d
	}
	defer func() { _ = fresh.Close() }()
	fresh.SetMaxOpenConns(1)

	fctx, fcancel := context.WithTimeout(ctx, 30*time.Second)
	defer fcancel()

	if err := fresh.PingContext(fctx); err != nil {
		d.freshErr = err
		d.verdict = nekiPooledConnInconclusive
		t.Logf("%s: VERDICT=%s — a FRESH connection could not reach the server either (%v), so a dead pooled "+
			"connection cannot be told apart from an unreachable router", nekiNK306DiagMarker, d.verdict, err)
		return d
	}

	if backends, source, err := nekiReadBackends(fctx, fresh); err != nil {
		t.Logf("%s: backend census over the fresh connection failed: %v", nekiNK306DiagMarker, err)
	} else {
		t.Logf("%s: %s", nekiNK306DiagMarker, nekiRenderBackends(backends, source))
	}

	probeStart := time.Now()
	d.freshErr = freshProbe(fctx, fresh)
	probeElapsed := time.Since(probeStart).Round(time.Millisecond)
	if d.freshErr == nil {
		d.verdict = nekiPooledConnHypothesisHolds
		t.Logf("%s: VERDICT=%s — the pooled connection was dead and a FRESH connection ran the same statement "+
			"in %s. The stall is the inherited pooled connection, not the platform refusing the statement.",
			nekiNK306DiagMarker, d.verdict, probeElapsed)
		return d
	}
	d.verdict = nekiPooledConnHypothesisRefuted
	t.Logf("%s: VERDICT=%s — a FRESH connection reached the server but the statement failed there too after "+
		"%s: %v. The stall is not the pooled connection; the platform is not serving the statement.",
		nekiNK306DiagMarker, d.verdict, probeElapsed, d.freshErr)
	return d
}

// nekiIsBareDeadline reports whether err is exactly the bare context error:
// the spelling `database/sql` returns when a pooled connection's reset ping
// consumed the deadline, as opposed to pgconn's `timeout: …` for a statement
// that timed out while running.
func nekiIsBareDeadline(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) && err.Error() == context.DeadlineExceeded.Error()
}
