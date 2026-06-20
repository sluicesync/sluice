// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// Bug (Track D, 2026-06-20): verifyPositionResumable passed the stream's
// unbounded context to the source verify queries (SHOW BINARY LOGS /
// GTID_SUBSET). On a half-dead source connection — one left in the pool by a
// prior broken pipe after a transaction-killer-induced restart — the query
// blocked on the TCP read FOREVER, hanging the whole stream (goroutine 1 stuck
// 302 minutes in verifyBinlogFilePresent [IO wait]) with the apply position
// frozen. The fix bounds the verify with a timeout that surfaces RETRIABLE
// (reconnect) — never ir.ErrPositionInvalid (which would force a destructive
// cold-start on a transient blip). These pins exercise that deadline path
// deterministically via a fake driver whose queries block until ctx is done.

// blockingConnector / blockingDriver back a *sql.DB whose every query blocks
// until the query's context is cancelled, then returns ctx.Err() — the exact
// shape of a wedged source connection (the read never completes on its own).
type blockingDriver struct{}

type blockingConn struct{}

func (blockingDriver) Open(string) (driver.Conn, error) { return blockingConn{}, nil }

func (blockingConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (blockingConn) Close() error                        { return nil }
func (blockingConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

// QueryContext blocks until ctx is done — modeling a source connection whose
// TCP read never returns (the verify query that hung Track D).
func (blockingConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

var registerBlockingOnce sync.Once

func newBlockingDB(t *testing.T) *sql.DB {
	t.Helper()
	registerBlockingOnce.Do(func() { sql.Register("sluice-blocking-test", blockingDriver{}) })
	db, err := sql.Open("sluice-blocking-test", "")
	if err != nil {
		t.Fatalf("open blocking db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

var _ io.Closer = (*sql.DB)(nil)

// TestVerifyPositionResumable_TimeoutIsRetriableNotPositionInvalid pins the
// core guarantee: when the bounded verify deadline fires (parent ctx still
// live), the error is RETRIABLE and is NOT ir.ErrPositionInvalid — for BOTH
// position modes (file/pos → SHOW BINARY LOGS, GTID → GTID_SUBSET).
func TestVerifyPositionResumable_TimeoutIsRetriableNotPositionInvalid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		pos  binlogPos
	}{
		{"file_pos", binlogPos{Mode: positionModeFilePos, File: "binlog.000123"}},
		{"gtid", binlogPos{Mode: positionModeGTID, GTIDSet: "uuid:1-100"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := &CDCReader{
				db:               newBlockingDB(t),
				posVerifyTimeout: 50 * time.Millisecond, // small so the deadline fires fast
			}
			start := time.Now()
			err := r.verifyPositionResumable(context.Background(), tc.pos)
			elapsed := time.Since(start)

			if err == nil {
				t.Fatal("want a (retriable) error on verify timeout; got nil — the verify hung or passed")
			}
			// Bounded: it returned near the injected timeout, not forever.
			if elapsed > 5*time.Second {
				t.Fatalf("verify did not honor the bounded timeout (took %s) — the hang regressed", elapsed)
			}
			// RETRIABLE so the streamer reconnects + retries.
			var re ir.RetriableError
			if !errors.As(err, &re) || !re.Retriable() {
				t.Fatalf("want a retriable error so the stream reconnects; got %T: %v", err, err)
			}
			// NEVER ir.ErrPositionInvalid — that would force a destructive
			// cold-start re-snapshot on a transient source blip.
			if errors.Is(err, ir.ErrPositionInvalid) {
				t.Fatalf("verify timeout must NOT be ir.ErrPositionInvalid (would trigger cold-start): %v", err)
			}
		})
	}
}

// TestVerifyPositionResumable_ParentCancelNotMisclassified pins that a genuine
// PARENT-context cancel (shutdown) during verify is NOT rewritten into the
// retriable "source unresponsive" wrapper — the guard requires the parent ctx
// to still be live for the reconnect path. A shutdown should surface the
// cancellation, not a spurious reconnect signal.
func TestVerifyPositionResumable_ParentCancelNotMisclassified(t *testing.T) {
	t.Parallel()
	r := &CDCReader{
		db:               newBlockingDB(t),
		posVerifyTimeout: time.Hour, // never our deadline; the parent cancel wins
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	err := r.verifyPositionResumable(ctx, binlogPos{Mode: positionModeFilePos, File: "binlog.000123"})
	if err == nil {
		t.Fatal("want an error when the parent context is cancelled mid-verify")
	}
	// The retriable "source unresponsive; reconnecting" wrapper must NOT be
	// applied on a real shutdown cancel (ctx.Err() != nil at the guard).
	if errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("parent cancel must not be ErrPositionInvalid: %v", err)
	}
}
