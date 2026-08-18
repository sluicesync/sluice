//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Real-cluster end-to-end pin for the SNAPSHOT stream's COPY-pump join
// (2026-08-08 sibling sweep, the last openFinding on the pump-join roster).
// close() must join every COPY pump it launched BEFORE it closes s.conn, so
// no pump can Recv on the stream / reconnect on the client after the conn is
// torn down — and it must do so without deadlocking a pump parked in
// backpressure cond.Wait (which ctx-cancel cannot reach).
//
// The loop opens a snapshot stream against a real vttestserver, sets a tiny
// byte cap and lets the COPY pump park in backpressure (no consumer drains
// it), then Close()s MID-COPY 50+ times per config. Two failure shapes it
// catches locally, without -race:
//
//   - a HANG: Close in a goroutine with a timeout — if the pump parked in
//     cond.Wait is not woken+joined, Close blocks forever and the timeout
//     fires. This is the deadlock the join would otherwise expose.
//   - a MISSING JOIN: after Close returns, s.copyDone must already be closed
//     (the COPY pump's defer ran) and s.conn must be nil. A close that
//     returned before the pump exited leaves copyDone open.
//
// The DATA-RACE on s.conn itself (a straggling Recv/reconnect after the conn
// is closed) is graded by the CI `integration && vstream` leg under -race —
// this host runs CGO-off, so -race is CI-only. This test makes the window
// wide and frequent so that leg exercises it; it does not itself claim
// -race clean.
//
// Usage:
//
//	go test -tags='integration vstream' -v -count=1 -timeout=20m \
//	  -run 'TestVStream_SnapshotCloseJoinsCopyPumpsMidCopy' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestVStream_SnapshotCloseJoinsCopyPumpsMidCopy(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	// Two wide tables, each far larger than the tiny cap below, so the COPY
	// pump has rows in flight (and backpressures) throughout the mid-copy
	// close window.
	for _, tbl := range []string{"wc_a", "wc_b"} {
		applyVTTestSQL(t, mysqlDSN, fmt.Sprintf(`CREATE TABLE %s (
			id   BIGINT        NOT NULL,
			blob VARCHAR(4096) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, tbl))
	}
	const rowsPerTable = 400
	seedAutoShardWide(t, mysqlDSN, "wc_a", rowsPerTable)
	seedAutoShardWide(t, mysqlDSN, "wc_b", rowsPerTable)

	// Let vttestserver's async schema tracker pick the tables up before COPY
	// enumerates them.
	time.Sleep(3 * time.Second)

	baseDSN := func(extra string) string {
		return fmt.Sprintf(
			"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0%s",
			mysqlDSN, grpcEndpoint, extra,
		)
	}

	cases := []struct {
		name   string
		dsn    string
		tables []string
	}{
		{
			// One table → the single-stream copyPump path.
			name:   "single_stream_copyPump",
			dsn:    baseDSN(""),
			tables: []string{"wc_a"},
		},
		{
			// Two tables + K=2 → copyPumpAutoShardConcurrent: the ctx-cancel
			// waker plus K per-stream sub-goroutines, the richest goroutine
			// shape. (copyPumpAutoShard, the sequential multi-table path,
			// shares pumpOneTableCopy/enqueueRowLocked with these and is
			// additionally covered by the unit join pins.)
			name:   "concurrent_copyPumpAutoShardConcurrent",
			dsn:    baseDSN("&vstream_copy_table_parallelism=2"),
			tables: []string{"wc_a", "wc_b"},
		},
	}

	const iterations = 50
	eng := Engine{Flavor: FlavorPlanetScale}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < iterations; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)

				stream, err := eng.OpenSnapshotStreamForTables(ctx, tc.dsn, tc.tables)
				if err != nil {
					cancel()
					t.Fatalf("iter %d: OpenSnapshotStreamForTables: %v", i, err)
				}

				// Tiny cap + no consumer ⇒ the COPY pump fills the buffer and
				// parks in backpressure cond.Wait — the deadlock-prone park the
				// join must survive.
				if setter, ok := stream.Rows.(ir.MaxBufferBytesSetter); ok {
					setter.SetMaxBufferBytes(16 << 10) // 16 KiB
				} else {
					cancel()
					t.Fatal("snapshot Rows must implement ir.MaxBufferBytesSetter")
				}

				// Give the pump time to start copying and reach backpressure.
				time.Sleep(75 * time.Millisecond)

				snap := stream.Rows.(*vstreamSnapshotRows).snap

				// Close MID-COPY, in a goroutine so a failure to wake+join a
				// backpressured pump surfaces as a timeout rather than a hang.
				done := make(chan error, 1)
				go func() { done <- stream.Close() }()
				select {
				case cerr := <-done:
					if cerr != nil {
						cancel()
						t.Fatalf("iter %d: Close: %v", i, cerr)
					}
				case <-time.After(30 * time.Second):
					cancel()
					t.Fatalf("iter %d: Close hung — a COPY pump parked in backpressure was not woken and joined "+
						"(cancel()+cancelCopyForShutdown() must unwedge every park before joinPumps)", i)
				}

				// Post-join value assertions. Safe to read snap fields without a
				// lock: Close joined every COPY pump, so nothing touches them now.
				if snap.conn != nil {
					cancel()
					t.Fatalf("iter %d: Close left s.conn non-nil", i)
				}
				select {
				case <-snap.copyDone:
				default:
					cancel()
					t.Fatalf("iter %d: Close returned before the COPY pump exited (copyDone still open) — "+
						"the join did not wait for the pump", i)
				}

				cancel()
			}
		})
	}
}
