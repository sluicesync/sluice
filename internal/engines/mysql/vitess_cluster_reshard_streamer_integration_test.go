//go:build integration && vitessreshard

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0094 STREAMER-LEVEL reshard-follow end-to-end test.
//
// The sibling file vitess_cluster_reshard_integration_test.go proves
// the SAME reshard-follow behaviour at the READER level: it drives
// rdr.Reopen() from an in-test collector loop. This file proves the
// behaviour now works through the PRODUCTION pipeline.Streamer — i.e.
// that Streamer.applyWithReshardFollow (internal/pipeline/streamer.go)
// bridges the journal seam by calling the reader's ReopenAfterReshard
// (ir.ReshardReopener, implemented by *vstreamCDCReader) instead of
// exiting loudly with a terminal ShardLayoutChangedError.
//
// Topology / harness: reuses the multi-process vitess/lite cluster +
// the Reshard-workflow helpers from the reader-level file
// (startVitessReshardCluster, addTargetShards, waitReshardRunning,
// vtctldExec, vrApplySQL). Target is the shared MySQL container
// (newSharedDB) — same family as the source (vtgate speaks MySQL), so
// the production wiring runs as a same-engine MySQL stream:
//   Source = Engine{Flavor: FlavorPlanetScale}  (VStream CDC + snapshot)
//   Target = Engine{Flavor: FlavorVanilla}      (plain mysqld applier)
//
// Acyclic import note: this file is in package mysql and imports
// internal/pipeline. pipeline's PRODUCTION code never imports the mysql
// engine (only its *_test.go files do, via blank registration imports),
// so package mysql -> pipeline is acyclic for compilation. Verified by
// `go vet -tags='integration vitessreshard' ./internal/engines/mysql/`.
//
// THE LOAD-BEARING ASSERTION (re-run by the maintainer): after the
// reshard fires mid-stream and more rows are written on the NEW 2-shard
// layout, src COUNT == dst COUNT exactly (every pre- and post-reshard
// row present exactly once on the target — no gap, no dup across the
// seam) AND Streamer.Run did NOT return a terminal ShardLayoutChanged
// error. A silent partial or a terminal reshard exit FAILS LOUDLY here.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/pipeline"
)

// TestVitessReshard_StreamerFollowsReshardEndToEnd is the ADR-0094
// production-wiring proof. Sequence:
//
//  1. 1-shard keyspace `commerce`, table `ledger` (hash vindex on id).
//  2. Seed a baseline (cold-start COPY must land these on the target).
//  3. Start a real pipeline.Streamer (planetscale -> mysql) in a
//     goroutine: cold-start bulk-copy + VStream CDC.
//  4. Write rows mid-stream; let CDC replicate them.
//  5. RESHARD 1 -> 2 mid-stream (addTargetShards + Reshard create +
//     SwitchTraffic) while writes continue.
//  6. Write MORE rows on the new 2-shard layout; let them replicate.
//  7. Stop writes, drain, cancel ctx.
//
// Oracle: Run never returned a terminal ShardLayoutChangedError, and
// src COUNT == dst COUNT (exactly-once across the seam).
func TestVitessReshard_StreamerFollowsReshardEndToEnd(t *testing.T) {
	c := startVitessReshardCluster(t, "-")
	defer c.terminate()

	// --- source schema (1-shard, hash-vindexed) ---
	vrApplySQL(t, c.mysqlDSN, `CREATE TABLE ledger (
		id    BIGINT       NOT NULL,
		memo  VARCHAR(128) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	vrApplySQL(t, c.mysqlDSN, `ALTER VSCHEMA ON `+vrKeyspace+`.ledger ADD VINDEX hash(id) USING hash`)
	time.Sleep(3 * time.Second)

	// --- seed baseline ids 1..seedCount (cold-start COPY lands these) ---
	const seedCount = 40
	var seed strings.Builder
	seed.WriteString("INSERT INTO ledger (id, memo) VALUES ")
	for i := 1; i <= seedCount; i++ {
		if i > 1 {
			seed.WriteByte(',')
		}
		fmt.Fprintf(&seed, "(%d,'seed-%d')", i, i)
	}
	vrApplySQL(t, c.mysqlDSN+"&multiStatements=true", seed.String())
	time.Sleep(2 * time.Second)

	// --- target: shared MySQL (plain mysqld) ---
	targetDSN, cleanupTgt := newSharedDB(t, "reshard_streamer_target")
	defer cleanupTgt()

	// --- production Streamer: planetscale (VStream) -> mysql ---
	// auto_discover_shards lets the CDC reader pick up the post-reshard
	// 2-shard layout (the same DSN param the reader-level test uses).
	sourceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_auto_discover_shards=true",
		c.mysqlDSN, c.grpcAddr,
	)
	streamer := &pipeline.Streamer{
		Source:    Engine{Flavor: FlavorPlanetScale},
		Target:    Engine{Flavor: FlavorVanilla},
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "reshard-streamer-e2e",
		// Bounded retry so a transient post-SwitchTraffic stream blip
		// (target REPLICA still settling) is absorbed by the production
		// retry loop rather than failing the run — mirrors a real deploy.
		ApplyRetryAttempts: 8,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// failFast surfaces an early terminal Run exit (incl. a terminal
	// reshard error) as a precise failure instead of a generic timeout.
	failFast := func(stage string) {
		select {
		case e := <-runErr:
			var resh *ShardLayoutChangedError
			if errors.As(e, &resh) {
				t.Fatalf("%s: Streamer.Run exited with a TERMINAL ShardLayoutChangedError (it did NOT follow the reshard — ADR-0094 wiring regressed): %v", stage, e)
			}
			t.Fatalf("%s: Streamer.Run exited early: %v", stage, e)
		default:
		}
	}

	// Phase A: cold-start COPY lands the seed baseline on the target.
	if got := waitTargetCount(t, targetDSN, "ledger", seedCount, 120*time.Second); got != seedCount {
		failFast("phase A")
		t.Fatalf("phase A: cold-start COPY landed %d/%d seed rows on target", got, seedCount)
	}
	t.Logf("phase A OK: cold-start COPY landed %d seed rows", seedCount)

	// --- continuous writer: ids 1000.. at ~20/s on its own conn ---
	// committed is the oracle's source-of-truth: ids the writer actually
	// COMMITTED (a write rejected mid-SwitchTraffic is not recorded).
	committed := &committedSet{m: make(map[int64]string)}
	writerCtx, stopWriter := context.WithCancel(streamCtx)
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		db, derr := sql.Open("mysql", c.mysqlDSN)
		if derr != nil {
			t.Errorf("writer open: %v", derr)
			return
		}
		defer func() { _ = db.Close() }()
		id := int64(1000)
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-writerCtx.Done():
				return
			case <-tick.C:
				if _, e := db.ExecContext(writerCtx,
					"INSERT INTO ledger (id, memo) VALUES (?, ?)", id, fmt.Sprintf("w-%d", id)); e != nil {
					if errors.Is(e, context.Canceled) {
						return
					}
					// Mid-SwitchTraffic write rejection: that id was NOT
					// committed, so don't record it. Keep the same id and
					// retry on the next tick.
					continue
				}
				committed.add(id, fmt.Sprintf("w-%d", id))
				id++
			}
		}
	}()

	// Let a pre-reshard backlog build and replicate through CDC.
	time.Sleep(8 * time.Second)
	failFast("pre-reshard CDC")

	// --- RESHARD 1 -> 2 MID-STREAM (writes still flowing) ---
	c.addTargetShards(t, "-80", "80-")
	if out, rerr := c.vtctldExec(t, "Reshard", "create",
		"--workflow", "streamere2e", "--target-keyspace", vrKeyspace,
		"--source-shards", "-", "--target-shards", "-80,80-"); rerr != nil {
		t.Fatalf("Reshard create: %v\n%s", rerr, out)
	}
	c.waitReshardRunning(t, "streamere2e")
	if _, rerr := c.vtctldExec(t, "Reshard", "SwitchTraffic",
		"--workflow", "streamere2e", "--target-keyspace", vrKeyspace); rerr != nil {
		t.Fatalf("Reshard SwitchTraffic: %v", rerr)
	}
	t.Logf("RESHARD: SwitchTraffic completed 1 -> 2; vtgate shards now %v", vrShowShards(t, c.mysqlDSN))

	// The VStream JOURNAL fires asynchronously on the source-shard stream
	// after the cutover; the production Streamer's applyWithReshardFollow
	// must observe the clean channel close, call ReopenAfterReshard, and
	// continue on the new layout. Give writes a generous window on the
	// NEW 2-shard layout so the oracle covers ids committed strictly
	// AFTER the seam (the no-gap-across-the-cut property), with bounded
	// retry cycles for target REPLICA settling absorbed by the streamer.
	time.Sleep(45 * time.Second)
	failFast("post-reshard CDC")

	// --- stop writes, drain the tail ---
	stopWriter()
	writerWG.Wait()

	// Wait for the target to converge to the committed source count. The
	// post-reshard tail (incl. any in-flight streamer retry cycles) needs
	// time to drain; convergence is the load-bearing signal.
	srcIDs := committed.ids()
	wantTotal := seedCount + len(srcIDs)
	if len(srcIDs) < 20 {
		t.Fatalf("writer only committed %d rows across the reshard window; test is not exercising the cut (need >=20)", len(srcIDs))
	}

	got := waitTargetCount(t, targetDSN, "ledger", wantTotal, 120*time.Second)

	// Run must still be alive (did NOT exit on a terminal reshard error).
	select {
	case e := <-runErr:
		var resh *ShardLayoutChangedError
		if errors.As(e, &resh) {
			t.Fatalf("ORACLE FAIL: Streamer.Run returned a TERMINAL ShardLayoutChangedError — the Streamer did NOT follow the reshard (ADR-0094 production wiring broken): %v", e)
		}
		t.Fatalf("ORACLE FAIL: Streamer.Run exited before teardown: %v", e)
	default:
		// still running — followed the reshard. Good.
	}

	// --- ORACLE: src == dst exactly ---
	srcCount := vrCountLedger(t, c.mysqlDSN)
	if got != wantTotal {
		// Surface a precise gap/dup diagnosis against the target.
		t.Fatalf("ORACLE FAIL: target ledger COUNT=%d, want %d (seed=%d + committed=%d); src COUNT=%d. The Streamer did NOT bridge the reshard seam exactly-once (gap or dup across the cut).",
			got, wantTotal, seedCount, len(srcIDs), srcCount)
	}
	if srcCount != wantTotal {
		t.Fatalf("sanity: source ledger COUNT=%d != expected %d (seed=%d + committed=%d) — source itself lost/gained rows; test setup issue, not a streamer verdict",
			srcCount, wantTotal, seedCount, len(srcIDs))
	}
	t.Logf("ORACLE PASSED: Streamer followed the 1->2 reshard via ReopenAfterReshard (no terminal exit); src COUNT=%d == dst COUNT=%d (seed=%d + post-stream committed=%d) — exactly-once across the journal seam, no gap no dup.",
		srcCount, got, seedCount, len(srcIDs))

	// Clean teardown: cancel ctx, Run must return cleanly (nil / ctx).
	streamCancel()
	select {
	case e := <-runErr:
		if e != nil && !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded) {
			t.Fatalf("Streamer.Run returned a non-clean error on ctx cancel: %v", e)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Streamer.Run did not return within 30s after ctx cancel")
	}
}

// waitTargetCount polls the target MySQL table's COUNT(*) until it
// equals want or the deadline elapses, returning the last-seen count so
// the caller's assertion produces a precise gap/dup failure.
func waitTargetCount(t *testing.T, dsn, table string, want int, timeout time.Duration) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()

	deadline := time.Now().Add(timeout)
	last := -1
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		var n int
		qerr := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n)
		cancel()
		if qerr != nil {
			// target table may not exist until cold-start creates it.
			time.Sleep(2 * time.Second)
			continue
		}
		last = n
		if n == want {
			return n
		}
		time.Sleep(2 * time.Second)
	}
	return last
}

// vrCountLedger returns the source-side ledger COUNT(*) via vtgate,
// retrying the brief post-SwitchTraffic "no healthy tablet" window.
func vrCountLedger(t *testing.T, dsn string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = db.Close() }()
	deadline := time.Now().Add(60 * time.Second)
	last := -1
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		var n int
		qerr := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ledger").Scan(&n)
		cancel()
		if qerr != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		last = n
		return last
	}
	return last
}
