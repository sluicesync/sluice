//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// TestPostgresSuite_NekiCopyBurstApparatus proves the concurrent-COPY probe's
// APPARATUS on an ordinary PostgreSQL container, so a provisioned Neki cluster
// is never spent discovering that the probe does not do what it says.
//
// # Why this exists
//
// [nekiConcurrentCopyLimitHoldsOnTheCluster] has now had three apparatus
// defects, and every one of them was found by spending a cluster: a zero-byte
// COPY that may never have reached a shard; a serial release that burned 87 s
// on timeouts; and a four-second silence read as acceptance, which produced a
// confidently wrong "at least 12" across three paid runs while the platform's
// own message in the same logs said `limit: 4`. None of the three needed a
// router to find. They needed somebody to check that the mechanism behaved as
// described.
//
// # WHAT THIS REACHES, and what it cannot
//
// REACHES: that the burst is fully written and `burstDone` closes only after
// the last byte; that the COPY stays OPEN afterwards, holding its slot, until
// released; that releasing ends it cleanly and every burst row lands; that the
// teardown predicate the arm deletes with actually matches what the reader
// writes; and that [nekiCountRouterCopySessions] sees a held COPY through the
// activity view.
//
// CANNOT REACH: anything about refusal. A vanilla PostgreSQL has no
// concurrent-COPY ceiling, never emits `53300`, and does not defer a refusal
// past the client's next send — which is the exact behaviour the burst exists
// to outrun. Whether the burst is LONG ENOUGH is a question only a real router
// can answer, and the arm's deferred-refusal check is what asks it. This test
// proves the instrument works, not that it is calibrated.
func TestPostgresSuite_NekiCopyBurstApparatus(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const (
		table   = "nk_burst_probe"
		tenant  = 7
		firstID = 500000
	)
	applyDDL(t, dsn, fmt.Sprintf(
		`DROP TABLE IF EXISTS %s;
		 CREATE TABLE %s (tenant_id int NOT NULL, id int NOT NULL, v text, PRIMARY KEY (tenant_id, id))`,
		table, table,
	))

	// The session that holds the COPY. MaxOpenConns(1) mirrors the arm: the
	// connection is pinned to the COPY for its whole life.
	holder, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open the holder: %v", err)
	}
	defer func() { _ = holder.Close() }()
	holder.SetMaxOpenConns(1)

	// A second connection, because the holder cannot answer questions while it
	// is holding a COPY — which is itself part of what is being proven.
	observer, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open the observer: %v", err)
	}
	defer func() { _ = observer.Close() }()

	release := make(chan struct{})
	burstDone := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- holdOneCopy(ctx, holder, table, release, burstDone, tenant, firstID)
	}()

	// (1) The burst completes and SAYS SO. This is the signal the arm's
	// acceptance criterion is built on; if it never arrives, the arm falls
	// through to its burst deadline and reports every session inconclusive.
	select {
	case <-burstDone:
	case err := <-done:
		t.Fatalf("the COPY returned before its burst finished: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatalf("burstDone never closed: the reader wrote %d rows of %d bytes and never signalled. The live "+
			"arm's acceptance criterion would never be satisfied and every session would report inconclusive",
			nekiCopyBurstRows, nekiCopyBurstPayload)
	}

	// (2) And the COPY is STILL OPEN. A burst that ended the COPY would hold
	// no slot, and the arm would be measuring nothing at all — which is the
	// zero-byte cut's defect wearing different clothes.
	select {
	case err := <-done:
		t.Fatalf("the COPY ended as soon as its burst was written (%v) — it holds no slot afterwards, so the "+
			"arm would open twelve sessions that each occupy the cluster for an instant", err)
	case <-time.After(2 * time.Second):
	}

	// (3) An INDEPENDENT observer sees it. This is the same census the live arm
	// cross-checks its client-side count against; here it proves the query runs
	// and its marker matches a real held COPY. On this server it answers from
	// pg_stat_activity — the router half is ungated outside a cluster.
	running, withSidecars, note := nekiCountRouterCopySessions(t, observer, table)
	t.Logf("census while the COPY is held: %s", note)
	if running < 1 {
		t.Errorf("the activity census reports %d backend(s) running a COPY into %s while one is demonstrably "+
			"held. The live arm's independent cross-check would report zero against any client-side count, "+
			"which reads as a disagreement rather than as a broken query", running, table)
	}
	// The sidecar count must declare itself NOT APPLICABLE here. `extra` on the
	// vanilla view carries wait_event, which nearly every backend has, so a
	// count would report "this COPY reached a shard" on a server with no
	// shards — and that number is the tiebreaker the live arm's inconclusive
	// branch tells its reader to believe.
	if withSidecars != -1 {
		t.Errorf("the census reported a sidecar count of %d from pg_stat_activity, where the column it reads "+
			"is wait_event rather than sidecar detail. -1 is the only honest answer off the router view; a "+
			"number here would be counted as shard-reaching COPYs by the live arm's tiebreaker", withSidecars)
	}

	// (4) Release ends it cleanly, and every burst row LANDS. A burst that
	// silently dropped rows would still satisfy (1) and (2) while writing
	// something other than what the arm's teardown then has to delete.
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the COPY failed on release against a server with no concurrency ceiling: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the COPY did not finish within 60s of release")
	}

	var total, marked int
	if err := observer.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT count(*), count(*) FILTER (WHERE v LIKE '%s%%') FROM %s`,
			nekiCopyProbeMarker, table)).Scan(&total, &marked); err != nil {
		t.Fatalf("count the burst's rows: %v", err)
	}
	if total != nekiCopyBurstRows {
		t.Errorf("the burst landed %d rows, want %d — the reader is not writing what it claims", total, nekiCopyBurstRows)
	}
	// The teardown predicate, pinned. The arm deletes its rows with
	// `v LIKE '<marker>%'`; the burst appends a per-row suffix, so equality
	// (what the one-row cut used) would match none of them and leave every
	// probe row standing in the shared fixture for the backup arm to read.
	if marked != total {
		t.Errorf("%d of %d burst rows match the teardown predicate v LIKE '%s%%'. The arm's cleanup would "+
			"leave the rest standing in the shared fixture table, where the corpus and backup arms read",
			marked, total, nekiCopyProbeMarker)
	}
}
