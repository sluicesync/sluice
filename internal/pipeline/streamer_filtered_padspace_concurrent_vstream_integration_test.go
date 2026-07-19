//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-07-19 T3 — the A0 client-side COPY fallback driven through the
// REAL pipeline glue (preflightRowFilters -> coldStart -> ApplyClientCopyFilter),
// concurrently, under -race.
//
// The cluster gate TestVitessClusterFilteredSyncPadSpaceFallback proves the
// fallback at the ENGINE level: it opens the VStream snapshot directly and calls
// SetClientCopyFilter by hand. It does NOT exercise the pipeline seam that wires
// a --where into that setter: preflightRowFilters classifies each predicate
// pad-safe/unsafe, reduces s.serverSideRowFilters, builds s.clientCopyFilter, and
// streamer_coldstart installs it via ApplyClientCopyFilter. That glue was
// unit-pinned piecewise (TestClientCopyFilter / TestServerSidePadSpaceDetection)
// but never driven end-to-end (audit 2026-07-19 T3), and the concurrent-copy path
// — vstream_copy_table_parallelism>1, so N copy goroutines invoke the SHARED keep
// closure at once — was clean only by happens-before code-read, not by an
// executed race test.
//
// This test drives a filtered sync whose --where lands on a PAD-SPACE-collation
// column across TWO tables with copy parallelism 2, via the actual Streamer.Run
// cold-start, against a real MySQL target. It asserts the trailing-space in-scope
// row survives on BOTH tables (the fallback ran for every pad-unsafe table, not
// just one) and the out-of-scope row is dropped. Run under -race (the CI vstream
// shard) it also proves the shared keep closure is invoked race-free from the
// parallel copy goroutines.
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
//	$env:TESTCONTAINERS_RYUK_DISABLED = "true"
//	go test -tags='integration vstream' -v -count=1 -timeout=20m \
//	  -run 'TestStreamer_FilteredColdStart_PadSpace_ConcurrentCopy' ./internal/pipeline/...

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_FilteredColdStart_PadSpace_ConcurrentCopy_VStream is the audit
// 2026-07-19 T3 gate (see file header).
func TestStreamer_FilteredColdStart_PadSpace_ConcurrentCopy_VStream(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, "commerce", 1)
	defer cleanupSrc()
	targetDSN, cleanupTgt := startMySQLTarget(t)
	defer cleanupTgt()

	tables := []string{"orders_eu", "accounts_eu"}
	// Each table's region column is a legacy PAD-SPACE collation
	// (utf8mb4_general_ci): its `=` ignores trailing spaces, which the VStream
	// server-side filter does NOT (NO-PAD) — so BOTH tables must take the A0
	// client-side COPY fallback.
	for _, tbl := range tables {
		applySQL(t, mysqlDSN, fmt.Sprintf(`CREATE TABLE %s (
			id     BIGINT      NOT NULL AUTO_INCREMENT,
			region VARCHAR(8)  CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
			body   VARCHAR(64) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, tbl))
		// id=2 stores 'EU ' WITH a trailing space: in scope under the source's
		// PAD-SPACE region='EU', dropped by the NO-PAD server filter, and kept
		// only if the PAD-faithful client fallback ran for THIS table. id=3
		// ('US') is genuinely out of scope and must be dropped.
		applySQL(t, mysqlDSN+"&multiStatements=true", fmt.Sprintf(`
			INSERT INTO %s (id, region, body) VALUES
				(1, 'EU',  'in-scope-plain'),
				(2, 'EU ', 'in-scope-trailing'),
				(3, 'US',  'out-of-scope')`, tbl))
	}
	time.Sleep(3 * time.Second) // let the async schema tracker see the tables

	srcEng, ok := engines.Get("planetscale")
	if !ok {
		t.Fatal("source engine \"planetscale\" not registered")
	}
	tgtEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("target engine \"mysql\" not registered")
	}

	// vstream_copy_table_parallelism=2 → the cold-start copies BOTH tables
	// concurrently, so the shared clientCopyFilter keep closure is invoked from
	// two goroutines at once (the -race lever).
	sourceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0&vstream_copy_table_parallelism=2",
		mysqlDSN, grpcEndpoint,
	)
	streamer := &Streamer{
		Source:    srcEng,
		Target:    tgtEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "t3-padspace-concurrent",
		// The --where on the PAD-SPACE region column of BOTH tables. Driving
		// this through Run exercises preflightRowFilters' real pad-split +
		// clientCopyFilter install, not a hand-set filter.
		RowFilters: map[string]string{
			"orders_eu":   "region = 'EU'",
			"accounts_eu": "region = 'EU'",
		},
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	tgtDB, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	// Both in-scope rows (id 1 + the trailing-space id 2) must land on EACH
	// table, and the out-of-scope id 3 must be dropped — i.e. the stable id set
	// is exactly {1,2} per table. If the fallback failed to install for a table
	// the trailing-space id 2 would be missing ({1}); if it were bypassed
	// entirely the out-of-scope id 3 would leak ({1,2,3}).
	for _, tbl := range tables {
		// Wait for the trailing-space survivor to arrive (the copy for this
		// table has then produced its in-scope rows), then settle and assert the
		// exact set — id 3 copies AFTER id 2 (PK order), so a leak would be
		// visible within the settle window.
		if !waitForExactRowCountMySQL(targetDSN, tbl, 2, 120*time.Second) {
			select {
			case e := <-runErr:
				t.Fatalf("%s: never reached 2 kept rows; Run returned: %v; ids=%v", tbl, e, targetIDSet(t, tgtDB, tbl))
			default:
			}
			t.Fatalf("%s: never reached 2 kept rows (fallback dropped the trailing-space id 2?); ids=%v", tbl, targetIDSet(t, tgtDB, tbl))
		}
		time.Sleep(2 * time.Second) // settle: a leaked id 3 would arrive here
		got := targetIDSet(t, tgtDB, tbl)
		if len(got) != 2 || got[0] != 1 || got[1] != 2 {
			t.Fatalf("%s: kept id set = %v; want [1 2] (id 2='EU ' must survive the PAD-faithful fallback; id 3='US' must be dropped)", tbl, got)
		}
		t.Logf("%s: A0 pipeline-glue fallback PASS — trailing-space id 2 survived, out-of-scope id 3 dropped (ids=%v)", tbl, got)
	}

	cancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_FilteredColdStart_FloatSingleRefused_VStream is the audit
// 2026-07-19 SL1 gate: a filtered `sync --where` on a VStream source whose
// predicate rides a single-precision FLOAT ordering term on a PAD-SPACE-forced
// table must be REFUSED loudly at sync-start, not silently mis-filtered.
//
// The pad-space `region` column forces the whole table through the client-side
// COPY fallback; its cold-start COPY carries a single-precision FLOAT
// display-rounded by mysqld's float->text formatter (the exact re-read repair
// runs AFTER copy, so the keep predicate would see the lossy value). A FLOAT
// stored 0.1 is 0.10000000149 at the source so `amount > 0.1` KEEPS the row, but
// the rounded "0.1" carrier compares 0.1 > 0.1 = false and would DROP it — a
// source-in-scope row silently absent after cold-start. sluice refuses instead.
func TestStreamer_FilteredColdStart_FloatSingleRefused_VStream(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, "commerce", 1)
	defer cleanupSrc()
	targetDSN, cleanupTgt := startMySQLTarget(t)
	defer cleanupTgt()

	applySQL(t, mysqlDSN, `CREATE TABLE orders_f (
		id     BIGINT      NOT NULL AUTO_INCREMENT,
		region VARCHAR(8)  CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
		amount FLOAT       NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	// A boundary FLOAT that would mis-classify under the rounded carrier.
	applySQL(t, mysqlDSN, `INSERT INTO orders_f (id, region, amount) VALUES (1, 'EU', 0.1)`)
	time.Sleep(3 * time.Second)

	srcEng, ok := engines.Get("planetscale")
	if !ok {
		t.Fatal("source engine \"planetscale\" not registered")
	}
	tgtEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("target engine \"mysql\" not registered")
	}
	sourceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)
	streamer := &Streamer{
		Source:    srcEng,
		Target:    tgtEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "t-sl1-float-refuse",
		// PAD-SPACE region forces the client-copy fallback; amount is the
		// single-precision FLOAT ordering term the fallback can't filter faithfully.
		RowFilters: map[string]string{"orders_f": "region = 'EU' AND amount > 0.1"},
	}

	// The refusal fires at preflight (before any copy), so Run returns promptly;
	// the timeout only guards a regression that fails to refuse and proceeds.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	err := streamer.Run(ctx)
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeWhereCDCUnsupportedPredicate {
		t.Fatalf("Run() = %v; want coded %s (SL1 single-precision FLOAT refusal)", err, sluicecode.CodeWhereCDCUnsupportedPredicate)
	}
	t.Logf("SL1 refusal PASS: %v", err)
}

// targetIDSet returns the sorted id column of a target table (empty on error —
// the caller asserts, so a transient read error surfaces as a set mismatch, not
// a panic).
func targetIDSet(t *testing.T, db *sql.DB, table string) []int {
	t.Helper()
	rows, err := db.Query("SELECT id FROM " + table + " ORDER BY id")
	if err != nil {
		return nil
	}
	defer func() { _ = rows.Close() }()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return ids
		}
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}
