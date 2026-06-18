//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0102 end-to-end pin: native-MySQL → MySQL `sync start` cold-start with
// copy_table_parallelism=N (W = N concurrent read→write pipelines, ADR-0101)
// AND --copy-fanout-degree=D (each table's writes fanned across D plain-INSERT
// workers, ADR-0102) → W × D. Boots a real MySQL container (root → FTWRL
// available), seeds a multi-table DB with distinct sizable row counts plus a
// no-PK table, runs the cold-start sync, and asserts:
//
//   - the cold-start took the CONCURRENT path (the engine surfaced a partition
//     and runBulkCopyWithOpts engaged W pipelines — observed via the dispatch
//     seam, not timing);
//   - every PK'd table lands at its EXACT source count with matching content
//     checksum (no gap/dup across the W × D writers — the exactly-once
//     invariant, ADR-0102 §2);
//   - a NO-PK table is fully copied via the single-writer fallback (never
//     refused, never partially fanned out — ADR-0102 §5);
//   - CDC continues cleanly after the cold-start from the ONE recorded
//     position.
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
//	$env:TESTCONTAINERS_RYUK_DISABLED = "true"
//	go test -tags=integration -v -count=1 -timeout=20m \
//	  -run 'TestStreamer_NativeWxD' ./internal/pipeline/...

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// TestStreamer_NativeWxD_MySQL pins the full W × D native cold-copy: W = 2
// concurrent table pipelines, each fanning its table across D = 4 plain-INSERT
// workers, exactly-once across multiple PK'd tables + a no-PK fallback.
func TestStreamer_NativeWxD_MySQL(t *testing.T) {
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	src, tgt, cleanup := startMySQL(t)
	defer cleanup()

	// Four PK'd tables with distinct, sizable counts (so a mis-routed/dropped
	// row shows as a count mismatch, not a coincidental match) + one no-PK
	// table (the single-writer fallback).
	pkTables := map[string]int{"wd_a": 1200, "wd_b": 1700, "wd_c": 900, "wd_d": 1500}
	for tbl := range pkTables {
		applyMySQLDDL(t, src, fmt.Sprintf(
			"CREATE TABLE %s (id BIGINT NOT NULL, v VARCHAR(64) NOT NULL, PRIMARY KEY (id));", tbl,
		))
	}
	applyMySQLDDL(t, src,
		"CREATE TABLE wd_nopk (msg VARCHAR(64) NOT NULL);")

	// Seed rows in batches (multiStatements). Distinct content per row so the
	// checksum is meaningful.
	for tbl, n := range pkTables {
		var b []byte
		for i := 0; i < n; i++ {
			b = append(b, []byte(fmt.Sprintf("INSERT INTO %s (id, v) VALUES (%d, '%s-%d');", tbl, i, tbl, i))...)
		}
		applyMySQLDDL(t, src, string(b))
	}
	const noPKRows = 300
	{
		var b []byte
		for i := 0; i < noPKRows; i++ {
			b = append(b, []byte(fmt.Sprintf("INSERT INTO wd_nopk (msg) VALUES ('m-%d');", i))...)
		}
		applyMySQLDDL(t, src, string(b))
	}

	var sawConcurrent bool
	restore := setColdStartDispatchObserverForTest(func(fast bool) { _ = fast })
	defer restore()
	// The concurrent-vs-serial bulk dispatch is the load-bearing seam here.
	prevConc := concurrentCopyDispatchObserver
	concurrentCopyDispatchObserver = func(groups int) {
		if groups >= 2 {
			sawConcurrent = true
		}
	}
	defer func() { concurrentCopyDispatchObserver = prevConc }()

	streamer := &Streamer{
		Source: mysqlEng,
		Target: mysqlEng,
		// W = N: copy_table_parallelism is a source-DSN knob (ADR-0101 §1).
		SourceDSN: src + "&copy_table_parallelism=2",
		TargetDSN: tgt,
		StreamID:  "native-wxd-mysql",
		// D: the ADR-0097/0102 per-table write fan-out degree.
		CopyFanoutDegree: 4,
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Wait for every table to land at its exact source count.
	for tbl, want := range pkTables {
		if !waitForExactRowCountMySQL(tgt, tbl, want, 120*time.Second) {
			t.Fatalf("table %q never reached %d rows (got %d) — gap/dup across W × D writers?",
				tbl, want, pollRowCountMySQL(tgt, tbl))
		}
	}
	if !waitForExactRowCountMySQL(tgt, "wd_nopk", noPKRows, 120*time.Second) {
		t.Fatalf("no-PK table never reached %d rows (got %d) — single-writer fallback dropped rows?",
			noPKRows, pollRowCountMySQL(tgt, "wd_nopk"))
	}

	// Content checksum: the union of values per PK'd table must match source
	// exactly (a silent overwrite/swap that preserves count would slip a
	// count-only check).
	for tbl := range pkTables {
		srcSum := nativeWxDChecksum(t, src, tbl)
		tgtSum := nativeWxDChecksum(t, tgt, tbl)
		if srcSum != tgtSum {
			t.Errorf("table %q checksum mismatch: src=%d tgt=%d (W × D exactly-once violated)", tbl, srcSum, tgtSum)
		}
	}

	// CDC continues after the concurrent cold-start, from the ONE recorded
	// position.
	applyMySQLDDL(t, src, "INSERT INTO wd_a (id, v) VALUES (999999, 'post-snapshot');")
	if !waitForExactRowCountMySQL(tgt, "wd_a", pkTables["wd_a"]+1, 60*time.Second) {
		t.Fatalf("post-cold-start CDC INSERT never propagated; wd_a = %d, want %d",
			pollRowCountMySQL(tgt, "wd_a"), pkTables["wd_a"]+1)
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}

	if !sawConcurrent {
		t.Error("the cold-start never engaged the concurrent (W ≥ 2) path — copy_table_parallelism=2 should surface ≥2 groups (ADR-0101 dispatch regressed)")
	}
}

// nativeWxDChecksum returns a stable order-independent checksum over a
// table's (id, v) rows: SUM of CRC32 of each "id|v" pair. Order-independent
// (SUM) so it does not depend on which W × D worker wrote which row.
// Self-contained (opens its own DB) so it stays under the plain `integration`
// tag — the vstream-tagged mysqlScalar is unavailable here.
func nativeWxDChecksum(t *testing.T, dsn, table string) int64 {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("mysql open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var n int64
	q := fmt.Sprintf("SELECT COALESCE(SUM(CRC32(CONCAT(id, '|', v))), 0) FROM %s", table)
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatalf("mysql checksum %q: %v", table, err)
	}
	return n
}
