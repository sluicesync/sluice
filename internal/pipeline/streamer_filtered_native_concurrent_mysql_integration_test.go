//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 201 pin: `sync --where` on a NATIVE MySQL source with 2+ tables must
// cold-start through the ADR-0101 CONCURRENT cold-copy reader — which is the
// DEFAULT for a multi-table stream — with every table leg filtered on the
// source. Pre-fix, concurrentBinlogRows never implemented ir.RowFilterSetter,
// so migcore.ApplyRowFilters refused the whole sync at cold start (loud,
// zero-loss, but it blocked multi-table filtered sync on every self-managed
// MySQL source at default settings; the serial --copy-table-parallelism=1
// path worked). The pin matrix covers {N tables} × {some filtered}: two
// filtered tables with distinct predicates plus one unfiltered bystander,
// asserting per-table filter correctness (rows outside the predicate ABSENT,
// not just counts), that the CONCURRENT path actually engaged (a silent
// serial fallback would make this pin vacuous), and that the CDC leg keeps
// filtering after the handoff.
//
// Usage (Windows; see CLAUDE.md for the Rancher Desktop env):
//
//	$env:PATH += ";C:\Program Files\Rancher Desktop\resources\resources\win32\bin"
//	$env:TESTCONTAINERS_RYUK_DISABLED = "true"
//	go test -tags=integration -v -count=1 -timeout=20m \
//	  -run 'TestStreamer_FilteredNativeConcurrent' ./internal/pipeline/...

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

func TestStreamer_FilteredNativeConcurrentColdCopy_MySQL(t *testing.T) {
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	// startMySQLBinlog runs binlog-row-image=FULL — required by the filtered
	// CDC preflight (before-images drive client-side row-move evaluation).
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	// Two filtered tables with DISTINCT predicates + one unfiltered
	// bystander. Distinct in-scope counts so a cross-table filter mix-up
	// shows as a count mismatch.
	applyMySQLDDL(t, sourceDSN,
		"CREATE TABLE wf_low (id BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (id));")
	applyMySQLDDL(t, sourceDSN,
		"CREATE TABLE wf_mid (id BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (id));")
	applyMySQLDDL(t, sourceDSN,
		"CREATE TABLE wf_all (id BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (id));")
	seed := func(table string, n int) {
		var b []byte
		for i := 1; i <= n; i++ {
			b = append(b, []byte(fmt.Sprintf("INSERT INTO %s (id, v) VALUES (%d, %d);", table, i, i))...)
		}
		applyMySQLDDL(t, sourceDSN, string(b))
	}
	seed("wf_low", 200) // v > 100  → 100 rows in scope
	seed("wf_mid", 150) // v > 50   → 100 rows in scope (same count, different rows)
	seed("wf_all", 120) // unfiltered → all 120

	// Vacuous-green guard: the whole point is the CONCURRENT reader, so the
	// test must fail if the engine silently fell back to serial (which
	// implements the setter and would pass every data assertion).
	var sawConcurrent bool
	prevConc := concurrentCopyDispatchObserver
	concurrentCopyDispatchObserver = func(groups int) {
		if groups >= 2 {
			sawConcurrent = true
		}
	}
	defer func() { concurrentCopyDispatchObserver = prevConc }()

	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN + "&copy_table_parallelism=2",
		TargetDSN: targetDSN,
		StreamID:  "filtered-native-concurrent",
		RowFilters: map[string]string{
			"wf_low": "v > 100",
			"wf_mid": "v > 50",
		},
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// The pre-fix shape refused at cold start ("source engine \"mysql\" does
	// not support row-level filtering") — surface that immediately instead of
	// burning the row-count wait.
	select {
	case err := <-runErr:
		t.Fatalf("Run exited during cold start; the Bug 201 refusal shape is back: %v", err)
	case <-time.After(3 * time.Second):
	}

	waitFiltered := func(table string, want int) {
		t.Helper()
		if !waitForExactRowCountMySQL(targetDSN, table, want, 120*time.Second) {
			select {
			case err := <-runErr:
				t.Fatalf("table %q never reached %d rows (got %d); Run exited: %v",
					table, want, pollRowCountMySQL(targetDSN, table), err)
			default:
			}
			t.Fatalf("table %q never reached %d rows (got %d)",
				table, want, pollRowCountMySQL(targetDSN, table))
		}
	}
	waitFiltered("wf_low", 100)
	waitFiltered("wf_mid", 100)
	waitFiltered("wf_all", 120)

	// Per-table filter CORRECTNESS, not just counts: no out-of-predicate row
	// may exist on the target (a filter applied to the wrong table could
	// still produce a matching count).
	if n := countWhereMySQL(t, targetDSN, "wf_low", "v <= 100"); n != 0 {
		t.Errorf("wf_low carries %d rows outside its predicate (v <= 100); the wf_low leg copied unfiltered", n)
	}
	if n := countWhereMySQL(t, targetDSN, "wf_mid", "v <= 50"); n != 0 {
		t.Errorf("wf_mid carries %d rows outside its predicate (v <= 50); the wf_mid leg copied unfiltered", n)
	}

	// CDC keeps filtering after the handoff: an in-scope INSERT lands, an
	// out-of-scope INSERT stays absent.
	applyMySQLDDL(t, sourceDSN, "INSERT INTO wf_low (id, v) VALUES (9001, 500);")
	applyMySQLDDL(t, sourceDSN, "INSERT INTO wf_low (id, v) VALUES (9002, 1);")
	if !waitForExactRowCountMySQL(targetDSN, "wf_low", 101, 60*time.Second) {
		t.Fatalf("in-scope CDC INSERT never propagated; wf_low = %d, want 101",
			pollRowCountMySQL(targetDSN, "wf_low"))
	}
	if n := countWhereMySQL(t, targetDSN, "wf_low", "id = 9002"); n != 0 {
		t.Error("out-of-scope CDC INSERT (id 9002, v=1) landed on the target; the CDC leg stopped filtering")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}

	if !sawConcurrent {
		t.Error("the cold-start never engaged the concurrent (W >= 2) path — the Bug 201 pin is vacuous if the filtered sync silently fell back to serial")
	}
}

// countWhereMySQL returns COUNT(*) for table under predicate on dsn.
// Self-contained (opens its own DB), mirroring nativeWxDChecksum.
func countWhereMySQL(t *testing.T, dsn, table, predicate string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("mysql open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", table, predicate)
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatalf("mysql count %q: %v", table, err)
	}
	return n
}
