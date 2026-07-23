//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0106 (item 31): fast-by-default CDC apply. These end-to-end pins
// validate the streamer-level default-resolution at runtime against a real
// Postgres target (the engine WITH a connection-slot probe, so the auto:N
// budget-bound path is exercised, not just the unit table):
//
//   - The DEFAULT (ApplyConcurrency unset) ENGAGES concurrency end-to-end
//     without any operator action — the per-lane AIMD INFO line fires, which
//     happens only when the resolved W > 1 AND the applier wired the lane
//     pool. (The blast-radius bound: a test stub applier with no setter would
//     NOT log it — proven at the unit layer.)
//   - The DEFAULT and an explicit serial (`--apply-concurrency 1`) run
//     converge BYTE-IDENTICAL on the same source workload (the differential
//     the ADR mandates run under the new default).
//
// SHARD ROUTING: the TestStreamer_ prefix rides the streamer CI shard.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// laneEngagedLog is the INFO line attachLaneAIMDControllers emits when the
// concurrent key-hash apply path engages with per-lane AIMD. Its presence is
// the runtime proof that the resolved apply-concurrency was > 1 AND the
// applier wired the dedicated lane pool.
const laneEngagedLog = "per-lane AIMD apply-batch-size controllers engaged"

// TestStreamer_ApplyConcurrencyDefault_EngagesAndConvergesWithSerial drives
// two same-engine Postgres → Postgres streams over the SAME source workload:
// one with ApplyConcurrency UNSET (the ADR-0106 default → auto:N) and one with
// ApplyConcurrency=1 (explicit serial). It asserts the default engaged
// concurrency (the lane INFO fired) and both targets converge byte-identically
// to the source.
func TestStreamer_ApplyConcurrencyDefault_EngagesAndConvergesWithSerial(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	// Two source tables — one per stream — so the two runs never share a slot,
	// stream-id, or target table. REPLICA IDENTITY FULL so UPDATE/DELETE carry
	// a full before-image (the keyed apply path).
	const seedDDL = `
		CREATE TABLE acc_default (
			id    BIGINT PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			n     INT NOT NULL
		);
		ALTER TABLE acc_default REPLICA IDENTITY FULL;
		CREATE TABLE acc_serial (
			id    BIGINT PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			n     INT NOT NULL
		);
		ALTER TABLE acc_serial REPLICA IDENTITY FULL;
	`
	applyDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const totalRows = 300

	// runStream starts a Streamer over one table, waits for the slot, applies a
	// deterministic INSERT+UPDATE workload to that table on the source, waits
	// for convergence, then cancels. Returns the captured slog so the caller can
	// assert on engagement. applyConcurrency is passed VERBATIM to the field:
	// 0 = the unset default (auto:N), 1 = explicit serial.
	runStream := func(t *testing.T, table, streamID string, applyConcurrency int) fmt.Stringer {
		t.Helper()
		logs := captureSlog(t)

		streamer := &Streamer{
			Source:    pgEng,
			Target:    pgEng,
			SourceDSN: sourceDSN,
			TargetDSN: targetDSN,
			StreamID:  streamID,
			SlotName:  "slot_" + streamID,
			// Per-leg publication (the ADR-0175 remediation): the two legs
			// run SEQUENTIALLY with disjoint table scopes, and leg 1's slot
			// remains (inactive) after its ctx cancel — under the guard's
			// existence semantics (2026-07-23) leg 2's narrowing SET TABLE
			// on a SHARED publication is a refusal, by design. Isolating
			// per leg models what the refusal tells a real operator to do.
			PublicationName:  "pub_" + streamID,
			Filter:           migcore.TableFilter{Include: []string{table}},
			ApplyConcurrency: applyConcurrency,
			// AutoTune + ApplyBatchSize>1 so the per-lane controllers (and the
			// engagement INFO) are constructed when W>1; convergence is
			// independent of the exact size.
			AutoTune:       true,
			ApplyBatchSize: 50,
		}

		streamCtx, streamCancel := context.WithCancel(context.Background())
		defer streamCancel()

		runErr := make(chan error, 1)
		go func() { runErr <- streamer.Run(streamCtx) }()

		waitForSourceSlotWatching(t, sourceDSN, 120*time.Second, runErr, logs)

		srcDB, err := sql.Open("pgx", sourceDSN)
		if err != nil {
			t.Fatalf("open source: %v", err)
		}
		defer func() { _ = srcDB.Close() }()

		// Deterministic workload: insert N rows, then update the even ids. Same
		// sequence for both runs so the final state is identical by construction.
		for i := 1; i <= totalRows; i++ {
			if _, err := srcDB.ExecContext(
				streamCtx,
				fmt.Sprintf("INSERT INTO %s (id, email, n) VALUES ($1, $2, $3)", table),
				i, fmt.Sprintf("user%d@example.com", i), i,
			); err != nil {
				t.Fatalf("source insert: %v", err)
			}
		}
		for i := 2; i <= totalRows; i += 2 {
			if _, err := srcDB.ExecContext(
				streamCtx,
				fmt.Sprintf("UPDATE %s SET n = n + 1000 WHERE id = $1", table), i,
			); err != nil {
				t.Fatalf("source update: %v", err)
			}
		}

		if !waitForRowCount(t, targetDSN, table, totalRows, 90*time.Second) {
			t.Fatalf("[%s] dest only saw %d/%d rows after timeout", table, pollRowCount(targetDSN, table), totalRows)
		}
		// Give the UPDATE stream a brief grace beyond the row-count gate so the
		// post-insert updates drain before we snapshot the target content.
		waitForColumnConverge(t, sourceDSN, targetDSN, table, 30*time.Second)

		streamCancel()
		select {
		case err := <-runErr:
			if err != nil {
				t.Errorf("[%s] Streamer.Run returned error: %v", table, err)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("[%s] Streamer.Run did not return after ctx cancel", table)
		}
		return logs
	}

	// --- Default (unset → auto:N): must ENGAGE concurrency end-to-end. ---
	defLogs := runStream(t, "acc_default", "accdef", 0)
	if !strings.Contains(defLogs.String(), laneEngagedLog) {
		t.Fatalf("default --apply-concurrency did NOT engage the concurrent lane path "+
			"(missing %q in streamer log) — the ADR-0106 fast-by-default resolution is not engaging end-to-end.\nlog:\n%s",
			laneEngagedLog, defLogs.String())
	}

	// --- Explicit serial (=1): must NOT engage the lane path. ---
	serLogs := runStream(t, "acc_serial", "accser", 1)
	if strings.Contains(serLogs.String(), laneEngagedLog) {
		t.Fatalf("--apply-concurrency 1 engaged the concurrent lane path; expected serial "+
			"(the explicit opt-out must stay byte-identical to the prior default).\nlog:\n%s", serLogs.String())
	}

	// --- Differential: both targets must equal the source, and each other. ---
	srcDefault := dumpOrderedTable(t, sourceDSN, "acc_default")
	dstDefault := dumpOrderedTable(t, targetDSN, "acc_default")
	srcSerial := dumpOrderedTable(t, sourceDSN, "acc_serial")
	dstSerial := dumpOrderedTable(t, targetDSN, "acc_serial")

	if srcDefault != dstDefault {
		t.Fatalf("default-run target diverged from source:\nsrc:\n%s\ndst:\n%s", srcDefault, dstDefault)
	}
	if srcSerial != dstSerial {
		t.Fatalf("serial-run target diverged from source:\nsrc:\n%s\ndst:\n%s", srcSerial, dstSerial)
	}
	// The two source tables got the identical workload, so the default and
	// serial final states must be byte-identical to each other (the ADR's
	// "serial == default, byte-identical" convergence pin under the new default).
	if dstDefault != dstSerial {
		t.Fatalf("default (auto:N) and explicit-serial runs did NOT converge byte-identically:\ndefault:\n%s\nserial:\n%s",
			dstDefault, dstSerial)
	}
}

// waitForColumnConverge polls until the target's (id,email,n) content matches
// the source's for the given table, or the timeout elapses. The row-count gate
// alone doesn't guarantee the trailing UPDATEs have drained; this closes that
// window before the content snapshot.
func waitForColumnConverge(t *testing.T, sourceDSN, targetDSN, table string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var src, dst string
	for time.Now().Before(deadline) {
		src = dumpOrderedTable(t, sourceDSN, table)
		dst = dumpOrderedTable(t, targetDSN, table)
		if src == dst {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("[%s] target did not converge to source content within %s\nsrc:\n%s\ndst:\n%s", table, timeout, src, dst)
}

// dumpOrderedTable returns the table's (id, email, n) rows as a stable,
// id-ordered newline-joined string for byte-identical comparison.
func dumpOrderedTable(t *testing.T, dsn, table string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(context.Background(),
		fmt.Sprintf("SELECT id, email, n FROM %s ORDER BY id", table))
	if err != nil {
		t.Fatalf("query %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()

	var b strings.Builder
	for rows.Next() {
		var id, n int64
		var email string
		if err := rows.Scan(&id, &email, &n); err != nil {
			t.Fatalf("scan %s: %v", table, err)
		}
		fmt.Fprintf(&b, "%d|%s|%d\n", id, email, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err %s: %v", table, err)
	}
	return b.String()
}
