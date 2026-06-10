//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end integration test for the ADR-0052 AIMD apply-batch-size
// controller. Same-engine Postgres → Postgres: a source-side workload
// is streamed to the dest with AutoTune=true; the test asserts:
//
//   - The streamer constructs the controller successfully and the
//     applier sees a non-nil BatchSizeProvider on every batch boundary.
//   - All N rows land on the dest (correctness — the controller's
//     decisions must never produce a batch size that loses data).
//   - The cooperative scrape via MetricsServer exposes the four ADR-0052
//     gauges, scoped by stream_id.
//
// We deliberately don't pin convergence to a specific batch size — the
// behaviour is workload-dependent and the controller's design makes
// the exact value a function of observed latency. The pin instead is
// the easier "data lands AND the controller is engaged AND the
// metrics surface AND the gauges fire" shape, which catches any
// regression in the wiring.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_AIMDController_PostgresToPostgres_Engages drives the
// streamer with --auto-tune on (the v0.72.0 default) and asserts the
// controller fires end-to-end. Pin shape:
//
//  1. All source rows land on the dest (correctness invariant).
//  2. The /metrics endpoint exposes sluice_apply_batch_size_current
//     with the expected stream_id label (controller wired into the
//     metrics server).
func TestStreamer_AIMDController_PostgresToPostgres_Engages(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE aimd_users (
			id    BIGINT PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE aimd_users REPLICA IDENTITY FULL;
	`
	applyDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const batchSize = 100
	const totalRows = 250
	const streamID = "aimd-integration"

	// Use a fixed-port metrics listener so the test can scrape; pick a
	// high port that's unlikely to collide.
	const metricsAddr = "127.0.0.1:39052"
	streamer := &Streamer{
		Source:         pgEng,
		Target:         pgEng,
		SourceDSN:      sourceDSN,
		TargetDSN:      targetDSN,
		StreamID:       streamID,
		ApplyBatchSize: batchSize,
		AutoTune:       true,
		MetricsListen:  metricsAddr,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Wait for the replication slot to exist before writing the source
	// burst — a commit that lands BEFORE the slot is created is
	// captured by neither the snapshot nor CDC, so under CI shard
	// contention the old blind 2s sleep produced the permanent
	// "dest only saw 0/250 rows" failure (see [waitForSourceSlot]).
	// 120s, not 60s: the first CI failure of THIS gate (run 27307141746)
	// proved cold-start can take >60s to reach slot creation under
	// worst-case shard contention — slow-but-correct, and the gate's
	// loud message diagnosed it precisely. Matches the catch-up
	// window's tolerance below.
	waitForSourceSlot(t, sourceDSN, 120*time.Second)

	srcDB, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = srcDB.Close() }()

	tx, err := srcDB.BeginTx(streamCtx, nil)
	if err != nil {
		t.Fatalf("source begin: %v", err)
	}
	for i := 1; i <= totalRows; i++ {
		if _, err := tx.ExecContext(
			streamCtx,
			"INSERT INTO aimd_users (id, email) VALUES ($1, $2)",
			i, fmt.Sprintf("user%d@example.com", i),
		); err != nil {
			t.Fatalf("source insert: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("source commit: %v", err)
	}

	// Wait for all rows to land. Use the tolerant waitForRowCount /
	// pollRowCount: the cold-start creates aimd_users on the TARGET
	// asynchronously, so an early countRows() races the schema-apply and
	// hits "relation \"aimd_users\" does not exist" (42P01) — which countRows
	// turns into a t.Fatal (the recurring AIMD CI flake). pollRowCount treats
	// the not-yet-created table as 0 rows and keeps polling.
	if !waitForRowCount(t, targetDSN, "aimd_users", totalRows, 60*time.Second) {
		t.Fatalf("dest only saw %d/%d rows after timeout", pollRowCount(targetDSN, "aimd_users"), totalRows)
	}

	// Scrape the metrics endpoint and confirm the AIMD gauges fired.
	// The scrape's HTTP body must include the four ADR-0052 metric
	// names with the configured stream_id label.
	scrapeCtx, scrapeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scrapeCancel()
	req, err := http.NewRequestWithContext(scrapeCtx, http.MethodGet, "http://"+metricsAddr+"/metrics", http.NoBody)
	if err != nil {
		t.Fatalf("build scrape req: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("scrape /metrics: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read scrape body: %v", err)
	}
	for _, want := range []string{
		"sluice_apply_batch_size_current{stream_id=",
		"sluice_apply_batch_size_p95_seconds{stream_id=",
		"sluice_apply_batch_size_decreases_total{stream_id=",
		"sluice_apply_batch_size_cooloff{stream_id=",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("expected %q in scrape body; body:\n%s", want, body)
		}
	}
	if !strings.Contains(string(body), streamID) {
		t.Errorf("scrape body missing configured stream_id %q", streamID)
	}

	streamCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Streamer.Run returned error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
