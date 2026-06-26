//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end integration pin for the engine-neutral sync-lag metric (roadmap
// item 45). Same-engine Postgres → Postgres: a source-side workload is
// streamed to the dest with the /metrics endpoint on; the test scrapes
// sluice_sync_lag_seconds and asserts:
//
//   - the gauge surfaces with the configured stream_id label (the tracker is
//     wired into the metrics server AND the change-stream interceptor fed it);
//   - its value is a finite, non-negative, PLAUSIBLE number of seconds
//     (< 10 minutes). The plausibility bound is the load-bearing assertion: it
//     catches a units bug in the per-engine source-commit-time threading — a
//     raw epoch (~1.7e9) or a nanoseconds-as-seconds value would blow past it,
//     while a correctly-derived "behind by a few seconds" reading passes.
//
// The threshold-alert FIRING path is unit-pinned (sync_lag_test.go) rather
// than here: the alerter ticks at telemetryPollInterval (60s), too slow for a
// hermetic integration assertion. This test pins the value-fidelity end of the
// feature — that the real pgoutput commit timestamp flows through to a sane
// gauge reading.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

func TestStreamer_SyncLagMetric_PostgresToPostgres(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE synclag_users (
			id    BIGINT PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE synclag_users REPLICA IDENTITY FULL;
	`
	applyDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const (
		totalRows = 50
		streamID  = "synclag-integration"
	)
	metricsAddr := freeLoopbackAddr(t)
	streamer := &Streamer{
		Source:        pgEng,
		Target:        pgEng,
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		StreamID:      streamID,
		MetricsListen: metricsAddr,
	}

	logs := captureSlog(t)

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

	for i := 1; i <= totalRows; i++ {
		if _, err := srcDB.ExecContext(
			streamCtx,
			"INSERT INTO synclag_users (id, email) VALUES ($1, $2)",
			i, fmt.Sprintf("user%d@example.com", i),
		); err != nil {
			t.Fatalf("source insert: %v", err)
		}
	}

	if !waitForRowCount(t, targetDSN, "synclag_users", totalRows, 60*time.Second) {
		t.Fatalf("dest only saw %d/%d rows after timeout", pollRowCount(targetDSN, "synclag_users"), totalRows)
	}

	// Scrape /metrics and locate the sync-lag gauge line.
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

	value, found := scrapeSyncLagValue(t, string(body), streamID)
	if !found {
		t.Fatalf("sluice_sync_lag_seconds{stream_id=%q} not found in scrape body:\n%s", streamID, body)
	}
	// Plausibility bound — the units pin. A correctly-derived lag on a
	// just-applied burst is a few seconds at most; a raw-epoch or
	// nanoseconds-as-seconds bug would be orders of magnitude larger.
	if value < 0 || value > 600 {
		t.Fatalf("sync lag = %v seconds; want a plausible 0..600s reading (units pin)", value)
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

// scrapeSyncLagValue extracts the sluice_sync_lag_seconds value for streamID
// from a Prometheus exposition body. Returns (value, true) on a hit.
func scrapeSyncLagValue(t *testing.T, body, streamID string) (float64, bool) {
	t.Helper()
	prefix := fmt.Sprintf("sluice_sync_lag_seconds{stream_id=%q} ", streamID)
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 64)
		if err != nil {
			t.Fatalf("parse sync-lag value from %q: %v", line, err)
		}
		return v, true
	}
	return 0, false
}
