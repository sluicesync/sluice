//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
	"sluicesync.dev/sluice/internal/ir"
)

// TestStreamer_SQLiteTriggerGracefulStopIsGraceful (the second fix-aimed
// review of 6b5898dc): since 4c8ce383 a source pump error surfaced under a
// `sync stop` that the retry loop would retry ends the run with exit 4. A
// trigger-CDC reader's pump sees the stop's cancel mid-poll; it must surface
// that as a context cancellation (filtered by surfaceSourceError), not as a
// retriable transport error. By the code: sqlite-trigger wraps the poll error
// with %w, pgtrigger's classifyPollError and triggercdc's retriable wrapper
// keep Unwrap, and d1-trigger's HTTP client error carries the context error;
// this cell measures the sqlite-trigger reader end to end.
func TestStreamer_SQLiteTriggerGracefulStopIsGraceful(t *testing.T) {
	setPollIntervalForTest(t, 200*time.Millisecond)
	src := seedSQLiteTriggerSource(t)
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()
	srcEng, _ := engines.Get(sqlitetrigger.EngineName)
	pgEng, _ := engines.Get("postgres")
	const streamID = "sqlite-trigger-graceful"
	s := &Streamer{Source: srcEng, Target: pgEng, SourceDSN: src, TargetDSN: pgTarget, StreamID: streamID}
	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()
	if !waitForRowCount(t, pgTarget, "events", 2, 90*time.Second) {
		t.Fatal("RIG: the cold start never delivered the seed rows")
	}
	sqliteExec(t, src, `INSERT INTO events (id, big, blb, note) VALUES (3, 1, NULL, 'cdc-3')`)
	if !waitForRowCount(t, pgTarget, "events", 3, 60*time.Second) {
		t.Fatal("RIG: the CDC row never landed")
	}
	applier, err := pgEng.OpenChangeApplier(context.Background(), pgTarget)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = applier.(io.Closer).Close() }()
	if err := applier.RequestStop(context.Background(), streamID); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a graceful stop of a sqlite-trigger stream returned %v; want nil (not %s)", err, StopDrainHardCancelledMarker)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the stream did not drain after `sync stop`")
	}
	l, err := applier.(ir.StreamLivenessStore).ReadStreamLiveness(context.Background(), streamID)
	if err != nil || !l.Stopped {
		t.Fatalf("after the graceful stop the record is %+v (%v); want the stopped marker", l, err)
	}
}

// TestStreamer_PostgresTargetGracefulStopRecordsTheStop (the second fix-aimed
// review of 6b5898dc): on a POSTGRES target, a graceful `sync stop` must
// leave stream_alive_ms holding the stopped marker with the stop request
// cleared — exactly what `sync stop --wait` now requires to succeed — in
// each Postgres apply mode: the per-change path (batch size 1), the batched
// path (ADR-0092 pipelined flush), and concurrent lanes (ADR-0104). The
// record is read with raw SQL, not through the store that wrote it.
func TestStreamer_PostgresTargetGracefulStopRecordsTheStop(t *testing.T) {
	for _, mode := range []struct {
		name        string
		batch, lane int
	}{
		{name: "per-change", batch: 1, lane: 1},
		{name: "batched (pipelined)", batch: 50, lane: 1},
		{name: "concurrent lanes", batch: 50, lane: 4},
	} {
		t.Run(mode.name, func(t *testing.T) {
			setPollIntervalForTest(t, 200*time.Millisecond)
			sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
			defer cleanup()
			applyDDL(t, sourceDSN, `
				CREATE TABLE gs (id BIGINT PRIMARY KEY, payload TEXT NOT NULL);
				INSERT INTO gs SELECT g, 'seed-' || g FROM generate_series(1, 20) g;`)
			pgEng, ok := engines.Get("postgres")
			if !ok {
				t.Fatal("postgres engine not registered")
			}
			const streamID = "pg-graceful"
			s := &Streamer{
				Source: pgEng, Target: pgEng, SourceDSN: sourceDSN, TargetDSN: targetDSN, StreamID: streamID,
				ApplyBatchSize: mode.batch, ApplyConcurrency: mode.lane,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- s.Run(ctx) }()
			if !waitForRowCount(t, targetDSN, "gs", 20, 60*time.Second) {
				t.Fatal("RIG: the seed rows never reached the target")
			}
			applyDDL(t, sourceDSN, "INSERT INTO gs SELECT g, 'cdc-' || g FROM generate_series(21, 60) g;")
			if !waitForRowCount(t, targetDSN, "gs", 60, 60*time.Second) {
				t.Fatal("RIG: the CDC rows never reached the target")
			}
			db, err := sql.Open("pgx", targetDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			record := func() (alive sql.NullInt64, stopPending bool) {
				t.Helper()
				if err := db.QueryRow("SELECT stream_alive_ms, stop_requested_at IS NOT NULL FROM sluice_cdc_state WHERE stream_id = $1",
					streamID).Scan(&alive, &stopPending); err != nil {
					t.Fatalf("read the control row: %v", err)
				}
				return alive, stopPending
			}
			if alive, _ := record(); !alive.Valid || alive.Int64 == 0 {
				t.Fatalf("RIG: the running stream's record is %v; want a running stamp", alive)
			}

			applier, err := pgEng.OpenChangeApplier(context.Background(), targetDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = applier.(io.Closer).Close() }()
			if err := applier.RequestStop(context.Background(), streamID); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("the gracefully stopped Run returned %v; want nil", err)
				}
			case <-time.After(60 * time.Second):
				t.Fatal("the stream did not drain after `sync stop`")
			}
			alive, stopPending := record()
			if stopPending || !alive.Valid || alive.Int64 != 0 {
				t.Fatalf("after a graceful stop: stream_alive_ms=%v stop pending=%v; want the stopped marker (0) and the request cleared",
					fmt.Sprint(alive), stopPending)
			}
		})
	}
}
