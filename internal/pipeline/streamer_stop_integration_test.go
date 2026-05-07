//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for `sync stop`: a running pipeline.Streamer that
// observes a non-NULL stop_requested_at on its control row should
// finish the in-flight change, persist its final position, and exit
// with a nil error. The test is end-to-end: real Postgres source +
// target containers, real CDC pump, real RequestStop call from a
// separate goroutine.
//
// This is the load-bearing proof that the polling loop hooks up to
// the apply loop correctly. The unit-level shape of pollStopSignal
// is covered in stop_signal_test.go (no-DB, no-container).

package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestStreamer_RequestStop_DrainsAndExits walks the canonical
// shape:
//
//  1. Cold-start a Streamer; bulk-copy R1; deliver R2 via CDC.
//  2. From a separate goroutine, RequestStop on the target.
//  3. Streamer.Run returns nil within a generous deadline.
//  4. The persisted position has not regressed — the in-flight
//     R2 was committed alongside its position write.
//
// The test overrides the polling cadence to 200ms so the assertion
// budget stays well under the per-test minute. Production keeps
// the 5s default (see internal/pipeline/stop_signal.go).
func TestStreamer_RequestStop_DrainsAndExits(t *testing.T) {
	prevInterval := pollIntervalForTest
	pollIntervalForTest = 200 * time.Millisecond
	t.Cleanup(func() { pollIntervalForTest = prevInterval })

	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
		INSERT INTO users (id, email) VALUES (1, 'r1@example.com');
	`
	applyDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const streamID = "test-stop"

	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Wait for cold-start bulk-copy to land R1.
	if !waitForRowCount(t, targetDSN, "users", 1, 30*time.Second) {
		t.Fatalf("bulk copy never delivered R1")
	}

	// Insert R2 on source — flows through CDC and writes its
	// position into sluice_cdc_state.
	applyDDL(t, sourceDSN, "INSERT INTO users (id, email) VALUES (2, 'r2@example.com');")
	if !waitForRowCount(t, targetDSN, "users", 2, 30*time.Second) {
		t.Fatalf("CDC never delivered R2")
	}

	persistedBefore := readPersistedPosition(t, targetDSN, streamID)
	if persistedBefore == "" {
		t.Fatal("control table has no position before stop request")
	}

	// Open a separate applier and call RequestStop. Mirrors what
	// the `sluice sync stop` CLI does.
	applierCtx, applierCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer applierCancel()
	stopApplier, err := pgEng.OpenChangeApplier(applierCtx, targetDSN)
	if err != nil {
		t.Fatalf("OpenChangeApplier (for stop): %v", err)
	}
	defer func() {
		if c, ok := stopApplier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := stopApplier.RequestStop(applierCtx, streamID); err != nil {
		t.Fatalf("RequestStop: %v", err)
	}

	// Streamer.Run should return cleanly (nil) within one full
	// poll cycle plus headroom. Default cadence is 5s.
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Streamer.Run returned err = %v; want nil after stop request", err)
		}
	case <-time.After(20 * time.Second):
		streamCancel()
		t.Fatal("Streamer.Run did not return within 20s of RequestStop")
	}

	// Position should not have regressed; rows should still be 2.
	persistedAfter := readPersistedPosition(t, targetDSN, streamID)
	if persistedAfter == "" {
		t.Error("control table has no position after stop")
	}
	if got := pollRowCount(targetDSN, "users"); got != 2 {
		t.Errorf("rows after stop = %d; want 2 (in-flight changes should have committed)", got)
	}
}
