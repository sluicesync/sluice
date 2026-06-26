//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Supervisor failure-isolation, against real databases (ADR-0122,
// roadmap item 47 gotcha 1). Two real sync streams run under one
// Supervisor: a HEALTHY Postgres→Postgres stream over a real container,
// and a FAILING stream pointed at an unreachable DSN. The load-bearing
// assertion is that the failing stream churns through its restart budget
// and is permanently isolated (state=failed) WITHOUT ever disturbing the
// healthy stream — which cold-starts AND keeps delivering CDC after its
// peer has permanently failed.
//
// SHARD ROUTING: the name is deliberately NOT prefixed TestMigrate_ /
// TestStreamer_, so it rides the pipeline-rest-other CI shard (same as
// sync_converge). Keep it that way for new supervisor integration tests.

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

func TestSupervisorFleet_FailureIsolation_PostgresToPostgres(t *testing.T) {
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

	healthy := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "healthy",
		SlotName:  "healthy", // distinct slot per ADR-0122 §4
	}

	// Unreachable DSN: a bogus port + 1s connect timeout so the stream
	// fails fast and terminally. ApplyRetryAttempts=1 makes the
	// Streamer's own retry a no-op so the SUPERVISOR's restart budget is
	// what's exercised.
	const badDSN = "postgres://test:test@127.0.0.1:1/nope?sslmode=disable&connect_timeout=1"
	failing := &Streamer{
		Source:             pgEng,
		Target:             pgEng,
		SourceDSN:          badDSN,
		TargetDSN:          badDSN,
		StreamID:           "failing",
		SlotName:           "failing",
		ApplyRetryAttempts: 1,
	}

	sup := NewSupervisor(
		[]SupervisedSync{
			{ID: "healthy", Runner: healthy},
			{ID: "failing", Runner: failing},
		},
		RestartPolicy{
			BackoffBase:            50 * time.Millisecond,
			BackoffCap:             100 * time.Millisecond,
			HealthyRunThreshold:    time.Hour,
			MaxConsecutiveFailures: 3,
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx) }()

	// The healthy stream cold-starts its seed row while its peer fails.
	if !waitForRowCount(t, targetDSN, "users", 1, 60*time.Second) {
		t.Fatal("healthy sync never delivered the cold-start seed row")
	}

	// The failing stream must hit its restart cap and be isolated.
	deadline := time.Now().Add(60 * time.Second)
	var failedSnap SyncStatusSnapshot
	for time.Now().Before(deadline) {
		found := false
		for _, snap := range sup.Snapshot() {
			if snap.ID == "failing" && snap.State == SyncFailed {
				failedSnap = snap
				found = true
			}
		}
		if found {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if failedSnap.State != SyncFailed {
		t.Fatalf("failing sync never reached state %q; snapshot=%+v", SyncFailed, sup.Snapshot())
	}

	// THE isolation assertion: after the peer permanently failed, the
	// healthy stream is still running AND still delivering CDC.
	var healthySnap SyncStatusSnapshot
	for _, snap := range sup.Snapshot() {
		if snap.ID == "healthy" {
			healthySnap = snap
		}
	}
	if healthySnap.State != SyncRunning {
		t.Fatalf("healthy sync state = %q after peer failed; want %q — failure was NOT isolated", healthySnap.State, SyncRunning)
	}

	applyDDL(t, sourceDSN, "INSERT INTO users (id, email) VALUES (2, 'r2@example.com');")
	if !waitForRowCount(t, targetDSN, "users", 2, 60*time.Second) {
		t.Fatal("healthy sync stopped delivering CDC after its peer failed — isolation broken")
	}

	// Hot-reload reconcile (ADR-0122 §3): RESTART the healthy stream with a
	// new spec (changed fingerprint) and DROP the failed peer — the shape a
	// SIGHUP config reload produces. The load-bearing integration assertion
	// is that the restarted Postgres sync releases and reacquires its
	// replication slot CLEANLY (the old goroutine fully drains before the
	// replacement acquires the same slot) and resumes delivering CDC from
	// its persisted position.
	healthyV2 := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "healthy",
		SlotName:  "healthy",
	}
	res, err := sup.Reconcile([]SupervisedSync{
		{ID: "healthy", Runner: healthyV2, Fingerprint: "v2"},
	})
	if err != nil {
		t.Fatalf("Reconcile (restart healthy, drop failing) = %v; want nil", err)
	}
	if len(res.Restarted) != 1 || res.Restarted[0] != "healthy" {
		t.Errorf("Reconcile Restarted = %v; want [healthy]", res.Restarted)
	}
	if len(res.Stopped) != 1 || res.Stopped[0] != "failing" {
		t.Errorf("Reconcile Stopped = %v; want [failing]", res.Stopped)
	}
	if _, ok := snapshotFor(sup, "failing"); ok {
		t.Error("dropped sync \"failing\" still present after reconcile; want removed")
	}
	waitForState(t, sup, "healthy", SyncRunning, 60*time.Second)

	// The slot was reacquired cleanly: a post-restart insert still flows.
	applyDDL(t, sourceDSN, "INSERT INTO users (id, email) VALUES (3, 'r3@example.com');")
	if !waitForRowCount(t, targetDSN, "users", 3, 60*time.Second) {
		t.Fatal("restarted healthy sync did not resume delivering CDC — slot not reacquired cleanly")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Supervisor.Run after cancel = %v; want nil", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Supervisor.Run did not return within 30s of cancel")
	}
}
