//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end integration pin for the delayed-replica CDC apply mode (roadmap
// item 46, ADR-0121). Same-engine Postgres → Postgres. Two load-bearing
// properties:
//
//   - VISIBILITY: with --apply-delay set, a source-side CDC change is NOT on
//     the target until the delay elapses, then it appears. Cold-copy is
//     unaffected (the seed row lands immediately).
//   - RESUME-SAFETY / exactly-once (the pin that matters most, ADR-0121 §2):
//     a stream cancelled while changes are still held in the delay window
//     applies NONE of them (the position never advanced past a held change),
//     and a restart re-reads the held tail and converges to the exact row
//     count — no loss, no dupes.

package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

func TestStreamer_ApplyDelay_PostgresToPostgres(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE delayed_users (
			id    BIGINT       PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE delayed_users REPLICA IDENTITY FULL;
		INSERT INTO delayed_users (id, email) VALUES (1, 'r1@example.com');
	`
	applyDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const (
		streamID   = "test-apply-delay"
		applyDelay = 8 * time.Second
	)

	insertRows := func(from, to int) {
		t.Helper()
		for i := from; i <= to; i++ {
			applyDDL(t, sourceDSN, fmt.Sprintf(
				"INSERT INTO delayed_users (id, email) VALUES (%d, 'r%d@example.com');", i, i,
			))
		}
	}

	// ---- Phase 1: cold start with the delay armed ----
	streamer1 := &Streamer{
		Source:     pgEng,
		Target:     pgEng,
		SourceDSN:  sourceDSN,
		TargetDSN:  targetDSN,
		StreamID:   streamID,
		ApplyDelay: applyDelay,
	}
	logs := captureSlog(t)
	ctx1, cancel1 := context.WithCancel(context.Background())
	runErr1 := make(chan error, 1)
	go func() { runErr1 <- streamer1.Run(ctx1) }()

	// Cold-copy is UNAFFECTED by the delay: R1 lands promptly.
	if !waitForRowCount(t, targetDSN, "delayed_users", 1, 60*time.Second) {
		t.Fatalf("phase 1: cold-copy never delivered the seed row (delay must not gate bulk-copy)")
	}
	waitForSourceSlotWatching(t, sourceDSN, 120*time.Second, runErr1, logs)

	// ---- Phase 2: CDC changes are HELD for the delay window ----
	insertRows(2, 6) // 5 CDC inserts
	// Well inside the delay window (3s < 8s): the held rows must NOT be on the
	// target yet — only the cold-copied R1.
	time.Sleep(3 * time.Second)
	if got := countRows(t, targetDSN, "delayed_users"); got != 1 {
		t.Fatalf("phase 2: target has %d rows %ds into an %s delay window; want 1 (CDC changes held)",
			got, 3, applyDelay)
	}
	// After the window elapses they appear.
	if !waitForRowCount(t, targetDSN, "delayed_users", 6, 30*time.Second) {
		t.Fatalf("phase 2: held CDC rows never appeared after the delay window elapsed (have %d/6)",
			countRows(t, targetDSN, "delayed_users"))
	}

	// ---- Phase 3: insert a fresh batch, then CRASH mid-delay-window ----
	insertRows(7, 11) // 5 more CDC inserts
	// Sleep into (but not past) the delay window so these are definitely SITTING
	// in the delay gate, un-applied, when we crash.
	time.Sleep(3 * time.Second)
	if got := countRows(t, targetDSN, "delayed_users"); got != 6 {
		t.Fatalf("phase 3: target has %d rows mid-delay-window; want 6 (the fresh batch must still be held)", got)
	}
	cancel1()
	select {
	case <-runErr1:
	case <-time.After(15 * time.Second):
		t.Fatal("phase 3: streamer1 did not return after ctx cancel")
	}
	// The held-but-unapplied batch was dropped on cancel — the target is still
	// at 6, and (ADR-0121 §2) the persisted position never advanced past the
	// held changes, so they are recoverable on resume.
	if got := countRows(t, targetDSN, "delayed_users"); got != 6 {
		t.Fatalf("phase 3: target has %d rows after crash; want 6 (a held change must NOT have been applied)", got)
	}
	if readPersistedPosition(t, targetDSN, streamID) == "" {
		t.Fatal("phase 3: no persisted position after crash — warm resume can't re-read the held tail")
	}

	// ---- Phase 4: restart (no delay) re-reads the held tail exactly-once ----
	// The load-bearing pin: streamer1 acked the slot only as far as the APPLIED
	// position (row 6) — NOT the decoded position — so PG re-sends rows 7..11 on
	// reconnect. (This is what the ADR-0121 slot-ack-after-apply wiring fix
	// guarantees; without it, confirmed_flush_lsn ran past the held rows and
	// they were silently lost here.)
	streamer2 := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
		// ApplyDelay deliberately 0: prove the held tail is re-read from the
		// un-advanced position and applied, converging to the exact count.
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	runErr2 := make(chan error, 1)
	go func() { runErr2 <- streamer2.Run(ctx2) }()

	if !waitForRowCount(t, targetDSN, "delayed_users", 11, 60*time.Second) {
		t.Fatalf("phase 4: warm resume did not re-read the held tail (have %d/11) — resume-safety violated",
			countRows(t, targetDSN, "delayed_users"))
	}
	// Exactly-once: the PK makes dupes impossible at the row level, but a LOSS
	// would show < 11. Settle briefly and assert the exact count.
	time.Sleep(2 * time.Second)
	if got := countRows(t, targetDSN, "delayed_users"); got != 11 {
		t.Fatalf("phase 4: final row count = %d; want exactly 11 (exactly-once)", got)
	}

	cancel2()
	select {
	case <-runErr2:
	case <-time.After(15 * time.Second):
		t.Fatal("phase 4: streamer2 did not return after ctx cancel")
	}

	// Final set check: ids 1..11 each present exactly once.
	emails := selectAllEmails(t, targetDSN, "delayed_users")
	if len(emails) != 11 {
		t.Fatalf("final: %d emails; want 11", len(emails))
	}
	for i := 1; i <= 11; i++ {
		want := fmt.Sprintf("r%d@example.com", i)
		if emails[i-1] != want {
			t.Errorf("final: email[%d] = %q; want %q", i-1, emails[i-1], want)
		}
	}
}
