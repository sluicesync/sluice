//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-44 review item 2: Shape A's first boundary goes through the lease.
//
// The first cut refused any forwardable difference at a Shape A stream's
// first boundary, on the premise that the lease coordinates only a boundary
// every shard observes. For a running fleet that premise is false: a widen
// applied to every shard's source while the fleet is stopped reaches each
// shard ONLY as its first boundary, so every shard refused, permanently —
// and the refusal was decided against the warm-resume catalog read, which
// can predate a peer's coordinated apply.
//
// The cell: two shards consolidated into one target, cold-started and
// stopped; the same widen on both sources while stopped; both restarted on
// the quiet table; one row per source that needs the widened column. The
// independent expected values are the target's catalog type, the rows read
// back, and the lease row (exactly one apply).

package pipeline

import (
	"strings"
	"testing"
	"time"
)

func TestPhase2e_PG_FirstBoundaryWidenWhileStopped_RoutesThroughTheLeaseOnce(t *testing.T) {
	h := startPhase2eHarness(t)
	defer h.cleanup()
	for i := 0; i < 2; i++ {
		phase2eApplyDDL(t, h.sourceDSNs[i], phase2eSeedDDL(phase2eShardLabels()[i]))
	}
	logs := twfbCaptureLogs(t)

	// Cold start both shards, then stop them once both are streaming.
	_, cancelA, runErrA := startPhase2eStreamer(t, 0, h.sourceDSNs[0], h.targetDSN)
	if !waitForPhase2eTargetCount(h.targetDSN, 1, 90*time.Second) {
		cancelA()
		t.Fatalf("shard_a's seed row never landed: %v", <-runErrA)
	}
	_, cancelB, runErrB := startPhase2eStreamer(t, 1, h.sourceDSNs[1], h.targetDSN)
	if !waitForPhase2eTargetCount(h.targetDSN, 2, 90*time.Second) ||
		!waitForPersistedPositions(t, h.targetDSN, []string{phase2eStreamID(0), phase2eStreamID(1)}, 60*time.Second) {
		cancelA()
		cancelB()
		t.Fatalf("the cold starts did not both reach CDC: a=%v b=%v", <-runErrA, <-runErrB)
	}
	time.Sleep(2 * time.Second)
	cancelA()
	cancelB()
	<-runErrA
	<-runErrB

	// The fleet-wide widen, while every stream is stopped.
	for i := 0; i < 2; i++ {
		phase2eApplyDDL(t, h.sourceDSNs[i], `ALTER TABLE users ALTER COLUMN email TYPE varchar(512);`)
	}

	_, cancelA, runErrA = startPhase2eStreamer(t, 0, h.sourceDSNs[0], h.targetDSN)
	defer func() {
		cancelA()
		<-runErrA
	}()
	_, cancelB, runErrB = startPhase2eStreamer(t, 1, h.sourceDSNs[1], h.targetDSN)
	defer func() {
		cancelB()
		<-runErrB
	}()
	if !waitForPersistedPositions(t, h.targetDSN, []string{phase2eStreamID(0), phase2eStreamID(1)}, 60*time.Second) {
		t.Fatal("the restarted streams never resumed")
	}
	long := strings.Repeat("x", 400)
	for i := 0; i < 2; i++ {
		phase2eApplyDDL(t, h.sourceDSNs[i],
			`INSERT INTO users (email) VALUES ('`+phase2eShardLabels()[i]+`_`+long+`');`)
	}
	if !waitForPhase2eTargetCount(h.targetDSN, 4, 120*time.Second) {
		select {
		case err := <-runErrA:
			t.Fatalf("shard_a exited: %v\n%s", err, divergenceLines(logs.String()))
		case err := <-runErrB:
			t.Fatalf("shard_b exited: %v\n%s", err, divergenceLines(logs.String()))
		default:
		}
		t.Fatalf("the post-widen rows never landed\n%s", divergenceLines(logs.String()))
	}

	if strings.Contains(logs.String(), resumeDivergenceMarker) {
		t.Errorf("a shard refused the fleet-wide widen at its first boundary:\n%s", divergenceLines(logs.String()))
	}
	tgt := twfbDB{"postgres", h.targetDSN}
	if got := tgt.columnType(t, "users", "email"); got != "character varying(512)" {
		t.Errorf("target users.email is %q, want character varying(512)", got)
	}
	if got := tgt.scalar(t, "SELECT count(*) FROM users WHERE length(email) > 400"); got != "2" {
		t.Errorf("%s of the two shards' long rows landed whole, want 2", got)
	}
	row := readPhase2eLeaseRow(t, h.targetDSN, "public.users")
	if !row.Applied {
		t.Error("the lease row was never applied — the widen did not go through the lease")
	}
	if n := strings.Count(logs.String(), "forwarding the difference (target-witnessed, GC-44) through the lease"); n < 1 {
		t.Error("VACUOUS: no first boundary was forwarded through the lease")
	}
	if n := strings.Count(logs.String(), "shard consolidation lease acquired"); n != 1 {
		t.Errorf("the lease was acquired %d times, want exactly once (one apply)", n)
	}
	assertPhase2eShardDistribution(t, h.targetDSN, map[string]int{"shard_a": 2, "shard_b": 2})
}

// TestPhase2e_PG_FirstBoundaryPeerAddedColumn_BackfillsEveryShard pins
// GC-44 F13. Both sources ADD a column while the fleet is stopped, then
// change its DEFAULT, so each source's pre-existing rows hold the ORIGINAL
// default while the DEFAULT the holder carries is the new one. Shard A
// restarts first and forwards the ADD through the lease, backfilling its
// own rows. Shard B's first boundary then finds the target already
// matching. Before the fix it took that as its baseline and planned no
// backfill, so B's pre-existing rows kept the holder's fill — the new
// default, which B's source does not hold — at exit 0. The independent
// expected value is each seed row's own source.
func TestPhase2e_PG_FirstBoundaryPeerAddedColumn_BackfillsEveryShard(t *testing.T) {
	h := startPhase2eHarness(t)
	defer h.cleanup()
	for i := 0; i < 2; i++ {
		phase2eApplyDDL(t, h.sourceDSNs[i], phase2eSeedDDL(phase2eShardLabels()[i]))
	}
	logs := twfbCaptureLogs(t)

	// Cold start both shards; one change per source so each stream carries
	// (and retains a history version of) the table before the stop.
	_, cancelA, runErrA := startPhase2eStreamer(t, 0, h.sourceDSNs[0], h.targetDSN)
	if !waitForPhase2eTargetCount(h.targetDSN, 1, 90*time.Second) {
		cancelA()
		t.Fatalf("shard_a's seed row never landed: %v", <-runErrA)
	}
	_, cancelB, runErrB := startPhase2eStreamer(t, 1, h.sourceDSNs[1], h.targetDSN)
	if !waitForPhase2eTargetCount(h.targetDSN, 2, 90*time.Second) ||
		!waitForPersistedPositions(t, h.targetDSN, []string{phase2eStreamID(0), phase2eStreamID(1)}, 60*time.Second) {
		cancelA()
		cancelB()
		t.Fatalf("the cold starts did not both reach CDC: a=%v b=%v", <-runErrA, <-runErrB)
	}
	for i := 0; i < 2; i++ {
		phase2eApplyDDL(t, h.sourceDSNs[i], `INSERT INTO users (email) VALUES ('`+phase2eShardLabels()[i]+`_warm@example.com');`)
	}
	if !waitForPhase2eTargetCount(h.targetDSN, 4, 90*time.Second) {
		cancelA()
		cancelB()
		t.Fatalf("the warm rows never landed: a=%v b=%v", <-runErrA, <-runErrB)
	}
	time.Sleep(2 * time.Second)
	cancelA()
	cancelB()
	<-runErrA
	<-runErrB

	for i := 0; i < 2; i++ {
		phase2eApplyDDL(t, h.sourceDSNs[i], `ALTER TABLE users ADD COLUMN tier integer DEFAULT 5;
			ALTER TABLE users ALTER COLUMN tier SET DEFAULT 7;`)
	}

	// Shard A alone: it forwards the ADD and backfills its own rows.
	_, cancelA, runErrA = startPhase2eStreamer(t, 0, h.sourceDSNs[0], h.targetDSN)
	defer func() {
		cancelA()
		<-runErrA
	}()
	phase2eApplyDDL(t, h.sourceDSNs[0], `INSERT INTO users (email) VALUES ('shard_a_post@example.com');`)
	tgt := twfbDB{"postgres", h.targetDSN}
	deadline := time.Now().Add(120 * time.Second)
	for tgt.scalar(t, `SELECT tier::text FROM users WHERE email = 'shard_a_seed@example.com'`) != "5" {
		if time.Now().After(deadline) {
			t.Fatalf("shard_a never forwarded and backfilled the ADD\n%s", twfbNoticeLines(logs.String()))
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Shard B: its first boundary matches the target A already changed.
	_, cancelB, runErrB = startPhase2eStreamer(t, 1, h.sourceDSNs[1], h.targetDSN)
	defer func() {
		cancelB()
		<-runErrB
	}()
	phase2eApplyDDL(t, h.sourceDSNs[1], `INSERT INTO users (email) VALUES ('shard_b_post@example.com');`)
	if !waitForPhase2eTargetCount(h.targetDSN, 6, 120*time.Second) {
		t.Fatalf("shard_b's post-ADD row never landed\n%s", twfbNoticeLines(logs.String()))
	}
	deadline = time.Now().Add(60 * time.Second)
	for tgt.scalar(t, `SELECT tier::text FROM users WHERE email = 'shard_b_seed@example.com'`) != "5" && time.Now().Before(deadline) {
		time.Sleep(250 * time.Millisecond)
	}
	assertPhase2eSeedRowsMatchTheirSources(t, h, 2, "tier")
	if strings.Contains(logs.String(), resumeDivergenceMarker) {
		t.Errorf("a shard refused:\n%s", divergenceLines(logs.String()))
	}
	if !strings.Contains(logs.String(), "the target already holds columns this stream never carried") {
		t.Error("VACUOUS: shard_b's first boundary was not routed as a peer-added column")
	}
}
