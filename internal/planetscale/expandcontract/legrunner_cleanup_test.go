// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the branch-cleanup delete retry (roadmap item 71a): the
// post-deploy branch delete races the deploy/skip-revert settling
// window and gets HTTP 422 "cannot be deleted while a deployment is in
// progress" for ~1 min, so EVERY deploy-ddl/expand-contract/
// index-fallback run used to strand its dev branch with a WARN.
// cleanup now retries exactly that 422 class (bounded), and only that
// class — a non-422 delete failure still WARNs immediately.

package expandcontract

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/planetscale/api"
)

// captureLog runs fn with the default slog handler swapped for a
// buffer and returns what was logged (the ownership-preflight test
// pattern).
func captureLog(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	fn()
	return buf.String()
}

// newCleanupHarness serves a fakePS holding one leftover dev branch
// and returns it with a branchCleanup already tracking that branch.
func newCleanupHarness(t *testing.T, branch string, deleteScript []int) (*fakePS, *branchCleanup, *bytes.Buffer) {
	t.Helper()
	ps := newFakePS(t)
	ps.branches[branch] = &api.Branch{Name: branch, Ready: true}
	ps.deleteScript = deleteScript
	_, client := ps.serve()
	var out bytes.Buffer
	cleanup := &branchCleanup{
		api:      client,
		org:      "o",
		database: "d",
		out:      &out,
		command:  "deploy-ddl",
	}
	cleanup.add(branch)
	return ps, cleanup, &out
}

func (f *fakePS) remainingDeleteScript() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deleteScript)
}

func (f *fakePS) branchExists(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.branches[name]
	return ok
}

// TestBranchCleanup_RetriesDeleteRace422 pins the fix shape: two
// settling-window 422s then success ⇒ the branch is deleted, the
// success narration prints, and no WARN fires.
func TestBranchCleanup_RetriesDeleteRace422(t *testing.T) {
	ps, cleanup, out := newCleanupHarness(t, "sluice-ddl-a", []int{422, 422})

	logged := captureLog(t, func() { cleanup.run(context.Background()) })

	if ps.branchExists("sluice-ddl-a") {
		t.Error("branch survived cleanup; want it deleted after the 422s clear")
	}
	if got := ps.remainingDeleteScript(); got != 0 {
		t.Errorf("delete script has %d unconsumed entries; want 0 (one retry per 422)", got)
	}
	if !strings.Contains(out.String(), `cleanup: deleted dev branch "sluice-ddl-a"`) {
		t.Errorf("cleanup narration missing the deleted line; out = %q", out.String())
	}
	if strings.Contains(logged, "could not delete dev branch") {
		t.Errorf("cleanup WARNed despite the retry succeeding; log = %q", logged)
	}
}

// TestBranchCleanup_WarnsWhenRetryBudgetExhausted pins the bounded
// half: a 422 on every attempt ⇒ exactly deleteRetryAttempts deletes,
// then the existing WARN naming the branch (the pre-71a behavior,
// just later).
func TestBranchCleanup_WarnsWhenRetryBudgetExhausted(t *testing.T) {
	// One scripted 422 per attempt, plus a sentinel that MUST remain
	// unconsumed — a 7th attempt would pop it.
	script := make([]int, deleteRetryAttempts+1)
	for i := range script {
		script[i] = 422
	}
	ps, cleanup, out := newCleanupHarness(t, "sluice-ddl-b", script)

	logged := captureLog(t, func() { cleanup.run(context.Background()) })

	if got := ps.remainingDeleteScript(); got != 1 {
		t.Errorf("delete script has %d unconsumed entries; want exactly the 1 sentinel (%d attempts)", got, deleteRetryAttempts)
	}
	if !ps.branchExists("sluice-ddl-b") {
		t.Error("branch missing; a persistent 422 must never delete it")
	}
	if !strings.Contains(logged, "could not delete dev branch") || !strings.Contains(logged, "sluice-ddl-b") {
		t.Errorf("want the exhausted-retries WARN naming the branch; log = %q", logged)
	}
	if strings.Contains(out.String(), "deleted dev branch") {
		t.Errorf("cleanup narrated a delete that never happened; out = %q", out.String())
	}
}

// TestBranchCleanup_NonRetryable422OnlyOtherErrorsWarnImmediately pins
// the class boundary: a non-422 delete failure (500 here) is NOT the
// settling-window race and WARNs on the first attempt — no retry.
func TestBranchCleanup_NonRetryable422OnlyOtherErrorsWarnImmediately(t *testing.T) {
	// A single scripted 500: any retry would consume nothing further
	// and the second attempt would DELETE the branch — so the branch
	// still existing proves exactly one attempt was made.
	ps, cleanup, _ := newCleanupHarness(t, "sluice-ddl-c", []int{500})

	logged := captureLog(t, func() { cleanup.run(context.Background()) })

	if !ps.branchExists("sluice-ddl-c") {
		t.Error("branch was deleted; a non-422 failure must not be retried")
	}
	if got := ps.remainingDeleteScript(); got != 0 {
		t.Errorf("delete script has %d unconsumed entries; want 0 (single attempt)", got)
	}
	if !strings.Contains(logged, "could not delete dev branch") || !strings.Contains(logged, "sluice-ddl-c") {
		t.Errorf("want the immediate WARN naming the branch; log = %q", logged)
	}
}
