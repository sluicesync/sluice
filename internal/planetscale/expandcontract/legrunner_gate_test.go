// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the legRunner's pre-Deploy gates (audit MED-D0-7, ADR-0167):
// the deploy-request DIFF blast-radius assertion (diff within the
// leg's intended tables → deploys; a stranger object → coded refusal
// before the deploy, cleanup as usual; an empty intended set —
// deploy-ddl — skips the fetch entirely) and the post-wait PRODUCTION
// freshness recheck (a long deployable/review wait + production schema
// movement → refusal; a short wait skips the extra GET).
//
// NOTE the live caveat carried from the audit: whether PlanetScale
// re-diffs a DR at deploy time is derived-not-verified — these pins
// prove sluice's conservative behavior on the fakePS harness; the next
// live psverify dispatch should exercise the diff endpoint's real
// response shape.

package expandcontract

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// newGateLegRunner wires a legRunner directly against a fakePS (the
// cleanup-test pattern), so the gate pins can inject the clock and the
// expected-table set without going through an orchestrator.
func newGateLegRunner(t *testing.T, ps *fakePS) (*legRunner, *branchCleanup, *bytes.Buffer) {
	t.Helper()
	_, client := ps.serve()
	out := &bytes.Buffer{}
	r := &legRunner{
		api:           client,
		org:           "o",
		database:      "d",
		branch:        "main",
		pollInterval:  time.Millisecond,
		deployTimeout: 5 * time.Second,
		out:           out,
		execDDL: func(context.Context, *api.BranchPassword, string, string) error {
			return nil
		},
		name:         "gate-test",
		errPrefix:    "gate-test",
		passwordName: "gate-test",

		leftoverAdvice:        "continue",
		alreadyDeployedAdvice: "close the DR",
		reviewTimeoutAdvice:   "approve it and re-run",
		deployTimeoutAdvice:   "watch it",
	}
	cleanup := &branchCleanup{api: client, org: "o", database: "d", out: out, command: "gate-test"}
	return r, cleanup, out
}

// anyDeployed reports whether any fake DR received its deploy call.
func anyDeployed(ps *fakePS) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for _, fdr := range ps.drs {
		if fdr.deployed {
			return true
		}
	}
	return false
}

// TestLegRunner_DiffWithinExpectedDeploys pins the happy side of the
// blast-radius assertion: a diff that is a SUBSET of the intended set
// (the expand shape — the target table plus the staged control tables,
// where production already carrying some of them legitimately shrinks
// the diff) deploys normally.
func TestLegRunner_DiffWithinExpectedDeploys(t *testing.T) {
	ps := newFakePS(t)
	ps.drDiffs = []string{"items", "sluice_migrate_state"}
	r, cleanup, _ := newGateLegRunner(t, ps)
	r.expectedDiffTables = []string{"items", "sluice_migrate_state", "sluice_migrate_table_progress"}

	dr, err := r.run(context.Background(), "sluice-gate-ok", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if dr == nil || !anyDeployed(ps) {
		t.Fatal("deploy request was not deployed")
	}
	if ps.diffFetches != 1 {
		t.Errorf("diff fetches = %d; want exactly 1", ps.diffFetches)
	}
}

// TestLegRunner_DiffStrangerObjectRefusesBeforeDeploy pins the refusal
// side: a diff object outside the intended set refuses coded
// (SLUICE-E-PS-DEPLOY-REQUEST-FAILED), names the stranger, never calls
// deploy, and cleanup still deletes the dev branch.
func TestLegRunner_DiffStrangerObjectRefusesBeforeDeploy(t *testing.T) {
	ps := newFakePS(t)
	ps.drDiffs = []string{"items", "orders_evil"}
	r, cleanup, _ := newGateLegRunner(t, ps)
	r.expectedDiffTables = []string{"items"}

	_, err := r.run(context.Background(), "sluice-gate-stranger", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup)
	wantCode(t, err, sluicecode.CodePSDeployRequestFailed)
	for _, want := range []string{"orders_evil", "never intended", "deploy-requests/1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q missing %q", err.Error(), want)
		}
	}
	if anyDeployed(ps) {
		t.Error("deploy was called despite the stranger-object refusal")
	}
	cleanup.run(context.Background())
	if len(ps.deleted) != 1 || ps.deleted[0] != "sluice-gate-stranger" {
		t.Errorf("cleanup deleted %v; want the dev branch", ps.deleted)
	}
}

// TestLegRunner_EmptyDiffWithIntendedSetRefuses pins the fail-closed
// tripwire (audit 2026-07-16): a deployable DR whose diff decodes
// EMPTY on a leg with a non-empty intended set refuses coded before
// the deploy — a deployable DR structurally cannot have an empty diff
// (waitDeployable refuses no_changes), so an empty decode means the
// DERIVED diff response shape drifted from the real API, and passing
// would silently blind the whole blast-radius gate.
func TestLegRunner_EmptyDiffWithIntendedSetRefuses(t *testing.T) {
	ps := newFakePS(t)
	ps.drDiffs = []string{} // non-nil: serve a deployable DR with ZERO diff entries
	r, cleanup, _ := newGateLegRunner(t, ps)
	r.expectedDiffTables = []string{"items"}

	_, err := r.run(context.Background(), "sluice-gate-emptydiff", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup)
	wantCode(t, err, sluicecode.CodePSDeployRequestFailed)
	for _, want := range []string{"decoded EMPTY", "derived, not live-captured", "items", "deploy-requests/1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q missing %q", err.Error(), want)
		}
	}
	if anyDeployed(ps) {
		t.Error("deploy was called despite the empty-diff refusal")
	}
	cleanup.run(context.Background())
	if len(ps.deleted) != 1 || ps.deleted[0] != "sluice-gate-emptydiff" {
		t.Errorf("cleanup deleted %v; want the dev branch", ps.deleted)
	}
}

// TestLegRunner_EmptyExpectedSkipsDiffFetch pins the deploy-ddl
// carve-out: an empty intended set (arbitrary operator DDL that sluice
// deliberately does not parse) skips the diff fetch entirely and
// deploys.
func TestLegRunner_EmptyExpectedSkipsDiffFetch(t *testing.T) {
	ps := newFakePS(t)
	ps.drDiffs = []string{"anything_at_all"}
	r, cleanup, _ := newGateLegRunner(t, ps)

	if _, err := r.run(context.Background(), "sluice-gate-freeform", "CREATE TABLE anything_at_all (id BIGINT)", cleanup); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !anyDeployed(ps) {
		t.Fatal("deploy request was not deployed")
	}
	if ps.diffFetches != 0 {
		t.Errorf("diff fetches = %d; want 0 (no intended set → no assertion)", ps.diffFetches)
	}
}

// steppingClock returns t0 on the first call and t0+step on every
// later call — the baseline-then-recheck shape.
func steppingClock(t0 time.Time, step time.Duration) func() time.Time {
	calls := 0
	return func() time.Time {
		calls++
		if calls == 1 {
			return t0
		}
		return t0.Add(step)
	}
}

// TestLegRunner_PostWaitStalenessRefuses pins the TOCTOU half: the
// deployable/review wait ran long (injected clock > the recheck
// threshold) AND production's schema moved after the provisioning
// baseline (flipped when the DR was created) — the runner refuses
// coded (SLUICE-E-PS-BRANCH-STALE-BASE) instead of deploying a diff
// computed against the old schema.
func TestLegRunner_PostWaitStalenessRefuses(t *testing.T) {
	ps := newFakePS(t)
	ps.flipProdSchemaOnDRCreate = "\n-- production moved mid-wait"
	r, cleanup, _ := newGateLegRunner(t, ps)
	r.now = steppingClock(time.Now(), legFreshnessRecheckAfter+time.Minute)

	_, err := r.run(context.Background(), "sluice-gate-stale", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup)
	wantCode(t, err, sluicecode.CodePSBranchStaleBase)
	if !strings.Contains(err.Error(), "schema changed while deploy request") {
		t.Errorf("refusal %q should explain the mid-wait schema movement", err.Error())
	}
	if anyDeployed(ps) {
		t.Error("deploy was called despite the post-wait staleness refusal")
	}
	cleanup.run(context.Background())
	if len(ps.deleted) != 1 {
		t.Errorf("cleanup deleted %v; want the dev branch", ps.deleted)
	}
}

// TestLegRunner_ShortWaitSkipsFreshnessRecheck pins the threshold: a
// wait under legFreshnessRecheckAfter never issues the extra schema
// GET, so a fast deploy stays fast — even when production DID move,
// the sub-threshold window keeps the pre-ADR-0167 behavior.
func TestLegRunner_ShortWaitSkipsFreshnessRecheck(t *testing.T) {
	ps := newFakePS(t)
	ps.flipProdSchemaOnDRCreate = "\n-- production moved mid-wait"
	r, cleanup, _ := newGateLegRunner(t, ps)
	t0 := time.Now()
	r.now = func() time.Time { return t0 } // zero elapsed

	if _, err := r.run(context.Background(), "sluice-gate-fast", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !anyDeployed(ps) {
		t.Fatal("deploy request was not deployed")
	}
}

// TestLegRunner_PostWaitLongElapsedUnchangedProdDeploys pins the
// recheck's green path: a long wait with an UNCHANGED production
// schema re-verifies and deploys.
func TestLegRunner_PostWaitLongElapsedUnchangedProdDeploys(t *testing.T) {
	ps := newFakePS(t)
	r, cleanup, _ := newGateLegRunner(t, ps)
	r.now = steppingClock(time.Now(), legFreshnessRecheckAfter+time.Minute)

	if _, err := r.run(context.Background(), "sluice-gate-slow-ok", "ALTER TABLE items ADD COLUMN c BIGINT", cleanup); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !anyDeployed(ps) {
		t.Fatal("deploy request was not deployed")
	}
}

// TestExpandContract_StrangerDiffRefusesAndCleansUp drives the
// orchestrator end to end: the expand leg's DR diff carries a table
// the pattern never intended → coded refusal, nothing deployed, the
// dev branch cleaned up.
func TestExpandContract_StrangerDiffRefusesAndCleansUp(t *testing.T) {
	ps := newFakePS(t)
	ps.drDiffs = []string{"items", "orders_evil"}
	o, _, _, _ := newTestOrchestrator(t, ps)

	_, err := o.Run(context.Background())
	wantCode(t, err, sluicecode.CodePSDeployRequestFailed)
	if !strings.Contains(err.Error(), "orders_evil") {
		t.Errorf("refusal %q should name the stranger object", err.Error())
	}
	if anyDeployed(ps) {
		t.Error("a deploy request was deployed despite the stranger-object refusal")
	}
	if len(ps.deleted) != 1 || !strings.HasPrefix(ps.deleted[0], "sluice-expand-") {
		t.Errorf("deleted = %v; want just the expand dev branch", ps.deleted)
	}
}

// TestExpandContract_ExpandDiffMayCarryControlTables pins the expand
// leg's intended set: a DR diff carrying the target table AND the
// staged migrate-state control tables deploys — while the CONTRACT
// leg's stricter set (just the table) also holds, because by contract
// time the control tables are on production and out of its diff. The
// fake serves the expand-shaped diff for the expand DR and the
// table-only diff for the contract DR via the drDiffs flip below.
func TestExpandContract_ExpandDiffMayCarryControlTables(t *testing.T) {
	ps := newFakePS(t)
	ps.drDiffs = []string{"items", "sluice_migrate_state", "sluice_migrate_table_progress"}
	o, _, _, _ := newTestOrchestrator(t, ps)
	// After the expand leg deploys, the contract DR's diff is just the
	// table (control tables already shipped). Narrow the served diff
	// when the second DR is created.
	o.ExecDDL = func(_ context.Context, _ *api.BranchPassword, _, ddl string) error {
		if strings.Contains(ddl, "DROP COLUMN") {
			ps.mu.Lock()
			ps.drDiffs = []string{"items"}
			ps.mu.Unlock()
		}
		return nil
	}

	result, err := o.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.ExpandDeployRequest == 0 || result.ContractDeployRequest == 0 || !result.ContractRun {
		t.Errorf("result = %+v; want the full pattern", result)
	}
}

// TestDeployDDL_ArbitraryDDLSkipsDiffAssertion pins deploy-ddl's
// carve-out end to end: its DDL is unparsed operator input with no
// intended table set, so a diff naming any table deploys — the
// freshness gates (provisioning + post-wait) remain its guards.
func TestDeployDDL_ArbitraryDDLSkipsDiffAssertion(t *testing.T) {
	ps := newFakePS(t)
	ps.drDiffs = []string{"some_table_sluice_never_heard_of"}
	_, client := ps.serve()
	d := &DDLDeployer{
		API: client, Org: "o", Database: "d",
		DDL:          "CREATE TABLE some_table_sluice_never_heard_of (id BIGINT)",
		PollInterval: time.Millisecond, DeployTimeout: 5 * time.Second,
		Out: &bytes.Buffer{},
		ExecDDL: func(context.Context, *api.BranchPassword, string, string) error {
			return nil
		},
	}
	res, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.DeployRequest == 0 || !anyDeployed(ps) {
		t.Fatal("deploy-ddl did not deploy")
	}
	if ps.diffFetches != 0 {
		t.Errorf("diff fetches = %d; want 0", ps.diffFetches)
	}
}
