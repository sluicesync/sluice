// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the deploy-request progress/health surface: the LIVE-captured
// response shape (this is the one fixture in this package that is
// verbatim ground truth, not derived), and [DeployRequest.Progress]'s
// aggregation across both field locations the response carries operation
// rows in.

package api

import (
	"context"
	"testing"
	"time"
)

// liveInProgressDeployRequest is a VERBATIM excerpt of the real
// GET /organizations/{org}/databases/{db}/deploy-requests/1 response,
// captured 2026-07-30 at 02:47 UTC from org `sluicesync`, database
// `scaletest-my3` — a PS-160 mid-build on a 106 GB / 153 M-row
// `ALTER … ADD KEY ×4` that sluice's ADR-0148 fallback had opened. Only
// fields sluice does not read were dropped (syntax-highlighted DDL,
// throttler configuration, actor blocks); every value below is what
// PlanetScale actually served.
//
// It is the fixture that matters most in this file: the poller's whole
// narration is built from these fields, and a shape drift here turns
// progress reporting silently blind rather than loud.
const liveInProgressDeployRequest = `{
  "id": "7xctu6f8p9gx",
  "type": "DeployRequest",
  "branch": "sluice-index-1d1644d356",
  "into_branch": "main",
  "approved": false,
  "state": "open",
  "deployment_state": "in_progress",
  "deployment": {
    "id": "zvnr1b3fchvu",
    "type": "Deployment",
    "into_branch": "main",
    "deploy_request_number": 1,
    "deploy_operations": [
      {
        "id": "jkovxln5m9y1",
        "type": "DeployOperation",
        "state": "in_progress",
        "keyspace_name": "scaletest-my3",
        "table_name": "events",
        "operation_name": "ALTER",
        "eta_seconds": 4299,
        "progress_percentage": 67,
        "deploy_error_docs_url": null,
        "created_at": "2026-07-30T00:22:01.738Z",
        "updated_at": "2026-07-30T02:47:30.76Z",
        "throttled_at": "2026-07-30T00:21:59Z",
        "can_drop_data": false,
        "table_locked": false,
        "deploy_errors": ""
      }
    ],
    "deploy_operation_summaries": [
      {
        "id": "jkovxln5m9y1",
        "type": "DeployOperationSummary",
        "state": "in_progress",
        "table_name": "events",
        "operation_name": "ALTER",
        "eta_seconds": 4299,
        "progress_percentage": 67,
        "throttled_at": "2026-07-30T00:21:59Z",
        "shard_count": 1,
        "sharded": false
      }
    ],
    "deployable": true,
    "cutover_expiring": false,
    "lint_errors": [],
    "table_locked": false,
    "auto_cutover": true,
    "auto_delete_branch": false,
    "created_at": "2026-07-30T00:21:35.113Z",
    "cutover_at": null,
    "finished_at": null,
    "queued_at": "2026-07-30T00:21:55.774Z",
    "started_at": "2026-07-30T00:22:01.502Z",
    "state": "in_progress",
    "strategy": "serial",
    "submitted_at": "2026-07-30T00:21:56.103Z",
    "instant_ddl": false,
    "queue_paused": false,
    "parallel_lane_blocked": false,
    "queue_pause_reason": null
  },
  "number": 1,
  "notes": null,
  "created_at": "2026-07-30T00:21:35.079Z",
  "closed_at": null,
  "deployed_at": null,
  "strategy": "serial",
  "html_url": "https://app.planetscale.com/sluicesync/scaletest-my3/deploy-requests/1"
}`

// TestClient_GetDeployRequest_LiveInProgressShape decodes the verbatim
// live response and pins every field the poller narrates from.
func TestClient_GetDeployRequest_LiveInProgressShape(t *testing.T) {
	dr, err := serveFixture(t, liveInProgressDeployRequest).GetDeployRequest(context.Background(), "sluicesync", "scaletest-my3", 1)
	if err != nil {
		t.Fatalf("GetDeployRequest: %v", err)
	}
	if dr.Number != 1 || dr.State != "open" || dr.DeploymentState != "in_progress" {
		t.Errorf("decoded %d/%q/%q; want 1/open/in_progress", dr.Number, dr.State, dr.DeploymentState)
	}
	// approved=false on a request that was ALREADY deploying — this is
	// exactly why sluice advises about the approval gate rather than
	// inferring it from the flag.
	if dr.Approved {
		t.Error("Approved decoded true; the live mid-build request carried approved=false")
	}
	if !dr.CanDeploy() {
		t.Error("CanDeploy() = false; deployable lives in the nested deployment object")
	}
	if !dr.Deployment.AutoCutover {
		t.Error("AutoCutover = false; PlanetScale served auto_cutover=true (the human-gate downgrade depends on it)")
	}
	if dr.Deployment.TableLocked || dr.Deployment.CutoverExpiring || dr.Deployment.QueuePaused {
		t.Errorf("health flags decoded %+v; the live capture had all three false", dr.Deployment)
	}
	// queue_pause_reason was JSON null — it must decode to "" and not
	// error the whole response.
	if dr.Deployment.QueuePauseReason != "" {
		t.Errorf("QueuePauseReason = %q; a null reason must decode empty", dr.Deployment.QueuePauseReason)
	}
	if got := len(dr.Deployment.DeployOperations); got != 1 {
		t.Fatalf("deploy_operations = %d rows; want 1", got)
	}
	op := dr.Deployment.DeployOperations[0]
	if op.TableName != "events" || op.OperationName != "ALTER" || op.State != "in_progress" {
		t.Errorf("operation decoded %+v; want the events ALTER in progress", op)
	}
	if op.ProgressPercentage != 67 || op.ETASeconds != 4299 {
		t.Errorf("progress/eta = %d%%/%ds; want 67%%/4299s", op.ProgressPercentage, op.ETASeconds)
	}
	if op.ThrottledAt == nil || !op.ThrottledAt.Equal(time.Date(2026, 7, 30, 0, 21, 59, 0, time.UTC)) {
		t.Errorf("throttled_at = %v; want the live 2026-07-30T00:21:59Z stamp", op.ThrottledAt)
	}
	// The summaries carry the same values — both locations must decode,
	// because Progress() falls back to the summaries.
	if got := len(dr.Deployment.DeployOperationSummaries); got != 1 {
		t.Fatalf("deploy_operation_summaries = %d rows; want 1", got)
	}
	if dr.Deployment.DeployOperationSummaries[0].ProgressPercentage != 67 {
		t.Errorf("summary progress = %d%%; want 67%%", dr.Deployment.DeployOperationSummaries[0].ProgressPercentage)
	}

	prog := dr.Progress()
	if !prog.PercentKnown || prog.Percent != 67 {
		t.Errorf("Progress().Percent = %d (known=%v); want 67", prog.Percent, prog.PercentKnown)
	}
	if !prog.ETAKnown || prog.ETA != 4299*time.Second {
		t.Errorf("Progress().ETA = %s (known=%v); want 4299s", prog.ETA, prog.ETAKnown)
	}
	if prog.ThrottledAt.IsZero() {
		t.Error("Progress().ThrottledAt is zero; the live capture carried a throttle stamp")
	}
	if prog.Operations != 1 {
		t.Errorf("Progress().Operations = %d; want 1", prog.Operations)
	}
}

// TestDeployRequest_Progress covers the aggregation rules across the
// shapes the response can take: both field locations, multiple operations
// (the sharded/multi-table case, where the deployment is only as done as
// its SLOWEST leg), a null throttle stamp, and no operation rows at all.
func TestDeployRequest_Progress(t *testing.T) {
	early := time.Date(2026, 7, 30, 0, 21, 59, 0, time.UTC)
	late := time.Date(2026, 7, 30, 2, 30, 0, 0, time.UTC)

	tests := []struct {
		name            string
		ops             []DeployOperation
		summaries       []DeployOperation
		wantPercent     int
		wantPercentOK   bool
		wantETA         time.Duration
		wantETAOK       bool
		wantThrottledAt time.Time
		wantOperations  int
	}{
		{
			name: "single operation from deploy_operations",
			ops: []DeployOperation{
				{ProgressPercentage: 67, ETASeconds: 4299, ThrottledAt: &early},
			},
			wantPercent: 67, wantPercentOK: true,
			wantETA: 4299 * time.Second, wantETAOK: true,
			wantThrottledAt: early, wantOperations: 1,
		},
		{
			name: "falls back to deploy_operation_summaries",
			summaries: []DeployOperation{
				{ProgressPercentage: 42, ETASeconds: 600},
			},
			wantPercent: 42, wantPercentOK: true,
			wantETA: 600 * time.Second, wantETAOK: true,
			wantOperations: 1,
		},
		{
			name: "deploy_operations wins when both are present",
			ops: []DeployOperation{
				{ProgressPercentage: 67, ETASeconds: 4299},
			},
			summaries: []DeployOperation{
				{ProgressPercentage: 5, ETASeconds: 99999},
			},
			wantPercent: 67, wantPercentOK: true,
			wantETA: 4299 * time.Second, wantETAOK: true,
			wantOperations: 1,
		},
		{
			name: "several operations report the slowest percent and longest ETA",
			ops: []DeployOperation{
				{ProgressPercentage: 90, ETASeconds: 60, ThrottledAt: &early},
				{ProgressPercentage: 31, ETASeconds: 7200, ThrottledAt: &late},
				{ProgressPercentage: 55, ETASeconds: 900},
			},
			wantPercent: 31, wantPercentOK: true,
			wantETA: 7200 * time.Second, wantETAOK: true,
			wantThrottledAt: late, wantOperations: 3,
		},
		{
			name: "zero percent is a KNOWN zero, not unknown",
			ops: []DeployOperation{
				{ProgressPercentage: 0, ETASeconds: 0},
			},
			wantPercent: 0, wantPercentOK: true,
			wantETA: 0, wantETAOK: false, // a zero ETA carries no information
			wantOperations: 1,
		},
		{
			name:          "no operation rows reports nothing rather than inventing a value",
			wantPercentOK: false,
			wantETAOK:     false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dr := &DeployRequest{}
			dr.Deployment.DeployOperations = tc.ops
			dr.Deployment.DeployOperationSummaries = tc.summaries

			got := dr.Progress()
			if got.PercentKnown != tc.wantPercentOK || got.Percent != tc.wantPercent {
				t.Errorf("Percent = %d (known=%v); want %d (known=%v)", got.Percent, got.PercentKnown, tc.wantPercent, tc.wantPercentOK)
			}
			if got.ETAKnown != tc.wantETAOK || got.ETA != tc.wantETA {
				t.Errorf("ETA = %s (known=%v); want %s (known=%v)", got.ETA, got.ETAKnown, tc.wantETA, tc.wantETAOK)
			}
			if !got.ThrottledAt.Equal(tc.wantThrottledAt) {
				t.Errorf("ThrottledAt = %v; want %v", got.ThrottledAt, tc.wantThrottledAt)
			}
			if got.Operations != tc.wantOperations {
				t.Errorf("Operations = %d; want %d", got.Operations, tc.wantOperations)
			}
		})
	}
}
