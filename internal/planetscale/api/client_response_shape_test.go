// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Response-shape pins for EVERY PlanetScale endpoint whose response
// fields the client reads (audit MED-T4) — the v0.99.248 lesson
// applied to the class, not the instance: a fixture built by
// marshalling the client's OWN types is self-consistent and cannot
// catch a mis-modeled field (the nested deployment.deployable bug
// passed every such test while every REAL run timed out). Each test
// here serves raw JSON the client did not produce and asserts that
// every field the client reads decodes non-zero (or to an explicitly
// expected value), so a renamed/moved field fails the pin instead of
// silently zero-valuing.
//
// Fixture provenance, labeled per test (grep of docs/adr + workspace
// on 2026-07-15 found no verbatim captures in-repo beyond the DR #2
// GET-by-number capture already pinned in client_test.go):
//
//   - CAPTURED — sanitized verbatim capture of a real API response
//     (only the GET /deploy-requests/{n} pin in client_test.go so far).
//   - SHAPE LIVE-VERIFIED — the field layout was read off real
//     responses during the ADR-0148/0162 live runs (recorded in
//     client.go's type comments), but the body below is a sanitized
//     reconstruction, not a byte capture.
//   - DERIVED — constructed from the public PlanetScale API reference;
//     no live corroboration yet. The next live psverify dispatch
//     should replace these with sanitized captures (the ADR-0167
//     impl-note already says so for /diff).

package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// serveFixture wires a Client at a server that answers every request
// with body verbatim.
func serveFixture(t *testing.T, body string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return newTestClient(srv, nil)
}

// TestClient_GetBranch_ResponseShape pins the branch object's read
// fields against realistic full response bodies. SHAPE LIVE-VERIFIED:
// ready / safe_migrations / parent_branch / production were read off
// real branches throughout the ADR-0148/0162 runs; bodies sanitized.
//
// Two fixtures, because no single real branch exercises every read
// field non-zero: a production main (ready / production /
// safe_migrations all true; parent_branch is null — main has no
// parent, and null-into-string must stay "") and a dev branch
// (parent_branch present — the freshness-gate read).
func TestClient_GetBranch_ResponseShape(t *testing.T) {
	const realMainBranch = `{
	  "id": "ab1cd2efgh3i",
	  "type": "Branch",
	  "name": "main",
	  "created_at": "2026-07-01T00:00:00.000Z",
	  "updated_at": "2026-07-15T18:00:00.000Z",
	  "restore_checklist_completed_at": null,
	  "access_host_url": "aws.connect.psdb.cloud",
	  "schema_last_updated_at": "2026-07-15T18:00:00.000Z",
	  "mysql_address": "aws.connect.psdb.cloud",
	  "initial_restore_id": null,
	  "ready": true,
	  "production": true,
	  "sharded": false,
	  "shard_count": 0,
	  "safe_migrations": true,
	  "cluster_rate_name": "PS_10",
	  "parent_branch": null
	}`
	br, err := serveFixture(t, realMainBranch).GetBranch(context.Background(), "o", "d", "main")
	if err != nil {
		t.Fatalf("GetBranch: %v", err)
	}
	if br.Name != "main" {
		t.Errorf("Name = %q; a renamed name field would zero-value silently", br.Name)
	}
	if !br.Ready {
		t.Error("Ready = false on a ready:true body — the branch-ready poller would spin to timeout")
	}
	if !br.Production {
		t.Error("Production = false on a production:true body")
	}
	if !br.SafeMigrations {
		t.Error("SafeMigrations = false on a safe_migrations:true body — preflight would refuse a compliant branch")
	}
	if br.ParentBranch != "" {
		t.Errorf("ParentBranch = %q; main's parent_branch is null and must decode to the empty string", br.ParentBranch)
	}

	const realDevBranch = `{
	  "id": "xy9zw8vuts7r",
	  "type": "Branch",
	  "name": "sluice-expand-286cee0f33",
	  "created_at": "2026-07-15T18:01:00.000Z",
	  "updated_at": "2026-07-15T18:03:00.000Z",
	  "access_host_url": "aws.connect.psdb.cloud",
	  "schema_last_updated_at": "2026-07-15T18:03:00.000Z",
	  "ready": true,
	  "production": false,
	  "sharded": false,
	  "shard_count": 0,
	  "safe_migrations": false,
	  "cluster_rate_name": "PS_10",
	  "parent_branch": "main"
	}`
	dev, err := serveFixture(t, realDevBranch).GetBranch(context.Background(), "o", "d", "sluice-expand-286cee0f33")
	if err != nil {
		t.Fatalf("GetBranch(dev): %v", err)
	}
	if dev.ParentBranch != "main" {
		t.Errorf("ParentBranch = %q; a renamed/moved parent_branch would zero-value silently", dev.ParentBranch)
	}
	if !dev.Ready || dev.Production || dev.SafeMigrations {
		t.Errorf("dev branch reads = %+v; want ready:true production:false safe_migrations:false", dev)
	}
}

// TestClient_CreateBranch_ResponseShape pins the POST /branches
// response: the bare branch object (no data envelope), with the
// fresh-branch reality that ready is FALSE at creation — which is the
// whole reason callers poll GetBranch afterwards. DERIVED (the field
// mapping itself is pinned non-zero by the GetBranch fixtures above —
// both verbs decode the same Branch type; this pin proves the create
// response's envelope shape and the not-ready-at-birth reading).
func TestClient_CreateBranch_ResponseShape(t *testing.T) {
	const realCreateBranch = `{
	  "id": "qr5st6uvwx7y",
	  "type": "Branch",
	  "name": "sluice-expand-286cee0f33",
	  "created_at": "2026-07-15T18:01:00.000Z",
	  "updated_at": "2026-07-15T18:01:00.000Z",
	  "access_host_url": "aws.connect.psdb.cloud",
	  "ready": false,
	  "production": false,
	  "sharded": false,
	  "shard_count": 0,
	  "safe_migrations": false,
	  "cluster_rate_name": "PS_10",
	  "parent_branch": "main"
	}`
	br, err := serveFixture(t, realCreateBranch).CreateBranch(context.Background(), "o", "d", "sluice-expand-286cee0f33", "main")
	if err != nil {
		t.Fatalf("CreateBranch: %v", err)
	}
	if br.Name != "sluice-expand-286cee0f33" || br.ParentBranch != "main" {
		t.Errorf("create decoded %+v; want name + parent_branch from the bare (un-enveloped) object", br)
	}
	if br.Ready {
		t.Error("Ready = true on a fresh create body carrying ready:false — a just-created branch is not ready")
	}
}

// TestClient_CreateBranchPassword_ResponseShape pins the POST
// /passwords response fields the connector reads: id, username,
// plain_text (returned ONCE at creation), access_host_url. DERIVED
// from the PlanetScale API reference; the verb itself ran live in the
// ADR-0148/0162 chains but no sanitized capture exists in-repo.
func TestClient_CreateBranchPassword_ResponseShape(t *testing.T) {
	const realCreatePassword = `{
	  "id": "pw0a1b2c3d4e",
	  "type": "Password",
	  "name": "sluice-expand-contract",
	  "role": "admin",
	  "renewable": false,
	  "created_at": "2026-07-15T18:02:00.000Z",
	  "deleted_at": null,
	  "expires_at": null,
	  "ttl_seconds": null,
	  "username": "u1v2w3x4y5z6",
	  "access_host_url": "aws.connect.psdb.cloud",
	  "plain_text": "pscale_pw_fixture_not_a_real_secret",
	  "database_branch": {
	    "name": "sluice-expand-286cee0f33",
	    "id": "xy9zw8vuts7r",
	    "production": false
	  }
	}`
	pw, err := serveFixture(t, realCreatePassword).CreateBranchPassword(context.Background(), "o", "d", "dev", "sluice-expand-contract")
	if err != nil {
		t.Fatalf("CreateBranchPassword: %v", err)
	}
	if pw.ID != "pw0a1b2c3d4e" {
		t.Errorf("ID = %q; branch-password cleanup would target nothing", pw.ID)
	}
	if pw.Username != "u1v2w3x4y5z6" || pw.PlainText != "pscale_pw_fixture_not_a_real_secret" {
		t.Errorf("credentials = %q/%q; a renamed username/plain_text field means unusable DSNs, discovered only at connect time", pw.Username, pw.PlainText)
	}
	if pw.AccessHostURL != "aws.connect.psdb.cloud" {
		t.Errorf("AccessHostURL = %q; a renamed access_host_url would zero-value silently", pw.AccessHostURL)
	}
}

// TestClient_GetBranchSchema_ResponseShape pins GET /branches/{b}/schema
// against the {"data":[{"name","html","raw","annotated"}]} envelope.
// SHAPE LIVE-VERIFIED 2026-07-15 (the ADR-0162 freshness-gate work read
// this exact layout off a real PS-10); DDL bodies sanitized.
func TestClient_GetBranchSchema_ResponseShape(t *testing.T) {
	const realBranchSchema = `{
	  "data": [
	    {
	      "name": "items",
	      "html": "<div class=\"line\">CREATE TABLE...</div>",
	      "raw": "CREATE TABLE ` + "`items`" + ` (\n  ` + "`id`" + ` bigint NOT NULL,\n  PRIMARY KEY (` + "`id`" + `)\n) ENGINE InnoDB",
	      "annotated": "CREATE TABLE ` + "`items`" + ` ..."
	    },
	    {
	      "name": "orders",
	      "html": "<div>...</div>",
	      "raw": "CREATE TABLE ` + "`orders`" + ` (...)",
	      "annotated": "..."
	    }
	  ]
	}`
	tables, err := serveFixture(t, realBranchSchema).GetBranchSchema(context.Background(), "o", "d", "main")
	if err != nil {
		t.Fatalf("GetBranchSchema: %v", err)
	}
	if len(tables) != 2 || tables[0].Name != "items" || tables[1].Name != "orders" {
		t.Fatalf("tables = %+v; want both names decoded in order from the data envelope", tables)
	}
	for _, tb := range tables {
		if tb.Raw == "" {
			t.Errorf("table %q: Raw = \"\"; the freshness gate compares raw DDL — a renamed raw field would make every branch look identical (stale bases undetectable)", tb.Name)
		}
	}
}

// TestClient_Backups_ResponseShape pins POST/GET backups: id + state,
// the two fields the branch-rebase flow drives (state walks
// pending/running → success). SHAPE LIVE-VERIFIED 2026-07-15 (the
// ADR-0162 stale-base rebase ran against real backups); bodies
// sanitized.
func TestClient_Backups_ResponseShape(t *testing.T) {
	const realCreateBackup = `{
	  "id": "bkp1a2b3c4d5",
	  "type": "Backup",
	  "name": "sluice branch rebase",
	  "state": "pending",
	  "created_at": "2026-07-15T18:05:00.000Z",
	  "updated_at": "2026-07-15T18:05:00.000Z",
	  "size": 0,
	  "estimated_storage_cost": "0.0"
	}`
	bk, err := serveFixture(t, realCreateBackup).CreateBackup(context.Background(), "o", "d", "main")
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if bk.ID != "bkp1a2b3c4d5" {
		t.Errorf("ID = %q; GetBackup polling would 404 on the empty id", bk.ID)
	}
	if bk.State != "pending" {
		t.Errorf("State = %q; a renamed state field would poll forever (empty never reaches success)", bk.State)
	}

	const realGetBackup = `{
	  "id": "bkp1a2b3c4d5",
	  "type": "Backup",
	  "name": "sluice branch rebase",
	  "state": "success",
	  "created_at": "2026-07-15T18:05:00.000Z",
	  "updated_at": "2026-07-15T18:09:00.000Z",
	  "size": 1073741824,
	  "estimated_storage_cost": "0.02"
	}`
	got, err := serveFixture(t, realGetBackup).GetBackup(context.Background(), "o", "d", "main", "bkp1a2b3c4d5")
	if err != nil {
		t.Fatalf("GetBackup: %v", err)
	}
	if got.ID != "bkp1a2b3c4d5" || got.State != "success" {
		t.Errorf("GetBackup decoded %+v; want id + terminal state", got)
	}
}

// TestClient_CreateDeployRequest_ResponseShape pins the POST
// /deploy-requests response. DERIVED from the API reference, with one
// deliberate shape choice: "deployment": null — a just-created DR may
// not carry its deployment object yet, and decoding must tolerate
// that (CanDeploy simply reads false until a poll shows otherwise).
func TestClient_CreateDeployRequest_ResponseShape(t *testing.T) {
	const realCreateDeployRequest = `{
	  "id": "dr9f8e7d6c5b",
	  "type": "DeployRequest",
	  "number": 3,
	  "branch": "sluice-expand-286cee0f33",
	  "into_branch": "main",
	  "approved": false,
	  "state": "open",
	  "deployment_state": "pending",
	  "deployment": null,
	  "html_url": "https://app.planetscale.com/o/d/deploy-requests/3"
	}`
	dr, err := serveFixture(t, realCreateDeployRequest).CreateDeployRequest(context.Background(), "o", "d", "dev", "main")
	if err != nil {
		t.Fatalf("CreateDeployRequest: %v", err)
	}
	if dr.Number != 3 {
		t.Errorf("Number = %d; every follow-up verb addresses the DR by number — zero would target DR 0", dr.Number)
	}
	if dr.Branch != "sluice-expand-286cee0f33" || dr.IntoBranch != "main" {
		t.Errorf("branches = %q → %q; want both legs decoded", dr.Branch, dr.IntoBranch)
	}
	if dr.State != "open" || dr.DeploymentState != "pending" {
		t.Errorf("state/deployment_state = %q/%q; the poller keys on these", dr.State, dr.DeploymentState)
	}
	if dr.HTMLURL == "" {
		t.Error("HTMLURL = \"\"; refusal messages attach the DR URL for the operator")
	}
	if dr.CanDeploy() {
		t.Error("CanDeploy() = true on a deployment:null create body; deployability must come from polling")
	}
}

// TestClient_Deploy_ResponseShape pins the POST .../deploy response:
// the DR object with the deployment queued. DERIVED (lifecycle strings
// per the ADR-0148 ground truth: ready → queued → in_progress → ...).
func TestClient_Deploy_ResponseShape(t *testing.T) {
	const realDeploy = `{
	  "id": "dr9f8e7d6c5b",
	  "type": "DeployRequest",
	  "number": 3,
	  "branch": "sluice-expand-286cee0f33",
	  "into_branch": "main",
	  "state": "open",
	  "deployment_state": "queued",
	  "deployment": {
	    "id": "dep4g5h6i7j8",
	    "type": "Deployment",
	    "into_branch": "main",
	    "deploy_request_number": 3,
	    "deployable": true,
	    "state": "queued",
	    "strategy": "online"
	  },
	  "html_url": "https://app.planetscale.com/o/d/deploy-requests/3"
	}`
	dr, err := serveFixture(t, realDeploy).Deploy(context.Background(), "o", "d", 3)
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if dr.Number != 3 || dr.DeploymentState != "queued" {
		t.Errorf("deploy decoded %+v; want number + the queued deployment_state the poller resumes from", dr)
	}
	if dr.Deployment.State != "queued" {
		t.Errorf("Deployment.State = %q; the nested deployment object must keep decoding (the v248 field)", dr.Deployment.State)
	}
}

// TestClient_SkipRevert_ResponseShape pins the POST .../skip-revert
// response: the DR with its revert window closed. DERIVED (the state
// pair reflects ADR-0148 finding #4: skip-revert finalizes a
// complete_pending_revert deployment).
func TestClient_SkipRevert_ResponseShape(t *testing.T) {
	const realSkipRevert = `{
	  "id": "dr9f8e7d6c5b",
	  "type": "DeployRequest",
	  "number": 3,
	  "branch": "sluice-expand-286cee0f33",
	  "into_branch": "main",
	  "state": "closed",
	  "deployment_state": "complete",
	  "deployment": {
	    "id": "dep4g5h6i7j8",
	    "type": "Deployment",
	    "deploy_request_number": 3,
	    "deployable": false,
	    "state": "complete",
	    "strategy": "online"
	  },
	  "html_url": "https://app.planetscale.com/o/d/deploy-requests/3"
	}`
	dr, err := serveFixture(t, realSkipRevert).SkipRevert(context.Background(), "o", "d", 3)
	if err != nil {
		t.Fatalf("SkipRevert: %v", err)
	}
	if dr.Number != 3 || dr.State != "closed" || dr.DeploymentState != "complete" {
		t.Errorf("skip-revert decoded %+v; want number + closed/complete (the finalized-deployment reading)", dr)
	}
}
