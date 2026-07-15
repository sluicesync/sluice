// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit tests for the expand-contract orchestrator (ADR-0162): the
// deploy-request state-machine poller against an httptest mock PS API
// (happy path, error state, no_changes, timeout), the preflight
// refusals (safe-migrations disabled, missing table, no PK), the
// contract gate (verify-dirty ⇒ no contract; no --yes ⇒ stop with
// instructions; --yes + clean ⇒ proceeds), cleanup-on-failure /
// --keep-branches, the leftover-branch refusal, --resume-from, and
// the --dry-run zero-control-plane-calls pin.

package expandcontract

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/planetscale/api"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// ---- fake PlanetScale control plane ----

// fakePS is a minimal in-memory PlanetScale API: branches + deploy
// requests, with a scripted deployment_state sequence per DR and a
// total-call counter (the --dry-run zero-calls pin).
type fakePS struct {
	t *testing.T

	mu             sync.Mutex
	calls          int
	safeMigrations bool
	branches       map[string]*api.Branch
	drs            map[int]*fakeDR
	nextDR         int
	deleted        []string

	// preStates / postStates are the deployment_state scripts each
	// NEWLY created DR walks before / after its deploy call.
	preStates  []string
	postStates []string

	// staleNextBranches marks the next N created dev branches as
	// seeded from a stale backup: their /schema differs from
	// production's until recreated (the PlanetScale
	// branch-from-last-backup behavior, live-caught 2026-07-15).
	staleNextBranches int
	staleBranch       map[string]bool
	// backups counts on-demand backups taken; backupStates scripts the
	// GET-backup state walk (default: immediately "success").
	backups      int
	backupStates []string
}

type fakeDR struct {
	dr *api.DeployRequest
	// states is the deployment_state sequence returned by successive
	// GETs after the deploy call; the last entry repeats forever.
	states []string
	// preStates plays before the deploy call (the deployable wait);
	// empty means immediately deployable.
	preStates  []string
	deployed   bool
	skipRevert bool
}

func newFakePS(t *testing.T) *fakePS {
	return &fakePS{
		t:              t,
		safeMigrations: true,
		branches:       map[string]*api.Branch{"main": {Name: "main", Ready: true, Production: true}},
		drs:            map[int]*fakeDR{},
		nextDR:         1,
		postStates:     []string{"queued", "in_progress", "complete_pending_revert"},
		staleBranch:    map[string]bool{},
	}
}

func (f *fakePS) serve() (*httptest.Server, *api.Client) {
	srv := httptest.NewServer(http.HandlerFunc(f.handle))
	f.t.Cleanup(srv.Close)
	client := api.New(api.Config{
		TokenID: "id", Token: "secret", BaseURL: srv.URL,
		Sleep: func(context.Context, time.Duration) error { return nil },
	})
	return srv, client
}

func (f *fakePS) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakePS) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// v1 organizations o databases d <resource> ...
	if len(parts) < 6 || parts[0] != "v1" {
		http.NotFound(w, r)
		return
	}
	resource, rest := parts[5], parts[6:]
	switch resource {
	case "branches":
		f.handleBranches(w, r, rest)
	case "deploy-requests":
		f.handleDeployRequests(w, r, rest)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakePS) handleBranches(w http.ResponseWriter, r *http.Request, rest []string) {
	switch {
	case len(rest) == 0 && r.Method == http.MethodPost:
		var body struct {
			Name         string `json:"name"`
			ParentBranch string `json:"parent_branch"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		br := &api.Branch{Name: body.Name, ParentBranch: body.ParentBranch, Ready: true}
		f.branches[body.Name] = br
		if f.staleNextBranches > 0 {
			f.staleNextBranches--
			f.staleBranch[body.Name] = true
		}
		writeJSON(w, br)
	case len(rest) == 2 && rest[1] == "schema" && r.Method == http.MethodGet:
		if _, ok := f.branches[rest[0]]; !ok {
			writeNotFound(w)
			return
		}
		raw := "CREATE TABLE `items` (id bigint) -- current"
		if f.staleBranch[rest[0]] {
			raw = "CREATE TABLE `items` (id bigint) -- stale backup base"
		}
		writeJSON(w, map[string]any{"data": []map[string]string{{"name": "items", "raw": raw}}})
	case len(rest) == 2 && rest[1] == "backups" && r.Method == http.MethodPost:
		f.backups++
		writeJSON(w, api.Backup{ID: "bk" + strconv.Itoa(f.backups), State: "pending"})
	case len(rest) == 3 && rest[1] == "backups" && r.Method == http.MethodGet:
		state := "success"
		if len(f.backupStates) > 0 {
			state = f.backupStates[0]
			if len(f.backupStates) > 1 {
				f.backupStates = f.backupStates[1:]
			}
		}
		writeJSON(w, api.Backup{ID: rest[2], State: state})
	case len(rest) == 1 && r.Method == http.MethodGet:
		br, ok := f.branches[rest[0]]
		if !ok {
			writeNotFound(w)
			return
		}
		out := *br
		out.SafeMigrations = f.safeMigrations && out.Name == "main"
		writeJSON(w, out)
	case len(rest) == 1 && r.Method == http.MethodDelete:
		if _, ok := f.branches[rest[0]]; !ok {
			writeNotFound(w)
			return
		}
		delete(f.branches, rest[0])
		delete(f.staleBranch, rest[0])
		f.deleted = append(f.deleted, rest[0])
		writeJSON(w, map[string]any{})
	case len(rest) == 2 && rest[1] == "passwords" && r.Method == http.MethodPost:
		writeJSON(w, api.BranchPassword{
			ID: "pwid", Username: "u", PlainText: "p", AccessHostURL: "host.example",
		})
	default:
		http.NotFound(w, r)
	}
}

func (f *fakePS) handleDeployRequests(w http.ResponseWriter, r *http.Request, rest []string) {
	switch {
	case len(rest) == 0 && r.Method == http.MethodPost:
		var body struct {
			Branch     string `json:"branch"`
			IntoBranch string `json:"into_branch"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		n := f.nextDR
		f.nextDR++
		fdr := &fakeDR{
			dr: &api.DeployRequest{
				Number: n, Branch: body.Branch, IntoBranch: body.IntoBranch,
				State: "open", DeploymentState: "pending",
				HTMLURL: "https://app.planetscale.com/o/d/deploy-requests/" + strconv.Itoa(n),
			},
			preStates: append([]string(nil), f.preStates...),
			states:    append([]string(nil), f.postStates...),
		}
		f.drs[n] = fdr
		writeJSON(w, fdr.dr)
	case len(rest) == 1 && r.Method == http.MethodGet:
		n, _ := strconv.Atoi(rest[0])
		fdr, ok := f.drs[n]
		if !ok {
			writeNotFound(w)
			return
		}
		writeJSON(w, fdr.snapshot())
	case len(rest) == 2 && r.Method == http.MethodPost:
		n, _ := strconv.Atoi(rest[0])
		fdr, ok := f.drs[n]
		if !ok {
			writeNotFound(w)
			return
		}
		switch rest[1] {
		case "deploy":
			fdr.deployed = true
			writeJSON(w, fdr.snapshot())
		case "skip-revert":
			fdr.skipRevert = true
			fdr.dr.DeploymentState = "complete"
			writeJSON(w, fdr.dr)
		default:
			http.NotFound(w, r)
		}
	default:
		http.NotFound(w, r)
	}
}

// snapshot advances the scripted state walk one step per GET. The
// deployable flag is served ONLY inside the nested deployment object,
// mirroring the real GET-by-number response shape (which has no
// top-level "deployable" — the live-caught 2026-07-15 field-location
// bug); these tests must exercise the shape the real API serves.
func (fd *fakeDR) snapshot() *api.DeployRequest {
	out := *fd.dr
	if !fd.deployed {
		if len(fd.preStates) > 0 {
			out.DeploymentState = fd.preStates[0]
			if len(fd.preStates) > 1 {
				fd.preStates = fd.preStates[1:]
			}
			out.Deployment.State = out.DeploymentState
			out.Deployment.Deployable = out.DeploymentState == "ready"
			return &out
		}
		out.DeploymentState = "ready"
		out.Deployment.State = "ready"
		out.Deployment.Deployable = true
		return &out
	}
	out.DeploymentState = fd.states[0]
	if len(fd.states) > 1 {
		fd.states = fd.states[1:]
	}
	fd.dr.DeploymentState = out.DeploymentState
	return &out
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeNotFound(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": "not_found", "message": "Not Found"})
}

// ---- fake data plane (engine + executor + store) ----

// ecFakeExecutor mirrors the pipeline backfill fake at the scale this
// package needs: an in-memory int-PK table where "where" semantics are
// `new IS NULL` and "set" is `new = old`.
type ecFakeExecutor struct {
	mu        sync.Mutex
	rows      []ecRow
	execCalls int
	// remainingAfterWalk overrides CountRemaining once the walk is done
	// (the verify-dirty shape: rows written behind the cursor).
	remainingAfterWalk int64
	walked             bool
}

type ecRow struct {
	pk     int64
	filled bool
}

func (f *ecFakeExecutor) idx(after []any) int {
	if len(after) == 0 {
		return 0
	}
	target := after[0].(int64)
	for i, r := range f.rows {
		if r.pk > target {
			return i
		}
	}
	return len(f.rows)
}

func (f *ecFakeExecutor) NextChunkUpperBound(_ context.Context, _ *ir.Table, after []any, limit int) (upper []any, ok bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	start := f.idx(after)
	if start >= len(f.rows) {
		f.walked = true
		return nil, false, nil
	}
	end := start + limit
	if end > len(f.rows) {
		end = len(f.rows)
	}
	return []any{f.rows[end-1].pk}, true, nil
}

func (f *ecFakeExecutor) ExecBackfillChunk(_ context.Context, _ *ir.Table, _ []ir.BackfillSet, _ string, after, upper []any) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execCalls++
	var n int64
	start, endPK := f.idx(after), upper[0].(int64)
	for i := start; i < len(f.rows) && f.rows[i].pk <= endPK; i++ {
		if !f.rows[i].filled {
			f.rows[i].filled = true
			n++
		}
	}
	return n, nil
}

func (f *ecFakeExecutor) BackfillStatement(_ *ir.Table, sets []ir.BackfillSet, where string) (string, error) {
	return fmt.Sprintf("UPDATE `t` SET %s WHERE (`id`) > (?) AND (`id`) <= (?) AND (%s)", sets[0].Column+" = "+sets[0].Expr, where), nil
}

func (f *ecFakeExecutor) CountRemaining(context.Context, *ir.Table, string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.walked && f.remainingAfterWalk > 0 {
		return f.remainingAfterWalk, nil
	}
	var n int64
	for _, r := range f.rows {
		if !r.filled {
			n++
		}
	}
	return n, nil
}

func (f *ecFakeExecutor) Close() error { return nil }

// ecFakeStore is an in-memory ir.MigrationStateStore.
type ecFakeStore struct {
	mu       sync.Mutex
	states   map[string]ir.MigrationState
	progress map[string]map[string]ir.TableProgress
}

func newECFakeStore() *ecFakeStore {
	return &ecFakeStore{states: map[string]ir.MigrationState{}, progress: map[string]map[string]ir.TableProgress{}}
}

func (s *ecFakeStore) EnsureControlTable(context.Context) error { return nil }

func (s *ecFakeStore) Read(_ context.Context, id string) (ir.MigrationState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.states[id]
	if ok {
		st.TableProgress = s.progress[id]
	}
	return st, ok, nil
}

func (s *ecFakeStore) Write(_ context.Context, st ir.MigrationState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[st.MigrationID] = st
	return nil
}

func (s *ecFakeStore) WriteTableProgress(_ context.Context, id, table string, p ir.TableProgress) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.progress[id] == nil {
		s.progress[id] = map[string]ir.TableProgress{}
	}
	s.progress[id][table] = p
	return nil
}

func (s *ecFakeStore) ClearMigration(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.states, id)
	delete(s.progress, id)
	return nil
}

func (s *ecFakeStore) Close() error { return nil }

// ecStubBase panics on every ir.Engine method the tests must not
// reach (the pipeline stubEngine posture).
type ecStubBase struct{}

func (ecStubBase) Capabilities() ir.Capabilities { return ir.Capabilities{} }
func (ecStubBase) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	panic("unexpected OpenSchemaWriter")
}

func (ecStubBase) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	panic("unexpected OpenRowReader")
}

func (ecStubBase) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	panic("unexpected OpenRowWriter")
}

func (ecStubBase) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	panic("unexpected OpenCDCReader")
}

func (ecStubBase) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	panic("unexpected OpenChangeApplier")
}

func (ecStubBase) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	panic("unexpected OpenSnapshotStream")
}

type ecFakeSchemaReader struct{ schema *ir.Schema }

func (r ecFakeSchemaReader) ReadSchema(context.Context) (*ir.Schema, error) { return r.schema, nil }

type ecFakeEngine struct {
	ecStubBase
	schema *ir.Schema
	ex     *ecFakeExecutor
	store  *ecFakeStore
}

func (e *ecFakeEngine) Name() string { return "planetscale" }
func (e *ecFakeEngine) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return ecFakeSchemaReader{schema: e.schema}, nil
}

func (e *ecFakeEngine) OpenBackfillExecutor(context.Context, string) (ir.BackfillExecutor, error) {
	return e.ex, nil
}

func (e *ecFakeEngine) OpenMigrationStateStore(context.Context, string) (ir.MigrationStateStore, error) {
	return e.store, nil
}

// ---- fixtures ----

// ecSchema is the POST-expand table shape: the backfill leg re-reads
// the schema after the expand deploy, so the fake includes new_col
// from the start (the orchestrator's own preflight deliberately never
// checks --set columns pre-expand — the expand leg creates them).
func ecSchema(pk *ir.Index) *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "items",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "old_col", Type: ir.Integer{Width: 64}},
			{Name: "new_col", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: pk,
	}}}
}

func ecIntPK() *ir.Index { return &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}} }

func ecRows(n int) []ecRow {
	rows := make([]ecRow, n)
	for i := range rows {
		rows[i] = ecRow{pk: int64(i + 1)}
	}
	return rows
}

// ddlRecorder is the injected ExecDDL + EnsureStateOnBranch fake.
type ddlRecorder struct {
	mu            sync.Mutex
	ddls          []string
	stateStagings int
	stateErr      error
}

func (d *ddlRecorder) exec(_ context.Context, pw *api.BranchPassword, _, ddl string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if pw == nil || pw.PlainText == "" {
		return errors.New("no branch password")
	}
	d.ddls = append(d.ddls, ddl)
	return nil
}

func (d *ddlRecorder) ensureState(_ context.Context, pw *api.BranchPassword, _ string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if pw == nil || pw.PlainText == "" {
		return errors.New("no branch password")
	}
	if d.stateErr != nil {
		return d.stateErr
	}
	d.stateStagings++
	return nil
}

// newTestOrchestrator wires a full happy-path orchestrator; tests
// mutate what they need.
func newTestOrchestrator(t *testing.T, ps *fakePS) (*Orchestrator, *ecFakeEngine, *ddlRecorder, *bytes.Buffer) {
	t.Helper()
	_, client := ps.serve()
	eng := &ecFakeEngine{schema: ecSchema(ecIntPK()), ex: &ecFakeExecutor{rows: ecRows(10)}, store: newECFakeStore()}
	rec := &ddlRecorder{}
	out := &bytes.Buffer{}
	return &Orchestrator{
		API:                 client,
		Org:                 "o",
		Database:            "d",
		Engine:              eng,
		DSN:                 "dsn",
		Table:               "items",
		Sets:                []ir.BackfillSet{{Column: "new_col", Expr: "old_col"}},
		Where:               "new_col IS NULL",
		ExpandDDL:           "ALTER TABLE items ADD COLUMN new_col BIGINT",
		ContractDDL:         "ALTER TABLE items DROP COLUMN old_col",
		Yes:                 true,
		PollInterval:        time.Millisecond,
		DeployTimeout:       5 * time.Second,
		Out:                 out,
		ExecDDL:             rec.exec,
		EnsureStateOnBranch: rec.ensureState,
	}, eng, rec, out
}

func wantCode(t *testing.T, err error, code sluicecode.Code) {
	t.Helper()
	if err == nil {
		t.Fatal("want a coded error; got nil")
	}
	var coded *sluicecode.CodedError
	if !errors.As(err, &coded) {
		t.Fatalf("want *sluicecode.CodedError; got %T: %v", err, err)
	}
	if coded.Code != code {
		t.Errorf("code = %s; want %s", coded.Code, code)
	}
}

// ---- happy path ----

func TestExpandContract_HappyPathFullPattern(t *testing.T) {
	ps := newFakePS(t)
	o, eng, rec, _ := newTestOrchestrator(t, ps)

	result, err := o.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.ExpandDeployRequest == 0 || result.ContractDeployRequest == 0 || !result.ContractRun || !result.Verified {
		t.Errorf("result = %+v; want both DRs + contract run + verified", result)
	}
	if result.Backfill == nil || result.Backfill.RowsUpdated != 10 {
		t.Errorf("backfill result = %+v; want 10 rows updated", result.Backfill)
	}
	if len(rec.ddls) != 2 || rec.ddls[0] != o.ExpandDDL || rec.ddls[1] != o.ContractDDL {
		t.Errorf("DDLs applied = %q; want expand then contract", rec.ddls)
	}
	// The migrate-state control tables are staged on the EXPAND dev
	// branch only (they ship to production inside the expand deploy
	// request — safe migrations blocks creating them there directly;
	// live-caught 2026-07-15). Exactly once: the contract leg must not
	// re-stage.
	if rec.stateStagings != 1 {
		t.Errorf("state stagings = %d; want exactly 1 (expand leg only)", rec.stateStagings)
	}
	// Both scripted deploys ended in complete_pending_revert, so both
	// must have been finalized via skip-revert.
	for n, fdr := range ps.drs {
		if !fdr.skipRevert {
			t.Errorf("DR #%d was not skip-reverted (revert window left open)", n)
		}
	}
	// Cleanup deleted both dev branches.
	if len(ps.deleted) != 2 {
		t.Errorf("deleted branches = %v; want the two sluice dev branches", ps.deleted)
	}
	for _, name := range ps.deleted {
		if !strings.HasPrefix(name, "sluice-expand-") && !strings.HasPrefix(name, "sluice-contract-") {
			t.Errorf("deleted unexpected branch %q", name)
		}
	}
	if eng.ex.execCalls == 0 {
		t.Error("backfill executed no chunks")
	}
}

// TestExpandContract_StateStagingFailureFailsExpandLeg pins that a
// failure staging the migrate-state tables on the expand dev branch
// fails the run loudly BEFORE a deploy request opens (nothing to ship
// without the state tables — the migrate leg would die on production's
// safe-migrations DDL block).
func TestExpandContract_StateStagingFailureFailsExpandLeg(t *testing.T) {
	ps := newFakePS(t)
	o, _, rec, _ := newTestOrchestrator(t, ps)
	rec.stateErr = errors.New("branch conn refused")

	_, err := o.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "branch conn refused") {
		t.Fatalf("err = %v; want the staging failure surfaced", err)
	}
	if len(ps.drs) != 0 {
		t.Errorf("a deploy request was opened (%d) despite the staging failure", len(ps.drs))
	}
	// Cleanup still removed the expand dev branch.
	if len(ps.deleted) != 1 || !strings.HasPrefix(ps.deleted[0], "sluice-expand-") {
		t.Errorf("deleted = %v; want just the expand dev branch", ps.deleted)
	}
}

// TestExpandContract_StaleBranchRebasedViaBackup pins the freshness
// gate's self-heal: a dev branch seeded from a stale backup
// (PlanetScale branches from the parent's most recent backup — a
// branch created 14 minutes after a deploy still lacked the deployed
// column, live-caught 2026-07-15) is detected by schema comparison,
// deleted, and recreated after an on-demand production backup; the
// run then completes normally. Without the gate, the deploy request
// from the stale base silently REVERTS every production schema change
// newer than the backup — on the contract leg that drops the freshly
// backfilled expand column.
func TestExpandContract_StaleBranchRebasedViaBackup(t *testing.T) {
	ps := newFakePS(t)
	ps.staleNextBranches = 1
	o, _, _, out := newTestOrchestrator(t, ps)

	result, err := o.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !result.ContractRun || !result.Verified {
		t.Errorf("result = %+v; want full pattern", result)
	}
	if ps.backups != 1 {
		t.Errorf("backups taken = %d; want exactly 1 (the rebase)", ps.backups)
	}
	if !strings.Contains(out.String(), "taking a fresh backup to rebase") {
		t.Errorf("narration missing the rebase explanation:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "now matches") {
		t.Errorf("narration missing the rebased-branch confirmation:\n%s", out.String())
	}
}

// TestExpandContract_StillStaleAfterBackupRefusesCoded pins the
// bounded end of the self-heal: when the recreated branch STILL
// differs from production after a fresh backup, the run refuses with
// the coded error instead of deploying a schema-reverting DR.
func TestExpandContract_StillStaleAfterBackupRefusesCoded(t *testing.T) {
	ps := newFakePS(t)
	ps.staleNextBranches = 2 // the rebased branch comes back stale too
	o, _, _, _ := newTestOrchestrator(t, ps)

	_, err := o.Run(context.Background())
	wantCode(t, err, sluicecode.CodePSBranchStaleBase)
	if len(ps.drs) != 0 {
		t.Errorf("a deploy request was opened (%d) from a stale base", len(ps.drs))
	}
}

// ---- preflight refusals ----

func TestExpandContract_RefusesSafeMigrationsDisabled(t *testing.T) {
	ps := newFakePS(t)
	ps.safeMigrations = false
	o, _, rec, _ := newTestOrchestrator(t, ps)

	_, err := o.Run(context.Background())
	wantCode(t, err, sluicecode.CodePSSafeMigrationsDisabled)
	if !strings.Contains(err.Error(), "safe migrations") {
		t.Errorf("refusal %q should name the toggle", err)
	}
	// Refused before anything irreversible: no branch, no DDL, no DR.
	if len(ps.branches) != 1 || len(ps.drs) != 0 || len(rec.ddls) != 0 {
		t.Errorf("safe-migrations refusal touched the control plane: branches=%v drs=%d ddls=%q",
			ps.branches, len(ps.drs), rec.ddls)
	}
}

func TestExpandContract_RefusesMissingTableBeforeAnyControlPlaneWrite(t *testing.T) {
	ps := newFakePS(t)
	o, eng, _, _ := newTestOrchestrator(t, ps)
	eng.schema = &ir.Schema{} // no tables

	_, err := o.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Run = %v; want a table-not-found preflight error", err)
	}
	if ps.callCount() != 0 {
		t.Errorf("data-plane preflight failure made %d control-plane calls; want 0", ps.callCount())
	}
}

func TestExpandContract_RefusesNoPrimaryKey(t *testing.T) {
	ps := newFakePS(t)
	o, eng, _, _ := newTestOrchestrator(t, ps)
	eng.schema = ecSchema(nil)

	_, err := o.Run(context.Background())
	wantCode(t, err, sluicecode.CodeBackfillNoPrimaryKey)
	if ps.callCount() != 0 {
		t.Errorf("no-PK refusal made %d control-plane calls; want 0", ps.callCount())
	}
}

func TestExpandContract_RefusesMissingBranch(t *testing.T) {
	ps := newFakePS(t)
	o, _, _, _ := newTestOrchestrator(t, ps)
	o.Branch = "nope"

	_, err := o.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), `branch "nope"`) {
		t.Fatalf("Run = %v; want a branch-not-found preflight error", err)
	}
}

func TestExpandContract_ValidateRefusals(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Orchestrator)
		want   string
	}{
		{"missing where", func(o *Orchestrator) { o.Where = "" }, "Where is required"},
		{"missing expand ddl", func(o *Orchestrator) { o.ExpandDDL = "" }, "ExpandDDL is required"},
		{"missing sets", func(o *Orchestrator) { o.Sets = nil }, "at least one Set"},
		{"resume-from contract without contract ddl", func(o *Orchestrator) {
			o.ResumeFrom = LegContract
			o.ContractDDL = ""
		}, "requires ContractDDL"},
		{"unknown leg", func(o *Orchestrator) { o.ResumeFrom = "vibes" }, "unknown resume leg"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ps := newFakePS(t)
			o, _, _, _ := newTestOrchestrator(t, ps)
			tc.mutate(o)
			_, err := o.Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Run = %v; want error containing %q", err, tc.want)
			}
			if ps.callCount() != 0 {
				t.Errorf("validation refusal made %d control-plane calls; want 0", ps.callCount())
			}
		})
	}
}

// ---- the contract gate ----

func TestExpandContract_VerifyDirtyBlocksContractAndCleansUp(t *testing.T) {
	ps := newFakePS(t)
	o, eng, rec, _ := newTestOrchestrator(t, ps)
	eng.ex.remainingAfterWalk = 3 // rows written behind the cursor

	_, err := o.Run(context.Background())
	wantCode(t, err, sluicecode.CodeBackfillIncomplete)
	// The contract leg never ran: one DR (expand), one DDL.
	if len(ps.drs) != 1 || len(rec.ddls) != 1 {
		t.Errorf("verify-dirty ran the contract leg: drs=%d ddls=%q", len(ps.drs), rec.ddls)
	}
	// Cleanup still deleted the expand branch.
	if len(ps.deleted) != 1 || !strings.HasPrefix(ps.deleted[0], "sluice-expand-") {
		t.Errorf("deleted = %v; want the expand dev branch cleaned up on failure", ps.deleted)
	}
}

func TestExpandContract_NoYesStopsAfterVerifyWithInstructions(t *testing.T) {
	ps := newFakePS(t)
	o, _, rec, out := newTestOrchestrator(t, ps)
	o.Yes = false

	result, err := o.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v (a gate stop is a designed success)", err)
	}
	if result.ContractRun || !result.Verified {
		t.Errorf("result = %+v; want verified but contract NOT run", result)
	}
	if len(ps.drs) != 1 || len(rec.ddls) != 1 {
		t.Errorf("no---yes ran the contract leg: drs=%d ddls=%q", len(ps.drs), rec.ddls)
	}
	report := out.String()
	if !strings.Contains(report, "--yes") || !strings.Contains(report, "--resume-from contract") {
		t.Errorf("stop report missing the exact resume instructions:\n%s", report)
	}
}

func TestExpandContract_NoContractDDLStopsAfterVerify(t *testing.T) {
	ps := newFakePS(t)
	o, _, _, out := newTestOrchestrator(t, ps)
	o.ContractDDL = ""

	result, err := o.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.ContractRun {
		t.Error("contract ran without --contract-ddl")
	}
	if !strings.Contains(out.String(), "--resume-from contract") {
		t.Errorf("stop report missing resume instructions:\n%s", out.String())
	}
}

// ---- resume legs ----

func TestExpandContract_ResumeFromMigrateSkipsExpand(t *testing.T) {
	ps := newFakePS(t)
	o, _, rec, _ := newTestOrchestrator(t, ps)
	o.ResumeFrom = LegMigrate

	result, err := o.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.ExpandDeployRequest != 0 {
		t.Errorf("expand DR = %d; want 0 (leg skipped)", result.ExpandDeployRequest)
	}
	if !result.ContractRun || result.Backfill == nil {
		t.Errorf("result = %+v; want migrate + contract run", result)
	}
	if len(rec.ddls) != 1 || rec.ddls[0] != o.ContractDDL {
		t.Errorf("DDLs = %q; want only the contract DDL", rec.ddls)
	}
}

func TestExpandContract_ResumeFromContractStillVerifies(t *testing.T) {
	ps := newFakePS(t)
	o, eng, rec, _ := newTestOrchestrator(t, ps)
	o.ResumeFrom = LegContract

	// Dirty table ⇒ the never-skippable verify gate blocks contract.
	_, err := o.Run(context.Background())
	wantCode(t, err, sluicecode.CodeBackfillIncomplete)
	if len(rec.ddls) != 0 || len(ps.drs) != 0 {
		t.Errorf("verify-dirty resume ran the contract leg: ddls=%q drs=%d", rec.ddls, len(ps.drs))
	}

	// Clean table ⇒ verify passes, contract deploys, walk never runs.
	for i := range eng.ex.rows {
		eng.ex.rows[i].filled = true
	}
	result, err := o.Run(context.Background())
	if err != nil {
		t.Fatalf("Run (clean): %v", err)
	}
	if !result.ContractRun || result.Backfill != nil {
		t.Errorf("result = %+v; want contract run with NO walk", result)
	}
	if eng.ex.execCalls != 0 {
		t.Errorf("resume-from contract executed %d backfill chunks; want 0", eng.ex.execCalls)
	}
}

// ---- leftover-branch refusal ----

func TestExpandContract_RefusesLeftoverDevBranch(t *testing.T) {
	ps := newFakePS(t)
	o, _, _, _ := newTestOrchestrator(t, ps)
	leftover := o.expandBranchName()
	ps.branches[leftover] = &api.Branch{Name: leftover, Ready: true}

	_, err := o.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "already exists") || !strings.Contains(err.Error(), "--resume-from migrate") {
		t.Fatalf("Run = %v; want the leftover-branch refusal with resume guidance", err)
	}
	// The leftover is NOT ours to delete: cleanup must leave it alone.
	if len(ps.deleted) != 0 {
		t.Errorf("deleted = %v; want the leftover branch untouched", ps.deleted)
	}
}

// ---- DR state machine ----

func TestExpandContract_DeployErrorStateIsCodedWithURL(t *testing.T) {
	ps := newFakePS(t)
	ps.postStates = []string{"queued", "error"}
	o, _, _, _ := newTestOrchestrator(t, ps)

	_, err := o.Run(context.Background())
	wantCode(t, err, sluicecode.CodePSDeployRequestFailed)
	msg := err.Error()
	if !strings.Contains(msg, `"error"`) || !strings.Contains(msg, "deploy-requests/1") {
		t.Errorf("error %q should carry the DR state and URL", msg)
	}
	// Cleanup ran on failure.
	if len(ps.deleted) != 1 {
		t.Errorf("deleted = %v; want the expand branch cleaned up after the failed deploy", ps.deleted)
	}
}

func TestExpandContract_DeployTimeoutIsCoded(t *testing.T) {
	ps := newFakePS(t)
	ps.postStates = []string{"in_progress"} // never terminal
	o, _, _, _ := newTestOrchestrator(t, ps)
	o.DeployTimeout = 50 * time.Millisecond

	_, err := o.Run(context.Background())
	wantCode(t, err, sluicecode.CodePSDeployRequestFailed)
	if !strings.Contains(err.Error(), "still deploying") {
		t.Errorf("timeout error = %q; want the still-deploying guidance", err)
	}
}

func TestExpandContract_NoChangesDiffRefusedWithResumeGuidance(t *testing.T) {
	ps := newFakePS(t)
	ps.preStates = []string{"no_changes"} // the deployable wait sees an empty diff
	o, _, _, _ := newTestOrchestrator(t, ps)

	_, err := o.Run(context.Background())
	wantCode(t, err, sluicecode.CodePSDeployRequestFailed)
	if !strings.Contains(err.Error(), "no schema changes") || !strings.Contains(err.Error(), "--resume-from migrate") {
		t.Errorf("no_changes error = %q; want the already-deployed guidance", err)
	}
}

// ---- unknown intermediate state tolerance ----

func TestExpandContract_UnknownIntermediateStateKeepsWaiting(t *testing.T) {
	ps := newFakePS(t)
	ps.postStates = []string{"queued", "some_future_state", "complete"}
	o, _, _, _ := newTestOrchestrator(t, ps)

	result, err := o.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v — an unknown intermediate state must not fail a deploy that completes", err)
	}
	if !result.ContractRun {
		t.Error("pattern did not complete through the unknown intermediate state")
	}
}

// ---- cleanup posture ----

func TestExpandContract_KeepBranchesSkipsCleanup(t *testing.T) {
	ps := newFakePS(t)
	o, _, _, out := newTestOrchestrator(t, ps)
	o.KeepBranches = true

	if _, err := o.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(ps.deleted) != 0 {
		t.Errorf("deleted = %v; want none under --keep-branches", ps.deleted)
	}
	if !strings.Contains(out.String(), "keeping dev branches") {
		t.Error("keep-branches cleanup did not narrate what it kept")
	}
}

// ---- dry run ----

func TestExpandContract_DryRunMakesZeroControlPlaneCalls(t *testing.T) {
	ps := newFakePS(t)
	o, eng, rec, out := newTestOrchestrator(t, ps)
	o.DryRun = true

	result, err := o.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if ps.callCount() != 0 {
		t.Errorf("--dry-run made %d control-plane calls; want ZERO (pinned)", ps.callCount())
	}
	if len(rec.ddls) != 0 || eng.ex.execCalls != 0 {
		t.Errorf("--dry-run executed work: ddls=%q chunks=%d", rec.ddls, eng.ex.execCalls)
	}
	if result.ContractRun {
		t.Error("--dry-run reported a contract run")
	}
	plan := out.String()
	for _, want := range []string{
		"--dry-run",
		o.expandBranchName(),
		o.contractBranchName(),
		o.ExpandDDL,
		o.ContractDDL,
		"UPDATE `t` SET new_col = old_col", // the rendered backfill statement
		"SLUICE-E-BACKFILL-INCOMPLETE",
	} {
		if !strings.Contains(plan, want) {
			t.Errorf("plan missing %q:\n%s", want, plan)
		}
	}
}

// ---- branch naming ----

func TestExpandContract_BranchNamesDeterministicAndSpecSensitive(t *testing.T) {
	ps := newFakePS(t)
	o, _, _, _ := newTestOrchestrator(t, ps)
	a, b := o.expandBranchName(), o.expandBranchName()
	if a != b {
		t.Errorf("expand branch name not deterministic: %q vs %q", a, b)
	}
	if !strings.HasPrefix(a, "sluice-expand-") {
		t.Errorf("expand branch name %q missing prefix", a)
	}
	o.ExpandDDL += " NULL"
	if c := o.expandBranchName(); c == a {
		t.Errorf("different DDL hashed to the same branch name %q", c)
	}
	if o.contractBranchName() == a {
		t.Error("contract branch name collides with expand")
	}
}
