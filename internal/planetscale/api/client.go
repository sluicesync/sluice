// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package api is sluice's thin PlanetScale control-plane HTTP client:
// raw JSON over https://api.planetscale.com/v1 with service-token
// auth, no planetscale-go SDK (the ADR-0148 posture, shared with the
// telemetry provider in internal/planetscale/telemetry — the two
// control-plane features deliberately ride ONE client, ADR-0162).
//
// The client is verbs-only: it knows how to authenticate, retry a 429,
// and decode the PlanetScale error envelope — workflow (deploy-request
// polling, branch lifecycle ordering) belongs to callers. Error
// strings never carry the token or the request URL (the audit N-12
// treatment: a *url.Error embeds the full URL verbatim, so transport
// errors pass through diagnose.SafeParseError).
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/diagnose"
)

const defaultBaseURL = "https://api.planetscale.com"

// rate-limit retry shape: a 429 is retried with a modest backoff
// (Retry-After honoured when present, capped), never more than
// maxAttempts total tries. Anything else fails straight through —
// callers own their higher-level polling.
const (
	maxAttempts     = 4
	baseRetryDelay  = time.Second
	maxRetryDelay   = 15 * time.Second
	maxErrorBodyLen = 64 * 1024
)

// Config configures a control-plane [Client]. TokenID/Token are the
// PlanetScale service-token halves (`Authorization: {ID}:{TOKEN}`,
// the pscale CLI convention); the secret is NEVER logged and never
// appears in an error string. The remaining fields are injectable for
// tests or have safe defaults.
type Config struct {
	TokenID string
	Token   string

	// BaseURL overrides the API host root (tests / self-host).
	// "" ⇒ https://api.planetscale.com.
	BaseURL string

	// HTTPClient is injected in tests; nil ⇒ a default client with a
	// per-request timeout.
	HTTPClient *http.Client

	// Sleep is the 429-backoff wait, injectable so tests don't spend
	// wall-clock time; nil ⇒ a real ctx-aware sleep.
	Sleep func(ctx context.Context, d time.Duration) error
}

// Client is the shared authenticated PlanetScale API client.
type Client struct {
	httpClient *http.Client
	baseURL    string
	tokenID    string
	token      string
	sleep      func(ctx context.Context, d time.Duration) error
}

// New constructs a Client, applying Config defaults.
func New(cfg Config) *Client {
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	sleep := cfg.Sleep
	if sleep == nil {
		sleep = ctxSleep
	}
	return &Client{
		httpClient: httpClient,
		baseURL:    strings.TrimRight(baseURL, "/"),
		tokenID:    cfg.TokenID,
		token:      cfg.Token,
		sleep:      sleep,
	}
}

// ctxSleep waits d or until ctx is done, whichever comes first.
func ctxSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// StatusError is a non-2xx control-plane response, carrying the
// PlanetScale error envelope ({"code": ..., "message": ...}) when the
// body had one. It never carries the request URL or the token.
type StatusError struct {
	Status  int
	PSCode  string
	Message string
}

func (e *StatusError) Error() string {
	msg := fmt.Sprintf("planetscale api: HTTP %d", e.Status)
	if e.PSCode != "" {
		msg += " (" + e.PSCode + ")"
	}
	if e.Message != "" {
		msg += ": " + e.Message
	}
	if e.Status == http.StatusUnauthorized || e.Status == http.StatusForbidden {
		msg += " — check the service token and its database access grants"
	}
	return msg
}

// IsNotFound reports whether err is a control-plane 404 — the shape
// callers branch on for existence probes (branch already exists /
// already deleted).
func IsNotFound(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Status == http.StatusNotFound
}

// SleepFor waits d via the client's injectable Sleep (ctx-aware) —
// exposed so callers layering their own retry loop over the client
// (the branch-cleanup delete retry) share the test-injectable clock
// instead of spending wall-clock in tests.
func (c *Client) SleepFor(ctx context.Context, d time.Duration) error {
	return c.sleep(ctx, d)
}

// Get issues an authenticated GET for path (e.g.
// "/v1/organizations/{org}/metrics") and decodes the JSON response
// into out.
func (c *Client) Get(ctx context.Context, path string, out any) error {
	return c.do(ctx, http.MethodGet, path, nil, out)
}

// post issues an authenticated POST with a JSON body (nil for none),
// decoding the response into out when non-nil.
func (c *Client) post(ctx context.Context, path string, body, out any) error {
	return c.do(ctx, http.MethodPost, path, body, out)
}

// del issues an authenticated DELETE for path.
func (c *Client) del(ctx context.Context, path string) error {
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}

// do runs one authenticated JSON request with the 429 retry loop.
// Error strings name only the method — never the URL (which telemetry
// pins must stay free of) and never the token.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return fmt.Errorf("planetscale api: encode %s body: %w", method, err)
		}
	}
	for attempt := 1; ; attempt++ {
		retryAfter, err := c.doOnce(ctx, method, path, payload, out)
		if err == nil {
			return nil
		}
		var se *StatusError
		if attempt >= maxAttempts || !errors.As(err, &se) || se.Status != http.StatusTooManyRequests {
			return err
		}
		if sleepErr := c.sleep(ctx, retryAfter); sleepErr != nil {
			return fmt.Errorf("planetscale api: rate-limited and cancelled while backing off: %w", sleepErr)
		}
	}
}

// doOnce runs a single request attempt. On a 429 it also returns the
// backoff the caller should wait (Retry-After when present, clamped).
func (c *Client) doOnce(ctx context.Context, method, path string, payload []byte, out any) (time.Duration, error) {
	var reqBody io.Reader = http.NoBody
	if payload != nil {
		reqBody = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return 0, fmt.Errorf("planetscale api: build %s request: %w", method, diagnose.SafeParseError(err))
	}
	// PlanetScale service-token auth: `Authorization: {TOKEN_ID}:{TOKEN}`.
	// The token value is NEVER logged; only this header carries it.
	req.Header.Set("Authorization", c.tokenID+":"+c.token)
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// client.Do wraps failures in *url.Error, whose Error() embeds
		// the full request URL; SafeParseError strips the wrapper
		// (audit N-12 — same treatment as the telemetry legs).
		return 0, fmt.Errorf("planetscale api: %s request failed: %w", method, diagnose.SafeParseError(err))
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return retryAfterOf(resp), decodeStatusError(resp)
	}
	if out == nil {
		return 0, nil
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if err != nil {
		return 0, fmt.Errorf("planetscale api: read %s response: %w", method, err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return 0, fmt.Errorf("planetscale api: parse %s response JSON: %w", method, err)
	}
	return 0, nil
}

// decodeStatusError builds the StatusError for a non-2xx response,
// decoding the PlanetScale error envelope when the body carries one.
func decodeStatusError(resp *http.Response) error {
	se := &StatusError{Status: resp.StatusCode}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyLen))
	if err == nil {
		var envelope struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		if json.Unmarshal(raw, &envelope) == nil {
			se.PSCode = envelope.Code
			se.Message = envelope.Message
		}
	}
	return se
}

// retryAfterOf resolves the wait before retrying a 429: the
// Retry-After header (whole seconds) when present, else the base
// delay; clamped to maxRetryDelay either way.
func retryAfterOf(resp *http.Response) time.Duration {
	d := baseRetryDelay
	if v := resp.Header.Get("Retry-After"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			d = time.Duration(secs) * time.Second
		}
	}
	if d > maxRetryDelay {
		d = maxRetryDelay
	}
	return d
}

// ---- typed control-plane resources ----

// Branch is the subset of the PlanetScale branch object sluice reads.
// safe_migrations is the deploy-request prerequisite (ADR-0148 ground
// truth: deploy-request creation fails on a branch without it).
type Branch struct {
	Name           string `json:"name"`
	ParentBranch   string `json:"parent_branch"`
	Ready          bool   `json:"ready"`
	Production     bool   `json:"production"`
	SafeMigrations bool   `json:"safe_migrations"`
}

// BranchPassword is a data-plane credential minted for one branch:
// connect to AccessHostURL as Username/PlainText over TLS. PlainText
// is returned ONCE at creation and never logged by sluice.
type BranchPassword struct {
	ID            string `json:"id"`
	Username      string `json:"username"`
	PlainText     string `json:"plain_text"`
	AccessHostURL string `json:"access_host_url"`
}

// DeployRequest is the subset of the deploy-request object the
// expand-contract poller drives: CanDeploy gates the deploy call,
// DeploymentState walks the lifecycle (ADR-0148 ground truth:
// open/pending → ready → queued → … → complete_pending_revert).
//
// The real GET /deploy-requests/{number} response carries the
// deployable flag ONLY inside the nested "deployment" object — there
// is no top-level "deployable" field (live-verified 2026-07-15 on a
// real deploy request; the first cut read it top-level, so every real
// run timed out at --deploy-timeout). Both locations are read so a
// response shape that does carry it top-level keeps working.
type DeployRequest struct {
	Number          int    `json:"number"`
	Branch          string `json:"branch"`
	IntoBranch      string `json:"into_branch"`
	State           string `json:"state"`
	DeploymentState string `json:"deployment_state"`
	Deployable      bool   `json:"deployable"`
	Deployment      struct {
		State      string `json:"state"`
		Deployable bool   `json:"deployable"`
	} `json:"deployment"`
	HTMLURL string `json:"html_url"`
}

// CanDeploy reports whether PlanetScale will accept a deploy call for
// this request, reading the deployable flag from wherever the response
// shape carried it (nested deployment object on the GET-by-number
// endpoint; tolerated top-level for other shapes).
func (dr *DeployRequest) CanDeploy() bool {
	return dr.Deployable || dr.Deployment.Deployable
}

// branchesPath builds the escaped org/database branch collection path.
func branchesPath(org, db string) string {
	return "/v1/organizations/" + url.PathEscape(org) + "/databases/" + url.PathEscape(db) + "/branches"
}

// deployRequestsPath builds the escaped org/database deploy-request
// collection path.
func deployRequestsPath(org, db string) string {
	return "/v1/organizations/" + url.PathEscape(org) + "/databases/" + url.PathEscape(db) + "/deploy-requests"
}

// GetBranch fetches one branch — the org/database/branch existence
// probe and the safe-migrations read.
func (c *Client) GetBranch(ctx context.Context, org, db, branch string) (*Branch, error) {
	var out Branch
	if err := c.Get(ctx, branchesPath(org, db)+"/"+url.PathEscape(branch), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CreateBranch creates a dev branch off parent.
func (c *Client) CreateBranch(ctx context.Context, org, db, name, parent string) (*Branch, error) {
	body := struct {
		Name         string `json:"name"`
		ParentBranch string `json:"parent_branch"`
	}{Name: name, ParentBranch: parent}
	var out Branch
	if err := c.post(ctx, branchesPath(org, db), body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteBranch deletes a branch (its passwords die with it).
func (c *Client) DeleteBranch(ctx context.Context, org, db, name string) error {
	return c.del(ctx, branchesPath(org, db)+"/"+url.PathEscape(name))
}

// CreateBranchPassword mints a data-plane credential for branch,
// labelled displayName in the PlanetScale UI.
func (c *Client) CreateBranchPassword(ctx context.Context, org, db, branch, displayName string) (*BranchPassword, error) {
	body := struct {
		Name string `json:"name"`
	}{Name: displayName}
	var out BranchPassword
	if err := c.post(ctx, branchesPath(org, db)+"/"+url.PathEscape(branch)+"/passwords", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CreateDeployRequest opens a deploy request merging branch into
// intoBranch.
func (c *Client) CreateDeployRequest(ctx context.Context, org, db, branch, intoBranch string) (*DeployRequest, error) {
	body := struct {
		Branch     string `json:"branch"`
		IntoBranch string `json:"into_branch"`
	}{Branch: branch, IntoBranch: intoBranch}
	var out DeployRequest
	if err := c.post(ctx, deployRequestsPath(org, db), body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetDeployRequest fetches one deploy request for state polling.
func (c *Client) GetDeployRequest(ctx context.Context, org, db string, number int) (*DeployRequest, error) {
	var out DeployRequest
	if err := c.Get(ctx, deployRequestsPath(org, db)+"/"+strconv.Itoa(number), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Deploy queues a deployable deploy request for deployment.
func (c *Client) Deploy(ctx context.Context, org, db string, number int) (*DeployRequest, error) {
	var out DeployRequest
	if err := c.post(ctx, deployRequestsPath(org, db)+"/"+strconv.Itoa(number)+"/deploy", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeployRequestDiff is one object of a deploy request's computed diff
// (GET /deploy-requests/{number}/diff — the endpoint the ADR-0148 live
// prototype exercised on a real PS-10, 2026-07-02). Name is the table
// the deployment would alter/create/drop; the response also carries
// raw/html DDL legs sluice doesn't read. The {"data":[{"name",...}]}
// envelope mirrors the live-verified branch-schema shape; the diff
// object's exact field set is DERIVED from the pscale tooling, not yet
// live-captured — the next psverify dispatch should verbatim-capture a
// real response and tighten the client_test fixture.
type DeployRequestDiff struct {
	Name string `json:"name"`
}

// GetDeployRequestDiff fetches a deploy request's computed per-object
// diff — the legRunner's pre-Deploy blast-radius assertion input
// (audit MED-D0-7): a diff object outside the leg's intended table set
// means the branch base was stale (the empirically-deployed phantom
// revert) or the branch was touched out-of-band, and deploying it
// would ship schema changes sluice never intended.
func (c *Client) GetDeployRequestDiff(ctx context.Context, org, db string, number int) ([]DeployRequestDiff, error) {
	var out struct {
		Data []DeployRequestDiff `json:"data"`
	}
	if err := c.Get(ctx, deployRequestsPath(org, db)+"/"+strconv.Itoa(number)+"/diff", &out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

// SchemaTable is one table of a branch's rendered schema (the raw DDL
// leg of GET /branches/{branch}/schema; live-verified shape
// 2026-07-15: {"data":[{"name","html","raw","annotated"}]}).
type SchemaTable struct {
	Name string `json:"name"`
	Raw  string `json:"raw"`
}

// GetBranchSchema returns a branch's rendered per-table schema DDL —
// the freshness gate's comparison input (a just-created
// PlanetScale branch's schema can lag production — observed live
// 2026-07-15, intermittent — and a deploy request from a lagging
// branch would silently revert the missing changes).
func (c *Client) GetBranchSchema(ctx context.Context, org, db, branch string) ([]SchemaTable, error) {
	var out struct {
		Data []SchemaTable `json:"data"`
	}
	if err := c.Get(ctx, branchesPath(org, db)+"/"+url.PathEscape(branch)+"/schema", &out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

// Backup is the subset of the backup object the branch-rebase flow
// drives: State walks pending/running → success (live-verified shape
// 2026-07-15).
type Backup struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

// CreateBackup starts an on-demand backup of branch — the rebase
// vehicle for a stale dev-branch base (a fresh backup makes the next
// branch creation seed from current production schema).
func (c *Client) CreateBackup(ctx context.Context, org, db, branch string) (*Backup, error) {
	var out Backup
	if err := c.post(ctx, branchesPath(org, db)+"/"+url.PathEscape(branch)+"/backups", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetBackup fetches one backup for state polling.
func (c *Client) GetBackup(ctx context.Context, org, db, branch, id string) (*Backup, error) {
	var out Backup
	if err := c.Get(ctx, branchesPath(org, db)+"/"+url.PathEscape(branch)+"/backups/"+url.PathEscape(id), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SkipRevert finalizes a deployment sitting in its revert window
// (complete_pending_revert) — PlanetScale holds the deployment "in
// progress" and blocks lifecycle ops until the window closes or is
// skipped (ADR-0148 finding #4).
func (c *Client) SkipRevert(ctx context.Context, org, db string, number int) (*DeployRequest, error) {
	var out DeployRequest
	if err := c.post(ctx, deployRequestsPath(org, db)+"/"+strconv.Itoa(number)+"/skip-revert", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
