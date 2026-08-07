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
	Number          int        `json:"number"`
	Branch          string     `json:"branch"`
	IntoBranch      string     `json:"into_branch"`
	State           string     `json:"state"`
	DeploymentState string     `json:"deployment_state"`
	Deployable      bool       `json:"deployable"`
	Approved        bool       `json:"approved"`
	Deployment      Deployment `json:"deployment"`
	HTMLURL         string     `json:"html_url"`
}

// Deployment is the nested deployment object — the progress/health half
// of the deploy-request response, live-captured 2026-07-30 mid-build on
// a real PS-160 (a 106 GB / 153 M-row `ALTER … ADD KEY ×4`). Everything
// beyond State/Deployable exists so a multi-HOUR VReplication index
// build can be NARRATED instead of looking hung; the poller reads no
// other progress source.
type Deployment struct {
	State      string `json:"state"`
	Deployable bool   `json:"deployable"`

	// AutoCutover false is a HUMAN gate: the deployment parks in
	// `pending_cutover` waiting for a person to confirm the cutover.
	// sluice's own deploy requests come back auto_cutover=true
	// (live-verified 2026-07-30), but a database configured for gated
	// deployments changes that — and an unbounded wait on a human gate
	// is exactly the "looks hung" shape the poller must refuse instead
	// of sitting in.
	AutoCutover bool `json:"auto_cutover"`

	// TableLocked / CutoverExpiring / QueuePaused are the
	// operator-visible conditions under which a healthy-looking
	// deployment stops advancing. QueuePauseReason is PlanetScale's
	// human-readable explanation when it has one (JSON null decodes to
	// "").
	TableLocked      bool   `json:"table_locked"`
	CutoverExpiring  bool   `json:"cutover_expiring"`
	QueuePaused      bool   `json:"queue_paused"`
	QueuePauseReason string `json:"queue_pause_reason"`

	// DeployOperations is the per-table/per-shard operation list;
	// DeployOperationSummaries is PlanetScale's aggregated view of the
	// same set. BOTH carry progress — the live capture held identical
	// values in each — so [DeployRequest.Progress] reads the operations
	// and falls back to the summaries rather than going blind if
	// PlanetScale ever populates only one.
	DeployOperations         []DeployOperation `json:"deploy_operations"`
	DeployOperationSummaries []DeployOperation `json:"deploy_operation_summaries"`
}

// DeployOperation is one deploy operation's progress row. The field set
// is live-captured (2026-07-30); the real response carries considerably
// more (syntax-highlighted DDL, foreign-key bookkeeping, per-shard
// sub-operations) that sluice does not read.
type DeployOperation struct {
	State              string `json:"state"`
	TableName          string `json:"table_name"`
	OperationName      string `json:"operation_name"`
	ProgressPercentage int    `json:"progress_percentage"`
	ETASeconds         int64  `json:"eta_seconds"`

	// ThrottledAt is documented as "when the deploy operation was LAST
	// throttled" — and measurement says it is NOT a live throttle gauge.
	// On the 2026-07-30 capture PlanetScale stamped it ONCE at 00:21:59
	// (two seconds BEFORE the deployment's own started_at) and it had
	// not moved 2 h 25 m later — on the very run whose PlanetScale UI
	// read "This deployment is being throttled due to replication lag
	// on your database" the whole time. So a non-nil value means
	// throttling was involved at some point; it does NOT mean
	// "throttled right now", and sluice's narration must not claim
	// otherwise. The honest live signal for a throttled build is the
	// ETA failing to converge (measured: 4435 s → 4299 s of ETA burned
	// over 353 s of wall clock).
	ThrottledAt *time.Time `json:"throttled_at"`
}

// CanDeploy reports whether PlanetScale will accept a deploy call for
// this request, reading the deployable flag from wherever the response
// shape carried it (nested deployment object on the GET-by-number
// endpoint; tolerated top-level for other shapes).
func (dr *DeployRequest) CanDeploy() bool {
	return dr.Deployable || dr.Deployment.Deployable
}

// DeployProgress is the narratable summary of a deployment's operation
// rows — what a poller logs so an operator watching only sluice's output
// can tell a healthy-but-slow build from a stuck one.
type DeployProgress struct {
	// Percent is the LOWEST progress_percentage across the operation
	// rows (a deployment is only as done as its slowest leg);
	// PercentKnown is false when no row reported one.
	Percent      int
	PercentKnown bool

	// ETA is the LONGEST eta_seconds across the rows.
	ETA      time.Duration
	ETAKnown bool

	// ThrottledAt is the most recent throttle stamp across the rows,
	// zero when none carried one. See [DeployOperation.ThrottledAt] —
	// presence is historical, not live.
	ThrottledAt time.Time

	// Operations is how many rows the summary read, so a caller can
	// tell "no progress reported" from "no operations yet" (a queued
	// deployment has none).
	Operations int
}

// Progress summarizes the deployment's operation rows.
func (dr *DeployRequest) Progress() DeployProgress {
	ops := dr.Deployment.DeployOperations
	if len(ops) == 0 {
		ops = dr.Deployment.DeployOperationSummaries
	}
	p := DeployProgress{Operations: len(ops)}
	for i := range ops {
		op := &ops[i]
		if !p.PercentKnown || op.ProgressPercentage < p.Percent {
			p.Percent = op.ProgressPercentage
			p.PercentKnown = true
		}
		if eta := time.Duration(op.ETASeconds) * time.Second; eta > p.ETA {
			p.ETA = eta
			p.ETAKnown = true
		}
		if op.ThrottledAt != nil && op.ThrottledAt.After(p.ThrottledAt) {
			p.ThrottledAt = *op.ThrottledAt
		}
	}
	return p
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

// DeployRequestRef is the DISCOVERY view of a deploy request — the three
// fields the leftover-dev-branch adoption (roadmap item 108) reads off the
// LIST endpoint, and deliberately no more. Every adoption DECISION is made
// from the GET-by-number response instead, whose shape is live-captured
// (the DR #2 fixture in client_test.go); the list is used only to learn
// WHICH number was opened from a given dev branch. So a drift in the list
// element's shape can cost sluice the discovery — which degrades to the
// loud refusal that is today's behaviour — but can never feed a wrong
// state into the adopt/refuse dispatch.
//
// Shape DERIVED from the public PlanetScale API reference, like
// [DeployRequestDiff]; the next live psverify dispatch should replace it
// with a sanitized capture.
type DeployRequestRef struct {
	Number     int    `json:"number"`
	Branch     string `json:"branch"`
	IntoBranch string `json:"into_branch"`
}

// Deploy-request listing is paginated (`page` / `per_page`; the reference
// documents per_page default 25, maximum 100). The walk is BOUNDED, and
// exhausting the bound is an ERROR rather than a short answer: the caller
// (adoption) turns "no deploy request for this branch" into "delete the
// dev branch", and handing that remedy to someone whose deploy request was
// merely on a page sluice never read would destroy a running deployment.
// Loud beats wrong.
const (
	deployRequestPageSize = 100
	deployRequestMaxItems = 2000
)

// ErrDeployRequestListTruncated reports that the deploy-request collection
// did not fit in [deployRequestMaxItems], so the returned set would not
// have been exhaustive.
var ErrDeployRequestListTruncated = errors.New("planetscale api: the database has more deploy requests than sluice enumerates")

// ListDeployRequests returns every deploy request of the database, walking
// the paginated collection to exhaustion.
//
// The walk stops on an EMPTY page rather than on a short one, which costs
// one extra request and buys independence from the server's own per_page
// cap: if PlanetScale ever answers a per_page=100 request with 25 rows,
// a short-page stop would silently truncate — and truncation here reads as
// "there is no deploy request", whose remedy is a branch delete. Pinned by
// TestClient_ListDeployRequests_ServerCapsPerPage.
//
// Callers filter by branch themselves. The collection's filter parameters
// are not part of the reference sluice models against, and a filter the
// server silently ignores would be worse than none.
func (c *Client) ListDeployRequests(ctx context.Context, org, db string) ([]DeployRequestRef, error) {
	var all []DeployRequestRef
	for page := 1; ; page++ {
		var out struct {
			Data []DeployRequestRef `json:"data"`
		}
		path := fmt.Sprintf("%s?page=%d&per_page=%d", deployRequestsPath(org, db), page, deployRequestPageSize)
		if err := c.Get(ctx, path, &out); err != nil {
			return nil, err
		}
		if len(out.Data) == 0 {
			return all, nil
		}
		all = append(all, out.Data...)
		if len(all) >= deployRequestMaxItems {
			return nil, ErrDeployRequestListTruncated
		}
	}
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
