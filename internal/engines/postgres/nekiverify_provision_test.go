//go:build nekiverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// # nekiverify Tier 2 — provisioning, and the sweep that makes it safe
//
// Neki has no image to boot, so this suite runs against a REAL PlanetScale
// Neki database that it creates and destroys per run. PlanetScale prorates,
// so a database that lives twenty minutes costs twenty minutes — which
// makes per-run provisioning strictly better than a standing fixture: no
// idle spend, and no fixture quietly drifting between runs because somebody
// tested against it by hand.
//
// Everything here is behind the `nekiverify` build tag, so it is excluded
// from every ordinary build and from `go test ./...`.
//
// # The orphan sweep is not optional, and it runs FIRST
//
// A job that creates and destroys billable infrastructure ~52 times a year
// WILL eventually die between the two: a cancelled run, a runner timeout,
// a panic before cleanup. `t.Cleanup` does not cover a runner that
// vanishes.
//
// So every run sweeps before it provisions: any `nekiverify-*` database
// older than [orphanAge] is deleted. The previous run's orphan is cleaned
// by the next run, which bounds a missed teardown at hours of a $10/month
// database rather than forever.
//
// And the sweep is LOUD. Finding an orphan is evidence that a previous run
// did not complete, which is worth noticing — a silent tidy-up would hide a
// recurring teardown failure indefinitely.
//
// # Measured timings (2026-09-11), so the job timeout is not a guess
//
//	create database (--replicas 0)   418 s to ready
//	add a shard                       31 s to ready
//	delete database                    1 s
//
// The suite provisions `--replicas 2` (the HA shape the console offers and
// therefore what customers actually run) rather than the cheaper
// single-node shape, whose availability is contested — the console refuses
// it, `pscale size cluster list` sells it, and the API creates it
// (neki-issues/NEKI-015). The HA create is UNTIMED and may be slower than
// 418 s; the timeouts below carry headroom for that.

const (
	// nekiPrefix names every database this suite creates, so the sweep can
	// recognise its own litter and can never touch an operator's database.
	nekiPrefix = "nekiverify-"

	// orphanAge is how old a nekiverify-* database must be before the sweep
	// deletes it. Long enough that a concurrent run's database is never
	// destroyed underneath it; short enough that a leak is measured in
	// hours.
	orphanAge = 3 * time.Hour

	psAPI = "https://api.planetscale.com/v1"
)

// psCreds carries the PlanetScale service-token credentials and org.
type psCreds struct {
	tokenID string
	token   string
	org     string
}

// nekiverifyCreds reads credentials, skipping the suite when they are
// absent.
//
// A skip here is turned into a FAILURE by the workflow's fail-on-skip step,
// deliberately: a live suite that green-skips means "we did not look",
// which is the dishonest outcome. The psverify audit finding (2026-07-15
// MED-T1) was exactly that — a dispatch greened while silently skipping
// four of six suites.
func nekiverifyCreds(t *testing.T) psCreds {
	t.Helper()
	c := psCreds{
		tokenID: os.Getenv("PLANETSCALE_SERVICE_TOKEN_ID"),
		token:   os.Getenv("PLANETSCALE_SERVICE_TOKEN"),
		org:     os.Getenv("PLANETSCALE_ORG"),
	}
	if c.tokenID == "" || c.token == "" || c.org == "" {
		t.Skip("nekiverify: PLANETSCALE_SERVICE_TOKEN_ID / PLANETSCALE_SERVICE_TOKEN / PLANETSCALE_ORG not set")
	}
	return c
}

// api performs one PlanetScale API call. Returns the decoded body.
//
// It follows redirects on POST, which is load-bearing rather than
// incidental: the shard-creation endpoint answers 308 from the obvious
// `…/branches/{branch}/shards` path and only works at the
// `configuration-profiles/default/` form it redirects to. Go's default
// client drops the body on a 308 redirect for non-GET, so the redirect is
// followed explicitly.
func (c psCreds) api(ctx context.Context, method, path string, body any) (map[string]any, int, error) {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return nil, 0, err
		}
	}
	do := func(url string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", c.tokenID+":"+c.token)
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{
			Timeout: 60 * time.Second,
			// Never auto-follow: a 308 on POST must be re-issued with the
			// body intact, which is done explicitly below.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
		return client.Do(req)
	}

	url := psAPI + path
	resp, err := do(url)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode == http.StatusPermanentRedirect || resp.StatusCode == http.StatusTemporaryRedirect {
		loc := resp.Header.Get("Location")
		_ = resp.Body.Close()
		if loc == "" {
			return nil, resp.StatusCode, fmt.Errorf("redirect with no Location from %s", path)
		}
		if resp, err = do(loc); err != nil {
			return nil, 0, err
		}
	}
	defer func() { _ = resp.Body.Close() }()

	var out map[string]any
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&out); err != nil && resp.StatusCode >= 300 {
		return nil, resp.StatusCode, fmt.Errorf("%s %s: HTTP %d (undecodable body)", method, path, resp.StatusCode)
	}
	if resp.StatusCode >= 300 {
		return out, resp.StatusCode, fmt.Errorf("%s %s: HTTP %d: %v", method, path, resp.StatusCode, out["message"])
	}
	return out, resp.StatusCode, nil
}

// sweepOrphans deletes every nekiverify-* database older than orphanAge,
// and reports how many it found.
//
// Runs BEFORE provisioning, and is the reason a missed teardown is bounded
// rather than permanent. A non-zero count is logged as a WARNING because it
// means a previous run did not finish cleaning up.
func sweepOrphans(ctx context.Context, t *testing.T, c psCreds) int {
	t.Helper()
	out, _, err := c.api(ctx, http.MethodGet, "/organizations/"+c.org+"/databases?per_page=100", nil)
	if err != nil {
		// A sweep that cannot run must not fail the suite — but it must be
		// visible, because it means the leak bound is not in force.
		t.Logf("nekiverify: WARNING: orphan sweep could not list databases (%v); a leaked database from a "+
			"previous run would NOT be cleaned by this run", err)
		return 0
	}
	list, _ := out["data"].([]any)
	swept := 0
	for _, item := range list {
		db, _ := item.(map[string]any)
		name, _ := db["name"].(string)
		if !strings.HasPrefix(name, nekiPrefix) {
			continue
		}
		created, _ := db["created_at"].(string)
		age := time.Duration(0)
		if ts, perr := time.Parse(time.RFC3339, created); perr == nil {
			age = time.Since(ts)
		}
		if age < orphanAge {
			continue
		}
		t.Logf("nekiverify: WARNING: sweeping orphaned database %q (age %s) — a previous run created it and did "+
			"not delete it, so that run did not complete its teardown", name, age.Truncate(time.Minute))
		if _, _, derr := c.api(ctx, http.MethodDelete, "/organizations/"+c.org+"/databases/"+name, nil); derr != nil {
			t.Logf("nekiverify: WARNING: could not delete orphan %q: %v", name, derr)
			continue
		}
		swept++
	}
	return swept
}
