// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"sluicesync.dev/sluice/internal/diagnose"
	"sluicesync.dev/sluice/internal/planetscale/api"
)

// sdTarget is one element of the PlanetScale per-org metrics
// service-discovery response (Prometheus HTTP-SD shape). CONFIRMED against
// the live endpoint 2026-06-21 (ADR-0107 impl-plan §2a). Each element
// describes one scrapeable per-branch target with a SIGNED metrics path.
type sdTarget struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

// SD label keys carried in each element's Labels map.
const (
	sdLabelMetricsPath = "__metrics_path__"
	sdLabelParamSig    = "__param_sig"
	sdLabelParamExp    = "__param_exp"
	sdLabelScheme      = "__scheme__"
	sdLabelDatabase    = "planetscale_database_name"
	sdLabelBranch      = "planetscale_branch_name"
)

// client owns the two-step PlanetScale metrics fetch: the authenticated
// per-org service-discovery call (via the shared control-plane client in
// internal/planetscale/api — one client for every PlanetScale API feature,
// ADR-0162), then the SIGNED (no-auth) per-branch scrape, which goes to
// the metrics host rather than the API and keeps its own *http.Client.
type client struct {
	api        *api.Client
	httpClient *http.Client
	org        string
}

// discover fetches the per-org metrics SD document and returns the element
// for the target branch (matched by database name, branch=main unless a
// non-empty branch is requested). It returns a clear error — never the raw
// token, never the request URL (the shared client strips both) — on
// auth/HTTP/JSON failure or when no element matches.
func (c *client) discover(ctx context.Context, database, branch string) (sdTarget, error) {
	var targets []sdTarget
	if err := c.api.Get(ctx, "/v1/organizations/"+url.PathEscape(c.org)+"/metrics", &targets); err != nil {
		return sdTarget{}, fmt.Errorf("telemetry: service discovery: %w", err)
	}
	return selectBranch(targets, database, branch)
}

// selectBranch picks the SD element for the sync's target database and
// branch. Matching by database name is the load-bearing filter (an org may
// expose many databases); branch defaults to "main" when unspecified. A
// non-match is a clear error naming the database, not a silent empty
// snapshot — opt-in telemetry that silently watches the wrong DB would be
// worse than no telemetry.
func selectBranch(targets []sdTarget, database, branch string) (sdTarget, error) {
	if branch == "" {
		branch = "main"
	}
	for _, t := range targets {
		if t.Labels[sdLabelDatabase] != database {
			continue
		}
		if t.Labels[sdLabelBranch] != branch {
			continue
		}
		if t.Labels[sdLabelMetricsPath] == "" {
			return sdTarget{}, fmt.Errorf(
				"telemetry: SD element for database %q branch %q has no metrics path", database, branch,
			)
		}
		return t, nil
	}
	return sdTarget{}, fmt.Errorf(
		"telemetry: no SD target for database %q branch %q (check --planetscale-org and the target DSN's database)",
		database, branch,
	)
}

// scrape fetches the SIGNED per-branch metrics URL for the discovered
// target and returns the raw Prometheus exposition text. The URL is signed
// (sig + exp query params) so it needs NO Authorization header — the token
// never travels on the scrape leg.
func (c *client) scrape(ctx context.Context, t sdTarget) (string, error) {
	scrapeURL, err := buildScrapeURL(t)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, scrapeURL, http.NoBody)
	if err != nil {
		return "", fmt.Errorf("telemetry: build scrape request: %w", diagnose.SafeParseError(err))
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		// The scrape URL is SIGNED (sig + exp query params) — it is a
		// bearer credential for the metrics endpoint until exp. client.Do
		// wraps failures in *url.Error, whose Error() embeds that full URL;
		// SafeParseError strips the wrapper so the signature never reaches
		// a WARN log (audit N-12).
		return "", fmt.Errorf("telemetry: scrape request failed: %w", diagnose.SafeParseError(err))
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("telemetry: scrape returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024*1024))
	if err != nil {
		return "", fmt.Errorf("telemetry: read scrape body: %w", err)
	}
	return string(body), nil
}

// buildScrapeURL assembles the signed scrape URL from the SD element:
// `https://<target><__metrics_path__>?sig=<__param_sig>&exp=<__param_exp>`.
// scheme + host come from the SD element (scheme defaults to https; the
// metrics host is the first entry of Targets, e.g. metrics.psdb.cloud).
func buildScrapeURL(t sdTarget) (string, error) {
	if len(t.Targets) == 0 {
		return "", errors.New("telemetry: SD element has no metrics target host")
	}
	scheme := t.Labels[sdLabelScheme]
	if scheme == "" {
		scheme = "https"
	}
	host := t.Targets[0]
	path := t.Labels[sdLabelMetricsPath]
	if path == "" {
		return "", errors.New("telemetry: SD element has no metrics path")
	}
	u := url.URL{Scheme: scheme, Host: host, Path: path}
	q := url.Values{}
	if sig := t.Labels[sdLabelParamSig]; sig != "" {
		q.Set("sig", sig)
	}
	if exp := t.Labels[sdLabelParamExp]; exp != "" {
		q.Set("exp", exp)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}
